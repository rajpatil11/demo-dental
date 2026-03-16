"""
Backfill Email 1 for SENT contacts that never received it.
Reads research_brief + vapi_link from the sheet, regenerates the email
via Claude, sends via GHL, and stamps email_sent_time + subject/body in sheet.

Safe to re-run — skips any contact that already has email_sent_time AND
an outbound email confirmed in GHL.

Run locally:  python3 backfill_emails.py
Run on CI:    triggered via GitHub Actions workflow_dispatch (backfill.yml)
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

SHEET_ID        = os.environ["SHEET_ID"]
GOOGLE_SA_JSON  = os.environ["GOOGLE_SERVICE_ACCOUNT"]
GHL_API_KEY     = os.environ["GHL_API_KEY"].strip()
CLAUDE_API_KEY  = os.environ["CLAUDE_API_KEY"].strip()
VAPI_API_KEY    = os.environ.get("VAPI_API_KEY", "").strip()

GHL_BASE        = "https://services.leadconnectorhq.com"
GHL_VERSION     = "2021-07-28"
GHL_LOCATION_ID = "Y6vbtAtrSByzFIROGlK5"
SENDER_NAME     = "Dario Jovanovski"
SENDER_EMAIL    = "support@parakeeet.com"
CALENDAR_LINK   = "https://cal.com/parakeeet.ai/strategy-call-exelvo-ai"
CLAUDE_MODEL    = "claude-sonnet-4-20250514"

C_STATUS        = 14   # O
C_EMAIL         = 6    # G
C_COMPANY       = 5    # F
C_FIRST_NAME    = 2    # C
C_LAST_NAME     = 3    # D
C_TITLE         = 4    # E
C_WEBSITE       = 7    # H
C_PHONE         = 8    # I
C_CITY          = 9    # J
C_STATE         = 10   # K
C_VAPI_LINK     = 16   # Q
C_BRIEF         = 17   # R
C_EMAIL_SENT    = 18   # S
C_EMAIL_SUBJECT = 22   # W
C_EMAIL_BODY    = 23   # X


def letter(col_idx: int) -> str:
    return chr(ord("A") + col_idx)


def safe(row: list, idx: int) -> str:
    try:
        return str(row[idx]).strip()
    except IndexError:
        return ""


def _gh() -> dict:
    return {
        "Authorization": f"Bearer {GHL_API_KEY}",
        "Version": GHL_VERSION,
        "Content-Type": "application/json",
    }


# ── Claude ────────────────────────────────────────────────────────────────────
def make_email(contact: dict, vapi_link: str, brief: str) -> tuple[str, str]:
    system = (
        "You are Dario Jovanovski, founder of EXELVO AI. "
        "You personally looked at this healthcare organization's website. "
        "You found one specific real problem. You already built an AI receptionist named Rachel for them.\n\n"
        "The research brief tells you the PRACTICE TYPE (dental clinic, hospital, orthopedic, etc.). "
        "Adapt every beat to that type — never default to generic dental language if it's a different specialty.\n\n"
        "Write like a real founder — not a marketer. Short sentences. Confident. Zero fluff. Under 120 words total.\n\n"
        "EXACT 5-BEAT STRUCTURE:\n\n"
        "Hey [First Name],\n\n"
        "Beat 1 — ONE sentence. Specific to THIS organization. Must reference something real from the research brief. "
        "Do not open with 'I noticed', 'I saw', or 'I came across'. Start with an observation about them.\n\n"
        "Beat 2 — ONE sentence. The problem you identified — what is costing them patients or time right now.\n\n"
        "Beat 3 — ONE sentence. What you already built: 'So I built Rachel — an AI receptionist for [Company].'\n\n"
        "Beat 4 — The demo link on its own line:\n"
        "Talk to her here: [DEMO_LINK]\n\n"
        "Beat 5 — ONE sentence CTA. Soft ask for 10 minutes.\n\n"
        "Sign off:\n"
        "Dario\nEXELVO AI\n\n"
        "BANNED WORDS: excited, reaching out, hope this finds you, I came across, "
        "I noticed, innovative, cutting-edge, revolutionize, transform, leverage, synergy.\n\n"
        "QUALITY CHECK: Read it back. Does beat 1 reference something specific to THIS organization? "
        "Is the total under 120 words? Good.\n\n"
        "Output format — two lines, then the email body:\n"
        "SUBJECT: [subject line]\n"
        "---\n"
        "[email body]"
    )
    prompt = (
        f"Contact: {contact['first_name']} {contact['last_name']}, {contact['title']}\n"
        f"Organization: {contact['company']}\n"
        f"Location: {contact['city']}, {contact['state']}\n"
        f"Demo link: {vapi_link}\n\n"
        f"Research brief:\n{brief}"
    )
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": CLAUDE_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": CLAUDE_MODEL,
            "max_tokens": 1024,
            "system": system,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=60,
    )
    r.raise_for_status()
    raw = r.json()["content"][0]["text"].strip()

    lines = raw.split("\n")
    subject = ""
    body_lines = []
    past_divider = False
    for line in lines:
        if line.startswith("SUBJECT:"):
            subject = line.replace("SUBJECT:", "").strip()
        elif line.strip() == "---":
            past_divider = True
        elif past_divider:
            body_lines.append(line)

    body = "\n".join(body_lines).strip()
    if not subject:
        subject = f"AI receptionist for {contact['company']}"
    return subject, body


# ── GHL ───────────────────────────────────────────────────────────────────────
def ghl_find_contact(email: str) -> str | None:
    r = requests.get(
        f"{GHL_BASE}/contacts/",
        params={"locationId": GHL_LOCATION_ID, "query": email},
        headers=_gh(), timeout=15,
    )
    r.raise_for_status()
    for c in r.json().get("contacts", []):
        if c.get("email", "").lower() == email.lower():
            return c["id"]
    return None


def ghl_update_contact(contact_id: str, fields: dict):
    custom_fields = [{"key": k, "field_value": v} for k, v in fields.items()]
    r = requests.put(
        f"{GHL_BASE}/contacts/{contact_id}",
        json={"customFields": custom_fields},
        headers=_gh(), timeout=15,
    )
    r.raise_for_status()


def ghl_send_email(contact_id: str, to_email: str, subject: str, plain_body: str):
    r = requests.get(
        f"{GHL_BASE}/conversations/search",
        params={"contactId": contact_id, "locationId": GHL_LOCATION_ID},
        headers=_gh(), timeout=15,
    )
    r.raise_for_status()
    convs = r.json().get("conversations", [])

    if convs:
        conv_id = convs[0]["id"]
    else:
        rc = requests.post(
            f"{GHL_BASE}/conversations/",
            json={"contactId": contact_id, "locationId": GHL_LOCATION_ID},
            headers=_gh(), timeout=15,
        )
        rc.raise_for_status()
        conv_id = rc.json()["conversation"]["id"]

    html_body = "".join(
        f"<p>{line}</p>" if line.strip() else "<br>"
        for line in plain_body.split("\n")
    )
    rs = requests.post(
        f"{GHL_BASE}/conversations/messages",
        json={
            "type": "Email",
            "conversationId": conv_id,
            "contactId": contact_id,
            "subject": subject,
            "html": html_body,
            "emailFrom": f"{SENDER_NAME} <{SENDER_EMAIL}>",
            "emailTo": to_email,
        },
        headers=_gh(), timeout=15,
    )
    rs.raise_for_status()
    return rs.json()


def ghl_has_outbound_email(contact_id: str) -> bool:
    r = requests.get(
        f"{GHL_BASE}/conversations/search",
        params={"contactId": contact_id, "locationId": GHL_LOCATION_ID},
        headers=_gh(), timeout=15,
    )
    r.raise_for_status()
    convs = r.json().get("conversations", [])
    if not convs:
        return False
    conv = convs[0]
    # GHL surfaces lastMessageType on the conversation object
    if conv.get("lastMessageType") in ("TYPE_EMAIL", "Email"):
        if conv.get("lastMessageDirection") in ("outbound", 1):
            return True
    # Fallback: check unread count and type — if email was sent, emailCount > 0
    if conv.get("type") == "email" and conv.get("lastOutboundMessageId"):
        return True
    return False


# ── Sheet ─────────────────────────────────────────────────────────────────────
sa_info = json.loads(GOOGLE_SA_JSON)
creds = Credentials.from_service_account_info(
    sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
svc = build("sheets", "v4", credentials=creds).spreadsheets()
rows = (
    svc.values()
    .get(spreadsheetId=SHEET_ID, range="MASTER_450!A2:X")
    .execute()
    .get("values", [])
)

sent = already_done = skipped = not_found = 0

for i, row in enumerate(rows):
    sheet_row = i + 2
    if safe(row, C_STATUS) != "SENT":
        continue

    email           = safe(row, C_EMAIL)
    company         = safe(row, C_COMPANY)
    vapi_link       = safe(row, C_VAPI_LINK)
    brief           = safe(row, C_BRIEF)
    email_sent_time = safe(row, C_EMAIL_SENT)
    stored_subject  = safe(row, C_EMAIL_SUBJECT)

    if not email or not vapi_link or not brief:
        print(f"[Row {sheet_row}] {company} — missing data in sheet, skipping")
        skipped += 1
        continue

    print(f"\n[Row {sheet_row}] {company} ({email})")

    # Already sent and subject stored in sheet → skip
    if email_sent_time and stored_subject:
        print(f"  Already complete (sent: {email_sent_time})")
        already_done += 1
        continue

    ghl_id = ghl_find_contact(email)
    if not ghl_id:
        print(f"  NOT FOUND in GHL — skipping")
        not_found += 1
        continue

    # Check GHL outbound email as secondary confirmation
    if email_sent_time and ghl_has_outbound_email(ghl_id):
        print(f"  Email confirmed in GHL — skipping")
        already_done += 1
        continue

    contact = {
        "first_name": safe(row, C_FIRST_NAME),
        "last_name":  safe(row, C_LAST_NAME),
        "title":      safe(row, C_TITLE),
        "company":    company,
        "email":      email,
        "city":       safe(row, C_CITY),
        "state":      safe(row, C_STATE),
    }

    try:
        print(f"  Generating email via Claude...")
        subject, body = make_email(contact, vapi_link, brief)
        print(f"  Subject: {subject}")

        ghl_update_contact(ghl_id, {
            "email_subject": subject,
            "email_body":    body,
            "vapi_link":     vapi_link,
        })
        ghl_send_email(ghl_id, email, subject, body)
        print(f"  Email sent via GHL")

        now_iso = datetime.now(timezone.utc).isoformat()
        # Write email_sent_time, subject, body to sheet
        svc.values().update(
            spreadsheetId=SHEET_ID,
            range=f"MASTER_450!{letter(C_EMAIL_SENT)}{sheet_row}",
            valueInputOption="RAW",
            body={"values": [[now_iso]]},
        ).execute()
        svc.values().update(
            spreadsheetId=SHEET_ID,
            range=f"MASTER_450!{letter(C_EMAIL_SUBJECT)}{sheet_row}:{letter(C_EMAIL_BODY)}{sheet_row}",
            valueInputOption="RAW",
            body={"values": [[subject, body]]},
        ).execute()
        print(f"  Sheet updated")
        sent += 1
        time.sleep(5)   # small gap between contacts

    except Exception as e:
        print(f"  ERROR: {e}")
        skipped += 1

print(f"\n{'='*50}")
print(f"Done.  Sent: {sent} | Already done: {already_done} | Not in GHL: {not_found} | Errors: {skipped}")
