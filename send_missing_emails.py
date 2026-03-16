"""
Send Email 1 to SENT contacts that never actually received it in GHL.
Checks GHL conversation history — if no outbound email exists, sends it now.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

SHEET_ID        = os.environ["SHEET_ID"]
GOOGLE_SA_JSON  = os.environ["GOOGLE_SERVICE_ACCOUNT"]
GHL_API_KEY     = os.environ["GHL_API_KEY"].strip()
GHL_BASE        = "https://services.leadconnectorhq.com"
GHL_VERSION     = "2021-07-28"
GHL_LOCATION_ID = "Y6vbtAtrSByzFIROGlK5"
SENDER_NAME     = "Dario Jovanovski"
SENDER_EMAIL    = "support@parakeeet.com"

C_STATUS     = 14   # O
C_EMAIL      = 6    # G
C_COMPANY    = 5    # F
C_EMAIL_SENT = 18   # S


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


def ghl_get_contact(contact_id: str) -> dict:
    r = requests.get(f"{GHL_BASE}/contacts/{contact_id}", headers=_gh(), timeout=15)
    r.raise_for_status()
    return r.json().get("contact", {})


def ghl_has_outbound_email(contact_id: str) -> bool:
    """Returns True if GHL already has an outbound email in this contact's conversation."""
    r = requests.get(
        f"{GHL_BASE}/conversations/search",
        params={"contactId": contact_id, "locationId": GHL_LOCATION_ID},
        headers=_gh(), timeout=15,
    )
    r.raise_for_status()
    convs = r.json().get("conversations", [])
    if not convs:
        return False

    conv_id = convs[0]["id"]
    rm = requests.get(
        f"{GHL_BASE}/conversations/{conv_id}/messages",
        headers=_gh(), timeout=15,
    )
    rm.raise_for_status()
    messages = rm.json().get("messages", {}).get("messages", [])
    for msg in messages:
        # direction 1 = outbound, type Email
        if msg.get("direction") == "outbound" and msg.get("messageType") == "Email":
            return True
        # fallback: some versions use "TYPE_EMAIL" or "type": 3
        if msg.get("type") in ("Email", 3) and msg.get("direction") in ("outbound", 1):
            return True
    return False


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


# ── Load sheet ────────────────────────────────────────────────────────────────
sa_info = json.loads(GOOGLE_SA_JSON)
creds = Credentials.from_service_account_info(
    sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
svc = build("sheets", "v4", credentials=creds).spreadsheets()
rows = (
    svc.values()
    .get(spreadsheetId=SHEET_ID, range="MASTER_450!A2:V")
    .execute()
    .get("values", [])
)

sent = already_sent = skipped = not_found = 0

for i, row in enumerate(rows):
    sheet_row = i + 2
    if safe(row, C_STATUS) != "SENT":
        continue

    email   = safe(row, C_EMAIL)
    company = safe(row, C_COMPANY)
    if not email:
        continue

    print(f"\n[Row {sheet_row}] {company} ({email})")

    ghl_id = ghl_find_contact(email)
    if not ghl_id:
        print(f"  NOT FOUND in GHL — skipping")
        not_found += 1
        continue

    # Check if outbound email already exists in GHL
    if ghl_has_outbound_email(ghl_id):
        print(f"  Email already in GHL — nothing to do")
        already_sent += 1
        continue

    # No outbound email found — read stored subject/body from custom fields
    contact_data = ghl_get_contact(ghl_id)
    subject = ""
    body = ""
    for cf in contact_data.get("customFields", []):
        key = cf.get("key", "")
        val = cf.get("fieldValue", "") or cf.get("value", "")
        if key == "email_subject":
            subject = val
        elif key == "email_body":
            body = val

    if not subject or not body:
        print(f"  No email_subject/email_body in GHL custom fields — skipping")
        skipped += 1
        continue

    try:
        ghl_send_email(ghl_id, email, subject, body)
        # Update email_sent_time in sheet
        now_iso = datetime.now(timezone.utc).isoformat()
        svc.values().update(
            spreadsheetId=SHEET_ID,
            range=f"MASTER_450!{letter(C_EMAIL_SENT)}{sheet_row}",
            valueInputOption="RAW",
            body={"values": [[now_iso]]},
        ).execute()
        print(f"  Email 1 SENT: {subject}")
        sent += 1
    except Exception as e:
        print(f"  ERROR: {e}")
        skipped += 1

print(f"\n{'='*50}")
print(f"Done.  Sent: {sent} | Already had email: {already_sent} | Not in GHL: {not_found} | Errors/skipped: {skipped}")
