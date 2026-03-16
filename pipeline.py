"""
EXELVO AI — Automated Outbound Email Pipeline
Runs Mon–Fri at 5pm IST (11:30 UTC) via GitHub Actions.
Processes up to 15 PENDING dental contacts per run.
"""

from __future__ import annotations

import os
import sys
import json
import time
import logging
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ── Hardcoded constants ───────────────────────────────────────────────────────
GHL_LOCATION_ID  = "Y6vbtAtrSByzFIROGlK5"
SHEET_ID_DEFAULT = "1v63RXp3-OF-RVCxWvhKk8qXfxzXPd_3ZaZ7algdIUho"
CALENDAR_LINK    = "https://cal.com/parakeeet.ai/strategy-call-exelvo-ai"
WEBSITE          = "https://www.exelvoai.com"
SENDER_NAME      = "Dario Jovanovski"
SENDER_COMPANY   = "EXELVO AI"
DAILY_LIMIT      = 15
AGENT_GAP_SECS   = 120

# ── Secrets from environment ──────────────────────────────────────────────────
GHL_API_KEY   = os.environ["GHL_API_KEY"]
VAPI_API_KEY  = os.environ["VAPI_API_KEY"]
CLAUDE_API_KEY = os.environ["CLAUDE_API_KEY"].strip()
SHEET_ID       = os.environ.get("SHEET_ID", SHEET_ID_DEFAULT)
# GOOGLE_SERVICE_ACCOUNT must be the full JSON string of the service account
GOOGLE_SA_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT"]

# ── API bases ─────────────────────────────────────────────────────────────────
GHL_BASE      = "https://services.leadconnectorhq.com"
GHL_VERSION   = "2021-07-28"
VAPI_BASE     = "https://api.vapi.ai"
VAPI_VOICE_ID = "21m00Tcm4TlvDq8ikWAM"   # Rachel — ElevenLabs, top US female voice
CLAUDE_MODEL  = "claude-sonnet-4-20250514"
HAIKU_MODEL   = "claude-sonnet-4-6"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Column mapping (0-indexed) ────────────────────────────────────────────────
# Sheet: Rank(A), Campaign Day(B), First Name(C), Last Name(D), Title(E),
#        Company(F), Email(G), Website(H), Phone(I), City(J), State(K),
#        LinkedIn(L), Score(M), Score Reason(N), Status(O), Vapi Agent ID(P),
#        Vapi Link(Q), Research Brief(R), Email Sent Time(S),
#        Reply Received(T), Booked(U), Notes(V)
C = {
    "campaign_day":    1,   # B
    "first_name":      2,   # C
    "last_name":       3,   # D
    "title":           4,   # E
    "company":         5,   # F
    "email":           6,   # G
    "website":         7,   # H
    "phone":           8,   # I
    "city":            9,   # J
    "state":           10,  # K
    "linkedin":        11,  # L
    "status":          14,  # O
    "vapi_agent_id":   15,  # P
    "vapi_link":       16,  # Q
    "research_brief":  17,  # R
    "email_sent_time": 18,  # S
    "replied":         19,  # T
    "booked":          20,  # U
    "notes":           21,  # V
}


def letter(col_idx: int) -> str:
    return chr(ord("A") + col_idx)


def safe(row: list, idx: int) -> str:
    try:
        return str(row[idx]).strip()
    except IndexError:
        return ""


# ── Google Sheets ─────────────────────────────────────────────────────────────
def sheets_service():
    sa_info = json.loads(GOOGLE_SA_JSON)
    creds = Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds).spreadsheets()


def read_all_rows(svc) -> list[list]:
    res = svc.values().get(spreadsheetId=SHEET_ID, range="MASTER_450!A2:V").execute()
    return res.get("values", [])


def set_cell(svc, row: int, col_idx: int, value: str):
    """row is 1-indexed sheet row number."""
    cell = f"MASTER_450!{letter(col_idx)}{row}"
    svc.values().update(
        spreadsheetId=SHEET_ID,
        range=cell,
        valueInputOption="RAW",
        body={"values": [[value]]},
    ).execute()


def set_cells(svc, row: int, start_col: int, values: list):
    end_col = start_col + len(values) - 1
    range_ = f"MASTER_450!{letter(start_col)}{row}:{letter(end_col)}{row}"
    svc.values().update(
        spreadsheetId=SHEET_ID,
        range=range_,
        valueInputOption="RAW",
        body={"values": [values]},
    ).execute()


# ── Scraper ───────────────────────────────────────────────────────────────────
def scrape(url: str) -> str:
    if not url.startswith("http"):
        url = "https://" + url
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        text = " ".join(soup.get_text(separator=" ").split())
        return text[:4000]
    except Exception as e:
        log.warning(f"Scrape failed for {url}: {e}")
        return ""


# ── Claude ────────────────────────────────────────────────────────────────────
def ask_claude(system: str, user: str, model: str = CLAUDE_MODEL) -> str:
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": CLAUDE_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": model,
            "max_tokens": 1024,
            "system": system,
            "messages": [{"role": "user", "content": user}],
        },
        timeout=60,
    )
    if not r.ok:
        raise RuntimeError(f"Claude API error {r.status_code}: {r.text[:200]}")
    return r.json()["content"][0]["text"].strip()


def make_research_brief(contact: dict, site_text: str) -> str:
    system = (
        "You are a sharp B2B sales researcher. Given a dental practice's website content, "
        "write a 3–4 sentence research brief covering: what services they offer, what pain "
        "points they likely have around missed calls or appointment booking, and one specific "
        "detail from their site that a sales rep could reference in an email. Be factual and "
        "specific — no generic filler."
    )
    prompt = (
        f"Practice: {contact['company']}\n"
        f"Location: {contact['city']}, {contact['state']}\n"
        f"Website content:\n{site_text or 'No content scraped.'}"
    )
    return ask_claude(system, prompt)


def make_vapi_system_prompt(contact: dict, brief: str) -> str:
    system = (
        "Write a concise Vapi voice agent system prompt (under 300 words) for an AI receptionist "
        "named Rachel at a dental practice. The prompt must cover: greeting callers warmly, handling "
        "new patient inquiries, appointment booking requests, and missed call follow-ups. "
        "At the end of every call, Rachel must naturally mention she was built by EXELVO AI and "
        "offer the caller a link to book a strategy call with the EXELVO AI team. "
        "Write only the system prompt — no intro, no explanation."
    )
    prompt = (
        f"Practice: {contact['company']}\n"
        f"Location: {contact['city']}, {contact['state']}\n"
        f"Research brief: {brief}\n"
        f"EXELVO AI calendar link: {CALENDAR_LINK}"
    )
    return ask_claude(system, prompt)


def make_email(contact: dict, vapi_link: str, brief: str) -> tuple[str, str]:
    """Returns (subject, body). Body is plain text, under 100 words."""
    system = (
        "You write short cold emails for a B2B AI agency. Rules:\n"
        "1. Under 100 words total in the body\n"
        "2. Sound like a real person texting a peer — zero fluff, zero buzzwords\n"
        "3. Reference one specific pain point or detail from the research brief\n"
        "4. Say 'I built an AI receptionist for [Practice Name]' naturally\n"
        "5. Include the Vapi demo link with a clear CTA to try it\n"
        "6. Include the EXELVO AI website link\n"
        "7. Include the calendar booking link\n"
        "8. No bullet points in the body\n"
        "9. Sign off as sender name\n\n"
        "Output format — exactly two lines, nothing else:\n"
        "Subject: <subject line>\n"
        "Body: <full email body, newlines as \\n>"
    )
    prompt = (
        f"Contact: {contact['first_name']} {contact['last_name']}, "
        f"{contact['title'] or 'Owner'} at {contact['company']}, "
        f"{contact['city']}, {contact['state']}\n"
        f"Research brief: {brief}\n"
        f"Vapi demo link: {vapi_link}\n"
        f"EXELVO AI website: {WEBSITE}\n"
        f"Calendar link: {CALENDAR_LINK}\n"
        f"Sender: {SENDER_NAME}, {SENDER_COMPANY}"
    )
    raw = ask_claude(system, prompt)
    subject, body = "", ""
    for line in raw.splitlines():
        if line.lower().startswith("subject:"):
            subject = line.split(":", 1)[1].strip()
        elif line.lower().startswith("body:"):
            body = line.split(":", 1)[1].strip().replace("\\n", "\n")
    if not subject or not body:
        lines = raw.strip().splitlines()
        subject = lines[0].replace("Subject:", "").strip() if lines else "Quick question"
        body = "\n".join(lines[1:]).strip() if len(lines) > 1 else raw
    return subject, body


# ── Vapi ──────────────────────────────────────────────────────────────────────
def _vh() -> dict:
    return {"Authorization": f"Bearer {VAPI_API_KEY}", "Content-Type": "application/json"}


def create_vapi_agent(contact: dict, system_prompt: str) -> tuple[str, str]:
    """Creates Vapi assistant. Returns (agent_id, vapi_demo_url)."""
    payload = {
        "name": f"Rachel — {contact['company']}"[:40],
        "firstMessage": (
            f"Thank you for calling {contact['company']}, this is Rachel. "
            "How can I help you today?"
        ),
        "model": {
            "provider": "anthropic",
            "model": HAIKU_MODEL,
            "systemPrompt": system_prompt,
        },
        "voice": {
            "provider": "11labs",
            "voiceId": VAPI_VOICE_ID,
        },
        "transcriber": {
            "provider": "deepgram",
            "model": "nova-2",
        },
        "analysisPlan": {
            "structuredDataPlan": {
                "enabled": True,
                "schema": {
                    "type": "object",
                    "properties": {
                        "appointmentBooked": {
                            "type": "boolean",
                            "description": "Whether an appointment was booked during the call",
                        },
                        "appointmentCancelled": {
                            "type": "boolean",
                            "description": "Whether an appointment was cancelled during the call",
                        },
                        "appointmentRescheduled": {
                            "type": "boolean",
                            "description": "Whether an appointment was rescheduled during the call",
                        },
                        "appointmentDate": {
                            "type": "string",
                            "description": "The scheduled appointment date if booked or rescheduled",
                        },
                        "appointmentTime": {
                            "type": "string",
                            "description": "The scheduled appointment time if booked or rescheduled",
                        },
                        "customerSentiment": {
                            "type": "string",
                            "description": "Overall emotional tone of the customer: positive, neutral, or negative",
                        },
                    },
                    "required": ["appointmentBooked", "appointmentCancelled", "appointmentRescheduled"],
                },
            },
        },
    }
    r = requests.post(f"{VAPI_BASE}/assistant", json=payload, headers=_vh(), timeout=30)
    if not r.ok:
        log.error(f"Vapi error {r.status_code}: {r.text}")
    r.raise_for_status()
    agent_id = r.json()["id"]

    demo_base = os.environ.get("DEMO_BASE_URL", "https://rajpatil11.github.io/demo-dental")
    web_call_url = f"{demo_base}/demo.html?assistantId={agent_id}&practice={requests.utils.quote(contact['company'])}"
    return agent_id, web_call_url


def delete_vapi_agent(agent_id: str):
    try:
        r = requests.delete(f"{VAPI_BASE}/assistant/{agent_id}", headers=_vh(), timeout=15)
        r.raise_for_status()
        log.info(f"    Deleted Vapi agent {agent_id}")
    except Exception as e:
        log.warning(f"    Could not delete Vapi agent {agent_id}: {e}")


# ── GHL ───────────────────────────────────────────────────────────────────────
def _gh() -> dict:
    return {
        "Authorization": f"Bearer {GHL_API_KEY}",
        "Version": GHL_VERSION,
        "Content-Type": "application/json",
    }


def ghl_find_contact(email: str) -> str | None:
    params = {"locationId": GHL_LOCATION_ID, "query": email}
    r = requests.get(f"{GHL_BASE}/contacts/", params=params, headers=_gh(), timeout=15)
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
        headers=_gh(),
        timeout=15,
    )
    r.raise_for_status()


def ghl_add_tag(contact_id: str, tag: str):
    r = requests.post(
        f"{GHL_BASE}/contacts/{contact_id}/tags",
        json={"tags": [tag]},
        headers=_gh(),
        timeout=15,
    )
    r.raise_for_status()


# ── Cleanup job ───────────────────────────────────────────────────────────────
def run_cleanup(svc):
    log.info("━━━ CLEANUP JOB ━━━")
    rows = read_all_rows(svc)
    now = datetime.now(timezone.utc)
    cleaned = 0

    for i, row in enumerate(rows):
        sheet_row = i + 2
        if (safe(row, C["status"]) != "SENT"
                or safe(row, C["replied"])
                or not safe(row, C["email_sent_time"])
                or not safe(row, C["vapi_agent_id"])):
            continue

        try:
            sent_dt = datetime.fromisoformat(safe(row, C["email_sent_time"]))
            if sent_dt.tzinfo is None:
                sent_dt = sent_dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

        age_days = (now - sent_dt).days
        if age_days < 9:
            continue

        agent_id = safe(row, C["vapi_agent_id"])
        company  = safe(row, C["company"])
        log.info(f"  Row {sheet_row} | {company} | {age_days}d old — deleting agent")
        delete_vapi_agent(agent_id)
        set_cell(svc, sheet_row, C["status"],       "COLD")
        set_cell(svc, sheet_row, C["vapi_agent_id"], "")
        cleaned += 1

    log.info(f"Cleanup complete — {cleaned} agent(s) removed.\n")


# ── Single contact processor ──────────────────────────────────────────────────
def process_contact(svc, row: list, sheet_row: int) -> bool:
    contact = {
        "first_name": safe(row, C["first_name"]),
        "last_name":  safe(row, C["last_name"]),
        "title":      safe(row, C["title"]),
        "company":    safe(row, C["company"]),
        "email":      safe(row, C["email"]),
        "website":    safe(row, C["website"]),
        "phone":      safe(row, C["phone"]),
        "city":       safe(row, C["city"]),
        "state":      safe(row, C["state"]),
    }
    name = f"{contact['first_name']} {contact['last_name']}"
    log.info(f"  Processing: {name} | {contact['company']} | {contact['email']}")

    # Guard: skip if agent already exists (no duplicates)
    if safe(row, C["vapi_agent_id"]):
        log.info("  Skipping — Vapi agent already exists for this contact.")
        return False

    # Lock the row immediately
    set_cell(svc, sheet_row, C["status"], "IN_PROGRESS")

    try:
        log.info("  [1/7] Scraping website...")
        site_text = scrape(contact["website"])
        log.info(f"        {len(site_text)} chars scraped")

        log.info("  [2/7] Writing research brief...")
        brief = make_research_brief(contact, site_text)
        log.info(f"        Brief: {brief[:120]}...")

        log.info("  [3/7] Writing Vapi system prompt...")
        vapi_prompt = make_vapi_system_prompt(contact, brief)
        log.info(f"        Prompt: {vapi_prompt[:80]}...")

        log.info("  [4/7] Creating Vapi agent...")
        agent_id, vapi_link = create_vapi_agent(contact, vapi_prompt)
        log.info(f"        Agent ID: {agent_id}")
        log.info(f"        Demo link: {vapi_link}")

        log.info("  [5/7] Writing cold email...")
        subject, body = make_email(contact, vapi_link, brief)
        log.info(f"        Subject: {subject}")

        log.info("  [6/7] Updating GHL CRM...")
        ghl_id = ghl_find_contact(contact["email"])
        if ghl_id:
            ghl_update_contact(ghl_id, {
                "vapi_link":      vapi_link,
                "vapi_agent_id":  agent_id,
                "research_brief": brief,
                "email_subject":  subject,
                "email_body":     body,
                "day_added":      datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            })
            ghl_add_tag(ghl_id, "Day1-Campaign-Live")
            log.info(f"        GHL contact {ghl_id} updated + tagged")
        else:
            log.warning("        GHL contact not found — CRM skipped")

        log.info("  [7/7] Updating Google Sheet...")
        now_iso = datetime.now(timezone.utc).isoformat()
        set_cells(svc, sheet_row, C["vapi_agent_id"],
                  [agent_id, vapi_link, brief, now_iso])
        set_cell(svc, sheet_row, C["status"], "SENT")
        log.info(f"  ✓ SENT\n")
        return True

    except Exception as e:
        log.error(f"  ✗ FAILED: {e}\n")
        set_cell(svc, sheet_row, C["status"], "FAILED")
        set_cell(svc, sheet_row, C["notes"],  str(e)[:200])
        return False


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log.info("EXELVO AI — Daily Outbound Pipeline")
    log.info(f"Run time: {datetime.now(timezone.utc).isoformat()}")
    log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

    svc = sheets_service()
    run_cleanup(svc)

    log.info("━━━ MAIN PIPELINE ━━━")
    rows = read_all_rows(svc)
    log.info(f"Loaded {len(rows)} rows. Looking for PENDING contacts...\n")

    sent = failed = skipped = 0

    for i, row in enumerate(rows):
        if sent >= DAILY_LIMIT:
            log.info(f"Daily limit of {DAILY_LIMIT} reached.")
            break

        sheet_row = i + 2
        status = safe(row, C["status"])

        if status != "PENDING":
            skipped += 1
            continue

        success = process_contact(svc, row, sheet_row)
        if success:
            sent += 1
        else:
            failed += 1

        # Wait before next Vapi creation (skip wait after last contact)
        if sent < DAILY_LIMIT and (i + 1 < len(rows)):
            remaining = [r for r in rows[i+1:] if safe(r, C["status"]) == "PENDING"]
            if remaining:
                log.info(f"  Waiting {AGENT_GAP_SECS}s before next contact...\n")
                time.sleep(AGENT_GAP_SECS)

    log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log.info(f"Done. Sent: {sent} | Failed: {failed} | Skipped: {skipped}")
    log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")


if __name__ == "__main__":
    main()
