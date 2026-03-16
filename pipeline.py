"""
EXELVO AI — Automated Outbound Email Pipeline
Runs Mon–Fri at 5pm IST (11:30 UTC) via GitHub Actions.
Processes up to 15 PENDING healthcare contacts per run.
Industry-agnostic: adapts to dental, hospital, ortho, therapy, urgent care, etc.
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
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

# ── Hardcoded constants ───────────────────────────────────────────────────────
GHL_LOCATION_ID  = "Y6vbtAtrSByzFIROGlK5"
SHEET_ID_DEFAULT = "1v63RXp3-OF-RVCxWvhKk8qXfxzXPd_3ZaZ7algdIUho"
CALENDAR_LINK    = "https://cal.com/parakeeet.ai/strategy-call-exelvo-ai"
WEBSITE          = "https://www.exelvoai.com"
SENDER_NAME      = "Dario Jovanovski"
SENDER_EMAIL     = "support@parakeeet.com"
SENDER_COMPANY   = "EXELVO AI"
DAILY_LIMIT      = int(os.environ.get("DAILY_LIMIT_OVERRIDE", 15))
AGENT_GAP_SECS   = 60

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
#        Reply Received(T), Booked(U), Notes(V), Email Subject(W), Email Body(X)
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
    "email_subject":   22,  # W
    "email_body":      23,  # X
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
def scrape(url: str, max_pages: int = 10, max_chars: int = 12000) -> str:
    """Crawl the website: homepage + all internal links found on it, up to max_pages."""
    if not url.startswith("http"):
        url = "https://" + url
    base = url.rstrip("/")
    from urllib.parse import urljoin, urlparse

    def same_domain(href: str) -> bool:
        try:
            return urlparse(urljoin(base, href)).netloc == urlparse(base).netloc
        except Exception:
            return False

    headers = {"User-Agent": "Mozilla/5.0 (compatible; ExelvoBot/1.0)"}
    queue, seen, chunks = [base], set(), []

    while queue and len(seen) < max_pages:
        page = queue.pop(0)
        if page in seen:
            continue
        try:
            r = requests.get(page, headers=headers, timeout=12, allow_redirects=True)
            if not r.ok or "text/html" not in r.headers.get("content-type", ""):
                continue
            final_url = r.url
            if final_url in seen:
                continue
            seen.add(final_url)
            soup = BeautifulSoup(r.text, "html.parser")
            # Queue new internal links from this page
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
                    continue
                full = urljoin(base, href).split("#")[0].rstrip("/")
                if full not in seen and full not in queue and same_domain(full):
                    queue.append(full)
            # Extract text
            for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
                tag.decompose()
            text = " ".join(soup.get_text(separator=" ").split())
            if text:
                chunks.append(f"[{final_url}]\n{text[:1500]}")
        except Exception:
            pass

    combined = "\n\n".join(chunks)
    return combined[:max_chars] if combined else ""


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
            "max_tokens": 2048,
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
        "You are a senior B2B sales researcher preparing a detailed intelligence brief on a healthcare organization. "
        "A cold outreach team will use this brief to build a personalized AI demo and write a targeted email. "
        "The brief must be thorough enough that someone who has never visited the site understands exactly "
        "who this organization is, what they do, how they operate, and where they are vulnerable to losing patients.\n\n"
        "First line must always be:\n"
        "PRACTICE TYPE: [exact type — Dental Clinic, Hospital, Orthopedic Clinic, Physical Therapy, "
        "Mental Health Practice, Urgent Care, Pediatric Clinic, Medical Spa, Chiropractic, etc.]\n\n"
        "Then write a full brief in the following sections. Each section should be 2–4 sentences of real detail, "
        "not bullet fragments. If information is not on the site, say so briefly and move on — never fabricate.\n\n"
        "OVERVIEW: Who is this organization? What do they do, who do they serve, and what is their positioning? "
        "Include their city, any tagline or mission statement, and how long they've been operating if mentioned.\n\n"
        "PROVIDERS & TEAM: Who are the doctors, specialists, or key staff? Include names, credentials, and any "
        "notable background (fellowships, specializations, years of experience). If a solo practice, say so.\n\n"
        "SERVICES: List every specific service, procedure, or treatment offered. Go deep — name individual "
        "procedures, not just categories. Include any unique or specialty offerings that competitors may not have.\n\n"
        "OPERATIONS: How do patients/clients book — online system, phone only, contact form, patient portal, "
        "walk-in? What are their hours? Do they offer evenings, weekends, emergency slots, or telehealth? "
        "Any insurance plans or financing options mentioned?\n\n"
        "TECHNOLOGY & DIFFERENTIATORS: Any equipment, software, certifications, awards, or specific claims "
        "mentioned on the site. What makes them stand out? Quote their own language where relevant.\n\n"
        "PAIN POINT & OUTREACH ANGLE: Based on everything above, what is the single most likely reason a new "
        "patient calling this practice might not get through or book? Be specific — phone-only with no after-hours "
        "option, solo provider who can't answer while treating, no online booking on a busy multi-doctor practice, etc. "
        "Then state in one sentence exactly how an AI receptionist would solve that specific problem for them.\n\n"
        "Be factual. Only report what is actually on the site. Write in clear, confident prose."
    )
    prompt = (
        f"Organization: {contact['company']}\n"
        f"Location: {contact['city']}, {contact['state']}\n"
        f"Website content:\n{site_text or 'No website content available.'}"
    )
    return ask_claude(system, prompt, model=CLAUDE_MODEL)


def make_vapi_system_prompt(contact: dict, brief: str) -> str:
    system = (
        "You are writing a voice AI system prompt for Rachel, the AI receptionist at a healthcare organization. "
        "The PRACTICE TYPE is specified in the research brief — adapt everything to that type. "
        "If it is a dental clinic, use dental-specific flows. If it is a hospital, adapt for hospital departments. "
        "If it is orthopedic, physical therapy, mental health, urgent care, or any other specialty — "
        "adapt accordingly. Rachel IS the receptionist — not an AI assistant. She has worked there for years. "
        "She knows every provider, every service, every department by name.\n\n"
        "The system prompt must be 400–500 words and cover ALL of the following:\n\n"
        "1. IDENTITY — Rachel introduces herself: 'Thank you for calling [Organization], this is Rachel, how can I help you?' "
        "She knows the practice type and speaks in that context naturally.\n\n"
        "2. SERVICES / DEPARTMENTS — She knows every specific service, procedure, or department from the research brief. "
        "When callers ask 'do you do X?' or 'do you have a Y department?', she answers from the actual list — never generically.\n\n"
        "3. APPOINTMENT / INTAKE BOOKING — Her primary job. She collects: caller name, phone, reason for visit, "
        "preferred day/time, new vs existing patient/client. Adapts intake questions to the practice type "
        "(e.g. referral needed? insurance pre-auth? emergency or scheduled?).\n\n"
        "4. NEW PATIENT / CLIENT FLOW — Warmly welcomes new contacts. Explains what to expect on first visit. "
        "Asks for insurance information and notes it.\n\n"
        "5. EXISTING PATIENT / CLIENT FLOW — Handles reschedules, cancellations, follow-up questions. "
        "Speaks as if she has their file in front of her.\n\n"
        "6. HOURS & LOCATION — Uses the actual hours from the research brief. "
        "If not found: 'Let me have someone from our team confirm that with you.'\n\n"
        "7. INSURANCE & PAYMENTS — References any specific plans from the research brief. "
        "If not found: says the organization works with most major plans and the team will verify.\n\n"
        "8. TONE — Warm, confident, professional. Never robotic. Short sentences. "
        "Never says 'As an AI' or 'I'm not able to'. If she doesn't know: "
        "'Let me have someone from our team follow up with you on that.'\n\n"
        "9. CALL CLOSE — At the end of every call Rachel naturally says: "
        "'By the way — I'm actually an AI receptionist built by EXELVO AI. "
        "If you're curious how this works or want to explore it for your own organization, "
        f"the team at EXELVO AI would love to chat: {CALENDAR_LINK}'\n\n"
        "Write ONLY the system prompt. No intro, no explanation, no headers. "
        "Write it as if it is the actual instructions Rachel will read before her first day."
    )
    prompt = (
        f"Organization: {contact['company']}\n"
        f"Location: {contact['city']}, {contact['state']}\n"
        f"Research brief (includes PRACTICE TYPE at the top):\n{brief}"
    )
    return ask_claude(system, prompt, model=CLAUDE_MODEL)


def make_email(contact: dict, vapi_link: str, brief: str) -> tuple[str, str]:
    """Returns (subject, body). 5-beat cold email. Rachel. Under 120 words."""
    system = (
        "You are Dario Jovanovski, founder of EXELVO AI. "
        "You personally looked at this healthcare organization's website. "
        "You found one specific real problem. You already built an AI receptionist named Rachel for them.\n\n"
        "The research brief tells you the PRACTICE TYPE (dental clinic, hospital, orthopedic, etc.). "
        "Adapt every beat to that type — never default to generic dental language if it's a different specialty.\n\n"
        "Write like a real founder — not a marketer. Short sentences. Confident. Zero fluff. Under 120 words total.\n\n"
        "EXACT 5-BEAT STRUCTURE:\n\n"
        "Hey [First Name],\n\n"
        "Beat 1 — ONE sentence. Specific to THIS organization only. Must reference something real from the research brief. "
        "Adapt to the practice type:\n"
        "  Dental, no online booking → calls going to voicemail after hours, patients book the next dentist.\n"
        "  Hospital / large clinic → missed calls to specific department = lost referrals or admissions.\n"
        "  Solo/small practice any specialty → every call missed while treating = new patient gone.\n"
        "  Phone-only any type → patients who can't get through on first try book elsewhere.\n"
        "  Use the STANDOUT DETAIL or PAIN POINT from the brief for maximum specificity.\n\n"
        "Beat 2 — 'So I built Rachel — an AI receptionist trained specifically for [Organization Name]. "
        "She answers calls, books appointments, and handles [patient/client] questions 24/7.'\n"
        "(Use 'patient' for medical/dental, 'client' for therapy/wellness/spa, adapt as needed.)\n\n"
        "Beat 3 — 'She already knows your [practice/clinic/hospital]. Try her right now:\n"
        "👉 [VAPI LINK]\nAsk her anything a new [patient/client] would ask.'\n\n"
        "Beat 4 — 'If it looks good for you:\n"
        "📅 Book a call: [CALENDAR LINK]\n"
        "🌐 See what we do: [WEBSITE]'\n\n"
        "Beat 5 — 'Would love to work with you.\n— Dario\nEXELVO AI'\n\n"
        "SUBJECT LINE rules:\n"
        "- Must mention their organization name\n"
        "- Curiosity-driven, matches their industry\n"
        "- Examples: 'I built an AI receptionist for [Org]' / '[Org] is losing calls — I fixed it' / "
        "'Your front desk, but AI — built for [Org]'\n"
        "- NEVER use: 'Quick question' / 'Improving your practice' / 'AI solution' / 'Following up'\n\n"
        "BANNED WORDS: revolutionize, game-changing, cutting-edge, leverage, seamless, streamline, "
        "innovative, excited to, hope this finds you, quick question, touch base, circle back, "
        "reach out, solutions, transform, empower, utilize, AI-powered, I wanted to, I came across, "
        "just following up, value proposition, pain points, ecosystem\n\n"
        "QUALITY CHECK before returning:\n"
        "□ Beat 1 uses something specific from the research brief — not generic?\n"
        "□ Beat 1 adapted to the correct practice type?\n"
        "□ Total words under 120?\n"
        "□ Zero banned words?\n"
        "□ All 3 links present?\n"
        "□ Would Beat 1 make sense unchanged at a completely different organization? If YES → rewrite.\n\n"
        "Output format — exactly this, nothing else:\n"
        "SUBJECT: <subject line>\n"
        "BODY:\n"
        "<full email body starting with Hey [First Name],>"
    )
    prompt = (
        f"Organization: {contact['company']}\n"
        f"First Name: {contact['first_name']}\n"
        f"City: {contact['city']}\n"
        f"Research brief (includes PRACTICE TYPE):\n{brief}\n\n"
        f"Vapi demo link: {vapi_link}\n"
        f"Book a call: {CALENDAR_LINK}\n"
        f"Our website: {WEBSITE}\n\n"
        "Follow the 5-beat structure exactly. Beat 1 must be specific and adapted to the practice type."
    )
    raw = ask_claude(system, prompt)
    subject, body_lines, in_body = "", [], False
    for line in raw.strip().splitlines():
        if line.upper().startswith("SUBJECT:"):
            subject = line.split(":", 1)[1].strip()
        elif line.upper().startswith("BODY:"):
            in_body = True
            rest = line.split(":", 1)[1].strip()
            if rest:
                body_lines.append(rest)
        elif in_body:
            body_lines.append(line)
    body = "\n".join(body_lines).strip()
    if not subject or not body:
        lines = raw.strip().splitlines()
        subject = lines[0].replace("SUBJECT:", "").replace("Subject:", "").strip() if lines else "I built an AI receptionist for you"
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

    demo_base = os.environ.get("DEMO_BASE_URL", "https://demo.exelvoai.com")
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


def ghl_add_note(contact_id: str, body: str):
    r = requests.post(
        f"{GHL_BASE}/contacts/{contact_id}/notes",
        json={"body": body},
        headers=_gh(),
        timeout=15,
    )
    r.raise_for_status()


def ghl_send_email(contact_id: str, to_email: str, subject: str, plain_body: str):
    """Find/create a GHL conversation then send an outbound email."""
    # Find existing conversation
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

    # Plain text → minimal HTML
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
def process_contact(svc, row: list, sheet_row: int, campaign_day: int = 1) -> bool:
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

        log.info("  [6/7] Updating GHL CRM + sending email...")
        ghl_id = ghl_find_contact(contact["email"])
        if not ghl_id:
            raise RuntimeError(f"GHL contact not found for {contact['email']} — cannot send email")
        ghl_update_contact(ghl_id, {
            "email_subject": subject,
            "email_body":    body,
            "vapi_link":     vapi_link,
        })
        ghl_add_note(ghl_id, f"EXELVO AI Day {campaign_day} | Agent: {agent_id}\nDemo: {vapi_link}")
        ghl_send_email(ghl_id, contact["email"], subject, body)
        ghl_add_tag(ghl_id, "Day1-Campaign-Live")
        log.info(f"        GHL {ghl_id} — email sent + tagged Day1-Campaign-Live")

        log.info("  [7/7] Updating Google Sheet...")
        now_iso = datetime.now(timezone.utc).isoformat()
        set_cell(svc, sheet_row, C["campaign_day"], str(campaign_day))
        set_cells(svc, sheet_row, C["vapi_agent_id"],
                  [agent_id, vapi_link, brief, now_iso])
        set_cells(svc, sheet_row, C["email_subject"], [subject, body])
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

    # Auto-calculate campaign day: max existing day + 1 (or 1 if none sent yet)
    existing_days = [
        int(safe(r, C["campaign_day"]))
        for r in rows
        if safe(r, C["status"]) == "SENT" and safe(r, C["campaign_day"]).isdigit()
    ]
    campaign_day = max(existing_days) + 1 if existing_days else 1
    log.info(f"Loaded {len(rows)} rows. Campaign Day: {campaign_day}. Looking for PENDING contacts...\n")

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

        success = process_contact(svc, row, sheet_row, campaign_day)
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
