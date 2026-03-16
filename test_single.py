"""
test_single.py — Runs the full pipeline on ONE hardcoded contact.
Edit line marked ← YOUR EMAIL before running.

Usage:
  export GHL_API_KEY="your-ghl-key"
  export VAPI_API_KEY="your-vapi-key"
  export CLAUDE_API_KEY="your-claude-key"
  export GOOGLE_SERVICE_ACCOUNT="$(cat service-account.json)"
  python test_single.py
"""

import os
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()  # loads .env file automatically
from pipeline import (
    scrape,
    make_research_brief,
    make_vapi_system_prompt,
    make_email,
    create_vapi_agent,
    ghl_find_contact,
    ghl_update_contact,
    ghl_add_tag,
    CALENDAR_LINK,
    WEBSITE,
    SENDER_NAME,
    SENDER_COMPANY,
)

# ── Test contact — edit your email below ──────────────────────────────────────
TEST = {
    "first_name": "Dario",
    "last_name":  "Jovanovski",
    "title":      "Founder",
    "company":    "EXELVO AI Test",
    "email":      "support@parakeeet.com",    # ← YOUR EMAIL (line 32)
    "website":    "https://www.exelvoai.com",
    "phone":      "",
    "city":       "Mumbai",
    "state":      "Maharashtra",
}
# ─────────────────────────────────────────────────────────────────────────────


def divider(title=""):
    width = 60
    if title:
        pad = (width - len(title) - 2) // 2
        print(f"\n{'━' * pad} {title} {'━' * pad}")
    else:
        print("━" * width)


def check_env():
    missing = [k for k in ["GHL_API_KEY", "VAPI_API_KEY", "CLAUDE_API_KEY", "GOOGLE_SERVICE_ACCOUNT"]
               if not os.environ.get(k)]
    if missing:
        print(f"ERROR: Missing environment variables: {', '.join(missing)}")
        print("Set them before running. See README.md for instructions.")
        raise SystemExit(1)

    if TEST["email"] == "PUT_YOUR_EMAIL_HERE":
        print("ERROR: Open test_single.py and replace PUT_YOUR_EMAIL_HERE on line 32.")
        raise SystemExit(1)


def run_test():
    check_env()
    contact = TEST

    divider()
    print(f"  EXELVO AI — Test Run")
    print(f"  Contact : {contact['first_name']} {contact['last_name']}")
    print(f"  Company : {contact['company']}")
    print(f"  Email   : {contact['email']}")
    print(f"  Website : {contact['website']}")
    divider()

    # ── Step 1: Scrape ────────────────────────────────────────────────────────
    divider("STEP 1 — Scrape Website")
    site_text = scrape(contact["website"])
    if site_text:
        print(f"✓ Scraped {len(site_text)} characters")
        print(f"  Preview: {site_text[:200]}...")
    else:
        print("⚠ No content scraped (will continue with empty text)")

    # ── Step 2: Research brief ────────────────────────────────────────────────
    divider("STEP 2 — Research Brief")
    brief = make_research_brief(contact, site_text)
    print(f"✓ Brief generated:\n")
    print(brief)

    # ── Step 3: Vapi system prompt ────────────────────────────────────────────
    divider("STEP 3 — Vapi System Prompt")
    vapi_prompt = make_vapi_system_prompt(contact, brief)
    print(f"✓ System prompt generated ({len(vapi_prompt)} chars):\n")
    print(vapi_prompt)

    # ── Step 4 + 5: Create Vapi agent ─────────────────────────────────────────
    divider("STEP 4+5 — Create Vapi Agent")
    print("Creating agent (this calls Vapi API live)...")
    agent_id, vapi_link = create_vapi_agent(contact, vapi_prompt)
    print(f"✓ Agent created!")
    print(f"  Agent ID  : {agent_id}")
    print(f"  Demo link : {vapi_link}")
    print(f"\n  ➜ Open the demo link in your browser to test Aria now.")

    # ── Step 6: Email ─────────────────────────────────────────────────────────
    divider("STEP 6 — Cold Email")
    subject, body = make_email(contact, vapi_link, brief)
    print(f"✓ Email generated:")
    print(f"\n  Subject: {subject}")
    print(f"\n  Body:\n")
    print(body)
    word_count = len(body.split())
    print(f"\n  Word count: {word_count} {'✓' if word_count <= 100 else '⚠ over 100 words'}")

    # ── Step 7: GHL update ────────────────────────────────────────────────────
    divider("STEP 7 — GHL CRM Update")
    ghl_id = ghl_find_contact(contact["email"])
    if ghl_id:
        print(f"✓ Found GHL contact: {ghl_id}")
        ghl_update_contact(ghl_id, {
            "vapi_link":      vapi_link,
            "vapi_agent_id":  agent_id,
            "research_brief": brief,
            "email_subject":  subject,
            "email_body":     body,
            "day_added":      datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        })
        ghl_add_tag(ghl_id, "Day1-Campaign-Live")
        print("✓ Custom fields updated + tag added")
    else:
        print("⚠ GHL contact not found for this email — CRM update skipped")
        print("  (This is fine for testing if you haven't added this contact to GHL)")

    # ── Summary ───────────────────────────────────────────────────────────────
    divider("TEST COMPLETE")
    print(f"  Agent ID  : {agent_id}")
    print(f"  Demo link : {vapi_link}")
    print(f"\n  ➜ Try the demo: {vapi_link}")
    print(f"  ➜ If everything looked good, you're ready to go live.")
    divider()


if __name__ == "__main__":
    run_test()
