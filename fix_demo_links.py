"""
One-time fix: replace rajpatil11.github.io/demo-dental with demo.exelvoai.com
in Google Sheet (col Q) and GHL custom field for all SENT contacts.
"""
from __future__ import annotations
import os, json, time, logging
import requests
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

GHL_BASE        = "https://services.leadconnectorhq.com"
GHL_VERSION     = "2021-07-28"
GHL_LOCATION_ID = "Y6vbtAtrSByzFIROGlK5"
GHL_API_KEY     = os.environ["GHL_API_KEY"]
SHEET_ID        = os.environ.get("SHEET_ID", "1v63RXp3-OF-RVCxWvhKk8qXfxzXPd_3ZaZ7algdIUho")
GOOGLE_SA_JSON  = os.environ["GOOGLE_SERVICE_ACCOUNT"]
OLD_BASE        = "https://rajpatil11.github.io/demo-dental"
NEW_BASE        = "https://demo.exelvoai.com"

C = {
    "status":       14,  # O
    "vapi_link":    16,  # Q
    "email":         6,  # G
    "company":       5,  # F
}

def ghl_headers():
    return {"Authorization": f"Bearer {GHL_API_KEY}", "Version": GHL_VERSION, "Content-Type": "application/json"}

def get_sheet_service():
    sa_info = json.loads(GOOGLE_SA_JSON)
    creds = Credentials.from_service_account_info(sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds).spreadsheets()

def get_all_rows(svc):
    result = svc.values().get(spreadsheetId=SHEET_ID, range="MASTER_450!A2:Z").execute()
    return result.get("values", [])

def update_sheet_cell(svc, row_num, col, value):
    col_letter = chr(ord('A') + col)
    range_str = f"MASTER_450!{col_letter}{row_num}"
    svc.values().update(
        spreadsheetId=SHEET_ID,
        range=range_str,
        valueInputOption="RAW",
        body={"values": [[value]]}
    ).execute()

def safe(row, idx):
    try: return row[idx].strip()
    except: return ""

def ghl_find_contact(email: str):
    r = requests.get(
        f"{GHL_BASE}/contacts/search/duplicate",
        params={"locationId": GHL_LOCATION_ID, "email": email},
        headers=ghl_headers(), timeout=15
    )
    if r.ok:
        c = r.json().get("contact")
        if c: return c.get("id")
    r2 = requests.get(
        f"{GHL_BASE}/contacts/",
        params={"locationId": GHL_LOCATION_ID, "email": email},
        headers=ghl_headers(), timeout=15
    )
    if r2.ok:
        contacts = r2.json().get("contacts", [])
        if contacts: return contacts[0].get("id")
    return None

def ghl_update_vapi_link(ghl_id: str, new_link: str):
    payload = {"customFields": [{"key": "vapi_link", "field_value": new_link}]}
    r = requests.put(f"{GHL_BASE}/contacts/{ghl_id}", json=payload, headers=ghl_headers(), timeout=15)
    return r.ok

def main():
    svc = get_sheet_service()
    rows = get_all_rows(svc)

    fixed = 0
    skipped = 0

    for i, row in enumerate(rows):
        sheet_row = i + 2  # 1-indexed + header
        status = safe(row, C["status"])
        vapi_link = safe(row, C["vapi_link"])
        email = safe(row, C["email"])
        company = safe(row, C["company"])

        if status != "SENT":
            continue

        if OLD_BASE not in vapi_link:
            skipped += 1
            continue

        new_link = vapi_link.replace(OLD_BASE, NEW_BASE)
        log.info(f"Row {sheet_row} | {company} | {email}")
        log.info(f"  OLD: {vapi_link}")
        log.info(f"  NEW: {new_link}")

        # Update Google Sheet
        update_sheet_cell(svc, sheet_row, C["vapi_link"], new_link)
        log.info(f"  ✅ Sheet updated")

        # Update GHL
        if email:
            ghl_id = ghl_find_contact(email)
            if ghl_id:
                ok = ghl_update_vapi_link(ghl_id, new_link)
                log.info(f"  ✅ GHL updated: {ok}")
            else:
                log.warning(f"  ⚠️  GHL contact not found")

        fixed += 1
        time.sleep(0.5)

    log.info(f"\n=== Done: {fixed} fixed, {skipped} already correct ===")

if __name__ == "__main__":
    main()
