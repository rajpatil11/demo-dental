"""
One-time sync: cross-reference the sheet against GHL contacts.
- Contacts in sheet but NOT in GHL → deleted from sheet (rows removed)
- Only touches PENDING rows — leaves SENT/FAILED/IN_PROGRESS untouched
- Run once, then the pipeline will only hit contacts that exist in GHL

Usage:
    python3 sync_ghl.py --csv /path/to/Export_Contacts.csv [--dry-run]
"""
from __future__ import annotations

import csv
import json
import os
import sys
import argparse
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

SHEET_ID       = os.environ["SHEET_ID"]
GOOGLE_SA_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT"]

C_EMAIL  = 6    # G
C_STATUS = 14   # O
C_COMPANY = 5   # F


def safe(row: list, idx: int) -> str:
    try:
        return str(row[idx]).strip()
    except IndexError:
        return ""


parser = argparse.ArgumentParser()
parser.add_argument("--csv", required=True, help="Path to GHL contacts CSV export")
parser.add_argument("--dry-run", action="store_true", help="Preview only, no changes")
args = parser.parse_args()

# ── Load GHL emails from CSV ──────────────────────────────────────────────────
ghl_emails: set[str] = set()
with open(args.csv, newline="", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for row in reader:
        e = row.get("Email", "").strip().lower()
        if e:
            ghl_emails.add(e)

print(f"GHL contacts loaded: {len(ghl_emails)}")

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

print(f"Sheet rows loaded: {len(rows)}\n")

# ── Find PENDING rows not in GHL ──────────────────────────────────────────────
# Collect in reverse order so row deletion doesn't shift indices
rows_to_delete: list[int] = []   # sheet row numbers (1-indexed)

for i, row in enumerate(rows):
    sheet_row = i + 2   # row 2 = first data row
    status = safe(row, C_STATUS)
    email  = safe(row, C_EMAIL).lower()

    if status != "PENDING":
        continue

    if email not in ghl_emails:
        company = safe(row, C_COMPANY)
        print(f"  [Row {sheet_row}] NOT IN GHL: {company} ({email})")
        rows_to_delete.append(sheet_row)

print(f"\nTotal PENDING rows not in GHL: {len(rows_to_delete)}")

if args.dry_run:
    print("\nDry run — no changes made.")
    sys.exit(0)

if not rows_to_delete:
    print("Nothing to delete.")
    sys.exit(0)

confirm = input(f"\nDelete {len(rows_to_delete)} rows from the sheet? (yes/no): ").strip().lower()
if confirm != "yes":
    print("Aborted.")
    sys.exit(0)

# ── Get spreadsheet metadata for sheet ID ────────────────────────────────────
meta = svc.get(spreadsheetId=SHEET_ID).execute()
sheet_id = None
for s in meta.get("sheets", []):
    if s["properties"]["title"] == "MASTER_450":
        sheet_id = s["properties"]["sheetId"]
        break

if sheet_id is None:
    print("Could not find sheet tab 'MASTER_450'. Aborted.")
    sys.exit(1)

# ── Delete rows in reverse order ──────────────────────────────────────────────
requests_body = []
for sheet_row in sorted(rows_to_delete, reverse=True):
    row_idx = sheet_row - 1   # 0-indexed
    requests_body.append({
        "deleteDimension": {
            "range": {
                "sheetId": sheet_id,
                "dimension": "ROWS",
                "startIndex": row_idx,
                "endIndex": row_idx + 1,
            }
        }
    })

svc.batchUpdate(
    spreadsheetId=SHEET_ID,
    body={"requests": requests_body},
).execute()

print(f"\nDeleted {len(rows_to_delete)} rows. Sheet now contains only contacts that exist in GHL.")
