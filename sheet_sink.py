from __future__ import annotations
from typing import Dict, Any, List
from google.oauth2 import service_account
from googleapiclient.discovery import build

APPLICANTS_COLUMNS = [
  "applicant_id","name","phone","email",
  "outcome","stage_reached","dropoff_stage",
  "primary_reason_code","secondary_reason_codes","reason_text",
  "skills_summary","skills","experience_level","role_interest",
  "availability","location",
  "sentiment","message_count","last_message_ts",
  "confidence","needs_human_review","evidence_quote_1","evidence_quote_2",
  "analysis_ts"
]

class SheetSink:
    def __init__(self, service_account_json: str):
        creds = service_account.Credentials.from_service_account_file(
            service_account_json,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        self.sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)

    def ensure_header(self, spreadsheet_id: str, sheet_name: str):
        rng = f"{sheet_name}!A1:Z1"
        resp = self.sheets.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=rng
        ).execute()
        vals = resp.get("values", [])
        if not vals or vals[0] != APPLICANTS_COLUMNS:
            self.sheets.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption="RAW",
                body={"values":[APPLICANTS_COLUMNS]}
            ).execute()

    def append_row(self, spreadsheet_id: str, sheet_name: str, row_values: List[Any]) -> int:
        # append returns updatedRange like 'Aplicantes!A137:Z137'
        resp = self.sheets.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A:Z",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values":[row_values]}
        ).execute()
        updated_range = resp.get("updates", {}).get("updatedRange", "")
        # parse row number
        # e.g. Aplicantes!A137:Z137
        import re
        m = re.search(r"!A(\d+):", updated_range)
        return int(m.group(1)) if m else -1

    def update_row(self, spreadsheet_id: str, sheet_name: str, row_num: int, row_values: List[Any]):
        rng = f"{sheet_name}!A{row_num}:Z{row_num}"
        self.sheets.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueInputOption="RAW",
            body={"values":[row_values]}
        ).execute()
