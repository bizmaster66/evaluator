from __future__ import annotations

from typing import Any, Dict, List, Optional

from googleapiclient.discovery import build


class SheetsClient:
    def __init__(self, credentials):
        self.service = build("sheets", "v4", credentials=credentials, cache_discovery=False)
        self.drive = build("drive", "v3", credentials=credentials, cache_discovery=False)

    def create_spreadsheet(self, title: str, parent_folder_id: str) -> str:
        body = {
            "name": title,
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [parent_folder_id],
        }
        created = self.drive.files().create(body=body, fields="id").execute()
        return created["id"]

    def append_rows(self, spreadsheet_id: str, rows: List[List[Any]], sheet_name: str = "Sheet1") -> None:
        body = {"values": rows}
        self.service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body=body,
        ).execute()

    def ensure_header(self, spreadsheet_id: str, header: List[str], sheet_name: str = "Sheet1") -> None:
        existing = (
            self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=f"{sheet_name}!A1:Z1"
            ).execute()
        )
        if existing.get("values"):
            return
        self.append_rows(spreadsheet_id, [header], sheet_name=sheet_name)


def make_row(data: Dict[str, Any], columns: List[str]) -> List[Any]:
    return [data.get(col, "") for col in columns]
