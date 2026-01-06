from __future__ import annotations

import io
from typing import Any, Dict, List, Optional

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload


class DriveClient:
    def __init__(self, credentials):
        self.service = build("drive", "v3", credentials=credentials, cache_discovery=False)

    def list_md_files(self, folder_id: str) -> List[Dict[str, Any]]:
        folder_meta = (
            self.service.files()
            .get(fileId=folder_id, fields="id, driveId", supportsAllDrives=True)
            .execute()
        )
        drive_id = folder_meta.get("driveId")
        query = (
            f"'{folder_id}' in parents and trashed = false and mimeType != 'application/vnd.google-apps.folder'"
        )
        fields = "files(id,name,modifiedTime,mimeType)"
        results: List[Dict[str, Any]] = []
        page_token: Optional[str] = None
        while True:
            resp = (
                self.service.files()
                .list(
                    q=query,
                    fields=f"nextPageToken,{fields}",
                    pageToken=page_token,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    corpora="drive" if drive_id else "allDrives",
                    driveId=drive_id,
                )
                .execute()
            )
            for f in resp.get("files", []):
                if f.get("name", "").lower().endswith(".md"):
                    results.append(f)
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return results

    def get_file_text(self, file_id: str) -> str:
        request = self.service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return fh.getvalue().decode("utf-8", errors="replace")

    def get_or_create_folder(self, name: str, parent_id: Optional[str] = None) -> str:
        query = ["trashed = false", "mimeType = 'application/vnd.google-apps.folder'", f"name = '{name}'"]
        if parent_id:
            query.append(f"'{parent_id}' in parents")
        resp = (
            self.service.files()
            .list(q=" and ".join(query), fields="files(id,name)")
            .execute()
        )
        files = resp.get("files", [])
        if files:
            return files[0]["id"]
        metadata = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
        if parent_id:
            metadata["parents"] = [parent_id]
        created = self.service.files().create(body=metadata, fields="id").execute()
        return created["id"]

    def find_file_in_folder(self, folder_id: str, name: str, mime_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        query = ["trashed = false", f"'{folder_id}' in parents", f"name = '{name}'"]
        if mime_type:
            query.append(f"mimeType = '{mime_type}'")
        resp = (
            self.service.files()
            .list(q=" and ".join(query), fields="files(id,name,mimeType)")
            .execute()
        )
        files = resp.get("files", [])
        return files[0] if files else None

    def download_json(self, file_id: str) -> Dict[str, Any]:
        text = self.get_file_text(file_id)
        if not text.strip():
            return {}
        return json_loads_safe(text)

    def upload_text(self, folder_id: str, name: str, content: str, mime_type: str) -> str:
        existing = self.find_file_in_folder(folder_id, name, mime_type=mime_type)
        media = MediaIoBaseUpload(io.BytesIO(content.encode("utf-8")), mimetype=mime_type, resumable=False)
        if existing:
            updated = (
                self.service.files()
                .update(fileId=existing["id"], media_body=media, fields="id")
                .execute()
            )
            return updated["id"]
        body = {"name": name, "parents": [folder_id], "mimeType": mime_type}
        created = self.service.files().create(body=body, media_body=media, fields="id").execute()
        return created["id"]

    def upload_markdown(self, folder_id: str, name: str, content: str) -> str:
        return self.upload_text(folder_id, name, content, "text/markdown")

    def get_file_link(self, file_id: str) -> str:
        resp = self.service.files().get(fileId=file_id, fields="webViewLink").execute()
        return resp.get("webViewLink", "")

    def move_file_to_folder(self, file_id: str, folder_id: str) -> None:
        file_info = self.service.files().get(fileId=file_id, fields="parents").execute()
        previous_parents = ",".join(file_info.get("parents", []))
        self.service.files().update(
            fileId=file_id,
            addParents=folder_id,
            removeParents=previous_parents,
            fields="id, parents",
        ).execute()


def json_loads_safe(text: str) -> Dict[str, Any]:
    import json

    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return data
    except json.JSONDecodeError:
        return {}
    return {}
