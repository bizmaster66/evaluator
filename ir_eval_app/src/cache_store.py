from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from .config import CACHE_FILE_NAME


@dataclass
class CacheEntry:
    key: str
    report_file_id: str
    report_file_url: str
    timestamp: str
    summary: str


class CacheStore:
    def __init__(self, drive_client, folder_id: str):
        self.drive = drive_client
        self.folder_id = folder_id
        self.cache_file_id = None
        self.data: Dict[str, Any] = {"version": 1, "meta": {}, "items": {}}

    def load(self) -> None:
        existing = self.drive.find_file_in_folder(self.folder_id, CACHE_FILE_NAME, mime_type="application/json")
        if not existing:
            self.data = {"version": 1, "meta": {}, "items": {}}
            return
        self.cache_file_id = existing["id"]
        payload = self.drive.download_json(existing["id"])
        self.data = {
            "version": payload.get("version", 1),
            "meta": payload.get("meta", {}),
            "items": payload.get("items", {}),
        }

    def get(self, key: str) -> Dict[str, Any] | None:
        return self.data.get("items", {}).get(key)

    def set(self, key: str, value: Dict[str, Any]) -> None:
        self.data.setdefault("items", {})[key] = value

    def set_meta(self, key: str, value: Any) -> None:
        self.data.setdefault("meta", {})[key] = value

    def get_meta(self, key: str, default: Any = None) -> Any:
        return self.data.get("meta", {}).get(key, default)

    def save(self) -> None:
        content = json_dumps(self.data)
        self.drive.upload_text(self.folder_id, CACHE_FILE_NAME, content, "application/json")


def json_dumps(data: Dict[str, Any]) -> str:
    import json

    return json.dumps(data, ensure_ascii=True, indent=2)
