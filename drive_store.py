from __future__ import annotations
from typing import Any, Optional
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload, MediaIoBaseDownload
import io

class DriveStore:
    def __init__(self, service_account_json: str):
        creds = service_account.Credentials.from_service_account_file(
            service_account_json,
            scopes=[
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/spreadsheets"
            ],
        )
        self.drive = build("drive", "v3", credentials=creds, cache_discovery=False)

    def list_files(self, folder_id: str, mime_type: str | None = None, limit: int = 100):
        q = f"'{folder_id}' in parents and trashed=false"
        if mime_type:
            q += f" and mimeType='{mime_type}'"
        resp = self.drive.files().list(
            q=q,
            fields="files(id,name,mimeType,modifiedTime,createdTime)",
            pageSize=min(limit, 1000),
            orderBy="createdTime asc"
        ).execute()
        return resp.get("files", [])

    def find_by_name(self, folder_id: str, name: str):
        q = f"'{folder_id}' in parents and trashed=false and name='{name}'"
        resp = self.drive.files().list(
            q=q,
            fields="files(id,name,mimeType)"
        ).execute()
        files = resp.get("files", [])
        return files[0] if files else None

    def download_bytes(self, file_id: str) -> bytes:
        request = self.drive.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return fh.getvalue()

    def upload_json(self, folder_id: str, filename: str, json_text: str) -> str:
        media = MediaInMemoryUpload(json_text.encode("utf-8"), mimetype="application/json")
        file_metadata = {"name": filename, "parents": [folder_id], "mimeType": "application/json"}
        created = self.drive.files().create(
            body=file_metadata,
            media_body=media,
            fields="id"
        ).execute()
        return created["id"]

    def update_file_json(self, file_id: str, json_text: str):
        media = MediaInMemoryUpload(json_text.encode("utf-8"), mimetype="application/json")
        self.drive.files().update(fileId=file_id, media_body=media).execute()

    def move_file(self, file_id: str, new_folder_id: str) -> None:
        file = self.drive.files().get(fileId=file_id, fields="parents").execute()
        previous_parents = ",".join(file.get("parents", []))
        self.drive.files().update(
            fileId=file_id,
            addParents=new_folder_id,
            removeParents=previous_parents,
            fields="id,parents"
        ).execute()
