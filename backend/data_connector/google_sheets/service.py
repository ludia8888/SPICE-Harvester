"""
Google Sheets Connector - Service Layer (connector library).

Foundry policy:
- This module is *only* responsible for I/O (read/preview) and normalization helpers.
- Change detection (polling/webhooks), durable registries, and auto-import live in:
  - `connector_trigger_service`
  - `connector_sync_worker`
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

import httpx

from .models import GoogleSheetPreviewResponse, SheetMetadata
from .utils import (
    build_sheets_api_url,
    build_sheets_metadata_url,
    extract_gid,
    extract_sheet_id,
    normalize_sheet_data,
    sanitize_worksheet_name,
)

logger = logging.getLogger(__name__)


class GoogleSheetsService:
    """Google Sheets API client (read-only)."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = (api_key or os.getenv("GOOGLE_API_KEY") or os.getenv("GOOGLE_SHEETS_API_KEY") or "").strip()
        if not self.api_key:
            logger.warning("Google API key not configured. Only public sheets will be accessible.")
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=30.0,
                headers={"Accept": "application/json", "User-Agent": "SPICE-HARVESTER/1.0"},
            )
        return self._client

    async def fetch_sheet_values(
        self,
        sheet_url: str,
        *,
        worksheet_name: Optional[str] = None,
        api_key: Optional[str] = None,
        access_token: Optional[str] = None,
    ) -> tuple[str, SheetMetadata, str, Optional[int], List[List[Any]]]:
        """
        Fetch raw values + metadata for a Google Sheet URL.

        Returns:
            (sheet_id, metadata, worksheet_title, worksheet_sheet_id, values)
        """
        sheet_id = extract_sheet_id(sheet_url)
        gid = extract_gid(sheet_url)

        metadata = await self._get_sheet_metadata(sheet_id, api_key=api_key, access_token=access_token)

        worksheet_title = worksheet_name or "Sheet1"
        worksheet_sheet_id: Optional[int] = None

        if worksheet_name:
            for sheet in metadata.sheets or []:
                if str(sheet.get("properties", {}).get("title", "")) == worksheet_name:
                    worksheet_title = worksheet_name
                    try:
                        worksheet_sheet_id = int(sheet.get("properties", {}).get("sheetId"))
                    except (TypeError, ValueError):
                        worksheet_sheet_id = None
                    break
        elif gid and metadata.sheets:
            for sheet in metadata.sheets:
                if str(sheet.get("properties", {}).get("sheetId", "")) == gid:
                    worksheet_title = sheet["properties"]["title"]
                    try:
                        worksheet_sheet_id = int(sheet.get("properties", {}).get("sheetId"))
                    except (TypeError, ValueError):
                        worksheet_sheet_id = None
                    break
        elif metadata.sheets:
            worksheet_title = metadata.sheets[0]["properties"]["title"]
            try:
                worksheet_sheet_id = int(metadata.sheets[0].get("properties", {}).get("sheetId"))
            except (TypeError, ValueError):
                worksheet_sheet_id = None

        # Worksheets with special chars require quoting
        if any(ch in worksheet_title for ch in (" ", "/", "(", ")")):
            worksheet_range = f"'{worksheet_title}'"
        else:
            worksheet_range = worksheet_title

        values = await self._get_sheet_data(
            sheet_id,
            worksheet_range,
            api_key=api_key,
            access_token=access_token,
        )
        return sheet_id, metadata, worksheet_title, worksheet_sheet_id, values

    async def preview_sheet(
        self,
        sheet_url: str,
        *,
        worksheet_name: Optional[str] = None,
        limit: int = 10,
        api_key: Optional[str] = None,
        access_token: Optional[str] = None,
    ) -> GoogleSheetPreviewResponse:
        sheet_id, metadata, worksheet_title, _, data = await self.fetch_sheet_values(
            sheet_url,
            worksheet_name=worksheet_name,
            api_key=api_key,
            access_token=access_token,
        )

        columns, rows = normalize_sheet_data(data)
        sample_rows = rows[: max(1, int(limit))] if rows else []

        return GoogleSheetPreviewResponse(
            sheet_id=sheet_id,
            sheet_url=sheet_url,
            sheet_title=metadata.title,
            worksheet_title=sanitize_worksheet_name(worksheet_title),
            worksheet_name=worksheet_title,
            metadata=metadata,
            columns=columns,
            sample_rows=sample_rows,
            preview_data=data[: max(1, int(limit))] if data else [],
            total_rows=len(rows),
            total_columns=len(columns),
        )

    async def get_sheet_metadata(
        self,
        sheet_id: str,
        *,
        api_key: Optional[str] = None,
        access_token: Optional[str] = None,
    ) -> SheetMetadata:
        return await self._get_sheet_metadata(sheet_id, api_key=api_key, access_token=access_token)

    async def list_spreadsheets(
        self,
        *,
        access_token: str,
        query: Optional[str] = None,
        page_size: int = 50,
    ) -> List[Dict[str, Any]]:
        if not access_token:
            raise ValueError("access_token is required")
        client = await self._get_client()
        base_url = "https://www.googleapis.com/drive/v3/files"
        size = max(1, min(int(page_size or 50), 200))
        q = "mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
        if query:
            safe = str(query).replace("'", "\\'")
            q = f"{q} and name contains '{safe}'"
        params = {
            "q": q,
            "pageSize": size,
            "fields": "files(id,name,modifiedTime)",
            "orderBy": "modifiedTime desc",
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
        }
        response = await client.get(
            base_url,
            params=params,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        response.raise_for_status()
        data = response.json() or {}
        files = data.get("files") or []
        results: List[Dict[str, Any]] = []
        for item in files:
            if not isinstance(item, dict):
                continue
            results.append(
                {
                    "spreadsheet_id": item.get("id"),
                    "name": item.get("name"),
                    "modified_time": item.get("modifiedTime"),
                }
            )
        return results

    async def _get_sheet_metadata(
        self,
        sheet_id: str,
        *,
        api_key: Optional[str] = None,
        access_token: Optional[str] = None,
    ) -> SheetMetadata:
        client = await self._get_client()
        url = build_sheets_metadata_url(sheet_id)

        params = {}
        key = (api_key or self.api_key or "").strip()
        headers = {}
        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"
        elif key:
            params["key"] = key

        try:
            response = await client.get(url, params=params, headers=headers or None)
            response.raise_for_status()

            data = response.json()
            return SheetMetadata(
                sheet_id=sheet_id,
                title=data.get("properties", {}).get("title", "Untitled"),
                locale=data.get("properties", {}).get("locale", "ko_KR"),
                time_zone=data.get("properties", {}).get("timeZone", "Asia/Seoul"),
                sheets=data.get("sheets", []),
            )

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                raise ValueError(
                    "Cannot access the Google Sheet. "
                    "Please ensure it's shared with 'Anyone with the link can view' permission."
                ) from e
            raise

    async def _get_sheet_data(
        self,
        sheet_id: str,
        range_name: str = "Sheet1",
        *,
        api_key: Optional[str] = None,
        access_token: Optional[str] = None,
    ) -> List[List[Any]]:
        client = await self._get_client()
        url = build_sheets_api_url(sheet_id, range_name)

        params = {
            "majorDimension": "ROWS",
            "valueRenderOption": "FORMATTED_VALUE",
            "dateTimeRenderOption": "FORMATTED_STRING",
        }
        key = (api_key or self.api_key or "").strip()
        headers = {}
        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"
        elif key:
            params["key"] = key

        try:
            response = await client.get(url, params=params, headers=headers or None)
            response.raise_for_status()

            data = response.json()
            return data.get("values", [])

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                raise ValueError("Cannot access the Google Sheet data. Please ensure it's shared publicly.") from e
            if e.response.status_code == 400:
                # Invalid range: retry with Sheet1 for resilience
                logger.warning(f"Invalid range '{range_name}', trying 'Sheet1'")
                if range_name != "Sheet1":
                    return await self._get_sheet_data(sheet_id, "Sheet1", api_key=api_key)
            raise

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
