"""
Google Sheets Connector - Service Layer
"""

import asyncio
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from confluent_kafka import Producer

from .models import (
    GoogleSheetDataUpdate,
    GoogleSheetPreviewResponse,
    GoogleSheetRegisterResponse,
    RegisteredSheet,
    SheetMetadata,
)
from .utils import (
    build_sheets_api_url,
    build_sheets_metadata_url,
    calculate_data_hash,
    extract_gid,
    extract_sheet_id,
    format_datetime_iso,
    normalize_sheet_data,
    sanitize_worksheet_name,
)

logger = logging.getLogger(__name__)


class GoogleSheetsService:
    """Google Sheets API 서비스"""

    def __init__(self, producer: Producer, api_key: Optional[str] = None):
        """
        초기화

        Args:
            producer: Kafka Producer instance
            api_key: Google API Key (환경변수 대체 가능)
        """
        self.producer = producer
        self.api_key = api_key or os.getenv("GOOGLE_API_KEY", "")
        if not self.api_key:
            logger.warning("Google API key not configured. Only public sheets will be accessible.")

        # 등록된 시트 저장소 (실제로는 DB나 Redis 사용)
        self._registered_sheets: Dict[str, RegisteredSheet] = {}

        # HTTP 클라이언트
        self._client = None

    async def _get_client(self) -> httpx.AsyncClient:
        """HTTP 클라이언트 생성/반환"""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=30.0,
                headers={"Accept": "application/json", "User-Agent": "SPICE-HARVESTER/1.0"},
            )
        return self._client

    async def preview_sheet(self, sheet_url: str) -> GoogleSheetPreviewResponse:
        """
        Google Sheet 미리보기

        Args:
            sheet_url: Google Sheets URL

        Returns:
            미리보기 응답

        Raises:
            ValueError: 유효하지 않은 URL
            httpx.HTTPError: API 호출 실패
        """
        # Extract sheet ID
        sheet_id = extract_sheet_id(sheet_url)
        gid = extract_gid(sheet_url)

        # Get sheet metadata first
        metadata = await self._get_sheet_metadata(sheet_id)

        # Determine worksheet name
        worksheet_name = "Sheet1"
        if gid and metadata.sheets:
            # Find worksheet by gid
            for sheet in metadata.sheets:
                if str(sheet.get("properties", {}).get("sheetId", "")) == gid:
                    worksheet_name = sheet["properties"]["title"]
                    break
        elif metadata.sheets:
            # Use first sheet if no gid specified
            worksheet_name = metadata.sheets[0]["properties"]["title"]

        # 특수 문자가 있는 워크시트 이름은 작은따옴표로 감싸야 함
        if (
            " " in worksheet_name
            or "/" in worksheet_name
            or "(" in worksheet_name
            or ")" in worksheet_name
        ):
            worksheet_range = f"'{worksheet_name}'"
        else:
            worksheet_range = worksheet_name

        # Get sheet data
        data = await self._get_sheet_data(sheet_id, worksheet_range)

        # Normalize data
        columns, rows = normalize_sheet_data(data)

        # Get sample rows (max 5)
        sample_rows = rows[:5] if rows else []

        return GoogleSheetPreviewResponse(
            sheet_id=sheet_id,
            sheet_url=sheet_url,
            sheet_title=metadata.title,
            worksheet_title=sanitize_worksheet_name(worksheet_name),
            worksheet_name=worksheet_name,
            metadata=metadata,
            columns=columns,
            sample_rows=sample_rows,
            preview_data=data[:5] if data else [],  # First 5 rows as preview
            total_rows=len(rows),
            total_columns=len(columns),
        )

    async def register_sheet(
        self, sheet_url: str, worksheet_name: Optional[str] = None, polling_interval: int = 300
    ) -> GoogleSheetRegisterResponse:
        """
        Google Sheet 등록 (폴링용)

        Args:
            sheet_url: Google Sheets URL
            worksheet_name: 워크시트 이름
            polling_interval: 폴링 간격 (초)

        Returns:
            등록 응답
        """
        sheet_id = extract_sheet_id(sheet_url)

        # Get metadata to validate worksheet
        metadata = await self._get_sheet_metadata(sheet_id)

        # Validate or set worksheet name
        if not worksheet_name:
            if metadata.sheets:
                worksheet_name = metadata.sheets[0]["properties"]["title"]
            else:
                worksheet_name = "Sheet1"

        # Create registered sheet record
        registered_sheet = RegisteredSheet(
            sheet_id=sheet_id,
            sheet_url=str(sheet_url),
            worksheet_name=sanitize_worksheet_name(worksheet_name),
            polling_interval=polling_interval,
            registered_at=format_datetime_iso(datetime.utcnow()),
        )

        # Store in memory (실제로는 DB 저장)
        self._registered_sheets[sheet_id] = registered_sheet

        # Start polling task (실제로는 별도 worker 사용)
        asyncio.create_task(self._start_polling(sheet_id))

        return GoogleSheetRegisterResponse(
            sheet_id=sheet_id,
            status="success",
            message=f"Successfully registered sheet {sheet_id} for polling",
            registered_sheet=registered_sheet,
        )

    async def _get_sheet_metadata(self, sheet_id: str) -> SheetMetadata:
        """
        Google Sheet 메타데이터 조회

        Args:
            sheet_id: Sheet ID

        Returns:
            메타데이터
        """
        client = await self._get_client()
        url = build_sheets_metadata_url(sheet_id)

        params = {}
        if self.api_key:
            params["key"] = self.api_key

        try:
            response = await client.get(url, params=params)
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
                )
            raise

    async def _get_sheet_data(self, sheet_id: str, range_name: str = "Sheet1") -> List[List[Any]]:
        """
        Google Sheet 데이터 조회

        Args:
            sheet_id: Sheet ID
            range_name: 범위 (워크시트명 또는 A1 notation)

        Returns:
            2차원 데이터 배열
        """
        client = await self._get_client()
        url = build_sheets_api_url(sheet_id, range_name)

        params = {
            "majorDimension": "ROWS",
            "valueRenderOption": "FORMATTED_VALUE",
            "dateTimeRenderOption": "FORMATTED_STRING",
        }
        if self.api_key:
            params["key"] = self.api_key

        try:
            response = await client.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            return data.get("values", [])

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                raise ValueError(
                    "Cannot access the Google Sheet data. " "Please ensure it's shared publicly."
                )
            elif e.response.status_code == 400:
                # Invalid range
                logger.warning(f"Invalid range '{range_name}', trying 'Sheet1'")
                if range_name != "Sheet1":
                    return await self._get_sheet_data(sheet_id, "Sheet1")
            raise

    async def _start_polling(self, sheet_id: str):
        """
        시트 폴링 시작 (백그라운드 태스크)

        Args:
            sheet_id: Sheet ID
        """
        registered_sheet = self._registered_sheets.get(sheet_id)
        if not registered_sheet:
            return

        logger.info(f"Starting polling for sheet {sheet_id}")

        while registered_sheet.is_active:
            try:
                # Wait for polling interval
                await asyncio.sleep(registered_sheet.polling_interval)

                # Get current data
                data = await self._get_sheet_data(sheet_id, registered_sheet.worksheet_name)

                # Calculate hash
                current_hash = calculate_data_hash(data)

                # Check if data changed
                if registered_sheet.last_hash and current_hash != registered_sheet.last_hash:
                    logger.info(f"Data change detected in sheet {sheet_id}")

                    # Create update notification
                    columns, rows = normalize_sheet_data(data)
                    update = GoogleSheetDataUpdate(
                        sheet_id=sheet_id,
                        worksheet_name=registered_sheet.worksheet_name,
                        changed_rows=len(rows),
                        changed_columns=columns,
                        timestamp=format_datetime_iso(datetime.utcnow()),
                    )

                    # Send notification to Kafka
                    await self._notify_data_change(update)

                # Update polling info
                registered_sheet.last_hash = current_hash
                registered_sheet.last_polled = format_datetime_iso(datetime.utcnow())

            except Exception as e:
                logger.error(f"Polling error for sheet {sheet_id}: {e}")
                # Continue polling even on error
                await asyncio.sleep(60)  # Wait 1 minute on error

    async def _notify_data_change(self, update: GoogleSheetDataUpdate):
        """
        데이터 변경을 Kafka에 알림

        Args:
            update: 변경 정보
        """
        try:
            topic = "google-sheets-updates"
            value = update.json()
            key = update.sheet_id.encode("utf-8")

            self.producer.produce(topic=topic, value=value, key=key)
            self.producer.flush()
            logger.info(f"Successfully produced data change event to Kafka topic '{topic}'")

        except Exception as e:
            logger.error(f"Failed to produce message to Kafka: {e}")


    async def unregister_sheet(self, sheet_id: str) -> bool:
        """
        시트 등록 해제

        Args:
            sheet_id: Sheet ID

        Returns:
            성공 여부
        """
        if sheet_id in self._registered_sheets:
            self._registered_sheets[sheet_id].is_active = False
            del self._registered_sheets[sheet_id]
            logger.info(f"Unregistered sheet {sheet_id}")
            return True
        return False

    async def get_registered_sheets(self) -> List[RegisteredSheet]:
        """
        등록된 시트 목록 조회

        Returns:
            등록된 시트 목록
        """
        return list(self._registered_sheets.values())

    async def close(self):
        """리소스 정리"""
        if self._client:
            await self._client.aclose()
            self._client = None

        # Stop all polling tasks
        for sheet in self._registered_sheets.values():
            sheet.is_active = False
