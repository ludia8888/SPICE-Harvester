"""
Google Sheets Connector - Service Layer
"""

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from confluent_kafka import Producer
from redis.asyncio import Redis
from uuid import uuid4

from .models import (
    GoogleSheetDataUpdate,
    GoogleSheetPreviewResponse,
    GoogleSheetRegisterResponse,
    RegisteredSheet,
    SheetMetadata,
)
from .registry import GoogleSheetsRegistry
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

    _LOCK_PREFIX = "spice:google_sheets:poll_lock"
    _LOCK_RELEASE_SCRIPT = """
    if redis.call("GET", KEYS[1]) == ARGV[1] then
      return redis.call("DEL", KEYS[1])
    else
      return 0
    end
    """

    def __init__(
        self,
        producer: Optional[Producer] = None,
        api_key: Optional[str] = None,
        *,
        redis_client: Optional[Redis] = None,
        registry: Optional[GoogleSheetsRegistry] = None,
    ):
        """
        초기화

        Args:
            producer: Kafka Producer instance
            api_key: Google API Key (환경변수 대체 가능)
            redis_client: Optional Redis client (durable registrations + scale-out safe locks)
            registry: Optional registry override (tests)
        """
        self.producer = producer
        self.api_key = api_key or os.getenv("GOOGLE_API_KEY", "")
        if not self.api_key:
            logger.warning("Google API key not configured. Only public sheets will be accessible.")

        self._redis = redis_client
        self._registry = registry or GoogleSheetsRegistry(redis_client)
        self._poller_id = uuid4().hex
        self._polling_tasks: Dict[str, asyncio.Task] = {}

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

    async def fetch_sheet_values(
        self,
        sheet_url: str,
        *,
        worksheet_name: Optional[str] = None,
        api_key: Optional[str] = None,
    ) -> tuple[str, SheetMetadata, str, Optional[int], List[List[Any]]]:
        """
        Fetch raw values + metadata for a Google Sheet URL.

        Returns:
            (sheet_id, metadata, worksheet_title, worksheet_sheet_id, values)
        """
        # Extract sheet ID
        sheet_id = extract_sheet_id(sheet_url)
        gid = extract_gid(sheet_url)

        # Get sheet metadata first
        metadata = await self._get_sheet_metadata(sheet_id, api_key=api_key)

        # Determine worksheet name (+ numeric sheetId when possible)
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

        # 특수 문자가 있는 워크시트 이름은 작은따옴표로 감싸야 함
        if any(ch in worksheet_title for ch in (" ", "/", "(", ")")):
            worksheet_range = f"'{worksheet_title}'"
        else:
            worksheet_range = worksheet_title

        values = await self._get_sheet_data(sheet_id, worksheet_range, api_key=api_key)
        return sheet_id, metadata, worksheet_title, worksheet_sheet_id, values

    async def preview_sheet(
        self,
        sheet_url: str,
        *,
        worksheet_name: Optional[str] = None,
        limit: int = 10,
        api_key: Optional[str] = None,
    ) -> GoogleSheetPreviewResponse:
        """
        Google Sheet 미리보기

        Args:
            sheet_url: Google Sheets URL
            worksheet_name: 워크시트 이름(선택)
            limit: 샘플 행 수
            api_key: 요청 단위 API 키(선택)

        Returns:
            미리보기 응답

        Raises:
            ValueError: 유효하지 않은 URL
            httpx.HTTPError: API 호출 실패
        """
        sheet_id, metadata, worksheet_title, _, data = await self.fetch_sheet_values(
            sheet_url,
            worksheet_name=worksheet_name,
            api_key=api_key,
        )

        # Normalize data
        columns, rows = normalize_sheet_data(data)

        # Get sample rows
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

    async def register_sheet(
        self,
        sheet_url: str,
        worksheet_name: Optional[str] = None,
        polling_interval: int = 300,
        *,
        database_name: Optional[str] = None,
        branch: str = "main",
        class_label: Optional[str] = None,
        auto_import: bool = False,
        max_import_rows: Optional[int] = None,
    ) -> GoogleSheetRegisterResponse:
        """
        Google Sheet 등록 (폴링용)

        Args:
            sheet_url: Google Sheets URL
            worksheet_name: 워크시트 이름
            polling_interval: 폴링 간격 (초)
            database_name: Optional target database (auto-import)
            branch: Optional target branch (auto-import)
            class_label: Optional target class label (auto-import)
            auto_import: Enable auto-import on change
            max_import_rows: Optional max rows imported per run

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

        now_iso = format_datetime_iso(datetime.now(timezone.utc))

        # Create registered sheet record (durable)
        registered_sheet = RegisteredSheet(
            sheet_id=sheet_id,
            sheet_url=str(sheet_url),
            worksheet_name=str(worksheet_name),
            polling_interval=polling_interval,
            database_name=database_name,
            branch=branch or "main",
            class_label=class_label,
            auto_import=bool(auto_import),
            max_import_rows=max_import_rows,
            registered_at=now_iso,
        )

        # Best-effort: establish an initial hash baseline at registration time.
        try:
            range_name = registered_sheet.worksheet_name
            if any(ch in range_name for ch in (" ", "/", "(", ")")):
                range_name = f"'{range_name}'"
            data = await self._get_sheet_data(sheet_id, range_name)
            registered_sheet.last_hash = calculate_data_hash(data)
            registered_sheet.last_polled = now_iso
        except Exception as e:
            logger.warning(f"Initial sheet fetch failed (sheet_id={sheet_id}): {e}")

        await self._registry.upsert(registered_sheet)

        # Start polling task (durable registrations will be reloaded on restart)
        self._ensure_polling_task(sheet_id)

        return GoogleSheetRegisterResponse(
            sheet_id=sheet_id,
            status="success",
            message=f"Successfully registered sheet {sheet_id} for polling",
            registered_sheet=registered_sheet,
        )

    async def start_polling_for_active_sheets(self) -> None:
        """
        Start pollers for all active sheets in the registry.

        Intended to be called once at service startup so registrations survive restarts.
        """
        try:
            sheets = await self.get_registered_sheets()
        except Exception as e:
            logger.warning(f"Failed to load registered sheets for polling: {e}")
            return

        for sheet in sheets:
            if sheet and sheet.is_active:
                self._ensure_polling_task(sheet.sheet_id)

    def _ensure_polling_task(self, sheet_id: str) -> None:
        existing = self._polling_tasks.get(sheet_id)
        if existing and not existing.done():
            return
        self._polling_tasks[sheet_id] = asyncio.create_task(self._start_polling(sheet_id))

    async def _try_acquire_poll_lock(self, sheet_id: str, *, ttl_seconds: int) -> bool:
        if not self._redis:
            return True
        if ttl_seconds < 10:
            ttl_seconds = 10
        key = f"{self._LOCK_PREFIX}:{sheet_id}"
        try:
            return bool(await self._redis.set(key, self._poller_id, nx=True, ex=int(ttl_seconds)))
        except Exception as e:
            logger.warning(f"Failed to acquire poll lock (sheet_id={sheet_id}): {e}")
            # Fail-open in dev; worst case we might double-poll briefly.
            return True

    async def _release_poll_lock(self, sheet_id: str) -> None:
        if not self._redis:
            return
        key = f"{self._LOCK_PREFIX}:{sheet_id}"
        try:
            await self._redis.eval(self._LOCK_RELEASE_SCRIPT, 1, key, self._poller_id)
        except Exception as e:
            logger.debug(f"Failed to release poll lock (sheet_id={sheet_id}): {e}")

    async def _get_sheet_metadata(self, sheet_id: str, *, api_key: Optional[str] = None) -> SheetMetadata:
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
        key = api_key or self.api_key
        if key:
            params["key"] = key

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

    async def _get_sheet_data(
        self, sheet_id: str, range_name: str = "Sheet1", *, api_key: Optional[str] = None
    ) -> List[List[Any]]:
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
        key = api_key or self.api_key
        if key:
            params["key"] = key

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
        logger.info(f"Starting polling for sheet {sheet_id}")

        while True:
            try:
                registered_sheet = await self._registry.get(sheet_id)
                if not registered_sheet or not registered_sheet.is_active:
                    return

                # Wait for polling interval
                await asyncio.sleep(int(registered_sheet.polling_interval))

                # Refresh config (interval/worksheet may have changed)
                registered_sheet = await self._registry.get(sheet_id)
                if not registered_sheet or not registered_sheet.is_active:
                    return

                # Scale-out safety: ensure only one instance polls a sheet per cycle.
                # TTL is short because we release after each poll.
                acquired = await self._try_acquire_poll_lock(
                    sheet_id,
                    ttl_seconds=min(300, max(30, int(registered_sheet.polling_interval))) + 60,
                )
                if not acquired:
                    continue

                # Get current data
                range_name = registered_sheet.worksheet_name
                if any(ch in range_name for ch in (" ", "/", "(", ")")):
                    range_name = f"'{range_name}'"
                data = await self._get_sheet_data(sheet_id, range_name)

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
                        timestamp=format_datetime_iso(datetime.now(timezone.utc)),
                        previous_hash=registered_sheet.last_hash,
                        current_hash=current_hash,
                    )

                    # Send notification to Kafka
                    await self._notify_data_change(update)

                # Update polling info
                registered_sheet.last_hash = current_hash
                registered_sheet.last_polled = format_datetime_iso(datetime.now(timezone.utc))
                await self._registry.upsert(registered_sheet)

            except Exception as e:
                logger.error(f"Polling error for sheet {sheet_id}: {e}")
                # Continue polling even on error
                await asyncio.sleep(60)  # Wait 1 minute on error
            finally:
                await self._release_poll_lock(sheet_id)

    async def _notify_data_change(self, update: GoogleSheetDataUpdate):
        """
        데이터 변경을 Kafka에 알림

        Args:
            update: 변경 정보
        """
        try:
            if not self.producer:
                logger.warning("Kafka producer not configured; skipping update notification")
                return
            from shared.config.app_config import AppConfig

            topic = AppConfig.GOOGLE_SHEETS_UPDATES_TOPIC
            value = update.model_dump_json()
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
        deleted = await self._registry.delete(sheet_id)
        task = self._polling_tasks.pop(sheet_id, None)
        if task and not task.done():
            task.cancel()
        if deleted:
            logger.info(f"Unregistered sheet {sheet_id}")
        return bool(deleted)

    async def get_registered_sheets(self) -> List[RegisteredSheet]:
        """
        등록된 시트 목록 조회

        Returns:
            등록된 시트 목록
        """
        return await self._registry.list()

    async def close(self):
        """리소스 정리"""
        if self._client:
            await self._client.aclose()
            self._client = None

        # Stop polling tasks (registrations remain in the registry)
        for task in list(self._polling_tasks.values()):
            if task and not task.done():
                task.cancel()
        self._polling_tasks.clear()
