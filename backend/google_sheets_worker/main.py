"""
Google Sheets Update Worker

Consumes Google Sheets change events and triggers a best-effort pipeline.

Current pipeline (minimal, but end-to-end):
- Consume `google-sheets-updates`
- Lookup registration in Redis-backed registry
- If `auto_import=true` and target is configured:
  - Fetch latest sheet values
  - Convert rows into instance payloads (column header -> cell value)
  - Call BFF bulk-create instances endpoint

This avoids the "emit-only" gap where changes were detected but nothing acted on them.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Dict, Optional

import httpx
from confluent_kafka import Consumer, KafkaError

from data_connector.google_sheets.registry import GoogleSheetsRegistry
from data_connector.google_sheets.service import GoogleSheetsService
from data_connector.google_sheets.utils import normalize_sheet_data
from shared.config.service_config import ServiceConfig
from shared.services.redis_service import create_redis_service_legacy

logger = logging.getLogger(__name__)


def _processed_hash_key(sheet_id: str) -> str:
    return f"spice:google_sheets:processed_hash:{sheet_id}"


class GoogleSheetsUpdateWorker:
    def __init__(self) -> None:
        self.running = False
        self.topic = os.getenv("GOOGLE_SHEETS_UPDATES_TOPIC", "google-sheets-updates")
        self.group_id = os.getenv("GOOGLE_SHEETS_WORKER_GROUP", "google-sheets-worker-group")
        self.kafka_servers = ServiceConfig.get_kafka_bootstrap_servers()

        self.consumer: Optional[Consumer] = None
        self.redis_service = None
        self.registry: Optional[GoogleSheetsRegistry] = None
        self.sheets_service: Optional[GoogleSheetsService] = None
        self.http: Optional[httpx.AsyncClient] = None

    async def initialize(self) -> None:
        # Redis (durable registry + idempotency)
        self.redis_service = create_redis_service_legacy()
        await self.redis_service.connect()
        self.registry = GoogleSheetsRegistry(self.redis_service.client)

        # Google Sheets fetcher (no producer needed here)
        api_key = (os.getenv("GOOGLE_API_KEY") or os.getenv("GOOGLE_SHEETS_API_KEY") or "").strip() or None
        self.sheets_service = GoogleSheetsService(api_key=api_key)

        # HTTP client to call BFF for auto-import
        self.http = httpx.AsyncClient(timeout=60.0)

        # Kafka consumer
        self.consumer = Consumer(
            {
                "bootstrap.servers": self.kafka_servers,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "session.timeout.ms": 45000,
                "max.poll.interval.ms": 300000,
            }
        )
        self.consumer.subscribe([self.topic])

        logger.info(f"✅ GoogleSheetsUpdateWorker initialized (topic={self.topic}, group={self.group_id})")

    async def close(self) -> None:
        if self.http:
            await self.http.aclose()
            self.http = None
        if self.sheets_service:
            await self.sheets_service.close()
            self.sheets_service = None
        if self.redis_service:
            await self.redis_service.disconnect()
            self.redis_service = None
        if self.consumer:
            try:
                self.consumer.close()
            except Exception:
                pass
            self.consumer = None

    async def _maybe_skip_processed(self, sheet_id: str, current_hash: Optional[str]) -> bool:
        """
        Best-effort idempotency.

        Returns True when this update has already been processed (by hash), so it can be skipped safely.
        """
        if not self.redis_service or not current_hash:
            return False

        key = _processed_hash_key(sheet_id)
        last = await self.redis_service.client.get(key)
        return bool(last and last == current_hash)

    async def _mark_processed(self, sheet_id: str, current_hash: Optional[str]) -> None:
        if not self.redis_service or not current_hash:
            return
        key = _processed_hash_key(sheet_id)
        await self.redis_service.client.set(key, current_hash)

    async def _auto_import_if_configured(self, *, sheet_id: str, current_hash: Optional[str]) -> None:
        if not self.registry or not self.sheets_service or not self.http:
            return

        registered = await self.registry.get(sheet_id)
        if not registered or not registered.is_active:
            await self._mark_processed(sheet_id, current_hash)
            return

        if not registered.auto_import:
            await self._mark_processed(sheet_id, current_hash)
            return

        db_name = (registered.database_name or "").strip()
        class_label = (registered.class_label or "").strip()
        branch = (registered.branch or "main").strip() or "main"

        if not db_name or not class_label:
            logger.warning(
                f"Auto-import enabled but target not configured (sheet_id={sheet_id}, "
                f"database_name={registered.database_name}, class_label={registered.class_label})"
            )
            await self._mark_processed(sheet_id, current_hash)
            return

        # Fetch full sheet data
        _, _, _, _, values = await self.sheets_service.fetch_sheet_values(
            registered.sheet_url,
            worksheet_name=registered.worksheet_name,
        )
        columns, rows = normalize_sheet_data(values)

        if not columns or not rows:
            logger.info(f"No data to import (sheet_id={sheet_id})")
            await self._mark_processed(sheet_id, current_hash)
            return

        max_rows = registered.max_import_rows
        if max_rows is not None and max_rows > 0:
            rows = rows[: int(max_rows)]

        instances: list[Dict[str, Any]] = []
        for row in rows:
            item = {columns[i]: row[i] for i in range(len(columns))}
            if all(str(v).strip() == "" for v in item.values()):
                continue
            instances.append(item)

        if not instances:
            logger.info(f"No non-empty rows to import (sheet_id={sheet_id})")
            await self._mark_processed(sheet_id, current_hash)
            return

        bff_url = ServiceConfig.get_bff_url()
        url = f"{bff_url}/api/v1/database/{db_name}/instances/{class_label}/bulk-create"
        payload = {
            "instances": instances,
            "metadata": {
                "source": "google_sheets_worker",
                "sheet_id": sheet_id,
                "sheet_url": registered.sheet_url,
                "worksheet_name": registered.worksheet_name,
                "detected_hash": current_hash,
            },
        }

        resp = await self.http.post(url, params={"branch": branch}, json=payload)
        resp.raise_for_status()
        result = resp.json()

        logger.info(
            f"✅ Auto-import submitted (sheet_id={sheet_id}, db={db_name}, class={class_label}, "
            f"instances={len(instances)}, response={result})"
        )

        await self._mark_processed(sheet_id, current_hash)

    async def run(self) -> None:
        if not self.consumer:
            raise RuntimeError("Worker not initialized")

        self.running = True
        logger.info("🚀 GoogleSheetsUpdateWorker started")

        while self.running:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                await asyncio.sleep(0)  # yield
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                raw = msg.value().decode("utf-8")
                payload = json.loads(raw)
                sheet_id = str(payload.get("sheet_id") or "")
                current_hash = payload.get("current_hash")

                if not sheet_id:
                    # Poison pill: commit to avoid infinite retry loop.
                    logger.warning("Skipping invalid google-sheets-updates message (missing sheet_id)")
                    self.consumer.commit(msg, asynchronous=False)
                    continue

                if await self._maybe_skip_processed(sheet_id, current_hash):
                    self.consumer.commit(msg, asynchronous=False)
                    continue

                await self._auto_import_if_configured(sheet_id=sheet_id, current_hash=current_hash)

                # Commit only after successful processing.
                self.consumer.commit(msg, asynchronous=False)

            except httpx.HTTPStatusError as e:
                logger.error(f"BFF call failed (will retry): {e.response.status_code} {e.response.text}")
                # Do not commit → retry
                await asyncio.sleep(2)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in google-sheets-updates (dropping): {e}")
                self.consumer.commit(msg, asynchronous=False)
            except Exception as e:
                logger.error(f"Unhandled error processing google-sheets-updates (will retry): {e}")
                await asyncio.sleep(2)


async def _main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    worker = GoogleSheetsUpdateWorker()
    await worker.initialize()
    try:
        await worker.run()
    finally:
        await worker.close()


if __name__ == "__main__":
    asyncio.run(_main())

