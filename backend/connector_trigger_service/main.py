"""
Connector Trigger Service (Foundry-style).

Responsibilities:
- Read connector sources from Postgres registry
- Detect external changes (polling/webhooks; v1 implements Google Sheets polling)
- Transactionally enqueue connector update events into Postgres outbox
- Publish outbox to Kafka `connector-updates` as EventEnvelope (metadata.kind='connector_update')

Connector libraries must not decide ontology/mapping. This service only emits change signals.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Producer

from data_connector.google_sheets.service import GoogleSheetsService
from data_connector.google_sheets.utils import calculate_data_hash
from shared.config.service_config import ServiceConfig
from shared.services.connector_registry import ConnectorRegistry, ConnectorSource

logger = logging.getLogger(__name__)


def _parse_int(name: str, default: int, *, min_value: int = 1, max_value: int = 10_000) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        v = int(raw)
    except Exception:
        return default
    return max(min_value, min(max_value, v))


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ConnectorTriggerService:
    def __init__(self) -> None:
        self.running = False
        self.topic = (os.getenv("CONNECTOR_UPDATES_TOPIC") or "connector-updates").strip() or "connector-updates"
        self.source_type = (os.getenv("CONNECTOR_TRIGGER_SOURCE_TYPE") or "google_sheets").strip() or "google_sheets"
        self.tick_seconds = _parse_int("CONNECTOR_TRIGGER_TICK_SECONDS", 5, min_value=1, max_value=3600)
        self.poll_concurrency = _parse_int("CONNECTOR_TRIGGER_POLL_CONCURRENCY", 5, min_value=1, max_value=100)
        self.outbox_batch = _parse_int("CONNECTOR_TRIGGER_OUTBOX_BATCH", 50, min_value=1, max_value=500)

        self._producer_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="kafka-producer")
        self.registry: Optional[ConnectorRegistry] = None
        self.producer: Optional[Producer] = None
        self.sheets: Optional[GoogleSheetsService] = None

    async def initialize(self) -> None:
        # Durable registry (Postgres)
        self.registry = ConnectorRegistry()
        await self.registry.initialize()

        # Connector library (no polling inside)
        api_key = (os.getenv("GOOGLE_API_KEY") or os.getenv("GOOGLE_SHEETS_API_KEY") or "").strip() or None
        self.sheets = GoogleSheetsService(api_key=api_key)

        # Kafka producer (blocking)
        kafka_config = {
            "bootstrap.servers": ServiceConfig.get_kafka_bootstrap_servers(),
            "client.id": os.getenv("SERVICE_NAME") or "connector-trigger-service",
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 100,
            "linger.ms": 20,
            "compression.type": "snappy",
        }
        self.producer = Producer(kafka_config)

        logger.info(
            "✅ ConnectorTriggerService initialized "
            f"(source_type={self.source_type}, topic={self.topic}, tick={self.tick_seconds}s)"
        )

    async def close(self) -> None:
        if self.sheets:
            try:
                await self.sheets.close()
            except Exception:
                pass
            self.sheets = None
        if self.producer:
            try:
                await asyncio.get_running_loop().run_in_executor(self._producer_executor, lambda: self.producer.flush(5))
            except Exception:
                pass
            self.producer = None
        if self.registry:
            try:
                await self.registry.close()
            except Exception:
                pass
            self.registry = None
        self._producer_executor.shutdown(wait=False, cancel_futures=True)

    async def _producer_call(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._producer_executor, lambda: func(*args, **kwargs))

    async def _is_due(self, source: ConnectorSource) -> bool:
        if not self.registry:
            return False
        cfg = source.config_json or {}
        interval = int(cfg.get("polling_interval") or 300)
        state = await self.registry.get_sync_state(source_type=source.source_type, source_id=source.source_id)
        if not state or not state.last_polled_at:
            return True
        elapsed = (_utcnow() - state.last_polled_at).total_seconds()
        return elapsed >= max(1, interval)

    async def _poll_google_sheets(self, source: ConnectorSource) -> None:
        if not self.registry or not self.sheets:
            return

        cfg = source.config_json or {}
        sheet_url = (cfg.get("sheet_url") or "").strip()
        worksheet_name = (cfg.get("worksheet_name") or "").strip() or None

        if not sheet_url:
            logger.warning(f"Skipping google_sheets source with missing sheet_url (source_id={source.source_id})")
            return

        access_token = (cfg.get("access_token") or "").strip() or None
        if not access_token:
            refresh_token = (cfg.get("refresh_token") or "").strip() or None
            if refresh_token:
                from data_connector.google_sheets.auth import GoogleOAuth2Client

                oauth_client = GoogleOAuth2Client()
                if oauth_client.client_id and oauth_client.client_secret:
                    refreshed = await oauth_client.refresh_access_token(refresh_token)
                    access_token = (refreshed.get("access_token") or "").strip() or None
                    cfg.update(
                        {
                            "access_token": refreshed.get("access_token"),
                            "refresh_token": refreshed.get("refresh_token", refresh_token),
                            "expires_at": refreshed.get("expires_at"),
                        }
                    )
                    await self.registry.upsert_source(
                        source_type=source.source_type,
                        source_id=source.source_id,
                        enabled=True,
                        config_json=cfg,
                    )
        sheet_id, _, resolved_ws, _, values = await self.sheets.fetch_sheet_values(
            sheet_url,
            worksheet_name=worksheet_name,
            access_token=access_token,
        )

        if str(sheet_id) != str(source.source_id):
            logger.warning(
                "google_sheets source_id mismatch; continuing (registry_id=%s, fetched_id=%s)",
                source.source_id,
                sheet_id,
            )

        cursor = calculate_data_hash(values)
        envelope = await self.registry.record_poll_result(
            source_type=source.source_type,
            source_id=source.source_id,
            current_cursor=cursor,
            kafka_topic=self.topic,
        )

        if envelope:
            logger.info(
                "🔔 Change detected and enqueued (source=%s:%s, seq=%s, event_id=%s, worksheet=%s)",
                source.source_type,
                source.source_id,
                envelope.sequence_number,
                envelope.event_id,
                resolved_ws,
            )

    async def _poll_source(self, source: ConnectorSource, sem: asyncio.Semaphore) -> None:
        async with sem:
            try:
                if self.source_type == "google_sheets":
                    await self._poll_google_sheets(source)
            except Exception as e:
                logger.error("Polling error (source=%s:%s): %s", source.source_type, source.source_id, e)

    async def _poll_loop(self) -> None:
        if not self.registry:
            raise RuntimeError("Trigger service not initialized")

        sem = asyncio.Semaphore(self.poll_concurrency)
        while self.running:
            try:
                sources = await self.registry.list_sources(source_type=self.source_type, enabled=True, limit=2000)
                tasks = []
                for src in sources:
                    if not await self._is_due(src):
                        continue
                    tasks.append(asyncio.create_task(self._poll_source(src, sem)))
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                logger.error(f"Trigger poll loop error: {e}")
            await asyncio.sleep(self.tick_seconds)

    async def _publish_outbox_loop(self) -> None:
        if not self.registry or not self.producer:
            raise RuntimeError("Trigger service not initialized")

        while self.running:
            try:
                batch = await self.registry.claim_outbox_batch(limit=self.outbox_batch)
                if not batch:
                    await asyncio.sleep(1)
                    continue

                delivery_errors: dict[str, str] = {}

                def _cb(err, msg, outbox_id: str):
                    if err is not None:
                        delivery_errors[outbox_id] = str(err)

                for item in batch:
                    value = json.dumps(item.payload, ensure_ascii=False).encode("utf-8")
                    key = f"{item.source_type}:{item.source_id}".encode("utf-8")
                    await self._producer_call(
                        self.producer.produce,
                        topic=self.topic,
                        value=value,
                        key=key,
                        on_delivery=lambda err, msg, outbox_id=item.outbox_id: _cb(err, msg, outbox_id),
                    )

                remaining = await self._producer_call(self.producer.flush, 10)
                if remaining != 0:
                    logger.warning(f"Producer flush incomplete (remaining={remaining}); will retry outbox")

                for item in batch:
                    err = delivery_errors.get(item.outbox_id)
                    if err:
                        await self.registry.mark_outbox_failed(outbox_id=item.outbox_id, error=err)
                    elif remaining == 0:
                        await self.registry.mark_outbox_published(outbox_id=item.outbox_id)
                    else:
                        # Unknown delivery state; keep pending for retry (duplicates are acceptable).
                        await self.registry.mark_outbox_failed(
                            outbox_id=item.outbox_id, error="flush incomplete; delivery unknown"
                        )

            except Exception as e:
                logger.error(f"Outbox publish loop error: {e}")
                await asyncio.sleep(2)

    async def run(self) -> None:
        self.running = True
        logger.info("🚀 ConnectorTriggerService started")

        poll_task = asyncio.create_task(self._poll_loop())
        publish_task = asyncio.create_task(self._publish_outbox_loop())
        try:
            await asyncio.gather(poll_task, publish_task)
        finally:
            for t in (poll_task, publish_task):
                if t and not t.done():
                    t.cancel()


async def _main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    svc = ConnectorTriggerService()
    await svc.initialize()
    try:
        await svc.run()
    finally:
        await svc.close()


if __name__ == "__main__":
    asyncio.run(_main())
