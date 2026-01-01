from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from confluent_kafka import Producer

from shared.config.app_config import AppConfig
from shared.config.service_config import ServiceConfig
from shared.services.dataset_registry import DatasetRegistry, DatasetIngestOutboxItem
from shared.services.event_store import event_store
from shared.services.lineage_store import LineageStore
from shared.models.event_envelope import EventEnvelope
from shared.utils.env_utils import parse_bool_env, parse_int_env

logger = logging.getLogger(__name__)


class DatasetIngestOutboxPublisher:
    def __init__(
        self,
        *,
        dataset_registry: DatasetRegistry,
        lineage_store: Optional[LineageStore],
        batch_size: int = 50,
    ) -> None:
        self.registry = dataset_registry
        self.lineage_store = lineage_store
        self.batch_size = batch_size
        self.flush_timeout_seconds = float(os.getenv("DATASET_INGEST_OUTBOX_FLUSH_TIMEOUT_SECONDS", "10") or "10")
        self.backoff_base = parse_int_env("DATASET_INGEST_OUTBOX_BACKOFF_BASE_SECONDS", 2, min_value=1, max_value=300)
        self.backoff_max = parse_int_env("DATASET_INGEST_OUTBOX_BACKOFF_MAX_SECONDS", 60, min_value=1, max_value=3600)
        self.max_retries = parse_int_env("DATASET_INGEST_OUTBOX_MAX_RETRIES", 5, min_value=1, max_value=100)
        self.claim_timeout_seconds = parse_int_env(
            "DATASET_INGEST_OUTBOX_CLAIM_TIMEOUT_SECONDS",
            300,
            min_value=0,
            max_value=86_400,
        )
        self.worker_id = (
            os.getenv("DATASET_INGEST_OUTBOX_WORKER_ID")
            or f"{os.getenv('SERVICE_NAME') or 'dataset-ingest-outbox'}:{os.getenv('HOSTNAME') or 'local'}:{os.getpid()}"
        )
        self.purge_interval_seconds = parse_int_env(
            "DATASET_INGEST_OUTBOX_PURGE_INTERVAL_SECONDS",
            3600,
            min_value=300,
            max_value=86_400,
        )
        self.retention_days = parse_int_env("DATASET_INGEST_OUTBOX_RETENTION_DAYS", 7, min_value=0, max_value=365)
        self.purge_limit = parse_int_env("DATASET_INGEST_OUTBOX_PURGE_LIMIT", 10_000, min_value=1, max_value=100_000)
        self._last_purge = datetime.now(timezone.utc)
        self.enable_dlq = parse_bool_env("ENABLE_DATASET_INGEST_OUTBOX_DLQ", True)
        self.dlq_topic = (AppConfig.DATASET_INGEST_OUTBOX_DLQ_TOPIC or "").strip() or "dataset-ingest-outbox-dlq"
        self.dlq_producer: Optional[Producer] = None
        if self.enable_dlq:
            max_in_flight = parse_int_env("DATASET_INGEST_OUTBOX_MAX_IN_FLIGHT", 5, min_value=1, max_value=5)
            delivery_timeout_ms = parse_int_env(
                "DATASET_INGEST_OUTBOX_DELIVERY_TIMEOUT_MS",
                120_000,
                min_value=10_000,
                max_value=1_800_000,
            )
            request_timeout_ms = parse_int_env(
                "DATASET_INGEST_OUTBOX_REQUEST_TIMEOUT_MS",
                30_000,
                min_value=5_000,
                max_value=600_000,
            )
            if delivery_timeout_ms <= request_timeout_ms:
                delivery_timeout_ms = request_timeout_ms + 1_000
            retries = parse_int_env("DATASET_INGEST_OUTBOX_RETRIES", 5, min_value=0, max_value=100)
            self.dlq_producer = Producer(
                {
                    "bootstrap.servers": ServiceConfig.get_kafka_bootstrap_servers(),
                    "client.id": os.getenv("SERVICE_NAME") or "dataset-ingest-outbox",
                    "acks": "all",
                    "retries": retries,
                    "retry.backoff.ms": 250,
                    "linger.ms": 10,
                    "compression.type": "snappy",
                    "enable.idempotence": True,
                    "max.in.flight.requests.per.connection": max_in_flight,
                    "delivery.timeout.ms": delivery_timeout_ms,
                    "request.timeout.ms": request_timeout_ms,
                }
            )

    async def close(self) -> None:
        if self.dlq_producer:
            try:
                await asyncio.to_thread(self.dlq_producer.flush, self.flush_timeout_seconds)
            except Exception:
                pass

    def _next_attempt_at(self, attempts: int) -> datetime:
        delay = min(self.backoff_max, self.backoff_base * (2 ** max(0, attempts - 1)))
        return datetime.now(timezone.utc) + timedelta(seconds=delay)

    async def _send_to_dlq(self, item: DatasetIngestOutboxItem, *, error: str, attempts: int) -> bool:
        if not self.dlq_producer:
            logger.error("Dataset ingest DLQ producer not configured; dropping payload for outbox %s", item.outbox_id)
            return False
        payload = {
            "outbox_id": item.outbox_id,
            "ingest_request_id": item.ingest_request_id,
            "kind": item.kind,
            "attempts": attempts,
            "retry_count": getattr(item, "retry_count", None),
            "error": error,
            "payload": item.payload,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "source": "dataset_ingest_outbox",
        }
        encoded = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        key = item.ingest_request_id.encode("utf-8")
        try:
            self.dlq_producer.produce(topic=self.dlq_topic, value=encoded, key=key)
            remaining = await asyncio.to_thread(self.dlq_producer.flush, self.flush_timeout_seconds)
            if remaining != 0:
                logger.warning("Dataset ingest DLQ flush incomplete (remaining=%s)", remaining)
                return False
            return True
        except Exception as exc:
            logger.warning("Failed to publish dataset ingest DLQ payload: %s", exc)
            return False

    async def _handle_failure(self, item: DatasetIngestOutboxItem, *, error: str) -> None:
        attempts = max(int(item.publish_attempts), int(getattr(item, "retry_count", 0) or 0)) + 1
        if attempts >= self.max_retries:
            sent = await self._send_to_dlq(item, error=error, attempts=attempts)
            if sent:
                await self.registry.mark_ingest_outbox_dead(outbox_id=item.outbox_id, error=error)
            else:
                await self.registry.mark_ingest_outbox_failed(
                    outbox_id=item.outbox_id,
                    error=f"dlq_publish_failed: {error}",
                    next_attempt_at=self._next_attempt_at(attempts),
                )
            return
        await self.registry.mark_ingest_outbox_failed(
            outbox_id=item.outbox_id,
            error=error,
            next_attempt_at=self._next_attempt_at(attempts),
        )

    async def _publish_item(self, item: DatasetIngestOutboxItem) -> None:
        if item.kind == "eventstore":
            payload = item.payload or {}
            event = EventEnvelope(
                event_id=str(payload.get("event_id") or ""),
                event_type=str(payload.get("event_type") or ""),
                aggregate_type=str(payload.get("aggregate_type") or ""),
                aggregate_id=str(payload.get("aggregate_id") or ""),
                occurred_at=payload.get("occurred_at") or datetime.now(timezone.utc),
                actor=payload.get("actor"),
                data=payload.get("data") or {},
                metadata={
                    "kind": "command",
                    "command_type": payload.get("command_type"),
                    "command_id": str(payload.get("event_id") or ""),
                },
            )
            event.metadata["kafka_topic"] = AppConfig.PIPELINE_EVENTS_TOPIC
            await event_store.append_event(event)
            await self.registry.mark_ingest_outbox_published(outbox_id=item.outbox_id)
            return

        if item.kind == "lineage":
            if self.lineage_store is None:
                raise RuntimeError("LineageStore unavailable")
            payload = item.payload or {}
            await self.lineage_store.record_link(
                from_node_id=payload.get("from_node_id"),
                to_node_id=payload.get("to_node_id"),
                edge_type=payload.get("edge_type"),
                occurred_at=payload.get("occurred_at"),
                from_label=payload.get("from_label"),
                to_label=payload.get("to_label"),
                db_name=payload.get("db_name"),
                edge_metadata=payload.get("edge_metadata") or {},
            )
            await self.registry.mark_ingest_outbox_published(outbox_id=item.outbox_id)
            return

        raise RuntimeError(f"Unknown ingest outbox kind: {item.kind}")

    async def flush_once(self) -> int:
        await event_store.connect()
        batch = await self.registry.claim_ingest_outbox_batch(
            limit=self.batch_size,
            claimed_by=self.worker_id,
            claim_timeout_seconds=self.claim_timeout_seconds,
        )
        if not batch:
            return 0
        for item in batch:
            try:
                await self._publish_item(item)
            except Exception as exc:
                await self._handle_failure(item, error=str(exc))
        return len(batch)

    async def maybe_purge(self) -> None:
        if self.retention_days <= 0:
            return
        now = datetime.now(timezone.utc)
        if (now - self._last_purge).total_seconds() < self.purge_interval_seconds:
            return
        self._last_purge = now
        try:
            deleted = await self.registry.purge_ingest_outbox(
                retention_days=self.retention_days,
                limit=self.purge_limit,
            )
            if deleted:
                logger.info("Purged %s published ingest outbox rows", deleted)
        except Exception as exc:
            logger.warning("Failed to purge ingest outbox rows: %s", exc)


async def flush_dataset_ingest_outbox(
    *,
    dataset_registry: DatasetRegistry,
    lineage_store: Optional[LineageStore],
    batch_size: int = 50,
) -> None:
    publisher = DatasetIngestOutboxPublisher(
        dataset_registry=dataset_registry,
        lineage_store=lineage_store,
        batch_size=batch_size,
    )
    try:
        while True:
            processed = await publisher.flush_once()
            if processed == 0:
                break
    finally:
        await publisher.close()


async def run_dataset_ingest_outbox_worker(
    *,
    dataset_registry: DatasetRegistry,
    lineage_store: Optional[LineageStore],
    poll_interval_seconds: int = 5,
    batch_size: int = 50,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    stop_event = stop_event or asyncio.Event()
    publisher = DatasetIngestOutboxPublisher(
        dataset_registry=dataset_registry,
        lineage_store=lineage_store,
        batch_size=batch_size,
    )
    try:
        while not stop_event.is_set():
            try:
                await publisher.flush_once()
                await publisher.maybe_purge()
            except Exception as exc:
                logger.warning("Dataset ingest outbox worker failed: %s", exc)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=poll_interval_seconds)
            except asyncio.TimeoutError:
                continue
    finally:
        await publisher.close()


def build_dataset_event_payload(
    *,
    event_id: str,
    event_type: str,
    aggregate_type: str,
    aggregate_id: str,
    command_type: str,
    actor: Optional[str],
    data: dict[str, Any],
) -> dict[str, Any]:
    command_payload = {
        "command_id": event_id,
        "command_type": command_type,
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id,
        "payload": data,
        "metadata": {},
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": actor,
        "version": 1,
    }
    return {
        "event_id": event_id,
        "event_type": event_type,
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id,
        "command_type": command_type,
        "actor": actor,
        "data": command_payload,
    }
