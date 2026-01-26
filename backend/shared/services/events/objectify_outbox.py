from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from confluent_kafka import Producer

from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.observability.context_propagation import (
    attach_context_from_carrier,
    carrier_from_envelope_metadata,
    kafka_headers_from_envelope_metadata,
)
from shared.observability.tracing import get_tracing_service
from shared.services.registries.objectify_registry import ObjectifyOutboxItem, ObjectifyRegistry

logger = logging.getLogger(__name__)


class ObjectifyOutboxPublisher:
    def __init__(
        self,
        *,
        objectify_registry: ObjectifyRegistry,
        topic: Optional[str] = None,
        batch_size: int = 50,
    ) -> None:
        settings = get_settings()
        cfg = settings.workers.objectify_outbox
        self.registry = objectify_registry
        self.topic = (topic or AppConfig.OBJECTIFY_JOBS_TOPIC or "objectify-jobs").strip() or "objectify-jobs"
        self.batch_size = batch_size
        self.flush_timeout_seconds = float(cfg.flush_timeout_seconds)
        self.backoff_base = int(cfg.backoff_base_seconds)
        self.backoff_max = int(cfg.backoff_max_seconds)
        self.claim_timeout_seconds = int(cfg.claim_timeout_seconds)
        self.purge_interval_seconds = int(cfg.purge_interval_seconds)
        self.retention_days = int(cfg.retention_days)
        self.purge_limit = int(cfg.purge_limit)

        configured_worker_id = str(cfg.worker_id or "").strip()
        if configured_worker_id:
            self.worker_id = configured_worker_id
        else:
            service_name = (settings.observability.service_name or "objectify-outbox").strip() or "objectify-outbox"
            hostname = (settings.observability.hostname or "local").strip() or "local"
            self.worker_id = f"{service_name}:{hostname}:{os.getpid()}"
        self._last_purge = datetime.now(timezone.utc)
        max_in_flight = int(cfg.producer_max_in_flight)
        delivery_timeout_ms = int(cfg.producer_delivery_timeout_ms)
        request_timeout_ms = int(cfg.producer_request_timeout_ms)
        if delivery_timeout_ms <= request_timeout_ms:
            delivery_timeout_ms = request_timeout_ms + 1_000
        retries = int(cfg.producer_retries)
        client_id = (settings.observability.service_name or "objectify-outbox").strip() or "objectify-outbox"

        self.producer = Producer(
            {
                "bootstrap.servers": settings.database.kafka_servers,
                "client.id": client_id,
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
        self.tracing = get_tracing_service("objectify-outbox")

    async def close(self) -> None:
        try:
            await asyncio.to_thread(self.producer.flush, self.flush_timeout_seconds)
        except Exception as exc:
            logger.warning("Kafka producer flush failed during shutdown: %s", exc, exc_info=True)

    def _next_attempt_at(self, attempts: int) -> datetime:
        delay = min(self.backoff_max, self.backoff_base * (2 ** max(0, attempts - 1)))
        return datetime.now(timezone.utc) + timedelta(seconds=delay)

    async def _publish_batch(self, batch: list[ObjectifyOutboxItem]) -> None:
        if not batch:
            return
        delivery_errors: dict[str, str] = {}

        def _cb(err, _msg, outbox_id: str) -> None:
            if err is not None:
                delivery_errors[outbox_id] = str(err)

        for item in batch:
            payload = json.dumps(item.payload, ensure_ascii=False, default=str).encode("utf-8")
            key = item.job_id.encode("utf-8")
            headers = kafka_headers_from_envelope_metadata(item.payload)
            carrier = carrier_from_envelope_metadata(item.payload)
            with attach_context_from_carrier(carrier, service_name="objectify-outbox"):
                with self.tracing.span(
                    "objectify_outbox.produce",
                    attributes={
                        "messaging.system": "kafka",
                        "messaging.destination": self.topic,
                        "messaging.destination_kind": "topic",
                        "objectify.job_id": item.job_id,
                    },
                ):
                    self.producer.produce(
                        topic=self.topic,
                        value=payload,
                        key=key,
                        headers=headers or None,
                        on_delivery=lambda err, msg, outbox_id=item.outbox_id: _cb(err, msg, outbox_id),
                    )

        remaining = await asyncio.to_thread(self.producer.flush, self.flush_timeout_seconds)
        if remaining != 0:
            logger.warning("Objectify outbox flush incomplete (remaining=%s)", remaining)

        for item in batch:
            err = delivery_errors.get(item.outbox_id)
            if err:
                attempts = int(item.publish_attempts) + 1
                await self.registry.mark_objectify_outbox_failed(
                    outbox_id=item.outbox_id,
                    error=err,
                    next_attempt_at=self._next_attempt_at(attempts),
                )
            elif remaining == 0:
                await self.registry.mark_objectify_outbox_published(
                    outbox_id=item.outbox_id,
                    job_id=item.job_id,
                )
            else:
                attempts = int(item.publish_attempts) + 1
                await self.registry.mark_objectify_outbox_failed(
                    outbox_id=item.outbox_id,
                    error="flush incomplete; delivery unknown",
                    next_attempt_at=self._next_attempt_at(attempts),
                )

    async def flush_once(self) -> None:
        batch = await self.registry.claim_objectify_outbox_batch(
            limit=self.batch_size,
            claimed_by=self.worker_id,
            claim_timeout_seconds=self.claim_timeout_seconds,
        )
        if not batch:
            return
        await self._publish_batch(batch)

    async def maybe_purge(self) -> None:
        if self.retention_days <= 0:
            return
        now = datetime.now(timezone.utc)
        if (now - self._last_purge).total_seconds() < self.purge_interval_seconds:
            return
        self._last_purge = now
        try:
            deleted = await self.registry.purge_objectify_outbox(
                retention_days=self.retention_days,
                limit=self.purge_limit,
            )
            if deleted:
                logger.info("Purged %s published objectify outbox rows", deleted)
        except Exception as exc:
            logger.warning("Failed to purge objectify outbox rows: %s", exc)


async def run_objectify_outbox_worker(
    *,
    objectify_registry: ObjectifyRegistry,
    poll_interval_seconds: int = 5,
    batch_size: int = 50,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    stop_event = stop_event or asyncio.Event()
    publisher = ObjectifyOutboxPublisher(
        objectify_registry=objectify_registry,
        batch_size=batch_size,
    )
    try:
        while not stop_event.is_set():
            try:
                await publisher.flush_once()
                await publisher.maybe_purge()
            except Exception as exc:
                logger.warning("Objectify outbox worker failed: %s", exc)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=poll_interval_seconds)
            except asyncio.TimeoutError:
                continue
    finally:
        await publisher.close()
