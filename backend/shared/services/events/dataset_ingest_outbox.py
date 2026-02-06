from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from confluent_kafka import Producer

from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.observability.context_propagation import (
    attach_context_from_carrier,
    carrier_from_envelope_metadata,
)
from shared.observability.tracing import get_tracing_service
from shared.services.registries.dataset_registry import DatasetRegistry, DatasetIngestOutboxItem
from shared.services.storage.event_store import event_store
from shared.services.registries.lineage_store import LineageStore
from shared.models.event_envelope import EventEnvelope
from shared.services.kafka.dlq_publisher import DlqPublishSpec, publish_contextual_dlq_json
from shared.services.kafka.producer_factory import create_kafka_dlq_producer
from shared.services.kafka.producer_ops import close_kafka_producer
from shared.services.events.outbox_runtime import (
    build_outbox_worker_id,
    flush_outbox_until_empty,
    maybe_purge_with_interval,
    run_outbox_poll_loop,
)
from shared.utils.backoff_utils import next_exponential_backoff_at

logger = logging.getLogger(__name__)


class DatasetIngestOutboxPublisher:
    def __init__(
        self,
        *,
        dataset_registry: DatasetRegistry,
        lineage_store: Optional[LineageStore],
        batch_size: int = 50,
    ) -> None:
        settings = get_settings()
        cfg = settings.workers.dataset_ingest_outbox
        self.registry = dataset_registry
        self.lineage_store = lineage_store
        self.batch_size = batch_size
        self.flush_timeout_seconds = float(cfg.flush_timeout_seconds)
        self.backoff_base = int(cfg.backoff_base_seconds)
        self.backoff_max = int(cfg.backoff_max_seconds)
        self.max_retries = int(cfg.max_retries)
        self.claim_timeout_seconds = int(cfg.claim_timeout_seconds)
        self.purge_interval_seconds = int(cfg.purge_interval_seconds)
        self.retention_days = int(cfg.retention_days)
        self.purge_limit = int(cfg.purge_limit)

        self.worker_id = build_outbox_worker_id(
            configured_worker_id=cfg.worker_id,
            service_name=settings.observability.service_name,
            hostname=settings.observability.hostname,
            default_service_name="dataset-ingest-outbox",
        )
        self._last_purge = datetime.now(timezone.utc)
        self.enable_dlq = bool(cfg.enable_dlq)
        self.dlq_topic = (AppConfig.DATASET_INGEST_OUTBOX_DLQ_TOPIC or "").strip() or "dataset-ingest-outbox-dlq"
        self._dlq_spec = DlqPublishSpec(
            dlq_topic=self.dlq_topic,
            service_name="dataset-ingest-outbox",
            span_name="dataset_ingest_outbox.dlq_produce",
            flush_timeout_seconds=self.flush_timeout_seconds,
        )
        self.dlq_producer: Optional[Producer] = None
        self.tracing = get_tracing_service("dataset-ingest-outbox")
        if self.enable_dlq:
            max_in_flight = int(cfg.dlq_max_in_flight)
            delivery_timeout_ms = int(cfg.dlq_delivery_timeout_ms)
            request_timeout_ms = int(cfg.dlq_request_timeout_ms)
            if delivery_timeout_ms <= request_timeout_ms:
                delivery_timeout_ms = request_timeout_ms + 1_000
            retries = int(cfg.dlq_retries)
            client_id = (settings.observability.service_name or "dataset-ingest-outbox").strip() or "dataset-ingest-outbox"
            self.dlq_producer = create_kafka_dlq_producer(
                bootstrap_servers=settings.database.kafka_servers,
                client_id=client_id,
                retries=retries,
                retry_backoff_ms=250,
                linger_ms=10,
                enable_idempotence=True,
                max_in_flight_requests_per_connection=max_in_flight,
                extra_config={
                    "delivery.timeout.ms": delivery_timeout_ms,
                    "request.timeout.ms": request_timeout_ms,
                },
            )

    async def close(self) -> None:
        await close_kafka_producer(
            producer=self.dlq_producer,
            timeout_s=self.flush_timeout_seconds,
            warning_logger=logger,
            warning_message="DLQ producer flush failed during shutdown: %s",
        )
        self.dlq_producer = None

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
        key = item.ingest_request_id.encode("utf-8")
        try:
            await publish_contextual_dlq_json(
                producer=self.dlq_producer,
                spec=self._dlq_spec,
                payload=payload,
                message_key=key,
                source_topic="dataset_ingest_outbox",
                source_partition=-1,
                source_offset=-1,
                tracing=self.tracing,
                metrics=None,
                kafka_headers=None,
                fallback_metadata=item.payload if isinstance(item.payload, dict) else None,
                span_attributes={
                    "messaging.system": "kafka",
                    "messaging.destination": self.dlq_topic,
                    "messaging.destination_kind": "topic",
                    "dataset.ingest_request_id": item.ingest_request_id,
                    "dataset.outbox_id": item.outbox_id,
                },
            )
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
                    next_attempt_at=next_exponential_backoff_at(
                        attempts,
                        base_seconds=self.backoff_base,
                        max_seconds=self.backoff_max,
                    ),
                )
            return
        await self.registry.mark_ingest_outbox_failed(
            outbox_id=item.outbox_id,
            error=error,
            next_attempt_at=next_exponential_backoff_at(
                attempts,
                base_seconds=self.backoff_base,
                max_seconds=self.backoff_max,
            ),
        )

    async def _publish_item(self, item: DatasetIngestOutboxItem) -> None:
        if item.kind == "eventstore":
            payload = item.payload or {}
            carrier = carrier_from_envelope_metadata(payload)
            with attach_context_from_carrier(carrier, service_name="dataset-ingest-outbox"):
                with self.tracing.span(
                    "dataset_ingest_outbox.append_event",
                    attributes={
                        "dataset.ingest_request_id": item.ingest_request_id,
                        "dataset.outbox_id": item.outbox_id,
                    },
                ):
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
        self._last_purge = await maybe_purge_with_interval(
            retention_days=self.retention_days,
            purge_interval_seconds=self.purge_interval_seconds,
            purge_limit=self.purge_limit,
            last_purge=self._last_purge,
            purge_call=self.registry.purge_ingest_outbox,
            info_logger=logger.info,
            warning_logger=logger.warning,
            success_message="Purged %s published ingest outbox rows",
            failure_message="Failed to purge ingest outbox rows: %s",
        )


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
    await flush_outbox_until_empty(
        publisher=publisher,
        is_empty=lambda processed: processed == 0,
    )


async def run_dataset_ingest_outbox_worker(
    *,
    dataset_registry: DatasetRegistry,
    lineage_store: Optional[LineageStore],
    poll_interval_seconds: int = 5,
    batch_size: int = 50,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    publisher = DatasetIngestOutboxPublisher(
        dataset_registry=dataset_registry,
        lineage_store=lineage_store,
        batch_size=batch_size,
    )
    await run_outbox_poll_loop(
        publisher=publisher,
        poll_interval_seconds=poll_interval_seconds,
        stop_event=stop_event,
        warning_logger=logger.warning,
        failure_message="Dataset ingest outbox worker failed: %s",
    )


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
