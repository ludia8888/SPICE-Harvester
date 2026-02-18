"""
Connector Trigger Service (shared runtime).

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
import time
from typing import Optional

from confluent_kafka import Producer

from data_connector.adapters.factory import (
    ConnectorAdapterFactory,
    SUPPORTED_CONNECTOR_KINDS,
    connector_kind_from_source_type,
    file_import_source_type_for_kind,
    table_import_source_type_for_kind,
)
from data_connector.adapters.runtime_credentials import resolve_source_runtime_credentials
from data_connector.google_sheets.service import GoogleSheetsService
from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.observability.context_propagation import (
    attach_context_from_carrier,
    carrier_from_envelope_metadata,
    kafka_headers_from_current_context,
    kafka_headers_from_envelope_metadata,
)
from shared.observability.metrics import get_metrics_collector
from shared.observability.tracing import get_tracing_service
from shared.services.kafka.producer_factory import create_kafka_producer
from shared.services.kafka.producer_ops import ExecutorKafkaProducerOps, InlineKafkaProducerOps, KafkaProducerOps
from shared.services.registries.connector_registry import ConnectorRegistry, ConnectorSource
from shared.utils.app_logger import configure_logging
from shared.utils.time_utils import utcnow
from shared.utils.worker_runner import run_component_lifecycle

logger = logging.getLogger(__name__)


class ConnectorTriggerService:
    def __init__(self) -> None:
        settings = get_settings()
        cfg = settings.workers.connector_trigger

        self.running = False
        self.topic = AppConfig.CONNECTOR_UPDATES_TOPIC
        self.tick_seconds = int(cfg.tick_seconds)
        self.poll_concurrency = int(cfg.poll_concurrency)
        self.outbox_batch = int(cfg.outbox_batch)
        self.tracing = get_tracing_service("connector-trigger-service")
        self.metrics = get_metrics_collector("connector-trigger-service")
        table_source_types = [table_import_source_type_for_kind(kind) for kind in SUPPORTED_CONNECTOR_KINDS]
        file_source_types = [file_import_source_type_for_kind(kind) for kind in SUPPORTED_CONNECTOR_KINDS]
        self.source_types = tuple(table_source_types + file_source_types)

        self.registry: Optional[ConnectorRegistry] = None
        self.producer: Optional[Producer] = None
        self.producer_ops: Optional[KafkaProducerOps] = None
        self.sheets: Optional[GoogleSheetsService] = None
        self.adapter_factory: Optional[ConnectorAdapterFactory] = None

    async def initialize(self) -> None:
        settings = get_settings()
        # Durable registry (Postgres)
        self.registry = ConnectorRegistry()
        await self.registry.initialize()

        # Connector library (no polling inside)
        api_key = settings.google_sheets.google_sheets_api_key
        self.sheets = GoogleSheetsService(api_key=api_key)
        self.adapter_factory = ConnectorAdapterFactory(google_sheets_service=self.sheets)

        # Kafka producer (blocking)
        self.producer = create_kafka_producer(
            bootstrap_servers=settings.database.kafka_servers,
            client_id=settings.observability.service_name or "connector-trigger-service",
            retry_backoff_ms=100,
            linger_ms=20,
            enable_idempotence=True,
            max_in_flight_requests_per_connection=5,
            producer_ctor=Producer,
        )
        self.producer_ops = ExecutorKafkaProducerOps(
            self.producer,
            thread_name_prefix="connector-trigger-service-kafka-producer",
        )

        logger.info(
            "✅ ConnectorTriggerService initialized "
            f"(source_types={','.join(self.source_types)}, topic={self.topic}, tick={self.tick_seconds}s)"
        )

    async def close(self) -> None:
        if self.sheets:
            try:
                await self.sheets.close()
            except Exception:
                logger.warning("Failed to close GoogleSheetsService", exc_info=True)
            self.sheets = None
        self.adapter_factory = None
        ops = self.producer_ops
        producer = self.producer
        if ops:
            try:
                await ops.close(timeout_s=5.0)
            except Exception:
                logger.warning("Failed to flush Kafka producer", exc_info=True)
        elif producer:
            try:
                producer.flush(5.0)
            except Exception:
                logger.warning("Failed to flush Kafka producer", exc_info=True)
        self.producer_ops = None
        self.producer = None
        if self.registry:
            try:
                await self.registry.close()
            except Exception:
                logger.warning("Failed to close ConnectorRegistry", exc_info=True)
            self.registry = None

    def _get_producer_ops(self) -> KafkaProducerOps:
        ops = self.producer_ops
        if ops is not None:
            return ops
        producer = self.producer
        if producer is None:
            raise RuntimeError("ConnectorTriggerService producer not configured")
        ops = InlineKafkaProducerOps(producer)
        self.producer_ops = ops
        return ops

    async def _is_due(self, source: ConnectorSource) -> bool:
        if not self.registry:
            return False
        cfg = source.config_json or {}
        interval = int(cfg.get("polling_interval") or 300)
        state = await self.registry.get_sync_state(source_type=source.source_type, source_id=source.source_id)
        if not state or not state.last_polled_at:
            return True
        elapsed = (utcnow() - state.last_polled_at).total_seconds()
        return elapsed >= max(1, interval)

    async def _poll_with_adapter(self, source: ConnectorSource) -> None:
        if not self.registry or not self.adapter_factory:
            return
        connector_kind = connector_kind_from_source_type(source.source_type)
        adapter = self.adapter_factory.get_adapter(connector_kind)
        config, secrets = await resolve_source_runtime_credentials(
            connector_registry=self.registry,
            source_type=source.source_type,
            source_config=dict(source.config_json or {}),
        )
        import_config = (
            source.config_json.get("table_import_config")
            if isinstance((source.config_json or {}).get("table_import_config"), dict)
            else None
        )
        cursor = await adapter.peek_change_token(
            config=config,
            secrets=secrets,
            import_config=import_config,
        )
        envelope = await self.registry.record_poll_result(
            source_type=source.source_type,
            source_id=source.source_id,
            current_cursor=cursor,
            kafka_topic=self.topic,
        )
        if envelope:
            logger.info(
                "🔔 Change detected and enqueued (source=%s:%s, seq=%s, event_id=%s)",
                source.source_type,
                source.source_id,
                envelope.sequence_number,
                envelope.event_id,
            )

    async def _poll_source(self, source: ConnectorSource, sem: asyncio.Semaphore) -> None:
        async with sem:
            try:
                with self.tracing.span(
                    "connector_trigger.poll",
                    attributes={
                        "connector.source_type": str(source.source_type),
                        "connector.source_id": str(source.source_id),
                    },
                ):
                    await self._poll_with_adapter(source)
            except Exception as e:
                logger.error("Polling error (source=%s:%s): %s", source.source_type, source.source_id, e)

    async def _poll_loop(self) -> None:
        if not self.registry:
            raise RuntimeError("Trigger service not initialized")

        sem = asyncio.Semaphore(self.poll_concurrency)
        while self.running:
            try:
                sources: list[ConnectorSource] = []
                for source_type in self.source_types:
                    sources.extend(await self.registry.list_sources(source_type=source_type, enabled=True, limit=2000))
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

        producer_ops = self._get_producer_ops()
        while self.running:
            try:
                batch = await self.registry.claim_outbox_batch(limit=self.outbox_batch)
                if not batch:
                    await asyncio.sleep(1)
                    continue

                with self.tracing.span(
                    "connector_trigger.outbox_batch",
                    attributes={
                        "messaging.system": "kafka",
                        "messaging.destination": self.topic,
                        "connector.outbox_batch_limit": self.outbox_batch,
                        "connector.outbox_count": len(batch),
                    },
                ):

                    batch_start = time.monotonic()

                    delivery_errors: dict[str, str] = {}

                    def _cb(err, msg, outbox_id: str):
                        if err is not None:
                            delivery_errors[outbox_id] = str(err)

                    for item in batch:
                        carrier = carrier_from_envelope_metadata(item.payload)
                        with attach_context_from_carrier(carrier or None, service_name="connector-trigger-service"):
                            attrs = {
                                "messaging.system": "kafka",
                                "messaging.destination": self.topic,
                                "connector.outbox_id": item.outbox_id,
                                "connector.event_id": item.event_id,
                                "connector.source_type": item.source_type,
                                "connector.source_id": item.source_id,
                            }
                            if item.sequence_number is not None:
                                attrs["connector.sequence_number"] = int(item.sequence_number)

                            with self.tracing.span("connector_trigger.outbox_produce", attributes=attrs):
                                headers = (
                                    kafka_headers_from_envelope_metadata(item.payload)
                                    or kafka_headers_from_current_context()
                                )
                                value = json.dumps(item.payload, ensure_ascii=False).encode("utf-8")
                                key = f"{item.source_type}:{item.source_id}".encode("utf-8")
                                await producer_ops.produce(
                                    topic=self.topic,
                                    value=value,
                                    key=key,
                                    headers=headers or None,
                                    on_delivery=lambda err, msg, outbox_id=item.outbox_id: _cb(err, msg, outbox_id),
                                )
                                try:
                                    self.metrics.record_event("CONNECTOR_UPDATE", action="published")
                                except Exception as exc:
                                    logger.warning("Failed to record connector publish metric: %s", exc, exc_info=True)

                    remaining = await producer_ops.flush(10)
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

                    try:
                        self.metrics.record_event(
                            "CONNECTOR_OUTBOX_BATCH",
                            action="processed",
                            duration=time.monotonic() - batch_start,
                        )
                    except Exception as exc:
                        logger.warning("Failed to record connector outbox batch metric: %s", exc, exc_info=True)

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
            # Ensure cancellations are fully processed so we don't leak background tasks.
            await asyncio.gather(poll_task, publish_task, return_exceptions=True)


if __name__ == "__main__":
    configure_logging(get_settings().observability.log_level)
    asyncio.run(run_component_lifecycle(ConnectorTriggerService()))
