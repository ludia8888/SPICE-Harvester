"""
Search Projection Worker
Consumes instance domain events and indexes them into Elasticsearch/OpenSearch.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from confluent_kafka import Producer

from elasticsearch.exceptions import ApiError as ElasticsearchException, RequestError, ConnectionError as ESConnectionError

from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.models.event_envelope import EventEnvelope
from shared.observability.metrics import get_metrics_collector
from shared.observability.tracing import get_tracing_service
from shared.services.kafka.dlq_publisher import (
    EnvelopeDlqSpec,
)
from shared.services.kafka.processed_event_worker import EventEnvelopeKafkaWorker, HeartbeatOptions
from shared.services.kafka.producer_factory import create_kafka_dlq_producer
from shared.services.kafka.producer_ops import ExecutorKafkaProducerOps, KafkaProducerOps, close_kafka_producer
from shared.services.kafka.retry_classifier import (
    SEARCH_PROJECTION_RETRY_PROFILE,
    classify_retryable_with_profile,
)
from shared.services.kafka.safe_consumer import SafeKafkaConsumer
from shared.services.storage.elasticsearch_service import create_elasticsearch_service_legacy
from shared.services.registries.processed_event_registry import ProcessedEventRegistry
from shared.services.registries.processed_event_registry_factory import create_processed_event_registry
from shared.utils.app_logger import configure_logging

logger = logging.getLogger(__name__)


class SearchProjectionWorker(EventEnvelopeKafkaWorker[None]):
    def __init__(self) -> None:
        settings = get_settings()
        cfg = settings.workers.search_projection
        self.service_name = "search-projection-worker"
        self.enabled = bool(cfg.enabled)
        self.topic = (AppConfig.INSTANCE_EVENTS_TOPIC or "").strip() or "instance_events"
        self.dlq_topic = (
            (AppConfig.SEARCH_PROJECTION_DLQ_TOPIC or "").strip()
            or (AppConfig.PROJECTION_DLQ_TOPIC or "").strip()
            or "projection_failures_dlq"
        )
        self.group_id = (AppConfig.SEARCH_PROJECTION_GROUP or "search-projection-worker").strip()
        self.handler = str(cfg.handler or "search_projection_worker").strip() or "search_projection_worker"
        self.index_name = str(cfg.index_name or "objects").strip() or "objects"
        self.max_retries = int(cfg.max_retries)
        self.backoff_base = int(cfg.backoff_base_seconds)
        self.backoff_max = int(cfg.backoff_max_seconds)
        self._dlq_spec = EnvelopeDlqSpec(
            dlq_topic=self.dlq_topic,
            service_name=self.service_name,
            kind="search_projection_dlq",
            failed_event_type="SEARCH_PROJECTION_FAILED",
            span_name="search_projection.dlq_produce",
        )
        self.consumer: Optional[SafeKafkaConsumer] = None
        self.consumer_ops = None
        self.dlq_producer: Optional[Producer] = None
        self.dlq_producer_ops: Optional[KafkaProducerOps] = None
        self.processed: Optional[ProcessedEventRegistry] = None
        self.es = None
        self.tracing = get_tracing_service(self.service_name)
        self.metrics = get_metrics_collector(self.service_name)

    async def initialize(self) -> None:
        if not self.enabled:
            logger.info("Search projection disabled; worker will not start.")
            return
        settings = get_settings()
        self.es = create_elasticsearch_service_legacy()
        await self.es.connect()
        self.processed = await create_processed_event_registry()
        try:
            exists = await self.es.index_exists(self.index_name)
            if not exists:
                await self.es.create_index(self.index_name)
        except Exception as exc:
            logger.warning("Failed to ensure search index exists: %s", exc)

        self._initialize_safe_consumer_runtime(
            group_id=self.group_id,
            topics=[self.topic],
            service_name="search-projection-worker",
            thread_name_prefix="search-projection-worker-kafka",
        )

        service_name = settings.observability.service_name or self.service_name
        self.dlq_producer = create_kafka_dlq_producer(
            bootstrap_servers=settings.database.kafka_servers,
            client_id=service_name,
        )
        self.dlq_producer_ops = ExecutorKafkaProducerOps(
            self.dlq_producer,
            thread_name_prefix="search-projection-worker-kafka-producer",
        )

    async def close(self) -> None:
        await self._close_consumer_runtime()
        await close_kafka_producer(
            producer_ops=self.dlq_producer_ops,
            timeout_s=5.0,
            warning_logger=logger,
            warning_message="DLQ producer flush failed during shutdown: %s",
        )
        self.dlq_producer_ops = None
        self.dlq_producer = None
        if self.processed:
            await self.processed.close()
            self.processed = None
        if self.es:
            await self.es.disconnect()
            self.es = None

    async def run(self) -> None:
        await self.initialize()
        if not self.enabled:
            return
        logger.info("SearchProjectionWorker started (topic=%s index=%s)", self.topic, self.index_name)
        try:
            self.running = True
            await self.run_loop(poll_timeout=1.0, idle_sleep=0.0, catch_exceptions=False)
        finally:
            await self.close()

    async def _index_event(self, envelope: EventEnvelope) -> None:
        if not self.es:
            return
        if envelope.event_type not in {"INSTANCE_CREATED", "INSTANCE_UPDATED", "INSTANCE_MERGED"}:
            return
        aggregate_id = str(envelope.aggregate_id or "")
        if not aggregate_id:
            return
        doc = {
            "event_type": envelope.event_type,
            "aggregate_type": envelope.aggregate_type,
            "aggregate_id": aggregate_id,
            "occurred_at": envelope.occurred_at.isoformat() if envelope.occurred_at else None,
            "data": envelope.data or {},
            "metadata": envelope.metadata or {},
        }
        await self.es.client.index(index=self.index_name, id=aggregate_id, document=doc)

    def _heartbeat_options(self) -> HeartbeatOptions:  # type: ignore[override]
        return HeartbeatOptions(
            warning_message="Search projection heartbeat failed (handler=%s event_id=%s): %s",
        )

    async def _send_to_dlq(  # type: ignore[override]
        self,
        *,
        msg: Any,
        payload: EventEnvelope,
        raw_payload: Optional[str],
        error: str,
        attempt_count: int,
    ) -> None:
        await self._send_envelope_failure_to_dlq(
            msg=msg,
            payload=payload,
            error=error,
            attempt_count=attempt_count,
            producer_ops=self.dlq_producer_ops,
            spec=self._dlq_spec,
            key_fallback="search_projection",
            missing_producer_message="DLQ producer not configured; dropping search projection DLQ payload: %s",
        )

    async def _process_payload(self, payload: EventEnvelope) -> None:  # type: ignore[override]
        await self._index_event(payload)

    def _span_name(self, *, payload: EventEnvelope) -> str:  # type: ignore[override]
        return "search_projection.process_event"

    def _is_retryable_error(self, exc: Exception, *, payload: EventEnvelope) -> bool:  # type: ignore[override]
        return self._is_retryable_error_impl(exc)

    @staticmethod
    def _is_retryable_error_impl(exc: Exception) -> bool:
        if isinstance(exc, (ESConnectionError, asyncio.TimeoutError)):
            return True
        if isinstance(exc, RequestError):
            return False
        if isinstance(exc, ElasticsearchException):
            status = getattr(exc, "status_code", None)
            if status == 429 or (status is not None and status >= 500):
                return True
            return False
        return classify_retryable_with_profile(exc, SEARCH_PROJECTION_RETRY_PROFILE)


async def main() -> None:
    configure_logging(get_settings().observability.log_level)
    worker = SearchProjectionWorker()
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
