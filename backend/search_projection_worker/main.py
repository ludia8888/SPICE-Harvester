"""
Search Projection Worker
Consumes instance domain events and indexes them into Elasticsearch/OpenSearch.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import KafkaError, Producer, TopicPartition

from shared.services.kafka.safe_consumer import SafeKafkaConsumer, ConsumerState
from elasticsearch.exceptions import ApiError as ElasticsearchException, RequestError, ConnectionError as ESConnectionError

from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.models.event_envelope import EventEnvelope
from shared.observability.context_propagation import (
    attach_context_from_kafka,
    kafka_headers_from_envelope_metadata,
)
from shared.observability.logging import install_trace_context_filter
from shared.observability.metrics import get_metrics_collector
from shared.observability.tracing import get_tracing_service
from shared.services.storage.elasticsearch_service import create_elasticsearch_service_legacy
from shared.services.registries.processed_event_registry import ClaimDecision, ProcessedEventRegistry

logger = logging.getLogger(__name__)


class SearchProjectionWorker:
    def __init__(self) -> None:
        settings = get_settings()
        cfg = settings.workers.search_projection
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
        self.consumer: Optional[Consumer] = None
        self.dlq_producer: Optional[Producer] = None
        self.processed: Optional[ProcessedEventRegistry] = None
        self.es = None
        self.tracing = get_tracing_service("search-projection-worker")
        self.metrics = get_metrics_collector("search-projection-worker")

    async def initialize(self) -> None:
        if not self.enabled:
            logger.info("Search projection disabled; worker will not start.")
            return
        settings = get_settings()
        self.es = create_elasticsearch_service_legacy()
        await self.es.connect()
        self.processed = ProcessedEventRegistry()
        await self.processed.initialize()
        try:
            exists = await self.es.index_exists(self.index_name)
            if not exists:
                await self.es.create_index(self.index_name)
        except Exception as exc:
            logger.warning("Failed to ensure search index exists: %s", exc)

        # Use SafeKafkaConsumer for strong consistency guarantees
        self.consumer = SafeKafkaConsumer(
            group_id=self.group_id,
            topics=[self.topic],
            service_name="search-projection-worker",
            max_poll_interval_ms=300000,
            session_timeout_ms=45000,
            on_revoke=self._on_partitions_revoked,
            on_assign=self._on_partitions_assigned,
        )
        self._rebalance_in_progress = False

        service_name = settings.observability.service_name or "search-projection-worker"
        self.dlq_producer = Producer(
            {
                "bootstrap.servers": settings.database.kafka_servers,
                "client.id": service_name,
                "acks": "all",
                "retries": 3,
                "retry.backoff.ms": 100,
                "linger.ms": 20,
                "compression.type": "snappy",
            }
        )

    def _on_partitions_revoked(self, partitions: list) -> None:
        """Handle partition revocation during rebalance."""
        self._rebalance_in_progress = True
        logger.info(
            "Search projection worker partitions revoked: %s",
            [(p.topic, p.partition) for p in partitions],
        )

    def _on_partitions_assigned(self, partitions: list) -> None:
        """Handle partition assignment during rebalance."""
        self._rebalance_in_progress = False
        logger.info(
            "Search projection worker partitions assigned: %s",
            [(p.topic, p.partition) for p in partitions],
        )

    async def close(self) -> None:
        if self.consumer:
            self.consumer.close()
            self.consumer = None
        if self.dlq_producer:
            try:
                self.dlq_producer.flush(5)
            except Exception as exc:
                logger.warning("DLQ producer flush failed during shutdown: %s", exc, exc_info=True)
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
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    await asyncio.sleep(0)
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Kafka error: %s", msg.error())
                    continue

                payload = msg.value()
                if not payload:
                    continue
                raw_text: Optional[str] = None
                envelope: Optional[EventEnvelope] = None
                try:
                    raw_text = payload.decode("utf-8", errors="replace") if isinstance(payload, (bytes, bytearray)) else None
                    envelope = EventEnvelope.model_validate_json(payload)
                except Exception as exc:
                    logger.exception("Invalid event envelope; skipping: %s", exc)
                    self.consumer.commit_sync(msg)
                    continue

                claim = None
                heartbeat_task: Optional[asyncio.Task] = None
                try:
                    if not self.processed:
                        raise RuntimeError("ProcessedEventRegistry not initialized")

                    kafka_headers = msg.headers()
                    start = time.monotonic()
                    with attach_context_from_kafka(
                        kafka_headers=kafka_headers,
                        fallback_metadata=envelope.metadata if isinstance(envelope.metadata, dict) else None,
                        service_name="search-projection-worker",
                    ):
                        with self.tracing.span(
                            "search_projection.process_event",
                            attributes={
                                "messaging.system": "kafka",
                                "messaging.destination": msg.topic(),
                                "messaging.destination_kind": "topic",
                                "messaging.kafka.partition": msg.partition(),
                                "messaging.kafka.offset": msg.offset(),
                                "event.id": str(envelope.event_id),
                                "event.type": str(envelope.event_type),
                                "event.aggregate_type": str(envelope.aggregate_type),
                                "event.aggregate_id": str(envelope.aggregate_id or ""),
                            },
                        ):
                            claim = await self.processed.claim(
                                handler=self.handler,
                                event_id=str(envelope.event_id),
                                aggregate_id=str(envelope.aggregate_id or ""),
                                sequence_number=envelope.sequence_number,
                            )
                            if claim.decision in {ClaimDecision.DUPLICATE_DONE, ClaimDecision.STALE}:
                                self.consumer.commit_sync(msg)
                                continue
                            if claim.decision == ClaimDecision.IN_PROGRESS:
                                await asyncio.sleep(2)
                                self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                                continue

                            heartbeat_task = asyncio.create_task(
                                self._heartbeat_loop(handler=self.handler, event_id=str(envelope.event_id))
                            )

                            await self._index_event(envelope)
                            if self.processed:
                                await self.processed.mark_done(
                                    handler=self.handler,
                                    event_id=str(envelope.event_id),
                                    aggregate_id=str(envelope.aggregate_id or ""),
                                    sequence_number=envelope.sequence_number,
                                )
                            self.consumer.commit_sync(msg)
                            try:
                                self.metrics.record_event(
                                    str(envelope.event_type),
                                    action="processed",
                                    duration=time.monotonic() - start,
                                )
                            except Exception:
                                pass
                except Exception as exc:
                    err = str(exc)
                    retryable = self._is_retryable_error(exc)
                    attempt_count = int(getattr(claim, "attempt_count", 1) or 1)

                    if self.processed and envelope:
                        try:
                            await self.processed.mark_failed(handler=self.handler, event_id=str(envelope.event_id), error=err)
                        except Exception as mark_err:
                            logger.warning("Failed to mark projection failed: %s", mark_err)

                    if not retryable:
                        attempt_count = self.max_retries

                    if attempt_count >= self.max_retries:
                        logger.error(
                            "Search projection max retries exceeded; sending to DLQ (event_id=%s attempt=%s)",
                            envelope.event_id if envelope else None,
                            attempt_count,
                        )
                        await self._send_to_dlq(
                            envelope=envelope,
                            raw_payload=raw_text,
                            error=err,
                            attempt_count=attempt_count,
                        )
                        self.consumer.commit_sync(msg)
                        continue

                    backoff_s = min(self.backoff_max, int(self.backoff_base * (2 ** max(0, attempt_count - 1))))
                    logger.warning(
                        "Search projection failed; will retry (event_id=%s attempt=%s backoff=%ss): %s",
                        envelope.event_id if envelope else None,
                        attempt_count,
                        backoff_s,
                        err,
                    )
                    await asyncio.sleep(backoff_s)
                    self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                finally:
                    if heartbeat_task:
                        heartbeat_task.cancel()
                        try:
                            await heartbeat_task
                        except asyncio.CancelledError:
                            pass
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

    async def _heartbeat_loop(self, *, handler: str, event_id: str) -> None:
        if not self.processed:
            return
        interval = int(get_settings().event_sourcing.processed_event_heartbeat_interval_seconds)
        while True:
            try:
                await asyncio.sleep(interval)
                await self.processed.heartbeat(handler=handler, event_id=event_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Search projection heartbeat failed (handler=%s event_id=%s): %s", handler, event_id, exc)

    async def _send_to_dlq(
        self,
        *,
        envelope: Optional[EventEnvelope],
        raw_payload: Optional[str],
        error: str,
        attempt_count: int,
    ) -> None:
        if not self.dlq_producer:
            logger.error("DLQ producer not configured; dropping search projection DLQ payload: %s", error)
            return

        if envelope:
            dlq_env = envelope.model_copy(deep=True)
            if not isinstance(dlq_env.metadata, dict):
                dlq_env.metadata = {}
            dlq_env.metadata.update(
                {
                    "kind": "search_projection_dlq",
                    "dlq_error": (error or "").strip()[:4000],
                    "dlq_attempt_count": int(attempt_count),
                    "dlq_topic": self.dlq_topic,
                }
            )
            dlq_env.event_type = "SEARCH_PROJECTION_FAILED"
            key = str(dlq_env.aggregate_id or dlq_env.event_id or "search_projection").encode("utf-8")
            value = dlq_env.model_dump_json().encode("utf-8")
        else:
            payload = {
                "kind": "search_projection_dlq",
                "error": error,
                "attempt_count": int(attempt_count),
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "raw_payload": raw_payload,
            }
            key = b"search_projection"
            value = json.dumps(payload, ensure_ascii=True).encode("utf-8")

        dlq_metadata = dlq_env.metadata if envelope else None
        headers = kafka_headers_from_envelope_metadata(dlq_metadata) if isinstance(dlq_metadata, dict) else []
        with attach_context_from_kafka(
            kafka_headers=headers,
            fallback_metadata=dlq_metadata if isinstance(dlq_metadata, dict) else None,
            service_name="search-projection-worker",
        ):
            with self.tracing.span(
                "search_projection.dlq_produce",
                attributes={
                    "messaging.system": "kafka",
                    "messaging.destination": self.dlq_topic,
                    "messaging.destination_kind": "topic",
                    "event.id": str(envelope.event_id) if envelope else None,
                },
            ):
                self.dlq_producer.produce(self.dlq_topic, key=key, value=value, headers=headers or None)
                self.dlq_producer.flush(10)

    @staticmethod
    def _is_retryable_error(exc: Exception) -> bool:
        if isinstance(exc, (ESConnectionError, asyncio.TimeoutError)):
            return True
        if isinstance(exc, RequestError):
            return False
        if isinstance(exc, ElasticsearchException):
            status = getattr(exc, "status_code", None)
            if status == 429 or (status is not None and status >= 500):
                return True
            return False
        msg = str(exc).lower()
        non_retryable_markers = [
            "mapper_parsing_exception",
            "document_parsing_exception",
            "illegal_argument_exception",
            "validation",
            "bad request",
        ]
        return not any(marker in msg for marker in non_retryable_markers)


async def main() -> None:
    log_level = get_settings().observability.log_level
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - trace_id=%(trace_id)s span_id=%(span_id)s req_id=%(request_id)s corr_id=%(correlation_id)s db=%(db_name)s - %(message)s",
    )
    install_trace_context_filter()
    worker = SearchProjectionWorker()
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
