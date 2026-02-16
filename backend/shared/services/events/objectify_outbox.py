from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional


from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.observability.context_propagation import (
    attach_context_from_carrier,
    carrier_from_envelope_metadata,
    kafka_headers_with_dedup,
)
from shared.observability.tracing import get_tracing_service
from shared.services.registries.objectify_registry import ObjectifyOutboxItem, ObjectifyRegistry
from shared.services.events.outbox_runtime import (
    build_outbox_worker_id,
    maybe_purge_with_interval,
    run_outbox_poll_loop,
)
from shared.services.kafka.producer_factory import create_kafka_producer
from shared.utils.backoff_utils import next_exponential_backoff_at

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

        self.worker_id = build_outbox_worker_id(
            configured_worker_id=cfg.worker_id,
            service_name=settings.observability.service_name,
            hostname=settings.observability.hostname,
            default_service_name="objectify-outbox",
        )
        self._last_purge = datetime.now(timezone.utc)
        max_in_flight = int(cfg.producer_max_in_flight)
        delivery_timeout_ms = int(cfg.producer_delivery_timeout_ms)
        request_timeout_ms = int(cfg.producer_request_timeout_ms)
        if delivery_timeout_ms <= request_timeout_ms:
            delivery_timeout_ms = request_timeout_ms + 1_000
        retries = int(cfg.producer_retries)
        client_id = (settings.observability.service_name or "objectify-outbox").strip() or "objectify-outbox"

        self.producer = create_kafka_producer(
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
        self.tracing = get_tracing_service("objectify-outbox")

    async def close(self) -> None:
        try:
            await asyncio.to_thread(self.producer.flush, self.flush_timeout_seconds)
        except Exception as exc:
            logger.warning("Kafka producer flush failed during shutdown: %s", exc, exc_info=True)

    async def _publish_batch(self, batch: list[ObjectifyOutboxItem]) -> None:
        """
        Publish a batch of outbox items with atomic delivery tracking.

        Critical Fix (Gap 3): Track individual message delivery status via callbacks
        to ensure we only mark messages as published when Kafka broker confirms receipt.
        """
        if not batch:
            return

        # Track delivery status for each message (success or error)
        # Using dict to store either None (success) or error string
        delivery_results: dict[str, Optional[str]] = {}
        pending_count = len(batch)

        def _on_delivery(err, _msg, outbox_id: str) -> None:
            nonlocal pending_count
            if err is not None:
                delivery_results[outbox_id] = str(err)
            else:
                # Explicitly track successful delivery
                delivery_results[outbox_id] = None
            pending_count -= 1

        for item in batch:
            payload = json.dumps(item.payload, ensure_ascii=False, default=str).encode("utf-8")
            # Ordering contract: partition by db+branch to preserve dependency order across
            # multiple datasets within the same database/branch (enterprise DAG ingest).
            #
            # Enterprise evolution (Foundry-style):
            # - If a run_id is present, scope ordering to that run to preserve throughput.
            # - Otherwise fall back to db+branch ordering for compatibility callers.
            aggregate_id = None
            if isinstance(item.payload, dict):
                db_name = str(item.payload.get("db_name") or "").strip()
                # NOTE: "branch" here must match the target branch used by objectify/instances.
                # Objectify writes to `ontology_branch` when set, otherwise `dataset_branch`.
                branch = str(
                    item.payload.get("ontology_branch")
                    or item.payload.get("dataset_branch")
                    or "main"
                ).strip() or "main"
                if db_name:
                    aggregate_id = f"{db_name}:{branch}"
            ordering_key = aggregate_id
            if isinstance(item.payload, dict):
                options = item.payload.get("options") if isinstance(item.payload.get("options"), dict) else {}
                run_id = (
                    str(options.get("run_id") or item.payload.get("run_id") or "").strip()
                    if isinstance(options, dict)
                    else str(item.payload.get("run_id") or "").strip()
                )
                target_class_id = str(item.payload.get("target_class_id") or "").strip()
                if run_id and aggregate_id:
                    # Foundry-style: preserve order within a run *per class* to allow
                    # DAG-level parallelism across independent classes, while keeping
                    # db+branch+run_id scoped ordering for each class.
                    if target_class_id:
                        ordering_key = f"{aggregate_id}:{run_id}:{target_class_id}"
                    else:
                        ordering_key = f"{aggregate_id}:{run_id}"
            key = (ordering_key or aggregate_id or item.job_id).encode("utf-8")
            # Use kafka_headers_with_dedup for idempotent message processing
            headers = kafka_headers_with_dedup(
                item.payload,
                event_id=item.job_id,
                aggregate_id=aggregate_id,
            )
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
                        on_delivery=lambda err, msg, oid=item.outbox_id: _on_delivery(err, msg, oid),
                    )

        # Flush and poll until all callbacks are received or timeout
        remaining = await asyncio.to_thread(self.producer.flush, self.flush_timeout_seconds)

        # Additional poll to ensure all callbacks are processed
        if pending_count > 0:
            await asyncio.to_thread(self.producer.poll, 1.0)

        if remaining != 0:
            logger.warning("Objectify outbox flush incomplete (remaining=%s)", remaining)

        # Process each message based on its delivery callback result
        for item in batch:
            outbox_id = item.outbox_id

            if outbox_id in delivery_results:
                err = delivery_results[outbox_id]
                if err is None:
                    # Callback confirmed successful delivery
                    await self.registry.mark_objectify_outbox_published(
                        outbox_id=outbox_id,
                        job_id=item.job_id,
                    )
                else:
                    # Callback returned an error
                    attempts = int(item.publish_attempts) + 1
                    await self.registry.mark_objectify_outbox_failed(
                        outbox_id=outbox_id,
                        error=err,
                        next_attempt_at=next_exponential_backoff_at(
                            attempts,
                            base_seconds=self.backoff_base,
                            max_seconds=self.backoff_max,
                        ),
                    )
            else:
                # No callback received - delivery status unknown
                # Mark as failed to retry (safe approach)
                attempts = int(item.publish_attempts) + 1
                await self.registry.mark_objectify_outbox_failed(
                    outbox_id=outbox_id,
                    error="delivery callback not received; status unknown",
                    next_attempt_at=next_exponential_backoff_at(
                        attempts,
                        base_seconds=self.backoff_base,
                        max_seconds=self.backoff_max,
                    ),
                )
                logger.warning(
                    "Outbox item %s delivery callback not received after flush",
                    outbox_id,
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
        self._last_purge = await maybe_purge_with_interval(
            retention_days=self.retention_days,
            purge_interval_seconds=self.purge_interval_seconds,
            purge_limit=self.purge_limit,
            last_purge=self._last_purge,
            purge_call=self.registry.purge_objectify_outbox,
            info_logger=logger.info,
            warning_logger=logger.warning,
            success_message="Purged %s published objectify outbox rows",
            failure_message="Failed to purge objectify outbox rows: %s",
        )


async def run_objectify_outbox_worker(
    *,
    objectify_registry: ObjectifyRegistry,
    poll_interval_seconds: int = 5,
    batch_size: int = 50,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    publisher = ObjectifyOutboxPublisher(
        objectify_registry=objectify_registry,
        batch_size=batch_size,
    )
    await run_outbox_poll_loop(
        publisher=publisher,
        poll_interval_seconds=poll_interval_seconds,
        stop_event=stop_event,
        warning_logger=logger.warning,
        failure_message="Objectify outbox worker failed: %s",
    )
