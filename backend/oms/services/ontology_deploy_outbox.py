from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from shared.config.settings import get_settings
from shared.services.events.outbox_runtime import (
    build_outbox_worker_id,
    maybe_purge_with_interval,
    run_outbox_poll_loop,
)
from shared.services.storage.event_store import event_store
from shared.models.event_envelope import EventEnvelope
from shared.observability.tracing import trace_external_call
from shared.utils.backoff_utils import next_exponential_backoff_at

from oms.services.ontology_deployment_registry import OntologyDeploymentRegistry

logger = logging.getLogger(__name__)


class OntologyDeployOutboxPublisher:
    def __init__(
        self,
        *,
        registry: OntologyDeploymentRegistry,
        batch_size: int = 50,
    ) -> None:
        settings = get_settings()
        cfg = settings.workers.ontology_deploy_outbox
        self.registry = registry
        self.batch_size = batch_size
        self.claim_timeout_seconds = int(cfg.claim_timeout_seconds)
        self.backoff_base = int(cfg.backoff_base_seconds)
        self.backoff_max = int(cfg.backoff_max_seconds)
        self.retention_days = int(cfg.retention_days)
        self.purge_interval_seconds = int(cfg.purge_interval_seconds)
        self.purge_limit = int(cfg.purge_limit)

        self.worker_id = build_outbox_worker_id(
            configured_worker_id=cfg.worker_id,
            service_name=settings.observability.service_name,
            hostname=settings.observability.hostname,
            default_service_name="ontology-deploy-outbox",
        )
        self._last_purge = datetime.now(timezone.utc)

    @trace_external_call("oms.deploy_outbox.flush_once")
    async def flush_once(self) -> None:
        await event_store.connect()
        batch = await self.registry.claim_outbox_batch(
            limit=self.batch_size,
            claimed_by=self.worker_id,
            claim_timeout_seconds=self.claim_timeout_seconds,
        )
        if not batch:
            return
        for item in batch:
            try:
                payload = item.payload or {}
                event = EventEnvelope(
                    event_id=str(payload.get("event_id") or item.deployment_id),
                    event_type=str(payload.get("event_type") or "ONTOLOGY_DEPLOYED"),
                    aggregate_type=str(payload.get("aggregate_type") or "OntologyDeployment"),
                    aggregate_id=str(payload.get("aggregate_id") or item.deployment_id),
                    occurred_at=payload.get("occurred_at") or datetime.now(timezone.utc),
                    actor=payload.get("actor"),
                    data=payload.get("data") or {},
                    metadata=payload.get("metadata") or {},
                    schema_version=str(payload.get("schema_version") or "1"),
                )
                await event_store.append_event(event)
                await self.registry.mark_outbox_published(outbox_id=item.outbox_id)
            except Exception as exc:
                attempts = int(item.publish_attempts) + 1
                await self.registry.mark_outbox_failed(
                    outbox_id=item.outbox_id,
                    error=str(exc),
                    next_attempt_at=next_exponential_backoff_at(
                        attempts,
                        base_seconds=self.backoff_base,
                        max_seconds=self.backoff_max,
                    ),
                )

    @trace_external_call("oms.deploy_outbox.maybe_purge")
    async def maybe_purge(self) -> None:
        self._last_purge = await maybe_purge_with_interval(
            retention_days=self.retention_days,
            purge_interval_seconds=self.purge_interval_seconds,
            purge_limit=self.purge_limit,
            last_purge=self._last_purge,
            purge_call=self.registry.purge_outbox,
            info_logger=logger.info,
            warning_logger=logger.warning,
            success_message="Purged %s ontology deploy outbox rows",
            failure_message="Failed to purge ontology deploy outbox rows: %s",
        )


async def run_ontology_deploy_outbox_worker(
    *,
    registry: OntologyDeploymentRegistry,
    poll_interval_seconds: int = 5,
    batch_size: int = 50,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    publisher = OntologyDeployOutboxPublisher(registry=registry, batch_size=batch_size)
    await run_outbox_poll_loop(
        publisher=publisher,
        poll_interval_seconds=poll_interval_seconds,
        stop_event=stop_event,
        warning_logger=logger.warning,
        failure_message="Ontology deploy outbox worker failed: %s",
    )
