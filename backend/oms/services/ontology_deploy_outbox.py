from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from shared.services.event_store import event_store
from shared.models.event_envelope import EventEnvelope
from shared.utils.env_utils import parse_int_env

from oms.services.ontology_deployment_registry import OntologyDeploymentRegistry

logger = logging.getLogger(__name__)


class OntologyDeployOutboxPublisher:
    def __init__(
        self,
        *,
        registry: OntologyDeploymentRegistry,
        batch_size: int = 50,
    ) -> None:
        self.registry = registry
        self.batch_size = batch_size
        self.claim_timeout_seconds = parse_int_env(
            "ONTOLOGY_DEPLOY_OUTBOX_CLAIM_TIMEOUT_SECONDS",
            300,
            min_value=0,
            max_value=86_400,
        )
        self.backoff_base = parse_int_env(
            "ONTOLOGY_DEPLOY_OUTBOX_BACKOFF_BASE_SECONDS",
            2,
            min_value=1,
            max_value=300,
        )
        self.backoff_max = parse_int_env(
            "ONTOLOGY_DEPLOY_OUTBOX_BACKOFF_MAX_SECONDS",
            60,
            min_value=1,
            max_value=3600,
        )
        self.retention_days = parse_int_env(
            "ONTOLOGY_DEPLOY_OUTBOX_RETENTION_DAYS",
            7,
            min_value=0,
            max_value=365,
        )
        self.purge_interval_seconds = parse_int_env(
            "ONTOLOGY_DEPLOY_OUTBOX_PURGE_INTERVAL_SECONDS",
            3600,
            min_value=300,
            max_value=86_400,
        )
        self.purge_limit = parse_int_env(
            "ONTOLOGY_DEPLOY_OUTBOX_PURGE_LIMIT",
            10_000,
            min_value=1,
            max_value=100_000,
        )
        self.worker_id = (
            os.getenv("ONTOLOGY_DEPLOY_OUTBOX_WORKER_ID")
            or f"{os.getenv('SERVICE_NAME') or 'ontology-deploy-outbox'}:{os.getenv('HOSTNAME') or 'local'}:{os.getpid()}"
        )
        self._last_purge = datetime.now(timezone.utc)

    def _next_attempt_at(self, attempts: int) -> datetime:
        delay = min(self.backoff_max, self.backoff_base * (2 ** max(0, attempts - 1)))
        return datetime.now(timezone.utc) + timedelta(seconds=delay)

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
                    next_attempt_at=self._next_attempt_at(attempts),
                )

    async def maybe_purge(self) -> None:
        if self.retention_days <= 0:
            return
        now = datetime.now(timezone.utc)
        if (now - self._last_purge).total_seconds() < self.purge_interval_seconds:
            return
        self._last_purge = now
        try:
            deleted = await self.registry.purge_outbox(
                retention_days=self.retention_days,
                limit=self.purge_limit,
            )
            if deleted:
                logger.info("Purged %s ontology deploy outbox rows", deleted)
        except Exception as exc:
            logger.warning("Failed to purge ontology deploy outbox rows: %s", exc)


async def run_ontology_deploy_outbox_worker(
    *,
    registry: OntologyDeploymentRegistry,
    poll_interval_seconds: int = 5,
    batch_size: int = 50,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    stop_event = stop_event or asyncio.Event()
    publisher = OntologyDeployOutboxPublisher(registry=registry, batch_size=batch_size)
    while not stop_event.is_set():
        try:
            await publisher.flush_once()
            await publisher.maybe_purge()
        except Exception as exc:
            logger.warning("Ontology deploy outbox worker failed: %s", exc)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=poll_interval_seconds)
        except asyncio.TimeoutError:
            continue
