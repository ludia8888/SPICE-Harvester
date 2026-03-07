"""
Objectify job queue publisher using a Postgres outbox.
"""

from __future__ import annotations

import logging
from typing import Optional

from shared.models.objectify_job import ObjectifyJob
from shared.services.registries.objectify_registry import ObjectifyJobEnqueueResult, ObjectifyRegistry

logger = logging.getLogger(__name__)


class ObjectifyJobQueue:
    def __init__(self, *, objectify_registry: Optional[ObjectifyRegistry] = None) -> None:
        self._registry = objectify_registry
        self._owns_registry = False

    async def _get_registry(self) -> ObjectifyRegistry:
        if self._registry is None:
            self._registry = ObjectifyRegistry()
            await self._registry.initialize()
            self._owns_registry = True
        return self._registry

    async def close(self) -> None:
        if self._owns_registry and self._registry is not None:
            await self._registry.close()
        self._registry = None
        self._owns_registry = False

    async def publish(self, job: ObjectifyJob, *, require_delivery: bool = True) -> ObjectifyJobEnqueueResult:
        _ = require_delivery
        registry = await self._get_registry()
        result = await registry.get_or_enqueue_objectify_job(job=job)
        logger.info(
            "Enqueued objectify job %s via outbox (created=%s, resolved_job_id=%s)",
            job.job_id,
            result.created,
            result.record.job_id,
        )
        return result
