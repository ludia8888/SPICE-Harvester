"""Worker store bootstrap helpers (Facade).

Several workers share the same optional runtime stores:
- ProcessedEventRegistry (required for idempotency + ordering)
- LineageStore (best-effort)
- AuditLogStore (best-effort)

This module centralizes that bootstrap flow to reduce duplication across
workers while keeping failure semantics explicit.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

from shared.config.settings import get_settings
from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.errors.runtime_exception_policy import (
    RuntimeZone,
    assert_lineage_available,
    log_exception_rate_limited,
    record_lineage_or_raise,
)
from shared.services.core.audit_log_store import AuditLogStore, create_audit_log_store
from shared.services.registries.lineage_store import LineageStore, create_lineage_store
from shared.services.registries.processed_event_registry import ProcessedEventRegistry
from shared.services.registries.processed_event_registry_factory import create_processed_event_registry


@dataclass(slots=True)
class WorkerStores:
    processed: ProcessedEventRegistry
    lineage_store: Optional[LineageStore]
    audit_store: Optional[AuditLogStore]
    lineage_required: bool


@dataclass(slots=True)
class WorkerObservability:
    """
    Facade over optional provenance stores used by many workers.

    Lineage is fail-closed by default (`LINEAGE_FAIL_CLOSED=true`) and can be
    temporarily relaxed via `LINEAGE_FAIL_OPEN_OVERRIDE=true`.
    Audit logging remains best-effort.
    """

    lineage_store: Optional[LineageStore]
    audit_store: Optional[AuditLogStore]
    logger: logging.Logger
    lineage_required: bool = False

    async def record_link(self, **kwargs: Any) -> None:
        async def _record() -> None:
            await self.lineage_store.record_link(**kwargs)  # type: ignore[union-attr]

        await record_lineage_or_raise(
            lineage_store=self.lineage_store,
            required=self.lineage_required,
            record_call=_record,
            logger=self.logger,
            operation="worker_observability.record_link",
            zone=RuntimeZone.OBSERVABILITY,
            context={
                "edge_type": kwargs.get("edge_type"),
                "from_node_id": kwargs.get("from_node_id"),
                "to_node_id": kwargs.get("to_node_id"),
            },
        )

    async def audit_log(self, **kwargs: Any) -> None:
        if not self.audit_store:
            return
        try:
            await self.audit_store.log(**kwargs)
        except Exception as exc:
            self.logger.debug("Audit record failed (non-fatal): %s", exc)


async def initialize_worker_stores(
    *,
    enable_lineage: bool,
    enable_audit_logs: bool,
    logger: logging.Logger,
) -> WorkerStores:
    settings = get_settings()
    observability = settings.observability
    lineage_required = bool(observability.lineage_required_effective and enable_lineage)

    processed = await create_processed_event_registry()
    logger.info("ProcessedEventRegistry connected (Postgres)")

    lineage_store: Optional[LineageStore] = None
    if enable_lineage:
        try:
            lineage_store = create_lineage_store(settings)
            await lineage_store.initialize()
            logger.info("LineageStore connected (Postgres)")
        except Exception as exc:
            if lineage_required:
                raise
            log_exception_rate_limited(
                logger,
                zone=RuntimeZone.OBSERVABILITY,
                operation="initialize_worker_stores.lineage",
                exc=exc,
                code=ErrorCode.LINEAGE_UNAVAILABLE,
                category=ErrorCategory.UPSTREAM,
            )
            lineage_store = None

    if lineage_required:
        assert_lineage_available(
            lineage_store=lineage_store,
            required=True,
            logger=logger,
            operation="initialize_worker_stores.lineage_required",
            zone=RuntimeZone.OBSERVABILITY,
        )

    audit_store: Optional[AuditLogStore] = None
    if enable_audit_logs:
        try:
            audit_store = create_audit_log_store(settings)
            await audit_store.initialize()
            logger.info("AuditLogStore connected (Postgres)")
        except Exception as exc:
            logger.warning("AuditLogStore unavailable (continuing without audit logs): %s", exc)
            audit_store = None

    return WorkerStores(
        processed=processed,
        lineage_store=lineage_store,
        audit_store=audit_store,
        lineage_required=lineage_required,
    )
