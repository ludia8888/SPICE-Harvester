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
from typing import Optional

from shared.config.settings import get_settings
from shared.services.core.audit_log_store import AuditLogStore, create_audit_log_store
from shared.services.registries.lineage_store import LineageStore, create_lineage_store
from shared.services.registries.processed_event_registry import ProcessedEventRegistry
from shared.services.registries.processed_event_registry_factory import create_processed_event_registry


@dataclass(slots=True)
class WorkerStores:
    processed: ProcessedEventRegistry
    lineage_store: Optional[LineageStore]
    audit_store: Optional[AuditLogStore]


async def initialize_worker_stores(
    *,
    enable_lineage: bool,
    enable_audit_logs: bool,
    logger: logging.Logger,
) -> WorkerStores:
    processed = await create_processed_event_registry()
    logger.info("ProcessedEventRegistry connected (Postgres)")

    lineage_store: Optional[LineageStore] = None
    if enable_lineage:
        try:
            lineage_store = create_lineage_store(get_settings())
            await lineage_store.initialize()
            logger.info("LineageStore connected (Postgres)")
        except Exception as exc:
            logger.warning("LineageStore unavailable (continuing without lineage): %s", exc)
            lineage_store = None

    audit_store: Optional[AuditLogStore] = None
    if enable_audit_logs:
        try:
            audit_store = create_audit_log_store(get_settings())
            await audit_store.initialize()
            logger.info("AuditLogStore connected (Postgres)")
        except Exception as exc:
            logger.warning("AuditLogStore unavailable (continuing without audit logs): %s", exc)
            audit_store = None

    return WorkerStores(
        processed=processed,
        lineage_store=lineage_store,
        audit_store=audit_store,
    )
