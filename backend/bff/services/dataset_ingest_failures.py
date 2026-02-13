"""Shared helpers for dataset ingest workflows (BFF)."""

from __future__ import annotations

import logging
from typing import Any
from shared.observability.tracing import trace_db_operation

logger = logging.getLogger(__name__)


@trace_db_operation("bff.dataset_ingest_failures.mark_ingest_failed")
async def mark_ingest_failed(
    *,
    dataset_registry: Any,
    ingest_request: Any,
    error: str,
    stage: str,
) -> None:
    if ingest_request is None:
        return

    mark_failed = getattr(dataset_registry, "mark_ingest_failed", None)
    mark_aborted = getattr(dataset_registry, "mark_ingest_transaction_aborted", None)
    if not callable(mark_failed) or not callable(mark_aborted):
        return

    try:
        await mark_failed(
            ingest_request_id=ingest_request.ingest_request_id,
            error=error,
        )
        await mark_aborted(
            ingest_request_id=ingest_request.ingest_request_id,
            error=error,
        )
    except Exception as exc:
        logger.warning("Failed to mark %s ingest request failed: %s", stage, exc)

