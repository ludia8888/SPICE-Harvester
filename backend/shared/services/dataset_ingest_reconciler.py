from __future__ import annotations

import asyncio
import logging
from typing import Optional

from shared.services.dataset_registry import DatasetRegistry

logger = logging.getLogger(__name__)


async def run_dataset_ingest_reconciler(
    *,
    dataset_registry: DatasetRegistry,
    poll_interval_seconds: int = 60,
    stale_after_seconds: int = 3600,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    stop_event = stop_event or asyncio.Event()
    while not stop_event.is_set():
        try:
            result = await dataset_registry.reconcile_ingest_state(
                stale_after_seconds=stale_after_seconds,
            )
            if any(value > 0 for value in result.values()):
                logger.info("Ingest reconciler ran: %s", result)
        except Exception as exc:
            logger.warning("Ingest reconciler failed: %s", exc)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=poll_interval_seconds)
        except asyncio.TimeoutError:
            continue
