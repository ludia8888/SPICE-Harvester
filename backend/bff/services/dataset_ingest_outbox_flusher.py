from __future__ import annotations

import logging
from typing import Any, Optional

from shared.config.settings import get_settings
from shared.observability.tracing import trace_external_call

logger = logging.getLogger(__name__)


def _dataset_ingest_outbox_worker_enabled() -> bool:
    return bool(get_settings().workers.dataset_ingest_outbox.enabled)


@trace_external_call("bff.dataset_ingest_outbox.maybe_flush_dataset_ingest_outbox_inline")
async def maybe_flush_dataset_ingest_outbox_inline(
    *,
    dataset_registry: Any,
    lineage_store: Optional[Any],
    flush_dataset_ingest_outbox: Any,
    batch_size: int = 50,
) -> None:
    """
    Best-effort inline flush of the dataset ingest outbox.

    In the BFF runtime, a background worker (`run_dataset_ingest_outbox_worker`) is enabled by default
    and continuously drains the outbox. Inline flushing after each upload can:
    - duplicate work (two drainers racing)
    - block request latency on storage hiccups (S3/MinIO)

    Therefore we only flush inline when the background worker is disabled.
    """
    if _dataset_ingest_outbox_worker_enabled():
        return

    await flush_dataset_ingest_outbox(
        dataset_registry=dataset_registry,
        lineage_store=lineage_store,
        batch_size=batch_size,
    )
