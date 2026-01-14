from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from shared.services.agent_session_registry import AgentSessionRegistry
from shared.services.storage_service import StorageService

logger = logging.getLogger(__name__)


async def run_agent_session_retention_worker(
    *,
    session_registry: AgentSessionRegistry,
    poll_interval_seconds: int,
    retention_days: int,
    stop_event: asyncio.Event,
    tenant_id: Optional[str] = None,
    action: str = "redact",
    storage_service: Optional[StorageService] = None,
    delete_file_upload_objects: bool = True,
) -> None:
    """
    Background retention worker for agent session data (SEC-005).

    This worker is intentionally best-effort; failures are logged and retried.
    """
    poll_seconds = max(1, int(poll_interval_seconds))
    retention_days = max(0, int(retention_days))
    action_value = str(action or "").strip().lower() or "redact"
    delete_uploads = bool(delete_file_upload_objects)

    while not stop_event.is_set():
        try:
            if retention_days > 0:
                cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
                if delete_uploads and storage_service is not None:
                    try:
                        expired = await session_registry.list_expired_file_uploads(cutoff=cutoff, tenant_id=tenant_id, limit=500)
                        for item in expired:
                            bucket = str(item.get("bucket") or "").strip()
                            key = str(item.get("key") or "").strip()
                            if not bucket or not key:
                                continue
                            with contextlib.suppress(Exception):
                                await storage_service.delete_object(bucket=bucket, key=key)
                    except Exception as exc:
                        logger.warning("Agent retention upload cleanup failed: %s", exc)
                await session_registry.apply_retention(
                    cutoff=cutoff,
                    tenant_id=tenant_id,
                    action=action_value,
                )
        except Exception as exc:
            logger.warning("Agent retention worker failed: %s", exc)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=poll_seconds)
        except asyncio.TimeoutError:
            continue
