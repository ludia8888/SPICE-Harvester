from __future__ import annotations

import asyncio
import inspect
import os
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional


def build_outbox_worker_id(
    *,
    configured_worker_id: Any,
    service_name: Optional[str],
    hostname: Optional[str],
    default_service_name: str,
) -> str:
    configured = str(configured_worker_id or "").strip()
    if configured:
        return configured
    resolved_service_name = (service_name or default_service_name).strip() or default_service_name
    resolved_hostname = (hostname or "local").strip() or "local"
    return f"{resolved_service_name}:{resolved_hostname}:{os.getpid()}"


async def maybe_purge_with_interval(
    *,
    retention_days: int,
    purge_interval_seconds: int,
    purge_limit: int,
    last_purge: datetime,
    purge_call: Callable[..., Awaitable[int]],
    info_logger: Callable[[str, Any], None],
    warning_logger: Callable[[str, Any], None],
    success_message: str,
    failure_message: str,
) -> datetime:
    if retention_days <= 0:
        return last_purge
    now = datetime.now(timezone.utc)
    if (now - last_purge).total_seconds() < purge_interval_seconds:
        return last_purge
    try:
        deleted = await purge_call(retention_days=retention_days, limit=purge_limit)
        if deleted:
            info_logger(success_message, deleted)
    except Exception as exc:
        warning_logger(failure_message, exc)
    return now


async def run_outbox_poll_loop(
    *,
    publisher: Any,
    poll_interval_seconds: int,
    stop_event: Optional[asyncio.Event],
    warning_logger: Callable[[str, Any], None],
    failure_message: str,
) -> None:
    stop_event = stop_event or asyncio.Event()
    try:
        while not stop_event.is_set():
            try:
                await publisher.flush_once()
                await publisher.maybe_purge()
            except Exception as exc:
                warning_logger(failure_message, exc)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=poll_interval_seconds)
            except asyncio.TimeoutError:
                continue
    finally:
        close = getattr(publisher, "close", None)
        if close is None:
            return
        result = close()
        if inspect.isawaitable(result):
            await result


async def flush_outbox_until_empty(
    *,
    publisher: Any,
    is_empty: Callable[[Any], bool],
) -> None:
    try:
        while True:
            processed = await publisher.flush_once()
            if is_empty(processed):
                return
    finally:
        close = getattr(publisher, "close", None)
        if close is None:
            return
        result = close()
        if inspect.isawaitable(result):
            await result
