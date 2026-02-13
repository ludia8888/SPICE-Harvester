from __future__ import annotations

import asyncio
import inspect
import logging
import os
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)


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
        logging.getLogger(__name__).warning("Broad exception fallback at shared/services/events/outbox_runtime.py:49", exc_info=True)
        warning_logger(failure_message, exc)
    return now


async def run_outbox_poll_loop(
    *,
    publisher: Any,
    poll_interval_seconds: int,
    stop_event: Optional[asyncio.Event],
    warning_logger: Callable[[str, Any], None],
    failure_message: str,
    adaptive: bool = True,
    min_poll_interval: float = 0.5,
    max_poll_interval: float = 5.0,
) -> None:
    """Run outbox poll loop with optional adaptive polling.

    When ``adaptive`` is True (default), the poll interval adjusts dynamically:
    - After processing messages: immediately re-poll (``min_poll_interval``)
    - After empty poll: gradually increase interval toward ``max_poll_interval``
    This reduces latency from ~5s to <1s when events are flowing.
    """
    stop_event = stop_event or asyncio.Event()
    current_interval = float(poll_interval_seconds)
    primary_exc: Optional[BaseException] = None
    try:
        while not stop_event.is_set():
            had_work = False
            try:
                result = await publisher.flush_once()
                await publisher.maybe_purge()
                # Detect if work was done (flush_once may return count or None)
                if result and (isinstance(result, int) and result > 0):
                    had_work = True
                elif result and isinstance(result, dict) and result.get("published", 0) > 0:
                    had_work = True
            except Exception as exc:
                logging.getLogger(__name__).warning("Broad exception fallback at shared/services/events/outbox_runtime.py:86", exc_info=True)
                warning_logger(failure_message, exc)

            # Adaptive poll interval
            if adaptive:
                if had_work:
                    current_interval = min_poll_interval
                else:
                    # Gradual backoff: multiply by 1.5, cap at max
                    current_interval = min(current_interval * 1.5, max_poll_interval)
            else:
                current_interval = float(poll_interval_seconds)

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=current_interval)
            except asyncio.TimeoutError:
                continue
    except BaseException as exc:
        primary_exc = exc
        raise
    finally:
        close = getattr(publisher, "close", None)
        if close is not None:
            try:
                result = close()
                if inspect.isawaitable(result):
                    await result
            except Exception as close_exc:
                if primary_exc is not None:
                    warning_logger("Outbox publisher close failed after primary error: %s", close_exc)
                else:
                    raise


async def flush_outbox_until_empty(
    *,
    publisher: Any,
    is_empty: Callable[[Any], bool],
) -> None:
    primary_exc: Optional[BaseException] = None
    try:
        while True:
            processed = await publisher.flush_once()
            if is_empty(processed):
                return
    except BaseException as exc:
        primary_exc = exc
        raise
    finally:
        close = getattr(publisher, "close", None)
        if close is not None:
            try:
                result = close()
                if inspect.isawaitable(result):
                    await result
            except Exception as close_exc:
                if primary_exc is not None:
                    logger.warning(
                        "Outbox publisher close failed after primary error: %s",
                        close_exc,
                        exc_info=True,
                    )
                else:
                    raise
