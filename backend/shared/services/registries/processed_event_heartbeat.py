from __future__ import annotations

import asyncio
import logging
from typing import Optional

from shared.config.settings import get_settings
from shared.services.registries.processed_event_registry import ProcessedEventRegistry

DEFAULT_WARNING_MESSAGE = "Heartbeat failed (handler=%s event_id=%s): %s"


async def run_processed_event_heartbeat_loop(
    registry: Optional[ProcessedEventRegistry],
    *,
    handler: str,
    event_id: str,
    interval_seconds: Optional[int] = None,
    stop_when_false: bool = True,
    continue_on_exception: bool = False,
    logger: Optional[logging.Logger] = None,
    warning_message: Optional[str] = None,
) -> None:
    """
    Keep a ProcessedEventRegistry lease alive while processing an event.

    Workers use this to avoid duplicating "sleep + heartbeat" loops.

    Behavior knobs:
    - stop_when_false: return when the registry indicates the lease is gone.
    - continue_on_exception: swallow heartbeat exceptions (best-effort heartbeat).
    """

    if not registry:
        return

    interval = int(
        interval_seconds
        if interval_seconds is not None
        else int(get_settings().event_sourcing.processed_event_heartbeat_interval_seconds)
    )

    while True:
        try:
            await asyncio.sleep(interval)
            ok = await registry.heartbeat(handler=handler, event_id=event_id)
            if stop_when_false and not ok:
                return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not continue_on_exception:
                raise

            log = logger or logging.getLogger(__name__)
            msg = warning_message or DEFAULT_WARNING_MESSAGE
            try:
                log.warning(msg, handler, event_id, exc)
            except Exception:
                # Formatter mismatch or logger misconfiguration; keep the loop alive.
                log.warning("%s", msg)

