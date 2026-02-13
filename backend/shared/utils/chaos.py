"""
Chaos/fault injection helpers (test-only).

These helpers allow deterministic reproduction of partial-failure scenarios
without mocks:
- worker crash after claiming an event (lease recovery)
- crash before/after side-effects (idempotency + retry correctness)

Enable via env:
- ENABLE_CHAOS_INJECTION=true
- CHAOS_CRASH_POINT="instance_worker:after_claim" (exact match, case-insensitive)

Optional:
- CHAOS_CRASH_ONCE=true (default) -> crash only once per container for a point
- CHAOS_CRASH_EXIT_CODE=42
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional

from shared.config.settings import get_settings
import logging


def chaos_enabled() -> bool:
    return bool(get_settings().chaos.enabled)


def _sanitize_marker(point: str) -> str:
    safe = (point or "unknown").strip().lower()
    safe = re.sub(r"[^a-z0-9_.:-]+", "_", safe)
    return safe[:120] or "unknown"


def maybe_crash(point: str, *, logger: Optional[object] = None) -> None:
    """
    Crash the current process if CHAOS_CRASH_POINT matches.

    This uses os._exit to simulate an abrupt crash (no cleanup), which is what
    exercises lease recovery and at-least-once correctness.
    """
    chaos = get_settings().chaos
    if not chaos.enabled:
        return

    selected = (chaos.crash_point or "").strip()
    if not selected:
        return
    if selected.lower() != (point or "").strip().lower():
        return

    if chaos.crash_once:
        marker = Path("/tmp") / f"spice_chaos_crashed_{_sanitize_marker(point)}"
        try:
            if marker.exists():
                return
            marker.write_text("crashed\n", encoding="utf-8")
        except Exception:
            # Marker best-effort; still crash if requested.
            logging.getLogger(__name__).warning("Broad exception fallback at shared/utils/chaos.py:61", exc_info=True)
            pass

    msg = f"CHAOS: crashing at {point}"
    try:
        if logger and hasattr(logger, "error"):
            logger.error(msg)
        else:
            print(msg, flush=True)
    except Exception:
        logging.getLogger(__name__).warning("Broad exception fallback at shared/utils/chaos.py:71", exc_info=True)
        pass

    os._exit(int(chaos.crash_exit_code))
