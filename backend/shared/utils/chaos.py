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


def chaos_enabled() -> bool:
    return (os.getenv("ENABLE_CHAOS_INJECTION") or "").strip().lower() in {"1", "true", "yes", "on"}


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
    if not chaos_enabled():
        return

    selected = (os.getenv("CHAOS_CRASH_POINT") or "").strip()
    if not selected:
        return
    if selected.lower() != (point or "").strip().lower():
        return

    crash_once = (os.getenv("CHAOS_CRASH_ONCE") or "true").strip().lower() in {"1", "true", "yes", "on"}
    if crash_once:
        marker = Path("/tmp") / f"spice_chaos_crashed_{_sanitize_marker(point)}"
        try:
            if marker.exists():
                return
            marker.write_text("crashed\n", encoding="utf-8")
        except Exception:
            # Marker best-effort; still crash if requested.
            pass

    msg = f"CHAOS: crashing at {point}"
    try:
        if logger and hasattr(logger, "error"):
            logger.error(msg)
        else:
            print(msg, flush=True)
    except Exception:
        pass

    code_raw = (os.getenv("CHAOS_CRASH_EXIT_CODE") or "42").strip()
    try:
        code = int(code_raw)
    except Exception:
        code = 42
    os._exit(code)

