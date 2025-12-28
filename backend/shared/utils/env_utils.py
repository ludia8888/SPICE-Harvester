from __future__ import annotations

import os
from typing import Optional


def parse_bool(raw: str) -> Optional[bool]:
    value = (raw or "").strip().lower()
    if value in {"true", "1", "yes", "on"}:
        return True
    if value in {"false", "0", "no", "off"}:
        return False
    return None


def parse_bool_env(name: str, default: Optional[bool] = None) -> Optional[bool]:
    parsed = parse_bool(os.getenv(name, ""))
    return parsed if parsed is not None else default


def parse_int_env(name: str, default: int, *, min_value: int = 0, max_value: int = 1_000_000) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except Exception:
        return default
    return max(min_value, min(max_value, value))
