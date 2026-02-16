from __future__ import annotations

import os
from typing import Optional
import logging


def parse_bool(raw: str) -> Optional[bool]:
    value = (raw or "").strip().lower()
    if value in {"true", "1", "yes", "on"}:
        return True
    if value in {"false", "0", "no", "off"}:
        return False
    return None


def parse_bool_env(name: str, default: Optional[bool] = None) -> Optional[bool]:
    parsed = parse_bool(str(os.environ.get(name, "")))
    return parsed if parsed is not None else default


def parse_int_env(name: str, default: int, *, min_value: int = 0, max_value: int = 1_000_000) -> int:
    raw = str(os.environ.get(name, "")).strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except Exception:
        logging.getLogger(__name__).warning("Exception fallback at shared/utils/env_utils.py:27", exc_info=True)
        return default
    return max(min_value, min(max_value, value))
