from __future__ import annotations

import os
from typing import Optional
import logging

from shared.utils.bool_utils import parse_boolish


def parse_bool(raw: str) -> Optional[bool]:
    return parse_boolish(raw)


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
