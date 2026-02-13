from __future__ import annotations

from datetime import datetime
from typing import Any

from shared.services.pipeline.pipeline_type_utils import (
    parse_datetime_text,
    parse_decimal_text,
    parse_int_text,
)
import logging


def is_bool_like(value: Any) -> bool:
    if isinstance(value, bool):
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"true", "false"}
    return False


def is_int_like(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, str):
        return parse_int_text(value) is not None
    return False


def is_decimal_like(value: Any, *, include_int: bool = False) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, float):
        return True
    if isinstance(value, int):
        return bool(include_int)
    if isinstance(value, str):
        return parse_decimal_text(value) is not None
    return False


def is_datetime_like(value: Any, *, iso_only: bool = False, allow_ambiguous: bool = False) -> bool:
    if isinstance(value, datetime):
        return True
    if not isinstance(value, str):
        return False
    if iso_only:
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            return True
        except Exception:
            logging.getLogger(__name__).warning("Broad exception fallback at shared/services/pipeline/pipeline_value_predicates.py:52", exc_info=True)
            return False
    return parse_datetime_text(value, allow_ambiguous=allow_ambiguous) is not None

