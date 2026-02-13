from __future__ import annotations

from typing import Any, Optional
import logging


def to_int_or_none(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        logging.getLogger(__name__).warning("Broad exception fallback at shared/utils/number_utils.py:11", exc_info=True)
        return None
