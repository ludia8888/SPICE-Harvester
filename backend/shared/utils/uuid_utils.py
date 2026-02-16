from __future__ import annotations

from typing import Any, Optional
from uuid import UUID
import logging


def safe_uuid(value: Any) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return str(UUID(raw))
    except Exception:
        logging.getLogger(__name__).warning("Exception fallback at shared/utils/uuid_utils.py:13", exc_info=True)
        return None

