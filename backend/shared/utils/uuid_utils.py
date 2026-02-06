from __future__ import annotations

from typing import Any, Optional
from uuid import UUID


def safe_uuid(value: Any) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return str(UUID(raw))
    except Exception:
        return None

