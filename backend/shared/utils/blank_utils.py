from __future__ import annotations

from typing import Any, Optional


def strip_to_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def is_blank_value(value: Any) -> bool:
    return strip_to_none(value) is None
