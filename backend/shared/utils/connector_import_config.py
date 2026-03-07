from __future__ import annotations

from typing import Any, Optional


def resolve_primary_key_column(config: Any) -> Optional[str]:
    if not isinstance(config, dict):
        return None
    for key in ("primary_key_column", "primaryKeyColumn"):
        value = config.get(key)
        text = str(value or "").strip()
        if text:
            return text
    return None
