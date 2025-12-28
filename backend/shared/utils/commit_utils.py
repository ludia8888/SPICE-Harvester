from __future__ import annotations

from typing import Any, Optional


def coerce_commit_id(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        raw = value.strip()
        return raw or None
    if isinstance(value, dict):
        for key in ("commit", "commit_id", "identifier", "id", "@id", "head"):
            raw = value.get(key)
            if isinstance(raw, str) and raw.strip():
                return raw.strip()
    return str(value).strip() or None
