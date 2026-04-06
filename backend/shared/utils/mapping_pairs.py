from __future__ import annotations

from typing import Any, Optional, Tuple


def normalize_mapping_pair(item: Any) -> Optional[Tuple[str, str]]:
    if not isinstance(item, dict):
        return None
    source_field = item.get("source_field")
    if source_field is None:
        source_field = item.get("sourceField")
    target_field = item.get("target_field")
    if target_field is None:
        target_field = item.get("targetField")
    source = str(source_field or "").strip()
    target = str(target_field or "").strip()
    if not source or not target:
        return None
    return source, target
