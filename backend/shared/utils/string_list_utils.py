from __future__ import annotations

from typing import Any, List


def normalize_string_list(
    value: Any,
    *,
    dedupe: bool = False,
    split_commas: bool = True,
) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        values = [str(item).strip() for item in value if str(item).strip()]
    elif isinstance(value, str):
        if split_commas:
            values = [part.strip() for part in value.split(",") if part.strip()]
        else:
            text = value.strip()
            values = [text] if text else []
    else:
        text = str(value).strip()
        values = [text] if text else []
    if not dedupe:
        return values
    deduped: List[str] = []
    seen: set[str] = set()
    for item in values:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped
