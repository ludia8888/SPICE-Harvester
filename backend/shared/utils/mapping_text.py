from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional, Tuple


def normalized_text(value: Any) -> str:
    return str(value or "").strip()


def first_mapping_text(
    *,
    keys: Iterable[str],
    sources: Iterable[Mapping[str, Any]],
) -> str:
    for source in sources:
        for key in keys:
            if key not in source:
                continue
            value = normalized_text(source.get(key))
            if value:
                return value
    return ""


def first_mapping_text_with_key(
    *,
    keys: Iterable[str],
    sources: Iterable[Mapping[str, Any]],
) -> Tuple[str, Optional[str]]:
    for source in sources:
        for key in keys:
            if key not in source:
                continue
            value = normalized_text(source.get(key))
            if value:
                return value, key
    return "", None


def first_mapping_raw(
    *,
    keys: Iterable[str],
    sources: Iterable[Mapping[str, Any]],
) -> Any:
    for source in sources:
        for key in keys:
            if key in source and source.get(key) is not None:
                return source.get(key)
    return None
