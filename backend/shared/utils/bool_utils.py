from __future__ import annotations

from typing import Any


_BASE_TRUTHY = frozenset({"true", "1", "yes", "on"})
_BASE_FALSY = frozenset({"false", "0", "no", "off"})
_SHORT_TRUTHY = frozenset({"t", "y"})
_SHORT_FALSY = frozenset({"f", "n"})


def parse_boolish(
    value: Any,
    *,
    allow_numeric: bool = False,
    allow_short_tokens: bool = False,
) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if allow_numeric and isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if not normalized:
            return None
        truthy_tokens = _BASE_TRUTHY | (_SHORT_TRUTHY if allow_short_tokens else frozenset())
        falsy_tokens = _BASE_FALSY | (_SHORT_FALSY if allow_short_tokens else frozenset())
        if normalized in truthy_tokens:
            return True
        if normalized in falsy_tokens:
            return False
    return None


def coerce_optional_bool(
    value: Any,
    *,
    default: bool,
    allow_numeric: bool = False,
    allow_short_tokens: bool = False,
) -> bool:
    parsed = parse_boolish(
        value,
        allow_numeric=allow_numeric,
        allow_short_tokens=allow_short_tokens,
    )
    return default if parsed is None else parsed
