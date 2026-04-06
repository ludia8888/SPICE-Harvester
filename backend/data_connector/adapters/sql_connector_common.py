from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Mapping


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$.]*$")


def safe_columns(rows: List[Mapping[str, Any]]) -> List[str]:
    if not rows:
        return []
    return [str(key) for key in rows[0].keys()]


def sanitize_simple_identifier(value: str, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} is required")
    if not _IDENTIFIER_RE.fullmatch(text):
        raise ValueError(f"{field_name} must be a simple identifier")
    return text


def row_value_case_insensitive(row: Mapping[str, Any], key: str) -> Any:
    if key in row:
        return row.get(key)
    lower = key.lower()
    for candidate, value in row.items():
        if str(candidate).lower() == lower:
            return value
    return None


def mapping_rows_to_dict_rows(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return [{str(key): value for key, value in row.items()} for row in rows]
