"""Shared helpers for extracting schema column definitions."""

from __future__ import annotations

from typing import Any, Dict, List


def extract_schema_columns(
    schema: Any,
    *,
    strip_bom: bool = False,
    dedupe: bool = False,
) -> List[Dict[str, Any]]:
    """Extract a normalized list of column definitions from a schema payload.

    Supports common payload shapes:
    - ``{"columns": [{"name": ..., "type": ...}, ...]}``
    - ``{"fields": [{"name": ..., "type": ...}, ...]}``
    - ``{"columns": ["col_a", "col_b"]}`` (type-less)

    The result is a list of dicts with at least a ``name`` key and an optional
    ``type`` key when available.
    """
    if not isinstance(schema, dict):
        return []

    def _normalize_name(value: Any) -> str:
        name = str(value or "").strip()
        if strip_bom:
            name = name.lstrip("\ufeff") or name
        return name

    def _dedupe_name(name: str, seen: set[str]) -> str:
        if not dedupe:
            return name
        if name not in seen:
            return name
        base = name
        idx = 1
        while f"{base}__{idx}" in seen:
            idx += 1
        return f"{base}__{idx}"

    columns = schema.get("columns")
    if isinstance(columns, list):
        output: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for col in columns:
            if isinstance(col, dict):
                name = _normalize_name(col.get("name") or col.get("column") or "")
                raw_type = col.get("type") or col.get("data_type") or col.get("datatype")
            else:
                name = _normalize_name(col)
                raw_type = None
            if name:
                deduped = _dedupe_name(name, seen)
                seen.add(deduped)
                output.append({"name": deduped, "type": raw_type})
        return output

    fields = schema.get("fields")
    if isinstance(fields, list):
        output = []
        seen: set[str] = set()
        for col in fields:
            if not isinstance(col, dict):
                continue
            name = _normalize_name(col.get("name") or "")
            if name:
                deduped = _dedupe_name(name, seen)
                seen.add(deduped)
                output.append({"name": deduped, "type": col.get("type")})
        return output

    props = schema.get("properties")
    if isinstance(props, dict):
        output = []
        seen: set[str] = set()
        for key, value in props.items():
            name = _normalize_name(key)
            if not name:
                continue
            if isinstance(value, dict):
                raw_type = value.get("type") or value.get("data_type") or value.get("datatype")
            else:
                raw_type = value
            deduped = _dedupe_name(name, seen)
            seen.add(deduped)
            output.append({"name": deduped, "type": raw_type})
        return output

    return []
