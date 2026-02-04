"""Shared helpers for extracting schema column definitions."""

from __future__ import annotations

from typing import Any, Dict, List


def extract_schema_columns(schema: Any) -> List[Dict[str, Any]]:
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

    columns = schema.get("columns")
    if isinstance(columns, list):
        output: List[Dict[str, Any]] = []
        for col in columns:
            if isinstance(col, dict):
                name = str(col.get("name") or col.get("column") or "").strip()
                raw_type = col.get("type") or col.get("data_type") or col.get("datatype")
            else:
                name = str(col).strip()
                raw_type = None
            if name:
                output.append({"name": name, "type": raw_type})
        return output

    fields = schema.get("fields")
    if isinstance(fields, list):
        output = []
        for col in fields:
            if not isinstance(col, dict):
                continue
            name = str(col.get("name") or "").strip()
            if name:
                output.append({"name": name, "type": col.get("type")})
        return output

    return []

