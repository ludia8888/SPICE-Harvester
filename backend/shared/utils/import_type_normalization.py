"""
Import type normalization (domain-neutral).

Normalizes user/ontology-provided type strings into xsd types understood by the shared import service.
"""

from __future__ import annotations

from typing import Any, Optional, Set

SUPPORTED_IMPORT_TYPES: Set[str] = {
    "string",
    "text",
    "integer",
    "int",
    "long",
    "decimal",
    "number",
    "float",
    "double",
    "boolean",
    "bool",
    "date",
    "datetime",
    "timestamp",
    "email",
    "url",
    "uri",
    "uuid",
    "ip",
    "phone",
    "json",
    "array",
    "struct",
    "object",
    "vector",
    "geopoint",
    "geoshape",
    "marking",
    "cipher",
    "attachment",
    "media",
    "time_series",
    "timeseries",
}


def normalize_import_target_type(type_value: Any) -> str:
    """
    Normalize a target field type for import.

    This is intentionally conservative and domain-neutral.
    """
    if not type_value:
        return "xsd:string"
    t = str(type_value).strip()
    if not t:
        return "xsd:string"
    if t.startswith("xsd:") or t.startswith("sys:") or t == "link":
        return t

    t_lower = t.lower()
    if t_lower in {"string", "text"}:
        return "xsd:string"
    if t_lower in {"uri", "url"}:
        return "xsd:anyURI"
    if t_lower in {"int", "integer", "long"}:
        return "xsd:integer"
    if t_lower in {"decimal", "number", "float", "double"}:
        return "xsd:decimal"
    if t_lower in {"bool", "boolean"}:
        return "xsd:boolean"
    if t_lower in {"date"}:
        return "xsd:date"
    if t_lower in {"datetime", "timestamp"}:
        return "xsd:dateTime"
    if t_lower in {"money", "currency"}:
        # Stored as numeric in instances; currency metadata is not imported yet.
        return "xsd:decimal"
    if t_lower in {"array", "list", "set", "struct", "object", "json", "vector"}:
        return "sys:JSON"
    if t_lower in {
        "geopoint",
        "geoshape",
        "marking",
        "cipher",
        "attachment",
        "media",
        "media_reference",
        "time_series",
        "timeseries",
    }:
        return "xsd:string"

    return "xsd:string"


def resolve_import_type(raw_type: Any) -> Optional[str]:
    """
    Resolve a raw type hint to an importable target type.

    Returns None for unknown/unsupported types.
    """
    if not raw_type:
        return None
    raw = str(raw_type).strip()
    if not raw:
        return None
    lowered = raw.lower()
    if lowered.startswith("xsd:") or lowered.startswith("sys:"):
        return raw
    if lowered not in SUPPORTED_IMPORT_TYPES:
        return None
    return normalize_import_target_type(lowered)
