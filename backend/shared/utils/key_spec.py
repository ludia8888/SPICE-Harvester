"""
Key spec normalization helpers.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


_KEY_FIELDS = {
    "columns",
    "column",
    "keys",
    "key",
    "fields",
    "field",
    "primaryKey",
    "primary_key",
    "primaryKeys",
    "primary_keys",
    "titleKey",
    "title_key",
    "uniqueKey",
    "unique_key",
    "uniqueKeys",
    "unique_keys",
    "required",
    "required_fields",
    "nullable",
    "nullable_fields",
}


def _dedupe(values: List[str]) -> List[str]:
    seen: set[str] = set()
    output: List[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def normalize_key_columns(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return _dedupe([part.strip() for part in value.split(",") if part.strip()])
    if isinstance(value, list):
        output: List[str] = []
        for item in value:
            if isinstance(item, dict):
                for key in _KEY_FIELDS:
                    if key in item:
                        output.extend(normalize_key_columns(item.get(key)))
            else:
                output.extend(normalize_key_columns(item))
        return _dedupe(output)
    if isinstance(value, dict):
        output = []
        for key in _KEY_FIELDS:
            if key in value:
                output.extend(normalize_key_columns(value.get(key)))
        return _dedupe(output)
    return []


def normalize_unique_keys(value: Any) -> List[List[str]]:
    if value is None:
        return []
    if isinstance(value, str):
        parts = [part.strip() for part in value.split(",") if part.strip()]
        return [parts] if parts else []
    if isinstance(value, list):
        output: List[List[str]] = []
        for item in value:
            if isinstance(item, list):
                columns = normalize_key_columns(item)
                if columns:
                    output.append(columns)
            else:
                columns = normalize_key_columns(item)
                if columns:
                    output.append(columns)
        return output
    if isinstance(value, dict):
        return normalize_unique_keys(value.get("unique") or value.get("unique_keys") or value.get("uniqueKeys"))
    return []


def derive_key_spec_from_properties(properties: Any) -> Dict[str, Any]:
    if not isinstance(properties, list):
        return {}

    primary_key = [
        str(item.get("name")).strip()
        for item in properties
        if isinstance(item, dict) and item.get("primary_key") and str(item.get("name") or "").strip()
    ]
    title_key = [
        str(item.get("name")).strip()
        for item in properties
        if isinstance(item, dict) and item.get("title_key") and str(item.get("name") or "").strip()
    ]
    if not primary_key and not title_key:
        return {}
    return {
        "primary_key": _dedupe(primary_key),
        "title_key": _dedupe(title_key),
    }


def normalize_object_type_key_spec(
    spec: Optional[Dict[str, Any]],
    *,
    columns: Optional[List[str]] = None,
) -> Dict[str, Any]:
    payload = spec or {}
    raw_key_spec = payload.get("pk_spec") if isinstance(payload, dict) else None
    if not raw_key_spec and isinstance(payload, dict):
        raw_key_spec = derive_key_spec_from_properties(payload.get("properties"))
    return normalize_key_spec(raw_key_spec if isinstance(raw_key_spec, dict) else {}, columns=columns)


def normalize_key_spec(
    spec: Optional[Dict[str, Any]],
    *,
    columns: Optional[List[str]] = None,
) -> Dict[str, Any]:
    payload = spec or {}
    primary_key = normalize_key_columns(
        payload.get("primary_key")
        or payload.get("primaryKey")
        or payload.get("primary_keys")
        or payload.get("primaryKeys")
        or payload.get("pk")
        or payload.get("pk_columns")
        or payload.get("pkColumns")
    )
    title_key = normalize_key_columns(payload.get("title_key") or payload.get("titleKey"))
    unique_keys = normalize_unique_keys(payload.get("unique_keys") or payload.get("uniqueKeys") or payload.get("unique"))
    required_fields = normalize_key_columns(
        payload.get("required_fields")
        or payload.get("required")
        or payload.get("non_nullable")
        or payload.get("nonNullable")
    )
    nullable_fields = normalize_key_columns(
        payload.get("nullable_fields") or payload.get("nullable") or payload.get("nullableFields")
    )
    if not required_fields and nullable_fields and columns:
        nullable_set = set(nullable_fields)
        required_fields = [col for col in columns if col not in nullable_set]

    return {
        "primary_key": primary_key,
        "title_key": title_key,
        "unique_keys": unique_keys,
        "required_fields": required_fields,
        "nullable_fields": nullable_fields,
    }
