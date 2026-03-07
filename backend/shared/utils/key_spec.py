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


def _property_flag(item: Dict[str, Any], snake_key: str, camel_key: str) -> bool:
    return bool(item.get(snake_key) or item.get(camel_key))


def _property_name(item: Dict[str, Any]) -> str:
    return str(item.get("name") or item.get("id") or "").strip()


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
        _property_name(item)
        for item in properties
        if isinstance(item, dict) and _property_flag(item, "primary_key", "primaryKey") and _property_name(item)
    ]
    title_key = [
        _property_name(item)
        for item in properties
        if isinstance(item, dict) and _property_flag(item, "title_key", "titleKey") and _property_name(item)
    ]
    if not primary_key and not title_key:
        return {}
    return {
        "primary_key": _dedupe(primary_key),
        "title_key": _dedupe(title_key),
    }


def extract_payload_key_spec(payload: Any) -> tuple[List[str], List[str]]:
    if not isinstance(payload, dict):
        return [], []

    derived = derive_key_spec_from_properties(payload.get("properties"))
    derived_primary = list(derived.get("primary_key") or [])
    derived_title = list(derived.get("title_key") or [])
    if derived_primary or derived_title:
        return derived_primary, derived_title

    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    raw_key_spec = metadata.get("key_spec") or metadata.get("keySpec") or metadata.get("key_specification")
    if not isinstance(raw_key_spec, dict):
        return [], []

    normalized = normalize_key_spec(raw_key_spec)
    return list(normalized.get("primary_key") or []), list(normalized.get("title_key") or [])


def normalize_object_type_key_spec(
    spec: Optional[Dict[str, Any]],
    *,
    columns: Optional[List[str]] = None,
) -> Dict[str, Any]:
    payload = spec or {}
    raw_key_spec = payload.get("pk_spec") if isinstance(payload, dict) else None
    if not raw_key_spec and isinstance(payload, dict):
        raw_key_spec = payload.get("pkSpec")
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
