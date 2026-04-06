"""Shared normalization helpers for ontology read surfaces."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from shared.utils.language import first_localized_text
from shared.utils.ontology_fields import list_ontology_properties
from shared.utils.payload_utils import extract_payload_object, extract_payload_rows, unwrap_data_payload


def _unwrap_data(payload: Any) -> Dict[str, Any]:
    return unwrap_data_payload(payload)


def _extract_resources(payload: Any) -> List[Dict[str, Any]]:
    return extract_payload_rows(payload, key="resources")


def _extract_resource(payload: Any) -> Dict[str, Any]:
    return extract_payload_object(payload)


def _extract_ontology_properties(payload: Any) -> List[Dict[str, Any]]:
    return list_ontology_properties(payload)


def _localized_text(value: Any) -> Optional[str]:
    return first_localized_text(value)


def _normalize_object_ref(raw: Any) -> Optional[str]:
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    if not value:
        return None
    for prefix in ("object_type:", "object:", "class:"):
        if value.startswith(prefix):
            value = value[len(prefix) :].strip()
            break
    if "@" in value:
        value = value.split("@", 1)[0].strip()
    return value or None
