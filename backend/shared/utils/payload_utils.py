"""Shared helpers for normalizing payload shapes."""

from __future__ import annotations

from typing import Any, Dict, List


def unwrap_data_payload(payload: Any) -> Dict[str, Any]:
    """Return the inner data dict when payloads wrap data under `data`."""
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    if isinstance(payload, dict):
        return payload
    return {}


def extract_payload_rows(payload: Any, *, key: str) -> List[Dict[str, Any]]:
    """Return dict rows from a list stored under `key` in a normalized payload."""
    rows = unwrap_data_payload(payload).get(key)
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def extract_payload_object(payload: Any) -> Dict[str, Any]:
    """Return the normalized payload object for object-shaped responses."""
    return unwrap_data_payload(payload)
