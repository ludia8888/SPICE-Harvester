"""Shared helpers for normalizing payload shapes."""

from __future__ import annotations

from typing import Any, Dict


def unwrap_data_payload(payload: Any) -> Dict[str, Any]:
    """Return the inner data dict when payloads wrap data under `data`."""
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    if isinstance(payload, dict):
        return payload
    return {}
