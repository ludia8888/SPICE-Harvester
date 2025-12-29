from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict


def json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def normalize_json_payload(value: Any, *, default_handler=json_default) -> str:
    if value is None:
        return "{}"
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, default=default_handler)
    except TypeError:
        return json.dumps(str(value))


def coerce_json_dataset(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
            return {"value": parsed}
        except Exception:
            return {"raw": value}
    try:
        return dict(value)
    except Exception:
        return {}


def coerce_json_pipeline(value: Any) -> Any:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, list):
        return list(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except Exception:
            return {}
        if isinstance(parsed, dict):
            return dict(parsed)
        if isinstance(parsed, list):
            return list(parsed)
        return {}
    return {}


def coerce_json_strict(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    raise TypeError("config_json must be a dict")
