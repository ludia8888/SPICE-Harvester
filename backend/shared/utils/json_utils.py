from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional


def json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def maybe_decode_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


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


def coerce_json_list(
    value: Any,
    *,
    allow_wrapped_value: bool = False,
    wrap_dict: bool = False,
) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, dict):
        if allow_wrapped_value:
            nested = value.get("value")
            if isinstance(nested, list):
                return list(nested)
        return [dict(value)] if wrap_dict else []
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except Exception:
            return []
        if isinstance(parsed, list):
            return list(parsed)
        if isinstance(parsed, dict):
            if allow_wrapped_value:
                nested = parsed.get("value")
                if isinstance(nested, list):
                    return list(nested)
            return [dict(parsed)] if wrap_dict else []
        return []
    try:
        return list(value)
    except Exception:
        return []


def coerce_json_dict(
    value: Any,
    *,
    parsed_fallback_key: Optional[str] = "value",
) -> Dict[str, Any]:
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
        except Exception:
            return {}
        if isinstance(parsed, dict):
            return dict(parsed)
        if parsed_fallback_key:
            return {parsed_fallback_key: parsed}
        return {}
    try:
        return dict(value)
    except Exception:
        return {}


def coerce_json_strict(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    raise TypeError("config_json must be a dict")
