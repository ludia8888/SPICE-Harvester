from __future__ import annotations

from typing import Any, Dict

from shared.utils.foundry_pagination import (
    decode_foundry_page_token as _decode_page_token,
    encode_foundry_page_token as _encode_page_token,
)


def _extract_api_response_data(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            return data
        return payload
    data = getattr(payload, "data", None)
    if isinstance(data, dict):
        return data
    return {}


def _default_expected_head_commit(branch: str) -> str:
    normalized = str(branch or "").strip() or "main"
    if normalized.lower().startswith("branch:"):
        return normalized
    return f"branch:{normalized}"
