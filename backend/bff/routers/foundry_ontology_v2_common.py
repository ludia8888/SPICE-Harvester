from __future__ import annotations

from typing import Any, Dict

from shared.utils.foundry_page_token import decode_offset_page_token, encode_offset_page_token

_FOUNDRY_PAGE_TOKEN_TTL_SECONDS = 60 * 60 * 24


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


def _decode_page_token(page_token: str | None, *, scope: str | None = None) -> int:
    return decode_offset_page_token(
        page_token,
        ttl_seconds=_FOUNDRY_PAGE_TOKEN_TTL_SECONDS,
        expected_scope=scope,
    )


def _encode_page_token(offset: int, *, scope: str | None = None) -> str:
    return encode_offset_page_token(offset, scope=scope)
