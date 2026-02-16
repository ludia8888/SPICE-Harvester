"""Foundry-style opaque page token helpers.

Tokens are opaque strings that encode pagination state and issuance time.
Current format:
    base64url(json({"v":1,"offset":<int>,"iat":<unix_seconds>,"scope":"..."}))
"""

from __future__ import annotations

import base64
import json
import time
from typing import Any

_TOKEN_VERSION = 1
_MAX_CLOCK_SKEW_SECONDS = 5


def encode_offset_page_token(
    offset: int,
    *,
    issued_at: int | None = None,
    scope: str | None = None,
) -> str:
    """Encode a pagination offset into an opaque page token."""
    payload = {
        "v": _TOKEN_VERSION,
        "offset": max(0, int(offset)),
        "iat": int(issued_at) if issued_at is not None else int(time.time()),
    }
    if scope is not None:
        payload["scope"] = str(scope)
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_offset_page_token(
    page_token: str | None,
    *,
    ttl_seconds: int = 60,
    now: int | None = None,
    expected_scope: str | None = None,
) -> int:
    """Decode an opaque page token and return its offset.

    Empty/missing tokens resolve to 0.
    """
    if page_token is None:
        return 0
    token = str(page_token).strip()
    if not token:
        return 0

    try:
        padding = "=" * (-len(token) % 4)
        decoded = base64.urlsafe_b64decode(f"{token}{padding}".encode("ascii")).decode("utf-8")
        payload: Any = json.loads(decoded)
    except Exception as exc:
        raise ValueError("pageToken must be a valid opaque token") from exc

    if not isinstance(payload, dict):
        raise ValueError("pageToken must be a valid opaque token")
    if int(payload.get("v") or 0) != _TOKEN_VERSION:
        raise ValueError("pageToken version is not supported")

    try:
        offset = int(payload.get("offset"))
    except Exception as exc:
        raise ValueError("pageToken must include a non-negative offset") from exc
    if offset < 0:
        raise ValueError("pageToken offset must be >= 0")

    try:
        issued_at = int(payload.get("iat"))
    except Exception as exc:
        raise ValueError("pageToken must include an issued timestamp") from exc

    if ttl_seconds > 0:
        current = int(now) if now is not None else int(time.time())
        if issued_at > current + _MAX_CLOCK_SKEW_SECONDS:
            raise ValueError("pageToken timestamp is invalid")
        if (current - issued_at) > int(ttl_seconds):
            raise ValueError("pageToken has expired")

    if expected_scope is not None:
        token_scope = payload.get("scope")
        # Compatibility: allow unscope'd tokens for one TTL window after rollout.
        if token_scope is not None and str(token_scope) != str(expected_scope):
            raise ValueError("pageToken is invalid for this request")

    return offset
