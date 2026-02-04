"""Request header helpers (BFF).

Centralizes logic for forwarding privileged headers to upstream services.
"""

from __future__ import annotations

from typing import Dict, Iterable

from starlette.requests import Request

DEFAULT_FORWARD_HEADER_KEYS = (
    "X-Admin-Token",
    "X-Change-Reason",
    "X-Admin-Actor",
    "X-Actor",
    "Authorization",
)


def extract_forward_headers(
    request: Request,
    *,
    keys: Iterable[str] = DEFAULT_FORWARD_HEADER_KEYS,
) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for key in keys:
        value = request.headers.get(key)
        if value:
            headers[key] = value
    return headers

