"""HTTPX error helpers (BFF).

Centralizes conversion of upstream HTTPX failures into FastAPI HTTPExceptions.
"""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import HTTPException, status

from shared.errors.error_types import ErrorCode, classified_http_exception


def extract_httpx_detail(exc: httpx.HTTPStatusError) -> Any:
    resp = getattr(exc, "response", None)
    if resp is None:
        return str(exc)

    detail: Any = resp.text
    try:
        detail_json = resp.json()
        if isinstance(detail_json, dict):
            detail = detail_json.get("detail") or detail_json.get("message") or detail_json.get("error") or detail_json
    except Exception:
        pass
    return detail


def raise_httpx_as_http_exception(exc: httpx.HTTPStatusError) -> None:
    resp = getattr(exc, "response", None)
    if resp is None:
        raise classified_http_exception(status.HTTP_502_BAD_GATEWAY, str(exc), code=ErrorCode.UPSTREAM_ERROR) from exc
    raise classified_http_exception(int(resp.status_code), str(extract_httpx_detail(exc)), code=ErrorCode.UPSTREAM_ERROR) from exc
