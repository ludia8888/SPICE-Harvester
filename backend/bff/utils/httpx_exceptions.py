"""HTTPX error helpers (BFF).

Centralizes conversion of upstream HTTPX failures into FastAPI HTTPExceptions.
"""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import HTTPException, status


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
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    raise HTTPException(status_code=int(resp.status_code), detail=extract_httpx_detail(exc)) from exc
