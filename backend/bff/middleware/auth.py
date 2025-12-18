from __future__ import annotations

import hmac
import os
from typing import Optional

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from shared.config.service_config import ServiceConfig


_TOKEN_ENV_KEYS = ("BFF_ADMIN_TOKEN", "BFF_WRITE_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN")
_SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}


def _get_expected_token() -> Optional[str]:
    for key in _TOKEN_ENV_KEYS:
        value = (os.getenv(key) or "").strip()
        if value:
            return value
    return None


def _auth_required() -> bool:
    raw = (os.getenv("BFF_REQUIRE_AUTH") or "").strip().lower()
    if raw in {"true", "1", "yes", "on"}:
        return True
    if raw in {"false", "0", "no", "off"}:
        return False
    if _get_expected_token():
        return True
    return ServiceConfig.is_production()


def _extract_presented_token(request: Request) -> Optional[str]:
    raw = (request.headers.get("X-Admin-Token") or "").strip()
    if raw:
        return raw
    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip() or None
    return None


def ensure_bff_auth_configured() -> None:
    if not _auth_required():
        return
    if not _get_expected_token():
        raise RuntimeError(
            "BFF auth is required but no token is configured. "
            "Set BFF_ADMIN_TOKEN (or BFF_WRITE_TOKEN/ADMIN_API_KEY/ADMIN_TOKEN)."
        )


def install_bff_auth_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def _bff_auth_middleware(request: Request, call_next):
        if not _auth_required():
            return await call_next(request)

        if request.method in _SAFE_METHODS:
            return await call_next(request)

        expected = _get_expected_token()
        if not expected:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"detail": "BFF auth required but no token configured"},
            )

        presented = _extract_presented_token(request)
        if not presented:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                headers={"WWW-Authenticate": "Bearer"},
                content={"detail": "Authentication required"},
            )

        if not hmac.compare_digest(presented, expected):
            return JSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={"detail": "Invalid authentication credentials"},
            )

        return await call_next(request)
