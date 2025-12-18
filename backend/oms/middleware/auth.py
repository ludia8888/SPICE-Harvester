from __future__ import annotations

import hmac
import os
from typing import Iterable, Optional

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse


_TOKEN_ENV_KEYS = ("OMS_ADMIN_TOKEN", "OMS_WRITE_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN")
_REQUIRE_ENV_KEY = "OMS_REQUIRE_AUTH"
_ALLOW_DISABLE_ENV_KEYS = ("ALLOW_INSECURE_OMS_AUTH_DISABLE", "ALLOW_INSECURE_AUTH_DISABLE")
_EXEMPT_PATHS_DEFAULT = ("/health", "/")


def _parse_bool(raw: str) -> Optional[bool]:
    value = (raw or "").strip().lower()
    if value in {"true", "1", "yes", "on"}:
        return True
    if value in {"false", "0", "no", "off"}:
        return False
    return None


def _get_expected_token() -> Optional[str]:
    for key in _TOKEN_ENV_KEYS:
        value = (os.getenv(key) or "").strip()
        if value:
            return value
    return None


def _auth_disable_allowed() -> bool:
    for key in _ALLOW_DISABLE_ENV_KEYS:
        if _parse_bool(os.getenv(key, "")) is True:
            return True
    return False


def _auth_required() -> bool:
    parsed = _parse_bool(os.getenv(_REQUIRE_ENV_KEY, ""))
    if parsed is False:
        return False
    if parsed is True:
        return True
    if _get_expected_token():
        return True
    # Fail-closed: require auth by default in all environments.
    return True


def _extract_presented_token(headers: dict) -> Optional[str]:
    raw = (headers.get("X-Admin-Token") or "").strip()
    if raw:
        return raw
    auth = (headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip() or None
    return None


def _get_exempt_paths() -> set[str]:
    raw = (os.getenv("OMS_AUTH_EXEMPT_PATHS") or "").strip()
    if not raw:
        return set(_EXEMPT_PATHS_DEFAULT)
    return {path.strip() for path in raw.split(",") if path.strip()}


def _is_exempt_path(path: str, *, exempt_paths: Iterable[str]) -> bool:
    return path in set(exempt_paths)


def ensure_oms_auth_configured() -> None:
    parsed = _parse_bool(os.getenv(_REQUIRE_ENV_KEY, ""))
    if parsed is False and not _auth_disable_allowed():
        raise RuntimeError(
            "OMS auth explicitly disabled without approval. "
            "Set ALLOW_INSECURE_OMS_AUTH_DISABLE=true (or ALLOW_INSECURE_AUTH_DISABLE=true)."
        )
    if not _auth_required():
        return
    if not _get_expected_token():
        raise RuntimeError(
            "OMS auth is required but no token is configured. "
            "Set OMS_ADMIN_TOKEN (or OMS_WRITE_TOKEN/ADMIN_API_KEY/ADMIN_TOKEN)."
        )


def install_oms_auth_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def _oms_auth_middleware(request: Request, call_next):
        if not _auth_required():
            return await call_next(request)

        exempt_paths = _get_exempt_paths()
        if _is_exempt_path(request.url.path, exempt_paths=exempt_paths):
            return await call_next(request)

        expected = _get_expected_token()
        if not expected:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"detail": "OMS auth required but no token configured"},
            )

        presented = _extract_presented_token(request.headers)
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
