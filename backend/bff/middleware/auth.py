from __future__ import annotations

import hmac
import logging
import os
from typing import Iterable, Optional

from fastapi import FastAPI, Request, WebSocket, status
from fastapi.responses import JSONResponse

_TOKEN_ENV_KEYS = ("BFF_ADMIN_TOKEN", "BFF_WRITE_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN")
_REQUIRE_ENV_KEY = "BFF_REQUIRE_AUTH"
_ALLOW_DISABLE_ENV_KEYS = ("ALLOW_INSECURE_BFF_AUTH_DISABLE", "ALLOW_INSECURE_AUTH_DISABLE")
_EXEMPT_PATHS_DEFAULT = (
    "/api/v1/health",
    "/api/v1/",
    "/api/v1/data-connectors/google-sheets/oauth/callback",
)
logger = logging.getLogger(__name__)


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


def _auth_required() -> bool:
    parsed = _parse_bool(os.getenv(_REQUIRE_ENV_KEY, ""))
    if parsed is not None:
        return parsed
    if os.getenv("PYTEST_CURRENT_TEST"):
        return False
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


def _auth_disable_allowed() -> bool:
    for key in _ALLOW_DISABLE_ENV_KEYS:
        if _parse_bool(os.getenv(key, "")) is True:
            return True
    return False


def _get_exempt_paths() -> set[str]:
    raw = (os.getenv("BFF_AUTH_EXEMPT_PATHS") or "").strip()
    if not raw:
        return set(_EXEMPT_PATHS_DEFAULT)
    return {path.strip() for path in raw.split(",") if path.strip()}


def _is_exempt_path(path: str, *, exempt_paths: Iterable[str]) -> bool:
    return path in set(exempt_paths)


def ensure_bff_auth_configured() -> None:
    parsed = _parse_bool(os.getenv(_REQUIRE_ENV_KEY, ""))
    if parsed is False and not _auth_disable_allowed():
        raise RuntimeError(
            "BFF auth explicitly disabled without approval. "
            "Set ALLOW_INSECURE_BFF_AUTH_DISABLE=true (or ALLOW_INSECURE_AUTH_DISABLE=true)."
        )
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

        exempt_paths = _get_exempt_paths()
        if _is_exempt_path(request.url.path, exempt_paths=exempt_paths):
            return await call_next(request)

        expected = _get_expected_token()
        if not expected:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"detail": "BFF auth required but no token configured"},
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


async def enforce_bff_websocket_auth(websocket: WebSocket, token: Optional[str]) -> bool:
    if not _auth_required():
        return True

    expected = _get_expected_token()
    if not expected:
        await websocket.close(code=1011, reason="BFF auth required but no token configured")
        return False

    query_token = None
    try:
        query_token = websocket.query_params.get("token")
    except Exception:
        query_token = None

    presented = token or query_token or _extract_presented_token(websocket.headers)
    if not presented:
        has_query = bool(token or query_token)
        has_header = (
            "x-admin-token" in websocket.headers
            or "authorization" in websocket.headers
        )
        logger.warning(
            "WebSocket auth missing token (path=%s, has_query=%s, has_header=%s)",
            websocket.url.path,
            has_query,
            has_header,
        )
        await websocket.close(code=4401, reason="Authentication required")
        return False

    if not hmac.compare_digest(presented, expected):
        logger.warning(
            "WebSocket auth token mismatch (path=%s)",
            websocket.url.path,
        )
        await websocket.close(code=4403, reason="Invalid authentication credentials")
        return False

    return True
