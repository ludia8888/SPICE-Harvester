from __future__ import annotations

import hmac
import logging
import os
from typing import Optional

from fastapi import FastAPI, Request, WebSocket, status
from fastapi.responses import JSONResponse

from shared.security.auth_utils import (
    auth_disable_allowed,
    auth_required,
    extract_presented_token,
    get_exempt_paths,
    get_expected_token,
    is_exempt_path,
    parse_bool,
)
_TOKEN_ENV_KEYS = ("BFF_ADMIN_TOKEN", "BFF_WRITE_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN")
_REQUIRE_ENV_KEY = "BFF_REQUIRE_AUTH"
_ALLOW_DISABLE_ENV_KEYS = ("ALLOW_INSECURE_BFF_AUTH_DISABLE", "ALLOW_INSECURE_AUTH_DISABLE")
_EXEMPT_PATHS_DEFAULT = (
    "/api/v1/health",
    "/api/v1/",
    "/api/v1/data-connectors/google-sheets/oauth/callback",
)
logger = logging.getLogger(__name__)


def ensure_bff_auth_configured() -> None:
    parsed = parse_bool(os.getenv(_REQUIRE_ENV_KEY, ""))
    if parsed is False and not auth_disable_allowed(_ALLOW_DISABLE_ENV_KEYS):
        raise RuntimeError(
            "BFF auth explicitly disabled without approval. "
            "Set ALLOW_INSECURE_BFF_AUTH_DISABLE=true (or ALLOW_INSECURE_AUTH_DISABLE=true)."
        )
    if not auth_required(
        _REQUIRE_ENV_KEY,
        token_env_keys=_TOKEN_ENV_KEYS,
        default_required=True,
        allow_pytest=True,
    ):
        return
    if not get_expected_token(_TOKEN_ENV_KEYS):
        raise RuntimeError(
            "BFF auth is required but no token is configured. "
            "Set BFF_ADMIN_TOKEN (or BFF_WRITE_TOKEN/ADMIN_API_KEY/ADMIN_TOKEN)."
        )


def install_bff_auth_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def _bff_auth_middleware(request: Request, call_next):
        if not auth_required(
            _REQUIRE_ENV_KEY,
            token_env_keys=_TOKEN_ENV_KEYS,
            default_required=True,
            allow_pytest=True,
        ):
            return await call_next(request)

        exempt_paths = get_exempt_paths("BFF_AUTH_EXEMPT_PATHS", defaults=_EXEMPT_PATHS_DEFAULT)
        if is_exempt_path(request.url.path, exempt_paths=exempt_paths):
            return await call_next(request)

        expected = get_expected_token(_TOKEN_ENV_KEYS)
        if not expected:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"detail": "BFF auth required but no token configured"},
            )

        presented = extract_presented_token(request.headers)
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
    if not auth_required(
        _REQUIRE_ENV_KEY,
        token_env_keys=_TOKEN_ENV_KEYS,
        default_required=True,
        allow_pytest=True,
    ):
        return True

    expected = get_expected_token(_TOKEN_ENV_KEYS)
    if not expected:
        await websocket.close(code=1011, reason="BFF auth required but no token configured")
        return False

    query_token = None
    try:
        query_token = websocket.query_params.get("token")
    except Exception:
        query_token = None

    presented = token or query_token or extract_presented_token(websocket.headers)
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
