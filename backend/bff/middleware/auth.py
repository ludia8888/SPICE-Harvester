from __future__ import annotations

import hmac
import logging
from typing import Optional

from fastapi import FastAPI, Request, WebSocket, status
from fastapi.responses import JSONResponse

from shared.config.settings import get_settings
from shared.security.auth_utils import extract_presented_token, is_exempt_path

_EXEMPT_PATHS_DEFAULT = (
    "/api/v1/health",
    "/api/v1/",
    "/api/v1/data-connectors/google-sheets/oauth/callback",
)
logger = logging.getLogger(__name__)


def ensure_bff_auth_configured() -> None:
    auth = get_settings().auth
    if auth.bff_require_auth is False and not auth.bff_auth_disable_allowed:
        raise RuntimeError(
            "BFF auth explicitly disabled without approval. "
            "Set ALLOW_INSECURE_BFF_AUTH_DISABLE=true (or ALLOW_INSECURE_AUTH_DISABLE=true)."
        )
    if not auth.is_bff_auth_required(allow_pytest=True, default_required=True):
        return
    if not auth.bff_expected_token:
        raise RuntimeError(
            "BFF auth is required but no token is configured. "
            "Set BFF_ADMIN_TOKEN (or BFF_WRITE_TOKEN/ADMIN_API_KEY/ADMIN_TOKEN)."
        )


def install_bff_auth_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def _bff_auth_middleware(request: Request, call_next):
        auth = get_settings().auth
        if not auth.is_bff_auth_required(allow_pytest=True, default_required=True):
            return await call_next(request)

        exempt_paths = auth.resolve_bff_exempt_paths(defaults=_EXEMPT_PATHS_DEFAULT)
        if is_exempt_path(request.url.path, exempt_paths=exempt_paths):
            return await call_next(request)

        expected = auth.bff_expected_token
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

        agent_token = (auth.bff_agent_token or "").strip()
        if agent_token and hmac.compare_digest(presented, agent_token):
            request.state.is_internal_agent = True
            return await call_next(request)

        if not hmac.compare_digest(presented, expected):
            return JSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={"detail": "Invalid authentication credentials"},
            )

        return await call_next(request)


async def enforce_bff_websocket_auth(websocket: WebSocket, token: Optional[str]) -> bool:
    auth = get_settings().auth
    if not auth.is_bff_auth_required(allow_pytest=True, default_required=True):
        return True

    expected = auth.bff_expected_token
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

    agent_token = (auth.bff_agent_token or "").strip()
    if agent_token and hmac.compare_digest(presented, agent_token):
        return True

    if not hmac.compare_digest(presented, expected):
        logger.warning(
            "WebSocket auth token mismatch (path=%s)",
            websocket.url.path,
        )
        await websocket.close(code=4403, reason="Invalid authentication credentials")
        return False

    return True
