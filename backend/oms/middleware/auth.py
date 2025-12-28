from __future__ import annotations

import hmac
import os
from typing import Optional

from fastapi import FastAPI, Request, status
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

_TOKEN_ENV_KEYS = ("OMS_ADMIN_TOKEN", "OMS_WRITE_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN")
_REQUIRE_ENV_KEY = "OMS_REQUIRE_AUTH"
_ALLOW_DISABLE_ENV_KEYS = ("ALLOW_INSECURE_OMS_AUTH_DISABLE", "ALLOW_INSECURE_AUTH_DISABLE")
_EXEMPT_PATHS_DEFAULT = ("/health", "/")


def ensure_oms_auth_configured() -> None:
    parsed = parse_bool(os.getenv(_REQUIRE_ENV_KEY, ""))
    if parsed is False and not auth_disable_allowed(_ALLOW_DISABLE_ENV_KEYS):
        raise RuntimeError(
            "OMS auth explicitly disabled without approval. "
            "Set ALLOW_INSECURE_OMS_AUTH_DISABLE=true (or ALLOW_INSECURE_AUTH_DISABLE=true)."
        )
    if not auth_required(_REQUIRE_ENV_KEY, token_env_keys=_TOKEN_ENV_KEYS, default_required=True):
        return
    if not get_expected_token(_TOKEN_ENV_KEYS):
        raise RuntimeError(
            "OMS auth is required but no token is configured. "
            "Set OMS_ADMIN_TOKEN (or OMS_WRITE_TOKEN/ADMIN_API_KEY/ADMIN_TOKEN)."
        )


def install_oms_auth_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def _oms_auth_middleware(request: Request, call_next):
        if not auth_required(_REQUIRE_ENV_KEY, token_env_keys=_TOKEN_ENV_KEYS, default_required=True):
            return await call_next(request)

        exempt_paths = get_exempt_paths("OMS_AUTH_EXEMPT_PATHS", defaults=_EXEMPT_PATHS_DEFAULT)
        if is_exempt_path(request.url.path, exempt_paths=exempt_paths):
            return await call_next(request)

        expected = get_expected_token(_TOKEN_ENV_KEYS)
        if not expected:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"detail": "OMS auth required but no token configured"},
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
