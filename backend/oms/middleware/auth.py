from __future__ import annotations

import hmac

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from shared.config.settings import get_settings
from shared.security.auth_utils import extract_presented_token, is_exempt_path

_EXEMPT_PATHS_DEFAULT = ("/health", "/")
_INTERNAL_BRIDGE_HEADER = "X-OMS-Internal-Bridge"


def ensure_oms_auth_configured() -> None:
    auth = get_settings().auth
    if auth.oms_require_auth is False and not auth.oms_auth_disable_allowed:
        raise RuntimeError(
            "OMS auth explicitly disabled without approval. "
            "Set ALLOW_INSECURE_OMS_AUTH_DISABLE=true (or ALLOW_INSECURE_AUTH_DISABLE=true)."
        )
    if not auth.is_oms_auth_required(default_required=True):
        return
    if not auth.oms_expected_tokens:
        raise RuntimeError(
            "OMS auth is required but no token is configured. "
            "Set OMS_ADMIN_TOKEN (or OMS_WRITE_TOKEN/ADMIN_API_KEY/ADMIN_TOKEN)."
        )


def install_oms_auth_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def _oms_auth_middleware(request: Request, call_next):
        settings = get_settings()
        auth = settings.auth
        if not auth.is_oms_auth_required(default_required=True):
            return await call_next(request)

        exempt_paths = auth.resolve_oms_exempt_paths(defaults=_EXEMPT_PATHS_DEFAULT)
        if is_exempt_path(request.url.path, exempt_paths=exempt_paths):
            return await call_next(request)

        # gRPC server dispatches into FastAPI via internal ASGI transport after transport-level auth.
        if request.headers.get(_INTERNAL_BRIDGE_HEADER) == "1":
            return await call_next(request)

        dev_master = bool(auth.dev_master_auth_enabled and settings.is_development)
        expected_tokens = auth.oms_expected_tokens
        if not expected_tokens:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"detail": "OMS auth required but no token configured"},
            )

        presented = extract_presented_token(request.headers)
        if not presented:
            if dev_master:
                return await call_next(request)
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                headers={"WWW-Authenticate": "Bearer"},
                content={"detail": "Authentication required"},
            )

        if not any(hmac.compare_digest(presented, expected) for expected in expected_tokens):
            if dev_master:
                return await call_next(request)
            return JSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={"detail": "Invalid authentication credentials"},
            )

        return await call_next(request)
