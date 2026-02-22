"""
Admin router dependency providers.

This module keeps auth-related dependencies separated so `bff.routers.admin`
can stay a small composition root (Composite/router composition).
"""


import hmac
import logging
from typing import Tuple

from fastapi import Request, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from shared.config.settings import get_settings
from shared.security.auth_utils import extract_presented_token, get_expected_token

logger = logging.getLogger(__name__)

_ADMIN_TOKEN_ENV_KEYS: Tuple[str, ...] = ("BFF_ADMIN_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN")


async def require_admin(request: Request) -> None:
    """
    Minimal admin guard for operational endpoints.

    Contract:
    - Requires a shared secret token in `X-Admin-Token` or `Authorization: Bearer ...`.
    - If token is not configured, admin endpoints are effectively disabled.
    """
    settings = get_settings()
    if settings.is_development and settings.auth.dev_master_auth_enabled:
        actor = (request.headers.get("X-Admin-Actor") or str(settings.auth.dev_master_user_id or "dev-admin")).strip()
        request.state.admin_actor = actor or "dev-admin"
        request.state.dev_master_auth = True
        return

    expected = get_expected_token(_ADMIN_TOKEN_ENV_KEYS)
    if not expected:
        raise classified_http_exception(
            status.HTTP_403_FORBIDDEN,
            "Admin endpoints are disabled (set BFF_ADMIN_TOKEN to enable)",
            code=ErrorCode.PERMISSION_DENIED,
        )

    presented = extract_presented_token(request.headers)
    if not presented or not hmac.compare_digest(presented, expected):
        raise classified_http_exception(status.HTTP_403_FORBIDDEN, "Admin authorization failed", code=ErrorCode.PERMISSION_DENIED)

    actor = (request.headers.get("X-Admin-Actor") or "admin").strip() or "admin"
    request.state.admin_actor = actor


async def require_admin_strict(request: Request) -> None:
    """
    Strict admin guard for sensitive endpoints.

    Differences from `require_admin`:
    - Missing credentials -> 401 (not 403)
    - No dev-master bypass (must present credentials)
    """
    settings = get_settings()
    auth = settings.auth

    expected_tokens = auth.bff_expected_tokens
    if not expected_tokens:
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Admin auth required but no token configured",
            code=ErrorCode.AUTH_REQUIRED,
        )

    presented = extract_presented_token(request.headers)
    if not presented:
        raise classified_http_exception(
            status.HTTP_401_UNAUTHORIZED,
            "Authentication required",
            code=ErrorCode.AUTH_REQUIRED,
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not any(hmac.compare_digest(presented, expected) for expected in expected_tokens):
        raise classified_http_exception(
            status.HTTP_403_FORBIDDEN,
            "Invalid authentication credentials",
            code=ErrorCode.AUTH_INVALID,
        )

    actor = (request.headers.get("X-Admin-Actor") or "admin").strip() or "admin"
    request.state.admin_actor = actor
