"""
Admin router dependency providers.

This module keeps auth-related dependencies separated so `bff.routers.admin`
can stay a small composition root (Composite/router composition).
"""

from __future__ import annotations

import hmac
import logging
from typing import Tuple

from fastapi import HTTPException, Request, status

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
