"""
Google Sheets OAuth endpoints (BFF).

Composed by `bff.routers.data_connector` via router composition (Composite pattern).
"""

import logging
from typing import Any, Dict
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse

from bff.routers.data_connector_deps import get_connector_registry
from bff.routers.data_connector_ops import (
    _OAUTH_STATE_CACHE,
    _append_query_param,
    _build_google_oauth_client,
    _connector_oauth_enabled,
)
from shared.middleware.rate_limiter import RateLimitPresets, rate_limit
from shared.models.requests import ApiResponse
from shared.observability.tracing import trace_endpoint
from shared.security.input_sanitizer import sanitize_input
from shared.services.registries.connector_registry import ConnectorRegistry

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Data Connectors"])


@router.post(
    "/oauth/start",
    response_model=Dict[str, Any],
    summary="Start Google Sheets OAuth flow",
)
@rate_limit(**RateLimitPresets.RELAXED)
@trace_endpoint("google_sheets_oauth_start")
async def start_google_sheets_oauth(
    payload: Dict[str, Any],
    http_request: Request,
) -> Dict[str, Any]:
    oauth_client = _build_google_oauth_client()
    if not _connector_oauth_enabled(oauth_client):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Google OAuth is not configured")
    if not oauth_client.redirect_uri:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Google OAuth redirect URI is missing")

    sanitized = sanitize_input(payload or {})
    state = uuid4().hex
    redirect_uri = str(sanitized.get("redirect_uri") or "").strip()
    label = str(sanitized.get("label") or "").strip() or None
    db_name = str(sanitized.get("db_name") or "").strip() or None
    requested_branch = str(sanitized.get("branch") or "").strip() or None

    _OAUTH_STATE_CACHE[state] = {
        "redirect_uri": redirect_uri,
        "label": label,
        "db_name": db_name,
        "branch": requested_branch,
    }

    auth_url = oauth_client.get_authorization_url(state)
    return ApiResponse.success(
        message="OAuth flow started",
        data={"authorization_url": auth_url, "state": state},
    ).to_dict()


@router.get(
    "/oauth/callback",
    summary="Google Sheets OAuth callback",
)
@rate_limit(**RateLimitPresets.RELAXED)
@trace_endpoint("google_sheets_oauth_callback")
async def google_sheets_oauth_callback(
    request: Request,
    code: str,
    state: str,
    connector_registry: ConnectorRegistry = Depends(get_connector_registry),
) -> RedirectResponse:
    oauth_client = _build_google_oauth_client()
    if not _connector_oauth_enabled(oauth_client):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Google OAuth is not configured")

    state_payload = _OAUTH_STATE_CACHE.pop(state, None)
    if state_payload is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid OAuth state")

    token_data = await oauth_client.exchange_code_for_token(code)
    expires_at = token_data.get("expires_at")
    connection_id = uuid4().hex

    config = {
        "provider": "google_sheets",
        "access_token": token_data.get("access_token"),
        "refresh_token": token_data.get("refresh_token"),
        "expires_at": expires_at,
        "label": state_payload.get("label"),
        "db_name": state_payload.get("db_name"),
        "branch": state_payload.get("branch"),
    }
    await connector_registry.upsert_source(
        source_type="google_sheets_connection",
        source_id=connection_id,
        enabled=True,
        config_json=config,
    )

    redirect_url = state_payload.get("redirect_uri") or "/"
    redirect_url = _append_query_param(redirect_url, "connection_id", connection_id)
    return RedirectResponse(url=redirect_url)

