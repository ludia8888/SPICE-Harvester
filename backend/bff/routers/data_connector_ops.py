"""Data connector helpers (BFF).

This module acts as a small Facade over helper logic shared by the data
connector endpoint routers. Keeping logic here supports router composition
(Composite pattern) and keeps subrouters focused.
"""

import logging
from typing import Any, Dict, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from fastapi import status
from shared.errors.error_types import ErrorCode, classified_http_exception

from data_connector.google_sheets.auth import GoogleOAuth2Client
from shared.services.registries.connector_registry import ConnectorRegistry

logger = logging.getLogger(__name__)

_OAUTH_STATE_CACHE: Dict[str, Dict[str, Any]] = {}


def _build_google_oauth_client() -> GoogleOAuth2Client:
    return GoogleOAuth2Client()


def _connector_oauth_enabled(oauth_client: GoogleOAuth2Client) -> bool:
    return bool(oauth_client.client_id and oauth_client.client_secret and oauth_client.redirect_uri)


def _append_query_param(url: str, key: str, value: str) -> str:
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    params[key] = [value]
    return urlunparse(parsed._replace(query=urlencode(params, doseq=True)))


async def _resolve_google_connection(
    *,
    connector_registry: ConnectorRegistry,
    oauth_client: GoogleOAuth2Client,
    connection_id: str,
) -> Tuple[Any, Optional[str]]:
    connection_id = (connection_id or "").strip()
    if not connection_id:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "connection_id is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)

    source = await connector_registry.get_source(
        source_type="google_sheets_connection",
        source_id=connection_id,
    )
    if not source or not source.enabled:
        raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Connection not found", code=ErrorCode.RESOURCE_NOT_FOUND)

    config = source.config_json or {}
    token = config.get("access_token")
    expires_at = config.get("expires_at")
    refresh_token = config.get("refresh_token")

    if token and expires_at:
        try:
            expires_at_float = float(expires_at)
        except Exception:
            logging.getLogger(__name__).warning("Broad exception fallback at bff/routers/data_connector_ops.py:63", exc_info=True)
            expires_at_float = None
        if expires_at_float and oauth_client.is_token_expired(expires_at_float):
            token = None

    if not token and refresh_token:
        refreshed = await oauth_client.refresh_access_token(str(refresh_token))
        config.update(
            {
                "access_token": refreshed.get("access_token"),
                "expires_at": refreshed.get("expires_at"),
                "refresh_token": refreshed.get("refresh_token", refresh_token),
            }
        )
        await connector_registry.upsert_source(
            source_type=source.source_type,
            source_id=source.source_id,
            enabled=True,
            config_json=config,
        )
        token = config.get("access_token")

    return source, str(token) if token else None


async def _resolve_optional_access_token(
    *,
    connector_registry: ConnectorRegistry,
    connection_id: Optional[str],
) -> Optional[str]:
    if not connection_id:
        return None
    oauth_client = _build_google_oauth_client()
    _source, access_token = await _resolve_google_connection(
        connector_registry=connector_registry,
        oauth_client=oauth_client,
        connection_id=str(connection_id),
    )
    return access_token

