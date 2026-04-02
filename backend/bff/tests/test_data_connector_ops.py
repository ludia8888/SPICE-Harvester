from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from bff.routers import data_connector_ops


@dataclass
class _Source:
    source_type: str
    source_id: str
    enabled: bool = True
    config_json: dict[str, Any] | None = None


class _ConnectorRegistry:
    def __init__(self, *, source: _Source, secrets: dict[str, Any]) -> None:
        self.source = source
        self.secrets = dict(secrets)
        self.upserts: list[dict[str, Any]] = []

    async def get_source(self, *, source_type: str, source_id: str) -> _Source | None:
        if source_type == self.source.source_type and source_id == self.source.source_id:
            return self.source
        return None

    async def get_connection_secrets(self, *, source_type: str, source_id: str) -> dict[str, Any]:
        _ = source_type, source_id
        return dict(self.secrets)

    async def upsert_connection_secrets(self, *, source_type: str, source_id: str, secrets_json: dict[str, Any]) -> dict[str, Any]:
        _ = source_type, source_id
        self.secrets = dict(secrets_json)
        self.upserts.append(dict(secrets_json))
        return dict(self.secrets)


class _OAuthClient:
    def __init__(self) -> None:
        self.refresh_calls = 0

    def is_token_expired(self, expires_at: float) -> bool:
        _ = expires_at
        return False

    async def refresh_access_token(self, refresh_token: str) -> dict[str, Any]:
        self.refresh_calls += 1
        return {
            "access_token": f"refreshed-{refresh_token}",
            "expires_at": 3600.0,
            "refresh_token": refresh_token,
        }


@pytest.mark.asyncio
async def test_resolve_google_connection_refreshes_when_expires_at_is_malformed() -> None:
    registry = _ConnectorRegistry(
        source=_Source(source_type="google_sheets_connection", source_id="conn-1"),
        secrets={
            "access_token": "cached-token",
            "expires_at": "not-a-number",
            "refresh_token": "refresh-1",
        },
    )
    oauth_client = _OAuthClient()

    source, token = await data_connector_ops._resolve_google_connection(
        connector_registry=registry,  # type: ignore[arg-type]
        oauth_client=oauth_client,  # type: ignore[arg-type]
        connection_id="conn-1",
    )

    assert source.source_id == "conn-1"
    assert token == "refreshed-refresh-1"
    assert oauth_client.refresh_calls == 1
    assert registry.upserts[-1]["access_token"] == "refreshed-refresh-1"
    assert registry.upserts[-1]["expires_at"] == 3600.0


@pytest.mark.asyncio
async def test_resolve_google_connection_clears_bad_cached_token_without_refresh_token() -> None:
    registry = _ConnectorRegistry(
        source=_Source(source_type="google_sheets_connection", source_id="conn-2"),
        secrets={
            "access_token": "cached-token",
            "expires_at": "bad-expiry",
        },
    )
    oauth_client = _OAuthClient()

    _source, token = await data_connector_ops._resolve_google_connection(
        connector_registry=registry,  # type: ignore[arg-type]
        oauth_client=oauth_client,  # type: ignore[arg-type]
        connection_id="conn-2",
    )

    assert token is None
    assert oauth_client.refresh_calls == 0
    assert registry.upserts[-1]["access_token"] is None
    assert registry.upserts[-1]["expires_at"] is None
