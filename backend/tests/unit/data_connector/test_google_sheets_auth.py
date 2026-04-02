from __future__ import annotations

import pytest

from data_connector.google_sheets.auth import GoogleOAuth2Client


@pytest.mark.asyncio
async def test_get_valid_access_token_refreshes_when_expires_at_is_malformed() -> None:
    client = GoogleOAuth2Client(client_id="cid", client_secret="secret", redirect_uri="http://localhost")
    client.store_user_token(
        "user-1",
        {
            "access_token": "stale-token",
            "refresh_token": "refresh-1",
            "expires_at": "not-an-iso-timestamp",
        },
    )

    refreshed = {
        "access_token": "fresh-token",
        "refresh_token": "refresh-1",
        "expires_at": "2099-01-01T00:00:00+00:00",
    }

    async def _refresh(refresh_token: str) -> dict[str, str]:
        assert refresh_token == "refresh-1"
        return refreshed

    client.refresh_access_token = _refresh  # type: ignore[assignment]

    access_token = await client.get_valid_access_token("user-1")

    assert access_token == "fresh-token"
    assert client.get_user_token("user-1") == refreshed


@pytest.mark.asyncio
async def test_get_valid_access_token_returns_none_for_malformed_expiry_without_refresh_token() -> None:
    client = GoogleOAuth2Client(client_id="cid", client_secret="secret", redirect_uri="http://localhost")
    client.store_user_token(
        "user-2",
        {
            "access_token": "stale-token",
            "expires_at": "not-an-iso-timestamp",
        },
    )

    access_token = await client.get_valid_access_token("user-2")

    assert access_token is None
