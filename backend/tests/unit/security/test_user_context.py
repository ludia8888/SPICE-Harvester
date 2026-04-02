from __future__ import annotations

import pytest
import httpx

import shared.security.user_context as user_context_module
from shared.security.user_context import UserTokenUnavailableError, _fetch_jwks


class _FailingAsyncClient:
    async def __aenter__(self) -> "_FailingAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    async def get(self, url: str):  # noqa: ANN001
        request = httpx.Request("GET", url)
        raise httpx.ConnectError("jwks down", request=request)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_jwks_wraps_transport_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    user_context_module._jwks_cache.clear()
    monkeypatch.setattr(user_context_module.httpx, "AsyncClient", lambda timeout=10.0: _FailingAsyncClient())

    with pytest.raises(UserTokenUnavailableError):
        await _fetch_jwks("https://example.invalid/jwks.json")
