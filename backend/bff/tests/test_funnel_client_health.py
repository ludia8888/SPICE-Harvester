from __future__ import annotations

import json

import pytest

from bff.services.funnel_client import FunnelClient


class _FakeResponse:
    def __init__(self, *, payload: object | None = None, json_error: Exception | None = None) -> None:
        self._payload = payload
        self._json_error = json_error

    def raise_for_status(self) -> None:
        return None

    def json(self):  # noqa: ANN201
        if self._json_error is not None:
            raise self._json_error
        return self._payload


class _FakeClient:
    def __init__(self, response: _FakeResponse) -> None:
        self._response = response

    async def get(self, path: str) -> _FakeResponse:
        assert path == "/health"
        return self._response


@pytest.mark.asyncio
async def test_funnel_client_check_health_returns_true_for_healthy_payload() -> None:
    client = FunnelClient.__new__(FunnelClient)
    client.client = _FakeClient(_FakeResponse(payload={"status": "healthy"}))

    assert await client.check_health() is True


@pytest.mark.asyncio
async def test_funnel_client_check_health_returns_true_for_canonical_runtime_payload() -> None:
    client = FunnelClient.__new__(FunnelClient)
    client.client = _FakeClient(_FakeResponse(payload={"status": "ready", "ready": True}))

    assert await client.check_health() is True


@pytest.mark.asyncio
async def test_funnel_client_check_health_returns_true_for_wrapped_runtime_payload() -> None:
    client = FunnelClient.__new__(FunnelClient)
    client.client = _FakeClient(
        _FakeResponse(payload={"status": "success", "data": {"status": "ready", "ready": True}})
    )

    assert await client.check_health() is True


@pytest.mark.asyncio
async def test_funnel_client_check_health_returns_false_for_invalid_json_payload() -> None:
    client = FunnelClient.__new__(FunnelClient)
    client.client = _FakeClient(_FakeResponse(json_error=json.JSONDecodeError("bad json", "{}", 1)))

    assert await client.check_health() is False
