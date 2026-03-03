from __future__ import annotations

import asyncio

import httpx
import pytest


@pytest.mark.asyncio
async def test_oms_json_uses_grpc_transport(monkeypatch):
    from mcp_servers import pipeline_mcp_http as target

    called: dict[str, object] = {"closed": False, "http_json_called": False}

    async def _should_not_be_called(*args, **kwargs):
        called["http_json_called"] = True
        raise AssertionError("oms_json must not use direct HTTP path")

    class _DummyCompat:
        async def request(self, method: str, path: str, **kwargs):
            called["method"] = method
            called["path"] = path
            called["kwargs"] = kwargs
            return httpx.Response(
                200,
                json={"ok": True},
                request=httpx.Request(method, f"http://oms.local{path}"),
            )

        async def aclose(self) -> None:
            called["closed"] = True

    monkeypatch.setattr(target, "http_json", _should_not_be_called)
    monkeypatch.setattr(target, "OMSGrpcHttpCompatClient", _DummyCompat)

    payload = await target.oms_json(
        "POST",
        "/api/v1/database/list",
        params={"branch": "main"},
        json_body={"name": "demo"},
        timeout_seconds=0.1,
    )

    assert payload == {"ok": True}
    assert called["http_json_called"] is False
    assert called["closed"] is True
    assert called["method"] == "POST"
    assert called["path"] == "/api/v1/database/list"

    kwargs = called["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs.get("params") == {"branch": "main"}
    assert kwargs.get("json_body") == {"name": "demo"}
    headers = kwargs.get("headers") or {}
    assert isinstance(headers, dict)
    assert headers.get("Accept") == "application/json"


@pytest.mark.asyncio
async def test_oms_json_timeout_maps_to_504(monkeypatch):
    from mcp_servers import pipeline_mcp_http as target

    class _SlowCompat:
        async def request(self, method: str, path: str, **kwargs):
            await asyncio.sleep(0.2)
            return httpx.Response(
                200,
                json={"ok": True},
                request=httpx.Request(method, f"http://oms.local{path}"),
            )

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(target, "OMSGrpcHttpCompatClient", _SlowCompat)

    payload = await target.oms_json("GET", "/api/v1/database/list", timeout_seconds=0.01)
    assert payload.get("status_code") == 504
    assert "timed out" in str(payload.get("error", "")).lower()
