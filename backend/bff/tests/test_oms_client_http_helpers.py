import httpx
import pytest

from bff.services.oms_client import OMSClient


@pytest.mark.asyncio
async def test_oms_client_http_helpers_roundtrip_json():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        return httpx.Response(200, json={"ok": True, "path": request.url.path})

    oms = OMSClient(base_url="http://test")
    await oms.client.aclose()
    oms.client = httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(handler))

    assert await oms.get("/api/v1/ping") == {"ok": True, "path": "/api/v1/ping"}
    assert await oms.post("/api/v1/ping", json={"x": 1}) == {"ok": True, "path": "/api/v1/ping"}
    assert await oms.put("/api/v1/ping", json={"x": 1}) == {"ok": True, "path": "/api/v1/ping"}
    assert await oms.delete("/api/v1/ping") == {"ok": True, "path": "/api/v1/ping"}

    await oms.client.aclose()

    assert calls == [
        ("GET", "/api/v1/ping"),
        ("POST", "/api/v1/ping"),
        ("PUT", "/api/v1/ping"),
        ("DELETE", "/api/v1/ping"),
    ]
