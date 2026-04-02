import grpc
import httpx
import pytest

from bff.services.oms_client import OMSClient
from shared.errors.infra_errors import UpstreamUnavailableError


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


@pytest.mark.asyncio
async def test_oms_client_http_helpers_wrap_transport_errors() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom", request=request)

    oms = OMSClient(base_url="http://test")
    await oms.client.aclose()
    oms.client = httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(handler))

    with pytest.raises(UpstreamUnavailableError) as exc_info:
        await oms.get("/api/v1/ping")

    await oms.client.aclose()

    assert exc_info.value.service == "oms"
    assert exc_info.value.operation == "GET"
    assert exc_info.value.path == "/api/v1/ping"
    assert isinstance(exc_info.value.__cause__, httpx.ConnectError)


@pytest.mark.asyncio
async def test_oms_client_grpc_helpers_wrap_transport_errors() -> None:
    class _FakeRpcError(grpc.RpcError):
        pass

    class _FakeGrpcClient:
        async def call_unary(self, rpc_name: str, request: object) -> object:
            _ = rpc_name, request
            raise _FakeRpcError("boom")

    oms = OMSClient(base_url="grpc://test")
    await oms.client.aclose()
    oms.client = _FakeGrpcClient()

    with pytest.raises(UpstreamUnavailableError) as exc_info:
        await oms.get("/api/v1/ping")

    assert exc_info.value.service == "oms"
    assert exc_info.value.operation == "GenericGet"
    assert exc_info.value.path == "/api/v1/ping"
    assert isinstance(exc_info.value.__cause__, _FakeRpcError)
