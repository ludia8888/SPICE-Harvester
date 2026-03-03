from __future__ import annotations

import json

import grpc
import pytest
from fastapi import FastAPI

from oms.grpc.server import OmsGatewayServicer
from shared.generated.grpc.spice.oms.v1 import oms_gateway_pb2
from shared.services.grpc.oms_gateway_client import OMSGatewayGrpcClient


class _AbortCalled(Exception):
    pass


class _FakeContext:
    def __init__(self, *, metadata: list[tuple[str, str]] | None = None, auth_ctx: dict | None = None) -> None:
        self._metadata = metadata or []
        self._auth_ctx = auth_ctx or {}
        self.aborted: tuple[grpc.StatusCode, str] | None = None

    def invocation_metadata(self):
        return self._metadata

    def auth_context(self):
        return self._auth_ctx

    async def abort(self, code: grpc.StatusCode, details: str):
        self.aborted = (code, details)
        raise _AbortCalled()


class _DummyStreamResponse:
    def __init__(self, *, status_code: int, headers: dict[str, str], chunks: list[bytes]) -> None:
        self.status_code = status_code
        self.headers = headers
        self._chunks = chunks

    async def aiter_bytes(self, chunk_size: int = 65536):
        _ = chunk_size
        for chunk in self._chunks:
            yield chunk

    async def aread(self) -> bytes:
        return b"".join(self._chunks)


class _DummyStreamContext:
    def __init__(self, response: _DummyStreamResponse) -> None:
        self._response = response

    async def __aenter__(self) -> _DummyStreamResponse:
        return self._response

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _DummyHttpClient:
    def __init__(self, response: _DummyStreamResponse) -> None:
        self._response = response

    def stream(self, **kwargs):  # noqa: ANN003
        _ = kwargs
        return _DummyStreamContext(self._response)

    async def aclose(self) -> None:
        return None


@pytest.mark.asyncio
async def test_grpc_authorize_rejects_invalid_service_token():
    servicer = OmsGatewayServicer(FastAPI(), require_mtls=False, expected_service_tokens=("token-ok",))
    try:
        ctx = _FakeContext(metadata=[("x-service-token", "bad-token")])
        with pytest.raises(_AbortCalled):
            await servicer._authorize(ctx)
        assert ctx.aborted is not None
        assert ctx.aborted[0] == grpc.StatusCode.UNAUTHENTICATED
        assert "invalid service token" in ctx.aborted[1]
    finally:
        await servicer.close()


@pytest.mark.asyncio
async def test_grpc_authorize_requires_mtls_when_enabled():
    servicer = OmsGatewayServicer(FastAPI(), require_mtls=True, expected_service_tokens=("token-ok",))
    try:
        ctx = _FakeContext(metadata=[("x-service-token", "token-ok")], auth_ctx={})
        with pytest.raises(_AbortCalled):
            await servicer._authorize(ctx)
        assert ctx.aborted is not None
        assert ctx.aborted[0] == grpc.StatusCode.PERMISSION_DENIED
        assert "certificate" in ctx.aborted[1].lower()
    finally:
        await servicer.close()


def test_gateway_build_request_and_response_mapping_roundtrip():
    request = OMSGatewayGrpcClient.build_request(
        db_name="demo",
        class_id="Order",
        json_body={"x": 1, "ok": True},
        query={"limit": 10},
        headers={"X-Request-Id": "req-1"},
    )
    assert request.db_name == "demo"
    assert request.class_id == "Order"
    assert request.metadata.query["limit"] == "10"
    assert request.metadata.headers["X-Request-Id"] == "req-1"
    assert json.loads(request.json_body) == {"x": 1, "ok": True}

    grpc_response = oms_gateway_pb2.OmsResponse(
        status_code=409,
        json_body='{"detail":"conflict"}',
        content_type="application/json",
        headers={"x-correlation-id": "corr-1"},
    )
    http_response = OMSGatewayGrpcClient.to_httpx_response("POST", "/api/v1/database/create", grpc_response)
    assert http_response.status_code == 409
    assert http_response.headers.get("x-correlation-id") == "corr-1"
    assert http_response.json().get("detail") == "conflict"


def test_gateway_call_metadata_includes_service_and_delegated_auth():
    client = OMSGatewayGrpcClient.__new__(OMSGatewayGrpcClient)
    client._service_token = "svc-token"  # type: ignore[attr-defined]

    metadata = client._call_metadata(headers={"X-Delegated-Authorization": "Bearer user-jwt"})  # type: ignore[attr-defined]
    assert ("x-service-token", "svc-token") in metadata
    assert ("x-delegated-authorization", "Bearer user-jwt") in metadata


@pytest.mark.asyncio
async def test_get_database_returns_selected_row(monkeypatch: pytest.MonkeyPatch):
    servicer = OmsGatewayServicer(FastAPI(), require_mtls=False, expected_service_tokens=())
    try:
        async def _fake_dispatch(*, context, method, path, request):  # noqa: ANN001
            _ = context
            _ = request
            assert method == "GET"
            assert path == "/api/v1/database/list"
            return oms_gateway_pb2.OmsResponse(
                status_code=200,
                json_body=json.dumps({"data": {"databases": [{"name": "qa_db", "description": "demo"}]}}),
                headers={"content-type": "application/json"},
                content_type="application/json",
            )

        monkeypatch.setattr(servicer, "_dispatch", _fake_dispatch)

        response = await servicer.GetDatabase(oms_gateway_pb2.OmsRequest(db_name="qa_db"), _FakeContext())
        payload = json.loads(response.json_body)
        assert response.status_code == 200
        assert payload["status"] == "success"
        assert payload["data"]["name"] == "qa_db"
        assert payload["data"]["description"] == "demo"
    finally:
        await servicer.close()


@pytest.mark.asyncio
async def test_get_database_returns_404_when_missing(monkeypatch: pytest.MonkeyPatch):
    servicer = OmsGatewayServicer(FastAPI(), require_mtls=False, expected_service_tokens=())
    try:
        async def _fake_dispatch(*, context, method, path, request):  # noqa: ANN001
            _ = context
            _ = method
            _ = path
            _ = request
            return oms_gateway_pb2.OmsResponse(
                status_code=200,
                json_body=json.dumps({"data": {"databases": [{"name": "other_db"}]}}),
                headers={"content-type": "application/json"},
                content_type="application/json",
            )

        monkeypatch.setattr(servicer, "_dispatch", _fake_dispatch)

        response = await servicer.GetDatabase(oms_gateway_pb2.OmsRequest(db_name="missing_db"), _FakeContext())
        assert response.status_code == 404
        assert "missing_db" in response.detail
    finally:
        await servicer.close()


@pytest.mark.asyncio
async def test_dispatch_stream_emits_multiple_chunks(monkeypatch: pytest.MonkeyPatch):
    servicer = OmsGatewayServicer(FastAPI(), require_mtls=False, expected_service_tokens=())
    try:
        async def _allow(_context):  # noqa: ANN001
            return None

        monkeypatch.setattr(servicer, "_authorize", _allow)
        servicer._http = _DummyHttpClient(
            _DummyStreamResponse(
                status_code=200,
                headers={"content-type": "application/x-ndjson"},
                chunks=[b'{"a":1}\n', b'{"a":2}\n'],
            )
        )

        request = oms_gateway_pb2.OmsRequest()
        chunks = []
        async for chunk in servicer._dispatch_stream(
            context=_FakeContext(),
            method="POST",
            path="/api/v2/ontologies/db/objects/O/1/timeseries/p/streamPoints",
            request=request,
            chunk_size=4,
        ):
            chunks.append(chunk)

        data_chunks = [c for c in chunks if c.chunk]
        assert len(data_chunks) == 2
        assert data_chunks[0].headers.get("content-type") == "application/x-ndjson"
        assert chunks[-1].eof is True
    finally:
        await servicer.close()
