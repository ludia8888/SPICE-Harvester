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
