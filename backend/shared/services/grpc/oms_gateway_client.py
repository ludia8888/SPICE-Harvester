from __future__ import annotations

import json
from typing import Any, Iterable, Mapping, Optional

import grpc
import httpx

from shared.config.settings import get_settings
from shared.generated.grpc.spice.oms.v1 import oms_gateway_pb2, oms_gateway_pb2_grpc

_DEFAULT_MAX_MESSAGE_BYTES = 64 * 1024 * 1024


def _read_file_bytes(path: str | None) -> bytes | None:
    target = str(path or "").strip()
    if not target:
        return None
    with open(target, "rb") as handle:
        return handle.read()


def _infer_service_token() -> str:
    settings = get_settings()
    auth = settings.auth
    effective = str(auth.oms_grpc_service_token_effective or "").strip()
    if effective:
        return effective

    if settings.agent.bff_token:
        return str(settings.agent.bff_token).strip()

    candidates: Iterable[str] = (
        *auth.oms_grpc_expected_service_tokens,
    )
    for token in candidates:
        cleaned = str(token or "").strip()
        if cleaned:
            return cleaned
    return ""


def _build_grpc_channel(target: str) -> grpc.aio.Channel:
    settings = get_settings()
    services = settings.services
    options = [
        ("grpc.max_send_message_length", _DEFAULT_MAX_MESSAGE_BYTES),
        ("grpc.max_receive_message_length", _DEFAULT_MAX_MESSAGE_BYTES),
    ]
    if not services.oms_grpc_use_tls:
        return grpc.aio.insecure_channel(target, options=options)

    ca_path = services.oms_grpc_client_ca_path or services.ssl_ca_path
    cert_path = services.oms_grpc_client_cert_path
    key_path = services.oms_grpc_client_key_path

    root_ca = _read_file_bytes(ca_path)
    cert_chain = _read_file_bytes(cert_path) if cert_path else None
    private_key = _read_file_bytes(key_path) if key_path else None

    credentials = grpc.ssl_channel_credentials(
        root_certificates=root_ca,
        private_key=private_key,
        certificate_chain=cert_chain,
    )
    if services.oms_grpc_server_name:
        options.append(("grpc.ssl_target_name_override", services.oms_grpc_server_name))
    return grpc.aio.secure_channel(target, credentials, options=options)


def _request_metadata_from_inputs(
    *,
    headers: Optional[Mapping[str, str]] = None,
    query: Optional[Mapping[str, Any]] = None,
) -> oms_gateway_pb2.RequestMetadata:
    metadata = oms_gateway_pb2.RequestMetadata()
    if headers:
        metadata.headers.update({str(k): str(v) for k, v in headers.items() if str(k).strip()})
    if query:
        metadata.query.update({str(k): str(v) for k, v in query.items() if str(k).strip() and v is not None})
    return metadata


def _httpx_response_from_grpc(
    *,
    method: str,
    path: str,
    response: oms_gateway_pb2.OmsResponse,
) -> httpx.Response:
    body: bytes
    if response.binary_body:
        body = bytes(response.binary_body)
    elif response.json_body:
        body = response.json_body.encode("utf-8")
    else:
        body = b""

    content_type = str(response.content_type or "").strip() or None
    headers = dict(response.headers or {})
    if content_type:
        headers.setdefault("content-type", content_type)

    request = httpx.Request(method.upper(), f"http://oms.local{path}")
    return httpx.Response(
        status_code=int(response.status_code or 500),
        content=body,
        headers=headers,
        request=request,
    )


class OMSGatewayGrpcClient:
    def __init__(self, *, target: Optional[str] = None) -> None:
        settings = get_settings()
        self._target = str(target or settings.services.oms_grpc_target).strip()
        if not self._target:
            raise ValueError("OMS gRPC target is required")
        self._channel = _build_grpc_channel(self._target)
        self._stub = oms_gateway_pb2_grpc.OmsGatewayServiceStub(self._channel)
        self._service_token = _infer_service_token()
        self._timeout_seconds = float(settings.clients.oms_client_timeout_seconds)

    async def close(self) -> None:
        await self._channel.close()

    async def aclose(self) -> None:
        await self.close()

    def _call_metadata(self, *, headers: Optional[Mapping[str, str]] = None) -> list[tuple[str, str]]:
        metadata: list[tuple[str, str]] = []
        token = self._service_token
        if token:
            metadata.append(("x-service-token", token))
        if headers:
            delegated = headers.get("X-Delegated-Authorization") or headers.get("x-delegated-authorization")
            if delegated:
                metadata.append(("x-delegated-authorization", str(delegated)))
        return metadata

    async def call_unary(self, rpc_name: str, request: Any) -> oms_gateway_pb2.OmsResponse:
        rpc = getattr(self._stub, rpc_name)
        metadata_obj = getattr(request, "metadata", None)
        metadata_headers = dict(getattr(metadata_obj, "headers", {}) or {})
        return await rpc(
            request,
            metadata=self._call_metadata(headers=metadata_headers),
            timeout=self._timeout_seconds,
        )

    async def call_stream(self, rpc_name: str, request: Any) -> list[oms_gateway_pb2.OmsStreamChunk]:
        rpc = getattr(self._stub, rpc_name)
        chunks: list[oms_gateway_pb2.OmsStreamChunk] = []
        metadata_obj = getattr(request, "metadata", None)
        metadata_headers = dict(getattr(metadata_obj, "headers", {}) or {})
        async for item in rpc(
            request,
            metadata=self._call_metadata(headers=metadata_headers),
            timeout=self._timeout_seconds,
        ):
            chunks.append(item)
        return chunks

    @staticmethod
    def build_request(
        *,
        json_body: Optional[Any] = None,
        binary_body: Optional[bytes] = None,
        headers: Optional[Mapping[str, str]] = None,
        query: Optional[Mapping[str, Any]] = None,
        **fields: str,
    ) -> oms_gateway_pb2.OmsRequest:
        request = oms_gateway_pb2.OmsRequest()
        for key, value in fields.items():
            if hasattr(request, key) and value is not None:
                setattr(request, key, str(value))
        request.metadata.CopyFrom(_request_metadata_from_inputs(headers=headers, query=query))
        if json_body is not None:
            request.json_body = json.dumps(json_body, ensure_ascii=False)
        if binary_body is not None:
            request.binary_body = binary_body
        return request

    @staticmethod
    def parse_json_response(response: oms_gateway_pb2.OmsResponse) -> Any:
        body = str(response.json_body or "").strip()
        if not body:
            return {}
        return json.loads(body)

    @staticmethod
    def to_httpx_response(method: str, path: str, response: oms_gateway_pb2.OmsResponse) -> httpx.Response:
        return _httpx_response_from_grpc(method=method, path=path, response=response)


class OMSGrpcHttpCompatClient:
    """HTTPX-compatible subset client backed by OMS gRPC bridge."""

    def __init__(self, grpc_client: Optional[OMSGatewayGrpcClient] = None) -> None:
        self._grpc_client = grpc_client or OMSGatewayGrpcClient()

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        json_body: Optional[Any] = None,
        content: Optional[bytes] = None,
    ) -> httpx.Response:
        parsed = httpx.URL(str(path))
        normalized_path = parsed.path or "/"
        normalized_params: dict[str, Any] = dict(params or {})
        for key, value in parsed.params.multi_items():
            normalized_params.setdefault(str(key), value)

        rpc_name_by_method = {
            "GET": "GenericGet",
            "POST": "GenericPost",
            "PUT": "GenericPut",
            "DELETE": "GenericDelete",
        }
        verb = str(method or "GET").strip().upper()
        rpc_name = rpc_name_by_method.get(verb)
        if not rpc_name:
            raise ValueError(f"Unsupported HTTP verb for gRPC bridge: {verb}")
        req = OMSGatewayGrpcClient.build_request(
            path=normalized_path,
            query=normalized_params,
            headers=headers,
            json_body=json_body,
            binary_body=content,
        )
        grpc_resp = await self._grpc_client.call_unary(rpc_name, req)
        return _httpx_response_from_grpc(method=verb, path=normalized_path, response=grpc_resp)

    async def get(self, path: str, *, params: Optional[Mapping[str, Any]] = None, headers: Optional[Mapping[str, str]] = None) -> httpx.Response:
        return await self.request("GET", path, params=params, headers=headers)

    async def get_ontology_typed(
        self,
        *,
        db_name: str,
        class_id: str,
        branch: str = "main",
        headers: Optional[Mapping[str, str]] = None,
    ) -> httpx.Response:
        path = f"/api/v1/database/{db_name}/ontology/{class_id}"
        typed_request = oms_gateway_pb2.GetOntologyTypedRequest(
            db_name=str(db_name),
            class_id=str(class_id),
            branch=str(branch or "main"),
        )
        typed_request.metadata.CopyFrom(_request_metadata_from_inputs(headers=headers))
        try:
            grpc_resp = await self._grpc_client.call_unary("GetOntologyTyped", typed_request)
            return _httpx_response_from_grpc(method="GET", path=path, response=grpc_resp)
        except Exception as exc:
            code_func = getattr(exc, "code", None)
            status_code = code_func() if callable(code_func) else None
            if status_code == grpc.StatusCode.UNIMPLEMENTED:
                return await self.get(path, params={"branch": branch}, headers=headers)
            raise

    async def post(
        self,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        json: Optional[Any] = None,
        headers: Optional[Mapping[str, str]] = None,
        content: Optional[bytes] = None,
    ) -> httpx.Response:
        return await self.request("POST", path, params=params, json_body=json, headers=headers, content=content)

    async def put(
        self,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        json: Optional[Any] = None,
        headers: Optional[Mapping[str, str]] = None,
    ) -> httpx.Response:
        return await self.request("PUT", path, params=params, json_body=json, headers=headers)

    async def delete(self, path: str, *, params: Optional[Mapping[str, Any]] = None, headers: Optional[Mapping[str, str]] = None) -> httpx.Response:
        return await self.request("DELETE", path, params=params, headers=headers)

    async def aclose(self) -> None:
        await self._grpc_client.close()
