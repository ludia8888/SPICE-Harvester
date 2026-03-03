from __future__ import annotations

import json
import logging
import os
from typing import Any, AsyncIterator, Mapping, Optional

import grpc
import httpx
from fastapi import FastAPI
from google.protobuf.json_format import MessageToDict

from shared.config.settings import get_settings
from shared.generated.grpc.spice.oms.v1 import oms_gateway_pb2, oms_gateway_pb2_grpc

logger = logging.getLogger(__name__)

_INTERNAL_BRIDGE_HEADER = "X-OMS-Internal-Bridge"


def _as_str_dict(values: Mapping[str, Any]) -> dict[str, str]:
    return {str(k): str(v) for k, v in values.items() if str(k).strip() and v is not None}


def _metadata_to_dict(context: grpc.aio.ServicerContext) -> dict[str, str]:
    return {str(k): str(v) for k, v in context.invocation_metadata()}


def _extract_service_tokens() -> tuple[str, ...]:
    settings = get_settings()
    raw = (os.getenv("OMS_GRPC_SERVICE_TOKENS") or "").strip()
    if raw:
        return tuple(part.strip() for part in raw.split(",") if part.strip())
    auth = settings.auth
    return tuple(
        token
        for token in (
            *auth.bff_agent_tokens,
            *auth.bff_expected_tokens,
            *auth.oms_expected_tokens,
        )
        if str(token or "").strip()
    )


class OmsGatewayServicer(oms_gateway_pb2_grpc.OmsGatewayServiceServicer):
    def __init__(self, app: FastAPI, *, require_mtls: bool, expected_service_tokens: tuple[str, ...]) -> None:
        self._app = app
        self._require_mtls = bool(require_mtls)
        self._expected_service_tokens = expected_service_tokens
        self._transport = httpx.ASGITransport(app=self._app)
        self._http = httpx.AsyncClient(transport=self._transport, base_url="http://oms-internal")

    async def close(self) -> None:
        await self._http.aclose()

    async def _authorize(self, context: grpc.aio.ServicerContext) -> None:
        metadata = _metadata_to_dict(context)
        presented = (metadata.get("x-service-token") or "").strip()
        if self._expected_service_tokens and presented not in self._expected_service_tokens:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid service token")

        if self._require_mtls:
            auth_context = context.auth_context()
            has_peer_cert = bool(auth_context.get("x509_common_name"))
            if not has_peer_cert:
                await context.abort(grpc.StatusCode.PERMISSION_DENIED, "mTLS client certificate required")

    async def _dispatch(
        self,
        *,
        context: grpc.aio.ServicerContext,
        method: str,
        path: str,
        request: oms_gateway_pb2.OmsRequest,
    ) -> oms_gateway_pb2.OmsResponse:
        await self._authorize(context)

        headers = _as_str_dict(request.metadata.headers)
        headers[_INTERNAL_BRIDGE_HEADER] = "1"
        query = _as_str_dict(request.metadata.query)

        try:
            json_body: Optional[Any] = None
            if request.json_body:
                json_body = json.loads(request.json_body)
        except json.JSONDecodeError as exc:
            return oms_gateway_pb2.OmsResponse(
                status_code=400,
                detail=f"invalid json_body: {exc}",
                content_type="application/json",
            )

        response = await self._http.request(
            method=method.upper(),
            url=path,
            params=query or None,
            headers=headers or None,
            json=json_body,
            content=(bytes(request.binary_body) if request.binary_body else None),
        )
        content_type = response.headers.get("content-type", "")
        wire_headers = _as_str_dict(response.headers)
        if "application/json" in content_type:
            json_body = response.text if response.text else ""
            return oms_gateway_pb2.OmsResponse(
                status_code=int(response.status_code),
                json_body=json_body,
                headers=wire_headers,
                content_type=content_type,
                detail=str((response.json() or {}).get("detail") if response.text else ""),
            )
        return oms_gateway_pb2.OmsResponse(
            status_code=int(response.status_code),
            binary_body=response.content or b"",
            headers=wire_headers,
            content_type=content_type or "application/octet-stream",
            detail=response.text if response.status_code >= 400 else "",
        )

    # Generic passthrough
    async def GenericGet(self, request, context):
        return await self._dispatch(context=context, method="GET", path=request.path or "/", request=request)

    async def GenericPost(self, request, context):
        return await self._dispatch(context=context, method="POST", path=request.path or "/", request=request)

    async def GenericPut(self, request, context):
        return await self._dispatch(context=context, method="PUT", path=request.path or "/", request=request)

    async def GenericDelete(self, request, context):
        return await self._dispatch(context=context, method="DELETE", path=request.path or "/", request=request)

    # DatabaseService
    async def CheckHealth(self, request, context):
        return await self._dispatch(context=context, method="GET", path="/health", request=request)

    async def ListDatabases(self, request, context):
        return await self._dispatch(context=context, method="GET", path="/api/v1/database/list", request=request)

    async def CreateDatabase(self, request, context):
        return await self._dispatch(context=context, method="POST", path="/api/v1/database/create", request=request)

    async def DeleteDatabase(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["expected_seq"] = request.expected_seq
        return await self._dispatch(
            context=context,
            method="DELETE",
            path=f"/api/v1/database/{request.db_name}",
            request=req,
        )

    async def GetDatabase(self, request, context):
        response = await self._dispatch(
            context=context,
            method="GET",
            path="/api/v1/database/list",
            request=request,
        )
        if int(response.status_code or 500) >= 400:
            return response

        db_name = str(request.db_name or "").strip()
        payload: dict[str, Any]
        try:
            payload = json.loads(response.json_body or "{}")
        except json.JSONDecodeError:
            payload = {}

        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        rows = data.get("databases") if isinstance(data.get("databases"), list) else []
        row = next(
            (
                item
                for item in rows
                if isinstance(item, dict) and str(item.get("name") or "").strip() == db_name
            ),
            None,
        )
        if row is None:
            return oms_gateway_pb2.OmsResponse(
                status_code=404,
                json_body=json.dumps(
                    {"detail": f"Database '{db_name}' not found"},
                    ensure_ascii=False,
                ),
                headers={"content-type": "application/json"},
                content_type="application/json",
                detail=f"Database '{db_name}' not found",
            )

        return oms_gateway_pb2.OmsResponse(
            status_code=200,
            json_body=json.dumps(
                {
                    "status": "success",
                    "message": f"데이터베이스 '{db_name}' 조회 완료",
                    "data": {"name": db_name, **row},
                },
                ensure_ascii=False,
            ),
            headers=dict(response.headers or {}),
            content_type="application/json",
        )

    async def DatabaseExists(self, request, context):
        return await self.GetDatabase(request, context)

    # OntologyService
    async def CreateOntologyTyped(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.db_name = request.db_name
        req.branch = request.branch or "main"
        req.metadata.CopyFrom(request.metadata)
        if request.ontology and request.ontology.fields:
            req.json_body = json.dumps(
                MessageToDict(request.ontology, preserving_proto_field_name=True),
                ensure_ascii=False,
            )
        return await self.CreateOntology(req, context)

    async def GetOntologyTyped(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.db_name = request.db_name
        req.class_id = request.class_id
        req.branch = request.branch or "main"
        req.metadata.CopyFrom(request.metadata)
        return await self.GetOntology(req, context)

    async def CreateOntology(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["branch"] = request.branch or request.metadata.query.get("branch", "main")
        return await self._dispatch(
            context=context,
            method="POST",
            path=f"/api/v1/database/{request.db_name}/ontology",
            request=req,
        )

    async def GetOntology(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["branch"] = request.branch or request.metadata.query.get("branch", "main")
        return await self._dispatch(
            context=context,
            method="GET",
            path=f"/api/v1/database/{request.db_name}/ontology/{request.class_id}",
            request=req,
        )

    async def ListOntologies(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["branch"] = request.branch or request.metadata.query.get("branch", "main")
        return await self._dispatch(
            context=context,
            method="GET",
            path=f"/api/v1/database/{request.db_name}/ontology",
            request=req,
        )

    async def UpdateOntology(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["branch"] = request.branch or request.metadata.query.get("branch", "main")
        req.metadata.query["expected_seq"] = request.expected_seq
        return await self._dispatch(
            context=context,
            method="PUT",
            path=f"/api/v1/database/{request.db_name}/ontology/{request.class_id}",
            request=req,
        )

    async def DeleteOntology(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["branch"] = request.branch or request.metadata.query.get("branch", "main")
        req.metadata.query["expected_seq"] = request.expected_seq
        return await self._dispatch(
            context=context,
            method="DELETE",
            path=f"/api/v1/database/{request.db_name}/ontology/{request.class_id}",
            request=req,
        )

    async def ListOntologyResources(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["branch"] = request.branch or request.metadata.query.get("branch", "main")
        path = f"/api/v1/database/{request.db_name}/ontology/resources"
        if request.resource_type:
            path = f"{path}/{request.resource_type}"
        return await self._dispatch(context=context, method="GET", path=path, request=req)

    async def GetOntologyResource(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["branch"] = request.branch or request.metadata.query.get("branch", "main")
        return await self._dispatch(
            context=context,
            method="GET",
            path=f"/api/v1/database/{request.db_name}/ontology/resources/{request.resource_type}/{request.resource_id}",
            request=req,
        )

    async def CreateOntologyResource(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["branch"] = request.branch or request.metadata.query.get("branch", "main")
        if request.expected_head_commit:
            req.metadata.query["expected_head_commit"] = request.expected_head_commit
        return await self._dispatch(
            context=context,
            method="POST",
            path=f"/api/v1/database/{request.db_name}/ontology/resources/{request.resource_type}",
            request=req,
        )

    async def UpdateOntologyResource(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["branch"] = request.branch or request.metadata.query.get("branch", "main")
        if request.expected_head_commit:
            req.metadata.query["expected_head_commit"] = request.expected_head_commit
        return await self._dispatch(
            context=context,
            method="PUT",
            path=f"/api/v1/database/{request.db_name}/ontology/resources/{request.resource_type}/{request.resource_id}",
            request=req,
        )

    async def DeleteOntologyResource(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["branch"] = request.branch or request.metadata.query.get("branch", "main")
        if request.expected_head_commit:
            req.metadata.query["expected_head_commit"] = request.expected_head_commit
        return await self._dispatch(
            context=context,
            method="DELETE",
            path=f"/api/v1/database/{request.db_name}/ontology/resources/{request.resource_type}/{request.resource_id}",
            request=req,
        )

    async def RecordOntologyDeployment(self, request, context):
        return await self._dispatch(
            context=context,
            method="POST",
            path=f"/api/v1/database/{request.db_name}/ontology/records/deployments",
            request=request,
        )

    # ObjectQueryService
    async def SearchObjects(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["branch"] = request.branch or request.metadata.query.get("branch", "main")
        return await self._dispatch(
            context=context,
            method="POST",
            path=f"/api/v2/ontologies/{request.db_name}/objects/{request.object_type}/search",
            request=req,
        )

    async def AggregateObjects(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["branch"] = request.branch or request.metadata.query.get("branch", "main")
        return await self._dispatch(
            context=context,
            method="POST",
            path=f"/api/v2/ontologies/{request.db_name}/objects/{request.object_type}/aggregate",
            request=req,
        )

    # TimeseriesService
    async def GetTimeseriesFirstPoint(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["branch"] = request.branch or request.metadata.query.get("branch", "main")
        path = (
            f"/api/v2/ontologies/{request.db_name}/objects/{request.object_type}/{request.primary_key}"
            f"/timeseries/{request.property_name}/firstPoint"
        )
        return await self._dispatch(context=context, method="GET", path=path, request=req)

    async def GetTimeseriesLastPoint(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["branch"] = request.branch or request.metadata.query.get("branch", "main")
        path = (
            f"/api/v2/ontologies/{request.db_name}/objects/{request.object_type}/{request.primary_key}"
            f"/timeseries/{request.property_name}/lastPoint"
        )
        return await self._dispatch(context=context, method="GET", path=path, request=req)

    async def StreamTimeseriesPoints(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["branch"] = request.branch or request.metadata.query.get("branch", "main")
        path = (
            f"/api/v2/ontologies/{request.db_name}/objects/{request.object_type}/{request.primary_key}"
            f"/timeseries/{request.property_name}/streamPoints"
        )
        async for chunk in self._dispatch_stream(context=context, method="POST", path=path, request=req):
            yield chunk

    async def _dispatch_stream(
        self,
        *,
        context: grpc.aio.ServicerContext,
        method: str,
        path: str,
        request: oms_gateway_pb2.OmsRequest,
        chunk_size: int = 64 * 1024,
    ) -> AsyncIterator[oms_gateway_pb2.OmsStreamChunk]:
        await self._authorize(context)

        headers = _as_str_dict(request.metadata.headers)
        headers[_INTERNAL_BRIDGE_HEADER] = "1"
        query = _as_str_dict(request.metadata.query)

        try:
            json_body: Optional[Any] = None
            if request.json_body:
                json_body = json.loads(request.json_body)
        except json.JSONDecodeError as exc:
            yield oms_gateway_pb2.OmsStreamChunk(
                status_code=400,
                json_body=json.dumps({"detail": f"invalid json_body: {exc}"}, ensure_ascii=False),
                content_type="application/json",
                detail=f"invalid json_body: {exc}",
                eof=True,
            )
            return

        async with self._http.stream(
            method=method.upper(),
            url=path,
            params=query or None,
            headers=headers or None,
            json=json_body,
            content=(bytes(request.binary_body) if request.binary_body else None),
        ) as response:
            status_code = int(response.status_code)
            content_type = response.headers.get("content-type", "") or "application/octet-stream"
            wire_headers = _as_str_dict(response.headers)

            if status_code >= 400:
                body = await response.aread()
                text = body.decode("utf-8", errors="replace") if body else ""
                detail = text
                if "application/json" in content_type and text:
                    try:
                        detail = str((json.loads(text) or {}).get("detail") or text)
                    except json.JSONDecodeError:
                        detail = text
                yield oms_gateway_pb2.OmsStreamChunk(
                    status_code=status_code,
                    chunk=body if body and "application/json" not in content_type else b"",
                    headers=wire_headers,
                    content_type=content_type,
                    json_body=text if "application/json" in content_type else "",
                    detail=detail,
                    eof=True,
                )
                return

            emitted = False
            first_chunk = True
            async for data_chunk in response.aiter_bytes(chunk_size=chunk_size):
                if not data_chunk:
                    continue
                emitted = True
                yield oms_gateway_pb2.OmsStreamChunk(
                    status_code=status_code,
                    chunk=data_chunk,
                    headers=wire_headers if first_chunk else {},
                    content_type=content_type,
                    eof=False,
                )
                first_chunk = False

            if not emitted:
                yield oms_gateway_pb2.OmsStreamChunk(
                    status_code=status_code,
                    headers=wire_headers,
                    content_type=content_type,
                    eof=True,
                )
                return

            yield oms_gateway_pb2.OmsStreamChunk(
                status_code=status_code,
                content_type=content_type,
                eof=True,
            )

    # AttachmentService
    async def UploadAttachment(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["filename"] = request.filename
        return await self._dispatch(
            context=context,
            method="POST",
            path="/api/v2/ontologies/attachments/upload",
            request=req,
        )

    async def ListPropertyAttachments(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["branch"] = request.branch or request.metadata.query.get("branch", "main")
        path = (
            f"/api/v2/ontologies/{request.db_name}/objects/{request.object_type}/{request.primary_key}"
            f"/attachments/{request.property_name}"
        )
        return await self._dispatch(context=context, method="GET", path=path, request=req)

    async def GetAttachmentByRid(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["branch"] = request.branch or request.metadata.query.get("branch", "main")
        path = (
            f"/api/v2/ontologies/{request.db_name}/objects/{request.object_type}/{request.primary_key}"
            f"/attachments/{request.property_name}/{request.attachment_rid}"
        )
        return await self._dispatch(context=context, method="GET", path=path, request=req)

    async def GetAttachmentContent(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["branch"] = request.branch or request.metadata.query.get("branch", "main")
        path = (
            f"/api/v2/ontologies/{request.db_name}/objects/{request.object_type}/{request.primary_key}"
            f"/attachments/{request.property_name}/content"
        )
        return await self._dispatch(context=context, method="GET", path=path, request=req)

    async def GetAttachmentContentByRid(self, request, context):
        req = oms_gateway_pb2.OmsRequest()
        req.CopyFrom(request)
        req.metadata.query["branch"] = request.branch or request.metadata.query.get("branch", "main")
        path = (
            f"/api/v2/ontologies/{request.db_name}/objects/{request.object_type}/{request.primary_key}"
            f"/attachments/{request.property_name}/{request.attachment_rid}/content"
        )
        return await self._dispatch(context=context, method="GET", path=path, request=req)


class OMSGrpcServer:
    def __init__(self, app: FastAPI):
        self._app = app
        self._server: Optional[grpc.aio.Server] = None
        self._servicer: Optional[OmsGatewayServicer] = None

    async def start(self) -> None:
        settings = get_settings()
        enabled = str(os.getenv("OMS_GRPC_ENABLED", "true")).strip().lower() not in {"0", "false", "no", "off"}
        if not enabled:
            logger.info("OMS gRPC server is disabled by OMS_GRPC_ENABLED")
            return

        bind_host = str(os.getenv("OMS_GRPC_BIND_HOST") or "0.0.0.0").strip()
        port = int(os.getenv("OMS_GRPC_PORT") or settings.services.oms_grpc_port or 50051)
        require_tls = str(os.getenv("OMS_GRPC_SERVER_USE_TLS", "false")).strip().lower() in {"1", "true", "yes", "on"}
        require_mtls = str(os.getenv("OMS_GRPC_REQUIRE_MTLS", "false")).strip().lower() in {"1", "true", "yes", "on"}
        service_tokens = _extract_service_tokens()

        if settings.is_production:
            if not require_tls or not require_mtls:
                raise RuntimeError(
                    "Production requires OMS gRPC TLS + mTLS. Set OMS_GRPC_SERVER_USE_TLS=true and OMS_GRPC_REQUIRE_MTLS=true."
                )
            if not service_tokens:
                raise RuntimeError("Production requires OMS_GRPC_SERVICE_TOKENS (or equivalent auth tokens).")

        self._server = grpc.aio.server(
            options=[
                ("grpc.max_send_message_length", 64 * 1024 * 1024),
                ("grpc.max_receive_message_length", 64 * 1024 * 1024),
            ]
        )
        self._servicer = OmsGatewayServicer(
            self._app,
            require_mtls=require_mtls,
            expected_service_tokens=service_tokens,
        )
        oms_gateway_pb2_grpc.add_OmsGatewayServiceServicer_to_server(self._servicer, self._server)

        bind_addr = f"{bind_host}:{port}"
        if require_tls:
            cert_path = os.getenv("OMS_GRPC_SERVER_CERT_PATH") or settings.services.ssl_cert_path
            key_path = os.getenv("OMS_GRPC_SERVER_KEY_PATH") or settings.services.ssl_key_path
            ca_path = os.getenv("OMS_GRPC_SERVER_CA_PATH") or settings.services.ssl_ca_path
            if not cert_path or not key_path:
                raise RuntimeError("OMS gRPC TLS enabled but cert/key path is missing.")
            if require_mtls and not ca_path:
                raise RuntimeError("OMS gRPC mTLS enabled but CA path is missing.")
            with open(cert_path, "rb") as cert_handle:
                cert_chain = cert_handle.read()
            with open(key_path, "rb") as key_handle:
                private_key = key_handle.read()
            if ca_path:
                with open(ca_path, "rb") as ca_handle:
                    root_ca = ca_handle.read()
            else:
                root_ca = None
            server_creds = grpc.ssl_server_credentials(
                ((private_key, cert_chain),),
                root_certificates=root_ca,
                require_client_auth=require_mtls,
            )
            self._server.add_secure_port(bind_addr, server_creds)
            logger.info("OMS gRPC server listening (secure) on %s", bind_addr)
        else:
            self._server.add_insecure_port(bind_addr)
            logger.warning("OMS gRPC server listening (insecure) on %s", bind_addr)

        await self._server.start()

    async def stop(self, grace: float = 5.0) -> None:
        if self._server is not None:
            await self._server.stop(grace)
            self._server = None
        if self._servicer is not None:
            await self._servicer.close()
            self._servicer = None
