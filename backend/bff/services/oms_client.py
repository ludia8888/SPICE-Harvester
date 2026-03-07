"""
OMS gRPC client used by BFF.

External API stays REST at BFF, while internal BFF→OMS transport is gRPC.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import grpc
import httpx
from google.protobuf.json_format import ParseDict

from shared.config.settings import get_settings
from shared.generated.grpc.spice.oms.v1 import oms_gateway_pb2
from shared.services.grpc.oms_gateway_client import OMSGatewayGrpcClient

from bff.services.base_http_client import ManagedAsyncClient

logger = logging.getLogger(__name__)

SERVICE_NAME = "BFF"


@dataclass(frozen=True)
class OntologyRef:
    id: str
    type: str  # "Class" | "Property"


class OMSClient(ManagedAsyncClient):
    """OMS gRPC client with HTTP-like compatibility helpers for existing BFF services."""

    def __init__(self, _base_url: Optional[str] = None, *, base_url: Optional[str] = None):
        self._settings = get_settings()
        self._debug_payload = self._settings.clients.oms_client_debug_payload
        self._base_url_override = (base_url or _base_url or "").strip() or None
        self.client = OMSGatewayGrpcClient()
        logger.info("OMS gRPC client initialized (target=%s)", self._settings.services.oms_grpc_target)

    @staticmethod
    def _get_auth_token() -> Optional[str]:
        token = get_settings().clients.oms_client_token
        return (token or "").strip() or None

    @property
    def base_url(self) -> str:
        return self._base_url_override or f"grpc://{self._settings.services.oms_grpc_target}"

    def _default_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        auth_token = self._get_auth_token()
        if auth_token:
            headers["X-Admin-Token"] = auth_token
            headers["Authorization"] = f"Bearer {auth_token}"
        return headers

    def _merged_headers(self, headers: Optional[Dict[str, str]]) -> Dict[str, str]:
        merged = self._default_headers()
        if headers:
            merged.update({str(k): str(v) for k, v in headers.items() if str(k).strip()})
        return merged

    async def _request_http_compat(
        self,
        method: str,
        path: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
        binary_body: Optional[bytes] = None,
    ) -> Optional[httpx.Response]:
        client = getattr(self, "client", None)
        if not isinstance(client, httpx.AsyncClient):
            return None
        return await client.request(
            method,
            path,
            headers=self._merged_headers(headers),
            params=params,
            json=json_body,
            content=binary_body,
        )

    async def _call_unary(
        self,
        rpc_name: str,
        *,
        path_for_error: str,
        json_body: Optional[Any] = None,
        binary_body: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        query: Optional[Dict[str, Any]] = None,
        **fields: str,
    ) -> httpx.Response:
        request = OMSGatewayGrpcClient.build_request(
            json_body=json_body,
            binary_body=binary_body,
            headers=self._merged_headers(headers),
            query=query,
            **fields,
        )
        response = await self.client.call_unary(rpc_name, request)
        http_response = OMSGatewayGrpcClient.to_httpx_response("POST", path_for_error, response)
        return http_response

    @staticmethod
    def _decode_json_or_empty(response: httpx.Response) -> Dict[str, Any]:
        if not response.text:
            return {}
        return response.json()

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> None:
        response.raise_for_status()

    async def get(self, path: str, **kwargs) -> Dict[str, Any]:
        response = await self._request_http_compat(
            "GET",
            path,
            headers=kwargs.get("headers"),
            params=kwargs.get("params"),
        )
        if response is None:
            response = await self._call_unary(
                "GenericGet",
                path_for_error=path,
                path=path,
                headers=kwargs.get("headers"),
                query=kwargs.get("params"),
            )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def post(self, path: str, **kwargs) -> Dict[str, Any]:
        response = await self._request_http_compat(
            "POST",
            path,
            headers=kwargs.get("headers"),
            params=kwargs.get("params"),
            json_body=kwargs.get("json"),
            binary_body=kwargs.get("content"),
        )
        if response is None:
            response = await self._call_unary(
                "GenericPost",
                path_for_error=path,
                path=path,
                headers=kwargs.get("headers"),
                query=kwargs.get("params"),
                json_body=kwargs.get("json"),
                binary_body=kwargs.get("content"),
            )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def put(self, path: str, **kwargs) -> Dict[str, Any]:
        response = await self._request_http_compat(
            "PUT",
            path,
            headers=kwargs.get("headers"),
            params=kwargs.get("params"),
            json_body=kwargs.get("json"),
        )
        if response is None:
            response = await self._call_unary(
                "GenericPut",
                path_for_error=path,
                path=path,
                headers=kwargs.get("headers"),
                query=kwargs.get("params"),
                json_body=kwargs.get("json"),
            )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def delete(self, path: str, **kwargs) -> Dict[str, Any]:
        response = await self._request_http_compat(
            "DELETE",
            path,
            headers=kwargs.get("headers"),
            params=kwargs.get("params"),
        )
        if response is None:
            response = await self._call_unary(
                "GenericDelete",
                path_for_error=path,
                path=path,
                headers=kwargs.get("headers"),
                query=kwargs.get("params"),
            )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def check_health(self) -> bool:
        try:
            response = await self._call_unary("CheckHealth", path_for_error="/health")
            self._raise_for_status(response)
            return True
        except Exception as e:
            logger.error("OMS health check failed: %s", e)
            return False

    async def list_databases(self) -> Dict[str, Any]:
        response = await self._call_unary("ListDatabases", path_for_error="/api/v1/database/list")
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def create_database(self, db_name: str, description: str = "") -> Dict[str, Any]:
        response = await self._call_unary(
            "CreateDatabase",
            path_for_error="/api/v1/database/create",
            json_body={"name": db_name, "description": description},
            db_name=db_name,
        )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def delete_database(self, db_name: str, *, expected_seq: int) -> Dict[str, Any]:
        response = await self._call_unary(
            "DeleteDatabase",
            path_for_error=f"/api/v1/database/{db_name}",
            db_name=db_name,
            expected_seq=str(int(expected_seq)),
        )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def get_database(self, db_name: str) -> Dict[str, Any]:
        response = await self._call_unary(
            "GetDatabase",
            path_for_error=f"/api/v1/database/{db_name}",
            db_name=db_name,
        )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def create_ontology(
        self,
        db_name: str,
        ontology_data: Dict[str, Any],
        *,
        branch: str = "main",
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        if self._debug_payload:
            logger.debug("OMS create_ontology payload: %s", json.dumps(ontology_data, ensure_ascii=False))
        path_for_error = f"/api/v1/database/{db_name}/ontology"
        try:
            typed_request = oms_gateway_pb2.CreateOntologyTypedRequest(
                db_name=db_name,
                branch=branch,
            )
            typed_request.metadata.headers.update(self._merged_headers(headers))
            if ontology_data:
                ParseDict(ontology_data, typed_request.ontology, ignore_unknown_fields=True)
            typed_response = await self.client.call_unary("CreateOntologyTyped", typed_request)
            response = OMSGatewayGrpcClient.to_httpx_response("POST", path_for_error, typed_response)
        except grpc.aio.AioRpcError as exc:
            if exc.code() != grpc.StatusCode.UNIMPLEMENTED:
                raise
            logger.warning("CreateOntologyTyped not implemented; falling back to legacy CreateOntology RPC")
            response = await self._call_unary(
                "CreateOntology",
                path_for_error=path_for_error,
                db_name=db_name,
                branch=branch,
                json_body=ontology_data,
                headers=headers,
            )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def get_ontology(self, db_name: str, class_id: str, *, branch: str = "main") -> Dict[str, Any]:
        path_for_error = f"/api/v1/database/{db_name}/ontology/{class_id}"
        try:
            typed_request = oms_gateway_pb2.GetOntologyTypedRequest(
                db_name=db_name,
                class_id=class_id,
                branch=branch,
            )
            typed_request.metadata.headers.update(self._default_headers())
            typed_response = await self.client.call_unary("GetOntologyTyped", typed_request)
            response = OMSGatewayGrpcClient.to_httpx_response("GET", path_for_error, typed_response)
        except grpc.aio.AioRpcError as exc:
            if exc.code() != grpc.StatusCode.UNIMPLEMENTED:
                raise
            logger.warning("GetOntologyTyped not implemented; falling back to legacy GetOntology RPC")
            response = await self._call_unary(
                "GetOntology",
                path_for_error=path_for_error,
                db_name=db_name,
                class_id=class_id,
                branch=branch,
            )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def list_ontologies(self, db_name: str, *, branch: str = "main") -> Dict[str, Any]:
        response = await self._call_unary(
            "ListOntologies",
            path_for_error=f"/api/v1/database/{db_name}/ontology",
            db_name=db_name,
            branch=branch,
        )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def get_ontologies(self, db_name: str, *, branch: str = "main") -> List[OntologyRef]:
        payload = await self.list_ontologies(db_name, branch=branch)
        data = payload.get("data") if isinstance(payload, dict) else None
        ontologies = (data or {}).get("ontologies") if isinstance(data, dict) else None
        if ontologies is None:
            raise ValueError("Unexpected OMS response shape: missing data.ontologies")

        refs: List[OntologyRef] = []
        for ontology in ontologies:
            if not isinstance(ontology, dict):
                continue
            class_id = (
                ontology.get("id")
                or ontology.get("@id")
                or ontology.get("class_id")
                or ontology.get("classId")
                or ontology.get("name")
            )
            if class_id:
                refs.append(OntologyRef(id=str(class_id), type="Class"))
            for prop in ontology.get("properties") or []:
                if not isinstance(prop, dict):
                    continue
                prop_id = prop.get("name") or prop.get("property_id") or prop.get("propertyId") or prop.get("id")
                if prop_id:
                    refs.append(OntologyRef(id=str(prop_id), type="Property"))
        return refs

    async def list_ontology_resources(
        self,
        db_name: str,
        *,
        resource_type: Optional[str] = None,
        branch: str = "main",
        limit: int = 200,
        offset: int = 0,
    ) -> Dict[str, Any]:
        response = await self._call_unary(
            "ListOntologyResources",
            path_for_error=f"/api/v1/database/{db_name}/ontology/resources",
            db_name=db_name,
            resource_type=resource_type or "",
            branch=branch,
            query={"limit": int(limit), "offset": int(offset)},
        )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def get_ontology_resource(
        self,
        db_name: str,
        *,
        resource_type: str,
        resource_id: str,
        branch: str = "main",
    ) -> Dict[str, Any]:
        response = await self._call_unary(
            "GetOntologyResource",
            path_for_error=f"/api/v1/database/{db_name}/ontology/resources/{resource_type}/{resource_id}",
            db_name=db_name,
            resource_type=resource_type,
            resource_id=resource_id,
            branch=branch,
        )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def create_ontology_resource(
        self,
        db_name: str,
        *,
        resource_type: str,
        payload: Dict[str, Any],
        branch: str = "main",
        expected_head_commit: Optional[str] = None,
    ) -> Dict[str, Any]:
        response = await self._call_unary(
            "CreateOntologyResource",
            path_for_error=f"/api/v1/database/{db_name}/ontology/resources/{resource_type}",
            db_name=db_name,
            resource_type=resource_type,
            branch=branch,
            expected_head_commit=expected_head_commit or "",
            json_body=payload,
        )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def record_ontology_deployment(
        self,
        db_name: str,
        *,
        target_branch: str = "main",
        ontology_commit_id: Optional[str] = None,
        snapshot_rid: Optional[str] = None,
        deployed_by: str = "system",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        response = await self._call_unary(
            "RecordOntologyDeployment",
            path_for_error=f"/api/v1/database/{db_name}/ontology/records/deployments",
            db_name=db_name,
            json_body={
                "target_branch": target_branch,
                "ontology_commit_id": ontology_commit_id,
                "snapshot_rid": snapshot_rid,
                "deployed_by": deployed_by,
                "metadata": metadata or {},
            },
        )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def update_ontology_resource(
        self,
        db_name: str,
        *,
        resource_type: str,
        resource_id: str,
        payload: Dict[str, Any],
        branch: str = "main",
        expected_head_commit: Optional[str] = None,
    ) -> Dict[str, Any]:
        response = await self._call_unary(
            "UpdateOntologyResource",
            path_for_error=f"/api/v1/database/{db_name}/ontology/resources/{resource_type}/{resource_id}",
            db_name=db_name,
            resource_type=resource_type,
            resource_id=resource_id,
            branch=branch,
            expected_head_commit=expected_head_commit or "",
            json_body=payload,
        )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def delete_ontology_resource(
        self,
        db_name: str,
        *,
        resource_type: str,
        resource_id: str,
        branch: str = "main",
        expected_head_commit: Optional[str] = None,
    ) -> Dict[str, Any]:
        response = await self._call_unary(
            "DeleteOntologyResource",
            path_for_error=f"/api/v1/database/{db_name}/ontology/resources/{resource_type}/{resource_id}",
            db_name=db_name,
            resource_type=resource_type,
            resource_id=resource_id,
            branch=branch,
            expected_head_commit=expected_head_commit or "",
        )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def update_ontology(
        self,
        db_name: str,
        class_id: str,
        ontology_data: Dict[str, Any],
        *,
        expected_seq: int,
        branch: str = "main",
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        response = await self._call_unary(
            "UpdateOntology",
            path_for_error=f"/api/v1/database/{db_name}/ontology/{class_id}",
            db_name=db_name,
            class_id=class_id,
            expected_seq=str(int(expected_seq)),
            branch=branch,
            headers=headers,
            json_body=ontology_data,
        )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def delete_ontology(
        self,
        db_name: str,
        class_id: str,
        *,
        expected_seq: int,
        branch: str = "main",
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        response = await self._call_unary(
            "DeleteOntology",
            path_for_error=f"/api/v1/database/{db_name}/ontology/{class_id}",
            db_name=db_name,
            class_id=class_id,
            expected_seq=str(int(expected_seq)),
            branch=branch,
            headers=headers,
        )
        self._raise_for_status(response)
        if response.text:
            return response.json()
        return {"status": "success", "message": f"온톨로지 '{class_id}' 삭제됨"}

    async def search_objects_v2(
        self,
        db_name: str,
        object_type: str,
        where: Optional[Dict[str, Any]] = None,
        *,
        page_size: int = 100,
        page_token: Optional[str] = None,
        select: Optional[List[str]] = None,
        order_by: Optional[str] = None,
        order_direction: str = "asc",
        exclude_rid: Optional[bool] = None,
        snapshot: Optional[bool] = None,
        branch: str = "main",
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {"pageSize": int(page_size)}
        body["where"] = where if where is not None else {"type": "not", "value": {"type": "isNull", "field": "instance_id"}}
        if page_token:
            body["pageToken"] = page_token
        if select:
            body["select"] = list(select)
        if order_by:
            direction = str(order_direction or "asc").strip().lower()
            if direction not in {"asc", "desc"}:
                raise ValueError("order_direction must be 'asc' or 'desc'")
            body["orderBy"] = {"orderType": "fields", "fields": [{"field": str(order_by).strip(), "direction": direction}]}
        if exclude_rid is not None:
            body["excludeRid"] = bool(exclude_rid)
        if snapshot is not None:
            body["snapshot"] = bool(snapshot)

        response = await self._call_unary(
            "SearchObjects",
            path_for_error=f"/api/v2/ontologies/{db_name}/objects/{object_type}/search",
            db_name=db_name,
            object_type=object_type,
            branch=branch,
            json_body=body,
        )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def aggregate_objects_v2(
        self,
        db_name: str,
        object_type: str,
        payload: Dict[str, Any],
        *,
        branch: str = "main",
    ) -> Dict[str, Any]:
        response = await self._call_unary(
            "AggregateObjects",
            path_for_error=f"/api/v2/ontologies/{db_name}/objects/{object_type}/aggregate",
            db_name=db_name,
            object_type=object_type,
            branch=branch,
            json_body=payload,
        )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def get_timeseries_first_point(
        self,
        db_name: str,
        object_type: str,
        primary_key: str,
        property_name: str,
        *,
        branch: str = "main",
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        response = await self._call_unary(
            "GetTimeseriesFirstPoint",
            path_for_error=f"/api/v2/ontologies/{db_name}/objects/{object_type}/{primary_key}/timeseries/{property_name}/firstPoint",
            db_name=db_name,
            object_type=object_type,
            primary_key=primary_key,
            property_name=property_name,
            branch=branch,
            headers=headers,
        )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def get_timeseries_last_point(
        self,
        db_name: str,
        object_type: str,
        primary_key: str,
        property_name: str,
        *,
        branch: str = "main",
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        response = await self._call_unary(
            "GetTimeseriesLastPoint",
            path_for_error=f"/api/v2/ontologies/{db_name}/objects/{object_type}/{primary_key}/timeseries/{property_name}/lastPoint",
            db_name=db_name,
            object_type=object_type,
            primary_key=primary_key,
            property_name=property_name,
            branch=branch,
            headers=headers,
        )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def stream_timeseries_points(
        self,
        db_name: str,
        object_type: str,
        primary_key: str,
        property_name: str,
        payload: Dict[str, Any],
        *,
        branch: str = "main",
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        request = OMSGatewayGrpcClient.build_request(
            db_name=db_name,
            object_type=object_type,
            primary_key=primary_key,
            property_name=property_name,
            branch=branch,
            json_body=payload,
            headers=self._merged_headers(headers),
        )
        chunks = await self.client.call_stream("StreamTimeseriesPoints", request)
        if not chunks:
            return httpx.Response(status_code=200, content=b"", request=httpx.Request("POST", "http://oms.local/stream"))
        first = chunks[0]
        content = b"".join(bytes(chunk.chunk) for chunk in chunks if chunk.chunk)
        if not content and first.json_body:
            content = first.json_body.encode("utf-8")
        response = httpx.Response(
            status_code=int(first.status_code or 500),
            content=content,
            headers=dict(first.headers or {}),
            request=httpx.Request("POST", "http://oms.local/stream"),
        )
        self._raise_for_status(response)
        return response

    async def upload_attachment(
        self,
        *,
        filename: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        merged = self._merged_headers(headers)
        merged["Content-Type"] = content_type
        response = await self._call_unary(
            "UploadAttachment",
            path_for_error="/api/v2/ontologies/attachments/upload",
            filename=filename,
            binary_body=data,
            headers=merged,
        )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def list_property_attachments(
        self,
        db_name: str,
        object_type: str,
        primary_key: str,
        property_name: str,
        *,
        branch: str = "main",
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        response = await self._call_unary(
            "ListPropertyAttachments",
            path_for_error=f"/api/v2/ontologies/{db_name}/objects/{object_type}/{primary_key}/attachments/{property_name}",
            db_name=db_name,
            object_type=object_type,
            primary_key=primary_key,
            property_name=property_name,
            branch=branch,
            headers=headers,
        )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def get_attachment_by_rid(
        self,
        db_name: str,
        object_type: str,
        primary_key: str,
        property_name: str,
        attachment_rid: str,
        *,
        branch: str = "main",
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        response = await self._call_unary(
            "GetAttachmentByRid",
            path_for_error=f"/api/v2/ontologies/{db_name}/objects/{object_type}/{primary_key}/attachments/{property_name}/{attachment_rid}",
            db_name=db_name,
            object_type=object_type,
            primary_key=primary_key,
            property_name=property_name,
            attachment_rid=attachment_rid,
            branch=branch,
            headers=headers,
        )
        self._raise_for_status(response)
        return self._decode_json_or_empty(response)

    async def get_attachment_content(
        self,
        db_name: str,
        object_type: str,
        primary_key: str,
        property_name: str,
        *,
        branch: str = "main",
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        response = await self._call_unary(
            "GetAttachmentContent",
            path_for_error=f"/api/v2/ontologies/{db_name}/objects/{object_type}/{primary_key}/attachments/{property_name}/content",
            db_name=db_name,
            object_type=object_type,
            primary_key=primary_key,
            property_name=property_name,
            branch=branch,
            headers=headers,
        )
        self._raise_for_status(response)
        return response

    async def get_attachment_content_by_rid(
        self,
        db_name: str,
        object_type: str,
        primary_key: str,
        property_name: str,
        attachment_rid: str,
        *,
        branch: str = "main",
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        response = await self._call_unary(
            "GetAttachmentContentByRid",
            path_for_error=f"/api/v2/ontologies/{db_name}/objects/{object_type}/{primary_key}/attachments/{property_name}/{attachment_rid}/content",
            db_name=db_name,
            object_type=object_type,
            primary_key=primary_key,
            property_name=property_name,
            attachment_rid=attachment_rid,
            branch=branch,
            headers=headers,
        )
        self._raise_for_status(response)
        return response

    async def database_exists(self, db_name: str) -> bool:
        response = await self._call_unary(
            "DatabaseExists",
            path_for_error=f"/api/v1/database/exists/{db_name}",
            db_name=db_name,
        )
        self._raise_for_status(response)
        data = self._decode_json_or_empty(response)
        return bool(data.get("data", {}).get("exists", False))

    async def __aenter__(self):
        return self

    async def __aexit__(self, _exc_type, _exc_val, _exc_tb):
        await self.close()
