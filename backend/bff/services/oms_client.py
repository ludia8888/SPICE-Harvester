"""
OMS (Ontology Management Service) 클라이언트
BFF에서 OMS와 통신하기 위한 HTTP 클라이언트
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx

from shared.config.settings import build_client_ssl_config, get_settings
from bff.services.base_http_client import ManagedAsyncClient

# shared 모델 import

logger = logging.getLogger(__name__)

SERVICE_NAME = "BFF"


@dataclass(frozen=True)
class OntologyRef:
    """Minimal ontology reference used by BFF validation flows."""

    id: str
    type: str  # "Class" | "Property"


class OMSClient(ManagedAsyncClient):
    """OMS HTTP 클라이언트"""

    def __init__(self, base_url: Optional[str] = None):
        settings = get_settings()
        client_settings = settings.clients

        self.base_url = base_url or settings.services.oms_base_url

        ssl_config = build_client_ssl_config(settings)

        # HTTPX 클라이언트 생성
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        auth_token = self._get_auth_token()
        if auth_token:
            headers["X-Admin-Token"] = auth_token
        timeout_seconds = client_settings.oms_client_timeout_seconds
        self._debug_payload = client_settings.oms_client_debug_payload
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout_seconds,
            headers=headers,
            verify=ssl_config.get("verify", True),
        )

        logger.info(f"OMS Client initialized with base URL: {self.base_url}")

    @staticmethod
    def _get_auth_token() -> Optional[str]:
        token = get_settings().clients.oms_client_token
        return (token or "").strip() or None

    # -----------------------------
    # Generic HTTP helpers
    # -----------------------------

    async def get(self, path: str, **kwargs) -> Dict[str, Any]:
        """Low-level GET helper (returns JSON dict)."""
        response = await self.client.get(path, **kwargs)
        response.raise_for_status()
        if not response.text:
            return {}
        return response.json()

    async def post(self, path: str, **kwargs) -> Dict[str, Any]:
        """Low-level POST helper (returns JSON dict)."""
        response = await self.client.post(path, **kwargs)
        response.raise_for_status()
        if not response.text:
            return {}
        return response.json()

    async def put(self, path: str, **kwargs) -> Dict[str, Any]:
        """Low-level PUT helper (returns JSON dict)."""
        response = await self.client.put(path, **kwargs)
        response.raise_for_status()
        if not response.text:
            return {}
        return response.json()

    async def delete(self, path: str, **kwargs) -> Dict[str, Any]:
        """Low-level DELETE helper (returns JSON dict when available)."""
        response = await self.client.delete(path, **kwargs)
        response.raise_for_status()
        if not response.text:
            return {}
        return response.json()

    async def check_health(self) -> bool:
        """OMS 서비스 상태 확인"""
        try:
            response = await self.client.get("/health")
            response.raise_for_status()
            # OMS 서비스 자체의 health endpoint가 정상이면 연결 성공으로 간주한다.
            return True
        except Exception as e:
            logger.error(f"OMS 헬스 체크 실패: {e}")
            return False

    async def list_databases(self) -> Dict[str, Any]:
        """데이터베이스 목록 조회"""
        try:
            response = await self.client.get("/api/v1/database/list")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"데이터베이스 목록 조회 실패: {e}")
            raise

    async def create_database(self, db_name: str, description: str = "") -> Dict[str, Any]:
        """데이터베이스 생성"""
        logger.info(f"🔥 OMS Client: Creating database - name: {db_name}, description: {description}")
        logger.info(f"🌐 OMS Client: Base URL: {self.base_url}")
        
        try:
            data = {"name": db_name, "description": description}
            url = "/api/v1/database/create"
            full_url = f"{self.base_url}{url}"
            logger.info(f"📤 OMS Client: POST {full_url} with data: {data}")
            
            response = await self.client.post(url, json=data)
            logger.info(f"📥 OMS Client: Response status: {response.status_code}")
            
            response.raise_for_status()
            result = response.json()
            logger.info(f"✅ OMS Client: Database created successfully: {result}")
            return result
        except Exception as e:
            logger.error(f"❌ OMS Client: Database creation failed ({db_name}): {type(e).__name__}: {e}")
            logger.error(f"🔍 OMS Client: Error details: {e.__dict__ if hasattr(e, '__dict__') else str(e)}")
            raise

    async def delete_database(self, db_name: str, *, expected_seq: int) -> Dict[str, Any]:
        """데이터베이스 삭제"""
        try:
            response = await self.client.delete(
                f"/api/v1/database/{db_name}",
                params={"expected_seq": int(expected_seq)},
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"데이터베이스 삭제 실패 ({db_name}): {e}")
            raise

    async def get_database(self, db_name: str) -> Dict[str, Any]:
        """데이터베이스 정보 조회"""
        try:
            response = await self.client.get(f"/api/v1/database/exists/{db_name}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"데이터베이스 조회 실패 ({db_name}): {e}")
            raise

    async def create_ontology(
        self,
        db_name: str,
        ontology_data: Dict[str, Any],
        *,
        branch: str = "main",
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """온톨로지 생성"""
        try:
            if self._debug_payload:
                import json

                logger.debug("OMS create_ontology payload: %s", json.dumps(ontology_data, ensure_ascii=False))
            
            # Send data as-is to OMS (no format conversion needed)
            response = await self.client.post(
                f"/api/v1/database/{db_name}/ontology",
                params={"branch": branch},
                json=ontology_data,
                headers=headers,
            )
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            logger.error(f"온톨로지 생성 실패 ({db_name}): {e}")
            raise

    async def get_ontology(self, db_name: str, class_id: str, *, branch: str = "main") -> Dict[str, Any]:
        """온톨로지 조회"""
        try:
            url = f"/api/v1/database/{db_name}/ontology/{class_id}"
            logger.info(f"Requesting OMS: GET {self.base_url}{url}")
            response = await self.client.get(url, params={"branch": branch})
            logger.info(f"OMS response status: {response.status_code}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"온톨로지 조회 실패 ({db_name}/{class_id}): {e}")
            raise

    async def list_ontologies(self, db_name: str, *, branch: str = "main") -> Dict[str, Any]:
        """온톨로지 목록 조회"""
        try:
            response = await self.client.get(
                f"/api/v1/database/{db_name}/ontology",
                params={"branch": branch},
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"온톨로지 목록 조회 실패 ({db_name}): {e}")
            raise

    async def get_ontologies(self, db_name: str, *, branch: str = "main") -> List[OntologyRef]:
        """
        Compatibility helper for BFF callers that need a flattened view of:
        - Class IDs
        - Property IDs (derived from class properties)

        Returns:
            List[OntologyRef] where each ref has `.id` and `.type` ("Class" or "Property").
        """
        payload = await self.list_ontologies(db_name, branch=branch)
        data = payload.get("data") if isinstance(payload, dict) else None
        ontologies = (data or {}).get("ontologies") if isinstance(data, dict) else None
        if ontologies is None:
            # Keep the error explicit; callers treat exceptions as upstream failures.
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
                prop_id = (
                    prop.get("name")
                    or prop.get("property_id")
                    or prop.get("propertyId")
                    or prop.get("id")
                )
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
        """온톨로지 리소스 목록 조회"""
        try:
            params = {"branch": branch, "limit": limit, "offset": offset}
            if resource_type:
                response = await self.client.get(
                    f"/api/v1/database/{db_name}/ontology/resources/{resource_type}",
                    params=params,
                )
            else:
                response = await self.client.get(
                    f"/api/v1/database/{db_name}/ontology/resources",
                    params=params,
                )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Ontology resource list failed ({db_name}): {e}")
            raise

    async def get_ontology_resource(
        self,
        db_name: str,
        *,
        resource_type: str,
        resource_id: str,
        branch: str = "main",
    ) -> Dict[str, Any]:
        """단일 온톨로지 리소스 조회"""
        try:
            response = await self.client.get(
                f"/api/v1/database/{db_name}/ontology/resources/{resource_type}/{resource_id}",
                params={"branch": branch},
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Ontology resource get failed ({db_name}): {e}")
            raise

    async def create_ontology_resource(
        self,
        db_name: str,
        *,
        resource_type: str,
        payload: Dict[str, Any],
        branch: str = "main",
        expected_head_commit: Optional[str] = None,
    ) -> Dict[str, Any]:
        """온톨로지 리소스 생성"""
        try:
            params = {"branch": branch}
            if expected_head_commit is not None:
                params["expected_head_commit"] = expected_head_commit
            response = await self.client.post(
                f"/api/v1/database/{db_name}/ontology/resources/{resource_type}",
                params=params,
                json=payload,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Ontology resource create failed ({db_name}): {e}")
            raise

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
        """온톨로지 리소스 업데이트"""
        try:
            params = {"branch": branch}
            if expected_head_commit is not None:
                params["expected_head_commit"] = expected_head_commit
            response = await self.client.put(
                f"/api/v1/database/{db_name}/ontology/resources/{resource_type}/{resource_id}",
                params=params,
                json=payload,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Ontology resource update failed ({db_name}): {e}")
            raise

    async def delete_ontology_resource(
        self,
        db_name: str,
        *,
        resource_type: str,
        resource_id: str,
        branch: str = "main",
        expected_head_commit: Optional[str] = None,
    ) -> Dict[str, Any]:
        """온톨로지 리소스 삭제"""
        try:
            params = {"branch": branch}
            if expected_head_commit is not None:
                params["expected_head_commit"] = expected_head_commit
            response = await self.client.delete(
                f"/api/v1/database/{db_name}/ontology/resources/{resource_type}/{resource_id}",
                params=params,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Ontology resource delete failed ({db_name}): {e}")
            raise

    async def update_ontology(
        self,
        db_name: str,
        class_id: str,
        update_data: Dict[str, Any],
        *,
        expected_seq: int,
        branch: str = "main",
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """온톨로지 업데이트"""
        try:
            response = await self.client.put(
                f"/api/v1/database/{db_name}/ontology/{class_id}",
                params={"expected_seq": int(expected_seq), "branch": branch},
                json=update_data,
                headers=headers,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"온톨로지 업데이트 실패: {e}")
            raise

    async def delete_ontology(
        self,
        db_name: str,
        class_id: str,
        *,
        expected_seq: int,
        branch: str = "main",
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """온톨로지 삭제"""
        try:
            response = await self.client.delete(
                f"/api/v1/database/{db_name}/ontology/{class_id}",
                params={"expected_seq": int(expected_seq), "branch": branch},
                headers=headers,
            )
            response.raise_for_status()
            # 실제 삭제 응답 반환
            if response.text:
                return response.json()
            else:
                # 빈 응답이면 성공 메시지 반환
                return {"status": "success", "message": f"온톨로지 '{class_id}' 삭제됨"}
        except Exception as e:
            logger.error(f"온톨로지 삭제 실패: {e}")
            raise

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
        """Foundry Search Objects API v2 proxy."""
        try:
            body: Dict[str, Any] = {"pageSize": int(page_size)}
            if where is not None:
                body["where"] = where
            else:
                body["where"] = {"type": "not", "value": {"type": "isNull", "field": "instance_id"}}
            if page_token:
                body["pageToken"] = page_token
            if select:
                body["select"] = list(select)
            if order_by:
                direction = str(order_direction or "asc").strip().lower()
                if direction not in {"asc", "desc"}:
                    raise ValueError("order_direction must be 'asc' or 'desc'")
                body["orderBy"] = {
                    "orderType": "fields",
                    "fields": [{"field": str(order_by).strip(), "direction": direction}],
                }
            if exclude_rid is not None:
                body["excludeRid"] = bool(exclude_rid)
            if snapshot is not None:
                body["snapshot"] = bool(snapshot)

            response = await self.client.post(
                f"/api/v2/ontologies/{db_name}/objects/{object_type}/search",
                params={"branch": branch},
                json=body,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Foundry object search failed ({db_name}/{object_type}): {e}")
            raise

    async def aggregate_objects_v2(
        self,
        db_name: str,
        object_type: str,
        payload: Dict[str, Any],
        *,
        branch: str = "main",
    ) -> Dict[str, Any]:
        """Foundry Aggregate Objects API v2 proxy — delegates to OMS ES-native engine."""
        try:
            response = await self.client.post(
                f"/api/v2/ontologies/{db_name}/objects/{object_type}/aggregate",
                params={"branch": branch},
                json=payload,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Foundry object aggregate failed ({db_name}/{object_type}): {e}")
            raise

    # ------------------------------------------------------------------
    # Time Series Property proxy
    # ------------------------------------------------------------------

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
        """Proxy GET firstPoint to OMS Time Series router."""
        try:
            response = await self.client.get(
                f"/api/v2/ontologies/{db_name}/objects/{object_type}/{primary_key}/timeseries/{property_name}/firstPoint",
                params={"branch": branch},
                headers=headers,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Timeseries firstPoint failed ({db_name}/{object_type}/{primary_key}/{property_name}): {e}")
            raise

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
        """Proxy GET lastPoint to OMS Time Series router."""
        try:
            response = await self.client.get(
                f"/api/v2/ontologies/{db_name}/objects/{object_type}/{primary_key}/timeseries/{property_name}/lastPoint",
                params={"branch": branch},
                headers=headers,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Timeseries lastPoint failed ({db_name}/{object_type}/{primary_key}/{property_name}): {e}")
            raise

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
        """Proxy POST streamPoints to OMS (returns raw response for streaming)."""
        try:
            response = await self.client.post(
                f"/api/v2/ontologies/{db_name}/objects/{object_type}/{primary_key}/timeseries/{property_name}/streamPoints",
                params={"branch": branch},
                json=payload,
                headers=headers,
            )
            response.raise_for_status()
            return response
        except Exception as e:
            logger.error(f"Timeseries streamPoints failed ({db_name}/{object_type}/{primary_key}/{property_name}): {e}")
            raise

    # ------------------------------------------------------------------
    # Attachment Property proxy
    # ------------------------------------------------------------------

    async def upload_attachment(
        self,
        *,
        filename: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Proxy POST attachment upload to OMS."""
        try:
            response = await self.client.post(
                "/api/v2/ontologies/attachments/upload",
                params={"filename": filename},
                content=data,
                headers={
                    **(headers or {}),
                    "Content-Type": content_type,
                    "Accept": "application/json",
                },
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Attachment upload failed ({filename}): {e}")
            raise

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
        """Proxy GET list attachment property metadata."""
        try:
            response = await self.client.get(
                f"/api/v2/ontologies/{db_name}/objects/{object_type}/{primary_key}/attachments/{property_name}",
                params={"branch": branch},
                headers=headers,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"List attachments failed ({db_name}/{object_type}/{primary_key}/{property_name}): {e}")
            raise

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
        """Proxy GET attachment metadata by RID."""
        try:
            response = await self.client.get(
                f"/api/v2/ontologies/{db_name}/objects/{object_type}/{primary_key}/attachments/{property_name}/{attachment_rid}",
                params={"branch": branch},
                headers=headers,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Get attachment by RID failed ({attachment_rid}): {e}")
            raise

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
        """Proxy GET attachment content (returns raw response for binary data)."""
        try:
            response = await self.client.get(
                f"/api/v2/ontologies/{db_name}/objects/{object_type}/{primary_key}/attachments/{property_name}/content",
                params={"branch": branch},
                headers=headers,
            )
            response.raise_for_status()
            return response
        except Exception as e:
            logger.error(f"Get attachment content failed ({db_name}/{object_type}/{primary_key}/{property_name}): {e}")
            raise

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
        """Proxy GET attachment content by RID (returns raw response for binary data)."""
        try:
            response = await self.client.get(
                f"/api/v2/ontologies/{db_name}/objects/{object_type}/{primary_key}/attachments/{property_name}/{attachment_rid}/content",
                params={"branch": branch},
                headers=headers,
            )
            response.raise_for_status()
            return response
        except Exception as e:
            logger.error(f"Get attachment content by RID failed ({attachment_rid}): {e}")
            raise

    async def database_exists(self, db_name: str) -> bool:
        """데이터베이스 존재 여부 확인"""
        try:
            response = await self.client.get(f"/api/v1/database/exists/{db_name}")
            response.raise_for_status()
            data = response.json()
            return data.get("data", {}).get("exists", False)
        except Exception as e:
            logger.error(f"데이터베이스 존재 여부 확인 실패: {e}")
            raise

    async def __aenter__(self):
        return self

    async def __aexit__(self, _exc_type, _exc_val, _exc_tb):
        await self.close()
