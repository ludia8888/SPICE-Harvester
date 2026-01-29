"""
OMS (Ontology Management Service) 클라이언트
BFF에서 OMS와 통신하기 위한 HTTP 클라이언트
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx

from shared.config.settings import build_client_ssl_config, get_settings
from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.observability.request_context import get_correlation_id, get_request_id

# shared 모델 import
from shared.models.ontology import (
    OntologyCreateRequest,
    OntologyUpdateRequest,
    QueryRequestInternal,
)

logger = logging.getLogger(__name__)

SERVICE_NAME = "BFF"


@dataclass(frozen=True)
class OntologyRef:
    """Minimal ontology reference used by BFF validation flows."""

    id: str
    type: str  # "Class" | "Property"


class OMSClient:
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

    async def close(self):
        """클라이언트 연결 종료"""
        await self.client.aclose()

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
            # OMS가 실행 중이면 성공으로 처리 (TerminusDB 연결 여부와 무관)
            # 200 OK를 받았다는 것은 OMS 서비스 자체는 정상 작동 중
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

    async def validate_ontology_create(
        self,
        db_name: str,
        ontology_data: Dict[str, Any],
        *,
        branch: str = "main",
    ) -> Dict[str, Any]:
        """온톨로지 생성 검증 (no write)."""
        try:
            response = await self.client.post(
                f"/api/v1/database/{db_name}/ontology/validate",
                params={"branch": branch},
                json=ontology_data,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"온톨로지 생성 검증 실패 ({db_name}): {e}")
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

    async def list_branches(self, db_name: str) -> Dict[str, Any]:
        """브랜치 목록 조회"""
        try:
            response = await self.client.get(f"/api/v1/branch/{db_name}/list")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"브랜치 목록 조회 실패 ({db_name}): {e}")
            raise

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

    async def list_ontology_branches(self, db_name: str) -> Dict[str, Any]:
        """온톨로지 브랜치 목록 조회"""
        try:
            response = await self.client.get(f"/api/v1/database/{db_name}/ontology/branches")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Ontology branch list failed ({db_name}): {e}")
            raise

    async def create_ontology_branch(self, db_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """온톨로지 브랜치 생성"""
        try:
            response = await self.client.post(
                f"/api/v1/database/{db_name}/ontology/branches",
                json=payload,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Ontology branch create failed ({db_name}): {e}")
            raise

    async def list_ontology_proposals(
        self, db_name: str, *, status_filter: Optional[str] = None, limit: int = 100
    ) -> Dict[str, Any]:
        """온톨로지 제안 목록 조회"""
        try:
            params: Dict[str, Any] = {"limit": limit}
            if status_filter:
                params["status"] = status_filter
            response = await self.client.get(
                f"/api/v1/database/{db_name}/ontology/proposals",
                params=params,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Ontology proposal list failed ({db_name}): {e}")
            raise

    async def create_ontology_proposal(self, db_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """온톨로지 제안 생성"""
        try:
            response = await self.client.post(
                f"/api/v1/database/{db_name}/ontology/proposals",
                json=payload,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Ontology proposal create failed ({db_name}): {e}")
            raise

    async def approve_ontology_proposal(
        self, db_name: str, proposal_id: str, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """온톨로지 제안 승인"""
        try:
            response = await self.client.post(
                f"/api/v1/database/{db_name}/ontology/proposals/{proposal_id}/approve",
                json=payload,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Ontology proposal approve failed ({db_name}): {e}")
            raise

    async def deploy_ontology(self, db_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """온톨로지 배포(승격)"""
        try:
            response = await self.client.post(
                f"/api/v1/database/{db_name}/ontology/deploy",
                json=payload,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Ontology deploy failed ({db_name}): {e}")
            raise

    async def get_ontology_health(self, db_name: str, *, branch: str = "main") -> Dict[str, Any]:
        """온톨로지 헬스 체크"""
        try:
            response = await self.client.get(
                f"/api/v1/database/{db_name}/ontology/health",
                params={"branch": branch},
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Ontology health failed ({db_name}): {e}")
            raise

    async def create_branch(self, db_name: str, branch_data: Dict[str, Any]) -> Dict[str, Any]:
        """브랜치 생성"""
        try:
            response = await self.client.post(f"/api/v1/branch/{db_name}/create", json=branch_data)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"브랜치 생성 실패 ({db_name}): {e}")
            raise

    async def get_version_history(self, db_name: str) -> Dict[str, Any]:
        """버전 히스토리 조회"""
        try:
            response = await self.client.get(f"/api/v1/version/{db_name}/history")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"버전 히스토리 조회 실패 ({db_name}): {e}")
            raise

    async def get_version_head(self, db_name: str, *, branch: str = "main") -> Dict[str, Any]:
        """브랜치 head 커밋 ID 조회 (deploy gate)."""
        try:
            response = await self.client.get(
                f"/api/v1/version/{db_name}/head",
                params={"branch": branch},
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"브랜치 head 커밋 조회 실패 ({db_name}, branch={branch}): {e}")
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

    async def validate_ontology_update(
        self,
        db_name: str,
        class_id: str,
        update_data: Dict[str, Any],
        *,
        branch: str = "main",
    ) -> Dict[str, Any]:
        """온톨로지 업데이트 검증 (no write)."""
        try:
            response = await self.client.post(
                f"/api/v1/database/{db_name}/ontology/{class_id}/validate",
                params={"branch": branch},
                json=update_data,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"온톨로지 업데이트 검증 실패 ({db_name}/{class_id}): {e}")
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

    async def query_ontologies(self, db_name: str, query: Dict[str, Any]) -> Dict[str, Any]:
        """온톨로지 쿼리"""
        try:
            response = await self.client.post(f"/api/v1/database/{db_name}/ontology/query", json=query)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"온톨로지 쿼리 실패: {e}")
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

    async def commit_database_change(self, db_name: str, message: str, author: str = "system") -> Dict[str, Any]:
        """데이터베이스 변경사항 자동 커밋"""
        try:
            commit_data = {
                "message": message,
                "author": author,
                "operation": "database_change"
            }
            
            # OMS의 브랜치 커밋 엔드포인트 사용
            response = await self.client.post(f"/api/v1/branch/{db_name}/commit", json=commit_data)
            
            # 404 에러는 브랜치가 없다는 의미이므로 무시
            if response.status_code == 404:
                logger.info(f"Database {db_name} has no branches yet, skipping commit")
                return {"status": "skipped", "message": "No branches to commit"}
            
            response.raise_for_status()
            result = response.json()
            logger.info(f"Successfully committed changes to database {db_name}: {message}")
            return result
            
        except Exception as e:
            logger.warning(f"Failed to commit database change for {db_name}: {e}")
            # 커밋 실패는 심각한 오류가 아니므로 예외를 다시 던지지 않음
            return {"status": "failed", "error": str(e)}

    async def commit_system_change(
        self, 
        message: str, 
        author: str = "system", 
        operation: str = "system_change",
        target: str = None
    ) -> Dict[str, Any]:
        """시스템 레벨 변경사항 커밋 (데이터베이스 생성/삭제 등)"""
        try:
            commit_data = {
                "message": message,
                "author": author,
                "operation": operation,
                "target": target,
                "timestamp": "auto"
            }
            
            # 시스템 로그나 메타데이터 데이터베이스에 기록
            # 여기서는 간단히 로그로 기록하고 향후 확장 가능
            logger.info(f"System change committed - Operation: {operation}, Target: {target}, Message: {message}")
            
            # 향후 메타데이터 데이터베이스나 Git 레포지토리에 실제 커밋 구현 가능
            return {
                "status": "success", 
                "message": "System change logged",
                "operation": operation,
                "target": target
            }
            
        except Exception as e:
            logger.warning(f"Failed to commit system change: {e}")
            return {"status": "failed", "error": str(e)}

    async def get_class_metadata(self, db_name: str, class_id: str) -> Dict[str, Any]:
        """클래스의 메타데이터 가져오기"""
        try:
            response = await self.client.get(f"/api/v1/database/{db_name}/ontology/{class_id}")
            response.raise_for_status()
            ontology_data = response.json()
            
            # Extract metadata from the ontology response
            if isinstance(ontology_data, dict) and "data" in ontology_data:
                class_data = ontology_data["data"]
                # Return metadata fields or empty dict
                return {
                    "mapping_history": class_data.get("mapping_history", []),
                    "last_mapping_date": class_data.get("last_mapping_date"),
                    "total_mappings": class_data.get("total_mappings", 0),
                    "mapping_sources": class_data.get("mapping_sources", [])
                }
            return {}
        except Exception as e:
            logger.error(f"클래스 메타데이터 조회 실패: {e}")
            # Return empty metadata instead of raising
            return {}

    async def update_class_metadata(self, db_name: str, class_id: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """클래스의 메타데이터 업데이트"""
        try:
            # Get current class data
            response = await self.client.get(f"/api/v1/database/{db_name}/ontology/{class_id}")
            response.raise_for_status()
            current_data = response.json()
            
            # Update with new metadata fields
            if isinstance(current_data, dict) and "data" in current_data:
                class_data = current_data["data"]
                # Merge metadata into class data
                update_data = {
                    **class_data,
                    "mapping_history": metadata.get("mapping_history", []),
                    "last_mapping_date": metadata.get("last_mapping_date"),
                    "total_mappings": metadata.get("total_mappings", 0),
                    "mapping_sources": metadata.get("mapping_sources", [])
                }
                
                # Update the class with new metadata
                response = await self.client.put(
                    f"/api/v1/database/{db_name}/ontology/{class_id}",
                    json=update_data
                )
                response.raise_for_status()
                return response.json()
            
            return build_error_envelope(
                service_name=SERVICE_NAME,
                message="Unable to update metadata",
                detail="Unexpected ontology metadata response",
                code=ErrorCode.UPSTREAM_ERROR,
                category=ErrorCategory.UPSTREAM,
                status_code=502,
                context={"db_name": db_name, "class_id": class_id},
                request_id=get_request_id(),
                correlation_id=get_correlation_id(),
            )
        except Exception as e:
            logger.error(f"클래스 메타데이터 업데이트 실패: {e}")
            raise

    async def get_class_instances(
        self, 
        db_name: str, 
        class_id: str,
        limit: int = 100,
        offset: int = 0,
        search: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        특정 클래스의 인스턴스 목록을 효율적으로 조회 (N+1 Query 최적화)
        
        Args:
            db_name: 데이터베이스 이름
            class_id: 클래스 ID
            limit: 최대 결과 수
            offset: 시작 위치
            search: 검색 쿼리
            
        Returns:
            완전히 조립된 인스턴스 목록
        """
        try:
            params = {
                "limit": limit,
                "offset": offset
            }
            if search:
                params["search"] = search
                
            response = await self.client.get(
                f"/api/v1/instance/{db_name}/class/{class_id}/instances",
                params=params
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"클래스 인스턴스 목록 조회 실패: {e}")
            raise
    
    async def get_instance(
        self,
        db_name: str,
        instance_id: str,
        class_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        개별 인스턴스를 효율적으로 조회
        
        Args:
            db_name: 데이터베이스 이름
            instance_id: 인스턴스 ID
            class_id: 클래스 ID (선택사항)
            
        Returns:
            완전한 인스턴스 객체
        """
        try:
            params = {}
            if class_id:
                params["class_id"] = class_id
                
            response = await self.client.get(
                f"/api/v1/instance/{db_name}/instance/{instance_id}",
                params=params
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"인스턴스 조회 실패: {e}")
            raise
    
    async def count_class_instances(
        self,
        db_name: str,
        class_id: str
    ) -> Dict[str, Any]:
        """
        특정 클래스의 인스턴스 개수 조회
        
        Args:
            db_name: 데이터베이스 이름
            class_id: 클래스 ID
            
        Returns:
            인스턴스 개수
        """
        try:
            response = await self.client.get(
                f"/api/v1/instance/{db_name}/class/{class_id}/count"
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"인스턴스 개수 조회 실패: {e}")
            raise
    
    async def execute_sparql(
        self,
        db_name: str,
        query: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        SPARQL 쿼리 실행
        
        Args:
            db_name: 데이터베이스 이름
            query: SPARQL 쿼리
            limit: 최대 결과 수
            offset: 시작 위치
            
        Returns:
            쿼리 결과
        """
        try:
            data = {"query": query}
            params = {}
            if limit is not None:
                params["limit"] = limit
            if offset is not None:
                params["offset"] = offset
                
            response = await self.client.post(
                f"/api/v1/instance/{db_name}/sparql",
                json=data,
                params=params
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"SPARQL 쿼리 실행 실패: {e}")
            raise

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
