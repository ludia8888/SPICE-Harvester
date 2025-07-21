"""
OMS (Ontology Management Service) 클라이언트
BFF에서 OMS와 통신하기 위한 HTTP 클라이언트
"""

import logging
from typing import Any, Dict, List, Optional

import httpx

from shared.config.service_config import ServiceConfig

# shared 모델 import
from shared.models.ontology import (
    OntologyCreateRequest,
    OntologyUpdateRequest,
    QueryRequestInternal,
)

logger = logging.getLogger(__name__)


class OMSClient:
    """OMS HTTP 클라이언트"""

    def __init__(self, base_url: Optional[str] = None):
        # ServiceConfig에서 OMS URL 가져오기
        self.base_url = base_url or ServiceConfig.get_oms_url()

        # SSL 설정 가져오기
        ssl_config = ServiceConfig.get_client_ssl_config()

        # HTTPX 클라이언트 생성
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=30.0,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            verify=ssl_config.get("verify", True),
        )

        logger.info(f"OMS Client initialized with base URL: {self.base_url}")

    async def close(self):
        """클라이언트 연결 종료"""
        await self.client.aclose()

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
        try:
            data = {"name": db_name, "description": description}
            response = await self.client.post("/api/v1/database/create", json=data)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"데이터베이스 생성 실패 ({db_name}): {e}")
            raise

    async def delete_database(self, db_name: str) -> Dict[str, Any]:
        """데이터베이스 삭제"""
        try:
            response = await self.client.delete(f"/api/v1/database/{db_name}")
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

    async def create_ontology(self, db_name: str, ontology_data: Dict[str, Any]) -> Dict[str, Any]:
        """온톨로지 생성"""
        try:
            # Send data as-is to OMS (no format conversion needed)
            response = await self.client.post(
                f"/api/v1/ontology/{db_name}/create", json=ontology_data
            )
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            logger.error(f"온톨로지 생성 실패 ({db_name}): {e}")
            raise

    async def get_ontology(self, db_name: str, class_id: str) -> Dict[str, Any]:
        """온톨로지 조회"""
        try:
            url = f"/api/v1/ontology/{db_name}/{class_id}"
            logger.info(f"Requesting OMS: GET {self.base_url}{url}")
            response = await self.client.get(url)
            logger.info(f"OMS response status: {response.status_code}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"온톨로지 조회 실패 ({db_name}/{class_id}): {e}")
            raise

    async def list_ontologies(self, db_name: str) -> Dict[str, Any]:
        """온톨로지 목록 조회"""
        try:
            response = await self.client.get(f"/api/v1/ontology/{db_name}/list")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"온톨로지 목록 조회 실패 ({db_name}): {e}")
            raise

    async def list_branches(self, db_name: str) -> Dict[str, Any]:
        """브랜치 목록 조회"""
        try:
            response = await self.client.get(f"/api/v1/branch/{db_name}/list")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"브랜치 목록 조회 실패 ({db_name}): {e}")
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

    async def update_ontology(
        self, db_name: str, class_id: str, update_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """온톨로지 업데이트"""
        try:
            response = await self.client.put(
                f"/api/v1/ontology/{db_name}/{class_id}", json=update_data
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"온톨로지 업데이트 실패: {e}")
            raise

    async def delete_ontology(self, db_name: str, class_id: str) -> Dict[str, Any]:
        """온톨로지 삭제"""
        try:
            response = await self.client.delete(f"/api/v1/ontology/{db_name}/{class_id}")
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
            response = await self.client.post(f"/api/v1/ontology/{db_name}/query", json=query)
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

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
