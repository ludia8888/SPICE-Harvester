"""
BFF Dependencies
실제 OMS 클라이언트 사용
"""

from typing import Any, Dict, List, Optional
import json

import httpx
from fastapi import HTTPException, status

# 실제 OMS 클라이언트 import
from bff.services.oms_client import OMSClient

# 전역 OMS 클라이언트 인스턴스 (main.py에서 초기화)
oms_client: Optional[OMSClient] = None


def set_oms_client(client: OMSClient):
    """OMS 클라이언트 설정"""
    global oms_client
    oms_client = client


def get_oms_client() -> OMSClient:
    """OMS 클라이언트 반환"""
    if not oms_client:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="OMS 클라이언트가 초기화되지 않았습니다",
        )
    return oms_client


# OMS 클라이언트를 래핑하는 TerminusService 호환 클래스
class TerminusService:
    """OMS 클라이언트를 래핑하는 TerminusService 호환 클래스"""

    def __init__(self):
        self.connected = False

    async def list_databases(self):
        """데이터베이스 목록 조회"""
        client = get_oms_client()
        response = await client.list_databases()
        if isinstance(response, dict) and response.get("status") == "success":
            databases = response.get("data", {}).get("databases", [])
            return [db.get("name") for db in databases if db.get("name")]
        elif isinstance(response, list):
            # 직접 리스트가 반환된 경우
            return [db.get("name") for db in response if isinstance(db, dict) and db.get("name")]
        return []

    async def create_database(self, db_name: str, description: Optional[str] = None):
        """데이터베이스 생성"""
        client = get_oms_client()
        response = await client.create_database(db_name, description)
        return response

    async def delete_database(self, db_name: str):
        """데이터베이스 삭제"""
        client = get_oms_client()
        response = await client.delete_database(db_name)
        return response

    async def get_database_info(self, db_name: str):
        """데이터베이스 정보 조회"""
        client = get_oms_client()
        response = await client.check_database_exists(db_name)
        return response

    async def list_classes(self, db_name: str):
        """클래스 목록 조회"""
        client = get_oms_client()
        response = await client.list_ontologies(db_name)
        if response.get("status") == "success":
            ontologies = response.get("data", {}).get("ontologies", [])
            return ontologies
        return []

    async def create_class(self, db_name: str, class_data: dict):
        """클래스 생성"""
        print(f"🔥🔥🔥 TerminusService.create_class called! db_name={db_name}")
        print(f"🔥🔥🔥 class_data={json.dumps(class_data, ensure_ascii=False, indent=2)}")
        client = get_oms_client()
        response = await client.create_ontology(db_name, class_data)
        # Return the created data
        if response and response.get("status") == "success":
            return response.get("data", {})
        return response

    async def get_class(self, db_name: str, class_id: str):
        """클래스 조회"""
        client = get_oms_client()
        response = await client.get_ontology(db_name, class_id)
        # Extract the data from the response
        if response and response.get("status") == "success":
            return response.get("data", {})
        return None

    async def update_class(self, db_name: str, class_id: str, class_data: dict):
        """클래스 업데이트"""
        client = get_oms_client()
        response = await client.update_ontology(db_name, class_id, class_data)
        return response

    async def delete_class(self, db_name: str, class_id: str):
        """클래스 삭제"""
        client = get_oms_client()
        response = await client.delete_ontology(db_name, class_id)
        return response

    async def query_database(self, db_name: str, query: str):
        """데이터베이스 쿼리"""
        client = get_oms_client()
        response = await client.query_ontologies(db_name, query)
        return response

    # 브랜치 관리 메서드들 (실제 OMS API 호출)
    async def create_branch(
        self, db_name: str, branch_name: str, from_branch: Optional[str] = None
    ):
        """브랜치 생성 - 실제 OMS API 호출"""
        client = get_oms_client()
        branch_data = {"branch_name": branch_name}
        if from_branch:
            branch_data["from_branch"] = from_branch

        response = await client.create_branch(db_name, branch_data)
        return response

    async def delete_branch(self, db_name: str, branch_name: str):
        """브랜치 삭제 - 실제 OMS API 호출"""
        client = get_oms_client()
        # OMS 클라이언트에 delete_branch 메서드 추가 필요
        try:
            response = await client.client.delete(f"/api/v1/branch/{db_name}/branch/{branch_name}")
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            raise RuntimeError(f"브랜치 삭제 실패 ({db_name}/{branch_name}): {e}")

    async def checkout(self, db_name: str, target: str, target_type: str):
        """체크아웃 - 실제 OMS API 호출"""
        client = get_oms_client()
        checkout_data = {"target": target, "target_type": target_type}
        try:
            response = await client.client.post(
                f"/api/v1/branch/{db_name}/checkout", json=checkout_data
            )
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            raise RuntimeError(f"체크아웃 실패 ({db_name}): {e}")

    async def commit_changes(
        self, db_name: str, message: str, author: str, branch: Optional[str] = None
    ):
        """변경사항 커밋 - 실제 OMS API 호출"""
        client = get_oms_client()
        commit_data = {"message": message, "author": author}
        if branch:
            commit_data["branch"] = branch

        try:
            response = await client.client.post(
                f"/api/v1/version/{db_name}/commit", json=commit_data
            )
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            raise RuntimeError(f"커밋 실패 ({db_name}): {e}")

    async def get_commit_history(
        self, db_name: str, branch: Optional[str] = None, limit: int = 50, offset: int = 0
    ):
        """커밋 히스토리 조회 - 실제 OMS API 호출"""
        client = get_oms_client()
        response = await client.get_version_history(db_name)
        return response

    async def get_diff(self, db_name: str, base: str, compare: str):
        """차이 비교 - 실제 OMS API 호출"""
        client = get_oms_client()
        params = {"from_ref": base, "to_ref": compare}
        try:
            response = await client.client.get(f"/api/v1/version/{db_name}/diff", params=params)
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            raise RuntimeError(f"차이 비교 실패 ({db_name}): {e}")

    async def merge_branches(
        self,
        db_name: str,
        source: str,
        target: str,
        strategy: str = "merge",
        message: Optional[str] = None,
        author: Optional[str] = None,
    ):
        """브랜치 병합 - 실제 OMS API 호출"""
        client = get_oms_client()
        merge_data = {"source": source, "target": target, "strategy": strategy}
        if message:
            merge_data["message"] = message
        if author:
            merge_data["author"] = author

        try:
            response = await client.client.post(f"/api/v1/version/{db_name}/merge", json=merge_data)
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            raise RuntimeError(f"브랜치 병합 실패 ({db_name}): {e}")

    async def rollback(
        self,
        db_name: str,
        target_commit: str,
        create_branch: bool = True,
        branch_name: Optional[str] = None,
    ):
        """롤백 - 실제 OMS API 호출"""
        client = get_oms_client()
        rollback_data = {"target_commit": target_commit, "create_branch": create_branch}
        if branch_name:
            rollback_data["branch_name"] = branch_name

        try:
            response = await client.client.post(
                f"/api/v1/version/{db_name}/rollback", json=rollback_data
            )
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            raise RuntimeError(f"롤백 실패 ({db_name}): {e}")

    async def get_branch_info(self, db_name: str, branch_name: str):
        """브랜치 정보 조회 - 실제 OMS API 호출"""
        client = get_oms_client()
        try:
            response = await client.client.get(
                f"/api/v1/branch/{db_name}/branch/{branch_name}/info"
            )
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            raise RuntimeError(f"브랜치 정보 조회 실패 ({db_name}/{branch_name}): {e}")

    # Foundry-style 병합 충돌 관련 메서드들
    async def simulate_merge(
        self, db_name: str, source_branch: str, target_branch: str, strategy: str = "merge"
    ):
        """병합 시뮬레이션 - 충돌 감지 without 실제 병합"""
        client = get_oms_client()
        merge_data = {
            "source_branch": source_branch,
            "target_branch": target_branch,
            "strategy": strategy,
        }
        try:
            response = await client.client.post(
                f"/api/v1/database/{db_name}/merge/simulate", json=merge_data
            )
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            raise RuntimeError(f"병합 시뮬레이션 실패 ({db_name}): {e}")

    async def resolve_merge_conflicts(
        self,
        db_name: str,
        source_branch: str,
        target_branch: str,
        resolutions: List[Dict[str, Any]],
        strategy: str = "merge",
        message: Optional[str] = None,
        author: Optional[str] = None,
    ):
        """수동 충돌 해결 및 병합 실행"""
        client = get_oms_client()
        resolve_data = {
            "source_branch": source_branch,
            "target_branch": target_branch,
            "resolutions": resolutions,
            "strategy": strategy,
        }
        if message:
            resolve_data["message"] = message
        if author:
            resolve_data["author"] = author

        try:
            response = await client.client.post(
                f"/api/v1/database/{db_name}/merge/resolve", json=resolve_data
            )
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            raise RuntimeError(f"충돌 해결 실패 ({db_name}): {e}")

    async def get_merge_preview(self, db_name: str, source_branch: str, target_branch: str):
        """병합 미리보기 - 변경사항과 충돌 분석"""
        client = get_oms_client()
        try:
            # 먼저 시뮬레이션 수행
            simulation_result = await self.simulate_merge(db_name, source_branch, target_branch)

            # 추가로 상세 diff 정보 가져오기
            diff_params = {"from_ref": target_branch, "to_ref": source_branch}
            diff_response = await client.client.get(
                f"/api/v1/version/{db_name}/diff", params=diff_params
            )
            diff_response.raise_for_status()
            diff_data = diff_response.json()

            # 병합 미리보기 결합
            preview = {
                "simulation": simulation_result.get("data", {}),
                "detailed_changes": diff_data.get("data", {}),
                "summary": {
                    "source_branch": source_branch,
                    "target_branch": target_branch,
                    "has_conflicts": len(
                        simulation_result.get("data", {})
                        .get("merge_preview", {})
                        .get("conflicts", [])
                    )
                    > 0,
                    "changes_count": len(diff_data.get("data", {}).get("changes", [])),
                    "mergeable": simulation_result.get("data", {})
                    .get("merge_preview", {})
                    .get("statistics", {})
                    .get("mergeable", False),
                },
            }

            return preview

        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            raise RuntimeError(f"병합 미리보기 실패 ({db_name}): {e}")

    async def get_conflict_details(
        self, db_name: str, source_branch: str, target_branch: str, conflict_id: str
    ):
        """특정 충돌의 상세 정보 조회"""
        try:
            # 먼저 시뮬레이션으로 모든 충돌 가져오기
            simulation_result = await self.simulate_merge(db_name, source_branch, target_branch)
            conflicts = (
                simulation_result.get("data", {}).get("merge_preview", {}).get("conflicts", [])
            )

            # 특정 충돌 찾기
            target_conflict = None
            for conflict in conflicts:
                if conflict.get("id") == conflict_id:
                    target_conflict = conflict
                    break

            if not target_conflict:
                raise ValueError(f"충돌 ID '{conflict_id}'를 찾을 수 없습니다")

            # 추가 컨텍스트 정보 수집 (필요시)
            conflict_details = {
                **target_conflict,
                "context": {
                    "source_branch": source_branch,
                    "target_branch": target_branch,
                    "database": db_name,
                    "related_conflicts": [
                        c.get("id")
                        for c in conflicts
                        if c.get("path", {}).get("namespace")
                        == target_conflict.get("path", {}).get("namespace")
                    ],
                },
            }

            return conflict_details

        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            raise RuntimeError(f"충돌 상세 정보 조회 실패 ({db_name}): {e}")

    # 🔥 THINK ULTRA! 고급 관계 관리 메서드들 - OMS 실제 구현 호출

    async def create_ontology_with_advanced_relationships(
        self,
        db_name: str,
        ontology_data: Dict[str, Any],
        auto_generate_inverse: bool = True,
        validate_relationships: bool = True,
        check_circular_references: bool = True,
    ) -> Dict[str, Any]:
        """고급 관계 관리 기능을 포함한 온톨로지 생성 - OMS API 호출"""
        client = get_oms_client()
        try:
            response = await client.client.post(
                f"/api/v1/ontology/{db_name}/create-advanced",
                json=ontology_data,
                params={
                    "auto_generate_inverse": auto_generate_inverse,
                    "validate_relationships": validate_relationships,
                    "check_circular_references": check_circular_references,
                },
            )
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            raise RuntimeError(f"고급 온톨로지 생성 실패 ({db_name}): {e}")

    async def validate_relationships(
        self, db_name: str, ontology_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """관계 검증 - OMS API 호출"""
        client = get_oms_client()
        try:
            response = await client.client.post(
                f"/api/v1/ontology/{db_name}/validate-relationships", json=ontology_data
            )
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            raise RuntimeError(f"관계 검증 실패 ({db_name}): {e}")

    async def detect_circular_references(
        self, db_name: str, include_new_ontology: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """순환 참조 탐지 - OMS API 호출"""
        client = get_oms_client()
        try:
            # OMS는 new_ontology를 body 최상위에서 받음
            data = include_new_ontology if include_new_ontology else None
            response = await client.client.post(
                f"/api/v1/ontology/{db_name}/detect-circular-references", json=data
            )
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            raise RuntimeError(f"순환 참조 탐지 실패 ({db_name}): {e}")

    async def analyze_relationship_network(self, db_name: str) -> Dict[str, Any]:
        """관계 네트워크 분석 - OMS API 호출"""
        client = get_oms_client()
        try:
            response = await client.client.get(f"/api/v1/ontology/{db_name}/analyze-network")
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            raise RuntimeError(f"관계 네트워크 분석 실패 ({db_name}): {e}")

    async def find_relationship_paths(
        self, db_name: str, start_entity: str, end_entity: Optional[str] = None, **query_params
    ) -> Dict[str, Any]:
        """관계 경로 탐색 - OMS API 호출"""
        client = get_oms_client()
        try:
            params = {}
            if end_entity:
                params["end_entity"] = end_entity
            params.update(query_params)  # max_depth, path_type 등

            response = await client.client.get(
                f"/api/v1/ontology/{db_name}/relationship-paths/{start_entity}", params=params
            )
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            raise RuntimeError(f"관계 경로 탐색 실패 ({db_name}): {e}")


# JSON-LD 변환기
from shared.utils.jsonld import JSONToJSONLDConverter


# 의존성 제공 함수들
def get_terminus_service() -> TerminusService:
    """TerminusService 의존성 제공"""
    return TerminusService()


def get_jsonld_converter() -> JSONToJSONLDConverter:
    """JSON-LD 변환기 의존성 제공"""
    return JSONToJSONLDConverter()


# Label Mapper 의존성 제공
from shared.utils.label_mapper import LabelMapper

# 전역 Label Mapper 인스턴스 (main.py에서 초기화)
label_mapper: Optional[LabelMapper] = None


def set_label_mapper(mapper: LabelMapper):
    """Label Mapper 설정"""
    global label_mapper
    label_mapper = mapper


def get_label_mapper() -> LabelMapper:
    """Label Mapper 의존성 제공"""
    if not label_mapper:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Label Mapper가 초기화되지 않았습니다",
        )
    return label_mapper
