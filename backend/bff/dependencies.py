"""
BFF Dependencies - Modernized Version

This is the modernized version of BFF dependencies that resolves anti-pattern 13:
- Uses modern dependency injection container instead of global variables
- Eliminates setter/getter patterns with FastAPI Depends
- Type-safe dependency injection with proper error handling
- Test-friendly architecture with easy mocking support

Key improvements:
1. ✅ No global variables
2. ✅ No setter/getter functions
3. ✅ FastAPI Depends() compatible
4. ✅ Type-safe dependencies
5. ✅ Container-based service management
6. ✅ Easy testing and mocking
"""

from typing import Any, Dict, List, Optional
import json

import httpx
from fastapi import HTTPException, status, Depends

# Modern dependency injection imports
from shared.dependencies import get_container, ServiceContainer
from shared.dependencies.providers import (
    StorageServiceDep,
    RedisServiceDep,
    ElasticsearchServiceDep,
    SettingsDep
)
from shared.config.settings import ApplicationSettings
from shared.utils.label_mapper import LabelMapper
from shared.utils.jsonld import JSONToJSONLDConverter
from shared.services import ElasticsearchService

# BFF specific imports
from bff.services.oms_client import OMSClient


class BFFDependencyProvider:
    """
    Modern dependency provider for BFF services
    
    This class replaces the global variables and setter/getter pattern
    with a container-based approach that's type-safe and test-friendly.
    """
    
    @staticmethod
    async def get_oms_client(
        container: ServiceContainer = Depends(get_container)
    ) -> OMSClient:
        """
        Get OMS client from container
        
        This replaces the global oms_client variable and get_oms_client() function.
        """
        # Register OMSClient factory if not already registered
        if not container.has(OMSClient):
            def create_oms_client(settings: ApplicationSettings) -> OMSClient:
                return OMSClient(settings.services.oms_base_url)
            
            container.register_singleton(OMSClient, create_oms_client)
        
        try:
            return await container.get(OMSClient)
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"OMS client not available: {str(e)}",
            )
    
    @staticmethod
    async def get_label_mapper(
        container: ServiceContainer = Depends(get_container)
    ) -> LabelMapper:
        """
        Get label mapper from container
        
        This replaces the global label_mapper variable and get_label_mapper() function.
        """
        # Register LabelMapper factory if not already registered
        if not container.has(LabelMapper):
            def create_label_mapper(settings: ApplicationSettings) -> LabelMapper:
                return LabelMapper()
            
            container.register_singleton(LabelMapper, create_label_mapper)
        
        try:
            return await container.get(LabelMapper)
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Label mapper not available: {str(e)}",
            )
    
    @staticmethod
    async def get_jsonld_converter(
        container: ServiceContainer = Depends(get_container)
    ) -> JSONToJSONLDConverter:
        """
        Get JSON-LD converter from container
        
        This provides a centralized way to get the JSON-LD converter.
        """
        # Register JSONToJSONLDConverter factory if not already registered
        if not container.has(JSONToJSONLDConverter):
            def create_jsonld_converter(settings: ApplicationSettings) -> JSONToJSONLDConverter:
                return JSONToJSONLDConverter()
            
            container.register_singleton(JSONToJSONLDConverter, create_jsonld_converter)
        
        try:
            return await container.get(JSONToJSONLDConverter)
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"JSON-LD converter not available: {str(e)}",
            )


# Type-safe dependency annotations for cleaner injection
OMSClientDep = Depends(BFFDependencyProvider.get_oms_client)
LabelMapperDep = Depends(BFFDependencyProvider.get_label_mapper)
JSONLDConverterDep = Depends(BFFDependencyProvider.get_jsonld_converter)
# TerminusServiceDep is defined after get_terminus_service function


class TerminusService:
    """
    OMS client wrapper for TerminusService compatibility - Modernized version
    
    This class wraps the OMS client to provide TerminusDB-compatible interface
    without relying on global variables.
    """

    def __init__(self, oms_client: OMSClient):
        """
        Initialize with OMS client dependency
        
        Args:
            oms_client: OMS client instance from dependency injection
        """
        self.oms_client = oms_client
        self.connected = False

    async def list_databases(self):
        """데이터베이스 목록 조회"""
        response = await self.oms_client.list_databases()
        if isinstance(response, dict) and response.get("status") == "success":
            databases = response.get("data", {}).get("databases", [])
            return [db.get("name") for db in databases if db.get("name")]
        elif isinstance(response, list):
            # 직접 리스트가 반환된 경우
            return [db.get("name") for db in response if isinstance(db, dict) and db.get("name")]
        return []

    async def create_database(self, db_name: str, description: Optional[str] = None):
        """데이터베이스 생성"""
        response = await self.oms_client.create_database(db_name, description)
        return response

    async def delete_database(self, db_name: str):
        """데이터베이스 삭제"""
        response = await self.oms_client.delete_database(db_name)
        return response

    async def get_database_info(self, db_name: str):
        """데이터베이스 정보 조회"""
        response = await self.oms_client.check_database_exists(db_name)
        return response

    async def list_classes(self, db_name: str):
        """클래스 목록 조회"""
        response = await self.oms_client.list_ontologies(db_name)
        if response.get("status") == "success":
            ontologies = response.get("data", {}).get("ontologies", [])
            return ontologies
        return []

    async def create_class(self, db_name: str, class_data: dict):
        """클래스 생성"""
        response = await self.oms_client.create_ontology(db_name, class_data)
        # Return the created data
        if response and response.get("status") == "success":
            return response.get("data", {})
        return response

    async def get_class(self, db_name: str, class_id: str):
        """클래스 조회"""
        try:
            response = await self.oms_client.get_ontology(db_name, class_id)
            # Extract the data from the response
            if response and response.get("status") == "success":
                return response.get("data", {})
            return None
        except httpx.HTTPStatusError as e:
            # If it's a 404, return None (not found)
            if e.response.status_code == 404:
                return None
            # Re-raise other HTTP errors
            raise
        except Exception:
            # Re-raise other exceptions
            raise

    async def update_class(self, db_name: str, class_id: str, class_data: dict):
        """클래스 업데이트"""
        response = await self.oms_client.update_ontology(db_name, class_id, class_data)
        return response

    async def delete_class(self, db_name: str, class_id: str):
        """클래스 삭제"""
        response = await self.oms_client.delete_ontology(db_name, class_id)
        return response

    async def query_database(self, db_name: str, query: str):
        """데이터베이스 쿼리"""
        response = await self.oms_client.query_ontologies(db_name, query)
        return response

    # Branch management methods (실제 OMS API 호출)
    async def create_branch(
        self, db_name: str, branch_name: str, from_branch: Optional[str] = None
    ):
        """브랜치 생성 - 실제 OMS API 호출"""
        branch_data = {"branch_name": branch_name}
        if from_branch:
            branch_data["from_branch"] = from_branch

        response = await self.oms_client.create_branch(db_name, branch_data)
        return response

    async def delete_branch(self, db_name: str, branch_name: str):
        """브랜치 삭제 - 실제 OMS API 호출"""
        try:
            response = await self.oms_client.client.delete(f"/api/v1/branch/{db_name}/branch/{branch_name}")
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            raise RuntimeError(f"브랜치 삭제 실패 ({db_name}/{branch_name}): {e}")

    async def checkout(self, db_name: str, target: str, target_type: str):
        """체크아웃 - 실제 OMS API 호출"""
        checkout_data = {"target": target, "target_type": target_type}
        try:
            response = await self.oms_client.client.post(
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
        commit_data = {"message": message, "author": author}
        if branch:
            commit_data["branch"] = branch

        try:
            response = await self.oms_client.client.post(
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
        response = await self.oms_client.get_version_history(db_name)
        return response

    async def get_diff(self, db_name: str, base: str, compare: str):
        """차이 비교 - 실제 OMS API 호출"""
        params = {"from_ref": base, "to_ref": compare}
        try:
            response = await self.oms_client.client.get(f"/api/v1/version/{db_name}/diff", params=params)
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
        merge_data = {"source": source, "target": target, "strategy": strategy}
        if message:
            merge_data["message"] = message
        if author:
            merge_data["author"] = author

        try:
            response = await self.oms_client.client.post(f"/api/v1/version/{db_name}/merge", json=merge_data)
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
        rollback_data = {"target_commit": target_commit, "create_branch": create_branch}
        if branch_name:
            rollback_data["branch_name"] = branch_name

        try:
            response = await self.oms_client.client.post(
                f"/api/v1/version/{db_name}/rollback", json=rollback_data
            )
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            raise RuntimeError(f"롤백 실패 ({db_name}): {e}")

    async def get_branch_info(self, db_name: str, branch_name: str):
        """브랜치 정보 조회 - 실제 OMS API 호출"""
        try:
            response = await self.oms_client.client.get(
                f"/api/v1/branch/{db_name}/branch/{branch_name}/info"
            )
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            raise RuntimeError(f"브랜치 정보 조회 실패 ({db_name}/{branch_name}): {e}")

    # Merge conflict related methods (Foundry-style)
    async def simulate_merge(
        self, db_name: str, source_branch: str, target_branch: str, strategy: str = "merge"
    ):
        """병합 시뮬레이션 - 충돌 감지 without 실제 병합"""
        merge_data = {
            "source_branch": source_branch,
            "target_branch": target_branch,
            "strategy": strategy,
        }
        try:
            response = await self.oms_client.client.post(
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
            response = await self.oms_client.client.post(
                f"/api/v1/database/{db_name}/merge/resolve", json=resolve_data
            )
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, httpx.TimeoutException, ValueError) as e:
            raise RuntimeError(f"충돌 해결 실패 ({db_name}): {e}")

    # Advanced relationship management methods - OMS API calls
    async def create_ontology_with_advanced_relationships(
        self,
        db_name: str,
        ontology_data: Dict[str, Any],
        auto_generate_inverse: bool = True,
        validate_relationships: bool = True,
        check_circular_references: bool = True,
    ) -> Dict[str, Any]:
        """고급 관계 관리 기능을 포함한 온톨로지 생성 - OMS API 호출"""
        try:
            response = await self.oms_client.client.post(
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


async def get_terminus_service(
    oms_client: OMSClient = Depends(BFFDependencyProvider.get_oms_client)
) -> TerminusService:
    """
    Get TerminusService with modern dependency injection
    
    This replaces the old get_terminus_service() function that created
    a new instance every time, with a proper dependency-injected version.
    """
    return TerminusService(oms_client)


# Type-safe dependency annotation for TerminusService (defined after function)
TerminusServiceDep = Depends(get_terminus_service)

# Convenience dependency annotations for backward compatibility
get_oms_client = BFFDependencyProvider.get_oms_client
get_label_mapper = BFFDependencyProvider.get_label_mapper
get_jsonld_converter = BFFDependencyProvider.get_jsonld_converter
get_elasticsearch_service = ElasticsearchServiceDep
get_storage_service = StorageServiceDep


# Health check function for the modernized dependencies
async def check_bff_dependencies_health(
    container: ServiceContainer = Depends(get_container)
) -> Dict[str, Any]:
    """
    Check health of all BFF dependencies
    
    This provides a way to verify that all dependencies are properly
    initialized and accessible through the modern container system.
    """
    health_status = {}
    
    try:
        # Check each service
        services_to_check = [
            ("oms_client", OMSClient),
            ("label_mapper", LabelMapper),
            ("jsonld_converter", JSONToJSONLDConverter),
            ("elasticsearch_service", ElasticsearchService),
        ]
        
        for service_name, service_type in services_to_check:
            try:
                if container.has(service_type):
                    service = await container.get(service_type)
                    # Perform basic health check if available
                    if hasattr(service, 'health_check'):
                        is_healthy = await service.health_check()
                        health_status[service_name] = "healthy" if is_healthy else "unhealthy"
                    else:
                        health_status[service_name] = "available"
                else:
                    health_status[service_name] = "not_registered"
            except Exception as e:
                health_status[service_name] = f"error: {str(e)}"
        
        return {
            "status": "ok",
            "services": health_status,
            "container_initialized": container.is_initialized
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "services": health_status
        }