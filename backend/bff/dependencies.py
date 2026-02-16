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

import httpx
from fastapi import status, Depends

# Modern dependency injection imports
from shared.dependencies import get_container, ServiceContainer
from shared.dependencies.providers import (
    get_elasticsearch_service as get_shared_elasticsearch_service,
    get_jsonld_converter as get_shared_jsonld_converter,
    get_label_mapper as get_shared_label_mapper,
    get_redis_service as get_shared_redis_service,
    get_storage_service as get_shared_storage_service,
)
from shared.config.settings import ApplicationSettings
from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode, classified_http_exception
from shared.observability.request_context import get_correlation_id, get_request_id
from shared.utils.foundry_page_token import encode_offset_page_token
from shared.utils.label_mapper import LabelMapper
from shared.utils.jsonld import JSONToJSONLDConverter
from shared.services.storage.elasticsearch_service import ElasticsearchService
from shared.services.registries.action_log_registry import ActionLogRegistry

# BFF specific imports
from bff.services.oms_client import OMSClient
import logging

SERVICE_NAME = "BFF"


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
            raise classified_http_exception(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                f"OMS client not available: {str(e)}",
                code=ErrorCode.UPSTREAM_UNAVAILABLE,
            )
    
    @staticmethod
    async def get_action_log_registry(
        container: ServiceContainer = Depends(get_container),
    ) -> ActionLogRegistry:
        """
        Get ActionLogRegistry (Postgres-backed) from container.

        This provides a stable read surface for Action-only writeback audit logs.
        """
        if not container.has(ActionLogRegistry):
            def create_action_logs(settings: ApplicationSettings) -> ActionLogRegistry:  # noqa: ARG001
                return ActionLogRegistry()

            container.register_singleton(ActionLogRegistry, create_action_logs)

        try:
            return await container.get(ActionLogRegistry)
        except Exception as e:
            raise classified_http_exception(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                f"ActionLogRegistry not available: {str(e)}",
                code=ErrorCode.UPSTREAM_UNAVAILABLE,
            ) from e


# Type-safe dependency annotations for cleaner injection
OMSClientDep = Depends(BFFDependencyProvider.get_oms_client)
LabelMapperDep = Depends(get_shared_label_mapper)
JSONLDConverterDep = Depends(get_shared_jsonld_converter)
# FoundryQueryServiceDep is defined after get_foundry_query_service function


class FoundryQueryService:
    """
    OMS client wrapper for Foundry-style ontology/query operations.
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

    async def delete_database(self, db_name: str, *, expected_seq: int):
        """데이터베이스 삭제"""
        response = await self.oms_client.delete_database(db_name, expected_seq=int(expected_seq))
        return response

    async def get_database_info(self, db_name: str):
        """데이터베이스 정보 조회"""
        response = await self.oms_client.get_database(db_name)
        return response

    async def list_classes(self, db_name: str, *, branch: str = "main"):
        """클래스 목록 조회"""
        response = await self.oms_client.list_ontologies(db_name, branch=branch)
        if response.get("status") == "success":
            ontologies = response.get("data", {}).get("ontologies", [])
            return ontologies
        return []

    async def create_class(
        self,
        db_name: str,
        class_data: dict,
        *,
        branch: str = "main",
        headers: Optional[Dict[str, str]] = None,
    ):
        """클래스 생성"""
        response = await self.oms_client.create_ontology(db_name, class_data, branch=branch, headers=headers)
        # Return the created data
        if response and response.get("status") == "success":
            return response.get("data", {})
        return response

    async def get_class(self, db_name: str, class_id: str, *, branch: str = "main"):
        """클래스 조회"""
        try:
            response = await self.oms_client.get_ontology(db_name, class_id, branch=branch)
            # Extract the data from the response
            if response and response.get("status") == "success":
                return response.get("data", {})
            return None
        except httpx.HTTPStatusError as e:
            # If it's a 404, return None (not found)
            if e.response.status_code == 404:
                return None
            detail: Any = e.response.text
            try:
                detail_json = e.response.json()
                if isinstance(detail_json, dict):
                    detail = detail_json.get("detail") or detail_json
            except ValueError:
                logging.getLogger(__name__).warning(
                    "Failed to parse upstream ontology error payload as JSON",
                    exc_info=True,
                )
            raise classified_http_exception(e.response.status_code, str(detail), code=ErrorCode.UPSTREAM_ERROR) from e
        except Exception:
            # Re-raise other exceptions
            raise

    async def update_class(
        self,
        db_name: str,
        class_id: str,
        class_data: dict,
        *,
        expected_seq: int,
        branch: str = "main",
        headers: Optional[Dict[str, str]] = None,
    ):
        """클래스 업데이트"""
        response = await self.oms_client.update_ontology(
            db_name, class_id, class_data, expected_seq=int(expected_seq), branch=branch, headers=headers
        )
        return response

    async def delete_class(
        self,
        db_name: str,
        class_id: str,
        *,
        expected_seq: int,
        branch: str = "main",
        headers: Optional[Dict[str, str]] = None,
    ):
        """클래스 삭제"""
        response = await self.oms_client.delete_ontology(
            db_name, class_id, expected_seq=int(expected_seq), branch=branch, headers=headers
        )
        return response

    async def query_database(self, db_name: str, query: Dict[str, Any]):
        """데이터베이스 쿼리 (Foundry Search Objects v2 adapter)."""
        if not isinstance(query, dict):
            raise ValueError("query must be an object")

        class_id = str(query.get("class_id") or "").strip()
        if not class_id:
            raise ValueError("class_id is required")

        raw_limit = query.get("limit", 100)
        raw_offset = query.get("offset", 0)
        try:
            limit = int(raw_limit)
            offset = int(raw_offset)
        except Exception as exc:
            raise ValueError("limit/offset must be integers") from exc

        if limit < 1:
            limit = 1
        if limit > 1000:
            limit = 1000
        if offset < 0:
            raise ValueError("offset must be >= 0")

        filters = query.get("filters") or []
        where = self._build_foundry_where(filters)
        page_token = self._encode_page_token(offset) if offset > 0 else None
        branch = str(query.get("branch") or "main").strip() or "main"
        select_fields: Optional[List[str]] = None
        if isinstance(query.get("select"), list):
            select_fields = [str(v).strip() for v in query["select"] if str(v).strip()]

        order_by = str(query.get("order_by") or "").strip() or None
        order_direction = str(query.get("order_direction") or "asc").strip().lower() or "asc"
        if order_direction not in {"asc", "desc"}:
            raise ValueError("order_direction must be 'asc' or 'desc'")

        response = await self.oms_client.search_objects_v2(
            db_name=db_name,
            object_type=class_id,
            where=where,
            page_size=limit,
            page_token=page_token,
            select=select_fields,
            order_by=order_by,
            order_direction=order_direction,
            branch=branch,
        )

        data = response.get("data") if isinstance(response, dict) else None
        rows: List[Dict[str, Any]] = list(data) if isinstance(data, list) else []

        total_count = 0
        if isinstance(response, dict):
            try:
                total_count = int(response.get("totalCount") or 0)
            except (TypeError, ValueError):
                total_count = len(rows)

        return {
            "data": rows,
            "count": total_count,
            "nextPageToken": response.get("nextPageToken") if isinstance(response, dict) else None,
        }

    @staticmethod
    def _encode_page_token(offset: int) -> str:
        return encode_offset_page_token(offset)

    @staticmethod
    def _build_foundry_where(filters: Any) -> Dict[str, Any]:
        if not isinstance(filters, list) or not filters:
            # Match-all equivalent within SearchJsonQueryV2.
            return {"type": "not", "value": {"type": "isNull", "field": "instance_id"}}

        clauses: List[Dict[str, Any]] = []
        for f in filters:
            if not isinstance(f, dict):
                continue
            clauses.append(FoundryQueryService._map_filter_to_foundry(f))

        if not clauses:
            return {"type": "not", "value": {"type": "isNull", "field": "instance_id"}}
        if len(clauses) == 1:
            return clauses[0]
        return {"type": "and", "value": clauses}

    @staticmethod
    def _map_filter_to_foundry(filter_item: Dict[str, Any]) -> Dict[str, Any]:
        field = str(filter_item.get("field") or "").strip()
        if not field:
            raise ValueError("filter.field is required")

        operator = str(filter_item.get("operator") or "").strip().lower()
        value = filter_item.get("value")

        if operator == "eq":
            return {"type": "eq", "field": field, "value": value}
        if operator == "ne":
            return {"type": "not", "value": {"type": "eq", "field": field, "value": value}}
        if operator == "gt":
            return {"type": "gt", "field": field, "value": value}
        if operator in {"ge", "gte"}:
            return {"type": "gte", "field": field, "value": value}
        if operator == "lt":
            return {"type": "lt", "field": field, "value": value}
        if operator in {"le", "lte"}:
            return {"type": "lte", "field": field, "value": value}
        if operator == "is_null":
            return {"type": "isNull", "field": field}
        if operator == "is_not_null":
            return {"type": "not", "value": {"type": "isNull", "field": field}}
        if operator == "like":
            return {"type": "containsAnyTerm", "field": field, "value": str(value or "")}
        if operator == "in":
            if not isinstance(value, list) or not value:
                raise ValueError("in operator requires a non-empty list value")
            return {"type": "or", "value": [{"type": "eq", "field": field, "value": v} for v in value]}
        if operator == "not_in":
            if not isinstance(value, list) or not value:
                raise ValueError("not_in operator requires a non-empty list value")
            return {
                "type": "not",
                "value": {"type": "or", "value": [{"type": "eq", "field": field, "value": v} for v in value]},
            }

        raise ValueError(f"Unsupported query operator: {operator}")

async def get_foundry_query_service(
    oms_client: OMSClient = Depends(BFFDependencyProvider.get_oms_client)
) -> FoundryQueryService:
    """
    Get FoundryQueryService with modern dependency injection.
    """
    return FoundryQueryService(oms_client)


FoundryQueryServiceDep = Depends(get_foundry_query_service)

# Convenience dependency annotations for backward compatibility
get_oms_client = BFFDependencyProvider.get_oms_client
get_label_mapper = get_shared_label_mapper
get_jsonld_converter = get_shared_jsonld_converter
get_action_log_registry = BFFDependencyProvider.get_action_log_registry
get_elasticsearch_service = get_shared_elasticsearch_service
get_storage_service = get_shared_storage_service
get_redis_service = get_shared_redis_service


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
                logging.getLogger(__name__).warning("Broad exception fallback at bff/dependencies.py:607", exc_info=True)
                health_status[service_name] = f"error: {str(e)}"
        
        return {
            "status": "ok",
            "services": health_status,
            "container_initialized": container.is_initialized
        }
        
    except Exception as e:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/dependencies.py:616", exc_info=True)
        return build_error_envelope(
            service_name=SERVICE_NAME,
            message="BFF dependency health check failed",
            detail=str(e),
            code=ErrorCode.INTERNAL_ERROR,
            category=ErrorCategory.INTERNAL,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            context={"services": health_status},
            request_id=get_request_id(),
            correlation_id=get_correlation_id(),
        )
