"""
Service Providers for Dependency Injection

This module provides FastAPI dependency functions that use the modern
service container instead of global variables, resolving anti-pattern 13.

Features:
- FastAPI Depends() compatible functions
- Type-safe service injection
- Automatic service creation and caching
- Centralized service management
- Test-friendly mocking support
"""

import logging
from typing import Annotated, Optional

from fastapi import Depends, status

from shared.errors.error_types import ErrorCode, classified_http_exception

# Import service classes and factories - all dependencies are now explicit in pyproject.toml
from shared.services.storage.storage_service import StorageService, create_storage_service
from shared.services.storage.lakefs_storage_service import LakeFSStorageService, create_lakefs_storage_service
from shared.services.storage.redis_service import RedisService, create_redis_service
from shared.services.storage.elasticsearch_service import ElasticsearchService, create_elasticsearch_service
from shared.services.registries.lineage_store import LineageStore, create_lineage_store
from shared.services.core.audit_log_store import AuditLogStore, create_audit_log_store
from shared.services.core.background_task_manager import (
    BackgroundTaskManager,
    create_background_task_manager,
)
from shared.services.agent.llm_gateway import LLMGateway, create_llm_gateway
from shared.utils.jsonld import JSONToJSONLDConverter
from shared.utils.label_mapper import LabelMapper

# Import container and settings
from shared.dependencies.container import get_container, ServiceContainer
from shared.config.settings import ApplicationSettings, get_settings


async def get_settings_dependency() -> ApplicationSettings:
    """
    FastAPI dependency to get application settings
    
    Returns:
        ApplicationSettings: The global settings instance
    """
    return get_settings()


async def get_label_mapper(
    container: ServiceContainer = Depends(get_container),
) -> LabelMapper:
    def create_label_mapper(settings: ApplicationSettings) -> LabelMapper:  # noqa: ARG001
        return LabelMapper()
    container.ensure_singleton(LabelMapper, create_label_mapper)
    return await container.get(LabelMapper)


async def get_jsonld_converter(
    container: ServiceContainer = Depends(get_container),
) -> JSONToJSONLDConverter:
    def create_jsonld_converter(settings: ApplicationSettings) -> JSONToJSONLDConverter:  # noqa: ARG001
        return JSONToJSONLDConverter()
    container.ensure_singleton(JSONToJSONLDConverter, create_jsonld_converter)
    return await container.get(JSONToJSONLDConverter)


async def get_storage_service(
    container: ServiceContainer = Depends(get_container)
) -> StorageService:
    """
    FastAPI dependency to get StorageService instance
    
    Args:
        container: Service container (injected by FastAPI)
        
    Returns:
        StorageService: Storage service instance
        
    Note:
        boto3 is now a required dependency in shared/pyproject.toml.
        Missing dependencies will fail at build/install time, not runtime.
    """
    # Register factory if not already registered
    container.ensure_singleton(StorageService, create_storage_service)
    
    return await container.get(StorageService)


async def get_lakefs_storage_service(
    container: ServiceContainer = Depends(get_container),
) -> LakeFSStorageService:
    """
    FastAPI dependency to get LakeFSStorageService instance (S3 gateway via lakeFS).

    This is separate from the default StorageService (MinIO) because lakeFS repositories
    are not managed via S3 CreateBucket and require different endpoint/credentials.
    """
    container.ensure_singleton(LakeFSStorageService, create_lakefs_storage_service)
    return await container.get(LakeFSStorageService)


async def get_redis_service(
    container: ServiceContainer = Depends(get_container)
) -> RedisService:
    """
    FastAPI dependency to get RedisService instance
    
    Args:
        container: Service container (injected by FastAPI)
        
    Returns:
        RedisService: Redis service instance
    """
    # Register factory if not already registered
    container.ensure_singleton(RedisService, create_redis_service)
    
    return await container.get(RedisService)


async def get_elasticsearch_service(
    container: ServiceContainer = Depends(get_container)
) -> ElasticsearchService:
    """
    FastAPI dependency to get ElasticsearchService instance
    
    Args:
        container: Service container (injected by FastAPI)
        
    Returns:
        ElasticsearchService: Elasticsearch service instance
    """
    # Register factory if not already registered
    container.ensure_singleton(ElasticsearchService, create_elasticsearch_service)
    
    return await container.get(ElasticsearchService)

async def get_lineage_store(
    container: ServiceContainer = Depends(get_container),
) -> LineageStore:
    """FastAPI dependency to get LineageStore instance."""
    container.ensure_singleton(LineageStore, create_lineage_store)
    return await container.get(LineageStore)


async def get_audit_log_store(
    container: ServiceContainer = Depends(get_container),
) -> AuditLogStore:
    """FastAPI dependency to get AuditLogStore instance."""
    container.ensure_singleton(AuditLogStore, create_audit_log_store)
    return await container.get(AuditLogStore)

async def get_llm_gateway(
    container: ServiceContainer = Depends(get_container),
) -> LLMGateway:
    """FastAPI dependency to get LLMGateway instance."""
    container.ensure_singleton(LLMGateway, create_llm_gateway)
    return await container.get(LLMGateway)

async def get_background_task_manager(
    container: ServiceContainer = Depends(get_container),
) -> BackgroundTaskManager:
    """FastAPI dependency to get BackgroundTaskManager instance."""
    try:
        if container.has(BackgroundTaskManager) and container.is_created(BackgroundTaskManager):
            return await container.get(BackgroundTaskManager)

        container.ensure_singleton(RedisService, create_redis_service)
        redis_service = await container.get(RedisService)

        task_manager = create_background_task_manager(redis_service)
        container.ensure_instance(BackgroundTaskManager, task_manager)
        return await container.get(BackgroundTaskManager)
    except Exception as e:
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Background task service unavailable",
            code=ErrorCode.UPSTREAM_UNAVAILABLE,
        ) from e


async def get_initialized_background_task_manager(
    container: ServiceContainer = Depends(get_container),
) -> Optional[BackgroundTaskManager]:
    """FastAPI dependency to get an already-initialized BackgroundTaskManager, if present."""
    if not (container.has(BackgroundTaskManager) and container.is_created(BackgroundTaskManager)):
        return None
    return await container.get(BackgroundTaskManager)


# Type annotations for cleaner dependency injection - storage is now always available
StorageServiceDep = Annotated[StorageService, Depends(get_storage_service)]
LakeFSStorageServiceDep = Annotated[LakeFSStorageService, Depends(get_lakefs_storage_service)]

RedisServiceDep = Annotated[RedisService, Depends(get_redis_service)]
ElasticsearchServiceDep = Annotated[ElasticsearchService, Depends(get_elasticsearch_service)]
LineageStoreDep = Annotated[LineageStore, Depends(get_lineage_store)]
AuditLogStoreDep = Annotated[AuditLogStore, Depends(get_audit_log_store)]
LLMGatewayDep = Annotated[LLMGateway, Depends(get_llm_gateway)]
LabelMapperDep = Annotated[LabelMapper, Depends(get_label_mapper)]
JSONLDConverterDep = Annotated[JSONToJSONLDConverter, Depends(get_jsonld_converter)]
SettingsDep = Annotated[ApplicationSettings, Depends(get_settings_dependency)]
BackgroundTaskManagerDep = Annotated[BackgroundTaskManager, Depends(get_background_task_manager)]
InitializedBackgroundTaskManagerDep = Annotated[
    Optional[BackgroundTaskManager],
    Depends(get_initialized_background_task_manager),
]


def register_core_services(container: ServiceContainer) -> None:
    """
    Register all core services with the container
    
    This should be called during application startup to pre-register
    all service factories with the container.
    
    Args:
        container: Service container to register with
    """
    # Register service factories
    container.ensure_singleton(StorageService, create_storage_service)
    container.ensure_singleton(RedisService, create_redis_service)
    container.ensure_singleton(ElasticsearchService, create_elasticsearch_service)
    container.ensure_singleton(LineageStore, create_lineage_store)
    container.ensure_singleton(AuditLogStore, create_audit_log_store)
    container.ensure_singleton(LLMGateway, create_llm_gateway)
    
    # Log registration
    import logging
    logger = logging.getLogger(__name__)
    logger.info("Core services registered with container")


async def health_check_core_services(container: ServiceContainer) -> dict:
    """
    Perform health check on all core services
    
    Args:
        container: Service container
        
    Returns:
        Dict with health check results
    """
    results = {}
    
    # Check each core service
    core_services = [StorageService, RedisService, ElasticsearchService]
    
    for service_type in core_services:
        service_name = service_type.__name__
        try:
            if container.has(service_type) and container.is_created(service_type):
                service = await container.get(service_type)
                if hasattr(service, 'health_check'):
                    results[service_name] = await service.health_check()
                else:
                    results[service_name] = True  # Assume healthy if no health check
            else:
                results[service_name] = None  # Not created yet
        except Exception as e:
            logging.getLogger(__name__).warning("Exception fallback at shared/dependencies/providers.py:271", exc_info=True)
            results[service_name] = f"Error: {str(e)}"
    
    return results
