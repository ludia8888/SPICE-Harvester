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

from typing import Annotated
from fastapi import Depends

# Import service classes and factories - make storage conditional to avoid boto3 dependency
try:
    from shared.services.storage_service import StorageService, create_storage_service
    _STORAGE_AVAILABLE = True
except ImportError:
    StorageService = None
    create_storage_service = None
    _STORAGE_AVAILABLE = False

from shared.services.redis_service import RedisService, create_redis_service
from shared.services.elasticsearch_service import ElasticsearchService, create_elasticsearch_service

# Import container and settings
from shared.dependencies.container import get_container, ServiceContainer
from shared.config.settings import ApplicationSettings, get_settings


async def get_settings_dependency() -> ApplicationSettings:
    """
    FastAPI dependency to get application settings
    
    Returns:
        ApplicationSettings: The global settings instance
    """
    from shared.config.settings import settings
    return settings


if _STORAGE_AVAILABLE:
    async def get_storage_service(
        container: ServiceContainer = Depends(get_container)
    ) -> StorageService:
        """
        FastAPI dependency to get StorageService instance
        
        Args:
            container: Service container (injected by FastAPI)
            
        Returns:
            StorageService: Storage service instance
        """
        # Register factory if not already registered
        if not container.has(StorageService):
            container.register_singleton(StorageService, create_storage_service)
        
        return await container.get(StorageService)
else:
    async def get_storage_service(*args, **kwargs):
        """Fallback storage service when boto3 is not available"""
        raise RuntimeError("StorageService requires boto3 - install it to use storage features")


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
    if not container.has(RedisService):
        container.register_singleton(RedisService, create_redis_service)
    
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
    if not container.has(ElasticsearchService):
        container.register_singleton(ElasticsearchService, create_elasticsearch_service)
    
    return await container.get(ElasticsearchService)


# Type annotations for cleaner dependency injection - make storage conditional
if _STORAGE_AVAILABLE:
    StorageServiceDep = Annotated[StorageService, Depends(get_storage_service)]
else:
    StorageServiceDep = Annotated[type(None), Depends(get_storage_service)]  # Will raise error when used

RedisServiceDep = Annotated[RedisService, Depends(get_redis_service)]
ElasticsearchServiceDep = Annotated[ElasticsearchService, Depends(get_elasticsearch_service)]
SettingsDep = Annotated[ApplicationSettings, Depends(get_settings_dependency)]


def register_core_services(container: ServiceContainer) -> None:
    """
    Register all core services with the container
    
    This should be called during application startup to pre-register
    all service factories with the container.
    
    Args:
        container: Service container to register with
    """
    # Register service factories
    container.register_singleton(StorageService, create_storage_service)
    container.register_singleton(RedisService, create_redis_service)
    container.register_singleton(ElasticsearchService, create_elasticsearch_service)
    
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
            results[service_name] = f"Error: {str(e)}"
    
    return results