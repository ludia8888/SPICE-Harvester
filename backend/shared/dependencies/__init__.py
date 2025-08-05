"""
Modern Dependency Injection System for SPICE HARVESTER

This package provides a centralized, type-safe dependency injection system
to replace scattered global variables and resolve anti-pattern 13.

Main Components:
- container: Service container for centralized service management
- providers: FastAPI dependency providers for service injection

Usage:
    # In application startup
    from shared.dependencies import initialize_container
    from shared.config.settings import settings
    
    container = await initialize_container(settings)
    
    # In FastAPI routes
    from shared.dependencies.providers import StorageServiceDep
    
    async def my_route(storage: StorageServiceDep):
        # Use storage service
        pass
"""

from .container import (
    ServiceContainer,
    initialize_container,
    get_container,
    shutdown_container,
    container_lifespan,
    get_settings_from_container
)

from .providers import (
    get_storage_service,
    get_redis_service,
    get_elasticsearch_service,
    get_settings_dependency,
    StorageServiceDep,
    RedisServiceDep,
    ElasticsearchServiceDep,
    SettingsDep,
    register_core_services,
    health_check_core_services
)

from .type_inference import configure_type_inference_service

__all__ = [
    # Container
    'ServiceContainer',
    'initialize_container',
    'get_container',
    'shutdown_container',
    'container_lifespan',
    'get_settings_from_container',
    
    # Providers
    'get_storage_service',
    'get_redis_service', 
    'get_elasticsearch_service',
    'get_settings_dependency',
    'StorageServiceDep',
    'RedisServiceDep',
    'ElasticsearchServiceDep',
    'SettingsDep',
    'register_core_services',
    'health_check_core_services',
    
    # Type inference
    'configure_type_inference_service'
]