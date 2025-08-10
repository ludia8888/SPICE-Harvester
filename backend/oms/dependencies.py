"""
OMS Dependencies - Modernized Version

This is the modernized version of OMS dependencies that resolves anti-pattern 13:
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

from typing import Annotated
from fastapi import HTTPException, status, Path, Depends

# Modern dependency injection imports
from shared.dependencies import get_container, ServiceContainer
from shared.dependencies.providers import (
    RedisServiceDep,
    ElasticsearchServiceDep,
    SettingsDep
)
from shared.config.settings import ApplicationSettings
from shared.utils.label_mapper import LabelMapper
from shared.utils.jsonld import JSONToJSONLDConverter
from shared.services.elasticsearch_service import ElasticsearchService
from shared.services.redis_service import RedisService
from shared.services.command_status_service import CommandStatusService

# OMS specific imports
from oms.services.async_terminus import AsyncTerminusService
from oms.database.outbox import OutboxService
from shared.models.config import ConnectionConfig

# Import validation functions
from shared.security.input_sanitizer import validate_db_name, validate_class_id


class OMSDependencyProvider:
    """
    Modern dependency provider for OMS services
    
    This class replaces the global variables and setter/getter pattern
    with a container-based approach that's type-safe and test-friendly.
    """
    
    @staticmethod
    async def get_terminus_service(
        container: ServiceContainer = Depends(get_container)
    ) -> AsyncTerminusService:
        """
        Get AsyncTerminusService from container
        
        This replaces the global terminus_service variable and get_terminus_service() function.
        """
        # Register AsyncTerminusService factory if not already registered
        if not container.has(AsyncTerminusService):
            def create_terminus_service(settings: ApplicationSettings) -> AsyncTerminusService:
                connection_info = ConnectionConfig(
                    server_url=settings.database.terminus_url,
                    user=settings.database.terminus_user,
                    account=settings.database.terminus_account,
                    key="admin123",  # Fixed: Use admin password for TerminusDB authentication
                )
                return AsyncTerminusService(connection_info)
            
            container.register_singleton(AsyncTerminusService, create_terminus_service)
        
        try:
            return await container.get(AsyncTerminusService)
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"TerminusDB 서비스가 초기화되지 않았습니다: {str(e)}",
            )
    
    @staticmethod
    async def get_jsonld_converter(
        container: ServiceContainer = Depends(get_container)
    ) -> JSONToJSONLDConverter:
        """
        Get JSON-LD converter from container
        
        This replaces the global jsonld_converter variable and get_jsonld_converter() function.
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
                detail=f"JSON-LD 변환기가 초기화되지 않았습니다: {str(e)}",
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
                detail=f"레이블 매퍼가 초기화되지 않았습니다: {str(e)}",
            )
    
    @staticmethod
    async def get_outbox_service(
        container: ServiceContainer = Depends(get_container)
    ) -> OutboxService:
        """
        Get outbox service from container
        
        This replaces the global outbox_service variable and get_outbox_service() function.
        Note: Returns None if outbox service is not available (optional service).
        """
        # Register OutboxService factory if not already registered
        if not container.has(OutboxService):
            def create_outbox_service(settings: ApplicationSettings) -> OutboxService:
                # OutboxService creation will depend on PostgreSQL connection
                # This will be handled during container initialization
                from oms.database.postgres import db as postgres_db
                return OutboxService(postgres_db)
            
            container.register_singleton(OutboxService, create_outbox_service)
        
        try:
            return await container.get(OutboxService)
        except Exception:
            # Outbox service is optional - return None if not available
            return None
    
    @staticmethod
    async def get_command_status_service(
        container: ServiceContainer = Depends(get_container)
    ) -> CommandStatusService:
        """
        Get command status service from container
        
        This replaces the global command_status_service variable and get_command_status_service() function.
        """
        # Register CommandStatusService factory if not already registered
        if not container.has(CommandStatusService):
            def create_command_status_service(settings: ApplicationSettings) -> CommandStatusService:
                # CommandStatusService requires Redis service
                redis_service = container.get_sync(RedisService)
                return CommandStatusService(redis_service)
            
            container.register_singleton(CommandStatusService, create_command_status_service)
        
        try:
            return await container.get(CommandStatusService)
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Command 상태 추적 서비스가 초기화되지 않았습니다: {str(e)}",
            )


# Type-safe dependency annotations for cleaner injection
TerminusServiceDep = Depends(OMSDependencyProvider.get_terminus_service)
JSONLDConverterDep = Depends(OMSDependencyProvider.get_jsonld_converter)
LabelMapperDep = Depends(OMSDependencyProvider.get_label_mapper)
OutboxServiceDep = Depends(OMSDependencyProvider.get_outbox_service)
CommandStatusServiceDep = Depends(OMSDependencyProvider.get_command_status_service)


# Validation Dependencies (modernized)
def ValidatedDatabaseName(db_name: str = Path(..., description="데이터베이스 이름")) -> str:
    """데이터베이스 이름 검증 의존성 - Modernized version"""
    try:
        return validate_db_name(db_name)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"잘못된 데이터베이스 이름: {str(e)}"
        )


def ValidatedClassId(class_id: str = Path(..., description="클래스 ID")) -> str:
    """클래스 ID 검증 의존성 - Modernized version"""
    try:
        return validate_class_id(class_id)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"잘못된 클래스 ID: {str(e)}"
        )


# Combined validation for database existence check - Modernized version
async def ensure_database_exists(
    db_name: Annotated[str, ValidatedDatabaseName],
    terminus: AsyncTerminusService = Depends(OMSDependencyProvider.get_terminus_service)
) -> str:
    """데이터베이스 존재 확인 및 검증된 이름 반환 - Modernized version"""
    try:
        dbs = await terminus.list_databases()
        # 올바른 방식: 딕셔너리 리스트에서 name 필드 확인
        db_exists = any(db.get('name') == db_name for db in dbs if isinstance(db, dict))
        if not db_exists:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"데이터베이스 '{db_name}'이(가) 존재하지 않습니다"
            )
        return db_name
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"데이터베이스 확인 실패: {str(e)}"
        )


# Convenience dependency annotations for backward compatibility
get_terminus_service = OMSDependencyProvider.get_terminus_service
get_jsonld_converter = OMSDependencyProvider.get_jsonld_converter
get_label_mapper = OMSDependencyProvider.get_label_mapper
get_outbox_service = OMSDependencyProvider.get_outbox_service
get_redis_service = RedisServiceDep
get_command_status_service = OMSDependencyProvider.get_command_status_service
get_elasticsearch_service = ElasticsearchServiceDep


# Health check function for the modernized dependencies
async def check_oms_dependencies_health(
    container: ServiceContainer = Depends(get_container)
) -> dict:
    """
    Check health of all OMS dependencies
    
    This provides a way to verify that all dependencies are properly
    initialized and accessible through the modern container system.
    """
    health_status = {}
    
    try:
        # Check each service
        services_to_check = [
            ("terminus_service", AsyncTerminusService),
            ("jsonld_converter", JSONToJSONLDConverter),
            ("label_mapper", LabelMapper),
            ("elasticsearch_service", ElasticsearchService),
            ("redis_service", RedisService),
            ("command_status_service", CommandStatusService),
        ]
        
        for service_name, service_type in services_to_check:
            try:
                if container.has(service_type):
                    service = await container.get(service_type)
                    # Perform basic health check if available
                    if hasattr(service, 'health_check'):
                        is_healthy = await service.health_check()
                        health_status[service_name] = "healthy" if is_healthy else "unhealthy"
                    elif hasattr(service, 'check_connection'):
                        is_connected = await service.check_connection()
                        health_status[service_name] = "connected" if is_connected else "disconnected"
                    else:
                        health_status[service_name] = "available"
                else:
                    health_status[service_name] = "not_registered"
            except Exception as e:
                health_status[service_name] = f"error: {str(e)}"
        
        # Check optional services
        try:
            outbox_service = await OMSDependencyProvider.get_outbox_service(container)
            health_status["outbox_service"] = "available" if outbox_service else "not_available"
        except Exception as e:
            health_status["outbox_service"] = f"error: {str(e)}"
        
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