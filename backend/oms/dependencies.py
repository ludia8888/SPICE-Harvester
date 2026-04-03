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

import logging
from typing import Annotated, Optional
import asyncpg
from fastapi import status, Path, Depends

# Modern dependency injection imports
from shared.dependencies import get_container, ServiceContainer
from shared.dependencies.providers import (
    get_jsonld_converter as get_shared_jsonld_converter,
    get_label_mapper as get_shared_label_mapper,
    RedisServiceDep,
    ElasticsearchServiceDep
)
from shared.config.settings import get_settings
from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode, classified_http_exception
from shared.observability.request_context import get_correlation_id, get_request_id
from shared.services.core.runtime_status import (
    availability_surface,
    build_runtime_issue,
    probe_service_runtime_state,
)
from shared.utils.label_mapper import LabelMapper
from shared.utils.jsonld import JSONToJSONLDConverter
from shared.services.storage.elasticsearch_service import ElasticsearchService
from shared.services.storage.redis_service import RedisService, create_redis_service
from shared.services.core.command_status_service import CommandStatusService
from shared.services.registries.processed_event_registry import (
    MissingProcessedEventRegistrySchemaError,
    ProcessedEventRegistry,
)
from shared.services.registries.processed_event_registry_factory import create_processed_event_registry

# OMS specific imports
from oms.services.event_store import EventStore, event_store

# Import validation functions
from shared.security.input_sanitizer import validate_db_name, validate_class_id
from shared.security.database_access import inspect_database_access

SERVICE_NAME = "OMS"
logger = logging.getLogger(__name__)


def _log_optional_dependency_failure(name: str, exc: Exception, *, error: bool = False) -> None:
    if error:
        logger.error("%s unavailable: %s", name, exc, exc_info=True)
        return
    logger.warning("%s unavailable: %s", name, exc)


async def _health_status_for_service(service: object) -> str:
    return await probe_service_runtime_state(service)


class OMSDependencyProvider:
    """
    Modern dependency provider for OMS services
    
    This class replaces the global variables and setter/getter pattern
    with a container-based approach that's type-safe and test-friendly.
    """
    
    @staticmethod
    async def get_event_store(
        container: ServiceContainer = Depends(get_container)
    ) -> EventStore:
        """
        Get S3/MinIO Event Store - The Single Source of Truth.
        """
        # Event store is a global singleton
        return event_store
    
    @staticmethod
    async def get_command_status_service(
        container: ServiceContainer = Depends(get_container)
    ) -> Optional[CommandStatusService]:
        """
        Get command status service from container
        
        This replaces the global command_status_service variable and get_command_status_service() function.
        """
        if container.has(CommandStatusService):
            if not container.is_created(CommandStatusService):
                raise RuntimeError("CommandStatusService is registered without a created instance")
            return await container.get(CommandStatusService)

        container.ensure_singleton(RedisService, create_redis_service)
        try:
            redis_service = await container.get(RedisService)
        except RuntimeError as exc:
            _log_optional_dependency_failure("Redis service", exc)
            logger.warning("Redis service not available, command status tracking disabled")
            return None

        command_status_service = CommandStatusService(redis_service)
        try:
            container.register_instance(CommandStatusService, command_status_service)
        except ValueError:
            if container.has(CommandStatusService) and container.is_created(CommandStatusService):
                return await container.get(CommandStatusService)
            raise
        return command_status_service

    @staticmethod
    async def get_processed_event_registry(
        container: ServiceContainer = Depends(get_container),
    ) -> Optional[ProcessedEventRegistry]:
        if container.has(ProcessedEventRegistry):
            if not container.is_created(ProcessedEventRegistry):
                raise RuntimeError("ProcessedEventRegistry is registered without a created instance")
            return await container.get(ProcessedEventRegistry)

        try:
            registry = await create_processed_event_registry(validate=False)
        except (OSError, TimeoutError, asyncpg.PostgresError, MissingProcessedEventRegistrySchemaError) as exc:
            _log_optional_dependency_failure("ProcessedEventRegistry connection", exc)
            return None

        try:
            container.register_instance(ProcessedEventRegistry, registry)
        except ValueError:
            if container.has(ProcessedEventRegistry) and container.is_created(ProcessedEventRegistry):
                return await container.get(ProcessedEventRegistry)
            raise
        return registry


# Type-safe dependency annotations for cleaner injection
JSONLDConverterDep = Depends(get_shared_jsonld_converter)
LabelMapperDep = Depends(get_shared_label_mapper)
EventStoreDep = Depends(OMSDependencyProvider.get_event_store)
CommandStatusServiceDep = Depends(OMSDependencyProvider.get_command_status_service)
ProcessedEventRegistryDep = Depends(OMSDependencyProvider.get_processed_event_registry)


# Validation Dependencies (modernized)
def ValidatedDatabaseName(db_name: str = Path(..., description="데이터베이스 이름")) -> str:
    """데이터베이스 이름 검증 의존성 - Modernized version"""
    try:
        return validate_db_name(db_name)
    except Exception as e:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            f"잘못된 데이터베이스 이름: {str(e)}",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )


def ValidatedClassId(class_id: str = Path(..., description="클래스 ID")) -> str:
    """클래스 ID 검증 의존성 - Modernized version"""
    try:
        return validate_class_id(class_id)
    except Exception as e:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            f"잘못된 클래스 ID: {str(e)}",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )


# Combined validation for database existence check - Modernized version
async def database_exists_in_registry(
    *,
    db_name: str,
) -> bool:
    inspection = await inspect_database_access(db_name=db_name)
    if inspection.is_unavailable:
        raise classified_http_exception(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Database access registry unavailable",
            code=ErrorCode.UPSTREAM_UNAVAILABLE,
        )
    return inspection.is_configured


async def ensure_database_exists(
    db_name: Annotated[str, ValidatedDatabaseName],
) -> str:
    """데이터베이스 존재 확인 및 검증된 이름 반환 - Modernized version"""
    if await database_exists_in_registry(db_name=db_name):
        return db_name
    raise classified_http_exception(
        status.HTTP_404_NOT_FOUND,
        f"데이터베이스 '{db_name}'이(가) 존재하지 않습니다",
        code=ErrorCode.RESOURCE_NOT_FOUND,
    )


# Convenience dependency annotations for backward compatibility
get_jsonld_converter = get_shared_jsonld_converter
get_label_mapper = get_shared_label_mapper
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
    issues = []
    
    try:
        # Check each service
        services_to_check = [
            ("jsonld_converter", JSONToJSONLDConverter),
            ("label_mapper", LabelMapper),
            ("elasticsearch_service", ElasticsearchService),
            ("redis_service", RedisService),
            ("command_status_service", CommandStatusService),
            ("processed_event_registry", ProcessedEventRegistry),
        ]
        
        for service_name, service_type in services_to_check:
            try:
                if container.has(service_type):
                    service = await container.get(service_type)
                    health_status[service_name] = await _health_status_for_service(service)
                else:
                    health_status[service_name] = "not_registered"
                    issues.append(
                        build_runtime_issue(
                            component=service_name,
                            dependency=service_name,
                            message=f"{service_name} not registered",
                            state="degraded",
                            classification="internal",
                            affected_features=[f"oms.{service_name}"],
                            affects_readiness=False,
                        )
                    )
            except Exception as exc:
                logger.warning("OMS dependency health check failed for %s", service_name, exc_info=True)
                health_status[service_name] = f"error: {str(exc)}"
                issues.append(
                    build_runtime_issue(
                        component=service_name,
                        dependency=service_name,
                        message=f"{service_name} health check failed: {exc}",
                        state="degraded",
                        classification="internal",
                        affected_features=[f"oms.{service_name}"],
                        affects_readiness=False,
                    )
                )
        for service_name, service_status in health_status.items():
            if str(service_status) == "hard_down":
                issues.append(
                    build_runtime_issue(
                        component=service_name,
                        dependency=service_name,
                        message=f"{service_name} reported {service_status}",
                        state="degraded",
                        classification="unavailable",
                        affected_features=[f"oms.{service_name}"],
                        affects_readiness=False,
                    )
                )
        surface = availability_surface(
            service="oms-dependencies",
            container_ready=bool(container.is_initialized),
            runtime_status={"ready": True, "degraded": bool(issues), "issues": issues},
        )
        surface["services"] = health_status
        surface["container_initialized"] = bool(container.is_initialized)
        return surface
        
    except Exception as e:
        logger.warning("OMS dependency health check crashed", exc_info=True)
        issues = [
            build_runtime_issue(
                component="oms_dependencies",
                dependency="oms_dependencies",
                message=f"OMS dependency health check failed: {e}",
                state="hard_down",
                classification="internal",
                affected_features=["oms.dependencies"],
                affects_readiness=True,
            )
        ]
        surface = availability_surface(
            service="oms-dependencies",
            container_ready=False,
            runtime_status={"ready": False, "degraded": True, "issues": issues},
        )
        surface["services"] = health_status
        surface["container_initialized"] = False
        return surface
