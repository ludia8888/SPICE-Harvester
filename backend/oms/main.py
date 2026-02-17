"""
OMS (Ontology Management Service) - Modernized Version

This is the modernized version of the OMS service that resolves anti-pattern 13:
- Uses centralized Pydantic settings instead of scattered os.getenv() calls
- Uses modern dependency injection container instead of global variables
- All imports are at module level instead of inside functions
- Service lifecycle is properly managed through the container

Key improvements:
1. ✅ All imports at module level
2. ✅ No global service variables
3. ✅ Centralized configuration via ApplicationSettings
4. ✅ Type-safe dependency injection
5. ✅ Proper service lifecycle management
6. ✅ Test-friendly architecture
"""

# Standard library imports
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Dict, Optional

# Third party imports
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

# Centralized configuration and dependency injection
from shared.config.settings import get_settings, ApplicationSettings
from shared.dependencies import (
    initialize_container, 
    shutdown_container,
    register_core_services
)

# Service factory import
from shared.services.core.service_factory import create_fastapi_service, get_oms_service_info, run_service
from shared.services.core.service_container_common import (
    initialize_elasticsearch_service,
    initialize_rate_limiter_service,
)
from oms.middleware.auth import install_oms_auth_middleware, ensure_oms_auth_configured

# OMS specific imports  
from oms.services.event_store import event_store
from shared.services.storage.redis_service import RedisService
from shared.services.core.command_status_service import CommandStatusService
from shared.services.storage.elasticsearch_service import ElasticsearchService
from shared.models.requests import ApiResponse
from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.utils.jsonld import JSONToJSONLDConverter
from shared.utils.label_mapper import LabelMapper
from oms.services.ontology_deploy_outbox import run_ontology_deploy_outbox_worker
from oms.services.ontology_deployment_registry import OntologyDeploymentRegistry
from oms.services.ontology_deployment_registry_v2 import OntologyDeploymentRegistryV2
from oms.database.postgres import db as postgres_db

# Router imports
from oms.routers import (
    database, ontology, ontology_extensions,
    instance_async, instance, query, command_status, action_async
)

# Monitoring and observability routers
from shared.routers import monitoring, config_monitoring
from shared.observability.logging import install_trace_context_filter
from shared.utils.app_logger import DEFAULT_LOG_FORMAT

# Logging setup
logging.basicConfig(
    level=logging.INFO, 
    format=DEFAULT_LOG_FORMAT,
    force=True
)
install_trace_context_filter()
logger = logging.getLogger(__name__)


def _resource_storage_backend() -> str:
    return "postgres"


class OMSServiceContainer:
    """
    OMS-specific service container to manage OMS services
    
    This class extends the core service container functionality with OMS-specific
    services for the Foundry/Postgres runtime profile.
    """
    
    def __init__(self, container, settings: ApplicationSettings):
        self.container = container
        self.settings = settings
        self._oms_services = {}
    
    async def initialize_oms_services(self) -> None:
        """Initialize OMS-specific services"""
        logger.info("Initializing OMS-specific services...")
        
        # 1. Initialize S3/MinIO Event Store (SSoT) - CRITICAL: This goes first!
        await self._initialize_event_store()
        
        # 2. Initialize Postgres (MVCC) for OMS relational runtime state
        await self._initialize_postgres()

        # 3. Initialize JSON-LD converter
        await self._initialize_jsonld_converter()

        # 4. Initialize Label Mapper
        await self._initialize_label_mapper()

        # 5. Initialize Redis and Command Status service
        await self._initialize_redis_and_command_status()

        # 6. Initialize Elasticsearch service
        await self._initialize_elasticsearch()

        # 7. Initialize Rate Limiter
        await self._initialize_rate_limiter()
        
        logger.info("OMS services initialized successfully")
    
    async def _initialize_event_store(self) -> None:
        """
        Initialize S3/MinIO Event Store - The Single Source of Truth.
        """
        try:
            logger.info("🔥 Initializing S3/MinIO Event Store (SSoT)...")
            
            # Connect to MinIO/S3
            await event_store.connect()
            
            self._oms_services['event_store'] = event_store
            logger.info("✅ S3/MinIO Event Store connected - This is the SSoT!")
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to S3/MinIO Event Store: {e}")
            raise RuntimeError(
                "Event Store is required (Event Sourcing SSoT). "
                "Start MinIO/S3 and verify credentials/endpoints."
            ) from e
    
    async def _initialize_postgres(self) -> None:
        """Initialize Postgres MVCC pool for OMS relational runtime state."""
        try:
            await postgres_db.connect()
            self._oms_services["postgres_db"] = postgres_db
            logger.info("PostgreSQL MVCC pool initialized for OMS")
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            raise
    
    async def _initialize_jsonld_converter(self) -> None:
        """Initialize JSON-LD converter"""
        jsonld_converter = JSONToJSONLDConverter()
        self._oms_services['jsonld_converter'] = jsonld_converter
        logger.info("JSON-LD converter initialized")
    
    async def _initialize_label_mapper(self) -> None:
        """Initialize label mapper"""
        label_mapper = LabelMapper()
        self._oms_services['label_mapper'] = label_mapper
        logger.info("Label mapper initialized")
    
    async def _initialize_redis_and_command_status(self) -> None:
        """Initialize Redis service and Command Status service"""
        # Temporarily skip Redis to allow system to work
        logger.warning("Redis initialization temporarily disabled to avoid recursion issue")
        self._oms_services['redis_service'] = None
        self._oms_services['command_status_service'] = None
        return
        
        # Original code commented out until Redis recursion is fixed
        # try:
        #     redis_service = RedisService(
        #         host=self.settings.database.redis_host,
        #         port=self.settings.database.redis_port,
        #         password=self.settings.database.redis_password
        #     )
        #     await redis_service.connect()
        #     command_status_service = CommandStatusService(redis_service)
        #     self._oms_services['redis_service'] = redis_service
        #     self._oms_services['command_status_service'] = command_status_service
        #     logger.info("Redis 연결 성공")
        # except Exception as e:
        #     logger.error(f"Redis 연결 실패: {e}")
        #     self._oms_services['redis_service'] = None
        #     self._oms_services['command_status_service'] = None
    
    async def _initialize_elasticsearch(self) -> None:
        """Initialize Elasticsearch service"""
        await initialize_elasticsearch_service(
            services=self._oms_services,
            settings=self.settings,
            logger=logger,
        )
    
    async def _initialize_rate_limiter(self) -> None:
        """Initialize rate limiting service"""
        await initialize_rate_limiter_service(services=self._oms_services, logger=logger)
    
    async def shutdown_oms_services(self) -> None:
        """Shutdown OMS-specific services"""
        logger.info("Shutting down OMS services...")
        
        # Shutdown in reverse order of initialization
        if 'event_store' in self._oms_services and self._oms_services['event_store']:
            try:
                # Event Store doesn't have explicit disconnect, but we can log
                logger.info("✅ Event Store references cleaned up")
            except Exception as e:
                logger.error(f"Error cleaning up Event Store: {e}")
        
        if 'rate_limiter' in self._oms_services:
            try:
                await self._oms_services['rate_limiter'].close()
                logger.info("Rate limiter closed")
            except Exception as e:
                logger.error(f"Error closing rate limiter: {e}")
        
        if 'elasticsearch_service' in self._oms_services and self._oms_services['elasticsearch_service']:
            try:
                await self._oms_services['elasticsearch_service'].disconnect()
                logger.info("Elasticsearch service disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting Elasticsearch service: {e}")
        
        if 'redis_service' in self._oms_services and self._oms_services['redis_service']:
            try:
                await self._oms_services['redis_service'].disconnect()
                logger.info("Redis service disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting Redis service: {e}")
        
        if 'postgres_db' in self._oms_services and self._oms_services['postgres_db']:
            try:
                await self._oms_services['postgres_db'].disconnect()
                logger.info("PostgreSQL pool disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting PostgreSQL pool: {e}")
        
        self._oms_services.clear()
        logger.info("OMS services shutdown completed")
    
    def get_jsonld_converter(self) -> JSONToJSONLDConverter:
        """Get JSON-LD converter instance"""
        if 'jsonld_converter' not in self._oms_services:
            raise RuntimeError("JSON-LD converter not initialized")
        return self._oms_services['jsonld_converter']
    
    def get_label_mapper(self) -> LabelMapper:
        """Get label mapper instance"""
        if 'label_mapper' not in self._oms_services:
            raise RuntimeError("Label mapper not initialized")
        return self._oms_services['label_mapper']
    
    def get_redis_service(self) -> Optional[RedisService]:
        """Get Redis service instance (can be None)"""
        return self._oms_services.get('redis_service')
    
    def get_command_status_service(self) -> Optional[CommandStatusService]:
        """Get command status service instance (can be None)"""
        return self._oms_services.get('command_status_service')
    
    def get_elasticsearch_service(self) -> Optional[ElasticsearchService]:
        """Get Elasticsearch service instance (can be None)"""
        return self._oms_services.get('elasticsearch_service')


# Global OMS service container (replaces global variables)
_oms_container: Optional[OMSServiceContainer] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Modern application lifecycle management
    
    This replaces the old lifespan function that had scattered imports
    and environment variable loading with centralized service management.
    """
    global _oms_container
    
    logger.info("OMS Service startup beginning...")
    
    ontology_outbox_task: Optional[asyncio.Task] = None
    ontology_outbox_stop: Optional[asyncio.Event] = None

    try:
        ensure_oms_auth_configured()

        settings = get_settings()

        # 1. Initialize the main dependency injection container
        container = await initialize_container(settings)
        
        # 2. Register core services
        register_core_services(container)
        
        # 3. Initialize OMS-specific container
        _oms_container = OMSServiceContainer(container, settings)
        await _oms_container.initialize_oms_services()
        
        # 4. Store container in app state for easy access
        app.state.container = container
        app.state.oms_container = _oms_container
        
        # 5. Store rate limiter in app state for decorators
        if 'rate_limiter' in _oms_container._oms_services:
            app.state.rate_limiter = _oms_container._oms_services['rate_limiter']

        # 6. Middleware setup is handled during app creation (service_factory)
        logger.info("OMS Service startup completed successfully")

        outbox_settings = settings.workers.ontology_deploy_outbox
        if outbox_settings.enabled:
            ontology_outbox_stop = asyncio.Event()
            registry = OntologyDeploymentRegistryV2() if outbox_settings.use_deployments_v2 else OntologyDeploymentRegistry()
            ontology_outbox_task = asyncio.create_task(
                run_ontology_deploy_outbox_worker(
                    registry=registry,
                    poll_interval_seconds=int(outbox_settings.poll_seconds),
                    batch_size=int(outbox_settings.batch_size),
                    stop_event=ontology_outbox_stop,
                )
            )
            app.state.ontology_deploy_outbox_task = ontology_outbox_task
            app.state.ontology_deploy_outbox_stop = ontology_outbox_stop

        yield
        
    except Exception as e:
        logger.error(f"OMS Service startup failed: {e}")
        raise
    
    finally:
        # Shutdown in reverse order
        logger.info("OMS Service shutdown beginning...")

        if ontology_outbox_stop is not None:
            ontology_outbox_stop.set()
        if ontology_outbox_task is not None:
            try:
                await ontology_outbox_task
            except Exception as exc:
                logger.warning("Ontology deploy outbox worker shutdown failed: %s", exc)
        
        if _oms_container:
            await _oms_container.shutdown_oms_services()
        
        await shutdown_container()
        
        logger.info("OMS Service shutdown completed")


# FastAPI app creation using service factory
service_info = get_oms_service_info()
app = create_fastapi_service(
    service_info=service_info,
    custom_lifespan=lifespan,
    include_health_check=False,  # Handled by existing health endpoint
    include_logging_middleware=True,
    validation_error_status=status.HTTP_400_BAD_REQUEST,
)
install_oms_auth_middleware(app)

# ── OMS domain exception → enterprise error envelope mapping ──
from oms.exceptions import (  # noqa: E402
    OmsBaseException,
    OntologyNotFoundError,
    DuplicateOntologyError,
    OntologyValidationError,
    DatabaseNotFoundError,
    DatabaseError as OmsDatabaseError,
    AtomicUpdateError,
    RelationshipError,
    CircularReferenceError,
)

_OMS_EXCEPTION_MAP: Dict = {
    OntologyNotFoundError: (status.HTTP_404_NOT_FOUND, ErrorCode.ONTOLOGY_NOT_FOUND),
    DuplicateOntologyError: (status.HTTP_409_CONFLICT, ErrorCode.ONTOLOGY_DUPLICATE),
    OntologyValidationError: (status.HTTP_422_UNPROCESSABLE_ENTITY, ErrorCode.ONTOLOGY_VALIDATION_FAILED),
    DatabaseNotFoundError: (status.HTTP_404_NOT_FOUND, ErrorCode.RESOURCE_NOT_FOUND),
    OmsDatabaseError: (status.HTTP_500_INTERNAL_SERVER_ERROR, ErrorCode.DB_ERROR),
    AtomicUpdateError: (status.HTTP_409_CONFLICT, ErrorCode.ONTOLOGY_ATOMIC_UPDATE_FAILED),
    CircularReferenceError: (status.HTTP_400_BAD_REQUEST, ErrorCode.ONTOLOGY_CIRCULAR_REFERENCE),
    RelationshipError: (status.HTTP_400_BAD_REQUEST, ErrorCode.ONTOLOGY_RELATIONSHIP_ERROR),
}

@app.exception_handler(OmsBaseException)
async def _oms_domain_exception_handler(request: Request, exc: OmsBaseException):
    """Map OMS domain exceptions to enterprise error envelope responses."""
    from shared.observability.request_context import get_request_id, get_correlation_id

    for exc_type, (http_status, error_code) in _OMS_EXCEPTION_MAP.items():
        if isinstance(exc, exc_type):
            break
    else:
        http_status = status.HTTP_500_INTERNAL_SERVER_ERROR
        error_code = ErrorCode.INTERNAL_ERROR

    envelope = build_error_envelope(
        service_name="oms",
        message=str(exc),
        detail=getattr(exc, "message", str(exc)),
        code=error_code,
        category=None,
        status_code=http_status,
        errors=None,
        context=getattr(exc, "details", None) or None,
        external_code=None,
        objectify_error=None,
        enterprise=None,
        origin={"service": "oms", "method": request.method, "path": str(request.url.path), "endpoint": None},
        request_id=get_request_id(),
        correlation_id=get_correlation_id(),
        trace_id=None,
    )
    return JSONResponse(status_code=http_status, content=envelope)


# API endpoints (modernized)
@app.get("/")
async def root():
    """루트 엔드포인트 - Modernized version"""
    settings = get_settings()
    backend = _resource_storage_backend()
    features = [
        "내부 ID 기반 온톨로지 CRUD",
        "Foundry 스타일 Object Storage v2 검색",
        "SearchJsonQueryV2 DSL 지원",
        "JSON-LD 변환",
        "레거시 브랜치/버전 API 제거",
    ]
    return {
        "message": "Ontology Management Service (OMS)",
        "version": "1.0.0",
        "description": "내부 ID 기반 핵심 온톨로지 관리 서비스",
        "features": features,
        "environment": settings.environment.value,
        "resource_storage_backend": backend,
        "modernized": True
    }


@app.get("/health")
async def health_check():
    """헬스 체크 - Modernized version"""
    try:
        settings = get_settings()
        if _oms_container is None:
            error_payload = build_error_envelope(
                service_name="oms",
                message="OMS 서비스가 초기화되지 않았습니다",
                detail="Container not initialized",
                code=ErrorCode.INTERNAL_ERROR,
                category=ErrorCategory.INTERNAL,
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                errors=["Container not initialized"],
                context={
                    "service": "OMS",
                    "version": "1.0.0",
                    "status": "unhealthy",
                    "environment": settings.environment.value,
                    "resource_storage_backend": _resource_storage_backend(),
                    "modernized": True,
                },
            )
            return JSONResponse(status_code=error_payload["http_status"], content=error_payload)

        # 서비스 정상 상태
        health_response = ApiResponse.health_check(
            service_name="OMS",
            version="1.0.0",
            description="내부 ID 기반 핵심 온톨로지 관리 서비스",
        )
        health_response.data["environment"] = settings.environment.value
        health_response.data["resource_storage_backend"] = _resource_storage_backend()
        health_response.data["modernized"] = True
        return health_response.to_dict()

    except Exception as e:
        logger.error(f"헬스 체크 실패: {e}")

        error_payload = build_error_envelope(
            service_name="oms",
            message="OMS 서비스 헬스 체크 실패",
            detail=str(e),
            code=ErrorCode.INTERNAL_ERROR,
            category=ErrorCategory.INTERNAL,
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            errors=[str(e)],
            context={
                "service": "OMS",
                "version": "1.0.0",
                "status": "unhealthy",
                "environment": settings.environment.value,
                "resource_storage_backend": _resource_storage_backend(),
                "modernized": True,
            },
        )
        return JSONResponse(status_code=error_payload["http_status"], content=error_payload)


# Health check endpoint that uses the new container system
@app.get("/health/container")
async def container_health_check():
    """
    Health check for the modernized container system
    
    This endpoint provides visibility into the new dependency injection system.
    """
    try:
        settings = get_settings()
        if _oms_container is None:
            return {
                "status": "unhealthy",
                "message": "OMS container not initialized",
                "container_status": None
            }
        
        # Get container health
        container_health = await _oms_container.container.health_check_all()
        
        # Check OMS-specific services
        oms_services_status = {}
        
        try:
            _oms_container.get_jsonld_converter()
            oms_services_status["jsonld_converter"] = "healthy"
        except Exception as e:
            logging.getLogger(__name__).warning("Exception fallback at oms/main.py:640", exc_info=True)
            oms_services_status["jsonld_converter"] = f"unhealthy: {str(e)}"
        
        try:
            _oms_container.get_label_mapper()
            oms_services_status["label_mapper"] = "healthy"
        except Exception as e:
            logging.getLogger(__name__).warning("Exception fallback at oms/main.py:646", exc_info=True)
            oms_services_status["label_mapper"] = f"unhealthy: {str(e)}"
        
        # Check optional services
        redis_service = _oms_container.get_redis_service()
        oms_services_status["redis_service"] = "available" if redis_service else "not_available"
        
        command_status_service = _oms_container.get_command_status_service()
        oms_services_status["command_status_service"] = "available" if command_status_service else "not_available"
        
        elasticsearch_service = _oms_container.get_elasticsearch_service()
        oms_services_status["elasticsearch_service"] = "available" if elasticsearch_service else "not_available"
        
        return {
            "status": "healthy",
            "message": "OMS container system operational",
            "container_health": container_health,
            "oms_services": oms_services_status,
            "resource_storage_backend": _resource_storage_backend(),
            "settings_environment": settings.environment.value,
            "settings_debug": settings.debug
        }
        
    except Exception as e:
        logger.error(f"Container health check failed: {e}")
        return {
            "status": "unhealthy", 
            "message": f"Health check error: {str(e)}",
            "container_health": None
        }


# Debug endpoint for development (using modern config system)
if get_settings().is_development:
    @app.get("/debug/config")
    async def debug_config():
        """Debug endpoint to show configuration - Modern version"""
        settings = get_settings()
        return {
            "environment": settings.environment.value,
            "debug_mode": settings.debug,
            "resource_storage_backend": _resource_storage_backend(),
            "database_settings": {
                "host": settings.database.host,
                "port": settings.database.port,
                "name": settings.database.name
            },
            "modernized": True
        }


# Router registration
app.include_router(database.router, prefix="/api/v1", tags=["database"])
app.include_router(ontology_extensions.router, prefix="/api/v1", tags=["ontology"])
app.include_router(ontology.router, prefix="/api/v1", tags=["ontology"])
app.include_router(query.foundry_router, prefix="/api", tags=["foundry-object-search-v2"])
app.include_router(instance_async.router, prefix="/api/v1", tags=["async-instance"])
app.include_router(instance.router, prefix="/api/v1", tags=["instance"])
app.include_router(action_async.foundry_router, prefix="/api", tags=["foundry-actions-v2"])
app.include_router(command_status.router, prefix="/api/v1", tags=["command-status"])
logger.info("Deprecated /branch and /version routers are permanently disabled (Foundry-style profile)")
logger.info("Legacy pull-request router is disabled (Foundry-aligned public surface)")

# Monitoring and observability endpoints (modernized architecture)
app.include_router(monitoring.router, prefix="/api/v1/monitoring", tags=["monitoring"])
app.include_router(config_monitoring.router, prefix="/api/v1/config", tags=["config-monitoring"])


if __name__ == "__main__":
    # Use service factory for simplified service execution
    run_service(app, service_info, "oms.main:app")
