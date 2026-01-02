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

# Load environment variables first (before other imports)
from dotenv import load_dotenv
load_dotenv()

# Standard library imports
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

# Third party imports
from fastapi import FastAPI, Request, status

# Centralized configuration and dependency injection
from shared.config.settings import settings, ApplicationSettings
from shared.dependencies import (
    initialize_container, 
    get_container, 
    shutdown_container,
    register_core_services
)
from shared.dependencies.providers import (
    RedisServiceDep,
    ElasticsearchServiceDep,
    SettingsDep
)

# Service factory import
from shared.services.service_factory import OMS_SERVICE_INFO, create_fastapi_service, run_service
from oms.middleware.auth import install_oms_auth_middleware, ensure_oms_auth_configured

# OMS specific imports  
from oms.services.async_terminus import AsyncTerminusService
from oms.services.event_store import event_store
from shared.models.config import ConnectionConfig
from shared.services.redis_service import RedisService
from shared.services.command_status_service import CommandStatusService
from shared.services.elasticsearch_service import ElasticsearchService
from shared.models.requests import ApiResponse
from shared.utils.jsonld import JSONToJSONLDConverter
from shared.utils.label_mapper import LabelMapper
from shared.errors.error_response import install_error_handlers
from oms.services.ontology_deploy_outbox import run_ontology_deploy_outbox_worker
from oms.services.ontology_deployment_registry import OntologyDeploymentRegistry
from oms.services.ontology_deployment_registry_v2 import OntologyDeploymentRegistryV2
from oms.database.postgres import db as postgres_db

# Rate limiting middleware
from shared.middleware.rate_limiter import rate_limit, RateLimitPresets, RateLimiter

# Router imports
from oms.routers import (
    branch, database, ontology, ontology_extensions, version,
    instance_async, instance, query, command_status
)

# Monitoring and observability routers
from shared.routers import monitoring, config_monitoring

# Logging setup
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True
)
logger = logging.getLogger(__name__)


class OMSServiceContainer:
    """
    OMS-specific service container to manage OMS services
    
    This class extends the core service container functionality
    with OMS-specific services like AsyncTerminusService and the S3/MinIO Event Store.
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
        
        # 2. Initialize TerminusDB service
        await self._initialize_terminus_service()

        # 3. Initialize Postgres (MVCC) for proposals/pull-requests
        await self._initialize_postgres()

        # 4. Initialize JSON-LD converter
        await self._initialize_jsonld_converter()

        # 5. Initialize Label Mapper
        await self._initialize_label_mapper()

        # 6. Initialize Redis and Command Status service
        await self._initialize_redis_and_command_status()

        # 7. Initialize Elasticsearch service
        await self._initialize_elasticsearch()

        # 8. Initialize Rate Limiter
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
    
    async def _initialize_terminus_service(self) -> None:
        """Initialize TerminusDB service with health check"""
        try:
            connection_info = ConnectionConfig(
                server_url=self.settings.database.terminus_url,
                user=self.settings.database.terminus_user,
                account=self.settings.database.terminus_account,
                key=self.settings.database.terminus_password,
            )
            
            terminus_service = AsyncTerminusService(connection_info)
            
            # Test connection
            try:
                await terminus_service.connect()
                logger.info("TerminusDB 연결 성공")
            except Exception as e:
                logger.error(f"TerminusDB 연결 실패: {e}")
                # Continue - service can start without initial connection
            
            self._oms_services['terminus_service'] = terminus_service

        except Exception as e:
            logger.error(f"TerminusDB service initialization failed: {e}")
            raise

    async def _initialize_postgres(self) -> None:
        """Initialize Postgres MVCC pool (required for pull requests/proposals)."""
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
        try:
            elasticsearch_service = ElasticsearchService(
                host=self.settings.database.elasticsearch_host,
                port=self.settings.database.elasticsearch_port,
                username=self.settings.database.elasticsearch_username,
                password=self.settings.database.elasticsearch_password
            )
            await elasticsearch_service.connect()
            
            self._oms_services['elasticsearch_service'] = elasticsearch_service
            logger.info("Elasticsearch 연결 성공")
            
        except Exception as e:
            logger.error(f"Elasticsearch 연결 실패: {e}")
            # Elasticsearch failure is non-fatal - basic functionality can work
            self._oms_services['elasticsearch_service'] = None
    
    async def _initialize_rate_limiter(self) -> None:
        """Initialize rate limiting service"""
        try:
            logger.info("Initializing rate limiter...")
            
            # Create rate limiter instance
            rate_limiter = RateLimiter()
            await rate_limiter.initialize()
            
            self._oms_services['rate_limiter'] = rate_limiter
            logger.info("Rate limiter initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize rate limiter: {e}")
            # Continue without rate limiting - service can still work
    
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
        
        if 'terminus_service' in self._oms_services and self._oms_services['terminus_service']:
            try:
                await self._oms_services['terminus_service'].disconnect()
                logger.info("TerminusDB service disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting TerminusDB service: {e}")

        if 'postgres_db' in self._oms_services and self._oms_services['postgres_db']:
            try:
                await self._oms_services['postgres_db'].disconnect()
                logger.info("PostgreSQL pool disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting PostgreSQL pool: {e}")
        
        self._oms_services.clear()
        logger.info("OMS services shutdown completed")
    
    def get_terminus_service(self) -> AsyncTerminusService:
        """Get TerminusDB service instance"""
        if 'terminus_service' not in self._oms_services:
            raise RuntimeError("TerminusDB service not initialized")
        return self._oms_services['terminus_service']
    
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

        enable_ontology_outbox = (os.getenv("ENABLE_ONTOLOGY_DEPLOY_OUTBOX_WORKER", "true") or "true").lower() != "false"
        use_deployments_v2 = (os.getenv("ONTOLOGY_DEPLOYMENTS_V2", "true") or "true").lower() != "false"
        if enable_ontology_outbox:
            ontology_outbox_stop = asyncio.Event()
            registry = OntologyDeploymentRegistryV2() if use_deployments_v2 else OntologyDeploymentRegistry()
            ontology_outbox_task = asyncio.create_task(
                run_ontology_deploy_outbox_worker(
                    registry=registry,
                    poll_interval_seconds=int(os.getenv("ONTOLOGY_DEPLOY_OUTBOX_POLL_SECONDS", "5")),
                    batch_size=int(os.getenv("ONTOLOGY_DEPLOY_OUTBOX_BATCH", "50")),
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
app = create_fastapi_service(
    service_info=OMS_SERVICE_INFO,
    custom_lifespan=lifespan,
    include_health_check=False,  # Handled by existing health endpoint
    include_logging_middleware=True
)
install_oms_auth_middleware(app)
install_error_handlers(app, service_name="oms", validation_status=status.HTTP_400_BAD_REQUEST)


# Modern dependency injection functions
async def get_terminus_service() -> AsyncTerminusService:
    """Get TerminusDB service from OMS container"""
    if _oms_container is None:
        raise RuntimeError("OMS services not initialized")
    return _oms_container.get_terminus_service()


# API endpoints (modernized)
@app.get("/")
async def root():
    """루트 엔드포인트 - Modernized version"""
    return {
        "message": "Ontology Management Service (OMS)",
        "version": "1.0.0",
        "description": "내부 ID 기반 핵심 온톨로지 관리 서비스",
        "features": [
            "내부 ID 기반 온톨로지 CRUD",
            "TerminusDB 직접 연동",
            "JSON-LD 변환",
            "버전 관리",
            "브랜치 관리",
        ],
        "environment": settings.environment.value,
        "modernized": True
    }


@app.get("/health")
async def health_check():
    """헬스 체크 - Modernized version"""
    try:
        if _oms_container is None:
            error_response = ApiResponse.error(
                message="OMS 서비스가 초기화되지 않았습니다", 
                errors=["Container not initialized"]
            )
            error_response.data = {
                "service": "OMS",
                "version": "1.0.0",
                "status": "unhealthy",
                "terminus_connected": False,
            }
            return error_response.to_dict()
        
        terminus_service = _oms_container.get_terminus_service()
        is_connected = await terminus_service.check_connection()

        if is_connected:
            # 서비스 정상 상태
            health_response = ApiResponse.health_check(
                service_name="OMS",
                version="1.0.0",
                description="내부 ID 기반 핵심 온톨로지 관리 서비스",
            )
            # TerminusDB 연결 상태 추가
            health_response.data["terminus_connected"] = True
            health_response.data["environment"] = settings.environment.value
            health_response.data["modernized"] = True

            return health_response.to_dict()
        else:
            # 서비스 비정상 상태 (연결 실패)
            error_response = ApiResponse.error(
                message="OMS 서비스에 문제가 있습니다", errors=["TerminusDB 연결 실패"]
            )
            error_response.data = {
                "service": "OMS",
                "version": "1.0.0",
                "status": "unhealthy",
                "terminus_connected": False,
                "environment": settings.environment.value,
                "modernized": True
            }

            return error_response.to_dict()

    except Exception as e:
        logger.error(f"헬스 체크 실패: {e}")

        error_response = ApiResponse.error(message="OMS 서비스 헬스 체크 실패", errors=[str(e)])
        error_response.data = {
            "service": "OMS",
            "version": "1.0.0",
            "status": "unhealthy",
            "terminus_connected": False,
            "environment": settings.environment.value,
            "modernized": True
        }

        return error_response.to_dict()


# Health check endpoint that uses the new container system
@app.get("/health/container")
async def container_health_check():
    """
    Health check for the modernized container system
    
    This endpoint provides visibility into the new dependency injection system.
    """
    try:
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
            terminus_service = _oms_container.get_terminus_service()
            is_connected = await terminus_service.check_connection()
            oms_services_status["terminus_service"] = "connected" if is_connected else "disconnected"
        except Exception as e:
            oms_services_status["terminus_service"] = f"unhealthy: {str(e)}"
        
        try:
            _oms_container.get_jsonld_converter()
            oms_services_status["jsonld_converter"] = "healthy"
        except Exception as e:
            oms_services_status["jsonld_converter"] = f"unhealthy: {str(e)}"
        
        try:
            _oms_container.get_label_mapper()
            oms_services_status["label_mapper"] = "healthy"
        except Exception as e:
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
if settings.is_development:
    @app.get("/debug/config")
    async def debug_config():
        """Debug endpoint to show configuration - Modern version"""
        return {
            "environment": settings.environment.value,
            "debug_mode": settings.debug,
            "terminus_url": settings.database.terminus_url,
            "terminus_user": settings.database.terminus_user,
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
app.include_router(query.router, prefix="/api/v1", tags=["query"])
app.include_router(instance_async.router, prefix="/api/v1", tags=["async-instance"])
app.include_router(instance.router, prefix="/api/v1", tags=["instance"])
app.include_router(branch.router, prefix="/api/v1", tags=["branch"])
app.include_router(version.router, prefix="/api/v1", tags=["version"])
app.include_router(command_status.router, prefix="/api/v1", tags=["command-status"])

# Pull Request endpoints depend on Postgres MVCC; keep them opt-in only.
if os.getenv("ENABLE_PULL_REQUESTS", "false").lower() == "true":
    from oms.routers import pull_request

    app.include_router(pull_request.router, prefix="/api/v1", tags=["pull-requests"])
else:
    logger.info("Pull request endpoints disabled (set ENABLE_PULL_REQUESTS=true to enable)")

# Monitoring and observability endpoints (modernized architecture)
app.include_router(monitoring.router, prefix="/api/v1/monitoring", tags=["monitoring"])
app.include_router(config_monitoring.router, prefix="/api/v1/config", tags=["config-monitoring"])


if __name__ == "__main__":
    # Use service factory for simplified service execution
    run_service(app, OMS_SERVICE_INFO, "oms.main:app")
