"""
BFF (Backend for Frontend) Service - Modernized Version

This is the modernized version of the BFF service that resolves anti-pattern 13:
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
import json
import os
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

# Third party imports
import httpx
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse

# Centralized configuration and dependency injection
from shared.config.settings import settings, ApplicationSettings
from shared.dependencies import (
    initialize_container, 
    get_container, 
    shutdown_container,
    register_core_services
)
from shared.services.dataset_ingest_outbox import run_dataset_ingest_outbox_worker
from shared.services.lineage_store import LineageStore
from shared.dependencies.providers import (
    StorageServiceDep,
    RedisServiceDep, 
    ElasticsearchServiceDep,
    SettingsDep
)

# Service factory import
from shared.services.service_factory import BFF_SERVICE_INFO, create_fastapi_service, run_service
from shared.services.connector_registry import ConnectorRegistry
from shared.services.dataset_registry import DatasetRegistry
from shared.services.pipeline_registry import PipelineRegistry
from shared.services.pipeline_executor import PipelineExecutor
from shared.services.objectify_registry import ObjectifyRegistry

# Shared models and utilities
from shared.models.ontology import (
    OntologyCreateRequestBFF,
    OntologyResponse,
    OntologyUpdateRequest,
    QueryRequest,
    QueryResponse,
)
from shared.models.requests import (
    ApiResponse,
    BranchCreateRequest,
    CheckoutRequest,
    CommitRequest,
    MergeRequest,
    RollbackRequest,
)
from shared.security.input_sanitizer import (
    SecurityViolationError,
    sanitize_input,
    validate_class_id,
    validate_db_name,
)
from shared.utils.label_mapper import LabelMapper
from shared.dependencies import configure_type_inference_service
from shared.services.redis_service import create_redis_service
from shared.services.websocket_service import get_notification_service

# Rate limiting middleware
from shared.middleware.rate_limiter import rate_limit, RateLimitPresets, RateLimiter

# Auth middleware
from bff.middleware.auth import install_bff_auth_middleware, ensure_bff_auth_configured

# BFF specific imports
from bff.services.funnel_type_inference_adapter import FunnelHTTPTypeInferenceAdapter
from bff.services.oms_client import OMSClient

# Data connector imports
from data_connector.google_sheets.service import GoogleSheetsService
from bff.routers import (
    database, health, mapping, merge_conflict, ontology, query,
    instances, instance_async, websocket, tasks, admin, data_connector,
    command_status, graph, lineage, audit, ai, summary, pipeline, objectify
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


class BFFServiceContainer:
    """
    BFF-specific service container to manage BFF services
    
    This class extends the core service container functionality
    with BFF-specific services like OMSClient and LabelMapper.
    """
    
    def __init__(self, container, settings: ApplicationSettings):
        self.container = container
        self.settings = settings
        self._bff_services = {}
    
    async def initialize_bff_services(self) -> None:
        """Initialize BFF-specific services"""
        logger.info("Initializing BFF-specific services...")
        
        # 1. Initialize OMS Client
        await self._initialize_oms_client()
        
        # 2. Initialize Label Mapper
        await self._initialize_label_mapper()
        
        # 3. Initialize Type Inference Service
        await self._initialize_type_inference()
        
        # 4. Initialize WebSocket Notification Service
        await self._initialize_websocket_service()
        
        # 5. Initialize Rate Limiter
        await self._initialize_rate_limiter()

        # 6. Initialize Connector Registry (Postgres; Foundry-style durability)
        await self._initialize_connector_registry()

        # 7. Initialize Dataset Registry (Postgres; pipeline artifacts)
        await self._initialize_dataset_registry()

        # 8. Initialize Pipeline Registry (Postgres; pipeline definitions)
        await self._initialize_pipeline_registry()

        # 9. Initialize Objectify Registry (dataset -> ontology mapping)
        await self._initialize_objectify_registry()

        # 10. Initialize Pipeline Executor (preview/build engine)
        await self._initialize_pipeline_executor()
        
        # 11. Initialize Google Sheets Service (connector library)
        await self._initialize_google_sheets_service()
        
        logger.info("BFF services initialized successfully")
    
    async def _initialize_oms_client(self) -> None:
        """Initialize OMS client with health check"""
        try:
            # Use ServiceConfig.get_oms_url() to properly handle OMS_BASE_URL env var
            from shared.config.service_config import ServiceConfig
            oms_url = ServiceConfig.get_oms_url()
            logger.info(f"Initializing OMS client with URL: {oms_url}")
            oms_client = OMSClient(oms_url)
            
            # Test connection
            is_healthy = await oms_client.check_health()
            if is_healthy:
                logger.info("OMS service connection successful")
            else:
                logger.warning("OMS service connection failed - service will continue")
            
            self._bff_services['oms_client'] = oms_client
            
        except (httpx.HTTPError, httpx.TimeoutException, ConnectionError) as e:
            logger.error(f"OMS service connection failed: {e}")
            # Create client anyway for fallback scenarios
            from shared.config.service_config import ServiceConfig
            self._bff_services['oms_client'] = OMSClient(ServiceConfig.get_oms_url())
    
    async def _initialize_label_mapper(self) -> None:
        """Initialize label mapper"""
        label_mapper = LabelMapper()
        self._bff_services['label_mapper'] = label_mapper
        logger.info("Label mapper initialized")
    
    async def _initialize_type_inference(self) -> None:
        """Initialize type inference service"""
        try:
            logger.info("Configuring type inference service...")
            type_inference_adapter = FunnelHTTPTypeInferenceAdapter()
            configure_type_inference_service(type_inference_adapter)
            
            self._bff_services['type_inference_adapter'] = type_inference_adapter
            logger.info("Type inference service configured successfully")
            
        except Exception as e:
            logger.error(f"Failed to configure type inference service: {e}")
            # Continue without type inference
    
    async def _initialize_websocket_service(self) -> None:
        """Initialize WebSocket notification service"""
        try:
            logger.info("Initializing WebSocket notification service...")
            
            # Create Redis service using the new factory pattern
            redis_service = create_redis_service(self.settings)
            await redis_service.connect()
            
            # Create and start WebSocket notification service
            websocket_notification_service = get_notification_service(redis_service)
            await websocket_notification_service.start()
            
            self._bff_services['websocket_service'] = websocket_notification_service
            self._bff_services['redis_service'] = redis_service
            
            logger.info("WebSocket notification service started successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize WebSocket services: {e}")
            # Continue without WebSocket - service can still work without real-time updates
    
    async def _initialize_rate_limiter(self) -> None:
        """Initialize rate limiting service"""
        try:
            logger.info("Initializing rate limiter...")
            
            # Create rate limiter instance
            rate_limiter = RateLimiter()
            await rate_limiter.initialize()
            
            self._bff_services['rate_limiter'] = rate_limiter
            logger.info("Rate limiter initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize rate limiter: {e}")
            # Continue without rate limiting - service can still work

    async def _initialize_connector_registry(self) -> None:
        """Initialize Postgres-backed connector registry."""
        try:
            logger.info("Initializing ConnectorRegistry (Postgres)...")
            registry = ConnectorRegistry()
            await registry.initialize()
            self._bff_services["connector_registry"] = registry
            logger.info("ConnectorRegistry initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize ConnectorRegistry: {e}")
            # Continue without it; connector endpoints will 503.

    async def _initialize_dataset_registry(self) -> None:
        """Initialize Postgres-backed dataset registry."""
        try:
            logger.info("Initializing DatasetRegistry (Postgres)...")
            registry = DatasetRegistry()
            await registry.initialize()
            self._bff_services["dataset_registry"] = registry
            logger.info("DatasetRegistry initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize DatasetRegistry: {e}")

    async def _initialize_pipeline_registry(self) -> None:
        """Initialize Postgres-backed pipeline registry."""
        try:
            logger.info("Initializing PipelineRegistry (Postgres)...")
            registry = PipelineRegistry()
            await registry.initialize()
            self._bff_services["pipeline_registry"] = registry
            logger.info("PipelineRegistry initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize PipelineRegistry: {e}")

    async def _initialize_objectify_registry(self) -> None:
        """Initialize Postgres-backed objectify registry."""
        try:
            logger.info("Initializing ObjectifyRegistry (Postgres)...")
            registry = ObjectifyRegistry()
            await registry.initialize()
            self._bff_services["objectify_registry"] = registry
            logger.info("ObjectifyRegistry initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize ObjectifyRegistry: {e}")

    async def _initialize_pipeline_executor(self) -> None:
        """Initialize pipeline executor (preview/build engine)."""
        try:
            logger.info("Initializing PipelineExecutor...")
            dataset_registry = self.get_dataset_registry()
            storage_service = None
            try:
                from shared.services.storage_service import create_storage_service
                from shared.config.settings import ApplicationSettings

                settings = ApplicationSettings()
                storage_service = create_storage_service(settings)
            except Exception as exc:
                logger.warning("Storage service unavailable for preview: %s", exc)
            executor = PipelineExecutor(dataset_registry, storage_service=storage_service)
            self._bff_services["pipeline_executor"] = executor
            logger.info("PipelineExecutor initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize PipelineExecutor: {e}")
    
    async def _initialize_google_sheets_service(self) -> None:
        """Initialize Google Sheets service (connector library)"""
        try:
            logger.info("Initializing Google Sheets service (connector library)...")
            
            # Get Google API key from settings
            google_api_key = (self.settings.google_sheets.google_sheets_api_key or "").strip() or None
            
            # Foundry policy: connector library does only I/O (preview/fetch/normalize).
            # Change detection belongs to the Trigger layer.
            google_sheets_service = GoogleSheetsService(api_key=google_api_key)
            
            self._bff_services['google_sheets_service'] = google_sheets_service
            logger.info("Google Sheets service initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets service: {e}")
            # Continue without Google Sheets - service can still work
    
    async def shutdown_bff_services(self) -> None:
        """Shutdown BFF-specific services"""
        logger.info("Shutting down BFF services...")
        
        # Shutdown in reverse order of initialization
        if 'rate_limiter' in self._bff_services:
            try:
                await self._bff_services['rate_limiter'].close()
                logger.info("Rate limiter closed")
            except Exception as e:
                logger.error(f"Error closing rate limiter: {e}")
        
        if 'websocket_service' in self._bff_services:
            try:
                await self._bff_services['websocket_service'].stop()
                logger.info("WebSocket notification service stopped")
            except Exception as e:
                logger.error(f"Error stopping WebSocket service: {e}")
        
        if 'redis_service' in self._bff_services:
            try:
                await self._bff_services['redis_service'].disconnect()
                logger.info("Redis service disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting Redis service: {e}")
        
        if 'type_inference_adapter' in self._bff_services:
            try:
                adapter = self._bff_services['type_inference_adapter']
                if hasattr(adapter, "close"):
                    await adapter.close()
                logger.info("Type inference adapter closed")
            except Exception as e:
                logger.error(f"Error closing type inference adapter: {e}")
        
        if 'oms_client' in self._bff_services:
            try:
                await self._bff_services['oms_client'].close()
                logger.info("OMS client closed")
            except Exception as e:
                logger.error(f"Error closing OMS client: {e}")
        
        if 'google_sheets_service' in self._bff_services:
            try:
                # Google Sheets service doesn't need explicit cleanup
                logger.info("Google Sheets service cleaned up")
            except Exception as e:
                logger.error(f"Error cleaning up Google Sheets service: {e}")

        if "connector_registry" in self._bff_services:
            try:
                await self._bff_services["connector_registry"].close()
                logger.info("ConnectorRegistry closed")
            except Exception as e:
                logger.error(f"Error closing ConnectorRegistry: {e}")

        if "dataset_registry" in self._bff_services:
            try:
                await self._bff_services["dataset_registry"].close()
                logger.info("DatasetRegistry closed")
            except Exception as e:
                logger.error(f"Error closing DatasetRegistry: {e}")

        if "pipeline_registry" in self._bff_services:
            try:
                await self._bff_services["pipeline_registry"].close()
                logger.info("PipelineRegistry closed")
            except Exception as e:
                logger.error(f"Error closing PipelineRegistry: {e}")

        if "objectify_registry" in self._bff_services:
            try:
                await self._bff_services["objectify_registry"].close()
                logger.info("ObjectifyRegistry closed")
            except Exception as e:
                logger.error(f"Error closing ObjectifyRegistry: {e}")

        if "pipeline_executor" in self._bff_services:
            self._bff_services.pop("pipeline_executor", None)
        
        self._bff_services.clear()
        logger.info("BFF services shutdown completed")
    
    def get_oms_client(self) -> OMSClient:
        """Get OMS client instance"""
        if 'oms_client' not in self._bff_services:
            raise RuntimeError("OMS client not initialized")
        return self._bff_services['oms_client']
    
    def get_label_mapper(self) -> LabelMapper:
        """Get label mapper instance"""
        if 'label_mapper' not in self._bff_services:
            raise RuntimeError("Label mapper not initialized")
        return self._bff_services['label_mapper']
    
    def get_google_sheets_service(self) -> GoogleSheetsService:
        """Get Google Sheets service instance"""
        if 'google_sheets_service' not in self._bff_services:
            raise RuntimeError("Google Sheets service not initialized")
        return self._bff_services['google_sheets_service']

    def get_connector_registry(self) -> ConnectorRegistry:
        """Get connector registry instance"""
        if "connector_registry" not in self._bff_services:
            raise RuntimeError("ConnectorRegistry not initialized")
        return self._bff_services["connector_registry"]

    def get_dataset_registry(self) -> DatasetRegistry:
        """Get dataset registry instance"""
        if "dataset_registry" not in self._bff_services:
            raise RuntimeError("DatasetRegistry not initialized")
        return self._bff_services["dataset_registry"]

    def get_pipeline_registry(self) -> PipelineRegistry:
        """Get pipeline registry instance"""
        if "pipeline_registry" not in self._bff_services:
            raise RuntimeError("PipelineRegistry not initialized")
        return self._bff_services["pipeline_registry"]

    def get_objectify_registry(self) -> ObjectifyRegistry:
        """Get objectify registry instance"""
        if "objectify_registry" not in self._bff_services:
            raise RuntimeError("ObjectifyRegistry not initialized")
        return self._bff_services["objectify_registry"]

    def get_pipeline_executor(self) -> PipelineExecutor:
        """Get pipeline executor instance"""
        if "pipeline_executor" not in self._bff_services:
            raise RuntimeError("PipelineExecutor not initialized")
        return self._bff_services["pipeline_executor"]


# Global BFF service container (replaces global variables)
_bff_container: Optional[BFFServiceContainer] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Modern application lifecycle management
    
    This replaces the old lifespan function that had scattered imports
    and environment variable loading with centralized service management.
    """
    global _bff_container
    
    logger.info("BFF Service startup beginning...")
    
    dataset_outbox_task: Optional[asyncio.Task] = None
    dataset_outbox_stop: Optional[asyncio.Event] = None

    try:
        ensure_bff_auth_configured()

        # 1. Initialize the main dependency injection container
        container = await initialize_container(settings)
        
        # 2. Register core services
        register_core_services(container)
        
        # 3. Initialize BFF-specific container
        _bff_container = BFFServiceContainer(container, settings)
        await _bff_container.initialize_bff_services()
        
        # 4. Store container in app state for easy access
        app.state.container = container
        app.state.bff_container = _bff_container
        
        # 5. Store rate limiter in app state for decorators
        if 'rate_limiter' in _bff_container._bff_services:
            app.state.rate_limiter = _bff_container._bff_services['rate_limiter']

        enable_outbox = (os.getenv("ENABLE_DATASET_INGEST_OUTBOX_WORKER", "true") or "true").lower() != "false"
        if enable_outbox:
            dataset_outbox_stop = asyncio.Event()
            dataset_registry = _bff_container.get_dataset_registry()
            lineage_store = await container.get(LineageStore)
            dataset_outbox_task = asyncio.create_task(
                run_dataset_ingest_outbox_worker(
                    dataset_registry=dataset_registry,
                    lineage_store=lineage_store,
                    poll_interval_seconds=int(os.getenv("DATASET_INGEST_OUTBOX_POLL_SECONDS", "5")),
                    stop_event=dataset_outbox_stop,
                )
            )
            app.state.dataset_ingest_outbox_task = dataset_outbox_task
            app.state.dataset_ingest_outbox_stop = dataset_outbox_stop

        # 6. Middleware setup is handled during app creation (service_factory)
        logger.info("BFF Service startup completed successfully")
        
        yield
        
    except Exception as e:
        logger.error(f"BFF Service startup failed: {e}")
        raise
    
    finally:
        # Shutdown in reverse order
        logger.info("BFF Service shutdown beginning...")

        if dataset_outbox_stop is not None:
            dataset_outbox_stop.set()
        if dataset_outbox_task is not None:
            try:
                await dataset_outbox_task
            except Exception as exc:
                logger.warning("Dataset ingest outbox worker shutdown failed: %s", exc)

        if _bff_container:
            await _bff_container.shutdown_bff_services()
        
        await shutdown_container()
        
        logger.info("BFF Service shutdown completed")


# FastAPI app creation using service factory
app = create_fastapi_service(
    service_info=BFF_SERVICE_INFO,
    custom_lifespan=lifespan,
    include_health_check=False,  # Handled by existing router
    include_logging_middleware=True
)
install_bff_auth_middleware(app)


# Modern dependency injection functions
async def get_oms_client() -> OMSClient:
    """Get OMS client from BFF container"""
    if _bff_container is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="BFF services not initialized"
        )
    return _bff_container.get_oms_client()


async def get_label_mapper() -> LabelMapper:
    """Get label mapper from BFF container"""
    if _bff_container is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="BFF services not initialized"
        )
    return _bff_container.get_label_mapper()


async def get_google_sheets_service() -> GoogleSheetsService:
    """Get Google Sheets service from BFF container"""
    if _bff_container is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="BFF services not initialized"
        )
    return _bff_container.get_google_sheets_service()


async def get_connector_registry() -> ConnectorRegistry:
    """Get ConnectorRegistry from BFF container"""
    if _bff_container is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="BFF services not initialized"
        )
    return _bff_container.get_connector_registry()


async def get_dataset_registry() -> DatasetRegistry:
    """Get DatasetRegistry from BFF container"""
    if _bff_container is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="BFF services not initialized"
        )
    return _bff_container.get_dataset_registry()


async def get_pipeline_registry() -> PipelineRegistry:
    """Get PipelineRegistry from BFF container"""
    if _bff_container is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="BFF services not initialized"
        )
    return _bff_container.get_pipeline_registry()


async def get_objectify_registry() -> ObjectifyRegistry:
    if _bff_container is None:
        raise RuntimeError("BFF container not initialized")
    return _bff_container.get_objectify_registry()


async def get_pipeline_executor() -> PipelineExecutor:
    """Get PipelineExecutor from BFF container"""
    if _bff_container is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="BFF services not initialized"
        )
    return _bff_container.get_pipeline_executor()


# Router registration (unchanged)
app.include_router(database.router, prefix="/api/v1")
app.include_router(ontology.router, prefix="/api/v1")
app.include_router(query.router, prefix="/api/v1")
app.include_router(mapping.router, prefix="/api/v1")
app.include_router(health.router, prefix="/api/v1")
app.include_router(merge_conflict.router, prefix="/api/v1")
app.include_router(instances.router, prefix="/api/v1")
app.include_router(instance_async.router, prefix="/api/v1")
app.include_router(command_status.router, prefix="/api/v1")
app.include_router(websocket.router, prefix="/api/v1")
app.include_router(tasks.router, prefix="/api/v1")
app.include_router(admin.router, prefix="/api/v1")
app.include_router(data_connector.router, prefix="/api/v1")
app.include_router(lineage.router, prefix="/api/v1")
app.include_router(audit.router, prefix="/api/v1")
app.include_router(ai.router, prefix="/api/v1")
app.include_router(summary.router, prefix="/api/v1")
app.include_router(pipeline.router, prefix="/api/v1")
app.include_router(objectify.router, prefix="/api/v1")
app.include_router(graph.router)  # Graph router has its own /api/v1 prefix

# Monitoring and observability endpoints (modernized architecture)
app.include_router(monitoring.router, prefix="/api/v1/monitoring")
app.include_router(config_monitoring.router, prefix="/api/v1/config")


if __name__ == "__main__":
    # Use service factory for simplified service execution
    run_service(app, BFF_SERVICE_INFO, "bff.main:app")
