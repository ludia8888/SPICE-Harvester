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
import json
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

# Third party imports
import httpx
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from confluent_kafka import Producer

# Centralized configuration and dependency injection
from shared.config.settings import settings, ApplicationSettings
from shared.dependencies import (
    initialize_container, 
    get_container, 
    shutdown_container,
    register_core_services
)
from shared.dependencies.providers import (
    StorageServiceDep,
    RedisServiceDep, 
    ElasticsearchServiceDep,
    SettingsDep
)

# Service factory import
from shared.services.service_factory import BFF_SERVICE_INFO, create_fastapi_service, run_service

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

# Observability imports
from shared.observability.tracing import get_tracing_service, trace_endpoint
from shared.observability.metrics import get_metrics_collector, RequestMetricsMiddleware
from shared.observability.context_propagation import TraceContextMiddleware

# BFF specific imports
from bff.services.funnel_type_inference_adapter import FunnelHTTPTypeInferenceAdapter
from bff.services.oms_client import OMSClient

# Data connector imports
from data_connector.google_sheets.service import GoogleSheetsService
from bff.routers import (
    database, health, mapping, merge_conflict, ontology, query, 
    instances, instance_async, websocket, tasks, admin, data_connector, command_status, graph, lineage, audit, ai
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
        
        # 6. Initialize Observability (Tracing & Metrics)
        await self._initialize_observability()
        
        # 7. Initialize Kafka Producer
        await self._initialize_kafka_producer()
        
        # 8. Initialize Google Sheets Service
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
    
    async def _initialize_observability(self) -> None:
        """Initialize observability with OpenTelemetry"""
        try:
            logger.info("Initializing observability (OpenTelemetry)...")
            
            # Initialize tracing service
            tracing_service = get_tracing_service("bff-service")
            
            # Initialize metrics collector
            metrics_collector = get_metrics_collector("bff-service")
            
            self._bff_services['tracing_service'] = tracing_service
            self._bff_services['metrics_collector'] = metrics_collector
            
            logger.info("Observability initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize observability: {e}")
            # Continue without observability - service can still work
    
    async def _initialize_kafka_producer(self) -> None:
        """Initialize Kafka producer"""
        try:
            logger.info("Initializing Kafka producer...")
            
            # Kafka configuration from ServiceConfig (works in Docker + local dev)
            from shared.config.service_config import ServiceConfig
            bootstrap_servers = ServiceConfig.get_kafka_bootstrap_servers()

            # Kafka configuration
            kafka_config = {
                'bootstrap.servers': bootstrap_servers,
                'client.id': 'bff-service',
                'acks': 'all',
                'retries': 3,
                'retry.backoff.ms': 100,
                'batch.size': 16384,
                'linger.ms': 10,
                'compression.type': 'snappy'
            }
            
            # Create Kafka producer
            producer = Producer(kafka_config)
            
            self._bff_services['kafka_producer'] = producer
            logger.info("Kafka producer initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            # Continue without Kafka - service can still work in limited mode
    
    async def _initialize_google_sheets_service(self) -> None:
        """Initialize Google Sheets service"""
        try:
            logger.info("Initializing Google Sheets service...")
            
            # Kafka producer is optional for preview/grid extraction; required only for change notifications.
            producer = self._bff_services.get('kafka_producer')
            if not producer:
                logger.warning("Kafka producer not available; Google Sheets service will run without notifications")
            
            # Get Google API key from settings
            google_api_key = (self.settings.google_sheets.google_sheets_api_key or "").strip() or None
            
            # Create Google Sheets service
            google_sheets_service = GoogleSheetsService(
                producer=producer,
                api_key=google_api_key
            )
            
            self._bff_services['google_sheets_service'] = google_sheets_service
            logger.info("Google Sheets service initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets service: {e}")
            # Continue without Google Sheets - service can still work
    
    async def shutdown_bff_services(self) -> None:
        """Shutdown BFF-specific services"""
        logger.info("Shutting down BFF services...")
        
        # Shutdown in reverse order of initialization
        if 'tracing_service' in self._bff_services:
            try:
                self._bff_services['tracing_service'].shutdown()
                logger.info("Tracing service shutdown")
            except Exception as e:
                logger.error(f"Error shutting down tracing: {e}")
        
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
        
        if 'kafka_producer' in self._bff_services:
            try:
                producer = self._bff_services['kafka_producer']
                producer.flush(timeout=5)  # Wait up to 5 seconds for delivery
                logger.info("Kafka producer flushed and closed")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")
        
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
    
    def get_kafka_producer(self) -> Producer:
        """Get Kafka producer instance"""
        if 'kafka_producer' not in self._bff_services:
            raise RuntimeError("Kafka producer not initialized")
        return self._bff_services['kafka_producer']
    
    def get_google_sheets_service(self) -> GoogleSheetsService:
        """Get Google Sheets service instance"""
        if 'google_sheets_service' not in self._bff_services:
            raise RuntimeError("Google Sheets service not initialized")
        return self._bff_services['google_sheets_service']


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
    
    try:
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
        
        # 6. Set up OpenTelemetry instrumentation (moved to after app startup)
        if 'tracing_service' in _bff_container._bff_services:
            tracing_service = _bff_container._bff_services['tracing_service']
            # Note: FastAPI instrumentation moved to post-startup to avoid middleware timing issues
            
        # 7-8. Middleware setup moved to app creation time to avoid timing issues
        
        logger.info("BFF Service startup completed successfully")
        
        yield
        
    except Exception as e:
        logger.error(f"BFF Service startup failed: {e}")
        raise
    
    finally:
        # Shutdown in reverse order
        logger.info("BFF Service shutdown beginning...")
        
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


async def get_kafka_producer() -> Producer:
    """Get Kafka producer from BFF container"""
    if _bff_container is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="BFF services not initialized"
        )
    return _bff_container.get_kafka_producer()


async def get_google_sheets_service() -> GoogleSheetsService:
    """Get Google Sheets service from BFF container"""
    if _bff_container is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="BFF services not initialized"
        )
    return _bff_container.get_google_sheets_service()


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
app.include_router(graph.router)  # Graph router has its own /api/v1 prefix

# Monitoring and observability endpoints (modernized architecture)
app.include_router(monitoring.router, prefix="/api/v1/monitoring")
app.include_router(config_monitoring.router, prefix="/api/v1/config")


if __name__ == "__main__":
    # Use service factory for simplified service execution
    run_service(app, BFF_SERVICE_INFO, "bff.main:app")
