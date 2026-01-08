"""
Service Factory Module

Provides common FastAPI service creation utilities to eliminate code duplication
across BFF, OMS, and Funnel services.
"""

import logging
import time
from contextlib import asynccontextmanager
from typing import Any, Callable, Dict, List, Optional

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from starlette.responses import Response

from shared.config.service_config import ServiceConfig
from shared.models.requests import ApiResponse
from shared.i18n.middleware import install_i18n_middleware
from shared.middleware.rate_limiter import install_rate_limit_headers_middleware

logger = logging.getLogger(__name__)


class ServiceInfo:
    """Service configuration container"""
    
    def __init__(
        self,
        name: str,
        title: str,
        description: str,
        version: str = "1.0.0",
        port: int = 8000,
        host: str = "localhost",
        tags: Optional[List[Dict[str, str]]] = None
    ):
        self.name = name
        self.title = title
        self.description = description
        self.version = version
        self.port = port
        self.host = host
        self.tags = tags or []


def create_fastapi_service(
    service_info: ServiceInfo,
    custom_lifespan: Optional[Callable] = None,
    include_health_check: bool = True,
    include_logging_middleware: bool = True,
    custom_tags: Optional[List[Dict[str, str]]] = None
) -> FastAPI:
    """
    Create a standardized FastAPI application with common configurations.
    
    Args:
        service_info: Service configuration
        custom_lifespan: Optional custom lifespan function
        include_health_check: Whether to include default health check endpoint
        include_logging_middleware: Whether to include request logging middleware
        custom_tags: Custom OpenAPI tags
        
    Returns:
        Configured FastAPI application
    """
    
    # Use custom lifespan or create a simple default one
    if custom_lifespan:
        lifespan_func = custom_lifespan
    else:
        @asynccontextmanager
        async def default_lifespan(app: FastAPI):
            logger.info(f"{service_info.name} 서비스 시작")
            yield
            logger.info(f"{service_info.name} 서비스 종료")
        lifespan_func = default_lifespan
    
    # Merge default and custom tags
    openapi_tags = [
        {"name": "Health", "description": "Health check and service status"}
    ]
    if custom_tags:
        openapi_tags.extend(custom_tags)
    if service_info.tags:
        openapi_tags.extend(service_info.tags)
    
    # Create FastAPI app
    app = FastAPI(
        title=service_info.title,
        description=service_info.description,
        version=service_info.version,
        lifespan=lifespan_func,
        openapi_tags=openapi_tags
    )

    # Install i18n language negotiation + best-effort response localization
    install_i18n_middleware(app)
    install_rate_limit_headers_middleware(app)

    # Expose output language selection in OpenAPI for all services
    _install_openapi_language_contract(app)

    # Observability (real, no cargo-cult):
    # - request spans (if OpenTelemetry available)
    # - Prometheus /metrics endpoint (always, via prometheus-client)
    # - request metrics middleware
    _install_observability(app, service_info)
    
    # Configure CORS
    _configure_cors(app)
    
    # Add logging middleware
    if include_logging_middleware:
        _add_logging_middleware(app)
    
    # Add health check endpoint
    if include_health_check:
        _add_health_check(app, service_info)
    
    # Add CORS debug endpoint in non-production
    if ServiceConfig.is_debug_endpoints_enabled() and not ServiceConfig.is_production():
        _add_debug_endpoints(app)
    
    logger.info(f"✅ {service_info.name} FastAPI 앱 생성 완료")
    
    return app


def _install_openapi_language_contract(app: FastAPI) -> None:
    """
    Add `?lang=en|ko` and `Accept-Language` to OpenAPI so clients discover i18n support.
    """

    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema

        schema = get_openapi(
            title=app.title,
            version=app.version,
            description=app.description,
            routes=app.routes,
        )

        lang_param = {
            "name": "lang",
            "in": "query",
            "required": False,
            "schema": {"type": "string", "enum": ["en", "ko"]},
            "description": "Output language override for UI-facing fields (EN/KR). Overrides Accept-Language.",
        }
        accept_language_param = {
            "name": "Accept-Language",
            "in": "header",
            "required": False,
            "schema": {"type": "string"},
            "description": "Preferred output language for UI-facing fields (fallback when ?lang is not provided).",
            "example": "en-US,en;q=0.9,ko;q=0.8",
        }

        for _, methods in (schema.get("paths") or {}).items():
            for method, operation in (methods or {}).items():
                if method.lower() not in {"get", "post", "put", "patch", "delete"}:
                    continue
                params = operation.setdefault("parameters", [])
                has_lang = any(p.get("in") == "query" and p.get("name") == "lang" for p in params)
                has_accept = any(p.get("in") == "header" and p.get("name") == "Accept-Language" for p in params)
                if not has_lang:
                    params.append(lang_param)
                if not has_accept:
                    params.append(accept_language_param)

        app.openapi_schema = schema
        return app.openapi_schema

    app.openapi = custom_openapi


def _configure_cors(app: FastAPI) -> None:
    """Configure CORS middleware based on environment variables"""
    if ServiceConfig.is_cors_enabled():
        cors_config = ServiceConfig.get_cors_config()
        app.add_middleware(CORSMiddleware, **cors_config)
        logger.info(
            f"🌐 CORS enabled with origins: {cors_config['allow_origins'][:3]}..."
            if len(cors_config["allow_origins"]) > 3
            else f"🌐 CORS enabled with origins: {cors_config['allow_origins']}"
        )
    else:
        logger.info("🚫 CORS disabled")


def _add_logging_middleware(app: FastAPI) -> None:
    """Add request logging middleware"""
    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        logger.info(
            f'Request: {request.method} {request.url.path} - Response: {response.status_code} - Time: {process_time:.4f}s'
        )
        return response


def _add_health_check(app: FastAPI, service_info: ServiceInfo) -> None:
    """Add standardized health check endpoints"""
    
    @app.get("/", tags=["Health"])
    async def root():
        """루트 엔드포인트"""
        return {
            "service": service_info.name,
            "title": service_info.title,
            "version": service_info.version,
            "description": service_info.description,
            "status": "running"
        }
    
    @app.get("/health", tags=["Health"])
    async def health_check():
        """표준 헬스 체크 엔드포인트"""
        return ApiResponse.health_check(
            service_name=service_info.name,
            version=service_info.version,
            description=service_info.description
        ).to_dict()


def _add_debug_endpoints(app: FastAPI) -> None:
    """Add debug endpoints for development environment"""
    
    @app.get("/debug/cors")
    async def debug_cors():
        """CORS 설정 디버그 정보"""
        return ServiceConfig.get_cors_debug_info()


def _install_observability(app: FastAPI, service_info: ServiceInfo) -> None:
    """
    Install tracing + metrics in a way that is:
    - actually invoked in runtime paths
    - safe when optional deps are missing
    - observable (/metrics exists; tracing logs warn when exporters missing)
    """

    try:
        from shared.observability.tracing import get_tracing_service
        from shared.observability.metrics import (
            PROMETHEUS_CONTENT_TYPE_LATEST,
            RequestMetricsMiddleware,
            get_metrics_collector,
            prometheus_latest,
        )
    except Exception as e:  # pragma: no cover - env dependent
        logger.warning(f"Observability disabled (import failed): {e}")
        return

    # Tracing (best-effort; does not block app startup)
    try:
        tracing_service = get_tracing_service(service_info.name)
        tracing_service.instrument_fastapi(app)
        app.state.tracing_service = tracing_service
    except Exception as e:
        logger.warning(f"Tracing initialization failed (continuing without tracing): {e}")

    # Metrics (Prometheus)
    try:
        metrics_collector = get_metrics_collector(service_info.name)
        app.state.metrics_collector = metrics_collector

        # Request metrics middleware (call_next style)
        app.middleware("http")(RequestMetricsMiddleware(metrics_collector))
    except Exception as e:
        logger.warning(f"Metrics initialization failed (continuing without request metrics): {e}")

    @app.get("/metrics", include_in_schema=False)
    async def prometheus_metrics():
        return Response(content=prometheus_latest(), media_type=PROMETHEUS_CONTENT_TYPE_LATEST)


def create_uvicorn_config(service_info: ServiceInfo, reload: bool = True) -> Dict[str, Any]:
    """
    Create standardized uvicorn configuration.
    
    Args:
        service_info: Service configuration
        reload: Enable auto-reload for development
        
    Returns:
        Uvicorn configuration dictionary
    """
    
    # Get SSL configuration
    ssl_config = ServiceConfig.get_ssl_config()
    
    # Base uvicorn configuration
    config = {
        "host": service_info.host,
        "port": service_info.port,
        "reload": reload,
        "log_config": _get_logging_config(service_info.name)
    }
    
    # Add SSL config if available
    if ssl_config:
        config.update(ssl_config)
        logger.info(f"🔐 HTTPS enabled for {service_info.name} on port {service_info.port}")
    else:
        logger.info(f"🔓 HTTP enabled for {service_info.name} on port {service_info.port}")
    
    return config


def _get_logging_config(service_name: str) -> Dict[str, Any]:
    """Get standardized logging configuration for uvicorn"""
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "()": "uvicorn.logging.DefaultFormatter",
                "fmt": "%(levelprefix)s %(asctime)s - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "access": {
                "()": "uvicorn.logging.AccessFormatter",
                "fmt": '%(levelprefix)s %(asctime)s - %(client_addr)s - "%(request_line)s" %(status_code)s',
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "default": {
                "formatter": "default",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
            },
            "access": {
                "formatter": "access",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
            "uvicorn.error": {"level": "INFO"},
            "uvicorn.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
            service_name.lower(): {"handlers": ["default"], "level": "INFO", "propagate": False},
        },
    }


def run_service(
    app: FastAPI, 
    service_info: ServiceInfo, 
    app_module_path: str,
    reload: bool = True
) -> None:
    """
    Run the service with standardized uvicorn configuration.
    
    Args:
        app: FastAPI application instance
        service_info: Service configuration
        app_module_path: Module path for uvicorn (e.g., "bff.main:app")
        reload: Enable auto-reload for development
    """
    config = create_uvicorn_config(service_info, reload)
    uvicorn.run(app_module_path, **config)


# Predefined service configurations
BFF_SERVICE_INFO = ServiceInfo(
    name="BFF",
    title="BFF (Backend for Frontend) Service",
    description="사용자 친화적인 레이블 기반 온톨로지 관리 서비스",
    version="2.0.0",
    port=ServiceConfig.get_bff_port(),
    host=ServiceConfig.get_bff_host(),
    tags=[
        {"name": "Database Management", "description": "Databases/branches/classes/versions (BFF → OMS)"},
        {"name": "Ontology Management", "description": "Ontology CRUD + import pipeline (BFF → OMS/Funnel)"},
        {"name": "Query", "description": "Query builder + raw queries"},
        {"name": "Graph", "description": "Federated graph queries (multi-hop)"},
        {"name": "Projections (WIP)", "description": "🚧 Materialized view APIs (skeleton/fallback; do not use for FE yet)"},
        {"name": "Instance Management", "description": "Read-side instance retrieval (ES-first; Terminus fallback)"},
        {"name": "Async Instance Management", "description": "Write-side instance commands (async; idempotent)"},
        {"name": "Command Status", "description": "Async command status polling (command_id → status/result)"},
        {"name": "Label Mappings", "description": "Label mapping import/export/validate"},
        {"name": "Merge Conflict Resolution", "description": "Merge simulation + conflict resolution helpers"},
        {"name": "Lineage", "description": "Lineage graph/impact/metrics (read model)"},
        {"name": "Audit", "description": "Audit log query + chain verification"},
        {"name": "Data Connectors", "description": "External data connector helpers (Google Sheets, etc.)"},
        {"name": "AI", "description": "LLM-assisted query helpers"},
        {"name": "Background Tasks", "description": "Background task status/metrics/retry/cancel"},
        {"name": "Admin Operations", "description": "Operator-only recovery/maintenance endpoints (token required)"},
        {"name": "Monitoring", "description": "Service health/metrics/dependencies (operator-only)"},
        {"name": "Config Monitoring", "description": "Configuration drift/audit/validation (operator-only)"},
    ]
)

OMS_SERVICE_INFO = ServiceInfo(
    name="OMS",
    title="Ontology Management Service (OMS)",
    description="내부 ID 기반 핵심 온톨로지 관리 서비스",
    version="1.0.0",
    port=ServiceConfig.get_oms_port(),
    host=ServiceConfig.get_oms_host(),
    tags=[
        {"name": "Database", "description": "Database management operations"},
        {"name": "Ontology Management", "description": "Core ontology operations"},
        {"name": "Branch Management", "description": "Git-like branch operations"},
        {"name": "Version Control", "description": "Version control operations"},
    ]
)

FUNNEL_SERVICE_INFO = ServiceInfo(
    name="Funnel",
    title="Funnel Service",
    description="타입 추론 및 스키마 제안 전용 마이크로서비스",
    version="0.1.0",
    port=ServiceConfig.get_funnel_port(),
    host=ServiceConfig.get_funnel_host(),
    tags=[
        {"name": "Type Inference", "description": "Data type inference operations"},
        {"name": "Schema Suggestion", "description": "Schema generation operations"},
    ]
)

AGENT_SERVICE_INFO = ServiceInfo(
    name="Agent",
    title="Agent Service",
    description="LangGraph 기반 에이전트 실행 및 감사 이벤트 기록",
    version="0.1.0",
    port=ServiceConfig.get_agent_port(),
    host=ServiceConfig.get_agent_host(),
    tags=[
        {"name": "Agent", "description": "Agent runs, steps, and audit-traceable actions"},
    ],
)
