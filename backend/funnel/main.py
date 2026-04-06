"""Internal Funnel runtime app.

Funnel logic is retained as an in-process ASGI app only and is invoked via
`bff.services.funnel_client.FunnelClient` transport.

Provides structure analysis endpoints only (Data Island detection, multi-table
separation, orientation detection). Legacy type inference endpoints have been
removed (Palantir Foundry style: all columns default to xsd:string).
"""

from contextlib import asynccontextmanager
from typing import Any, Dict

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from funnel.routers.type_inference_router import router as structure_router
from funnel.services.structure_patch_store import close_patch_store, initialize_patch_store
from shared.config.settings import get_settings
from shared.models.responses import build_health_data, health_http_status
from shared.services.core.runtime_status import availability_surface, ensure_runtime_status, get_runtime_status, record_runtime_issue
from shared.services.storage.redis_service import create_redis_service
from shared.utils.app_logger import get_logger

# Rate limiting middleware
from shared.middleware.rate_limiter import RateLimiter

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle."""
    logger.info("Funnel Service starting")
    ensure_runtime_status(app)

    # Initialize Rate Limiter
    try:
        rate_limiter = RateLimiter()
        await rate_limiter.initialize()
        app.state.rate_limiter = rate_limiter
        logger.info("Rate limiter initialized")
    except Exception as e:
        logger.error(f"Failed to initialize rate limiter: {e}")
        record_runtime_issue(
            app,
            component="rate_limiter",
            dependency="rate_limiter",
            message=str(e),
            state="degraded",
            classification="unavailable",
            affected_features=("rate_limit_enforcement",),
            affects_readiness=False,
        )

    try:
        redis_service = create_redis_service(get_settings())
        await redis_service.connect()
        await initialize_patch_store(redis_service)
        app.state.structure_patch_store_ready = True
        logger.info("Structure patch store initialized")
    except Exception as e:
        app.state.structure_patch_store_ready = False
        app.state.structure_patch_store_error = str(e)
        logger.error(f"Failed to initialize structure patch store: {e}")
        record_runtime_issue(
            app,
            component="structure_patch_store",
            dependency="redis",
            message=f"Structure patch store unavailable: {e}",
            state="degraded",
            classification="unavailable",
            affected_features=("structure_patch_store", "funnel_structure_patch"),
            affects_readiness=False,
        )

    yield

    # Cleanup
    if hasattr(app.state, 'rate_limiter'):
        await app.state.rate_limiter.close()
    try:
        await close_patch_store()
    except Exception as e:
        logger.error(f"Failed to close structure patch store: {e}")

    logger.info("Funnel Service stopped")


# Internal ASGI app (not a standalone external service).
app = FastAPI(
    title="Internal Funnel Runtime",
    version="0.1.0",
    lifespan=lifespan,
)

# Internal-only route mount (no `/api/v1` legacy prefix).
app.include_router(structure_router, prefix="/internal")


@app.get("/")
async def root() -> Dict[str, Any]:
    """Root endpoint."""
    return {
        "service": "funnel",
        "version": "0.1.0",
        "status": "running",
        "description": "Sheet structure analysis internal runtime",
        "endpoints": {
            "health": "/health",
            "structure_analyze": "/internal/funnel/structure/analyze",
            "structure_analyze_excel": "/internal/funnel/structure/analyze/excel",
            "structure_analyze_google_sheets": "/internal/funnel/structure/analyze/google-sheets",
            "docs": "/docs",
        },
    }


@app.get("/health", response_model=None)
async def health_check(request: Request = None) -> Any:
    """Service health check."""
    surface = availability_surface(
        service="funnel",
        container_ready=True,
        runtime_status=get_runtime_status(request.app if request is not None else app),
    )
    payload = build_health_data(
        service_name="funnel",
        version="0.1.0",
        description="Sheet structure analysis service",
        availability=surface,
    )
    if surface["status"] == "ready":
        return payload
    return JSONResponse(
        status_code=health_http_status(surface["status"]),
        content=payload,
    )


if __name__ == "__main__":
    raise SystemExit("Standalone Funnel service runtime is removed; use BFF in-process runtime.")
