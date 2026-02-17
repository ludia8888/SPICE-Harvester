"""Internal Funnel runtime app.

Funnel logic is retained as an in-process ASGI app only and is invoked via
`bff.services.funnel_client.FunnelClient` transport.

Legacy versioned internal routes are removed. Runtime endpoints are mounted
under `/internal/funnel/*` only.
"""

from contextlib import asynccontextmanager
from typing import Any, Dict

from fastapi import FastAPI

from funnel.routers.type_inference_router import router as type_inference_router
from shared.utils.app_logger import get_logger

# Rate limiting middleware
from shared.middleware.rate_limiter import RateLimiter

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 시작/종료 이벤트"""
    logger.info("🚀 Funnel Service 시작")
    
    # Initialize Rate Limiter
    try:
        rate_limiter = RateLimiter()
        await rate_limiter.initialize()
        app.state.rate_limiter = rate_limiter
        logger.info("Rate limiter initialized")
    except Exception as e:
        logger.error(f"Failed to initialize rate limiter: {e}")
    
    yield
    
    # Cleanup
    if hasattr(app.state, 'rate_limiter'):
        await app.state.rate_limiter.close()
    
    logger.info("🔄 Funnel Service 종료")


# Internal ASGI app (not a standalone external service).
app = FastAPI(
    title="Internal Funnel Runtime",
    version="0.1.0",
    lifespan=lifespan,
)

# Internal-only route mount (no `/api/v1` legacy prefix).
app.include_router(type_inference_router, prefix="/internal")


# 기본 엔드포인트들
@app.get("/")
async def root() -> Dict[str, Any]:
    """루트 엔드포인트"""
    return {
        "service": "funnel",
        "version": "0.1.0",
        "status": "running",
        "description": "타입 추론 및 스키마 제안 internal runtime",
        "endpoints": {
            "health": "/health",
            "analyze": "/internal/funnel/analyze",
            "suggest_schema": "/internal/funnel/suggest-schema",
            "preview_google_sheets": "/internal/funnel/preview/google-sheets",
            "docs": "/docs",
        },
    }


@app.get("/health")
async def health_check() -> Dict[str, Any]:
    """서비스 상태 확인"""
    from shared.models.requests import ApiResponse

    return ApiResponse.health_check(
        service_name="funnel", version="0.1.0", description="타입 추론 및 스키마 제안 서비스"
    ).to_dict()


if __name__ == "__main__":
    raise SystemExit("Standalone Funnel service runtime is removed; use BFF in-process runtime.")
