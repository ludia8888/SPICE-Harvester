"""
🔥 THINK ULTRA! Funnel Service - 독립 마이크로서비스
타입 추론 및 스키마 제안 전용 서비스

Port: 8003
"""

from contextlib import asynccontextmanager
from typing import Any, Dict

from fastapi import FastAPI

# Shared service factory import
from shared.services.core.service_factory import create_fastapi_service, get_funnel_service_info, run_service

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


# FastAPI 앱 생성 - Service Factory 사용
service_info = get_funnel_service_info()
app = create_fastapi_service(
    service_info=service_info,
    custom_lifespan=lifespan,
    include_health_check=False,  # 기존 health check 유지
    include_logging_middleware=True
)

# 라우터 등록
app.include_router(type_inference_router, prefix="/api/v1")


# 기본 엔드포인트들
@app.get("/")
async def root() -> Dict[str, Any]:
    """루트 엔드포인트"""
    return {
        "service": "funnel",
        "version": "0.1.0",
        "status": "running",
        "description": "타입 추론 및 스키마 제안 전용 마이크로서비스",
        "endpoints": {
            "health": "/health",
            "analyze": "/api/v1/funnel/analyze",
            "suggest_schema": "/api/v1/funnel/suggest-schema",
            "preview_google_sheets": "/api/v1/funnel/preview/google-sheets",
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


# Note: CORS debug endpoint는 service_factory에서 자동 제공됨


if __name__ == "__main__":
    # Service Factory를 사용한 간소화된 서비스 실행
    run_service(app, service_info, "funnel.main:app")
