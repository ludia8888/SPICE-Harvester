"""
🔥 THINK ULTRA! Funnel Service - 독립 마이크로서비스
타입 추론 및 스키마 제안 전용 서비스

Port: 8003
"""

from dotenv import load_dotenv

load_dotenv()  # Load .env file

from contextlib import asynccontextmanager
from typing import Any, Dict

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from funnel.routers.type_inference_router import router as type_inference_router
from shared.config.service_config import ServiceConfig
from shared.utils.app_logger import get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 시작/종료 이벤트"""
    logger.info("🚀 Funnel Service 시작")
    yield
    logger.info("🔄 Funnel Service 종료")


# FastAPI 앱 생성
app = FastAPI(
    title="Funnel Service",
    description="타입 추론 및 스키마 제안 전용 마이크로서비스",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# CORS 설정 - 환경변수 기반 동적 설정
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


# CORS 디버그 엔드포인트 (개발 환경에서만 활성화)
if not ServiceConfig.is_production():

    @app.get("/debug/cors")
    async def debug_cors():
        """CORS 설정 디버그 정보"""
        return ServiceConfig.get_cors_debug_info()


if __name__ == "__main__":
    # SSL 설정 가져오기
    ssl_config = ServiceConfig.get_ssl_config()

    # uvicorn 설정
    uvicorn_config = {
        "host": ServiceConfig.get_funnel_host(),
        "port": ServiceConfig.get_funnel_port(),
        "reload": True,
        "log_level": "info",
    }

    # SSL 설정이 있으면 추가
    if ssl_config:
        uvicorn_config.update(ssl_config)
        logger.info(f"🔐 HTTPS enabled for Funnel on port {uvicorn_config['port']}")
    else:
        logger.info(f"🔓 HTTP enabled for Funnel on port {uvicorn_config['port']}")

    uvicorn.run("funnel.main:app", **uvicorn_config)
