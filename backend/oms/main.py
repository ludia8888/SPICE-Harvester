"""
OMS (Ontology Management Service) 메인 애플리케이션
내부 ID 기반 핵심 온톨로지 관리 서비스
"""

from dotenv import load_dotenv

load_dotenv()  # Load .env file

import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Optional

import uvicorn
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# OMS 서비스 import
from oms.services.async_terminus import AsyncTerminusService
from shared.config.service_config import ServiceConfig
from shared.models.config import ConnectionConfig

# shared 모델 import
from shared.models.requests import ApiResponse

# shared imports
from shared.utils.jsonld import JSONToJSONLDConverter

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True
)
logger = logging.getLogger(__name__)

# 전역 서비스 인스턴스
terminus_service: Optional[AsyncTerminusService] = None
jsonld_converter: Optional[JSONToJSONLDConverter] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 생명주기 관리"""
    # 시작 시
    global terminus_service, jsonld_converter

    logger.info("OMS 서비스 초기화 중...")

    # 서비스 초기화
    connection_info = ConnectionConfig(
        server_url=ServiceConfig.get_terminus_url(),
        user=os.getenv("TERMINUS_USER", "admin"),
        account=os.getenv("TERMINUS_ACCOUNT", "admin"),
        key=os.getenv("TERMINUS_KEY", "admin123"),
    )

    terminus_service = AsyncTerminusService(connection_info)
    jsonld_converter = JSONToJSONLDConverter()

    # 의존성 설정
    from oms.dependencies import set_services

    set_services(terminus_service, jsonld_converter)

    try:
        # TerminusDB 연결 테스트
        await terminus_service.connect()
        logger.info("TerminusDB 연결 성공")
    except Exception as e:
        logger.error(f"TerminusDB 연결 실패: {e}")
        # 연결 실패해도 서비스는 시작 (나중에 재연결 시도)

    yield

    # 종료 시
    logger.info("OMS 서비스 종료 중...")
    if terminus_service:
        await terminus_service.disconnect()


# FastAPI 앱 생성
app = FastAPI(
    title="Ontology Management Service (OMS)",
    description="내부 ID 기반 핵심 온톨로지 관리 서비스",
    version="1.0.0",
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


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    logger.info(
        f'Request: {request.method} {request.url.path} - Response: {response.status_code} - Time: {process_time:.4f}s'
    )
    return response


# 에러 핸들러


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """FastAPI validation error를 400으로 변환"""
    logger.warning(f"Validation error: {exc}")

    # JSON parsing 오류인지 확인
    body_errors = [error for error in exc.errors() if error.get("type") == "json_invalid"]
    if body_errors:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "status": "error",
                "message": "잘못된 JSON 형식입니다",
                "detail": "Invalid JSON format",
            },
        )

    # 기타 validation 오류는 400으로 변환
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"status": "error", "message": "입력 데이터 검증 실패", "detail": str(exc)},
    )


@app.exception_handler(json.JSONDecodeError)
async def json_decode_error_handler(request: Request, exc: json.JSONDecodeError):
    """JSON decode 오류를 400으로 처리"""
    logger.warning(f"JSON decode error: {exc}")
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "status": "error",
            "message": "잘못된 JSON 형식입니다",
            "detail": f"JSON parsing failed: {str(exc)}",
        },
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"예상치 못한 오류 발생: {exc}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"status": "error", "message": "내부 서버 오류가 발생했습니다"},
    )


# API 엔드포인트
@app.get("/")
async def root():
    """루트 엔드포인트"""
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
    }


@app.get("/health")
async def health_check():
    """헬스 체크"""
    try:
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
        }

        return error_response.to_dict()


# CORS 디버그 엔드포인트 (개발 환경에서만 활성화)
if not ServiceConfig.is_production():

    @app.get("/debug/cors")
    async def debug_cors():
        """CORS 설정 디버그 정보"""
        return ServiceConfig.get_cors_debug_info()


# 라우터 등록
from oms.routers import branch, database, ontology, version

app.include_router(database.router, prefix="/api/v1", tags=["database"])
app.include_router(ontology.router, prefix="/api/v1", tags=["ontology"])
app.include_router(branch.router, prefix="/api/v1", tags=["branch"])
app.include_router(version.router, prefix="/api/v1", tags=["version"])

if __name__ == "__main__":
    # SSL 설정 가져오기
    ssl_config = ServiceConfig.get_ssl_config()

    # uvicorn 설정
    uvicorn_config = {
        "host": ServiceConfig.get_oms_host(),
        "port": ServiceConfig.get_oms_port(),
        "reload": True,
        "log_config": {
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
                "oms": {"handlers": ["default"], "level": "INFO", "propagate": False},
            },
        },
    }

    # SSL 설정이 있으면 추가
    if ssl_config:
        uvicorn_config.update(ssl_config)
        logger.info(f"🔐 HTTPS enabled for OMS on port {uvicorn_config['port']}")
    else:
        logger.info(f"🔓 HTTP enabled for OMS on port {uvicorn_config['port']}")

    uvicorn.run("main:app", **uvicorn_config)
