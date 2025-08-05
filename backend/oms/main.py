"""
OMS (Ontology Management Service) 메인 애플리케이션
내부 ID 기반 핵심 온톨로지 관리 서비스
"""

from dotenv import load_dotenv

load_dotenv()  # Load .env file

import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

# Shared service factory import
from shared.services.service_factory import OMS_SERVICE_INFO, create_fastapi_service, run_service

# OMS 서비스 import
from oms.services.async_terminus import AsyncTerminusService
from shared.models.config import ConnectionConfig
from shared.config.service_config import ServiceConfig
from oms.database.postgres import db as postgres_db
from oms.database.outbox import OutboxService
from shared.services import RedisService, create_redis_service, CommandStatusService

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
outbox_service: Optional[OutboxService] = None
redis_service: Optional[RedisService] = None
command_status_service: Optional[CommandStatusService] = None
elasticsearch_service = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 생명주기 관리"""
    # 시작 시
    global terminus_service, jsonld_converter, outbox_service, redis_service, command_status_service

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
    
    # PostgreSQL 연결 초기화
    try:
        await postgres_db.connect()
        outbox_service = OutboxService(postgres_db)
        logger.info("PostgreSQL 연결 성공")
    except Exception as e:
        logger.error(f"PostgreSQL 연결 실패: {e}")
        # PostgreSQL 연결 실패해도 서비스는 시작 (기본 기능은 동작)

    # Redis 연결 초기화
    try:
        redis_service = create_redis_service()
        await redis_service.connect()
        command_status_service = CommandStatusService(redis_service)
        logger.info("Redis 연결 성공")
    except Exception as e:
        logger.error(f"Redis 연결 실패: {e}")
        # Redis 연결 실패해도 서비스는 시작 (기본 기능은 동작)

    # Elasticsearch 연결 초기화
    try:
        from shared.services import ElasticsearchService
        elasticsearch_service = ElasticsearchService(
            hosts=[f"{os.getenv('ELASTICSEARCH_HOST', 'localhost')}:{os.getenv('ELASTICSEARCH_PORT', '9200')}"],
            username=os.getenv('ELASTICSEARCH_USERNAME', 'elastic'),
            password=os.getenv('ELASTICSEARCH_PASSWORD', 'elasticpass123')
        )
        await elasticsearch_service.connect()
        logger.info("Elasticsearch 연결 성공")
    except Exception as e:
        logger.error(f"Elasticsearch 연결 실패: {e}")
        # Elasticsearch 연결 실패해도 서비스는 시작 (기본 기능은 동작)

    # 의존성 설정
    from oms.dependencies import set_services

    set_services(terminus_service, jsonld_converter, outbox_service, redis_service, command_status_service, elasticsearch_service)

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
    if postgres_db:
        await postgres_db.disconnect()
    if redis_service:
        await redis_service.disconnect()
    if elasticsearch_service:
        await elasticsearch_service.disconnect()


# FastAPI 앱 생성 - Service Factory 사용
app = create_fastapi_service(
    service_info=OMS_SERVICE_INFO,
    custom_lifespan=lifespan,
    include_health_check=False,  # 기존 health check 유지
    include_logging_middleware=True
)


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


# Note: CORS debug endpoint는 service_factory에서 자동 제공됨


# 라우터 등록
from oms.routers import branch, database, ontology, version, ontology_async, ontology_sync, instance_async

app.include_router(database.router, prefix="/api/v1", tags=["database"])
app.include_router(ontology.router, prefix="/api/v1", tags=["ontology"])
app.include_router(ontology_async.router, prefix="/api/v1", tags=["async-ontology"])
app.include_router(ontology_sync.router, prefix="/api/v1", tags=["sync-ontology"])
app.include_router(instance_async.router, prefix="/api/v1", tags=["async-instance"])
app.include_router(branch.router, prefix="/api/v1", tags=["branch"])
app.include_router(version.router, prefix="/api/v1", tags=["version"])

if __name__ == "__main__":
    # Service Factory를 사용한 간소화된 서비스 실행
    run_service(app, OMS_SERVICE_INFO, "oms.main:app")
