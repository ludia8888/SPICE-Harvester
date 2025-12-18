"""
헬스체크 및 기본 라우터
시스템 상태 확인을 위한 엔드포인트
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, status

from bff.dependencies import get_oms_client
from bff.services.oms_client import OMSClient

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Health"])


@router.get("/")
async def root():
    """
    루트 엔드포인트

    서비스 기본 정보를 반환합니다.
    """
    return {
        "service": "Ontology BFF Service",
        "version": "2.0.0",
        "description": "도메인 독립적인 온톨로지 관리 서비스",
    }


@router.get("/health")
async def health_check(oms_client: OMSClient = Depends(get_oms_client)):
    """
    헬스체크 엔드포인트

    서비스와 데이터베이스 연결 상태를 확인합니다.
    """
    from shared.models.requests import ApiResponse

    try:
        oms_connected = await oms_client.check_health()

        # 표준화된 헬스체크 응답 생성
        health_response = ApiResponse.health_check(
            service_name="BFF", version="2.0.0", description="백엔드 포 프론트엔드 서비스"
        )

        # OMS 연결 상태 추가
        health_response.data["oms_connected"] = bool(oms_connected)

        return health_response.to_dict()

    except Exception as e:
        logger.error(f"Health check failed: {e}")

        # 서비스는 동작하지만 OMS 연결에 문제가 있는 경우
        error_response = ApiResponse.error(message="서비스 연결에 문제가 있습니다", errors=[str(e)])
        error_response.data = {
            "service": "BFF",
            "version": "2.0.0",
            "oms_connected": False,
            "status": "unhealthy",
        }

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=error_response.to_dict()
        )
