"""
헬스체크 및 기본 라우터
시스템 상태 확인을 위한 엔드포인트
"""

import logging
from datetime import datetime, timezone
from typing import Any

from shared.observability.tracing import trace_endpoint

from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import JSONResponse

from bff.dependencies import get_oms_client
from bff.services.oms_client import OMSClient

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Health"])


def _bff_runtime_status_from_request(request: Request) -> dict[str, Any]:
    runtime_status = getattr(request.app.state, "bff_runtime_status", None)
    if not isinstance(runtime_status, dict):
        return {
            "ready": True,
            "degraded": False,
            "issues": [],
            "background_tasks": {},
        }
    return {
        "ready": bool(runtime_status.get("ready", True)),
        "degraded": bool(runtime_status.get("degraded", False)),
        "issues": list(runtime_status.get("issues") or []),
        "background_tasks": dict(runtime_status.get("background_tasks") or {}),
    }


@router.get("/", include_in_schema=False)
@trace_endpoint("bff.health.root")
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


@router.get("/health", include_in_schema=False)
@trace_endpoint("bff.health.health_check")
async def health_check(request: Request, oms_client: OMSClient = Depends(get_oms_client)):
    """
    헬스체크 엔드포인트

    서비스와 데이터베이스 연결 상태를 확인합니다.
    """
    from shared.models.requests import ApiResponse

    oms_connected = False
    oms_error: str | None = None
    try:
        oms_connected = bool(await oms_client.check_health())
    except Exception as e:
        # OMS is optional in minimal/dev stacks; treat as degraded, not unhealthy.
        logger.warning("Health check: OMS unavailable (continuing): %s", e)
        oms_connected = False
        oms_error = str(e)

    # 표준화된 헬스체크 응답 생성
    health_response = ApiResponse.health_check(
        service_name="BFF", version="2.0.0", description="백엔드 포 프론트엔드 서비스"
    )

    if health_response.data is None:
        health_response.data = {}

    runtime_status = _bff_runtime_status_from_request(request)

    # OMS 연결 상태 추가 (degraded but still healthy for core paths)
    health_response.data["oms_connected"] = bool(oms_connected)
    health_response.data["ready"] = runtime_status["ready"]
    health_response.data["accepting_traffic"] = runtime_status["ready"]
    health_response.data["startup_issues"] = runtime_status["issues"]
    health_response.data["background_tasks"] = runtime_status["background_tasks"]
    if runtime_status["degraded"]:
        health_response.data["startup_status"] = "degraded"
    if not oms_connected or runtime_status["degraded"] or not runtime_status["ready"]:
        health_response.data["status"] = "degraded"
        if oms_error:
            health_response.data["oms_error"] = oms_error
        if not runtime_status["ready"]:
            health_response.message = "Service is running but startup is degraded"
        elif not oms_connected:
            health_response.message = "Service is running (OMS unavailable)"

    return health_response.to_dict()

