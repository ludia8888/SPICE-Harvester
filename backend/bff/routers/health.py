"""
헬스체크 및 기본 라우터
시스템 상태 확인을 위한 엔드포인트
"""

import logging
from typing import Any

from shared.observability.tracing import trace_endpoint

from fastapi import APIRouter, Depends, Request

from bff.dependencies import get_oms_client
from bff.services.oms_client import OMSClient
from shared.models.responses import build_wrapped_health_response
from shared.services.core.runtime_status import availability_surface, get_runtime_status

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Health"])


def _bff_runtime_status_from_request(request: Request) -> dict[str, Any]:
    return get_runtime_status(request.app, attr_names=("bff_runtime_status",))


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
    oms_connected = False
    oms_error: str | None = None
    try:
        oms_connected = bool(await oms_client.check_health())
    except Exception as e:
        # OMS is optional in minimal/dev stacks; treat as degraded, not unhealthy.
        logger.warning("Health check: OMS unavailable (continuing): %s", e)
        oms_connected = False
        oms_error = str(e)

    runtime_status = _bff_runtime_status_from_request(request)
    surface = availability_surface(
        service="bff",
        container_ready=True,
        runtime_status=runtime_status,
    )
    if not oms_connected and surface["status"] != "hard_down":
        surface = {
            **surface,
            "status": "degraded",
            "degraded": True,
            "hard_down": False,
            "ready": True,
            "accepting_traffic": True,
        }

    message = "Service is healthy"
    if surface["status"] == "hard_down":
        message = "Service is running but startup is degraded"
    elif not oms_connected:
        message = "Service is running (OMS unavailable)"
    elif surface["status"] == "degraded":
        message = "Service is degraded"

    extra_data: dict[str, Any] = {
        "oms_connected": bool(oms_connected),
        "startup_issues": surface["issues"],
    }
    if oms_error:
        extra_data["oms_error"] = oms_error

    return build_wrapped_health_response(
        service_name="BFF",
        version="2.0.0",
        description="백엔드 포 프론트엔드 서비스",
        availability=surface,
        extra_data=extra_data,
        message=message,
        response_status="success",
    )
