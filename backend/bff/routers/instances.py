"""
인스턴스 관련 API 라우터

온톨로지 클래스의 실제 인스턴스 데이터 조회.
라우터는 얇게 유지하고(Thin Router), 도메인 로직은 서비스 레이어로 위임합니다(Facade/Service Layer).
"""

import logging
from typing import Any, Dict, Optional
from shared.observability.tracing import trace_endpoint

from fastapi import APIRouter, Depends, HTTPException, Query, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.dependencies import get_elasticsearch_service
from bff.routers.registry_deps import get_dataset_registry
from bff.services.instances_service import (
    get_class_sample_values as get_class_sample_values_service,
)
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.storage.elasticsearch_service import ElasticsearchService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/databases/{db_name}", tags=["Instance Management"])


@router.get("/class/{class_id}/sample-values")
@trace_endpoint("bff.instances.get_class_sample_values")
async def get_class_sample_values(
    db_name: str,
    class_id: str,
    property_name: Optional[str] = None,
    base_branch: str = Query(default="main", description="Base branch (ES base index)"),
    limit: int = Query(default=200, le=500),
    elasticsearch_service: ElasticsearchService = Depends(get_elasticsearch_service),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> Dict[str, Any]:
    try:
        return await get_class_sample_values_service(
            db_name=db_name,
            class_id=class_id,
            property_name=property_name,
            base_branch=base_branch,
            limit=limit,
            elasticsearch_service=elasticsearch_service,
            dataset_registry=dataset_registry,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get sample values: %s", exc)
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"샘플 값 조회 실패: {str(exc)}",
            code=ErrorCode.INTERNAL_ERROR,
        ) from exc
