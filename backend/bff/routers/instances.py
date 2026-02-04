"""
인스턴스 관련 API 라우터

온톨로지 클래스의 실제 인스턴스 데이터 조회.
라우터는 얇게 유지하고(Thin Router), 도메인 로직은 서비스 레이어로 위임합니다(Facade/Service Layer).
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from bff.dependencies import BFFDependencyProvider, get_elasticsearch_service, get_oms_client
from bff.routers.registry_deps import get_dataset_registry
from bff.services.instances_service import (
    get_class_sample_values as get_class_sample_values_service,
    get_instance_detail as get_instance_detail_service,
    list_class_instances as list_class_instances_service,
)
from bff.services.oms_client import OMSClient
from shared.dependencies import get_container
from shared.services.registries.action_log_registry import ActionLogRegistry
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.storage.elasticsearch_service import ElasticsearchService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/databases/{db_name}", tags=["Instance Management"])


async def _maybe_get_action_log_registry(
    class_id: str,
) -> Optional[ActionLogRegistry]:
    if str(class_id or "").strip().lower() != "actionlog":
        return None
    try:
        container = await get_container()
    except RuntimeError:
        return None
    return await BFFDependencyProvider.get_action_log_registry(container)


@router.get("/class/{class_id}/instances")
async def get_class_instances(
    db_name: str,
    class_id: str,
    http_request: Request,
    base_branch: str = Query(default="main", description="Base branch (Terminus/ES base index)"),
    overlay_branch: Optional[str] = Query(default=None, description="ES overlay branch (writeback)"),
    branch: Optional[str] = Query(default=None, description="Deprecated alias for base_branch"),
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0),
    search: Optional[str] = Query(default=None, description="Search query for filtering instances"),
    status_filter: Optional[List[str]] = Query(default=None, alias="status"),
    action_type_id: Optional[str] = Query(default=None),
    submitted_by: Optional[str] = Query(default=None),
    elasticsearch_service: ElasticsearchService = Depends(get_elasticsearch_service),
    oms_client: OMSClient = Depends(get_oms_client),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    action_logs: Optional[ActionLogRegistry] = Depends(_maybe_get_action_log_registry),
) -> Dict[str, Any]:
    try:
        return await list_class_instances_service(
            db_name=db_name,
            class_id=class_id,
            request_headers=http_request.headers,
            base_branch=base_branch,
            overlay_branch=overlay_branch,
            branch=branch,
            limit=limit,
            offset=offset,
            search=search,
            status_filter=status_filter,
            action_type_id=action_type_id,
            submitted_by=submitted_by,
            elasticsearch_service=elasticsearch_service,
            oms_client=oms_client,
            dataset_registry=dataset_registry,
            action_logs=action_logs,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get class instances: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"인스턴스 조회 실패: {str(exc)}",
        ) from exc


@router.get("/class/{class_id}/sample-values")
async def get_class_sample_values(
    db_name: str,
    class_id: str,
    property_name: Optional[str] = None,
    limit: int = Query(default=200, le=500),
    oms_client: OMSClient = Depends(get_oms_client),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> Dict[str, Any]:
    try:
        return await get_class_sample_values_service(
            db_name=db_name,
            class_id=class_id,
            property_name=property_name,
            limit=limit,
            oms_client=oms_client,
            dataset_registry=dataset_registry,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get sample values: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"샘플 값 조회 실패: {str(exc)}",
        ) from exc


@router.get("/class/{class_id}/instance/{instance_id}")
async def get_instance(
    db_name: str,
    class_id: str,
    instance_id: str,
    http_request: Request,
    base_branch: str = Query(default="main", description="Base branch (Terminus/ES base index)"),
    overlay_branch: Optional[str] = Query(default=None, description="ES overlay branch (writeback)"),
    branch: Optional[str] = Query(default=None, description="Deprecated alias for base_branch"),
    elasticsearch_service: ElasticsearchService = Depends(get_elasticsearch_service),
    oms_client: OMSClient = Depends(get_oms_client),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    action_logs: Optional[ActionLogRegistry] = Depends(_maybe_get_action_log_registry),
) -> Dict[str, Any]:
    try:
        return await get_instance_detail_service(
            db_name=db_name,
            class_id=class_id,
            instance_id=instance_id,
            request_headers=http_request.headers,
            base_branch=base_branch,
            overlay_branch=overlay_branch,
            branch=branch,
            elasticsearch_service=elasticsearch_service,
            oms_client=oms_client,
            dataset_registry=dataset_registry,
            action_logs=action_logs,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get instance %s: %s", instance_id, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"인스턴스 조회 실패: {str(exc)}",
        ) from exc
