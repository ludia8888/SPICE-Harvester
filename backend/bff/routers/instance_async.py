"""Async instance endpoints (BFF).

This router is intentionally thin: business logic lives in
`bff.services.instance_async_service` (Facade), and request schemas live in
`bff.schemas.instance_async_requests`.
"""

from shared.observability.tracing import trace_endpoint

from typing import Optional

from fastapi import APIRouter, Depends, Query, Request, status

from bff.dependencies import get_label_mapper, get_oms_client
from bff.schemas.instance_async_requests import BulkInstanceCreateRequest, InstanceCreateRequest, InstanceUpdateRequest
from bff.services import instance_async_service
from bff.services.oms_client import OMSClient
from shared.models.commands import CommandResult
from shared.utils.label_mapper import LabelMapper

router = APIRouter(prefix="/databases/{db_name}/instances", tags=["Async Instance Management"])


@router.post(
    "/{class_label}/create",
    response_model=CommandResult,
    status_code=status.HTTP_202_ACCEPTED,
)
@trace_endpoint("bff.instance_async.create_instance_async")
async def create_instance_async(
    db_name: str,
    class_label: str,
    request: InstanceCreateRequest,
    http_request: Request,
    branch: str = Query("main", description="Target branch (default: main)"),
    oms_client: OMSClient = Depends(get_oms_client),
    label_mapper: LabelMapper = Depends(get_label_mapper),
    user_id: Optional[str] = None,
):
    """
    인스턴스 생성 명령을 비동기로 처리 (Label 기반)
    
    사용자는 Label을 사용하여 데이터를 제공하며,
    BFF가 이를 ID로 변환하여 OMS에 전달합니다.
    """
    return await instance_async_service.create_instance_async(
        db_name=db_name,
        class_label=class_label,
        data=request.data,
        metadata=request.metadata,
        http_request=http_request,
        branch=branch,
        oms_client=oms_client,
        label_mapper=label_mapper,
        user_id=user_id,
    )


@router.put(
    "/{class_label}/{instance_id}/update",
    response_model=CommandResult,
    status_code=status.HTTP_202_ACCEPTED,
)
@trace_endpoint("bff.instance_async.update_instance_async")
async def update_instance_async(
    db_name: str,
    class_label: str,
    instance_id: str,
    request: InstanceUpdateRequest,
    http_request: Request,
    expected_seq: int = Query(..., ge=0, description="Expected current aggregate sequence (OCC)"),
    branch: str = Query("main", description="Target branch (default: main)"),
    oms_client: OMSClient = Depends(get_oms_client),
    label_mapper: LabelMapper = Depends(get_label_mapper),
    user_id: Optional[str] = None,
):
    """
    인스턴스 수정 명령을 비동기로 처리 (Label 기반)
    """
    return await instance_async_service.update_instance_async(
        db_name=db_name,
        class_label=class_label,
        instance_id=instance_id,
        data=request.data,
        metadata=request.metadata,
        http_request=http_request,
        expected_seq=expected_seq,
        branch=branch,
        oms_client=oms_client,
        label_mapper=label_mapper,
        user_id=user_id,
    )


@router.delete(
    "/{class_label}/{instance_id}/delete",
    response_model=CommandResult,
    status_code=status.HTTP_202_ACCEPTED,
)
@trace_endpoint("bff.instance_async.delete_instance_async")
async def delete_instance_async(
    db_name: str,
    class_label: str,
    instance_id: str,
    http_request: Request,
    branch: str = Query("main", description="Target branch (default: main)"),
    expected_seq: int = Query(..., ge=0, description="Expected current aggregate sequence (OCC)"),
    oms_client: OMSClient = Depends(get_oms_client),
    label_mapper: LabelMapper = Depends(get_label_mapper),
    user_id: Optional[str] = None,
):
    """
    인스턴스 삭제 명령을 비동기로 처리 (Label 기반)
    """
    return await instance_async_service.delete_instance_async(
        db_name=db_name,
        class_label=class_label,
        instance_id=instance_id,
        http_request=http_request,
        branch=branch,
        expected_seq=expected_seq,
        oms_client=oms_client,
        label_mapper=label_mapper,
        user_id=user_id,
    )


@router.post(
    "/{class_label}/bulk-create",
    response_model=CommandResult,
    status_code=status.HTTP_202_ACCEPTED,
)
@trace_endpoint("bff.instance_async.bulk_create_instances_async")
async def bulk_create_instances_async(
    db_name: str,
    class_label: str,
    request: BulkInstanceCreateRequest,
    http_request: Request,
    branch: str = Query("main", description="Target branch (default: main)"),
    oms_client: OMSClient = Depends(get_oms_client),
    label_mapper: LabelMapper = Depends(get_label_mapper),
    user_id: Optional[str] = None,
):
    """
    대량 인스턴스 생성 명령을 비동기로 처리 (Label 기반)
    """
    return await instance_async_service.bulk_create_instances_async(
        db_name=db_name,
        class_label=class_label,
        instances=request.instances,
        metadata=request.metadata,
        http_request=http_request,
        branch=branch,
        oms_client=oms_client,
        label_mapper=label_mapper,
        user_id=user_id,
    )


#
# NOTE: Command status is global and should be queried via:
#   GET /api/v1/commands/{command_id}/status
# This avoids misleading db-scoped status endpoints that cannot reliably prove
# the command belongs to the given db_name.
