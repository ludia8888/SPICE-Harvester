"""Merge conflict resolution endpoints (BFF).

Thin router delegating business logic to `bff.services.merge_conflict_service`
(Facade pattern).
"""

from shared.observability.tracing import trace_endpoint

from typing import Any, Dict

from fastapi import APIRouter, Depends

from bff.dependencies import OMSClient, get_oms_client
from bff.services import merge_conflict_service
from shared.models.requests import ApiResponse, MergeRequest

router = APIRouter(prefix="/databases/{db_name}/merge", tags=["Merge Conflict Resolution"])


@router.post("/simulate", response_model=ApiResponse)
@trace_endpoint("bff.merge_conflict.simulate_merge")
async def simulate_merge(
    db_name: str,
    request: MergeRequest,
    oms_client: OMSClient = Depends(get_oms_client),
) -> ApiResponse:
    return await merge_conflict_service.simulate_merge(db_name=db_name, request=request, oms_client=oms_client)


@router.post("/resolve", response_model=ApiResponse)
@trace_endpoint("bff.merge_conflict.resolve_merge_conflicts")
async def resolve_merge_conflicts(
    db_name: str,
    request: Dict[str, Any],
    oms_client: OMSClient = Depends(get_oms_client),
) -> ApiResponse:
    return await merge_conflict_service.resolve_merge_conflicts(db_name=db_name, request=request, oms_client=oms_client)

