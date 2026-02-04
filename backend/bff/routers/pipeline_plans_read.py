from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from bff.routers.pipeline_plans_deps import get_pipeline_plan_registry
from bff.routers.pipeline_plans_ops import _get_plan_record_or_404
from shared.models.responses import ApiResponse
from shared.services.registries.pipeline_plan_registry import PipelinePlanRegistry

router = APIRouter(tags=["Pipeline Plans"])


@router.get("/{plan_id}", response_model=ApiResponse)
async def get_plan(
    plan_id: str,
    request: Request,
    plan_registry: PipelinePlanRegistry = Depends(get_pipeline_plan_registry),
) -> ApiResponse:
    _resolved_plan_id, _tenant_id, record = await _get_plan_record_or_404(
        plan_id=plan_id,
        request=request,
        plan_registry=plan_registry,
    )

    return ApiResponse.success(message="Pipeline plan fetched", data={"plan": record.plan})

