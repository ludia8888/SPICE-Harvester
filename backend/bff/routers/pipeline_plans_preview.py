"""Pipeline plan preview endpoints (BFF).

Thin router delegating to `bff.services.pipeline_plan_preview_service` (Facade).
Composed by `bff.routers.pipeline_plans`.
"""

from __future__ import annotations
from shared.observability.tracing import trace_endpoint

from fastapi import APIRouter, Depends, Request

from bff.routers.pipeline_deps import get_dataset_registry, get_pipeline_registry
from bff.routers.pipeline_plans_deps import get_pipeline_plan_registry
from bff.schemas.pipeline_plans_requests import (
    PipelinePlanEvaluateJoinsRequest,
    PipelinePlanInspectPreviewRequest,
    PipelinePlanPreviewRequest,
)
from bff.services import pipeline_plan_preview_service
from shared.models.responses import ApiResponse
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.pipeline_plan_registry import PipelinePlanRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry

router = APIRouter(tags=["Pipeline Plans"])


@router.post("/{plan_id}/preview", response_model=ApiResponse)
@trace_endpoint("bff.pipeline_plans.preview_plan")
async def preview_plan(
    plan_id: str,
    body: PipelinePlanPreviewRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    plan_registry: PipelinePlanRegistry = Depends(get_pipeline_plan_registry),
) -> ApiResponse:
    return await pipeline_plan_preview_service.preview_plan(
        plan_id=plan_id,
        body=body,
        request=request,
        dataset_registry=dataset_registry,
        pipeline_registry=pipeline_registry,
        plan_registry=plan_registry,
    )


@router.post("/{plan_id}/inspect-preview", response_model=ApiResponse)
@trace_endpoint("bff.pipeline_plans.inspect_plan_preview")
async def inspect_plan_preview(
    plan_id: str,
    body: PipelinePlanInspectPreviewRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    plan_registry: PipelinePlanRegistry = Depends(get_pipeline_plan_registry),
) -> ApiResponse:
    return await pipeline_plan_preview_service.inspect_plan_preview(
        plan_id=plan_id,
        body=body,
        request=request,
        dataset_registry=dataset_registry,
        pipeline_registry=pipeline_registry,
        plan_registry=plan_registry,
    )


@router.post("/{plan_id}/evaluate-joins", response_model=ApiResponse)
@trace_endpoint("bff.pipeline_plans.evaluate_joins")
async def evaluate_joins(
    plan_id: str,
    body: PipelinePlanEvaluateJoinsRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    plan_registry: PipelinePlanRegistry = Depends(get_pipeline_plan_registry),
) -> ApiResponse:
    return await pipeline_plan_preview_service.evaluate_joins(
        plan_id=plan_id,
        body=body,
        request=request,
        dataset_registry=dataset_registry,
        pipeline_registry=pipeline_registry,
        plan_registry=plan_registry,
    )
