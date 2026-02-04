"""
Pipeline Plans API (BFF).

This module is the composition root for pipeline plan endpoints.
Routers are composed via `include_router` (Composite pattern) to keep each
sub-router focused and maintainable.

It also re-exports selected helpers used by `bff.routers.agent_proxy`.
"""

from fastapi import APIRouter

from bff.routers.pipeline_deps import get_dataset_registry, get_pipeline_registry
from bff.routers.pipeline_plans_compile import router as _compile_router
from bff.routers.pipeline_plans_deps import get_pipeline_plan_registry
from bff.routers.pipeline_plans_ops import _resolve_actor, _resolve_tenant_id, _resolve_tenant_policy
from bff.routers.pipeline_plans_preview import preview_plan, router as _preview_router
from bff.routers.pipeline_plans_read import router as _read_router
from bff.schemas.pipeline_plans_requests import PipelinePlanPreviewRequest

router = APIRouter(prefix="/pipeline-plans", tags=["Pipeline Plans"])

# Static routes first so `/compile` isn't shadowed by `/{plan_id}`.
router.include_router(_compile_router)
router.include_router(_preview_router)
router.include_router(_read_router)

__all__ = [
    "PipelinePlanPreviewRequest",
    "get_dataset_registry",
    "get_pipeline_plan_registry",
    "get_pipeline_registry",
    "preview_plan",
    "router",
    "_resolve_actor",
    "_resolve_tenant_id",
    "_resolve_tenant_policy",
]

