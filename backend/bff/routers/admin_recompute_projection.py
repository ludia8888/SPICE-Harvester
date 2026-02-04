"""Admin projection recompute endpoints (BFF).

Rebuilds Elasticsearch projections by replaying immutable domain events.
Composed by `bff.routers.admin`.

This router is intentionally thin: business logic lives in
`bff.services.admin_recompute_projection_service` (Facade), and request/response
schemas live in `bff.schemas.admin_projection_requests`.
"""

from typing import Any, Dict

from fastapi import APIRouter, BackgroundTasks, Depends, Request, status

from bff.dependencies import get_elasticsearch_service
from bff.schemas.admin_projection_requests import RecomputeProjectionRequest, RecomputeProjectionResponse
from bff.services import admin_recompute_projection_service
from shared.dependencies.providers import AuditLogStoreDep, BackgroundTaskManagerDep, LineageStoreDep, RedisServiceDep
from shared.middleware.rate_limiter import RateLimitPresets, rate_limit
from shared.services.storage.elasticsearch_service import ElasticsearchService

router = APIRouter(tags=["Admin Operations"])


@router.post(
    "/recompute-projection",
    response_model=RecomputeProjectionResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
@rate_limit(**RateLimitPresets.STRICT)
async def recompute_projection(
    http_request: Request,
    request: RecomputeProjectionRequest,
    background_tasks: BackgroundTasks,
    *,
    task_manager: BackgroundTaskManagerDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    lineage_store: LineageStoreDep,
    elasticsearch_service: ElasticsearchService = Depends(get_elasticsearch_service),
) -> RecomputeProjectionResponse:
    return await admin_recompute_projection_service.start_recompute_projection(
        http_request=http_request,
        request=request,
        background_tasks=background_tasks,
        task_manager=task_manager,
        redis_service=redis_service,
        audit_store=audit_store,
        lineage_store=lineage_store,
        elasticsearch_service=elasticsearch_service,
    )


@router.get("/recompute-projection/{task_id}/result")
async def get_recompute_projection_result(
    task_id: str,
    *,
    task_manager: BackgroundTaskManagerDep,
    redis_service: RedisServiceDep,
) -> Dict[str, Any]:
    return await admin_recompute_projection_service.get_recompute_projection_result(
        task_id=task_id,
        task_manager=task_manager,
        redis_service=redis_service,
    )
