"""Admin projection recompute endpoints (BFF).

Rebuilds Elasticsearch projections by replaying immutable domain events.
Composed by `bff.routers.admin`.

This router is intentionally thin: business logic lives in
`bff.services.admin_recompute_projection_service` (Facade), and request/response
schemas live in `bff.schemas.admin_projection_requests`.
"""

from typing import Any, Dict
from shared.observability.tracing import trace_endpoint

from fastapi import APIRouter, BackgroundTasks, Depends, Query, Request, status

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
@trace_endpoint("bff.admin.recompute_projection")
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
@trace_endpoint("bff.admin.get_recompute_projection_result")
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


@router.post(
    "/reindex-instances",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Reindex all instances for a database (dataset-primary rebuild)",
)
@rate_limit(**RateLimitPresets.STRICT)
@trace_endpoint("bff.admin.reindex_instances_endpoint")
async def reindex_instances_endpoint(
    db_name: str = Query(..., description="Database name"),
    branch: str = Query(default="main", description="Branch"),
    delete_index_first: bool = Query(default=False, description="Delete ES index before rebuild"),
    elasticsearch_service: ElasticsearchService = Depends(get_elasticsearch_service),
) -> Dict[str, Any]:
    """Rebuild the ES instances index by re-running all active objectify jobs.

    Unlike ontology projection rebuild (which replays S3 events), this endpoint
    re-executes objectify from source datasets — following the Palantir principle
    that dataset artifacts are the source of truth for instances.
    """
    # NOTE: objectify_registry, dataset_registry, and job_queue are obtained
    # from the router dependency overrides set by the composition root.
    # For now, return a descriptive stub that matches the service interface.
    return {
        "status": "endpoint_registered",
        "message": (
            "Reindex instances endpoint is registered. Wire objectify_registry, "
            "dataset_registry, and job_queue dependencies to enable full functionality."
        ),
        "db_name": db_name,
        "branch": branch,
    }
