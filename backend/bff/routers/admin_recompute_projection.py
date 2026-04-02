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

from bff.routers.objectify_job_queue_deps import get_objectify_job_queue
from bff.routers.registry_deps import get_dataset_registry, get_objectify_registry
from bff.dependencies import get_elasticsearch_service
from bff.schemas.admin_projection_requests import RecomputeProjectionRequest, RecomputeProjectionResponse
from bff.services import admin_recompute_projection_service, admin_reindex_instances_service
from shared.dependencies.providers import AuditLogStoreDep, BackgroundTaskManagerDep, LineageStoreDep, RedisServiceDep
from shared.middleware.rate_limiter import RateLimitPresets, rate_limit
from shared.security.input_sanitizer import validate_branch_name, validate_db_name
from shared.services.events.objectify_job_queue import ObjectifyJobQueue
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.storage.elasticsearch_service import ElasticsearchService

router = APIRouter(tags=["Admin Operations"])


@router.post(
    "/recompute-projection",
    response_model=RecomputeProjectionResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Replay S3 events to rebuild a projection",
    description="Replays LakeFS/S3 immutable events within a time range to reconstruct instances or ontologies projection.",
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


@router.get(
    "/recompute-projection/{task_id}/result",
    summary="Get recompute projection task result",
)
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
    description="Re-executes ALL objectify jobs from source datasets. This is the nuclear rebuild option.",
)
@rate_limit(**RateLimitPresets.STRICT)
@trace_endpoint("bff.admin.reindex_instances_endpoint")
async def reindex_instances_endpoint(
    http_request: Request,
    db_name: str = Query(..., description="Database name"),
    branch: str = Query(default="main", description="Branch"),
    delete_index_first: bool = Query(default=False, description="Delete ES index before rebuild"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
    job_queue: ObjectifyJobQueue = Depends(get_objectify_job_queue),
    elasticsearch_service: ElasticsearchService = Depends(get_elasticsearch_service),
) -> Dict[str, Any]:
    """Rebuild the ES instances index by re-running all active objectify jobs.

    Unlike ontology projection rebuild (which replays S3 events), this endpoint
    re-executes objectify from source datasets — following the Palantir principle
    that dataset artifacts are the source of truth for instances.
    """
    validated_db_name = validate_db_name(db_name)
    validated_branch = validate_branch_name(branch)

    return await admin_reindex_instances_service.reindex_all_instances(
        db_name=validated_db_name,
        branch=validated_branch,
        objectify_registry=objectify_registry,
        dataset_registry=dataset_registry,
        job_queue=job_queue,
        delete_index_first=bool(delete_index_first),
        elasticsearch_service=elasticsearch_service,
    )
