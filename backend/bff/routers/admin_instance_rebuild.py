"""Admin instance index rebuild endpoints (BFF).

Triggers an ES instance index rebuild with Blue-Green alias swap.
Composed by ``bff.routers.admin``.
"""

from shared.observability.tracing import trace_endpoint

import logging
from typing import Any, Dict
from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, Depends, Query, status

from bff.dependencies import get_elasticsearch_service
from shared.dependencies.providers import BackgroundTaskManagerDep, RedisServiceDep
from shared.models.background_task import TaskStatus
from shared.services.core.instance_index_rebuild_service import (
    RebuildIndexRequest,
    rebuild_instance_index,
)
from shared.services.storage.elasticsearch_service import ElasticsearchService

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Admin Operations"])


@router.post(
    "/databases/{db_name}/rebuild-index",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Migrate ES index mappings (alias-swap rebuild)",
    description="Creates a new versioned ES index, copies existing docs with updated mappings, then performs atomic alias swap. Does NOT re-derive from source data.",
)
@trace_endpoint("bff.admin.rebuild_instance_index_endpoint")
async def rebuild_instance_index_endpoint(
    db_name: str,
    background_tasks: BackgroundTasks,
    *,
    branch: str = Query(default="master"),
    task_manager: BackgroundTaskManagerDep,
    redis_service: RedisServiceDep,
    elasticsearch_service: ElasticsearchService = Depends(get_elasticsearch_service),
) -> Dict[str, Any]:
    """Trigger an async instance index rebuild for a database.

    Creates a new versioned ES index, reindexes all existing documents with
    up-to-date mappings, then performs an atomic alias swap.
    """
    task_id = str(uuid4())

    await task_manager.set_task_status(
        redis_service,
        task_id=task_id,
        status=TaskStatus.RUNNING,
        metadata={"db_name": db_name, "branch": branch},
    )

    async def _run_rebuild() -> None:
        try:
            result = await rebuild_instance_index(
                request=RebuildIndexRequest(
                    db_name=db_name,
                    branch=branch,
                ),
                elasticsearch_service=elasticsearch_service,
                task_id=task_id,
            )

            final_status = TaskStatus.COMPLETED if result.status == "COMPLETED" else TaskStatus.FAILED
            await task_manager.set_task_status(
                redis_service,
                task_id=task_id,
                status=final_status,
                metadata={
                    "db_name": db_name,
                    "branch": branch,
                    "new_index": result.new_index,
                    "old_indices": result.old_indices,
                    "total_instances_indexed": result.total_instances_indexed,
                    "classes_processed": [
                        {"class_id": c.class_id, "instances_indexed": c.instances_indexed, "error": c.error}
                        for c in result.classes_processed
                    ],
                    "error": result.error,
                },
            )
        except Exception as exc:
            logger.error("Rebuild task %s failed: %s", task_id, exc)
            await task_manager.set_task_status(
                redis_service,
                task_id=task_id,
                status=TaskStatus.FAILED,
                metadata={"error": str(exc)},
            )

    background_tasks.add_task(_run_rebuild)

    return {
        "task_id": task_id,
        "status": "ACCEPTED",
        "message": f"Instance index rebuild started for database '{db_name}' (branch={branch})",
    }


@router.get(
    "/databases/{db_name}/rebuild-index/{task_id}/status",
    summary="Check rebuild index task status",
)
@trace_endpoint("bff.admin.get_rebuild_status")
async def get_rebuild_status(
    db_name: str,
    task_id: str,
    *,
    task_manager: BackgroundTaskManagerDep,
    redis_service: RedisServiceDep,
) -> Dict[str, Any]:
    """Check the status of a rebuild task."""
    task = await task_manager.get_task_status(redis_service, task_id=task_id)
    if not task:
        return {"task_id": task_id, "status": "NOT_FOUND"}
    return {
        "task_id": task_id,
        "status": task.get("status", "UNKNOWN"),
        "metadata": task.get("metadata", {}),
    }
