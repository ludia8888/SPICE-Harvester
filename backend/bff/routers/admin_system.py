"""
Admin system endpoints (BFF).

Lightweight health endpoints for operational dashboards. Composed by
`bff.routers.admin`.
"""

from shared.observability.tracing import trace_endpoint

from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import APIRouter

from shared.dependencies.providers import BackgroundTaskManagerDep, RedisServiceDep
from shared.services.core.runtime_status import availability_surface, build_runtime_issue

router = APIRouter(tags=["Admin Operations"])


@router.get("/system-health", include_in_schema=False)
@trace_endpoint("bff.admin.get_system_health")
async def get_system_health(
    task_manager: BackgroundTaskManagerDep,
    redis_service: RedisServiceDep,
) -> Dict[str, Any]:
    task_metrics = await task_manager.get_task_metrics()
    redis_healthy = await redis_service.ping()
    task_manager_degraded = bool(task_metrics.success_rate < 90 or task_metrics.processing_tasks > 20)

    issues: list[dict[str, Any]] = []
    if not redis_healthy:
        issues.append(
            build_runtime_issue(
                component="admin_system",
                dependency="redis",
                message="Redis connection unhealthy",
                state="hard_down",
                classification="unavailable",
                affected_features=("admin.system_health", "background_tasks"),
                affects_readiness=True,
            )
        )
    if task_metrics.success_rate < 90:
        issues.append(
            build_runtime_issue(
                component="admin_system",
                dependency="background_task_manager",
                message=f"Low task success rate: {task_metrics.success_rate:.1f}%",
                state="degraded",
                classification="retryable",
                affected_features=("admin.system_health", "background_tasks"),
                affects_readiness=False,
            )
        )
    if task_metrics.processing_tasks > 20:
        issues.append(
            build_runtime_issue(
                component="admin_system",
                dependency="background_task_manager",
                message=f"High number of processing tasks: {task_metrics.processing_tasks}",
                state="degraded",
                classification="retryable",
                affected_features=("admin.system_health", "background_tasks"),
                affects_readiness=False,
            )
        )

    surface = availability_surface(
        service="bff.admin_system",
        container_ready=True,
        runtime_status={"ready": True, "degraded": bool(issues), "issues": issues},
        dependency_status_overrides={
            "redis": "ready" if redis_healthy else "hard_down",
            "background_task_manager": "degraded" if task_manager_degraded else "ready",
        },
        status_reason_override=(
            "Admin system health within thresholds"
            if not issues
            else "Admin system health thresholds exceeded"
        ),
        message=(
            "Admin system health within thresholds"
            if not issues
            else "Admin system health thresholds exceeded"
        ),
    )
    return {
        **surface,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "components": {
            "redis": {
                "status": "ready" if redis_healthy else "hard_down",
                "message": "Redis connection healthy" if redis_healthy else "Redis connection unhealthy",
            },
            "background_tasks": {
                "status": "ready" if not task_manager_degraded else "degraded",
                "metrics": {
                    "total_tasks": task_metrics.total_tasks,
                    "active_tasks": task_metrics.active_tasks,
                    "success_rate": task_metrics.success_rate,
                    "average_duration_seconds": task_metrics.average_duration,
                },
            },
        },
    }
