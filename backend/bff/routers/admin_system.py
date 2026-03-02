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

router = APIRouter(tags=["Admin Operations"])


@router.get("/system-health", include_in_schema=False)
@trace_endpoint("bff.admin.get_system_health")
async def get_system_health(
    task_manager: BackgroundTaskManagerDep,
    redis_service: RedisServiceDep,
) -> Dict[str, Any]:
    task_metrics = await task_manager.get_task_metrics()
    redis_healthy = await redis_service.ping()

    issues: list[str] = []
    if not redis_healthy:
        issues.append("Redis connection unhealthy")
    if task_metrics.success_rate < 90:
        issues.append(f"Low task success rate: {task_metrics.success_rate:.1f}%")
    if task_metrics.processing_tasks > 20:
        issues.append(f"High number of processing tasks: {task_metrics.processing_tasks}")

    health_status = "healthy" if not issues else "degraded"
    return {
        "status": health_status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "issues": issues,
        "components": {
            "redis": "healthy" if redis_healthy else "unhealthy",
            "background_tasks": {
                "status": "healthy" if task_metrics.success_rate >= 90 else "degraded",
                "metrics": {
                    "total_tasks": task_metrics.total_tasks,
                    "active_tasks": task_metrics.active_tasks,
                    "success_rate": task_metrics.success_rate,
                    "average_duration_seconds": task_metrics.average_duration,
                },
            },
        },
    }

