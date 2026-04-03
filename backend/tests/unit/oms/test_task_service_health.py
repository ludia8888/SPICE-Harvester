from __future__ import annotations

import pytest

from oms.routers.tasks import task_service_health
from shared.models.background_task import TaskMetrics


class _TaskManager:
    def __init__(self, metrics: TaskMetrics) -> None:
        self._metrics = metrics

    async def get_task_metrics(self) -> TaskMetrics:
        return self._metrics


@pytest.mark.asyncio
async def test_task_service_health_returns_ready_surface() -> None:
    payload = await task_service_health(
        task_manager=_TaskManager(
            TaskMetrics(
                total_tasks=10,
                pending_tasks=1,
                processing_tasks=1,
                completed_tasks=8,
                failed_tasks=0,
                cancelled_tasks=0,
                retrying_tasks=0,
                average_duration=1.5,
                success_rate=100.0,
            )
        )
    )

    assert payload["status"] == "ready"
    assert payload["dependency_status"]["background_task_manager"] == "ready"
    assert payload["metrics"]["active_tasks"] == 1


@pytest.mark.asyncio
async def test_task_service_health_returns_degraded_surface_with_retryable_classification() -> None:
    payload = await task_service_health(
        task_manager=_TaskManager(
            TaskMetrics(
                total_tasks=100,
                pending_tasks=2,
                processing_tasks=11,
                completed_tasks=70,
                failed_tasks=25,
                cancelled_tasks=0,
                retrying_tasks=6,
                average_duration=9.2,
                success_rate=70.0,
            )
        )
    )

    assert payload["status"] == "degraded"
    assert payload["dependency_status"]["background_task_manager"] == "degraded"
    assert payload["impact_summary"]["classifications"]["retryable"] == 1
    assert "oms.background_tasks" in payload["affected_features"]
