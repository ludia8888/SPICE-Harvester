from __future__ import annotations

from types import SimpleNamespace

import pytest

from bff.routers.admin_system import get_system_health


class _TaskManager:
    def __init__(self, *, success_rate: float, processing_tasks: int) -> None:
        self._metrics = SimpleNamespace(
            total_tasks=42,
            active_tasks=3,
            completed_tasks=39,
            failed_tasks=1,
            success_rate=success_rate,
            average_duration=1.5,
            processing_tasks=processing_tasks,
        )

    async def get_task_metrics(self) -> SimpleNamespace:
        return self._metrics


class _RedisService:
    def __init__(self, *, healthy: bool) -> None:
        self._healthy = healthy

    async def ping(self) -> bool:
        return self._healthy


@pytest.mark.asyncio
async def test_admin_system_health_uses_canonical_runtime_surface_when_ready() -> None:
    payload = await get_system_health(
        task_manager=_TaskManager(success_rate=98.0, processing_tasks=2),
        redis_service=_RedisService(healthy=True),
    )

    assert payload["status"] == "ready"
    assert payload["dependency_status"]["redis"] == "ready"
    assert payload["dependency_status"]["background_task_manager"] == "ready"
    assert payload["components"]["redis"]["status"] == "ready"
    assert payload["components"]["background_tasks"]["status"] == "ready"


@pytest.mark.asyncio
async def test_admin_system_health_reports_degraded_runtime_surface() -> None:
    payload = await get_system_health(
        task_manager=_TaskManager(success_rate=75.0, processing_tasks=25),
        redis_service=_RedisService(healthy=False),
    )

    assert payload["status"] == "hard_down"
    assert payload["dependency_status"]["redis"] == "hard_down"
    assert payload["dependency_status"]["background_task_manager"] == "degraded"
    assert payload["impact_summary"]["classifications"]["unavailable"] == 1
    assert payload["impact_summary"]["classifications"]["retryable"] == 1
