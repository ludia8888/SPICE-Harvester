from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from shared.routers.monitoring import router
from shared.dependencies.providers import get_initialized_background_task_manager


def _build_client() -> TestClient:
    app = FastAPI()
    app.include_router(router, prefix="/api/v1/monitoring")
    return TestClient(app)


def test_monitoring_health_endpoints_do_not_fail_open_without_container() -> None:
    client = _build_client()
    client.app.state.bff_runtime_status = {
        "ready": True,
        "issues": [],
        "background_tasks": {},
    }

    health = client.get("/api/v1/monitoring/health")
    readiness = client.get("/api/v1/monitoring/health/readiness")
    liveness = client.get("/api/v1/monitoring/health/liveness")
    detailed = client.get("/api/v1/monitoring/health/detailed")

    assert health.status_code == 503
    assert health.json()["ready"] is False
    assert health.json()["hard_down"] is True
    assert health.json()["accepting_traffic"] is False

    assert readiness.status_code == 503
    assert readiness.json()["ready"] is False
    assert readiness.json()["hard_down"] is True

    assert liveness.status_code == 200
    assert liveness.json()["alive"] is True
    assert liveness.json()["status"] == "ready"
    assert liveness.json()["ready"] is True

    assert detailed.status_code == 503
    assert detailed.json()["status"] == "hard_down"
    assert detailed.json()["hard_down"] is True


def test_monitoring_status_does_not_500_without_container() -> None:
    client = _build_client()
    client.app.state.bff_runtime_status = {
        "ready": False,
        "issues": ["registry_unavailable"],
        "background_tasks": {"projection": "degraded"},
    }

    response = client.get("/api/v1/monitoring/status")

    assert response.status_code == 503
    payload = response.json()
    assert payload["status"] == "hard_down"
    assert payload["ready"] is False
    assert payload["hard_down"] is True
    assert payload["issues"][0]["component"] == "registry_unavailable"
    assert payload["dependency_status"]["registry_unavailable"] == "hard_down"
    assert payload["dependency_details"]["registry_unavailable"]["classification"] == "unavailable"
    assert payload["root_causes"][0]["dependency"] == "registry_unavailable"
    assert payload["status_reason"] == "registry_unavailable"
    assert payload["impact_summary"]["affected_features"] == ["registry_unavailable"]
    assert payload["background_tasks"] == {"projection": {"status": "degraded"}}


@dataclass
class _Registration:
    service_type: type
    singleton: bool = True
    initialized: bool = True
    instance: object | None = object()


class _Container:
    def __init__(self) -> None:
        self.is_initialized = True
        self._services = {
            DummyService: _Registration(service_type=DummyService, instance=DummyService())
        }

    def get_service_info(self) -> dict[str, dict[str, object]]:
        return {}


class DummyService:
    async def health_check(self) -> bool:
        return True


class BrokenService:
    async def health_check(self) -> bool:
        return False


def test_detailed_health_serializes_type_based_service_keys(
    monkeypatch,
) -> None:
    client = _build_client()

    async def _fake_get_container() -> _Container:
        return _Container()

    monkeypatch.setattr("shared.routers.monitoring._safe_get_container", _fake_get_container)

    response = client.get("/api/v1/monitoring/health/detailed")

    assert response.status_code == 200
    payload = response.json()
    assert "DummyService" in payload["services"]
    assert payload["services"]["DummyService"]["status"] == "ready"
    assert payload["services"]["DummyService"]["probe_status"] == "ok"


def test_monitoring_surfaces_affected_features_and_dependency_states(monkeypatch) -> None:
    client = _build_client()
    client.app.state.runtime_status = {
        "ready": True,
        "degraded": True,
        "issues": [
            {
                "component": "redis",
                "dependency": "redis",
                "message": "Redis unavailable",
                "state": "degraded",
                "affected_features": ["command_status", "websocket"],
            }
        ],
        "background_tasks": {},
    }

    async def _fake_get_container() -> _Container:
        return _Container()

    monkeypatch.setattr("shared.routers.monitoring._safe_get_container", _fake_get_container)
    response = client.get("/api/v1/monitoring/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "degraded"
    assert payload["dependency_status"]["redis"] == "degraded"
    assert payload["dependency_details"]["redis"]["message"] == "Redis unavailable"
    assert payload["root_causes"][0]["dependency"] == "redis"
    assert payload["affected_features"] == ["command_status", "websocket"]


def test_monitoring_status_uses_runtime_probe_results_for_service_summary(monkeypatch) -> None:
    client = _build_client()

    class _StatusContainer:
        def __init__(self) -> None:
            self.is_initialized = True
            self._services = {
                DummyService: _Registration(service_type=DummyService, instance=DummyService()),
                BrokenService: _Registration(service_type=BrokenService, instance=BrokenService()),
            }

        def get_service_info(self) -> dict[str, dict[str, object]]:
            return {}

    async def _fake_get_container() -> _StatusContainer:
        return _StatusContainer()

    monkeypatch.setattr("shared.routers.monitoring._safe_get_container", _fake_get_container)
    response = client.get("/api/v1/monitoring/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "degraded"
    assert payload["summary"]["registered_services"] == 2
    assert payload["summary"]["instantiated_services"] == 2
    assert payload["summary"]["unhealthy_services"] == 1
    assert payload["services"]["DummyService"]["status"] == "ready"
    assert payload["services"]["BrokenService"]["status"] == "hard_down"
    assert payload["services"]["BrokenService"]["probe_status"] == "failed"


class _TaskManager:
    def __init__(
        self,
        *,
        success_rate: float,
        processing_tasks: int,
        retrying_tasks: int = 0,
        pending_tasks: int = 0,
        dead_tasks: int = 0,
    ) -> None:
        self._metrics = SimpleNamespace(
            total_tasks=42,
            active_tasks=5,
            pending_tasks=pending_tasks,
            processing_tasks=processing_tasks,
            retrying_tasks=retrying_tasks,
            completed_tasks=37,
            failed_tasks=5,
            success_rate=success_rate,
            average_duration=1.25,
            tasks_by_type={"projection": 5},
        )
        self._dead_tasks = [object() for _ in range(dead_tasks)]

    async def get_task_metrics(self) -> SimpleNamespace:
        return self._metrics

    async def _get_dead_tasks(self):  # noqa: ANN202
        return list(self._dead_tasks)

    async def get_all_tasks(self, *, status, limit: int):  # noqa: ANN001, ANN202
        _ = status
        now = datetime(2026, 4, 4, tzinfo=timezone.utc)
        task = SimpleNamespace(
            task_id="task-1",
            task_name="Projection",
            task_type="projection",
            status=SimpleNamespace(value="processing"),
            created_at=now,
            started_at=now,
            duration=3.0,
            progress=None,
            retry_count=0,
            metadata={},
        )
        return [task][:limit]


def test_background_task_metrics_returns_canonical_surface_when_unavailable() -> None:
    client = _build_client()
    client.app.dependency_overrides[get_initialized_background_task_manager] = lambda: None

    response = client.get("/api/v1/monitoring/background-tasks/metrics")

    assert response.status_code == 503
    payload = response.json()
    assert payload["status"] == "hard_down"
    assert payload["dependency_status"]["background_task_manager"] == "hard_down"
    assert payload["message"] == "Background task manager not initialized"


def test_background_task_health_returns_canonical_surface_for_degraded_metrics() -> None:
    client = _build_client()
    client.app.dependency_overrides[get_initialized_background_task_manager] = (
        lambda: _TaskManager(success_rate=75.0, processing_tasks=55, retrying_tasks=11, pending_tasks=120, dead_tasks=2)
    )

    response = client.get("/api/v1/monitoring/background-tasks/health")

    assert response.status_code == 503
    payload = response.json()
    assert payload["status"] == "degraded"
    assert payload["healthy"] is False
    assert payload["dependency_status"]["background_task_manager"] == "degraded"
    assert payload["impact_summary"]["classifications"]["retryable"] >= 1
    assert payload["recommendations"]
