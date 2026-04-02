from __future__ import annotations

from dataclasses import dataclass

from fastapi import FastAPI
from fastapi.testclient import TestClient

from shared.routers.monitoring import router


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

    assert readiness.status_code == 503
    assert readiness.json()["ready"] is False

    assert liveness.status_code == 200
    assert liveness.json()["alive"] is True

    assert detailed.status_code == 503
    assert detailed.json()["status"] == "unready"


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


class DummyService:
    async def health_check(self) -> bool:
        return True


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
    assert payload["services"]["DummyService"]["status"] == "healthy"
