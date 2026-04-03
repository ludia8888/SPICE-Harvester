from __future__ import annotations

from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from bff.dependencies import get_oms_client
from bff.main import app


class _Container:
    is_initialized = True


def test_bff_readiness_reflects_degraded_startup_state(monkeypatch) -> None:
    fake_oms = AsyncMock()
    fake_oms.check_health.return_value = True
    previous_runtime_status = getattr(app.state, "bff_runtime_status", None)
    app.state.bff_runtime_status = {
        "ready": True,
        "degraded": True,
        "issues": [
            {
                "component": "dataset_ingest_outbox",
                "dependency": "dataset_ingest_outbox",
                "state": "degraded",
                "classification": "unavailable",
                "message": "worker not started",
                "affected_features": ["dataset_ingest"],
            }
        ],
        "background_tasks": {"dataset_ingest_outbox": {"status": "failed"}},
    }
    app.dependency_overrides[get_oms_client] = lambda: fake_oms
    monkeypatch.setattr("shared.routers.monitoring._safe_get_container", AsyncMock(return_value=_Container()))

    client = TestClient(app)
    try:
        health = client.get("/api/v1/health")
        readiness = client.get("/api/v1/monitoring/health/readiness")
    finally:
        app.dependency_overrides.clear()
        if previous_runtime_status is None:
            delattr(app.state, "bff_runtime_status")
        else:
            app.state.bff_runtime_status = previous_runtime_status

    assert health.status_code == 200
    assert health.json()["data"]["ready"] is True
    assert health.json()["data"]["status"] == "degraded"
    assert health.json()["data"]["affected_features"] == ["dataset_ingest"]

    assert readiness.status_code == 200
    assert readiness.json()["ready"] is True
    assert readiness.json()["degraded"] is True
    assert readiness.json()["issues"][0]["component"] == "dataset_ingest_outbox"
