from __future__ import annotations

from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from bff.dependencies import get_oms_client
from bff.main import app


def test_bff_readiness_reflects_degraded_startup_state() -> None:
    fake_oms = AsyncMock()
    fake_oms.check_health.return_value = True
    previous_runtime_status = getattr(app.state, "bff_runtime_status", None)
    app.state.bff_runtime_status = {
        "ready": False,
        "degraded": True,
        "issues": [{"component": "dataset_ingest_outbox", "message": "worker not started"}],
        "background_tasks": {"dataset_ingest_outbox": {"status": "failed"}},
    }
    app.dependency_overrides[get_oms_client] = lambda: fake_oms

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
    assert health.json()["data"]["ready"] is False
    assert health.json()["data"]["status"] == "degraded"

    assert readiness.status_code == 503
    assert readiness.json()["ready"] is False
    assert readiness.json()["issues"][0]["component"] == "dataset_ingest_outbox"
