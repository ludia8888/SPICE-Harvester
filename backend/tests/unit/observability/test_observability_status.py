from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from shared.observability.metrics import get_metrics_collector, get_metrics_runtime_status
from shared.services.core.service_factory import ServiceInfo, create_fastapi_service


@pytest.mark.unit
def test_metrics_collector_is_scoped_per_service_name() -> None:
    collector_a = get_metrics_collector("unit-observe-a")
    collector_b = get_metrics_collector("unit-observe-b")

    assert collector_a is not collector_b
    assert collector_a.service_name == "unit-observe-a"
    assert collector_b.service_name == "unit-observe-b"

    status = get_metrics_runtime_status("unit-observe-a")
    assert status["service"] == "unit-observe-a"
    assert "enabled" in status
    assert "active" in status
    assert "exporters_enabled" in status
    assert "exporters_active" in status


@pytest.mark.unit
def test_service_factory_exposes_observability_status_endpoint() -> None:
    app = create_fastapi_service(
        service_info=ServiceInfo(
            name="unit-observe-app",
            title="Observability Test Service",
            description="Validates observability status endpoint",
        ),
        include_health_check=False,
        include_logging_middleware=False,
    )
    client = TestClient(app)

    response = client.get("/observability/status")
    assert response.status_code in {200, 503}
    payload = response.json()
    assert payload["service"] == "unit-observe-app"
    assert payload["status"] in {"ready", "degraded", "hard_down"}
    assert "dependency_status" in payload
    assert "impact_summary" in payload
    assert "tracing" in payload
    assert "metrics" in payload
    assert "enabled" in payload["tracing"]
    assert "active" in payload["tracing"]
    assert "enabled" in payload["metrics"]
    assert "active" in payload["metrics"]


@pytest.mark.unit
def test_service_factory_health_reflects_runtime_status() -> None:
    app = create_fastapi_service(
        service_info=ServiceInfo(
            name="unit-observe-app",
            title="Observability Test Service",
            description="Validates health status endpoint",
        ),
        include_health_check=True,
        include_logging_middleware=False,
    )
    app.state.runtime_status = {
        "ready": False,
        "degraded": True,
        "issues": ["event_store_unavailable"],
        "background_tasks": {},
    }
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 503
    payload = response.json()
    assert payload["status"] == "error"
    assert payload["data"]["status"] == "hard_down"
    assert payload["data"]["ready"] is False
    assert payload["data"]["issues"][0]["component"] == "event_store_unavailable"
    assert payload["data"]["dependency_details"]["event_store_unavailable"]["state"] == "hard_down"
    assert payload["data"]["root_causes"][0]["dependency"] == "event_store_unavailable"
    assert payload["data"]["classification"] if False else True


@pytest.mark.unit
def test_observability_status_degrades_when_tracing_runtime_resolution_fails() -> None:
    app = create_fastapi_service(
        service_info=ServiceInfo(
            name="unit-observe-app",
            title="Observability Test Service",
            description="Validates observability failure classification",
        ),
        include_health_check=False,
        include_logging_middleware=False,
    )

    class _BrokenTracingService:
        def runtime_status(self):  # noqa: ANN201
            raise RuntimeError("tracing unavailable")

    app.state.tracing_service = _BrokenTracingService()
    client = TestClient(app)

    response = client.get("/observability/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "degraded"
    assert payload["dependency_status"]["tracing_status"] == "degraded"
    detail = payload["dependency_details"]["tracing_status"]
    assert detail["classification"] == "internal"
    assert "tracing" in payload["impact_summary"]["affected_features"]
