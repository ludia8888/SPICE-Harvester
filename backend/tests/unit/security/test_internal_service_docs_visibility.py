from __future__ import annotations

from fastapi import FastAPI

from agent.main import app as agent_app
from ingest_reconciler_worker.main import app as ingest_reconciler_app
from shared.services.core.service_factory import ServiceInfo, create_fastapi_service


def _route_paths(app: FastAPI) -> set[str]:
    return {str(getattr(route, "path", "")) for route in app.routes}


def test_service_factory_disables_docs_when_urls_are_none() -> None:
    service_info = ServiceInfo(
        name="test-internal-service",
        title="Internal Service",
        description="test",
    )
    app = create_fastapi_service(
        service_info=service_info,
        include_health_check=True,
        include_logging_middleware=False,
        openapi_url=None,
        docs_url=None,
        redoc_url=None,
    )
    paths = _route_paths(app)
    assert "/openapi.json" not in paths
    assert "/docs" not in paths
    assert "/redoc" not in paths


def test_agent_service_hides_openapi_surface() -> None:
    paths = _route_paths(agent_app)
    assert "/openapi.json" not in paths
    assert "/docs" not in paths
    assert "/redoc" not in paths


def test_ingest_reconciler_service_hides_openapi_surface() -> None:
    paths = _route_paths(ingest_reconciler_app)
    assert "/openapi.json" not in paths
    assert "/docs" not in paths
    assert "/redoc" not in paths
