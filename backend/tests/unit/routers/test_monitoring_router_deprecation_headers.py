from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from shared.routers.monitoring import router


def _build_client() -> TestClient:
    app = FastAPI()
    app.include_router(router, prefix="/api/v1/monitoring")
    return TestClient(app)


def test_metrics_redirect_without_deprecated_query_has_no_deprecation_headers() -> None:
    client = _build_client()

    response = client.get("/api/v1/monitoring/metrics", follow_redirects=False)

    assert response.status_code == 307
    assert response.headers.get("deprecation") is None
    assert response.headers.get("warning") is None


def test_metrics_redirect_with_deprecated_query_sets_deprecation_headers() -> None:
    client = _build_client()

    response = client.get("/api/v1/monitoring/metrics?service_name=bff", follow_redirects=False)

    assert response.status_code == 307
    assert response.headers.get("deprecation") == "true"
    warning = response.headers.get("warning")
    assert warning is not None
    assert "service_name" in warning
    assert "deprecated" in warning.lower()
