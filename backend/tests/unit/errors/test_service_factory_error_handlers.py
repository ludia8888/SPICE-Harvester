import pytest
from fastapi import HTTPException, status
from fastapi.testclient import TestClient

from shared.errors import error_response
from shared.services.core.service_factory import ServiceInfo, create_fastapi_service


@pytest.mark.unit
def test_service_factory_installs_error_handlers_by_default() -> None:
    service_info = ServiceInfo(
        name="bff",
        title="Test Service",
        description="Test Service for enterprise error handlers",
    )
    app = create_fastapi_service(
        service_info=service_info,
        include_health_check=False,
        include_logging_middleware=False,
    )

    @app.get("/boom")
    def boom():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "MAPPING_SPEC_TARGET_UNKNOWN", "message": "bad mapping spec"},
        )

    client = TestClient(app)
    resp = client.get("/boom")
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["status"] == "error"
    assert payload["code"] == "HTTP_ERROR"
    assert payload["enterprise"]["external_code"] == "MAPPING_SPEC_TARGET_UNKNOWN"
    assert payload["enterprise"]["code"].startswith("SHV-")
    assert payload["classification"]["family"] == "validation"
    assert payload["classification"]["retryable"] is False
    assert payload["diagnostics"]["schema"] == "error_diagnostics.v1"
    assert payload["diagnostics"]["group_fingerprint"].startswith("sha256:")
    assert payload["diagnostics"]["instance_fingerprint"].startswith("sha256:")
    assert payload["diagnostics"]["runbook_ref"] == payload["enterprise"]["runbook_ref"]
    assert payload["diagnostics"]["lookup"]["enterprise_code"] == payload["enterprise"]["code"]
    assert resp.headers.get("x-error-code") == payload["code"]
    assert resp.headers.get("x-enterprise-code") == payload["enterprise"]["code"]
    assert resp.headers.get("x-error-classification") == payload["classification"]["family"]
    assert resp.headers.get("x-runbook-ref") == payload["enterprise"]["runbook_ref"]
    assert resp.headers.get("x-error-group") == payload["diagnostics"]["group_fingerprint"]
    assert resp.headers.get("x-error-instance") == payload["diagnostics"]["instance_fingerprint"]


@pytest.mark.unit
def test_error_handler_records_error_taxonomy_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: list[dict] = []

    class _FakeCollector:
        def record_error_envelope(self, payload, *, source="http"):  # noqa: ANN001
            recorded.append({"payload": payload, "source": source})

    monkeypatch.setattr(
        error_response,
        "_get_error_metrics_collector",
        lambda service_name: _FakeCollector(),
    )

    service_info = ServiceInfo(
        name="oms",
        title="Test Service",
        description="Test Service for enterprise error metrics",
    )
    app = create_fastapi_service(
        service_info=service_info,
        include_health_check=False,
        include_logging_middleware=False,
    )

    @app.get("/explode")
    def explode():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "UPSTREAM_UNAVAILABLE", "category": "upstream", "message": "upstream unavailable"},
        )

    client = TestClient(app)
    resp = client.get("/explode")
    assert resp.status_code == 503
    assert len(recorded) == 1
    metric_payload = recorded[0]["payload"]
    assert recorded[0]["source"] == "http"
    assert metric_payload["code"] == "UPSTREAM_UNAVAILABLE"
    assert metric_payload["category"] == "upstream"
    assert metric_payload["enterprise"]["class"] == "unavailable"
    assert metric_payload["classification"]["family"] == "retryable"
