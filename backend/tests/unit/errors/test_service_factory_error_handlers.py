import pytest
from fastapi import HTTPException, status
from fastapi.testclient import TestClient

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
    assert payload["enterprise"]["legacy_code"] == "MAPPING_SPEC_TARGET_UNKNOWN"
    assert payload["enterprise"]["code"].startswith("SHV-")

