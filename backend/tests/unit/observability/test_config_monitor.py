import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from shared.routers import config_monitoring


@pytest.mark.unit
def test_config_monitor_current_endpoint_ok():
    app = FastAPI()
    app.include_router(config_monitoring.router, prefix="/api/v1/config")

    client = TestClient(app)
    resp = client.get("/api/v1/config/config/current")
    assert resp.status_code == 200
    data = resp.json()
    assert "configuration" in data
    assert "config_hash" in data
