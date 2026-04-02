import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from shared.config.settings import ApplicationSettings, DatabaseSettings
from shared.observability.config_monitor import ConfigurationMonitor
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


@pytest.mark.unit
def test_config_monitor_snapshot_matches_current_schema() -> None:
    monitor = ConfigurationMonitor(ApplicationSettings())

    snapshot = monitor.get_config_snapshot()

    assert "postgres" in snapshot["database"]
    assert "redis" in snapshot["database"]
    assert "elasticsearch" in snapshot["database"]
    assert "password" in snapshot["database"]["postgres"]
    assert snapshot["database"]["postgres"]["password"] is None


@pytest.mark.unit
def test_config_monitor_validation_avoids_false_positive_for_existing_prod_settings() -> None:
    settings = ApplicationSettings(
        environment="production",
        debug=False,
        database=DatabaseSettings(
            postgres_host="db.internal",
            postgres_password="averysecurepassword",
            redis_password="redis-secret",
        ),
    )
    monitor = ConfigurationMonitor(settings)

    violations = monitor.validate_configuration()

    assert not any(item["key_path"] == "database.postgres.password" for item in violations)
    assert not any(item["key_path"] == "database.redis.password" for item in violations)
