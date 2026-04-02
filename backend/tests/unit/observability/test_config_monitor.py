import asyncio
import logging
from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from shared.config.settings import ApplicationSettings, DatabaseSettings
from shared.observability.config_monitor import (
    ConfigChange,
    ConfigChangeType,
    ConfigSeverity,
    ConfigurationMonitor,
)
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


@pytest.mark.unit
def test_config_monitor_callback_masks_sensitive_values(caplog: pytest.LogCaptureFixture) -> None:
    change = ConfigChange(
        change_type=ConfigChangeType.SECURITY_SENSITIVE,
        key_path="database.postgres.password",
        old_value="old-secret",
        new_value="new-secret",
        timestamp=datetime.now(timezone.utc),
        severity=ConfigSeverity.CRITICAL,
    )

    with caplog.at_level(logging.WARNING):
        config_monitoring._log_critical_changes(change)

    assert "new-secret" not in caplog.text
    assert "*****" in caplog.text


@pytest.mark.unit
def test_get_config_monitor_refreshes_singleton_when_settings_change(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_monitoring, "_config_monitor", None)
    monkeypatch.setattr(config_monitoring, "_config_monitor_settings_key", None)

    settings_a = ApplicationSettings(environment="development")
    settings_b = ApplicationSettings(environment="production")

    monitor_a = asyncio.run(config_monitoring.get_config_monitor(settings_a))
    monitor_b = asyncio.run(config_monitoring.get_config_monitor(settings_b))

    assert monitor_a is not monitor_b
    assert monitor_b.settings is settings_b


@pytest.mark.unit
def test_config_monitoring_status_returns_500_on_internal_error() -> None:
    class BrokenMonitor:
        @property
        def monitoring_enabled(self):  # noqa: ANN201
            raise RuntimeError("boom")

    app = FastAPI()
    app.include_router(config_monitoring.router, prefix="/api/v1/config")
    app.dependency_overrides[config_monitoring.get_config_monitor] = lambda: BrokenMonitor()

    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/api/v1/config/config/monitoring-status")

    assert response.status_code == 500
