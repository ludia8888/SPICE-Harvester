from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from shared.services.core.runtime_status import (
    availability_surface,
    probe_service_runtime_state,
    record_runtime_issue,
)


@pytest.mark.unit
def test_availability_surface_exposes_dependency_details_and_root_causes() -> None:
    surface = availability_surface(
        service="unit-runtime",
        container_ready=True,
        runtime_status={
            "ready": True,
            "degraded": True,
            "issues": [
                {
                    "component": "redis_listener",
                    "dependency": "redis",
                    "message": "Redis unavailable",
                    "state": "degraded",
                    "classification": "unavailable",
                    "affected_features": ["command_status", "websocket"],
                }
            ],
            "background_tasks": {"projection": {"status": "degraded"}},
        },
    )

    assert surface["status"] == "degraded"
    assert surface["status_reason"] == "Redis unavailable"
    assert surface["dependency_details"]["redis"]["state"] == "degraded"
    assert surface["dependency_details"]["redis"]["classification"] == "unavailable"
    assert surface["dependency_details"]["redis"]["components"] == ["redis_listener"]
    assert surface["dependency_details"]["redis"]["affected_features"] == ["command_status", "websocket"]
    assert surface["root_causes"][0]["dependency"] == "redis"
    assert surface["impact_summary"]["classifications"] == {"unavailable": 1}


@pytest.mark.unit
def test_availability_surface_adds_generic_services_root_cause_for_unhealthy_checks() -> None:
    surface = availability_surface(
        service="unit-runtime",
        container_ready=True,
        runtime_status={"ready": True, "degraded": False, "issues": [], "background_tasks": {}},
        unhealthy_services=2,
    )

    assert surface["status"] == "degraded"
    assert surface["dependency_status"]["services"] == "degraded"
    assert surface["dependency_details"]["services"]["message"] == "One or more initialized services failed health checks"
    assert surface["root_causes"][0]["dependency"] == "services"


@pytest.mark.unit
def test_record_runtime_issue_emits_structured_taxonomy_log(caplog: pytest.LogCaptureFixture) -> None:
    holder = SimpleNamespace(state=SimpleNamespace(service_name="unit-runtime"))

    with caplog.at_level(logging.WARNING, logger="shared.services.core.runtime_status"):
        record_runtime_issue(
            holder,
            component="redis_listener",
            dependency="redis",
            message="Redis unavailable",
            state="degraded",
            classification="unavailable",
            affected_features=("command_status", "websocket"),
            affects_readiness=False,
        )

    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.getMessage() == "Runtime dependency issue recorded"
    assert getattr(record, "_taxonomy_event_name") == "runtime.issue"
    assert getattr(record, "_taxonomy_event_family") == "availability"
    runtime_issue = getattr(record, "runtime_issue")
    assert runtime_issue["service"] == "unit-runtime"
    assert runtime_issue["dependency"] == "redis"
    assert runtime_issue["classification"] == "unavailable"
    assert runtime_issue["affected_features"] == ["command_status", "websocket"]


@pytest.mark.unit
def test_availability_surface_accepts_dependency_overrides_and_message() -> None:
    surface = availability_surface(
        service="unit-runtime",
        container_ready=True,
        runtime_status={"ready": True, "issues": []},
        dependency_status_overrides={"redis": "degraded"},
        status_reason_override="Redis warming up",
        message="Redis warming up",
    )

    assert surface["status"] == "degraded"
    assert surface["dependency_status"]["redis"] == "degraded"
    assert surface["status_reason"] == "Redis warming up"
    assert surface["message"] == "Redis warming up"
    assert surface["root_causes"][0]["dependency"] == "redis"
    assert surface["root_causes"][0]["classification"] == "unavailable"


@pytest.mark.unit
def test_availability_surface_does_not_emit_legacy_healthy_or_unknown_classifications() -> None:
    surface = availability_surface(
        service="unit-runtime",
        container_ready=True,
        runtime_status={"ready": True, "degraded": False, "issues": [], "background_tasks": {}},
    )

    assert surface["dependency_details"]["container"]["classification"] is None
    assert surface["dependency_details"]["runtime"]["classification"] is None


@pytest.mark.asyncio
async def test_probe_service_runtime_state_supports_common_probe_methods() -> None:
    healthy = SimpleNamespace(health_check=AsyncMock(return_value=True))
    ping_only = SimpleNamespace(ping=AsyncMock(return_value=True))
    connection_only = SimpleNamespace(check_connection=AsyncMock(return_value=False))
    unsupported = SimpleNamespace()

    assert await probe_service_runtime_state(healthy) == "ready"
    assert await probe_service_runtime_state(ping_only) == "ready"
    assert await probe_service_runtime_state(connection_only) == "hard_down"
    assert await probe_service_runtime_state(unsupported) == "degraded"
