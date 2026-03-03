from __future__ import annotations

from pathlib import Path

import pytest

from shared.services.core.agent_internal_client import build_agent_internal_headers


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


@pytest.mark.unit
def test_build_agent_internal_headers_requires_token() -> None:
    with pytest.raises(ValueError):
        build_agent_internal_headers(agent_token="")


@pytest.mark.unit
def test_build_agent_internal_headers_sets_required_auth_headers() -> None:
    headers = build_agent_internal_headers(
        agent_token="internal-agent-token",
        delegated_user_bearer="delegated-user-jwt",
        extra_headers={"X-Request-Id": "req-1"},
    )
    assert headers["Authorization"] == "Bearer internal-agent-token"
    assert headers["X-Delegated-Authorization"] == "Bearer delegated-user-jwt"
    assert headers["X-Request-Id"] == "req-1"


@pytest.mark.unit
def test_runtime_code_does_not_hardcode_agent_internal_endpoint() -> None:
    root = _repo_root()
    allowed_files = {
        "backend/shared/config/settings.py",
        "backend/shared/config/service_config.py",
        "backend/shared/routers/monitoring.py",
        "backend/shared/observability/config_monitor.py",
        "backend/scripts/validate_environment.py",
        "backend/shared/services/core/agent_internal_client.py",
    }
    offenders: list[str] = []
    for path in (root / "backend").rglob("*.py"):
        rel = path.relative_to(root).as_posix()
        if rel.startswith("backend/tests/"):
            continue
        if rel in allowed_files:
            continue
        content = path.read_text(encoding="utf-8")
        if "http://agent:8004" in content:
            offenders.append(rel)
    assert offenders == [], f"Hardcoded Agent endpoint found outside allowlist: {offenders}"
