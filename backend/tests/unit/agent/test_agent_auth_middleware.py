from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from agent.middleware.auth import ensure_agent_auth_configured, install_agent_auth_middleware


@contextmanager
def _set_env(**updates: str | None) -> Iterator[None]:
    original = {key: os.environ.get(key) for key in updates}
    for key, value in updates.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    try:
        yield
    finally:
        for key, value in original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _build_app() -> FastAPI:
    app = FastAPI()
    install_agent_auth_middleware(app)

    @app.get("/health")
    async def health():
        return {"ok": True}

    @app.post("/api/v1/agent/runs")
    async def create_run():
        return {"ok": True}

    return app


@pytest.mark.unit
def test_agent_auth_enforced_for_run_endpoints() -> None:
    with _set_env(
        AGENT_REQUIRE_AUTH="true",
        BFF_AGENT_TOKEN="agent-secret",
        ADMIN_TOKEN=None,
        ADMIN_API_KEY=None,
    ):
        ensure_agent_auth_configured()
        client = TestClient(_build_app())

        missing = client.post("/api/v1/agent/runs")
        assert missing.status_code == 401

        wrong = client.post("/api/v1/agent/runs", headers={"Authorization": "Bearer wrong"})
        assert wrong.status_code == 403

        ok = client.post("/api/v1/agent/runs", headers={"Authorization": "Bearer agent-secret"})
        assert ok.status_code == 200


@pytest.mark.unit
def test_agent_health_path_remains_exempt() -> None:
    with _set_env(
        AGENT_REQUIRE_AUTH="true",
        BFF_AGENT_TOKEN="agent-secret",
    ):
        client = TestClient(_build_app())
        response = client.get("/health")
        assert response.status_code == 200


@pytest.mark.unit
def test_agent_auth_configuration_requires_token() -> None:
    with _set_env(
        AGENT_REQUIRE_AUTH="true",
        BFF_AGENT_TOKEN=None,
        AGENT_BFF_TOKEN=None,
        BFF_ADMIN_TOKEN="admin-secret",
        ADMIN_TOKEN=None,
        ADMIN_API_KEY=None,
    ):
        with pytest.raises(RuntimeError):
            ensure_agent_auth_configured()
