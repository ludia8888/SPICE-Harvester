from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bff.middleware.auth import install_bff_auth_middleware


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
    install_bff_auth_middleware(app)

    @app.get("/metrics")
    async def metrics():
        return {"ok": True}

    return app


@pytest.mark.unit
def test_metrics_is_not_exempt_in_production_even_if_env_lists_it() -> None:
    with _set_env(
        ENVIRONMENT="production",
        BFF_REQUIRE_AUTH="true",
        BFF_ADMIN_TOKEN="prod-admin-token-value",
        BFF_AUTH_EXEMPT_PATHS="/metrics,/api/v1/health",
    ):
        client = TestClient(_build_app())
        unauthorized = client.get("/metrics")
        assert unauthorized.status_code == 401

        authorized = client.get("/metrics", headers={"X-Admin-Token": "prod-admin-token-value"})
        assert authorized.status_code == 200


@pytest.mark.unit
def test_metrics_can_stay_exempt_in_development() -> None:
    with _set_env(
        ENVIRONMENT="development",
        BFF_REQUIRE_AUTH="true",
        BFF_ADMIN_TOKEN="dev-admin-token-value",
        BFF_AUTH_EXEMPT_PATHS="/metrics,/api/v1/health",
    ):
        client = TestClient(_build_app())
        response = client.get("/metrics")
        assert response.status_code == 200
