import os
from contextlib import contextmanager

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from bff.middleware.auth import install_bff_auth_middleware
from shared.i18n.middleware import install_i18n_middleware
from shared.middleware.rate_limiter import rate_limit, install_rate_limit_headers_middleware


@contextmanager
def _set_env(**updates):
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


@pytest.mark.unit
def test_i18n_large_json_not_truncated():
    app = FastAPI()
    install_i18n_middleware(app, max_body_bytes=100)

    payload = {"message": "ok", "data": "x" * 1000}

    @app.get("/large")
    def large():
        return payload

    client = TestClient(app)
    resp = client.get("/large", headers={"Accept-Language": "en"})
    assert resp.status_code == 200
    assert resp.json() == payload


@pytest.mark.unit
def test_i18n_translates_description_field():
    app = FastAPI()
    install_i18n_middleware(app)

    @app.get("/health")
    def health():
        return {
            "message": "Service is healthy",
            "description": "도메인 독립적인 온톨로지 관리 서비스",
        }

    client = TestClient(app)
    resp = client.get("/health", headers={"Accept-Language": "en"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["description"] == "Domain-agnostic ontology management service."


@pytest.mark.unit
def test_rate_limit_headers_attach_for_dict_response():
    with _set_env(RATE_LIMIT_FAIL_OPEN="true", REDIS_URL="redis://127.0.0.1:6399"):
        app = FastAPI()
        install_rate_limit_headers_middleware(app)

        @app.get("/limited")
        @rate_limit(requests=5, window=60)
        async def limited(request: Request):
            return {"ok": True}

        client = TestClient(app)
        resp = client.get("/limited")
        assert resp.status_code == 200
        assert resp.headers.get("X-RateLimit-Limit") == "5"
        assert resp.headers.get("X-RateLimit-Remaining") is not None
        assert resp.headers.get("X-RateLimit-Disabled") == "true"


@pytest.mark.unit
def test_bff_auth_middleware_blocks_unsafe_methods():
    with _set_env(BFF_REQUIRE_AUTH="true", BFF_ADMIN_TOKEN="secret"):
        app = FastAPI()
        install_bff_auth_middleware(app)

        @app.post("/write")
        async def write():
            return {"ok": True}

        client = TestClient(app)

        resp = client.post("/write")
        assert resp.status_code == 401

        resp = client.post("/write", headers={"X-Admin-Token": "wrong"})
        assert resp.status_code == 403

        resp = client.post("/write", headers={"X-Admin-Token": "secret"})
        assert resp.status_code == 200


@pytest.mark.unit
def test_rate_limit_admin_bypass_requires_valid_token():
    def _build_app():
        app = FastAPI()
        install_rate_limit_headers_middleware(app)

        @app.post("/limited")
        @rate_limit(requests=2, window=60)
        async def limited(request: Request):  # noqa: ARG001
            return {"ok": True}

        return app

    with _set_env(REDIS_URL="redis://127.0.0.1:6399", RATE_LIMIT_FAIL_OPEN="false", BFF_ADMIN_TOKEN="secret"):
        # No token: rate limit enforced.
        client = TestClient(_build_app())
        headers = {"X-Forwarded-For": "203.0.113.10"}
        assert client.post("/limited", headers=headers).status_code == 200
        assert client.post("/limited", headers=headers).status_code == 200
        assert client.post("/limited", headers=headers).status_code == 429

        # Wrong token: still enforced.
        client = TestClient(_build_app())
        headers = {"X-Forwarded-For": "203.0.113.11", "X-Admin-Token": "wrong"}
        assert client.post("/limited", headers=headers).status_code == 200
        assert client.post("/limited", headers=headers).status_code == 200
        assert client.post("/limited", headers=headers).status_code == 429

        # Valid token: bypass after exhaustion.
        client = TestClient(_build_app())
        headers = {"X-Forwarded-For": "203.0.113.12", "X-Admin-Token": "secret"}
        assert client.post("/limited", headers=headers).status_code == 200
        assert client.post("/limited", headers=headers).status_code == 200
        assert client.post("/limited", headers=headers).status_code == 200
