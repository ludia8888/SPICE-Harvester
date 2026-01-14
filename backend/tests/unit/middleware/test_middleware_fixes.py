import os
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from jose import jwt
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from bff.middleware.auth import install_bff_auth_middleware
from shared.services.agent_tool_registry import AgentToolPolicyRecord
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


@pytest.mark.unit
def test_bff_auth_allows_user_jwt_when_enabled():
    token = jwt.encode({"sub": "user-1"}, "secret", algorithm="HS256")
    with _set_env(BFF_REQUIRE_AUTH="true", USER_JWT_ENABLED="true", USER_JWT_HS256_SECRET="secret"):
        app = FastAPI()
        install_bff_auth_middleware(app)

        @app.get("/hello")
        async def hello():  # noqa: ANN001
            return {"ok": True}

        client = TestClient(app)
        resp = client.get("/hello", headers={"Authorization": f"Bearer {token}"})
        assert resp.status_code == 200


@pytest.mark.unit
def test_bff_agent_auth_requires_delegated_user_jwt_when_enabled():
    token = jwt.encode({"sub": "user-1"}, "secret", algorithm="HS256")
    with _set_env(
        BFF_REQUIRE_AUTH="true",
        BFF_AGENT_TOKEN="agent-secret",
        USER_JWT_ENABLED="true",
        USER_JWT_HS256_SECRET="secret",
    ):
        app = FastAPI()
        install_bff_auth_middleware(app)

        @app.get("/hello")
        async def hello():  # noqa: ANN001
            return {"ok": True}

        client = TestClient(app)

        resp = client.get("/hello", headers={"X-Admin-Token": "agent-secret"})
        assert resp.status_code == 401

        resp = client.get(
            "/hello",
            headers={
                "X-Admin-Token": "agent-secret",
                "X-Delegated-Authorization": f"Bearer {token}",
                "X-Agent-Tool-ID": "unit.test",
            },
        )
        assert resp.status_code == 200


@pytest.mark.unit
def test_bff_agent_auth_requires_user_jwt_enabled_for_agent_calls():
    token = jwt.encode({"sub": "user-1"}, "secret", algorithm="HS256")
    with _set_env(BFF_REQUIRE_AUTH="true", BFF_AGENT_TOKEN="agent-secret", USER_JWT_ENABLED=None):
        app = FastAPI()
        install_bff_auth_middleware(app)

        @app.get("/hello")
        async def hello():  # noqa: ANN001
            return {"ok": True}

        client = TestClient(app)
        resp = client.get(
            "/hello",
            headers={
                "X-Admin-Token": "agent-secret",
                "X-Delegated-Authorization": f"Bearer {token}",
                "X-Agent-Tool-ID": "unit.test",
            },
        )
        assert resp.status_code == 503
        payload = resp.json()
        assert payload.get("context", {}).get("error") == "user-jwt-required-for-agent"


@pytest.mark.unit
def test_bff_admin_token_requires_user_jwt_for_agent_endpoints_when_enabled():
    user_token = jwt.encode({"sub": "user-1"}, "jwt-secret", algorithm="HS256")
    with _set_env(
        BFF_REQUIRE_AUTH="true",
        BFF_ADMIN_TOKEN="admin-secret",
        USER_JWT_ENABLED="true",
        USER_JWT_HS256_SECRET="jwt-secret",
    ):
        app = FastAPI()
        install_bff_auth_middleware(app)

        @app.get("/api/v1/agent/hello")
        async def hello(request: Request):  # noqa: ANN001
            return {"user_id": request.headers.get("X-User-ID")}

        client = TestClient(app)

        resp = client.get("/api/v1/agent/hello", headers={"X-Admin-Token": "admin-secret"})
        assert resp.status_code == 401

        resp = client.get(
            "/api/v1/agent/hello",
            headers={"X-Admin-Token": "admin-secret", "Authorization": f"Bearer {user_token}"},
        )
        assert resp.status_code == 200
        assert resp.json()["user_id"] == "user-1"


@pytest.mark.unit
def test_bff_agent_tool_policy_enforced_via_tool_registry():
    user_token = jwt.encode({"sub": "user-1", "roles": ["Owner"]}, "jwt-secret", algorithm="HS256")

    class StubToolRegistry:
        def __init__(self, policy):  # noqa: ANN001
            self._policy = policy

        async def get_tool_policy(self, *, tool_id: str):  # noqa: ANN001
            return self._policy.get(tool_id)

    class StubAgentRegistry:
        def __init__(self):  # noqa: D107
            self._store = {}

        async def get_tool_idempotency(self, *, tenant_id: str, idempotency_key: str):  # noqa: ANN001
            return self._store.get((tenant_id, idempotency_key))

        async def begin_tool_idempotency(  # noqa: ANN001
            self,
            *,
            tenant_id: str,
            idempotency_key: str,
            tool_id: str,
            request_digest: str,
            started_at=None,
        ):
            key = (tenant_id, idempotency_key)
            existing = self._store.get(key)
            if existing:
                return existing, False
            record = SimpleNamespace(
                tenant_id=tenant_id,
                idempotency_key=idempotency_key,
                tool_id=tool_id,
                request_digest=request_digest,
                status="IN_PROGRESS",
                response_status=None,
                response_body=None,
                error=None,
            )
            self._store[key] = record
            return record, True

        async def finalize_tool_idempotency(  # noqa: ANN001
            self,
            *,
            tenant_id: str,
            idempotency_key: str,
            tool_id: str,
            request_digest: str,
            response_status,
            response_body,
            error=None,
            finished_at=None,
        ):
            key = (tenant_id, idempotency_key)
            record = self._store.get(key)
            if not record or record.tool_id != tool_id or record.request_digest != request_digest:
                return None
            record.status = "COMPLETED"
            record.response_status = response_status
            record.response_body = response_body
            record.error = error
            return record

    class StubContainer:
        def __init__(self, *, tool_registry, agent_registry):  # noqa: ANN001
            self._tool_registry = tool_registry
            self._agent_registry = agent_registry

        def get_agent_tool_registry(self):  # noqa: ANN001
            return self._tool_registry

        def get_agent_registry(self):  # noqa: ANN001
            return self._agent_registry

    now = datetime.now(timezone.utc)
    policy = AgentToolPolicyRecord(
        tool_id="pipelines.build",
        method="POST",
        path="/api/v1/pipelines/{pipeline_id}/build",
        risk_level="write",
        requires_approval=True,
        requires_idempotency_key=True,
        status="ACTIVE",
        roles=["Owner"],
        max_payload_bytes=200000,
        created_at=now,
        updated_at=now,
    )

    with _set_env(
        BFF_REQUIRE_AUTH="true",
        BFF_AGENT_TOKEN="agent-secret",
        USER_JWT_ENABLED="true",
        USER_JWT_HS256_SECRET="jwt-secret",
    ):
        app = FastAPI()
        app.state.bff_container = StubContainer(
            tool_registry=StubToolRegistry({"pipelines.build": policy}),
            agent_registry=StubAgentRegistry(),
        )
        install_bff_auth_middleware(app)

        @app.post("/api/v1/pipelines/{pipeline_id}/build")
        async def build(pipeline_id: str):  # noqa: ANN001
            return {"pipeline_id": pipeline_id}

        client = TestClient(app)

        resp = client.post(
            "/api/v1/pipelines/123/build",
            headers={
                "X-Admin-Token": "agent-secret",
                "X-Delegated-Authorization": f"Bearer {user_token}",
                "X-Agent-Tool-ID": "pipelines.build",
                "X-Agent-Tool-Run-ID": "11111111-1111-1111-1111-111111111111",
                "Idempotency-Key": "unit-test-idem-1",
            },
        )
        assert resp.status_code == 200

        # Unknown tool_id => denied.
        resp = client.post(
            "/api/v1/pipelines/123/build",
            headers={
                "X-Admin-Token": "agent-secret",
                "X-Delegated-Authorization": f"Bearer {user_token}",
                "X-Agent-Tool-ID": "pipelines.deploy",
            },
        )
        assert resp.status_code == 403

        # Role mismatch => denied.
        user_no_role = jwt.encode({"sub": "user-2", "roles": ["Viewer"]}, "jwt-secret", algorithm="HS256")
        resp = client.post(
            "/api/v1/pipelines/123/build",
            headers={
                "X-Admin-Token": "agent-secret",
                "X-Delegated-Authorization": f"Bearer {user_no_role}",
                "X-Agent-Tool-ID": "pipelines.build",
                "Idempotency-Key": "unit-test-idem-2",
            },
        )
        assert resp.status_code == 403


@pytest.mark.unit
def test_bff_agent_tool_policy_enforces_session_enabled_tools_and_abac():
    user_token = jwt.encode({"sub": "user-1", "roles": ["Owner"], "tenant_id": "tenant-1"}, "jwt-secret", algorithm="HS256")

    class StubToolRegistry:
        def __init__(self, policy):  # noqa: ANN001
            self._policy = policy

        async def get_tool_policy(self, *, tool_id: str):  # noqa: ANN001
            return self._policy.get(tool_id)

    class StubSessionRegistry:
        def __init__(self, session):  # noqa: ANN001
            self._session = session

        async def get_session(self, *, session_id: str, tenant_id: str):  # noqa: ANN001
            if session_id != self._session["session_id"] or tenant_id != self._session["tenant_id"]:
                return None
            return SimpleNamespace(**self._session)

    class StubPolicyRegistry:
        def __init__(self, policy):  # noqa: ANN001
            self._policy = policy

        async def get_tenant_policy(self, *, tenant_id: str):  # noqa: ANN001
            if tenant_id != self._policy["tenant_id"]:
                return None
            return SimpleNamespace(**self._policy)

    class StubContainer:
        def __init__(self, *, tool_registry, session_registry, policy_registry):  # noqa: ANN001
            self._tool_registry = tool_registry
            self._session_registry = session_registry
            self._policy_registry = policy_registry

        def get_agent_tool_registry(self):  # noqa: ANN001
            return self._tool_registry

        def get_agent_session_registry(self):  # noqa: ANN001
            return self._session_registry

        def get_agent_policy_registry(self):  # noqa: ANN001
            return self._policy_registry

    now = datetime.now(timezone.utc)
    tool_policy = AgentToolPolicyRecord(
        tool_id="actions.list_logs",
        method="GET",
        path="/api/v1/databases/{db_name}/actions/logs",
        risk_level="read",
        requires_approval=False,
        requires_idempotency_key=False,
        status="ACTIVE",
        roles=["Owner"],
        max_payload_bytes=200000,
        created_at=now,
        updated_at=now,
    )

    session_id = "11111111-1111-1111-1111-111111111111"
    session = {
        "session_id": session_id,
        "tenant_id": "tenant-1",
        "created_by": "user-1",
        "status": "ACTIVE",
        "selected_model": None,
        "enabled_tools": [],
        "summary": None,
        "metadata": {"tools_restricted": True},
        "started_at": now,
        "terminated_at": None,
        "created_at": now,
        "updated_at": now,
    }
    tenant_policy = {
        "tenant_id": "tenant-1",
        "allowed_tools": ["actions.list_logs"],
        "allowed_models": [],
        "default_model": None,
        "auto_approve_rules": {},
        "data_policies": {"allowed_db_names": ["allowed-db"]},
        "created_at": now,
        "updated_at": now,
    }

    with _set_env(
        BFF_REQUIRE_AUTH="true",
        BFF_AGENT_TOKEN="agent-secret",
        USER_JWT_ENABLED="true",
        USER_JWT_HS256_SECRET="jwt-secret",
    ):
        app = FastAPI()
        app.state.bff_container = StubContainer(
            tool_registry=StubToolRegistry({"actions.list_logs": tool_policy}),
            session_registry=StubSessionRegistry(session),
            policy_registry=StubPolicyRegistry(tenant_policy),
        )
        install_bff_auth_middleware(app)

        @app.get("/api/v1/databases/{db_name}/actions/logs")
        async def list_logs(db_name: str):  # noqa: ANN001
            return {"db_name": db_name}

        client = TestClient(app)

        # Session tool restriction: tool not enabled for session => denied.
        resp = client.get(
            "/api/v1/databases/allowed-db/actions/logs",
            headers={
                "X-Admin-Token": "agent-secret",
                "X-Delegated-Authorization": f"Bearer {user_token}",
                "X-Agent-Tool-ID": "actions.list_logs",
                "X-Agent-Tool-Run-ID": "22222222-2222-2222-2222-222222222222",
                "X-Agent-Session-ID": session_id,
            },
        )
        assert resp.status_code == 403

        # Enable tool for the session and retry => allowed.
        session["enabled_tools"] = ["actions.list_logs"]
        resp = client.get(
            "/api/v1/databases/allowed-db/actions/logs",
            headers={
                "X-Admin-Token": "agent-secret",
                "X-Delegated-Authorization": f"Bearer {user_token}",
                "X-Agent-Tool-ID": "actions.list_logs",
                "X-Agent-Tool-Run-ID": "33333333-3333-3333-3333-333333333333",
                "X-Agent-Session-ID": session_id,
            },
        )
        assert resp.status_code == 200

        # ABAC: db_name not in allowed list => denied.
        resp = client.get(
            "/api/v1/databases/forbidden-db/actions/logs",
            headers={
                "X-Admin-Token": "agent-secret",
                "X-Delegated-Authorization": f"Bearer {user_token}",
                "X-Agent-Tool-ID": "actions.list_logs",
                "X-Agent-Tool-Run-ID": "44444444-4444-4444-4444-444444444444",
                "X-Agent-Session-ID": session_id,
            },
        )
        assert resp.status_code == 403


@pytest.mark.unit
def test_bff_agent_tool_idempotency_replays_without_reexecution():
    user_token = jwt.encode(
        {"sub": "user-1", "roles": ["Owner"], "tenant_id": "tenant-1"},
        "jwt-secret",
        algorithm="HS256",
    )

    class StubToolRegistry:
        def __init__(self, policy):  # noqa: ANN001
            self._policy = policy

        async def get_tool_policy(self, *, tool_id: str):  # noqa: ANN001
            return self._policy.get(tool_id)

    class StubAgentRegistry:
        def __init__(self):  # noqa: D107
            self._store = {}

        async def get_tool_idempotency(self, *, tenant_id: str, idempotency_key: str):  # noqa: ANN001
            return self._store.get((tenant_id, idempotency_key))

        async def begin_tool_idempotency(  # noqa: ANN001
            self,
            *,
            tenant_id: str,
            idempotency_key: str,
            tool_id: str,
            request_digest: str,
            started_at=None,
        ):
            key = (tenant_id, idempotency_key)
            existing = self._store.get(key)
            if existing:
                return existing, False
            record = SimpleNamespace(
                tenant_id=tenant_id,
                idempotency_key=idempotency_key,
                tool_id=tool_id,
                request_digest=request_digest,
                status="IN_PROGRESS",
                response_status=None,
                response_body=None,
                error=None,
            )
            self._store[key] = record
            return record, True

        async def finalize_tool_idempotency(  # noqa: ANN001
            self,
            *,
            tenant_id: str,
            idempotency_key: str,
            tool_id: str,
            request_digest: str,
            response_status,
            response_body,
            error=None,
            finished_at=None,
        ):
            key = (tenant_id, idempotency_key)
            record = self._store.get(key)
            if not record or record.tool_id != tool_id or record.request_digest != request_digest:
                return None
            record.status = "COMPLETED"
            record.response_status = response_status
            record.response_body = response_body
            record.error = error
            return record

    class StubContainer:
        def __init__(self, *, tool_registry, agent_registry):  # noqa: ANN001
            self._tool_registry = tool_registry
            self._agent_registry = agent_registry

        def get_agent_tool_registry(self):  # noqa: ANN001
            return self._tool_registry

        def get_agent_registry(self):  # noqa: ANN001
            return self._agent_registry

    now = datetime.now(timezone.utc)
    policy = AgentToolPolicyRecord(
        tool_id="pipelines.build",
        method="POST",
        path="/api/v1/pipelines/{pipeline_id}/build",
        risk_level="write",
        requires_approval=True,
        requires_idempotency_key=True,
        status="ACTIVE",
        roles=["Owner"],
        max_payload_bytes=200000,
        created_at=now,
        updated_at=now,
    )

    with _set_env(
        BFF_REQUIRE_AUTH="true",
        BFF_AGENT_TOKEN="agent-secret",
        USER_JWT_ENABLED="true",
        USER_JWT_HS256_SECRET="jwt-secret",
    ):
        app = FastAPI()
        agent_registry = StubAgentRegistry()
        app.state.bff_container = StubContainer(
            tool_registry=StubToolRegistry({"pipelines.build": policy}),
            agent_registry=agent_registry,
        )
        install_bff_auth_middleware(app)

        counter = {"calls": 0}

        @app.post("/api/v1/pipelines/{pipeline_id}/build")
        async def build(pipeline_id: str):  # noqa: ANN001
            counter["calls"] += 1
            return {"pipeline_id": pipeline_id, "calls": counter["calls"]}

        client = TestClient(app)

        headers = {
            "X-Admin-Token": "agent-secret",
            "X-Delegated-Authorization": f"Bearer {user_token}",
            "X-Agent-Tool-ID": "pipelines.build",
            "X-Agent-Tool-Run-ID": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "Idempotency-Key": "unit-test-idem-1",
        }

        resp1 = client.post("/api/v1/pipelines/123/build", headers=headers, json={"x": 1})
        assert resp1.status_code == 200
        assert resp1.json()["calls"] == 1
        assert counter["calls"] == 1

        # Same idempotency key => replay (no re-execution).
        resp2 = client.post("/api/v1/pipelines/123/build", headers=headers, json={"x": 1})
        assert resp2.status_code == 200
        assert resp2.json()["calls"] == 1
        assert counter["calls"] == 1

        # Same idempotency key with different request => conflict.
        resp3 = client.post("/api/v1/pipelines/123/build", headers=headers, json={"x": 2})
        assert resp3.status_code == 409
        assert counter["calls"] == 1
