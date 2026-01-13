from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from bff.main import app
from bff.routers import agent_plans as agent_plans_router
from bff.services.agent_plan_compiler import AgentPlanCompileResult
from shared.dependencies.providers import get_audit_log_store, get_llm_gateway, get_redis_service
from shared.models.agent_plan import AgentPlan, AgentPlanStep
from shared.services.agent_plan_registry import AgentPlanRecord
from shared.services.agent_registry import AgentApprovalRecord
from shared.services.agent_tool_registry import AgentToolPolicyRecord


@dataclass
class _FakeAuditStore:
    async def log(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return None


class _FakePlanRegistry:
    def __init__(self) -> None:
        self.plans: Dict[str, AgentPlanRecord] = {}
        self.upserts: list[dict[str, Any]] = []

    async def upsert_plan(self, **kwargs):  # noqa: ANN003
        self.upserts.append(dict(kwargs))
        plan_id = str(kwargs["plan_id"])
        now = datetime.now(timezone.utc)
        record = AgentPlanRecord(
            plan_id=plan_id,
            status=str(kwargs.get("status") or "COMPILED"),
            goal=str(kwargs.get("goal") or ""),
            risk_level=str(kwargs.get("risk_level") or "read"),
            requires_approval=bool(kwargs.get("requires_approval")),
            plan=dict(kwargs.get("plan") or {}),
            plan_digest="sha256:test",
            created_by=kwargs.get("created_by"),
            created_at=now,
            updated_at=now,
        )
        self.plans[plan_id] = record
        return record

    async def get_plan(self, *, plan_id: str) -> Optional[AgentPlanRecord]:
        return self.plans.get(str(plan_id))


class _FakeAgentRegistry:
    def __init__(self) -> None:
        self.approvals: Dict[str, list[AgentApprovalRecord]] = {}

    async def list_approvals(self, *, plan_id: str):
        return list(self.approvals.get(str(plan_id), []))


class _FakeToolRegistry:
    def __init__(self, policies: dict[str, AgentToolPolicyRecord]) -> None:
        self._policies = dict(policies)

    async def get_tool_policy(self, *, tool_id: str):
        return self._policies.get(tool_id)

    async def list_tool_policies(self, **kwargs):  # noqa: ANN003
        return list(self._policies.values())


def _policy(*, tool_id: str, method: str, path: str, risk_level: str = "write") -> AgentToolPolicyRecord:
    now = datetime.now(timezone.utc)
    return AgentToolPolicyRecord(
        tool_id=tool_id,
        method=method,
        path=path,
        risk_level=risk_level,
        requires_approval=risk_level != "read",
        requires_idempotency_key=risk_level != "read",
        status="ACTIVE",
        roles=[],
        max_payload_bytes=None,
        created_at=now,
        updated_at=now,
    )


@pytest.fixture
def client():
    return TestClient(app)


def _install_common_overrides(*, plan_registry, tool_registry, agent_registry=None):
    fake_redis = AsyncMock()
    fake_redis.get_json.return_value = None
    fake_redis.set_json.return_value = None

    app.dependency_overrides[get_llm_gateway] = lambda: AsyncMock()
    app.dependency_overrides[get_redis_service] = lambda: fake_redis
    app.dependency_overrides[get_audit_log_store] = lambda: _FakeAuditStore()

    app.dependency_overrides[agent_plans_router.get_agent_plan_registry] = lambda: plan_registry
    app.dependency_overrides[agent_plans_router.get_agent_tool_registry] = lambda: tool_registry
    if agent_registry is not None:
        app.dependency_overrides[agent_plans_router.get_agent_registry] = lambda: agent_registry


def test_compile_stores_plan_and_returns_plan_id(client):
    plan_id = str(uuid4())
    plan = AgentPlan(
        plan_id=plan_id,
        goal="simulate then submit",
        requires_approval=True,
        steps=[
            AgentPlanStep(step_id="s1", tool_id="actions.simulate", path_params={"db_name": "demo", "action_type_id": "A"}, idempotency_key="k1"),
        ],
    )

    plan_registry = _FakePlanRegistry()
    tool_registry = _FakeToolRegistry({})
    _install_common_overrides(plan_registry=plan_registry, tool_registry=tool_registry)

    with patch(
        "bff.routers.agent_plans.compile_agent_plan",
        new=AsyncMock(
            return_value=AgentPlanCompileResult(
                status="success",
                plan_id=plan_id,
                plan=plan,
                validation_errors=[],
                validation_warnings=[],
                questions=[],
                llm_meta=None,
                planner_confidence=0.9,
                planner_notes=["ok"],
            )
        ),
    ):
        try:
            res = client.post("/api/v1/agent-plans/compile", json={"goal": "do it"})
        finally:
            app.dependency_overrides.clear()

    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "success"
    assert body["data"]["plan_id"] == plan_id
    assert plan_registry.upserts and plan_registry.upserts[0]["plan_id"] == plan_id


def test_preview_stops_before_submit(client):
    plan_id = str(uuid4())
    plan_registry = _FakePlanRegistry()
    tool_registry = _FakeToolRegistry(
        {
            "actions.simulate": _policy(
                tool_id="actions.simulate",
                method="POST",
                path="/api/v1/databases/{db_name}/actions/{action_type_id}/simulate",
            ),
            "actions.submit": _policy(
                tool_id="actions.submit",
                method="POST",
                path="/api/v1/databases/{db_name}/actions/{action_type_id}/submit",
            ),
        }
    )
    plan = AgentPlan(
        plan_id=plan_id,
        goal="simulate then submit",
        requires_approval=True,
        steps=[
            AgentPlanStep(step_id="s1", tool_id="actions.simulate", path_params={"db_name": "demo", "action_type_id": "A"}, idempotency_key="k1"),
            AgentPlanStep(step_id="s2", tool_id="actions.submit", path_params={"db_name": "demo", "action_type_id": "A"}, idempotency_key="k2"),
        ],
    )
    now = datetime.now(timezone.utc)
    plan_registry.plans[plan_id] = AgentPlanRecord(
        plan_id=plan_id,
        status="COMPILED",
        goal=plan.goal,
        risk_level="write",
        requires_approval=True,
        plan=plan.model_dump(mode="json"),
        plan_digest="sha256:test",
        created_by="user:test",
        created_at=now,
        updated_at=now,
    )

    captured: dict[str, Any] = {}

    async def _fake_call_agent_create_run(*, request, payload):  # noqa: ANN001, ANN003
        captured["payload"] = payload
        return {"status": "accepted", "data": {"run_id": "run-1"}}

    _install_common_overrides(plan_registry=plan_registry, tool_registry=tool_registry)
    with patch("bff.routers.agent_plans._call_agent_create_run", new=_fake_call_agent_create_run):
        try:
            res = client.post(f"/api/v1/agent-plans/{plan_id}/preview")
        finally:
            app.dependency_overrides.clear()

    assert res.status_code == 202
    steps = captured["payload"]["steps"]
    assert len(steps) == 1
    assert steps[0]["path"].endswith("/simulate")


def test_execute_requires_approval(client):
    plan_id = str(uuid4())
    plan_registry = _FakePlanRegistry()
    agent_registry = _FakeAgentRegistry()
    tool_registry = _FakeToolRegistry(
        {
            "actions.simulate": _policy(
                tool_id="actions.simulate",
                method="POST",
                path="/api/v1/databases/{db_name}/actions/{action_type_id}/simulate",
            ),
            "actions.submit": _policy(
                tool_id="actions.submit",
                method="POST",
                path="/api/v1/databases/{db_name}/actions/{action_type_id}/submit",
            )
        }
    )
    plan = AgentPlan(
        plan_id=plan_id,
        goal="submit only",
        steps=[
            AgentPlanStep(step_id="s1", tool_id="actions.simulate", path_params={"db_name": "demo", "action_type_id": "A"}, idempotency_key="k0"),
            AgentPlanStep(step_id="s2", tool_id="actions.submit", path_params={"db_name": "demo", "action_type_id": "A"}, idempotency_key="k1"),
        ],
    )
    now = datetime.now(timezone.utc)
    plan_registry.plans[plan_id] = AgentPlanRecord(
        plan_id=plan_id,
        status="COMPILED",
        goal=plan.goal,
        risk_level="write",
        requires_approval=True,
        plan=plan.model_dump(mode="json"),
        plan_digest="sha256:test",
        created_by="user:test",
        created_at=now,
        updated_at=now,
    )

    _install_common_overrides(plan_registry=plan_registry, tool_registry=tool_registry, agent_registry=agent_registry)
    try:
        res = client.post(f"/api/v1/agent-plans/{plan_id}/execute")
    finally:
        app.dependency_overrides.clear()

    assert res.status_code == 403


def test_execute_calls_agent_when_approved(client):
    plan_id = str(uuid4())
    plan_registry = _FakePlanRegistry()
    agent_registry = _FakeAgentRegistry()
    tool_registry = _FakeToolRegistry(
        {
            "actions.simulate": _policy(
                tool_id="actions.simulate",
                method="POST",
                path="/api/v1/databases/{db_name}/actions/{action_type_id}/simulate",
            ),
            "actions.submit": _policy(
                tool_id="actions.submit",
                method="POST",
                path="/api/v1/databases/{db_name}/actions/{action_type_id}/submit",
            ),
        }
    )
    plan = AgentPlan(
        plan_id=plan_id,
        goal="simulate then submit",
        requires_approval=True,
        steps=[
            AgentPlanStep(step_id="s1", tool_id="actions.simulate", path_params={"db_name": "demo", "action_type_id": "A"}, idempotency_key="k1"),
            AgentPlanStep(step_id="s2", tool_id="actions.submit", path_params={"db_name": "demo", "action_type_id": "A"}, idempotency_key="k2"),
        ],
    )
    now = datetime.now(timezone.utc)
    plan_registry.plans[plan_id] = AgentPlanRecord(
        plan_id=plan_id,
        status="COMPILED",
        goal=plan.goal,
        risk_level="write",
        requires_approval=True,
        plan=plan.model_dump(mode="json"),
        plan_digest="sha256:test",
        created_by="user:test",
        created_at=now,
        updated_at=now,
    )
    agent_registry.approvals[plan_id] = [
        AgentApprovalRecord(
            approval_id=str(uuid4()),
            plan_id=plan_id,
            step_id=None,
            decision="APPROVED",
            approved_by="user:test",
            approved_at=now,
            comment=None,
            metadata={},
            created_at=now,
        )
    ]

    captured: dict[str, Any] = {}

    async def _fake_call_agent_create_run(*, request, payload):  # noqa: ANN001, ANN003
        captured["payload"] = payload
        return {"status": "accepted", "data": {"run_id": "run-2"}}

    _install_common_overrides(plan_registry=plan_registry, tool_registry=tool_registry, agent_registry=agent_registry)
    with patch("bff.routers.agent_plans._call_agent_create_run", new=_fake_call_agent_create_run):
        try:
            res = client.post(f"/api/v1/agent-plans/{plan_id}/execute")
        finally:
            app.dependency_overrides.clear()

    assert res.status_code == 202
    steps = captured["payload"]["steps"]
    assert len(steps) == 2
    assert steps[0]["path"].endswith("/simulate")
    assert steps[1]["path"].endswith("/submit")
