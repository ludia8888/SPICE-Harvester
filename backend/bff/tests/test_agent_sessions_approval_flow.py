from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException, status

from bff.routers import agent_sessions as agent_sessions_router
from bff.routers.agent_sessions import (
    AgentSessionApprovalDecisionRequest,
    AgentSessionCreateRequest,
    AgentSessionMessageRequest,
    create_session,
    decide_approval,
    list_approvals,
    post_message,
)
from bff.services.agent_plan_compiler import AgentPlanCompileResult
from shared.models.agent_plan import AgentPlan, AgentPlanStep
from shared.security.user_context import UserPrincipal
from shared.services.agent_plan_registry import AgentPlanRecord
from shared.services.agent_policy_registry import AgentTenantPolicyRecord
from shared.services.agent_registry import AgentApprovalRecord, AgentApprovalRequestRecord
from shared.services.agent_session_registry import AgentSessionJobRecord, AgentSessionMessageRecord, AgentSessionRecord
from shared.services.agent_tool_registry import AgentToolPolicyRecord


class _Request:
    def __init__(self, *, principal: UserPrincipal) -> None:
        self.headers: dict[str, str] = {}
        self.state = SimpleNamespace(user=principal)


def _principal(*, user_id: str = "user-1", tenant_id: str = "tenant-1") -> UserPrincipal:
    return UserPrincipal(id=user_id, tenant_id=tenant_id, verified=True)


class _FakeSessions:
    def __init__(self) -> None:
        self.sessions: dict[str, AgentSessionRecord] = {}
        self.jobs: dict[str, AgentSessionJobRecord] = {}
        self.messages: dict[str, list[AgentSessionMessageRecord]] = {}

    async def create_session(self, **kwargs) -> AgentSessionRecord:  # type: ignore[no-untyped-def]
        now = datetime.now(timezone.utc)
        record = AgentSessionRecord(
            session_id=str(kwargs["session_id"]),
            tenant_id=str(kwargs["tenant_id"]),
            created_by=str(kwargs["created_by"]),
            status=str(kwargs.get("status") or "ACTIVE"),
            selected_model=kwargs.get("selected_model"),
            enabled_tools=list(kwargs.get("enabled_tools") or []),
            summary=None,
            metadata=dict(kwargs.get("metadata") or {}),
            started_at=kwargs.get("started_at") or now,
            terminated_at=None,
            created_at=now,
            updated_at=now,
        )
        self.sessions[record.session_id] = record
        return record

    async def get_session(self, *, session_id: str, tenant_id: str):  # type: ignore[no-untyped-def]
        record = self.sessions.get(session_id)
        if not record or record.tenant_id != tenant_id:
            return None
        return record

    async def update_session(self, *, session_id: str, tenant_id: str, **kwargs):  # type: ignore[no-untyped-def]
        record = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not record:
            return None
        updated = replace(
            record,
            status=str(kwargs.get("status") or record.status),
            updated_at=datetime.now(timezone.utc),
        )
        self.sessions[session_id] = updated
        return updated

    async def add_message(self, **kwargs) -> AgentSessionMessageRecord:  # type: ignore[no-untyped-def]
        session = await self.get_session(session_id=str(kwargs["session_id"]), tenant_id=str(kwargs["tenant_id"]))
        if not session:
            raise ValueError("session not found")
        now = datetime.now(timezone.utc)
        record = AgentSessionMessageRecord(
            message_id=str(kwargs["message_id"]),
            session_id=str(kwargs["session_id"]),
            role=str(kwargs["role"]),
            content=str(kwargs["content"]),
            content_digest=kwargs.get("content_digest"),
            is_removed=False,
            removed_at=None,
            removed_by=None,
            removed_reason=None,
            token_count=None,
            cost_estimate=None,
            latency_ms=None,
            metadata=dict(kwargs.get("metadata") or {}),
            created_at=kwargs.get("created_at") or now,
        )
        self.messages.setdefault(record.session_id, []).append(record)
        return record

    async def list_context_items(self, *, session_id: str, tenant_id: str, limit: int, offset: int):  # type: ignore[no-untyped-def]
        session = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not session:
            raise ValueError("session not found")
        return []

    async def create_job(self, **kwargs) -> AgentSessionJobRecord:  # type: ignore[no-untyped-def]
        session = await self.get_session(session_id=str(kwargs["session_id"]), tenant_id=str(kwargs["tenant_id"]))
        if not session:
            raise ValueError("session not found")
        now = datetime.now(timezone.utc)
        record = AgentSessionJobRecord(
            job_id=str(kwargs["job_id"]),
            session_id=str(kwargs["session_id"]),
            plan_id=str(kwargs.get("plan_id")) if kwargs.get("plan_id") else None,
            run_id=str(kwargs.get("run_id")) if kwargs.get("run_id") else None,
            status=str(kwargs.get("status") or "PENDING"),
            error=kwargs.get("error"),
            metadata=dict(kwargs.get("metadata") or {}),
            created_at=now,
            updated_at=now,
            finished_at=None,
        )
        self.jobs[record.job_id] = record
        return record

    async def update_job(self, *, job_id: str, tenant_id: str, **kwargs):  # type: ignore[no-untyped-def]
        record = self.jobs.get(job_id)
        if not record:
            return None
        session = await self.get_session(session_id=record.session_id, tenant_id=tenant_id)
        if not session:
            return None
        updated = replace(
            record,
            status=str(kwargs.get("status") or record.status),
            run_id=str(kwargs.get("run_id")) if kwargs.get("run_id") else record.run_id,
            error=kwargs.get("error") if kwargs.get("error") is not None else record.error,
            updated_at=datetime.now(timezone.utc),
        )
        self.jobs[job_id] = updated
        return updated

    async def get_job(self, *, job_id: str, tenant_id: str):  # type: ignore[no-untyped-def]
        record = self.jobs.get(job_id)
        if not record:
            return None
        session = await self.get_session(session_id=record.session_id, tenant_id=tenant_id)
        if not session:
            return None
        return record


class _FakePlanRegistry:
    def __init__(self) -> None:
        self.plans: dict[str, AgentPlanRecord] = {}

    async def upsert_plan(self, **kwargs):  # noqa: ANN003
        plan_id = str(kwargs["plan_id"])
        tenant_id = str(kwargs.get("tenant_id") or "default")
        now = datetime.now(timezone.utc)
        record = AgentPlanRecord(
            plan_id=plan_id,
            tenant_id=tenant_id,
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

    async def get_plan(self, *, plan_id: str, tenant_id: str):  # noqa: ANN001
        record = self.plans.get(str(plan_id))
        if not record or record.tenant_id != tenant_id:
            return None
        return record


class _FakeToolRegistry:
    def __init__(self, policy: AgentToolPolicyRecord) -> None:
        self._policy = policy

    async def get_tool_policy(self, *, tool_id: str):  # noqa: ANN001
        return self._policy if tool_id == self._policy.tool_id else None

    async def list_tool_policies(self, **kwargs):  # noqa: ANN003
        return [self._policy]


class _FakeAgentRegistry:
    def __init__(self) -> None:
        self.requests: dict[str, AgentApprovalRequestRecord] = {}
        self.approvals: list[AgentApprovalRecord] = []

    async def create_approval_request(self, **kwargs) -> AgentApprovalRequestRecord:  # type: ignore[no-untyped-def]
        now = datetime.now(timezone.utc)
        record = AgentApprovalRequestRecord(
            approval_request_id=str(kwargs["approval_request_id"]),
            plan_id=str(kwargs["plan_id"]),
            tenant_id=str(kwargs["tenant_id"]),
            session_id=str(kwargs.get("session_id")) if kwargs.get("session_id") else None,
            job_id=str(kwargs.get("job_id")) if kwargs.get("job_id") else None,
            status=str(kwargs.get("status") or "PENDING"),
            risk_level=str(kwargs.get("risk_level") or "write"),
            requested_by=str(kwargs.get("requested_by") or "unknown"),
            requested_at=kwargs.get("requested_at") or now,
            decision=None,
            decided_by=None,
            decided_at=None,
            comment=None,
            request_payload=dict(kwargs.get("request_payload") or {}),
            metadata=dict(kwargs.get("metadata") or {}),
            created_at=now,
            updated_at=now,
        )
        self.requests[record.approval_request_id] = record
        return record

    async def list_approval_requests(self, *, tenant_id: str, session_id: str, limit: int, offset: int):  # noqa: ANN001
        items = [
            r
            for r in self.requests.values()
            if r.tenant_id == tenant_id and r.session_id == session_id
        ]
        return items[offset : offset + limit]

    async def get_approval_request(self, *, approval_request_id: str, tenant_id: str):  # noqa: ANN001
        record = self.requests.get(str(approval_request_id))
        if not record or record.tenant_id != tenant_id:
            return None
        return record

    async def decide_approval_request(self, **kwargs):  # noqa: ANN003
        record = await self.get_approval_request(
            approval_request_id=str(kwargs["approval_request_id"]),
            tenant_id=str(kwargs["tenant_id"]),
        )
        if not record:
            return None
        updated = replace(
            record,
            decision=str(kwargs.get("decision") or record.decision),
            decided_by=str(kwargs.get("decided_by") or record.decided_by),
            decided_at=kwargs.get("decided_at") or record.decided_at,
            comment=kwargs.get("comment") if kwargs.get("comment") is not None else record.comment,
            status=str(kwargs.get("status") or record.status),
            metadata=dict(kwargs.get("metadata") or record.metadata),
            updated_at=datetime.now(timezone.utc),
        )
        self.requests[record.approval_request_id] = updated
        return updated

    async def create_approval(self, **kwargs) -> AgentApprovalRecord:  # type: ignore[no-untyped-def]
        now = datetime.now(timezone.utc)
        record = AgentApprovalRecord(
            approval_id=str(kwargs["approval_id"]),
            plan_id=str(kwargs["plan_id"]),
            tenant_id=str(kwargs["tenant_id"]),
            step_id=kwargs.get("step_id"),
            decision=str(kwargs.get("decision") or ""),
            approved_by=str(kwargs.get("approved_by") or "unknown"),
            approved_at=kwargs.get("approved_at") or now,
            comment=kwargs.get("comment"),
            metadata=dict(kwargs.get("metadata") or {}),
            created_at=now,
        )
        self.approvals.append(record)
        return record


class _FakePolicyRegistry:
    def __init__(self, policy: AgentTenantPolicyRecord | None) -> None:
        self.policy = policy

    async def get_tenant_policy(self, *, tenant_id: str):  # noqa: ANN001
        return self.policy


def _policy(*, tool_id: str = "test.write") -> AgentToolPolicyRecord:
    now = datetime.now(timezone.utc)
    return AgentToolPolicyRecord(
        tool_id=tool_id,
        method="POST",
        path="/api/v1/test/write",
        risk_level="write",
        requires_approval=True,
        requires_idempotency_key=True,
        status="ACTIVE",
        roles=[],
        max_payload_bytes=None,
        created_at=now,
        updated_at=now,
    )


def _plan_requires_approval(*, plan_id: str, tool_id: str = "test.write") -> AgentPlan:
    return AgentPlan(
        plan_id=plan_id,
        goal="write something",
        risk_level="write",
        requires_approval=True,
        steps=[
            AgentPlanStep(
                step_id="s1",
                tool_id=tool_id,
                method="POST",
                path_params={},
                body={"x": 1},
                idempotency_key="k1",
            )
        ],
    )


@pytest.mark.asyncio
async def test_session_approval_request_and_resume(monkeypatch: pytest.MonkeyPatch) -> None:
    sessions = _FakeSessions()
    plans = _FakePlanRegistry()
    tools = _FakeToolRegistry(_policy())
    agent_registry = _FakeAgentRegistry()
    policy_registry = _FakePolicyRegistry(policy=None)
    req = _Request(principal=_principal())

    created = await create_session(
        AgentSessionCreateRequest(),
        request=req,
        sessions=sessions,
        policy_registry=policy_registry,
        tool_registry=tools,
    )
    session_id = created.data["session"]["session_id"]

    plan_id = str(uuid4())
    plan = _plan_requires_approval(plan_id=plan_id)

    async def _fake_compile_agent_plan(**kwargs):  # type: ignore[no-untyped-def]
        return AgentPlanCompileResult(
            status="success",
            plan_id=plan_id,
            plan=plan,
            validation_errors=[],
            validation_warnings=[],
            questions=[],
        )

    monkeypatch.setattr(agent_sessions_router, "compile_agent_plan", _fake_compile_agent_plan)

    response = await post_message(
        session_id=session_id,
        body=AgentSessionMessageRequest(content="do write", execute=True),
        request=req,
        llm=None,  # type: ignore[arg-type]
        redis_service=None,  # type: ignore[arg-type]
        audit_store=None,  # type: ignore[arg-type]
        sessions=sessions,
        policy_registry=policy_registry,
        tool_registry=tools,
        plan_registry=plans,
        agent_registry=agent_registry,
    )
    assert response.status == "accepted"
    assert response.data["status"] == "WAITING_APPROVAL"
    assert response.data["approval_request_id"]

    approvals = await list_approvals(
        session_id=session_id,
        request=req,
        sessions=sessions,
        agent_registry=agent_registry,
        limit=50,
        offset=0,
    )
    assert approvals.status == "success"
    assert approvals.data["count"] == 1
    approval_request_id = approvals.data["approvals"][0]["approval_request_id"]
    job_id = approvals.data["approvals"][0]["job_id"]

    async def _fake_call_agent_create_run(*, request, payload):  # noqa: ANN001, ANN003
        return {"status": "accepted", "data": {"run_id": "run-123"}}

    monkeypatch.setattr(agent_sessions_router, "_call_agent_create_run", _fake_call_agent_create_run)

    resumed = await decide_approval(
        session_id=session_id,
        approval_request_id=approval_request_id,
        body=AgentSessionApprovalDecisionRequest(decision="APPROVE"),
        request=req,
        sessions=sessions,
        tool_registry=tools,
        plan_registry=plans,
        agent_registry=agent_registry,
    )
    assert resumed.status == "accepted"
    assert resumed.data["job_id"] == job_id
    assert resumed.data["run_id"] == "run-123"

    job = await sessions.get_job(job_id=job_id, tenant_id=_principal().tenant_id)
    assert job is not None
    assert job.status == "RUNNING"


@pytest.mark.asyncio
async def test_session_auto_approve_policy_starts_job(monkeypatch: pytest.MonkeyPatch) -> None:
    sessions = _FakeSessions()
    plans = _FakePlanRegistry()
    tools = _FakeToolRegistry(_policy())
    agent_registry = _FakeAgentRegistry()
    policy = AgentTenantPolicyRecord(
        tenant_id="tenant-1",
        allowed_models=[],
        allowed_tools=[],
        default_model=None,
        auto_approve_rules={"enabled": True, "allow_risk_levels": ["write"], "allow_tool_ids": ["test.write"]},
        data_policies={},
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    policy_registry = _FakePolicyRegistry(policy=policy)
    req = _Request(principal=_principal())

    created = await create_session(
        AgentSessionCreateRequest(),
        request=req,
        sessions=sessions,
        policy_registry=policy_registry,
        tool_registry=tools,
    )
    session_id = created.data["session"]["session_id"]

    plan_id = str(uuid4())
    plan = _plan_requires_approval(plan_id=plan_id)

    async def _fake_compile_agent_plan(**kwargs):  # type: ignore[no-untyped-def]
        return AgentPlanCompileResult(
            status="success",
            plan_id=plan_id,
            plan=plan,
            validation_errors=[],
            validation_warnings=[],
            questions=[],
        )

    async def _fake_call_agent_create_run(*, request, payload):  # noqa: ANN001, ANN003
        return {"status": "accepted", "data": {"run_id": "run-auto"}}

    monkeypatch.setattr(agent_sessions_router, "compile_agent_plan", _fake_compile_agent_plan)
    monkeypatch.setattr(agent_sessions_router, "_call_agent_create_run", _fake_call_agent_create_run)

    response = await post_message(
        session_id=session_id,
        body=AgentSessionMessageRequest(content="do write", execute=True),
        request=req,
        llm=None,  # type: ignore[arg-type]
        redis_service=None,  # type: ignore[arg-type]
        audit_store=None,  # type: ignore[arg-type]
        sessions=sessions,
        policy_registry=policy_registry,
        tool_registry=tools,
        plan_registry=plans,
        agent_registry=agent_registry,
    )
    assert response.status == "accepted"
    assert response.data["auto_approved"] is True
    assert response.data["run_id"] == "run-auto"


@pytest.mark.asyncio
async def test_session_approval_reject_marks_job(monkeypatch: pytest.MonkeyPatch) -> None:
    sessions = _FakeSessions()
    plans = _FakePlanRegistry()
    tools = _FakeToolRegistry(_policy())
    agent_registry = _FakeAgentRegistry()
    policy_registry = _FakePolicyRegistry(policy=None)
    req = _Request(principal=_principal())

    created = await create_session(
        AgentSessionCreateRequest(),
        request=req,
        sessions=sessions,
        policy_registry=policy_registry,
        tool_registry=tools,
    )
    session_id = created.data["session"]["session_id"]

    plan_id = str(uuid4())
    plan = _plan_requires_approval(plan_id=plan_id)

    async def _fake_compile_agent_plan(**kwargs):  # type: ignore[no-untyped-def]
        return AgentPlanCompileResult(
            status="success",
            plan_id=plan_id,
            plan=plan,
            validation_errors=[],
            validation_warnings=[],
            questions=[],
        )

    monkeypatch.setattr(agent_sessions_router, "compile_agent_plan", _fake_compile_agent_plan)

    response = await post_message(
        session_id=session_id,
        body=AgentSessionMessageRequest(content="do write", execute=True),
        request=req,
        llm=None,  # type: ignore[arg-type]
        redis_service=None,  # type: ignore[arg-type]
        audit_store=None,  # type: ignore[arg-type]
        sessions=sessions,
        policy_registry=policy_registry,
        tool_registry=tools,
        plan_registry=plans,
        agent_registry=agent_registry,
    )
    approval_request_id = response.data["approval_request_id"]
    job_id = response.data["job_id"]

    rejected = await decide_approval(
        session_id=session_id,
        approval_request_id=approval_request_id,
        body=AgentSessionApprovalDecisionRequest(decision="REJECT"),
        request=req,
        sessions=sessions,
        tool_registry=tools,
        plan_registry=plans,
        agent_registry=agent_registry,
    )
    assert rejected.status == "success"

    job = await sessions.get_job(job_id=job_id, tenant_id=_principal().tenant_id)
    assert job is not None
    assert job.status == "REJECTED"


@pytest.mark.asyncio
async def test_decide_approval_requires_uuid() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await decide_approval(
            session_id="not-a-uuid",
            approval_request_id="also-bad",
            body=AgentSessionApprovalDecisionRequest(decision="APPROVE"),
            request=_Request(principal=_principal()),
            sessions=_FakeSessions(),
            tool_registry=_FakeToolRegistry(_policy()),
            plan_registry=_FakePlanRegistry(),
            agent_registry=_FakeAgentRegistry(),
        )
    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
