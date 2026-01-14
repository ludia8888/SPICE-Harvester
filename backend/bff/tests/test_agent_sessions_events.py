from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from bff.routers.agent_sessions import list_events
from shared.security.user_context import UserPrincipal
from shared.services.agent_registry import AgentApprovalRequestRecord
from shared.services.agent_session_registry import AgentSessionJobRecord, AgentSessionMessageRecord, AgentSessionRecord


class _Request:
    def __init__(self, *, principal: UserPrincipal) -> None:
        self.headers: dict[str, str] = {}
        self.state = SimpleNamespace(user=principal)


def _principal(*, user_id: str = "user-1", tenant_id: str = "tenant-1") -> UserPrincipal:
    return UserPrincipal(id=user_id, tenant_id=tenant_id, verified=True)


class _FakeSessions:
    def __init__(self, session: AgentSessionRecord) -> None:
        self.session = session
        self.messages: list[AgentSessionMessageRecord] = []
        self.jobs: list[AgentSessionJobRecord] = []

    async def get_session(self, *, session_id: str, tenant_id: str):  # noqa: ANN001
        if session_id == self.session.session_id and tenant_id == self.session.tenant_id:
            return self.session
        return None

    async def list_messages(self, *, session_id: str, tenant_id: str, limit: int, offset: int, include_removed: bool):  # noqa: ANN001
        _ = include_removed
        if not await self.get_session(session_id=session_id, tenant_id=tenant_id):
            raise ValueError("session not found")
        return self.messages[offset : offset + limit]

    async def list_jobs(self, *, session_id: str, tenant_id: str, limit: int, offset: int):  # noqa: ANN001
        if not await self.get_session(session_id=session_id, tenant_id=tenant_id):
            raise ValueError("session not found")
        return self.jobs[offset : offset + limit]

    async def update_job(self, *, job_id: str, tenant_id: str, **kwargs):  # noqa: ANN001, ANN003
        for idx, job in enumerate(self.jobs):
            if job.job_id != job_id:
                continue
            if job.session_id != self.session.session_id or tenant_id != self.session.tenant_id:
                return None
            self.jobs[idx] = replace(
                job,
                status=str(kwargs.get("status") or job.status),
                finished_at=kwargs.get("finished_at") or job.finished_at,
                updated_at=datetime.now(timezone.utc),
            )
            return self.jobs[idx]
        return None


class _FakeAgentRegistry:
    def __init__(self, approval: AgentApprovalRequestRecord) -> None:
        self.approval = approval

    async def list_approval_requests(self, *, tenant_id: str, session_id: str, limit: int, offset: int):  # noqa: ANN001
        if tenant_id == self.approval.tenant_id and session_id == self.approval.session_id:
            return [self.approval][offset : offset + limit]
        return []

    async def get_run(self, *, run_id: str, tenant_id: str):  # noqa: ANN001
        return None

    async def list_steps(self, *, run_id: str, tenant_id: str):  # noqa: ANN001
        return []


@pytest.mark.asyncio
async def test_session_events_aggregates_messages_jobs_approvals() -> None:
    now = datetime.now(timezone.utc)
    session = AgentSessionRecord(
        session_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        tenant_id="tenant-1",
        created_by="user-1",
        status="ACTIVE",
        selected_model=None,
        enabled_tools=[],
        summary=None,
        metadata={},
        started_at=now,
        terminated_at=None,
        created_at=now,
        updated_at=now,
    )
    sessions = _FakeSessions(session)
    sessions.messages.append(
        AgentSessionMessageRecord(
            message_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            session_id=session.session_id,
            role="user",
            content="hello",
            content_digest="sha256:msg",
            is_removed=False,
            removed_at=None,
            removed_by=None,
            removed_reason=None,
            token_count=2,
            cost_estimate=None,
            latency_ms=None,
            metadata={"kind": "user_message"},
            created_at=now,
        )
    )
    sessions.jobs.append(
        AgentSessionJobRecord(
            job_id="cccccccc-cccc-cccc-cccc-cccccccccccc",
            session_id=session.session_id,
            plan_id="dddddddd-dddd-dddd-dddd-dddddddddddd",
            run_id=None,
            status="WAITING_APPROVAL",
            error=None,
            metadata={},
            created_at=now,
            updated_at=now,
            finished_at=None,
        )
    )
    approval = AgentApprovalRequestRecord(
        approval_request_id="eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
        plan_id="dddddddd-dddd-dddd-dddd-dddddddddddd",
        tenant_id="tenant-1",
        session_id=session.session_id,
        job_id="cccccccc-cccc-cccc-cccc-cccccccccccc",
        status="PENDING",
        risk_level="write",
        requested_by="user-1",
        requested_at=now,
        decision=None,
        decided_by=None,
        decided_at=None,
        comment=None,
        request_payload={"plan_id": "dddd"},
        metadata={},
        created_at=now,
        updated_at=now,
    )
    agent_registry = _FakeAgentRegistry(approval)
    req = _Request(principal=_principal())

    resp = await list_events(
        session_id=session.session_id,
        request=req,
        sessions=sessions,
        agent_registry=agent_registry,
        include_messages=True,
        include_jobs=True,
        include_approvals=True,
        include_agent_steps=False,
        limit=200,
    )
    assert resp.status == "success"
    types = {e["event_type"] for e in resp.data["events"]}
    assert "SESSION_MESSAGE" in types
    assert "SESSION_JOB" in types
    assert "APPROVAL_REQUESTED" in types
