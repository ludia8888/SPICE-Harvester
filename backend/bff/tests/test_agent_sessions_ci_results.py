from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from bff.routers.agent_sessions import AgentSessionCIResultCreateRequest, create_ci_result, list_ci_results
from shared.security.user_context import UserPrincipal
from shared.services.agent_session_registry import AgentSessionCIResultRecord, AgentSessionRecord


class _Request:
    def __init__(self, *, principal: UserPrincipal) -> None:
        self.headers: dict[str, str] = {}
        self.state = SimpleNamespace(user=principal)


class _FakeSessions:
    def __init__(self, session: AgentSessionRecord) -> None:
        self.session = session
        self.ci_results: list[AgentSessionCIResultRecord] = []

    async def get_session(self, *, session_id: str, tenant_id: str):  # noqa: ANN001
        if session_id == self.session.session_id and tenant_id == self.session.tenant_id:
            return self.session
        return None

    async def record_ci_result(self, **kwargs):  # type: ignore[no-untyped-def]
        record = AgentSessionCIResultRecord(
            ci_result_id=str(kwargs["ci_result_id"]),
            session_id=str(kwargs["session_id"]),
            tenant_id=str(kwargs["tenant_id"]),
            job_id=kwargs.get("job_id"),
            plan_id=kwargs.get("plan_id"),
            run_id=kwargs.get("run_id"),
            provider=kwargs.get("provider"),
            status=str(kwargs.get("status") or "unknown"),
            details_url=kwargs.get("details_url"),
            summary=kwargs.get("summary"),
            checks=list(kwargs.get("checks") or []),
            raw=dict(kwargs.get("raw") or {}),
            created_at=kwargs.get("created_at") or datetime.now(timezone.utc),
        )
        self.ci_results.append(record)
        return record

    async def list_ci_results(self, *, session_id: str, tenant_id: str, limit: int, offset: int):  # noqa: ANN001
        if not await self.get_session(session_id=session_id, tenant_id=tenant_id):
            raise ValueError("session not found")
        return self.ci_results[offset : offset + limit]


@pytest.mark.asyncio
async def test_agent_session_ci_results_create_and_list() -> None:
    now = datetime.now(timezone.utc)
    tenant_id = "tenant-1"
    session = AgentSessionRecord(
        session_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        tenant_id=tenant_id,
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

    req = _Request(principal=UserPrincipal(id="user-1", tenant_id=tenant_id, verified=True))
    created = await create_ci_result(
        session_id=session.session_id,
        body=AgentSessionCIResultCreateRequest(status="success", summary="ok", raw={"provider": "x"}),
        request=req,
        sessions=sessions,  # type: ignore[arg-type]
    )
    assert created.status == "created"
    assert created.data["ci_result"]["status"] == "success"

    listed = await list_ci_results(
        session_id=session.session_id,
        request=req,
        sessions=sessions,  # type: ignore[arg-type]
        limit=50,
        offset=0,
    )
    assert listed.status == "success"
    assert listed.data["count"] == 1
