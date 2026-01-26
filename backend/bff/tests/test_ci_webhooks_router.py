from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from bff.routers.ci_webhooks import AgentSessionCIResultIngestRequest, ingest_ci_result
from shared.services.registries.agent_session_registry import AgentSessionCIResultRecord, AgentSessionRecord


class _Request:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}
        self.state = SimpleNamespace()


class _FakeSessions:
    def __init__(self, session: AgentSessionRecord) -> None:
        self.session = session
        self.ci_results: list[AgentSessionCIResultRecord] = []
        self.events: list[dict] = []

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

    async def append_event(self, **kwargs):  # type: ignore[no-untyped-def]
        self.events.append(dict(kwargs))
        return True


@pytest.mark.asyncio
async def test_ci_webhook_ingest_records_ci_result() -> None:
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

    req = _Request()
    created = await ingest_ci_result(
        body=AgentSessionCIResultIngestRequest(
            tenant_id=tenant_id,
            session_id=session.session_id,
            status="success",
            summary="ok",
            raw={"provider": "x"},
        ),
        request=req,  # type: ignore[arg-type]
        sessions=sessions,  # type: ignore[arg-type]
    )
    assert created.status == "created"
    assert created.data and created.data["ci_result"]["status"] == "success"
    assert len(sessions.ci_results) == 1
    assert len(sessions.events) == 1
