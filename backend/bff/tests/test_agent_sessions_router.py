from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi import HTTPException, status

from bff.routers.agent_sessions import (
    AgentSessionCreateRequest,
    create_session,
    get_session,
    list_sessions,
    terminate_session,
)
from shared.security.user_context import UserPrincipal
from shared.services.agent_session_registry import AgentSessionRecord


class _FakeSessions:
    def __init__(self) -> None:
        self.sessions: dict[str, AgentSessionRecord] = {}

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

    async def list_sessions(self, *, tenant_id: str, created_by: str, limit: int, offset: int):  # type: ignore[no-untyped-def]
        matches = [
            session
            for session in self.sessions.values()
            if session.tenant_id == tenant_id and session.created_by == created_by
        ]
        return matches[offset : offset + limit]

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
            terminated_at=kwargs.get("terminated_at") or record.terminated_at,
            updated_at=datetime.now(timezone.utc),
        )
        self.sessions[session_id] = updated
        return updated


class _Request:
    def __init__(self, *, principal: UserPrincipal) -> None:
        self.headers: dict[str, str] = {}
        self.state = SimpleNamespace(user=principal)


def _principal(*, user_id: str = "user-1", tenant_id: str = "tenant-1") -> UserPrincipal:
    return UserPrincipal(id=user_id, tenant_id=tenant_id, verified=True)


@pytest.mark.asyncio
async def test_agent_sessions_crud_round_trip() -> None:
    sessions = _FakeSessions()
    req = _Request(principal=_principal())

    created = await create_session(AgentSessionCreateRequest(), request=req, sessions=sessions)
    assert created.status == "created"
    session_id = created.data["session"]["session_id"]
    assert created.data["session"]["enabled_tools"] == []
    assert created.data["session"]["terminated_at"] is None

    listed = await list_sessions(request=req, sessions=sessions, limit=50, offset=0)
    assert listed.status == "success"
    assert listed.data["count"] == 1
    assert listed.data["sessions"][0]["session_id"] == session_id

    fetched = await get_session(session_id=session_id, request=req, sessions=sessions, include_messages=False, messages_limit=200)
    assert fetched.status == "success"
    assert fetched.data["session"]["session_id"] == session_id
    assert fetched.data["session"]["summary"] is None

    terminated = await terminate_session(session_id=session_id, request=req, sessions=sessions)
    assert terminated.status == "success"
    assert terminated.data["status"] == "TERMINATED"


@pytest.mark.asyncio
async def test_agent_sessions_enforces_tenant_boundary() -> None:
    sessions = _FakeSessions()
    req_tenant_a = _Request(principal=_principal(tenant_id="tenant-a"))
    req_tenant_b = _Request(principal=_principal(tenant_id="tenant-b"))

    created = await create_session(AgentSessionCreateRequest(), request=req_tenant_a, sessions=sessions)
    session_id = created.data["session"]["session_id"]

    with pytest.raises(HTTPException) as exc_info:
        await get_session(session_id=session_id, request=req_tenant_b, sessions=sessions, include_messages=False, messages_limit=200)
    assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.asyncio
async def test_agent_sessions_rejects_invalid_session_id() -> None:
    sessions = _FakeSessions()
    req = _Request(principal=_principal())

    with pytest.raises(HTTPException) as exc_info:
        await get_session(session_id="not-a-uuid", request=req, sessions=sessions, include_messages=False, messages_limit=200)
    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
