from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from bff.routers.agent_sessions import (
    AgentSessionClarificationCreateRequest,
    AgentSessionClarificationQuestion,
    AgentSessionVariablesUpdateRequest,
    create_clarification_request,
    update_session_variables,
)
from shared.security.user_context import UserPrincipal
from shared.services.agent_session_registry import AgentSessionMessageRecord, AgentSessionRecord


class _FakeSessions:
    def __init__(self) -> None:
        self.sessions: dict[str, AgentSessionRecord] = {}
        self.messages: dict[str, list[AgentSessionMessageRecord]] = {}
        self.events: list[dict] = []

    async def create_session(self, **kwargs):  # noqa: ANN003
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

    async def get_session(self, *, session_id: str, tenant_id: str):  # noqa: ANN001
        record = self.sessions.get(session_id)
        if not record or record.tenant_id != tenant_id:
            return None
        return record

    async def update_session(self, *, session_id: str, tenant_id: str, **kwargs):  # noqa: ANN001
        record = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not record:
            return None
        updated = replace(
            record,
            status=str(kwargs.get("status") or record.status),
            selected_model=kwargs.get("selected_model", record.selected_model),
            enabled_tools=kwargs.get("enabled_tools", record.enabled_tools),
            metadata=dict(kwargs.get("metadata") or record.metadata),
            terminated_at=kwargs.get("terminated_at") or record.terminated_at,
            updated_at=datetime.now(timezone.utc),
        )
        self.sessions[session_id] = updated
        return updated

    async def add_message(self, **kwargs):  # noqa: ANN003
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
            token_count=kwargs.get("token_count"),
            cost_estimate=kwargs.get("cost_estimate"),
            latency_ms=kwargs.get("latency_ms"),
            metadata=dict(kwargs.get("metadata") or {}),
            created_at=kwargs.get("created_at") or now,
        )
        self.messages.setdefault(record.session_id, []).append(record)
        return record

    async def append_event(self, **kwargs):  # noqa: ANN003
        self.events.append(dict(kwargs))
        return kwargs.get("event_id") or "evt"


class _Request:
    def __init__(self, *, principal: UserPrincipal) -> None:
        self.headers: dict[str, str] = {}
        self.state = SimpleNamespace(user=principal)


def _principal(*, user_id: str = "user-1", tenant_id: str = "tenant-1") -> UserPrincipal:
    return UserPrincipal(id=user_id, tenant_id=tenant_id, verified=True)


@pytest.mark.asyncio
async def test_agent_session_variables_update_records_metadata_and_event() -> None:
    sessions = _FakeSessions()
    req = _Request(principal=_principal(user_id="user-1"))
    session_id = str(uuid4())
    await sessions.create_session(session_id=session_id, tenant_id="tenant-1", created_by="user-1", metadata={"variables": {"a": 1}})

    result = await update_session_variables(
        session_id=session_id,
        body=AgentSessionVariablesUpdateRequest(variables={"b": 2}, unset_keys=["a"]),
        request=req,  # type: ignore[arg-type]
        sessions=sessions,  # type: ignore[arg-type]
    )
    assert result.status == "success"
    assert result.data and result.data["variables"] == {"b": 2}
    assert sessions.sessions[session_id].metadata["variables"] == {"b": 2}
    assert sessions.events
    assert sessions.events[-1]["event_type"] == "SESSION_VARIABLES_UPDATED"


@pytest.mark.asyncio
async def test_agent_session_clarification_request_sets_waiting_approval() -> None:
    sessions = _FakeSessions()
    req = _Request(principal=_principal(user_id="user-1"))
    session_id = str(uuid4())
    await sessions.create_session(session_id=session_id, tenant_id="tenant-1", created_by="user-1")

    result = await create_clarification_request(
        session_id=session_id,
        body=AgentSessionClarificationCreateRequest(
            questions=[
                AgentSessionClarificationQuestion(id="q1", question="What DB?", required=True, type="string"),
            ],
            reason="Need DB selection",
        ),
        request=req,  # type: ignore[arg-type]
        sessions=sessions,  # type: ignore[arg-type]
    )
    assert result.status == "accepted"
    assert sessions.sessions[session_id].status == "WAITING_APPROVAL"
    assert sessions.messages[session_id]
    assert sessions.messages[session_id][-1].metadata.get("kind") == "clarification_request"
    assert sessions.events
    assert sessions.events[-1]["event_type"] == "CLARIFICATION_REQUESTED"

