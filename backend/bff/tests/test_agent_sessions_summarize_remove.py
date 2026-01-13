from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from bff.routers.agent_sessions import (
    AgentSessionRemoveMessagesRequest,
    AgentSessionSummarizeRequest,
    remove_messages,
    summarize_session,
)
from shared.security.user_context import UserPrincipal
from shared.services.agent_session_registry import AgentSessionMessageRecord, AgentSessionRecord
from shared.services.llm_gateway import LLMCallMeta


class _FakeSessions:
    def __init__(self) -> None:
        self.sessions: dict[str, AgentSessionRecord] = {}
        self.messages: dict[str, list[AgentSessionMessageRecord]] = {}

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
            summary=kwargs.get("summary", record.summary),
            updated_at=datetime.now(timezone.utc),
        )
        self.sessions[session_id] = updated
        return updated

    async def list_messages(self, *, session_id: str, tenant_id: str, limit: int, offset: int, include_removed: bool = False):  # type: ignore[no-untyped-def]
        record = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not record:
            raise ValueError("session not found")
        items = list(self.messages.get(session_id, []))
        if not include_removed:
            items = [m for m in items if not m.is_removed]
        return items[offset : offset + limit]

    async def add_message(self, **kwargs):  # type: ignore[no-untyped-def]
        record = await self.get_session(session_id=kwargs["session_id"], tenant_id=kwargs["tenant_id"])
        if not record:
            raise ValueError("session not found")
        now = datetime.now(timezone.utc)
        msg = AgentSessionMessageRecord(
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
        self.messages.setdefault(msg.session_id, []).append(msg)
        return msg

    async def get_messages_by_ids(self, *, session_id: str, tenant_id: str, message_ids: list[str], include_removed: bool = True):  # type: ignore[no-untyped-def]
        record = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not record:
            raise ValueError("session not found")
        wanted = set(message_ids)
        out = [m for m in self.messages.get(session_id, []) if m.message_id in wanted]
        if not include_removed:
            out = [m for m in out if not m.is_removed]
        return out

    async def mark_messages_removed(self, *, session_id: str, tenant_id: str, message_ids: list[str], removed_by: str, removed_reason: str | None, removed_at, placeholder: str = "<removed>"):  # type: ignore[no-untyped-def]
        record = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not record:
            raise ValueError("session not found")
        count = 0
        updated: list[AgentSessionMessageRecord] = []
        for message in self.messages.get(session_id, []):
            if message.message_id in set(message_ids):
                count += 1
                updated.append(
                    replace(
                        message,
                        content=placeholder,
                        is_removed=True,
                        removed_at=removed_at,
                        removed_by=removed_by,
                        removed_reason=removed_reason,
                    )
                )
            else:
                updated.append(message)
        self.messages[session_id] = updated
        return count


class _Request:
    def __init__(self, *, principal: UserPrincipal) -> None:
        self.headers: dict[str, str] = {}
        self.state = SimpleNamespace(user=principal)


class _FakeLLM:
    async def complete_json(self, *, response_model, **_kwargs):  # type: ignore[no-untyped-def]
        return response_model(summary="요약", key_points=["k1"]), LLMCallMeta(provider="mock", model="m", cache_hit=False, latency_ms=1)


@pytest.mark.asyncio
async def test_summarize_session_adds_summary_message() -> None:
    tenant_id = "tenant-1"
    session_id = str(uuid4())
    now = datetime.now(timezone.utc)
    sessions = _FakeSessions()
    sessions.sessions[session_id] = AgentSessionRecord(
        session_id=session_id,
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
    await sessions.add_message(
        message_id=str(uuid4()),
        session_id=session_id,
        tenant_id=tenant_id,
        role="user",
        content="hello",
        content_digest="d1",
        metadata={},
        created_at=now,
    )

    req = _Request(principal=UserPrincipal(id="user-1", tenant_id=tenant_id, verified=True))
    audit_store = SimpleNamespace(log=lambda **_kwargs: None)

    result = await summarize_session(
        session_id=session_id,
        body=AgentSessionSummarizeRequest(max_messages=50),
        request=req,
        llm=_FakeLLM(),
        audit_store=audit_store,
        sessions=sessions,
    )

    assert result.status in {"success", "warning"}
    assert sessions.sessions[session_id].summary
    assert sessions.messages[session_id][-1].role == "system"


@pytest.mark.asyncio
async def test_remove_messages_marks_removed_and_audits() -> None:
    tenant_id = "tenant-1"
    session_id = str(uuid4())
    now = datetime.now(timezone.utc)
    sessions = _FakeSessions()
    sessions.sessions[session_id] = AgentSessionRecord(
        session_id=session_id,
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
    msg_id = str(uuid4())
    await sessions.add_message(
        message_id=msg_id,
        session_id=session_id,
        tenant_id=tenant_id,
        role="user",
        content="secret",
        content_digest="digest-1",
        metadata={},
        created_at=now,
    )

    req = _Request(principal=UserPrincipal(id="user-1", tenant_id=tenant_id, verified=True))

    class _Audit:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        async def log(self, **kwargs):  # type: ignore[no-untyped-def]
            self.calls.append(kwargs)

    audit_store = _Audit()

    result = await remove_messages(
        session_id=session_id,
        body=AgentSessionRemoveMessagesRequest(message_ids=[msg_id], reason="cleanup"),
        request=req,
        audit_store=audit_store,
        sessions=sessions,
    )

    assert result.status == "success"
    assert result.data["removed_count"] == 1
    assert sessions.messages[session_id][0].is_removed is True
    assert audit_store.calls

