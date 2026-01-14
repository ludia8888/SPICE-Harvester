from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from bff.routers.agent_sessions import get_session_token_budget
from shared.security.user_context import UserPrincipal
from shared.services.agent_session_registry import (
    AgentSessionContextItemRecord,
    AgentSessionMessageRecord,
    AgentSessionRecord,
)


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
        self.context_items: list[AgentSessionContextItemRecord] = []

    async def get_session(self, *, session_id: str, tenant_id: str):  # noqa: ANN001
        if session_id == self.session.session_id and tenant_id == self.session.tenant_id:
            return self.session
        return None

    async def list_messages(self, *, session_id: str, tenant_id: str, limit: int, offset: int, include_removed: bool):  # noqa: ANN001
        _ = include_removed
        if not await self.get_session(session_id=session_id, tenant_id=tenant_id):
            raise ValueError("session not found")
        return self.messages[offset : offset + limit]

    async def list_context_items(self, *, session_id: str, tenant_id: str, limit: int, offset: int):  # noqa: ANN001
        if not await self.get_session(session_id=session_id, tenant_id=tenant_id):
            raise ValueError("session not found")
        return self.context_items[offset : offset + limit]


class _FakeModelRegistry:
    def __init__(self, max_context_tokens: int):  # noqa: ANN001
        self._max = int(max_context_tokens)

    async def get_model(self, *, model_id: str):  # noqa: ANN001
        return SimpleNamespace(model_id=model_id, max_context_tokens=self._max)


@pytest.mark.asyncio
async def test_agent_session_token_budget_returns_candidates_when_over_limit() -> None:
    now = datetime.now(timezone.utc)
    session = AgentSessionRecord(
        session_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        tenant_id="tenant-1",
        created_by="user-1",
        status="ACTIVE",
        selected_model="gpt-test",
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
            content_digest=None,
            is_removed=False,
            removed_at=None,
            removed_by=None,
            removed_reason=None,
            token_count=60,
            cost_estimate=None,
            latency_ms=None,
            metadata={},
            created_at=now,
        )
    )
    sessions.messages.append(
        AgentSessionMessageRecord(
            message_id="cccccccc-cccc-cccc-cccc-cccccccccccc",
            session_id=session.session_id,
            role="user",
            content="hello2",
            content_digest=None,
            is_removed=False,
            removed_at=None,
            removed_by=None,
            removed_reason=None,
            token_count=60,
            cost_estimate=None,
            latency_ms=None,
            metadata={},
            created_at=now,
        )
    )
    sessions.context_items.append(
        AgentSessionContextItemRecord(
            item_id="dddddddd-dddd-dddd-dddd-dddddddddddd",
            session_id=session.session_id,
            item_type="document_bundle",
            include_mode="full",
            ref={"bundle_id": "docs"},
            token_count=50,
            metadata={},
            created_at=now,
            updated_at=now,
        )
    )

    req = _Request(principal=_principal())
    model_registry = _FakeModelRegistry(max_context_tokens=100)

    resp = await get_session_token_budget(
        session_id=session.session_id,
        request=req,
        sessions=sessions,  # type: ignore[arg-type]
        model_registry=model_registry,  # type: ignore[arg-type]
        soft_limit_ratio=0.8,
        hard_limit_ratio=1.0,
    )
    assert resp.status == "success"
    assert resp.data["flags"]["over_soft_limit"] is True
    assert resp.data["flags"]["over_hard_limit"] is True
    assert resp.data["candidates"], resp.data

