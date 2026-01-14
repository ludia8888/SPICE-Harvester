from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi import HTTPException, status

from bff.routers.agent_sessions import (
    AgentSessionContextItemCreateRequest,
    AgentSessionCreateRequest,
    AgentSessionMessageRequest,
    AgentSessionUpdateModelRequest,
    AgentSessionUpdateToolsRequest,
    attach_context_item,
    create_session,
    get_session,
    list_sessions,
    list_context_items,
    list_session_tools,
    update_session_model,
    update_session_tools,
    post_message,
    remove_context_item,
    terminate_session,
)
from shared.security.user_context import UserPrincipal
from shared.services.agent_session_registry import AgentSessionContextItemRecord, AgentSessionMessageRecord, AgentSessionRecord


class _FakeSessions:
    def __init__(self) -> None:
        self.sessions: dict[str, AgentSessionRecord] = {}
        self.context_items: dict[str, list[AgentSessionContextItemRecord]] = {}
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
            selected_model=kwargs.get("selected_model", record.selected_model),
            enabled_tools=kwargs.get("enabled_tools", record.enabled_tools),
            metadata=dict(kwargs.get("metadata") or record.metadata),
            terminated_at=kwargs.get("terminated_at") or record.terminated_at,
            updated_at=datetime.now(timezone.utc),
        )
        self.sessions[session_id] = updated
        return updated

    async def add_context_item(self, **kwargs) -> AgentSessionContextItemRecord:  # type: ignore[no-untyped-def]
        session = await self.get_session(session_id=str(kwargs["session_id"]), tenant_id=str(kwargs["tenant_id"]))
        if not session:
            raise ValueError("session not found")
        now = datetime.now(timezone.utc)
        record = AgentSessionContextItemRecord(
            item_id=str(kwargs["item_id"]),
            session_id=str(kwargs["session_id"]),
            item_type=str(kwargs["item_type"]),
            include_mode=str(kwargs.get("include_mode") or "summary"),
            ref=dict(kwargs.get("ref") or {}),
            token_count=kwargs.get("token_count"),
            metadata=dict(kwargs.get("metadata") or {}),
            created_at=kwargs.get("created_at") or now,
            updated_at=now,
        )
        self.context_items.setdefault(record.session_id, []).append(record)
        return record

    async def list_context_items(self, *, session_id: str, tenant_id: str, limit: int, offset: int):  # type: ignore[no-untyped-def]
        session = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not session:
            raise ValueError("session not found")
        items = list(self.context_items.get(session_id) or [])
        return items[offset : offset + limit]

    async def remove_context_item(self, *, session_id: str, tenant_id: str, item_id: str) -> int:  # type: ignore[no-untyped-def]
        session = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not session:
            raise ValueError("session not found")
        before = list(self.context_items.get(session_id) or [])
        after = [item for item in before if item.item_id != item_id]
        self.context_items[session_id] = after
        return 1 if len(after) != len(before) else 0

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
            token_count=kwargs.get("token_count"),
            cost_estimate=kwargs.get("cost_estimate"),
            latency_ms=kwargs.get("latency_ms"),
            metadata=dict(kwargs.get("metadata") or {}),
            created_at=kwargs.get("created_at") or now,
        )
        self.messages.setdefault(record.session_id, []).append(record)
        return record


class _Request:
    def __init__(self, *, principal: UserPrincipal) -> None:
        self.headers: dict[str, str] = {}
        self.state = SimpleNamespace(user=principal)


def _principal(*, user_id: str = "user-1", tenant_id: str = "tenant-1") -> UserPrincipal:
    return UserPrincipal(id=user_id, tenant_id=tenant_id, verified=True)


class _FakePolicyRegistry:
    async def get_tenant_policy(self, *, tenant_id: str):  # noqa: ANN001
        return None


class _FakeToolRegistry:
    async def get_tool_policy(self, *, tool_id: str):  # noqa: ANN001
        return None

    async def list_tool_policies(self, *, status: str, limit: int = 200):  # noqa: ANN001
        return []


class _ToolRegistryWithActive:
    def __init__(self, tool_ids: list[str]):  # noqa: ANN001
        self._tool_ids = tool_ids

    async def get_tool_policy(self, *, tool_id: str):  # noqa: ANN001
        if tool_id not in self._tool_ids:
            return None
        return SimpleNamespace(tool_id=tool_id, status="ACTIVE")

    async def list_tool_policies(self, *, status: str, limit: int = 200):  # noqa: ANN001
        return [
            SimpleNamespace(
                tool_id=tool_id,
                method="GET",
                path="/api/v1/health",
                risk_level="read",
                requires_approval=False,
                requires_idempotency_key=False,
                roles=[],
                max_payload_bytes=200000,
                status="ACTIVE",
            )
            for tool_id in self._tool_ids
        ]


class _ModelRegistryActive:
    def __init__(self, active_models: set[str]):  # noqa: ANN001
        self._active = set(active_models)

    async def get_model(self, *, model_id: str):  # noqa: ANN001
        if model_id not in self._active:
            return None
        return SimpleNamespace(
            model_id=model_id,
            provider="openai_compat",
            display_name=None,
            status="ACTIVE",
            supports_json_mode=True,
            supports_native_tool_calling=False,
            max_context_tokens=128000,
            max_output_tokens=4096,
            metadata={},
        )


@pytest.mark.asyncio
async def test_agent_sessions_crud_round_trip() -> None:
    sessions = _FakeSessions()
    req = _Request(principal=_principal())

    created = await create_session(
        AgentSessionCreateRequest(),
        request=req,
        sessions=sessions,
        policy_registry=_FakePolicyRegistry(),
        tool_registry=_FakeToolRegistry(),
    )
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

    created = await create_session(
        AgentSessionCreateRequest(),
        request=req_tenant_a,
        sessions=sessions,
        policy_registry=_FakePolicyRegistry(),
        tool_registry=_FakeToolRegistry(),
    )
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


@pytest.mark.asyncio
async def test_agent_sessions_context_items_round_trip() -> None:
    sessions = _FakeSessions()
    req = _Request(principal=_principal())

    created = await create_session(
        AgentSessionCreateRequest(),
        request=req,
        sessions=sessions,
        policy_registry=_FakePolicyRegistry(),
        tool_registry=_FakeToolRegistry(),
    )
    session_id = created.data["session"]["session_id"]

    attached = await attach_context_item(
        session_id=session_id,
        body=AgentSessionContextItemCreateRequest(item_type="dataset", include_mode="summary", ref={"dataset_id": "ds-1"}),
        request=req,
        sessions=sessions,
    )
    assert attached.status == "created"
    item_id = attached.data["context_item"]["item_id"]

    listed = await list_context_items(session_id=session_id, request=req, sessions=sessions, limit=200, offset=0)
    assert listed.status == "success"
    assert listed.data["count"] == 1
    assert listed.data["context_items"][0]["item_id"] == item_id

    removed = await remove_context_item(session_id=session_id, item_id=item_id, request=req, sessions=sessions)
    assert removed.status == "success"

    listed_again = await list_context_items(session_id=session_id, request=req, sessions=sessions, limit=200, offset=0)
    assert listed_again.data["count"] == 0


@pytest.mark.asyncio
async def test_agent_sessions_post_message_includes_only_attached_context(monkeypatch: pytest.MonkeyPatch) -> None:
    sessions = _FakeSessions()
    req = _Request(principal=_principal())

    created = await create_session(
        AgentSessionCreateRequest(),
        request=req,
        sessions=sessions,
        policy_registry=_FakePolicyRegistry(),
        tool_registry=_FakeToolRegistry(),
    )
    session_id = created.data["session"]["session_id"]

    await attach_context_item(
        session_id=session_id,
        body=AgentSessionContextItemCreateRequest(item_type="dataset", include_mode="full", ref={"dataset_id": "ds-ctx"}),
        request=req,
        sessions=sessions,
    )

    captured: dict = {}

    async def _fake_compile_agent_plan(**kwargs):  # type: ignore[no-untyped-def]
        captured["context_pack"] = kwargs.get("context_pack")
        from bff.services.agent_plan_compiler import AgentPlanCompileResult

        return AgentPlanCompileResult(
            status="clarification_required",
            plan_id="00000000-0000-0000-0000-000000000000",
            plan=None,
            validation_errors=["needs clarification"],
            validation_warnings=[],
            questions=[],
        )

    monkeypatch.setattr("bff.routers.agent_sessions.compile_agent_plan", _fake_compile_agent_plan)

    response = await post_message(
        session_id=session_id,
        body=AgentSessionMessageRequest(content="do something", execute=True),
        request=req,
        llm=None,  # type: ignore[arg-type]
        redis_service=None,  # type: ignore[arg-type]
        audit_store=None,  # type: ignore[arg-type]
        sessions=sessions,
        policy_registry=_FakePolicyRegistry(),  # type: ignore[arg-type]
        tool_registry=_FakeToolRegistry(),  # type: ignore[arg-type]
        plan_registry=SimpleNamespace(),
        agent_registry=SimpleNamespace(),
    )
    assert response.status == "warning"

    context_pack = captured.get("context_pack") or {}
    assert context_pack.get("attached_context")
    assert len(context_pack["attached_context"]) == 1
    assert context_pack["attached_context"][0]["item_type"] == "dataset"
    assert context_pack["attached_context"][0]["include_mode"] == "full"
    assert context_pack["attached_context"][0]["ref"]["dataset_id"] == "ds-ctx"


@pytest.mark.asyncio
async def test_agent_sessions_can_update_enabled_tools() -> None:
    sessions = _FakeSessions()
    req = _Request(principal=_principal())

    created = await create_session(
        AgentSessionCreateRequest(),
        request=req,
        sessions=sessions,
        policy_registry=_FakePolicyRegistry(),
        tool_registry=_FakeToolRegistry(),
    )
    session_id = created.data["session"]["session_id"]

    tool_registry = _ToolRegistryWithActive(["tool.a", "tool.b"])
    updated = await update_session_tools(
        session_id=session_id,
        body=AgentSessionUpdateToolsRequest(enabled_tools=["tool.a", "tool.b"]),
        request=req,
        sessions=sessions,
        policy_registry=_FakePolicyRegistry(),  # type: ignore[arg-type]
        tool_registry=tool_registry,  # type: ignore[arg-type]
    )
    assert updated.status == "success"
    assert set(updated.data["enabled_tools"]) == {"tool.a", "tool.b"}
    assert updated.data["metadata"]["tools_restricted"] is True

    listed = await list_session_tools(
        session_id=session_id,
        request=req,
        sessions=sessions,
        policy_registry=_FakePolicyRegistry(),  # type: ignore[arg-type]
        tool_registry=tool_registry,  # type: ignore[arg-type]
    )
    assert listed.status == "success"
    assert listed.data["count"] == 2


@pytest.mark.asyncio
async def test_agent_sessions_can_update_selected_model() -> None:
    sessions = _FakeSessions()
    req = _Request(principal=_principal())

    class _PolicyRegistry:
        async def get_tenant_policy(self, *, tenant_id: str):  # noqa: ANN001
            return SimpleNamespace(
                tenant_id=tenant_id,
                allowed_models=["gpt-4.1-mini"],
                allowed_tools=[],
                default_model=None,
                auto_approve_rules={},
                data_policies={},
            )

    created = await create_session(
        AgentSessionCreateRequest(),
        request=req,
        sessions=sessions,
        policy_registry=_PolicyRegistry(),  # type: ignore[arg-type]
        tool_registry=_FakeToolRegistry(),
    )
    session_id = created.data["session"]["session_id"]

    updated = await update_session_model(
        session_id=session_id,
        body=AgentSessionUpdateModelRequest(selected_model="gpt-4.1-mini"),
        request=req,
        sessions=sessions,
        policy_registry=_PolicyRegistry(),  # type: ignore[arg-type]
        model_registry=_ModelRegistryActive({"gpt-4.1-mini"}),  # type: ignore[arg-type]
    )
    assert updated.status == "success"
    assert updated.data["selected_model"] == "gpt-4.1-mini"
