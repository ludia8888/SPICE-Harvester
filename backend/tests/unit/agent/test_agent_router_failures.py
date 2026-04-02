from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException

from agent.models import AgentRunRequest, AgentToolCall
from agent.routers import agent as agent_router


class _FakeRegistry:
    def __init__(self) -> None:
        self.create_run_calls: list[dict[str, Any]] = []
        self.create_step_calls: list[dict[str, Any]] = []
        self.update_step_calls: list[dict[str, Any]] = []
        self.update_run_calls: list[dict[str, Any]] = []

    async def create_run(self, **kwargs: Any) -> None:
        self.create_run_calls.append(kwargs)

    async def create_step(self, **kwargs: Any) -> None:
        self.create_step_calls.append(kwargs)

    async def update_step_status(self, **kwargs: Any) -> None:
        self.update_step_calls.append(kwargs)

    async def update_run_status(self, **kwargs: Any) -> None:
        self.update_run_calls.append(kwargs)


class _FakeRuntime:
    def __init__(self) -> None:
        self.recorded_events: list[str] = []

    async def record_event(self, *, event_type: str, **kwargs: Any) -> None:
        _ = kwargs
        self.recorded_events.append(event_type)


class _DummyTask:
    def __init__(self) -> None:
        self.callbacks: list[Any] = []

    def add_done_callback(self, callback):  # noqa: ANN001
        self.callbacks.append(callback)


@dataclass
class _FakeRequest:
    headers: dict[str, str]
    app: Any
    client: Any = None


@pytest.mark.asyncio
async def test_create_agent_run_returns_503_when_event_store_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    request = _FakeRequest(
        headers={"X-Principal-Id": "user-1"},
        app=SimpleNamespace(state=SimpleNamespace(event_store=None, audit_store=None, agent_registry=None, agent_tasks={})),
        client=SimpleNamespace(host="127.0.0.1"),
    )
    body = AgentRunRequest(
        goal="demo",
        steps=[AgentToolCall(service="bff", method="GET", path="/api/v1/health")],
    )

    with pytest.raises(HTTPException) as raised:
        await agent_router.create_agent_run(request, body)

    assert raised.value.status_code == 503


@pytest.mark.asyncio
async def test_create_agent_run_records_started_event_after_registry_and_task_setup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _FakeRuntime()
    registry = _FakeRegistry()
    request = _FakeRequest(
        headers={"X-Principal-Id": "user-1"},
        app=SimpleNamespace(
            state=SimpleNamespace(event_store=object(), audit_store=None, agent_registry=registry, agent_tasks={})
        ),
        client=SimpleNamespace(host="127.0.0.1"),
    )
    body = AgentRunRequest(
        goal="demo",
        steps=[AgentToolCall(service="bff", method="GET", path="/api/v1/health")],
    )
    observed: list[str] = []

    monkeypatch.setattr(agent_router.AgentRuntime, "from_env", classmethod(lambda cls, **kwargs: runtime))

    async def _fake_record_run_start(**kwargs: Any) -> None:
        _ = kwargs
        observed.append("registry")

    monkeypatch.setattr(agent_router, "_record_run_start", _fake_record_run_start)
    monkeypatch.setattr(agent_router, "_execute_agent_run", lambda **kwargs: None)
    monkeypatch.setattr(
        agent_router.asyncio,
        "create_task",
        lambda coro: observed.append("task") or _DummyTask(),  # noqa: ARG005
    )

    await agent_router.create_agent_run(request, body)

    assert observed == ["registry", "task"]
    assert runtime.recorded_events == ["AGENT_RUN_STARTED"]


@pytest.mark.asyncio
async def test_execute_agent_run_marks_completed_steps_before_failed_step(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _FakeRuntime()
    registry = _FakeRegistry()

    async def _boom(runtime_arg, state_arg):  # noqa: ANN001
        _ = runtime_arg
        raise RuntimeError("boom")

    monkeypatch.setattr(agent_router, "run_agent_steps", _boom)

    await agent_router._execute_agent_run(
        runtime=runtime,  # type: ignore[arg-type]
        state={
            "run_id": "run-1",
            "actor": "user:user-1",
            "steps": [{}, {}, {}],
            "step_index": 1,
        },
        request_id="req-1",
        agent_registry=registry,  # type: ignore[arg-type]
        tenant_id="default",
    )

    assert [call["status"] for call in registry.update_step_calls] == ["COMPLETED", "FAILED", "SKIPPED"]
