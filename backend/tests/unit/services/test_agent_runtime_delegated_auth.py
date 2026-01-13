from __future__ import annotations

import pytest

from agent.models import AgentToolCall
from agent.services.agent_runtime import AgentRuntime, AgentRuntimeConfig
from uuid import UUID


class DummyEventStore:
    def __init__(self) -> None:
        self.events = []

    async def append_event(self, envelope) -> None:  # noqa: ANN001
        self.events.append(envelope)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_agent_runtime_sends_service_token_and_delegated_user_token(monkeypatch: pytest.MonkeyPatch) -> None:
    import agent.services.agent_runtime as module

    captured: dict[str, object] = {}

    class FakeResponse:
        def __init__(self) -> None:
            self.status_code = 200
            self.headers = {"content-type": "application/json"}
            self.text = ""

        def json(self):  # noqa: ANN001
            return {"status": "success", "data": {"ok": True}}

    class FakeClient:
        def __init__(self, **kwargs):  # noqa: ANN001
            captured["timeout"] = kwargs.get("timeout")

        async def __aenter__(self):  # noqa: ANN001
            return self

        async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        async def request(self, method, url, params=None, json=None, headers=None):  # noqa: ANN001
            captured["headers"] = dict(headers or {})
            return FakeResponse()

    monkeypatch.setattr(module.httpx, "AsyncClient", FakeClient)

    runtime = AgentRuntime(
        event_store=DummyEventStore(),  # type: ignore[arg-type]
        audit_store=None,
        config=AgentRuntimeConfig(
            bff_url="http://bff",
            allowed_services=("bff",),
            max_preview_chars=2000,
            max_payload_bytes=200000,
            timeout_s=1.0,
            service_name="agent",
            bff_token="agent-secret",
            command_timeout_s=1.0,
            command_poll_interval_s=0.1,
            command_ws_idle_s=0.1,
            command_ws_enabled=False,
            pipeline_wait_enabled=False,
            block_writes_on_overlay_degraded=False,
            allow_degraded_writes=True,
            auto_retry_enabled=False,
            auto_retry_max_attempts=1,
            auto_retry_base_delay_s=0.1,
            auto_retry_max_delay_s=0.1,
            auto_retry_allow_writes=False,
        ),
    )

    tool_call = AgentToolCall(
        tool_id="bff.health",
        service="bff",
        method="GET",
        path="/api/v1/health",
        query={},
        body=None,
        headers={},
        data_scope={},
    )

    await runtime.execute_tool_call(
        run_id="run-1",
        actor="user-1",
        step_index=0,
        attempt=0,
        tool_call=tool_call,
        context={},
        dry_run=False,
        request_headers={"Authorization": "Bearer user-jwt", "X-Request-Id": "req-1"},
        request_id="req-1",
    )

    headers = captured.get("headers") or {}
    assert isinstance(headers, dict)
    assert headers.get("X-Admin-Token") == "agent-secret"
    assert headers.get("X-Delegated-Authorization") == "Bearer user-jwt"
    assert headers.get("X-Agent-Tool-ID") == "bff.health"
    assert UUID(str(headers.get("X-Agent-Tool-Run-ID")))
