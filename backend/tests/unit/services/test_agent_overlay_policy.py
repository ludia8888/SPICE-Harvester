from __future__ import annotations

import pytest

from agent.models import AgentToolCall
from agent.services.agent_runtime import AgentRuntime, AgentRuntimeConfig


class DummyEventStore:
    def __init__(self) -> None:
        self.events = []

    async def append_event(self, envelope) -> None:
        self.events.append(envelope)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_agent_blocks_write_when_overlay_degraded() -> None:
    runtime = AgentRuntime(
        event_store=DummyEventStore(),  # type: ignore[arg-type]
        audit_store=None,
        config=AgentRuntimeConfig(
            bff_url="http://localhost:8000",
            allowed_services=("bff",),
            max_preview_chars=2000,
            max_payload_bytes=200000,
            timeout_s=1.0,
            service_name="agent",
            bff_token=None,
            command_timeout_s=1.0,
            command_poll_interval_s=0.1,
            command_ws_idle_s=0.1,
            command_ws_enabled=False,
            pipeline_wait_enabled=False,
            block_writes_on_overlay_degraded=True,
            allow_degraded_writes=False,
            auto_retry_enabled=True,
            auto_retry_max_attempts=3,
            auto_retry_base_delay_s=0.1,
            auto_retry_max_delay_s=1.0,
            auto_retry_allow_writes=False,
        ),
    )
    tool_call = AgentToolCall(
        service="bff",
        method="POST",
        path="/api/v1/databases/demo/actions/demo_action/submit",
        body={"input": {}},
    )

    context = {"overlay_status": "DEGRADED"}
    result = await runtime.execute_tool_call(
        run_id="run-1",
        actor="user:test",
        step_index=0,
        tool_call=tool_call,
        context=context,
        dry_run=False,
        request_headers={},
        request_id=None,
    )

    assert result["status"] == "failed"
    assert result["http_status"] == 409
    assert result["error"] == "blocked: overlay_degraded"
