from __future__ import annotations

import pytest

from agent.services.agent_runtime import AgentRuntime, AgentRuntimeConfig


class _DummyEventStore:
    def __init__(self) -> None:
        self.events = []

    async def append_event(self, envelope) -> None:  # noqa: ANN001
        self.events.append(envelope)


class _FailingAuditStore:
    async def log(self, **kwargs) -> None:  # noqa: ANN003
        _ = kwargs
        raise RuntimeError("audit unavailable")


async def _build_runtime() -> AgentRuntime:
    return AgentRuntime(
        event_store=_DummyEventStore(),  # type: ignore[arg-type]
        audit_store=_FailingAuditStore(),  # type: ignore[arg-type]
        config=AgentRuntimeConfig(
            bff_url="http://bff",
            allowed_services=("bff",),
            max_preview_chars=2000,
            max_payload_bytes=200000,
            timeout_s=1.0,
            service_name="agent",
            bff_token=None,
            command_timeout_s=5.0,
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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_record_event_returns_success_when_audit_logging_fails() -> None:
    runtime = await _build_runtime()

    event_id = await runtime.record_event(
        event_type="AGENT_RUN_STARTED",
        run_id="11111111-1111-1111-1111-111111111111",
        actor="user:user-1",
        status="success",
        data={"steps_total": 1},
        request_id="req-1",
    )

    assert event_id
    assert len(runtime.event_store.events) == 1  # type: ignore[attr-defined]
