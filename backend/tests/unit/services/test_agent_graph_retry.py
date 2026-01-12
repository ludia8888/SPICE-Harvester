from __future__ import annotations

import pytest

from agent.models import AgentToolCall
from agent.services.agent_graph import run_agent_graph


class _StubRuntime:
    def __init__(self, results):
        self._results = list(results)
        self.calls = []
        self.events = []

        class _Cfg:
            auto_retry_enabled = True
            auto_retry_max_attempts = 3
            auto_retry_base_delay_s = 0.0
            auto_retry_max_delay_s = 0.0
            auto_retry_allow_writes = False

        self.config = _Cfg()

    async def record_event(self, **kwargs):  # noqa: ANN003
        self.events.append(kwargs)
        return "evt"

    async def execute_tool_call(
        self,
        *,
        run_id: str,
        actor: str,
        step_index: int,
        attempt: int = 0,
        tool_call: AgentToolCall,
        context,
        dry_run: bool,
        request_headers,
        request_id,
    ):
        self.calls.append(
            {"run_id": run_id, "actor": actor, "step_index": step_index, "attempt": attempt, "method": tool_call.method}
        )
        if not self._results:
            raise AssertionError("No more stub results available")
        return self._results.pop(0)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_agent_graph_retries_transient_read_failure() -> None:
    tool_call = AgentToolCall(service="bff", method="GET", path="/api/v1/health")
    runtime = _StubRuntime(
        results=[
            {
                "status": "failure",
                "error": "HTTP 504",
                "http_status": 504,
                "error_key": "UPSTREAM_TIMEOUT",
                "api_code": "UPSTREAM_TIMEOUT",
                "enterprise": {
                    "code": "SHV-BFF-UPS-TMO-0001",
                    "class": "timeout",
                    "legacy_code": "UPSTREAM_TIMEOUT",
                    "retryable": True,
                    "default_retry_policy": "backoff",
                    "max_attempts": 2,
                    "base_delay_ms": 0,
                    "max_delay_ms": 0,
                    "jitter_strategy": "none",
                    "retry_after_header_respect": False,
                    "human_required": False,
                    "safe_next_actions": ["retry_backoff"],
                },
                "output_digest": "d1",
            },
            {
                "status": "success",
                "http_status": 200,
                "output_digest": "d2",
            },
        ]
    )

    final_state = await run_agent_graph(
        runtime,  # type: ignore[arg-type]
        {
            "run_id": "run-1",
            "actor": "user:test",
            "steps": [tool_call],
            "step_index": 0,
            "results": [],
            "context": {},
            "dry_run": False,
            "request_headers": {},
            "request_id": None,
            "failed": False,
            "attempts": {},
            "pending_result": None,
            "next_action": "continue",
            "retry_delay_s": None,
            "policy": None,
        },
    )

    assert final_state["failed"] is False
    assert len(runtime.calls) == 2
    assert final_state["results"][0]["status"] == "success"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_agent_graph_does_not_retry_writes_by_default() -> None:
    tool_call = AgentToolCall(service="bff", method="POST", path="/api/v1/health", body={})
    runtime = _StubRuntime(
        results=[
            {
                "status": "failure",
                "error": "HTTP 503",
                "http_status": 503,
                "error_key": "UPSTREAM_UNAVAILABLE",
                "api_code": "UPSTREAM_UNAVAILABLE",
                "enterprise": {"code": "SHV-BFF-UPS-UNA-0001", "class": "unavailable", "legacy_code": "UPSTREAM_UNAVAILABLE"},
                "output_digest": "d1",
            },
        ]
    )

    final_state = await run_agent_graph(
        runtime,  # type: ignore[arg-type]
        {
            "run_id": "run-2",
            "actor": "user:test",
            "steps": [tool_call],
            "step_index": 0,
            "results": [],
            "context": {},
            "dry_run": False,
            "request_headers": {},
            "request_id": None,
            "failed": False,
            "attempts": {},
            "pending_result": None,
            "next_action": "continue",
            "retry_delay_s": None,
            "policy": None,
        },
    )

    assert final_state["failed"] is True
    assert len(runtime.calls) == 1
    assert final_state["results"][0]["status"] == "failure"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_agent_graph_respects_enterprise_max_attempts() -> None:
    tool_call = AgentToolCall(service="bff", method="GET", path="/api/v1/health")
    runtime = _StubRuntime(
        results=[
            {
                "status": "failure",
                "error": "HTTP 504",
                "http_status": 504,
                "error_key": "UPSTREAM_TIMEOUT",
                "api_code": "UPSTREAM_TIMEOUT",
                "enterprise": {
                    "code": "SHV-BFF-UPS-TMO-0001",
                    "class": "timeout",
                    "legacy_code": "UPSTREAM_TIMEOUT",
                    "retryable": True,
                    "default_retry_policy": "backoff",
                    "max_attempts": 1,
                    "base_delay_ms": 0,
                    "max_delay_ms": 0,
                    "jitter_strategy": "none",
                    "retry_after_header_respect": False,
                    "human_required": False,
                    "safe_next_actions": ["retry_backoff"],
                },
                "output_digest": "d1",
            },
            {
                "status": "success",
                "http_status": 200,
                "output_digest": "d2",
            },
        ]
    )

    final_state = await run_agent_graph(
        runtime,  # type: ignore[arg-type]
        {
            "run_id": "run-3",
            "actor": "user:test",
            "steps": [tool_call],
            "step_index": 0,
            "results": [],
            "context": {},
            "dry_run": False,
            "request_headers": {},
            "request_id": None,
            "failed": False,
            "attempts": {},
            "pending_result": None,
            "next_action": "continue",
            "retry_delay_s": None,
            "policy": None,
        },
    )

    assert final_state["failed"] is True
    assert len(runtime.calls) == 1
    assert final_state["results"][0]["status"] == "failure"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_agent_graph_uses_retry_after_when_allowed() -> None:
    tool_call = AgentToolCall(service="bff", method="GET", path="/api/v1/health")
    runtime = _StubRuntime(
        results=[
            {
                "status": "failure",
                "error": "HTTP 429",
                "http_status": 429,
                "retry_after_ms": 1,
                "error_key": "RATE_LIMITED",
                "api_code": "RATE_LIMITED",
                "enterprise": {
                    "code": "SHV-BFF-RAT-LIM-0001",
                    "class": "limit",
                    "legacy_code": "RATE_LIMITED",
                    "retryable": True,
                    "default_retry_policy": "backoff",
                    "max_attempts": 2,
                    "base_delay_ms": 0,
                    "max_delay_ms": 1000,
                    "jitter_strategy": "none",
                    "retry_after_header_respect": True,
                    "human_required": False,
                    "safe_next_actions": ["retry_backoff"],
                },
                "output_digest": "d1",
            },
            {
                "status": "success",
                "http_status": 200,
                "output_digest": "d2",
            },
        ]
    )

    final_state = await run_agent_graph(
        runtime,  # type: ignore[arg-type]
        {
            "run_id": "run-4",
            "actor": "user:test",
            "steps": [tool_call],
            "step_index": 0,
            "results": [],
            "context": {},
            "dry_run": False,
            "request_headers": {},
            "request_id": None,
            "failed": False,
            "attempts": {},
            "pending_result": None,
            "next_action": "continue",
            "retry_delay_s": None,
            "policy": None,
        },
    )

    assert final_state["failed"] is False
    assert len(runtime.calls) == 2
    retry_events = [evt for evt in runtime.events if evt.get("event_type") == "AGENT_TOOL_RETRYING"]
    assert retry_events, runtime.events
    assert retry_events[0]["data"]["retry_delay_source"] == "retry_after"
