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
        self.calls.append({"step_index": step_index, "path": tool_call.path, "method": tool_call.method, "attempt": attempt})
        if not self._results:
            raise AssertionError("No more stub results available")
        return self._results.pop(0)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_simulation_rejection_stops_before_submit() -> None:
    simulate = AgentToolCall(service="bff", method="POST", path="/api/v1/databases/demo/actions/A/simulate", body={})
    submit = AgentToolCall(service="bff", method="POST", path="/api/v1/databases/demo/actions/A/submit", body={})

    runtime = _StubRuntime(
        results=[
            {
                "status": "failed",
                "error": "simulation rejected",
                "http_status": 200,
                "error_key": "submission_criteria_failed",
                "enterprise": {
                    "code": "SHV-ACT-ACC-PER-3004",
                    "class": "permission",
                    "legacy_code": "submission_criteria_failed",
                    "retryable": False,
                    "default_retry_policy": "none",
                    "human_required": True,
                    "action": "request_access",
                },
                "signals": {"action_log_reason": "missing_role"},
                "output_digest": "d1",
            }
        ]
    )

    final_state = await run_agent_graph(
        runtime,  # type: ignore[arg-type]
        {
            "run_id": "run-1",
            "actor": "user:test",
            "steps": [simulate, submit],
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
    assert final_state["results"][0]["error_key"] == "submission_criteria_failed"
