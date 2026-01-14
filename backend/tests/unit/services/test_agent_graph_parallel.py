from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import pytest

from agent.models import AgentToolCall
from agent.services.agent_graph import run_agent_graph


class _StubRuntime:
    def __init__(
        self,
        *,
        results_by_step: dict[int, dict],
        delays_s: dict[int, float] | None = None,
        max_concurrent: int = 2,
        fail_fast: bool = False,
    ) -> None:
        self._results_by_step = dict(results_by_step)
        self._delays_s = dict(delays_s or {})
        self.events: list[dict] = []
        self.calls: list[dict] = []
        self.active = 0
        self.max_active = 0
        self._lock = asyncio.Lock()
        self.config = SimpleNamespace(
            parallel_execution_enabled=True,
            max_concurrent_tool_calls=max_concurrent,
            fail_fast=fail_fast,
            auto_retry_enabled=False,
            auto_retry_max_attempts=1,
            auto_retry_base_delay_s=0.0,
            auto_retry_max_delay_s=0.0,
            auto_retry_allow_writes=False,
        )

    async def record_event(self, **kwargs):  # noqa: ANN003
        self.events.append(kwargs)
        return "evt"

    async def execute_tool_call(  # noqa: ANN001
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
        delay = float(self._delays_s.get(step_index, 0.0))
        async with self._lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            started_at = time.monotonic()
        await asyncio.sleep(delay)
        async with self._lock:
            finished_at = time.monotonic()
            self.active -= 1

        self.calls.append(
            {
                "step_index": step_index,
                "attempt": attempt,
                "started_at": started_at,
                "finished_at": finished_at,
                "tool_id": tool_call.tool_id,
            }
        )
        if step_index not in self._results_by_step:
            raise AssertionError(f"Missing stub result for step_index={step_index}")
        return dict(self._results_by_step[step_index])


def _initial_state(*, steps: list[AgentToolCall]) -> dict:
    return {
        "run_id": "run-1",
        "actor": "user:test",
        "steps": steps,
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
    }


@pytest.mark.unit
@pytest.mark.asyncio
async def test_agent_graph_parallel_respects_dependencies_and_concurrency() -> None:
    steps = [
        AgentToolCall(step_id="s0", tool_id="t0", service="bff", method="GET", path="/a", produces=["a"]),
        AgentToolCall(step_id="s1", tool_id="t1", service="bff", method="GET", path="/b", consumes=["a"]),
        AgentToolCall(step_id="s2", tool_id="t2", service="bff", method="GET", path="/c", produces=["b"]),
        AgentToolCall(step_id="s3", tool_id="t3", service="bff", method="GET", path="/d", consumes=["b"]),
    ]
    runtime = _StubRuntime(
        results_by_step={
            0: {"status": "success", "output_digest": "d0"},
            1: {"status": "success", "output_digest": "d1"},
            2: {"status": "success", "output_digest": "d2"},
            3: {"status": "success", "output_digest": "d3"},
        },
        delays_s={0: 0.05, 2: 0.05, 1: 0.0, 3: 0.0},
        max_concurrent=2,
    )

    final_state = await run_agent_graph(runtime, _initial_state(steps=steps))  # type: ignore[arg-type]

    assert final_state["failed"] is False
    assert runtime.max_active <= 2
    # Two independent roots (0 and 2) should overlap given delays.
    assert runtime.max_active == 2

    started = {c["step_index"]: c["started_at"] for c in runtime.calls}
    finished = {c["step_index"]: c["finished_at"] for c in runtime.calls}
    assert started[1] >= finished[0]
    assert started[3] >= finished[2]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_agent_graph_parallel_skips_dependents_after_failure() -> None:
    steps = [
        AgentToolCall(step_id="s0", tool_id="t0", service="bff", method="GET", path="/a", produces=["a"]),
        AgentToolCall(step_id="s1", tool_id="t1", service="bff", method="GET", path="/b", consumes=["a"]),
        AgentToolCall(step_id="s2", tool_id="t2", service="bff", method="GET", path="/c"),
    ]
    runtime = _StubRuntime(
        results_by_step={
            0: {"status": "failed", "error": "boom", "output_digest": "d0"},
            2: {"status": "success", "output_digest": "d2"},
        },
        delays_s={0: 0.0, 2: 0.0},
        max_concurrent=2,
    )
    final_state = await run_agent_graph(runtime, _initial_state(steps=steps))  # type: ignore[arg-type]

    assert final_state["failed"] is True
    assert any(r.get("status") == "skipped" for r in final_state["results"]), final_state["results"]
    called_indices = {c["step_index"] for c in runtime.calls}
    assert 1 not in called_indices


@pytest.mark.unit
@pytest.mark.asyncio
async def test_agent_graph_parallel_fail_fast_stops_scheduling() -> None:
    steps = [
        AgentToolCall(step_id="s0", tool_id="t0", service="bff", method="GET", path="/a"),
        AgentToolCall(step_id="s1", tool_id="t1", service="bff", method="GET", path="/b"),
    ]
    runtime = _StubRuntime(
        results_by_step={
            0: {"status": "failed", "error": "boom", "output_digest": "d0"},
        },
        delays_s={0: 0.0},
        max_concurrent=1,
        fail_fast=True,
    )
    final_state = await run_agent_graph(runtime, _initial_state(steps=steps))  # type: ignore[arg-type]

    assert final_state["failed"] is True
    called_indices = {c["step_index"] for c in runtime.calls}
    assert called_indices == {0}
    assert final_state["results"][1].get("status") == "skipped"
