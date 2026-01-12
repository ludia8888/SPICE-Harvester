from __future__ import annotations

import asyncio
from typing import Any, Dict, List, TypedDict

from langgraph.graph import END, StateGraph

from agent.models import AgentToolCall
from agent.services.agent_policy import compute_retry_delay_s, decide_policy
from agent.services.agent_runtime import AgentRuntime


class AgentState(TypedDict):
    run_id: str
    actor: str
    steps: List[AgentToolCall]
    step_index: int
    results: List[Dict[str, Any]]
    context: Dict[str, Any]
    dry_run: bool
    request_headers: Dict[str, str]
    request_id: str | None
    failed: bool
    attempts: Dict[int, int]
    pending_result: Dict[str, Any] | None
    next_action: str
    retry_delay_s: float | None
    policy: Dict[str, Any] | None


async def _execute_step(state: AgentState, runtime: AgentRuntime) -> AgentState:
    idx = state.get("step_index", 0)
    steps = state.get("steps", [])
    if idx >= len(steps):
        return {**state, "next_action": "end", "pending_result": None}
    tool_call = steps[idx]
    attempts = dict(state.get("attempts") or {})
    attempt = int(attempts.get(idx) or 0)
    result = await runtime.execute_tool_call(
        run_id=state["run_id"],
        actor=state["actor"],
        step_index=idx,
        attempt=attempt,
        tool_call=tool_call,
        context=state.get("context", {}),
        dry_run=bool(state.get("dry_run")),
        request_headers=state.get("request_headers", {}),
        request_id=state.get("request_id"),
    )
    return {
        **state,
        "attempts": attempts,
        "pending_result": result,
        "next_action": "decide",
    }


async def _decide_next(state: AgentState, runtime: AgentRuntime) -> AgentState:
    idx = state.get("step_index", 0)
    steps = state.get("steps", [])
    pending = state.get("pending_result")
    if pending is None:
        if idx >= len(steps):
            return {**state, "next_action": "end"}
        return {**state, "next_action": "end", "failed": True}

    status = str(pending.get("status") or "").strip().lower()
    results = list(state.get("results", []))
    attempts = dict(state.get("attempts") or {})
    attempt = int(attempts.get(idx) or 0)

    if status != "failure":
        results.append(pending)
        await runtime.record_event(
            event_type="AGENT_STEP_FINALIZED",
            run_id=state["run_id"],
            actor=state["actor"],
            status="success",
            data={
                "step_index": idx,
                "attempt": attempt,
                "final_status": status,
                "output_digest": pending.get("output_digest"),
            },
            request_id=state.get("request_id"),
            step_index=idx,
            resource_type="agent_step",
        )
        next_idx = idx + 1
        return {
            **state,
            "step_index": next_idx,
            "results": results,
            "pending_result": None,
            "failed": False,
            "next_action": "end" if next_idx >= len(steps) else "continue",
            "policy": None,
        }

    policy = decide_policy(tool_call=steps[idx], result=pending, context=state.get("context", {}))
    policy_payload = {
        "family": policy.family,
        "recommended_action": policy.recommended_action,
        "safe_to_auto_retry": policy.safe_to_auto_retry,
        "reason": policy.reason,
        "details": policy.details,
    }

    enterprise = pending.get("enterprise") if isinstance(pending.get("enterprise"), dict) else {}
    enterprise_max_attempts = enterprise.get("max_attempts")
    try:
        enterprise_max_attempts = int(enterprise_max_attempts)
    except (TypeError, ValueError):
        enterprise_max_attempts = None

    max_attempts_cap = max(1, int(runtime.config.auto_retry_max_attempts))
    max_attempts = (
        min(max_attempts_cap, max(1, int(enterprise_max_attempts)))
        if enterprise_max_attempts is not None
        else max_attempts_cap
    )
    auto_retry_enabled = bool(runtime.config.auto_retry_enabled)
    allow_writes = bool(runtime.config.auto_retry_allow_writes)
    method = str(steps[idx].method or "").strip().upper()
    safe_to_retry = policy.safe_to_auto_retry or (allow_writes and method in {"POST", "PUT", "PATCH", "DELETE"})
    attempt_remaining = (attempt + 1) < max_attempts

    if auto_retry_enabled and policy.recommended_action == "retry" and safe_to_retry and attempt_remaining:
        next_attempt = attempt + 1
        attempts[idx] = next_attempt
        base_delay_ms = enterprise.get("base_delay_ms")
        max_delay_ms = enterprise.get("max_delay_ms")
        jitter_strategy = enterprise.get("jitter_strategy")
        retry_after_header_respect = bool(enterprise.get("retry_after_header_respect"))

        try:
            base_delay_ms = int(base_delay_ms)
        except (TypeError, ValueError):
            base_delay_ms = int(float(runtime.config.auto_retry_base_delay_s) * 1000)
        try:
            max_delay_ms = int(max_delay_ms)
        except (TypeError, ValueError):
            max_delay_ms = int(float(runtime.config.auto_retry_max_delay_s) * 1000)

        seed = f"{state['run_id']}:{idx}:{next_attempt}:{policy.family}"
        retry_after_ms = pending.get("retry_after_ms")
        try:
            retry_after_ms = int(retry_after_ms)
        except (TypeError, ValueError):
            retry_after_ms = None

        if retry_after_header_respect and retry_after_ms is not None and retry_after_ms > 0:
            retry_delay_s = max(0.0, min(float(retry_after_ms) / 1000.0, float(max_delay_ms) / 1000.0))
            retry_delay_source = "retry_after"
        else:
            retry_delay_s = compute_retry_delay_s(
                seed=seed,
                attempt=next_attempt,
                base_delay_ms=int(base_delay_ms),
                max_delay_ms=int(max_delay_ms),
                jitter_strategy=str(jitter_strategy or ""),
            )
            retry_delay_source = "policy"
        await runtime.record_event(
            event_type="AGENT_TOOL_RETRYING",
            run_id=state["run_id"],
            actor=state["actor"],
            status="success",
            data={
                "step_index": idx,
                "attempt": next_attempt,
                "retry_delay_ms": int(retry_delay_s * 1000),
                "retry_delay_source": retry_delay_source,
                "retry_max_attempts": int(max_attempts),
                "retry_seed": seed,
                "policy": policy_payload,
            },
            request_id=state.get("request_id"),
            step_index=idx,
            resource_type="agent_tool",
        )
        return {
            **state,
            "attempts": attempts,
            "pending_result": None,
            "retry_delay_s": retry_delay_s,
            "next_action": "retry",
            "policy": policy_payload,
        }

    results.append(pending)
    await runtime.record_event(
        event_type="AGENT_STEP_FINALIZED",
        run_id=state["run_id"],
        actor=state["actor"],
        status="failure",
        data={
            "step_index": idx,
            "attempt": attempt,
            "final_status": status,
            "output_digest": pending.get("output_digest"),
            "error": pending.get("error"),
            "policy": policy_payload,
        },
        request_id=state.get("request_id"),
        step_index=idx,
        resource_type="agent_step",
        error=str(pending.get("error") or ""),
    )
    return {
        **state,
        "step_index": idx + 1,
        "results": results,
        "attempts": attempts,
        "pending_result": None,
        "failed": True,
        "next_action": "end",
        "policy": policy_payload,
    }


async def _wait_before_retry(state: AgentState) -> AgentState:
    delay = float(state.get("retry_delay_s") or 0.0)
    if delay > 0:
        await asyncio.sleep(delay)
    return {**state, "retry_delay_s": None, "next_action": "continue"}


def build_agent_graph(runtime: AgentRuntime):
    graph: StateGraph = StateGraph(AgentState)

    async def execute_step(state: AgentState) -> AgentState:
        return await _execute_step(state, runtime)

    async def decide_next(state: AgentState) -> AgentState:
        return await _decide_next(state, runtime)

    graph.add_node("execute_step", execute_step)
    graph.add_node("decide_next", decide_next)
    graph.add_node("wait_retry", _wait_before_retry)

    graph.set_entry_point("execute_step")
    graph.add_edge("execute_step", "decide_next")
    graph.add_conditional_edges(
        "decide_next",
        lambda state: state.get("next_action", "end"),
        {"continue": "execute_step", "retry": "wait_retry", "end": END},
    )
    graph.add_edge("wait_retry", "execute_step")
    return graph.compile()


async def run_agent_graph(runtime: AgentRuntime, initial_state: AgentState) -> AgentState:
    app = build_agent_graph(runtime)
    return await app.ainvoke(initial_state)
