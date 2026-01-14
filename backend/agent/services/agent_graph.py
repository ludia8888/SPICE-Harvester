from __future__ import annotations

import asyncio
import re
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


_TEMPLATE_TOKEN_RE = re.compile(r"\$\{([^{}]+)\}")
_FAILED_STATUSES = {"failed", "failure"}


def _step_id_for_index(tool_call: AgentToolCall, idx: int) -> str:
    raw = str(getattr(tool_call, "step_id", None) or "").strip()
    return raw or f"step_{idx}"


def _collect_template_tokens(value: Any) -> set[str]:
    tokens: set[str] = set()
    if value is None:
        return tokens
    if isinstance(value, str):
        for match in _TEMPLATE_TOKEN_RE.finditer(value):
            token = str(match.group(1) or "").strip()
            if token:
                tokens.add(token)
        return tokens
    if isinstance(value, dict):
        for item in value.values():
            tokens |= _collect_template_tokens(item)
        return tokens
    if isinstance(value, list):
        for item in value:
            tokens |= _collect_template_tokens(item)
        return tokens
    return tokens


def _extract_template_dependencies(tool_call: AgentToolCall) -> tuple[set[str], set[str]]:
    tokens: set[str] = set()
    tokens |= _collect_template_tokens(getattr(tool_call, "path", None))
    tokens |= _collect_template_tokens(getattr(tool_call, "query", None))
    tokens |= _collect_template_tokens(getattr(tool_call, "body", None))
    tokens |= _collect_template_tokens(getattr(tool_call, "headers", None))

    step_ids: set[str] = set()
    artifact_keys: set[str] = set()
    for token in tokens:
        if token.startswith("steps."):
            rest = token[len("steps.") :]
            parts = rest.split(".")
            if len(parts) >= 2:
                step_id = ".".join(parts[:-1]).strip()
                if step_id:
                    step_ids.add(step_id)
            continue
        if token.startswith("artifacts."):
            key = token[len("artifacts.") :].strip()
            if key:
                artifact_keys.add(key)
            continue
    return step_ids, artifact_keys


def _detect_cycle(deps: list[set[int]]) -> bool:
    indegree = [len(d) for d in deps]
    ready = [idx for idx, deg in enumerate(indegree) if deg == 0]
    visited = 0
    dependents: list[list[int]] = [[] for _ in range(len(deps))]
    for idx, parents in enumerate(deps):
        for parent in parents:
            dependents[parent].append(idx)

    while ready:
        node = ready.pop()
        visited += 1
        for child in dependents[node]:
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)
    return visited != len(deps)


def _build_step_dependencies(steps: list[AgentToolCall]) -> list[set[int]]:
    step_id_to_index: dict[str, int] = {}
    for idx, step in enumerate(steps):
        step_id_to_index[_step_id_for_index(step, idx)] = idx

    artifact_producers: dict[str, int] = {}
    for idx, step in enumerate(steps):
        produces = [str(k).strip() for k in (getattr(step, "produces", None) or []) if str(k).strip()]
        for key in produces:
            artifact_producers.setdefault(key, idx)

    deps: list[set[int]] = [set() for _ in range(len(steps))]
    for idx, step in enumerate(steps):
        consumes = [str(k).strip() for k in (getattr(step, "consumes", None) or []) if str(k).strip()]
        for key in consumes:
            producer = artifact_producers.get(key)
            if producer is not None and producer != idx:
                deps[idx].add(producer)

        template_step_ids, template_artifacts = _extract_template_dependencies(step)
        for dep_step_id in template_step_ids:
            dep_idx = step_id_to_index.get(dep_step_id)
            if dep_idx is not None and dep_idx != idx:
                deps[idx].add(dep_idx)
        for key in template_artifacts:
            producer = artifact_producers.get(key)
            if producer is not None and producer != idx:
                deps[idx].add(producer)

    if _detect_cycle(deps):
        raise ValueError("Dependency graph has a cycle")
    return deps


async def _run_single_step(
    *,
    runtime: AgentRuntime,
    semaphore: asyncio.Semaphore,
    run_id: str,
    actor: str,
    step_index: int,
    tool_call: AgentToolCall,
    attempts: dict[int, int],
    context: dict[str, Any],
    dry_run: bool,
    request_headers: dict[str, str],
    request_id: str | None,
) -> dict[str, Any]:
    attempt = int(attempts.get(step_index) or 0)

    async def _execute_once(current_attempt: int) -> dict[str, Any]:
        async with semaphore:
            return await runtime.execute_tool_call(
                run_id=run_id,
                actor=actor,
                step_index=step_index,
                attempt=current_attempt,
                tool_call=tool_call,
                context=context,
                dry_run=dry_run,
                request_headers=request_headers,
                request_id=request_id,
            )

    while True:
        result = await _execute_once(attempt)
        status = str(result.get("status") or "").strip().lower()
        if status not in _FAILED_STATUSES:
            await runtime.record_event(
                event_type="AGENT_STEP_FINALIZED",
                run_id=run_id,
                actor=actor,
                status="success",
                data={
                    "step_index": step_index,
                    "attempt": attempt,
                    "final_status": status or "success",
                    "output_digest": result.get("output_digest"),
                },
                request_id=request_id,
                step_index=step_index,
                resource_type="agent_step",
            )
            return result

        policy = decide_policy(tool_call=tool_call, result=result, context=context)
        policy_payload = {
            "family": policy.family,
            "recommended_action": policy.recommended_action,
            "safe_to_auto_retry": policy.safe_to_auto_retry,
            "reason": policy.reason,
            "details": policy.details,
        }

        enterprise = result.get("enterprise") if isinstance(result.get("enterprise"), dict) else {}
        enterprise_max_attempts = enterprise.get("max_attempts")
        try:
            enterprise_max_attempts = int(enterprise_max_attempts)
        except (TypeError, ValueError):
            enterprise_max_attempts = None

        max_attempts_cap = max(1, int(getattr(runtime.config, "auto_retry_max_attempts", 1) or 1))
        max_attempts = (
            min(max_attempts_cap, max(1, int(enterprise_max_attempts)))
            if enterprise_max_attempts is not None
            else max_attempts_cap
        )
        auto_retry_enabled = bool(getattr(runtime.config, "auto_retry_enabled", False))
        allow_writes = bool(getattr(runtime.config, "auto_retry_allow_writes", False))
        method = str(getattr(tool_call, "method", "") or "").strip().upper()
        safe_to_retry = policy.safe_to_auto_retry or (allow_writes and method in {"POST", "PUT", "PATCH", "DELETE"})
        attempt_remaining = (attempt + 1) < max_attempts

        if auto_retry_enabled and policy.recommended_action == "retry" and safe_to_retry and attempt_remaining:
            next_attempt = attempt + 1
            attempts[step_index] = next_attempt
            base_delay_ms = enterprise.get("base_delay_ms")
            max_delay_ms = enterprise.get("max_delay_ms")
            jitter_strategy = enterprise.get("jitter_strategy")
            retry_after_header_respect = bool(enterprise.get("retry_after_header_respect"))

            try:
                base_delay_ms = int(base_delay_ms)
            except (TypeError, ValueError):
                base_delay_ms = int(float(getattr(runtime.config, "auto_retry_base_delay_s", 0.0) or 0.0) * 1000)
            try:
                max_delay_ms = int(max_delay_ms)
            except (TypeError, ValueError):
                max_delay_ms = int(float(getattr(runtime.config, "auto_retry_max_delay_s", 0.0) or 0.0) * 1000)

            seed = f"{run_id}:{step_index}:{next_attempt}:{policy.family}"
            retry_after_ms = result.get("retry_after_ms")
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
                run_id=run_id,
                actor=actor,
                status="success",
                data={
                    "step_index": step_index,
                    "attempt": next_attempt,
                    "retry_delay_ms": int(retry_delay_s * 1000),
                    "retry_delay_source": retry_delay_source,
                    "retry_max_attempts": int(max_attempts),
                    "retry_seed": seed,
                    "policy": policy_payload,
                },
                request_id=request_id,
                step_index=step_index,
                resource_type="agent_tool",
            )

            if retry_delay_s > 0:
                await asyncio.sleep(float(retry_delay_s))
            attempt = next_attempt
            continue

        await runtime.record_event(
            event_type="AGENT_STEP_FINALIZED",
            run_id=run_id,
            actor=actor,
            status="failure",
            data={
                "step_index": step_index,
                "attempt": attempt,
                "final_status": status or "failed",
                "output_digest": result.get("output_digest"),
                "error": result.get("error"),
                "policy": policy_payload,
            },
            request_id=request_id,
            step_index=step_index,
            resource_type="agent_step",
            error=str(result.get("error") or ""),
        )
        return result


async def run_agent_graph_parallel(runtime: AgentRuntime, initial_state: AgentState) -> AgentState:
    steps = list(initial_state.get("steps") or [])
    deps = _build_step_dependencies(steps)

    run_id = initial_state["run_id"]
    actor = initial_state["actor"]
    context = initial_state.get("context") or {}
    dry_run = bool(initial_state.get("dry_run"))
    request_headers = initial_state.get("request_headers") or {}
    request_id = initial_state.get("request_id")
    attempts: dict[int, int] = dict(initial_state.get("attempts") or {})

    concurrency = int(getattr(runtime.config, "max_concurrent_tool_calls", 1) or 1)
    concurrency = max(1, concurrency)
    semaphore = asyncio.Semaphore(concurrency)
    fail_fast = bool(getattr(runtime.config, "fail_fast", False))

    results: list[dict[str, Any]] = [{} for _ in range(len(steps))]
    pending: set[int] = set(range(len(steps)))
    running: dict[int, asyncio.Task[dict[str, Any]]] = {}

    completed_success: set[int] = set()
    completed_failure: set[int] = set()
    completed_skipped: set[int] = set()
    saw_failure = False

    async def _finalize_skipped(idx: int, *, reason: str) -> None:
        completed_skipped.add(idx)
        skipped_result = {
            "status": "skipped",
            "error_code": "CONFLICT",
            "error_message": str(reason or "dependency failed"),
            "payload": None,
            "side_effect_summary": {},
            "http_status": None,
            "tool_run_id": None,
            "output_digest": None,
            "duration_ms": 0,
            "data_scope": {},
            "error": str(reason or "dependency failed"),
        }
        results[idx] = skipped_result
        await runtime.record_event(
            event_type="AGENT_STEP_FINALIZED",
            run_id=run_id,
            actor=actor,
            status="success",
            data={
                "step_index": idx,
                "attempt": int(attempts.get(idx) or 0),
                "final_status": "skipped",
                "output_digest": None,
                "reason": str(reason or "dependency failed"),
            },
            request_id=request_id,
            step_index=idx,
            resource_type="agent_step",
        )

    while pending or running:
        if pending:
            blocked = [idx for idx in sorted(pending) if deps[idx] & (completed_failure | completed_skipped)]
            for idx in blocked:
                pending.discard(idx)
                await _finalize_skipped(idx, reason="dependency failed")

        ready: list[int] = []
        if not (fail_fast and saw_failure):
            ready = [idx for idx in sorted(pending) if deps[idx].issubset(completed_success)]

        # Do not overschedule: keep at most `concurrency` steps in-flight.
        # This ensures fail-fast semantics are meaningful (unstarted steps remain pending).
        while ready and len(running) < concurrency:
            idx = ready.pop(0)
            pending.discard(idx)
            task = asyncio.create_task(
                _run_single_step(
                    runtime=runtime,
                    semaphore=semaphore,
                    run_id=run_id,
                    actor=actor,
                    step_index=idx,
                    tool_call=steps[idx],
                    attempts=attempts,
                    context=context,
                    dry_run=dry_run,
                    request_headers=request_headers,
                    request_id=request_id,
                )
            )
            running[idx] = task

        if not running:
            # No runnable work left => unresolved deps or fail-fast cancellation.
            for idx in sorted(pending):
                await _finalize_skipped(idx, reason="no runnable dependencies")
            pending.clear()
            break

        done, _ = await asyncio.wait(list(running.values()), return_when=asyncio.FIRST_COMPLETED)
        for idx, task in list(running.items()):
            if task not in done:
                continue
            running.pop(idx, None)
            result = task.result()
            results[idx] = result
            status = str(result.get("status") or "").strip().lower()
            if status in _FAILED_STATUSES:
                completed_failure.add(idx)
                saw_failure = True
            elif status == "skipped":
                completed_skipped.add(idx)
            else:
                completed_success.add(idx)

    failed = bool(completed_failure)
    return {
        **initial_state,
        "steps": steps,
        "results": results,
        "context": context,
        "attempts": attempts,
        "failed": failed,
        "step_index": len(steps),
    }


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

    failed_statuses = {"failed", "failure"}
    if status not in failed_statuses:
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
    if bool(getattr(getattr(runtime, "config", None), "parallel_execution_enabled", False)):
        return await run_agent_graph_parallel(runtime, initial_state)
    app = build_agent_graph(runtime)
    return await app.ainvoke(initial_state)
