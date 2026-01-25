from __future__ import annotations

"""
Agent Service step runner (single sequential loop).

This intentionally avoids graph-based orchestration and any parallel scheduling. Agent Plans are
validated in the BFF to ensure dependencies (templates/artifacts) only reference
prior steps, so a simple in-order executor is sufficient and easier to audit.
"""

import asyncio
from typing import Any, Dict, List, TypedDict

from agent.models import AgentToolCall
from agent.services.agent_policy import compute_retry_delay_s, decide_policy
from agent.services.agent_runtime import AgentRuntime

_FAILED_STATUSES = {"failed", "failure"}


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
    """
    Execute exactly one tool-call step, applying deterministic enterprise retry policy.
    """

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


async def run_agent_steps(runtime: AgentRuntime, initial_state: AgentState) -> AgentState:
    """
    Execute the provided steps in order, stopping at the first failure.

    Notes:
    - BFF plan validation guarantees artifacts/templates only depend on *prior* steps,
      so we do not attempt any dependency scheduling here.
    - The runtime records tool events; we add a deterministic AGENT_STEP_FINALIZED
      event per step (success/failure) for run progress.
    """

    steps = list(initial_state.get("steps") or [])
    run_id = initial_state["run_id"]
    actor = initial_state["actor"]
    context = initial_state.get("context") or {}
    dry_run = bool(initial_state.get("dry_run"))
    request_headers = initial_state.get("request_headers") or {}
    request_id = initial_state.get("request_id")
    attempts: dict[int, int] = dict(initial_state.get("attempts") or {})

    start_index = max(0, int(initial_state.get("step_index") or 0))
    results: list[dict[str, Any]] = list(initial_state.get("results") or [])

    semaphore = asyncio.Semaphore(1)
    failed = False
    step_index = start_index

    for idx in range(start_index, len(steps)):
        step_index = idx
        tool_call = steps[idx]
        result = await _run_single_step(
            runtime=runtime,
            semaphore=semaphore,
            run_id=run_id,
            actor=actor,
            step_index=idx,
            tool_call=tool_call,
            attempts=attempts,
            context=context,
            dry_run=dry_run,
            request_headers=request_headers,
            request_id=request_id,
        )
        results.append(result)
        status = str(result.get("status") or "").strip().lower()
        if status in _FAILED_STATUSES:
            failed = True
            step_index = idx + 1
            break
        step_index = idx + 1

    return {
        **initial_state,
        "steps": steps,
        "results": results,
        "context": context,
        "attempts": attempts,
        "failed": failed,
        "step_index": step_index,
        "pending_result": None,
        "next_action": "end",
        "retry_delay_s": None,
        "policy": None,
    }
