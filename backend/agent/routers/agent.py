from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request

from agent.models import AgentRunRequest
from agent.services.agent_graph import AgentState, run_agent_graph
from agent.services.agent_runtime import AgentRuntime
from shared.models.responses import ApiResponse
from shared.utils.llm_safety import digest_for_audit, mask_pii

router = APIRouter(prefix="/agent", tags=["Agent"])


def _resolve_principal(request: Optional[Request]) -> tuple[str, str]:
    headers = request.headers if request else {}
    principal_id = (
        headers.get("X-Principal-Id")
        or headers.get("X-User")
        or headers.get("X-Actor")
        or headers.get("X-User-Id")
        or headers.get("X-User-ID")
        or ""
    ).strip()
    principal_type = (
        headers.get("X-Principal-Type")
        or headers.get("X-Actor-Type")
        or headers.get("X-User-Type")
        or "user"
    ).strip()
    if not principal_id:
        principal_id = "system"
    return principal_type.lower(), principal_id


def _actor_label(principal_type: str, principal_id: str) -> str:
    principal_type = principal_type or "user"
    principal_id = principal_id or "unknown"
    return f"{principal_type}:{principal_id}"


def _request_meta(request: Request, body: AgentRunRequest) -> Dict[str, Any]:
    return {
        "request_id": body.request_id or request.headers.get("X-Request-Id"),
        "ip": request.client.host if request.client else None,
        "user_agent": request.headers.get("User-Agent"),
    }


async def _execute_agent_run(
    *,
    runtime: AgentRuntime,
    state: AgentState,
    request_id: Optional[str],
) -> None:
    run_id = state["run_id"]
    actor = state["actor"]
    steps_total = len(state.get("steps", []))
    try:
        final_state = await run_agent_graph(runtime, state)
        failed = bool(final_state.get("failed"))
        event_type = "AGENT_RUN_FAILED" if failed else "AGENT_RUN_COMPLETED"
        results_digest = digest_for_audit(final_state.get("results", []))
        await runtime.record_event(
            event_type=event_type,
            run_id=run_id,
            actor=actor,
            status="failure" if failed else "success",
            data={
                "steps_total": steps_total,
                "steps_completed": final_state.get("step_index", 0),
                "results_digest": results_digest,
                "failed": failed,
            },
            request_id=request_id,
        )
    except Exception as exc:
        await runtime.record_event(
            event_type="AGENT_RUN_FAILED",
            run_id=run_id,
            actor=actor,
            status="failure",
            data={
                "steps_total": steps_total,
                "steps_completed": state.get("step_index", 0),
            },
            request_id=request_id,
            error=str(exc),
        )


@router.post("/runs")
async def create_agent_run(request: Request, body: AgentRunRequest) -> Dict[str, Any]:
    if not body.steps:
        raise HTTPException(status_code=400, detail="steps are required")
    max_steps = int(os.getenv("AGENT_RUN_MAX_STEPS", "50"))
    if len(body.steps) > max_steps:
        raise HTTPException(status_code=400, detail="steps exceed AGENT_RUN_MAX_STEPS")

    principal_type, principal_id = _resolve_principal(request)
    actor = _actor_label(principal_type, principal_id)
    runtime = AgentRuntime.from_env(
        event_store=request.app.state.event_store,  # type: ignore[attr-defined]
        audit_store=request.app.state.audit_store,  # type: ignore[attr-defined]
    )

    run_id = str(uuid4())
    request_meta = _request_meta(request, body)
    masked_context = mask_pii(body.context or {}, max_string_chars=200)
    await runtime.record_event(
        event_type="AGENT_RUN_STARTED",
        run_id=run_id,
        actor=actor,
        status="success",
        data={
            "goal": body.goal,
            "steps_total": len(body.steps),
            "dry_run": body.dry_run,
            "context_digest": digest_for_audit(masked_context),
            "context": masked_context,
            "request_meta": request_meta,
        },
        request_id=request_meta.get("request_id"),
    )

    state: AgentState = {
        "run_id": run_id,
        "actor": actor,
        "steps": body.steps,
        "step_index": 0,
        "results": [],
        "context": body.context or {},
        "dry_run": bool(body.dry_run),
        "request_headers": dict(request.headers),
        "request_id": request_meta.get("request_id"),
        "failed": False,
    }

    task = asyncio.create_task(
        _execute_agent_run(runtime=runtime, state=state, request_id=request_meta.get("request_id"))
    )
    request.app.state.agent_tasks[run_id] = task  # type: ignore[attr-defined]
    task.add_done_callback(
        lambda _: request.app.state.agent_tasks.pop(run_id, None)  # type: ignore[attr-defined]
    )

    response = ApiResponse.accepted(
        "Agent run accepted",
        data={
            "run_id": run_id,
            "status": "running",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "steps_count": len(body.steps),
        },
    )
    return response.to_dict()


@router.get("/runs/{run_id}")
async def get_agent_run(
    request: Request,
    run_id: str,
    include_events: bool = Query(False, description="Include event payloads"),
    limit: int = Query(200, ge=1, le=1000),
) -> Dict[str, Any]:
    event_store = request.app.state.event_store  # type: ignore[attr-defined]
    events = await event_store.get_events("AgentRun", run_id)
    if not events:
        raise HTTPException(status_code=404, detail="run not found")

    steps_total = 0
    steps_completed = 0
    failed_step: Optional[int] = None
    started_at = None
    completed_at = None
    status = "running"

    for event in events:
        if event.event_type == "AGENT_RUN_STARTED":
            steps_total = int(event.data.get("steps_total") or 0)
            started_at = event.occurred_at
        if event.event_type == "AGENT_TOOL_RESULT":
            steps_completed += 1
            if event.data.get("error") and failed_step is None:
                failed_step = int(event.data.get("step_index") or 0)
        if event.event_type == "AGENT_RUN_COMPLETED":
            status = "completed"
            completed_at = event.occurred_at
        if event.event_type == "AGENT_RUN_FAILED":
            status = "failed"
            completed_at = event.occurred_at

    payload_events: List[Dict[str, Any]] = []
    if include_events:
        payload_events = [event.model_dump(mode="json") for event in events[:limit]]

    response = ApiResponse.success(
        "Agent run fetched",
        data={
            "run_id": run_id,
            "status": status,
            "started_at": started_at,
            "completed_at": completed_at,
            "steps_total": steps_total,
            "steps_completed": steps_completed,
            "failed_step": failed_step,
            "events": payload_events,
        },
    )
    return response.to_dict()


@router.get("/runs/{run_id}/events")
async def list_agent_run_events(
    request: Request,
    run_id: str,
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    event_store = request.app.state.event_store  # type: ignore[attr-defined]
    events = await event_store.get_events("AgentRun", run_id)
    if not events:
        raise HTTPException(status_code=404, detail="run not found")
    sliced = events[offset : offset + limit]
    response = ApiResponse.success(
        "Agent run events fetched",
        data={
            "run_id": run_id,
            "count": len(sliced),
            "events": [event.model_dump(mode="json") for event in sliced],
        },
    )
    return response.to_dict()
