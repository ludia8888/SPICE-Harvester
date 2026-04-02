from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Query, Request

from agent.models import AgentRunRequest, AgentToolCall
from agent.services.agent_run_loop import AgentState, run_agent_steps
from agent.services.agent_runtime import AgentRuntime
from shared.config.settings import get_settings
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.services.registries.agent_registry import AgentRegistry
from shared.models.responses import ApiResponse
from shared.security.principal_utils import actor_label, resolve_principal_from_headers
from shared.utils.llm_safety import digest_for_audit, mask_pii
import logging

router = APIRouter(prefix="/agent", tags=["Agent"])


def _resolve_principal(request: Optional[Request]) -> tuple[str, str]:
    headers = request.headers if request else None
    return resolve_principal_from_headers(headers)


def _resolve_tenant_id(request: Optional[Request]) -> str:
    if not request:
        return "default"
    candidate = request.headers.get("X-Tenant-ID") or request.headers.get("X-Org-ID") or "default"
    return str(candidate).strip() or "default"


def _actor_label(principal_type: str, principal_id: str) -> str:
    return actor_label(principal_type, principal_id)


def _require_event_store(request: Request) -> Any:
    event_store = getattr(request.app.state, "event_store", None)
    if event_store is None:
        raise classified_http_exception(
            503,
            "Agent event store unavailable",
            code=ErrorCode.UPSTREAM_UNAVAILABLE,
        )
    return event_store


def _request_meta(request: Request, body: AgentRunRequest) -> Dict[str, Any]:
    return {
        "request_id": body.request_id or request.headers.get("X-Request-Id"),
        "ip": request.client.host if request.client else None,
        "user_agent": request.headers.get("User-Agent"),
    }


def _step_id(index: int) -> str:
    return f"step_{index}"


def _resolve_tool_id(tool_call: AgentToolCall) -> str:
    tool_id = (tool_call.tool_id or "").strip()
    if tool_id:
        return tool_id
    method = (tool_call.method or "POST").strip().upper()
    path = (tool_call.path or "").strip()
    return f"{tool_call.service}:{method}:{path}"


def _extract_plan_id(context: Dict[str, Any]) -> Optional[str]:
    if not context:
        return None
    for key in ("plan_id", "planId"):
        value = context.get(key)
        if value:
            try:
                return str(UUID(str(value)))
            except Exception:
                logging.getLogger(__name__).warning("Exception fallback at agent/routers/agent.py:68", exc_info=True)
                return None
    return None


def _extract_plan_snapshot(context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not context:
        return None
    candidate = context.get("plan_snapshot") or context.get("plan")
    if isinstance(candidate, dict):
        return candidate
    return None


def _extract_risk_level(context: Dict[str, Any]) -> str:
    if not context:
        return "read"
    raw = str(context.get("risk_level") or "read").strip().lower()
    return raw or "read"


async def _record_run_start(
    *,
    agent_registry: Optional[AgentRegistry],
    run_id: str,
    tenant_id: str,
    actor: str,
    requester: str,
    delegated_actor: str,
    body: AgentRunRequest,
    request_meta: Dict[str, Any],
) -> None:
    if not agent_registry:
        return
    masked_context = mask_pii(body.context or {}, max_string_chars=200)
    plan_snapshot = _extract_plan_snapshot(body.context or {})
    if plan_snapshot is not None:
        plan_snapshot = mask_pii(plan_snapshot, max_string_chars=200)
    await agent_registry.create_run(
        run_id=run_id,
        tenant_id=tenant_id,
        plan_id=_extract_plan_id(body.context or {}),
        status="RUNNING",
        risk_level=_extract_risk_level(body.context or {}),
        requester=requester,
        delegated_actor=delegated_actor,
        context={**masked_context, "request_meta": request_meta},
        plan_snapshot=plan_snapshot or {},
    )
    for idx, step in enumerate(body.steps):
        tool_id = _resolve_tool_id(step)
        input_digest = digest_for_audit({"query": step.query, "body": step.body})
        await agent_registry.create_step(
            run_id=run_id,
            step_id=_step_id(idx),
            tenant_id=tenant_id,
            tool_id=tool_id,
            status="PENDING",
            input_digest=input_digest,
            metadata={
                "service": step.service,
                "method": step.method,
                "path": step.path,
                "description": step.description,
                "data_scope": mask_pii(step.data_scope or {}, max_string_chars=200),
            },
        )


async def _mark_run_start_failed(
    *,
    agent_registry: Optional[AgentRegistry],
    run_id: str,
    tenant_id: str,
    body: AgentRunRequest,
    error: str,
) -> None:
    if not agent_registry:
        return
    finished_at = datetime.now(timezone.utc)
    await agent_registry.update_run_status(
        run_id=run_id,
        tenant_id=tenant_id,
        status="FAILED",
        finished_at=finished_at,
    )
    for idx, _step in enumerate(body.steps):
        await agent_registry.update_step_status(
            run_id=run_id,
            step_id=_step_id(idx),
            tenant_id=tenant_id,
            status="FAILED" if idx == 0 else "SKIPPED",
            error=error if idx == 0 else None,
            finished_at=finished_at,
        )


async def _execute_agent_run(
    *,
    runtime: AgentRuntime,
    state: AgentState,
    request_id: Optional[str],
    agent_registry: Optional[AgentRegistry],
    tenant_id: str,
) -> None:
    run_id = state["run_id"]
    actor = state["actor"]
    steps_total = len(state.get("steps", []))
    try:
        final_state = await run_agent_steps(runtime, state)
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
        if agent_registry:
            results = final_state.get("results", []) or []
            steps = final_state.get("steps", []) or []
            for idx, step in enumerate(steps):
                result = results[idx] if idx < len(results) else None
                if result is None:
                    status = "SKIPPED"
                    output_digest = None
                    error = None
                else:
                    status_raw = str(result.get("status") or "").upper()
                    status = status_raw or "UNKNOWN"
                    if status == "SUCCESS":
                        status = "SUCCESS"
                    elif status in {"FAILED", "FAILURE"}:
                        status = "FAILED"
                    elif status == "SKIPPED":
                        status = "SKIPPED"
                    else:
                        status = status_raw or "UNKNOWN"
                    output_digest = result.get("output_digest")
                    error = result.get("error")
                await agent_registry.update_step_status(
                    run_id=run_id,
                    step_id=_step_id(idx),
                    tenant_id=tenant_id,
                    status=status,
                    output_digest=output_digest,
                    error=error,
                    finished_at=datetime.now(timezone.utc),
                )
            await agent_registry.update_run_status(
                run_id=run_id,
                tenant_id=tenant_id,
                status="FAILED" if failed else "COMPLETED",
                finished_at=datetime.now(timezone.utc),
            )
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception fallback at agent/routers/agent.py:203", exc_info=True)
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
        if agent_registry:
            await agent_registry.update_run_status(
                run_id=run_id,
                tenant_id=tenant_id,
                status="FAILED",
                finished_at=datetime.now(timezone.utc),
            )
            steps = state.get("steps", []) or []
            failed_step_index = int(state.get("step_index", 0) or 0)
            for idx in range(len(steps)):
                if idx < failed_step_index:
                    status = "COMPLETED"
                elif idx == failed_step_index:
                    status = "FAILED"
                else:
                    status = "SKIPPED"
                await agent_registry.update_step_status(
                    run_id=run_id,
                    step_id=_step_id(idx),
                    tenant_id=tenant_id,
                    status=status,
                    finished_at=datetime.now(timezone.utc),
                )


# Internal: Agent service (port 8004) is not directly exposed to clients.
@router.post("/runs", include_in_schema=False)
async def create_agent_run(request: Request, body: AgentRunRequest) -> Dict[str, Any]:
    if not body.steps:
        raise classified_http_exception(400, "steps are required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
    max_steps = get_settings().agent.run_max_steps
    if len(body.steps) > max_steps:
        raise classified_http_exception(400, "steps exceed AGENT_RUN_MAX_STEPS", code=ErrorCode.REQUEST_VALIDATION_FAILED)

    principal_type, principal_id = _resolve_principal(request)
    actor = _actor_label(principal_type, principal_id)
    tenant_id = _resolve_tenant_id(request)
    event_store = _require_event_store(request)
    runtime = AgentRuntime.from_env(
        event_store=event_store,
        audit_store=request.app.state.audit_store,  # type: ignore[attr-defined]
    )
    agent_registry = request.app.state.agent_registry  # type: ignore[attr-defined]

    run_id = str(uuid4())
    request_meta = _request_meta(request, body)
    masked_context = mask_pii(body.context or {}, max_string_chars=200)
    await _record_run_start(
        agent_registry=agent_registry,
        run_id=run_id,
        tenant_id=tenant_id,
        actor=actor,
        requester=principal_id,
        delegated_actor=request.headers.get("X-Actor") or "agent",
        body=body,
        request_meta=request_meta,
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
        "attempts": {},
        "pending_result": None,
        "next_action": "continue",
        "retry_delay_s": None,
        "policy": None,
    }

    task = asyncio.create_task(
        _execute_agent_run(
            runtime=runtime,
            state=state,
            request_id=request_meta.get("request_id"),
            agent_registry=agent_registry,
            tenant_id=tenant_id,
        )
    )
    request.app.state.agent_tasks[run_id] = task  # type: ignore[attr-defined]
    task.add_done_callback(
        lambda _: request.app.state.agent_tasks.pop(run_id, None)  # type: ignore[attr-defined]
    )
    try:
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
    except Exception as exc:
        task.cancel()
        request.app.state.agent_tasks.pop(run_id, None)  # type: ignore[attr-defined]
        try:
            await _mark_run_start_failed(
                agent_registry=agent_registry,
                run_id=run_id,
                tenant_id=tenant_id,
                body=body,
                error=str(exc) or exc.__class__.__name__,
            )
        except Exception:
            logging.getLogger(__name__).warning(
                "Failed to mark agent run start failure in registry",
                exc_info=True,
            )
        raise

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


# Internal: Agent service (port 8004) is not directly exposed to clients.
@router.get("/runs/{run_id}", include_in_schema=False)
async def get_agent_run(
    request: Request,
    run_id: str,
    include_events: bool = Query(False, description="Include event payloads"),
    limit: int = Query(200, ge=1, le=1000),
) -> Dict[str, Any]:
    event_store = _require_event_store(request)
    events = await event_store.get_events("AgentRun", run_id)
    if not events:
        raise classified_http_exception(404, "run not found", code=ErrorCode.RESOURCE_NOT_FOUND)

    steps_total = 0
    steps_completed = 0
    failed_step: Optional[int] = None
    started_at = None
    completed_at = None
    status = "running"
    latest_progress: Optional[Dict[str, Any]] = None
    latest_progress_at: Optional[datetime] = None
    finalized_steps: set[int] = set()
    saw_step_finalized = False

    for event in events:
        if event.event_type == "AGENT_RUN_STARTED":
            steps_total = int(event.data.get("steps_total") or 0)
            started_at = event.occurred_at
        if event.event_type == "AGENT_STEP_FINALIZED":
            saw_step_finalized = True
            try:
                step_idx = int(event.data.get("step_index") or 0)
            except Exception:
                logging.getLogger(__name__).warning("Exception fallback at agent/routers/agent.py:357", exc_info=True)
                step_idx = 0
            finalized_steps.add(step_idx)
            meta = event.metadata if isinstance(event.metadata, dict) else {}
            if meta.get("status") == "failure" and failed_step is None:
                failed_step = step_idx
        if event.event_type == "AGENT_RUN_COMPLETED":
            status = "completed"
            completed_at = event.occurred_at
        if event.event_type == "AGENT_RUN_FAILED":
            status = "failed"
            completed_at = event.occurred_at
        if event.event_type == "AGENT_TOOL_PROGRESS":
            if latest_progress_at is None or event.occurred_at > latest_progress_at:
                latest_progress_at = event.occurred_at
                latest_progress = event.data

    if saw_step_finalized:
        steps_completed = len(finalized_steps)
    else:
        # Back-compat for historical runs (pre retry-aware step finalization).
        steps_completed = sum(1 for ev in events if ev.event_type == "AGENT_TOOL_RESULT")
        if failed_step is None:
            for ev in events:
                if ev.event_type == "AGENT_TOOL_RESULT" and ev.data.get("error"):
                    try:
                        failed_step = int(ev.data.get("step_index") or 0)
                    except Exception:
                        logging.getLogger(__name__).warning("Exception fallback at agent/routers/agent.py:384", exc_info=True)
                        failed_step = 0
                    break

    payload_events: List[Dict[str, Any]] = []
    if include_events:
        payload_events = [event.model_dump(mode="json") for event in events[:limit]]

    progress_payload = None
    if latest_progress:
        progress_payload = {
            "command_id": latest_progress.get("command_id"),
            "status": latest_progress.get("status"),
            "step_index": latest_progress.get("step_index"),
            "progress": latest_progress.get("progress"),
            "updated_at": latest_progress_at.isoformat() if latest_progress_at else None,
        }

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
            "progress": progress_payload,
            "events": payload_events,
        },
    )
    return response.to_dict()


# Internal: Agent service (port 8004) is not directly exposed to clients.
@router.get("/runs/{run_id}/events", include_in_schema=False)
async def list_agent_run_events(
    request: Request,
    run_id: str,
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    event_store = _require_event_store(request)
    events = await event_store.get_events("AgentRun", run_id)
    if not events:
        raise classified_http_exception(404, "run not found", code=ErrorCode.RESOURCE_NOT_FOUND)
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
