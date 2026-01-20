from __future__ import annotations

import logging
from typing import Any, Dict
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request

from agent.models_pipeline import PipelineAgentRunRequest
from agent.services.agent_runtime import AgentRuntime
from agent.services.pipeline_agent_graph import build_pipeline_agent_state, run_pipeline_agent_graph
from shared.models.responses import ApiResponse
from shared.utils.llm_safety import digest_for_audit, mask_pii

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agent", tags=["Agent"])


def _resolve_principal(request: Request) -> tuple[str, str]:
    headers = request.headers
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


@router.post("/pipeline-runs", response_model=ApiResponse)
async def run_pipeline_agent(request: Request, body: PipelineAgentRunRequest) -> ApiResponse:
    principal_type, principal_id = _resolve_principal(request)
    actor = _actor_label(principal_type, principal_id)
    runtime = AgentRuntime.from_env(
        event_store=request.app.state.event_store,  # type: ignore[attr-defined]
        audit_store=request.app.state.audit_store,  # type: ignore[attr-defined]
    )

    request_id = request.headers.get("X-Request-Id")
    state = build_pipeline_agent_state(
        goal=body.goal,
        data_scope=body.data_scope.model_dump(mode="json"),
        answers=body.answers,
        planner_hints=body.planner_hints,
        output_bindings=(
            {key: value.model_dump(mode="json") for key, value in body.output_bindings.items()}
            if body.output_bindings
            else None
        ),
        preview_node_id=body.preview_node_id,
        preview_limit=body.preview_limit,
        max_repairs=body.max_repairs,
        max_cleansing=body.max_cleansing,
        max_transform=body.max_transform,
        apply_specs=body.apply_specs,
        auto_sync=body.auto_sync,
        ontology_branch=body.ontology_branch,
        dangling_policy=body.dangling_policy,
        dedupe_policy=body.dedupe_policy,
        request_headers=dict(request.headers),
        request_id=request_id,
        actor=actor,
    )

    masked_scope = mask_pii(state.get("data_scope") or {}, max_string_chars=200)
    await runtime.record_event(
        event_type="PIPELINE_AGENT_STARTED",
        run_id=state["run_id"],
        actor=actor,
        status="success",
        data={
            "goal": body.goal,
            "data_scope": masked_scope,
            "apply_specs": bool(body.apply_specs),
            "max_repairs": int(body.max_repairs),
            "request_id": request_id,
        },
        request_id=request_id,
    )

    final_state = await run_pipeline_agent_graph(runtime, state)
    status = str(final_state.get("status") or "failed")
    errors = []
    if final_state.get("error"):
        errors.append(str(final_state.get("error")))

    result_payload: Dict[str, Any] = {
        "run_id": final_state.get("run_id"),
        "status": status,
        "plan_id": final_state.get("plan_id"),
        "plan": final_state.get("plan"),
        "preflight": final_state.get("preflight"),
        "preview": final_state.get("preview"),
        "cleansing_inspector": final_state.get("cleansing_inspector"),
        "cleansing_actions": final_state.get("cleansing_actions"),
        "join_plan": final_state.get("join_hints"),
        "specs": final_state.get("specs"),
        "questions": final_state.get("questions"),
        "validation_errors": final_state.get("validation_errors"),
        "validation_warnings": final_state.get("validation_warnings"),
        "repair_attempts": final_state.get("repair_attempts"),
        "cleansing_attempts": final_state.get("cleansing_attempts"),
        "transform_attempts": final_state.get("transform_attempts"),
    }

    await runtime.record_event(
        event_type="PIPELINE_AGENT_COMPLETED",
        run_id=final_state.get("run_id") or str(uuid4()),
        actor=actor,
        status="success" if status in {"success", "partial"} else "failure",
        data={
            "status": status,
            "result_digest": digest_for_audit(result_payload),
            "errors": errors,
        },
        request_id=request_id,
    )

    if status in {"clarification_required"}:
        return ApiResponse.warning(message="Pipeline agent needs clarification", data=result_payload).to_dict()
    if status in {"partial"}:
        return ApiResponse.partial(
            message="Pipeline agent completed with warnings",
            data=result_payload,
            errors=errors or None,
        ).to_dict()
    if status not in {"success"}:
        raise HTTPException(status_code=500, detail={"message": "Pipeline agent failed", "errors": errors, "data": result_payload})

    return ApiResponse.success(message="Pipeline agent completed", data=result_payload).to_dict()
