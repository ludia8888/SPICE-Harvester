from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any, Dict
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request

from agent.models_pipeline import PipelineAgentRunRequest
from agent.services.agent_runtime import AgentRuntime
from agent.services.pipeline_agent_graph import build_pipeline_agent_state, run_pipeline_agent_graph
from shared.config.settings import get_settings
from shared.models.responses import ApiResponse
from shared.services.agent_session_registry import AgentSessionRegistry
from shared.utils.llm_safety import digest_for_audit, mask_pii

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agent", tags=["Agent"])

PIPELINE_AGENT_TOOL_IDS = [
    "pipeline_plans.context_pack",
    "pipeline_plans.task_spec",
    "pipeline_plans.clarify_scope",
    "pipeline_plans.join_keys",
    "pipeline_plans.compile",
    "pipeline_plans.split_outputs",
    "pipeline_plans.transform",
    "pipeline_plans.verify_intent",
    "pipeline_plans.preview",
    "pipeline_plans.evaluate_joins",
    "pipeline_plans.inspect_preview",
    "pipeline_plans.cleanse",
    "pipeline_plans.repair",
    "pipeline_plans.generate_specs",
]


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


def _resolve_tenant_id(request: Request) -> str:
    candidate = request.headers.get("X-Tenant-ID") or request.headers.get("X-Org-ID") or "default"
    return str(candidate).strip() or "default"


def _resolve_session_owner(request: Request, principal_id: str) -> str:
    delegated = request.headers.get("Authorization") or request.headers.get("X-Delegated-Authorization")
    if delegated:
        return principal_id or "system"
    dev_id = str(get_settings().auth.dev_master_user_id or "").strip()
    if dev_id:
        return dev_id
    return principal_id or "system"


@router.post("/pipeline-runs", response_model=ApiResponse)
async def run_pipeline_agent(request: Request, body: PipelineAgentRunRequest) -> ApiResponse:
    principal_type, principal_id = _resolve_principal(request)
    actor = _actor_label(principal_type, principal_id)
    runtime = AgentRuntime.from_env(
        event_store=request.app.state.event_store,  # type: ignore[attr-defined]
        audit_store=request.app.state.audit_store,  # type: ignore[attr-defined]
    )
    if request.headers.get("Authorization") or request.headers.get("X-Delegated-Authorization"):
        runtime.config = replace(runtime.config, bff_token=None)

    request_id = request.headers.get("X-Request-Id")
    state = build_pipeline_agent_state(
        goal=body.goal,
        data_scope=body.data_scope.model_dump(mode="json"),
        session_id=None,
        answers=body.answers,
        planner_hints=body.planner_hints,
        task_spec=body.task_spec,
        output_bindings=(
            {key: value.model_dump(mode="json") for key, value in body.output_bindings.items()}
            if body.output_bindings
            else None
        ),
        preview_node_id=body.preview_node_id,
        preview_limit=body.preview_limit,
        include_run_tables=body.include_run_tables,
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

    session_registry: AgentSessionRegistry | None = getattr(request.app.state, "agent_session_registry", None)
    if session_registry is None:
        raise HTTPException(status_code=500, detail="AgentSessionRegistry not initialized")
    session_id = str(uuid4())
    tenant_id = _resolve_tenant_id(request)
    created_by = _resolve_session_owner(request, principal_id)
    try:
        await session_registry.create_session(
            session_id=session_id,
            tenant_id=tenant_id,
            created_by=created_by,
            enabled_tools=PIPELINE_AGENT_TOOL_IDS,
            metadata={"source": "pipeline_agent", "run_id": state["run_id"]},
        )
    except Exception as exc:
        logger.error("Failed to create pipeline agent session: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to initialize agent session") from exc
    state["session_id"] = session_id

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
        "session_id": final_state.get("session_id"),
        "plan_id": final_state.get("plan_id"),
        "plan": final_state.get("plan"),
        "task_spec": final_state.get("task_spec"),
        "report": final_state.get("report"),
        "preflight": final_state.get("preflight"),
        "preview": final_state.get("preview"),
        "cleansing_inspector": final_state.get("cleansing_inspector"),
        "cleansing_actions": final_state.get("cleansing_actions"),
        "join_plan": final_state.get("join_hints"),
        "join_evaluation": final_state.get("join_evaluation"),
        "join_evaluation_warnings": final_state.get("join_evaluation_warnings"),
        "intent_status": final_state.get("intent_status"),
        "intent_issues": final_state.get("intent_issues"),
        "intent_actions": final_state.get("intent_actions"),
        "intent_warnings": final_state.get("intent_warnings"),
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
    if status not in {"success"} and (
        result_payload.get("questions")
        or result_payload.get("intent_status") == "needs_revision"
        or result_payload.get("validation_errors")
    ):
        return ApiResponse.warning(message="Pipeline agent needs clarification", data=result_payload).to_dict()
    if status not in {"success"}:
        raise HTTPException(status_code=500, detail={"message": "Pipeline agent failed", "errors": errors, "data": result_payload})

    return ApiResponse.success(message="Pipeline agent completed", data=result_payload).to_dict()
