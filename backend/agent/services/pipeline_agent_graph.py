from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict
from uuid import uuid4

from langgraph.graph import END, StateGraph

from agent.models import AgentToolCall
from agent.services.agent_runtime import AgentRuntime


class PipelineAgentState(TypedDict):
    run_id: str
    actor: str
    goal: str
    data_scope: Dict[str, Any]
    answers: Dict[str, Any] | None
    planner_hints: Dict[str, Any] | None
    join_hints: List[Dict[str, Any]] | None
    cleansing_hints: List[Dict[str, Any]] | None
    output_bindings: Dict[str, Any] | None
    preview_node_id: Optional[str]
    preview_limit: int
    include_run_tables: bool
    max_repairs: int
    repair_attempts: int
    max_cleansing: int
    cleansing_attempts: int
    max_transform: int
    transform_attempts: int
    apply_specs: bool
    auto_sync: bool
    ontology_branch: Optional[str]
    dangling_policy: str
    dedupe_policy: str
    request_headers: Dict[str, str]
    request_id: Optional[str]
    context_pack: Optional[Dict[str, Any]]
    plan_id: Optional[str]
    plan: Optional[Dict[str, Any]]
    validation_errors: List[str]
    validation_warnings: List[str]
    preflight: Optional[Dict[str, Any]]
    preview: Optional[Dict[str, Any]]
    run_tables: Optional[Dict[str, Any]]
    definition_digest: Optional[str]
    cleansing_inspector: Optional[Dict[str, Any]]
    cleansing_actions: Optional[List[Dict[str, Any]]]
    join_evaluation: Optional[List[Dict[str, Any]]]
    join_evaluation_warnings: Optional[List[str]]
    questions: List[Dict[str, Any]]
    specs: Optional[List[Dict[str, Any]]]
    status: str
    error: Optional[str]
    next_action: str


def _api_data(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload.get("data") or {}
    return {}


def _api_status(payload: Any) -> str:
    if isinstance(payload, dict):
        return str(payload.get("status") or "").strip().lower()
    return ""


def _merge_planner_hints(current: Optional[Dict[str, Any]], updates: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(current or {})
    for key, value in updates.items():
        if value is None:
            continue
        if key in merged:
            continue
        merged[key] = value
    return merged


def _select_join_hints(context_pack: Optional[Dict[str, Any]], *, max_items: int = 5) -> List[Dict[str, Any]]:
    if not isinstance(context_pack, dict):
        return []
    suggestions = context_pack.get("integration_suggestions")
    if not isinstance(suggestions, dict):
        return []
    candidates = suggestions.get("join_key_candidates")
    if not isinstance(candidates, list):
        return []
    trimmed: List[Dict[str, Any]] = []
    for item in candidates[: max(0, int(max_items))]:
        if not isinstance(item, dict):
            continue
        trimmed.append(
            {
                "left_dataset_id": item.get("left_dataset_id"),
                "right_dataset_id": item.get("right_dataset_id"),
                "left_column": item.get("left_column"),
                "right_column": item.get("right_column"),
                "score": item.get("score"),
                "reasons": item.get("reasons"),
            }
        )
    return trimmed


def _select_cleansing_hints(context_pack: Optional[Dict[str, Any]], *, max_items: int = 12) -> List[Dict[str, Any]]:
    if not isinstance(context_pack, dict):
        return []
    suggestions = context_pack.get("integration_suggestions")
    if not isinstance(suggestions, dict):
        return []
    candidates = suggestions.get("cleansing_suggestions")
    if not isinstance(candidates, list):
        return []
    trimmed: List[Dict[str, Any]] = []
    for item in candidates[: max(0, int(max_items))]:
        if not isinstance(item, dict):
            continue
        trimmed.append(
            {
                "dataset_id": item.get("dataset_id"),
                "column": item.get("column"),
                "suggestion": item.get("suggestion"),
                "evidence": item.get("evidence"),
            }
        )
    return trimmed


def _needs_output_split(plan: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(plan, dict):
        return False
    outputs = plan.get("outputs")
    if not isinstance(outputs, list):
        return False
    for output in outputs:
        if not isinstance(output, dict):
            continue
        kind = str(output.get("output_kind") or "unknown").strip().lower()
        if kind == "unknown":
            return True
        if kind == "object":
            if not str(output.get("target_class_id") or "").strip():
                return True
        if kind == "link":
            required = [
                "link_type_id",
                "source_class_id",
                "target_class_id",
                "predicate",
                "cardinality",
                "source_key_column",
                "target_key_column",
                "relationship_spec_type",
            ]
            if any(not str(output.get(field) or "").strip() for field in required):
                return True
    return False


def _only_output_errors(errors: List[str]) -> bool:
    if not errors:
        return False
    for err in errors:
        if not str(err).strip().lower().startswith("output "):
            return False
    return True


async def _call_bff(
    *,
    runtime: AgentRuntime,
    state: PipelineAgentState,
    step_id: str,
    method: str,
    path: str,
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    tool_call = AgentToolCall(
        step_id=step_id,
        service="bff",
        method=method,
        path=path,
        body=body,
        query={},
    )
    return await runtime.execute_tool_call(
        run_id=state["run_id"],
        actor=state["actor"],
        step_index=0,
        attempt=0,
        tool_call=tool_call,
        context={},
        dry_run=False,
        request_headers=state.get("request_headers") or {},
        request_id=state.get("request_id"),
    )


async def _build_context_pack(state: PipelineAgentState, runtime: AgentRuntime) -> PipelineAgentState:
    scope = state.get("data_scope") or {}
    db_name = str(scope.get("db_name") or "").strip()
    if not db_name:
        return {**state, "next_action": "end", "status": "failed", "error": "data_scope.db_name required"}

    body = {
        "db_name": db_name,
        "branch": scope.get("branch"),
        "dataset_ids": scope.get("dataset_ids") or [],
    }
    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id="pipeline_context_pack",
        method="POST",
        path="/api/v1/pipeline-plans/context-pack",
        body=body,
    )
    if result.get("status") != "success":
        return {**state, "next_action": "end", "status": "failed", "error": result.get("error")}

    payload = result.get("payload")
    state = {**state, "context_pack": _api_data(payload)}
    return {**state, "next_action": "route"}


async def _route_after_profile(state: PipelineAgentState) -> PipelineAgentState:
    if state.get("error"):
        return {**state, "next_action": "end", "status": "failed"}
    if not state.get("context_pack"):
        return {**state, "next_action": "plan"}
    if state.get("join_hints") is None:
        return {**state, "next_action": "join_keys"}
    if state.get("cleansing_hints") is None:
        return {**state, "next_action": "cleanse_hints"}
    return {**state, "next_action": "plan"}


async def _collect_join_hints(state: PipelineAgentState, runtime: AgentRuntime) -> PipelineAgentState:
    body = {
        "goal": state.get("goal"),
        "data_scope": state.get("data_scope") or {},
        "context_pack": state.get("context_pack") or {},
    }
    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id="pipeline_join_keys",
        method="POST",
        path="/api/v1/pipeline-plans/join-keys",
        body=body,
    )
    if result.get("status") != "success":
        join_hints = _select_join_hints(state.get("context_pack"))
        planner_hints = _merge_planner_hints(state.get("planner_hints"), {"join_plan": join_hints})
        return {**state, "next_action": "route", "join_hints": join_hints, "planner_hints": planner_hints}

    payload = result.get("payload")
    data = _api_data(payload)
    join_hints = data.get("joins") if isinstance(data.get("joins"), list) else []
    planner_hints = _merge_planner_hints(
        state.get("planner_hints"),
        {"join_plan": join_hints},
    )
    return {
        **state,
        "join_hints": join_hints,
        "planner_hints": planner_hints,
        "next_action": "route",
    }


async def _collect_cleansing_hints(state: PipelineAgentState) -> PipelineAgentState:
    cleansing_hints = _select_cleansing_hints(state.get("context_pack"))
    planner_hints = _merge_planner_hints(
        state.get("planner_hints"),
        {"cleansing_hints": cleansing_hints},
    )
    return {
        **state,
        "cleansing_hints": cleansing_hints,
        "planner_hints": planner_hints,
        "next_action": "route",
    }


async def _compile_plan(state: PipelineAgentState, runtime: AgentRuntime) -> PipelineAgentState:
    body = {
        "goal": state.get("goal"),
        "data_scope": state.get("data_scope") or {},
    }
    if state.get("answers"):
        body["answers"] = state.get("answers")
    if state.get("planner_hints"):
        body["planner_hints"] = state.get("planner_hints")

    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id="pipeline_plan_compile",
        method="POST",
        path="/api/v1/pipeline-plans/compile",
        body=body,
    )
    if result.get("status") != "success":
        return {**state, "next_action": "end", "status": "failed", "error": result.get("error")}

    payload = result.get("payload")
    data = _api_data(payload)
    plan_status = str(data.get("status") or "").strip().lower()
    plan_id = str(data.get("plan_id") or "").strip() or None
    plan = data.get("plan") if isinstance(data.get("plan"), dict) else None
    questions = data.get("questions") if isinstance(data.get("questions"), list) else []

    next_action = "preview"
    status = "running"
    if plan_status == "clarification_required":
        if plan and _needs_output_split(plan) and _only_output_errors(list(data.get("validation_errors") or [])):
            next_action = "split_outputs"
        else:
            next_action = "clarify"
            status = "clarification_required"
    elif plan_status != "success":
        next_action = "end"
        status = "failed"
    elif _needs_output_split(plan):
        next_action = "split_outputs"

    return {
        **state,
        "plan_id": plan_id,
        "plan": plan,
        "validation_errors": list(data.get("validation_errors") or []),
        "validation_warnings": list(data.get("validation_warnings") or []),
        "preflight": data.get("preflight") if isinstance(data.get("preflight"), dict) else None,
        "questions": questions,
        "next_action": next_action,
        "status": status,
    }


async def _split_outputs(state: PipelineAgentState, runtime: AgentRuntime) -> PipelineAgentState:
    plan_id = str(state.get("plan_id") or "").strip()
    if not plan_id:
        return {**state, "next_action": "end", "status": "failed", "error": "plan_id missing"}

    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id="pipeline_plan_split_outputs",
        method="POST",
        path=f"/api/v1/pipeline-plans/{plan_id}/split-outputs",
        body={
            "output_bindings": state.get("output_bindings") or {},
        },
    )
    if result.get("status") != "success":
        return {**state, "next_action": "end", "status": "failed", "error": result.get("error")}

    payload = result.get("payload")
    data = _api_data(payload)
    updated_plan = data.get("plan") if isinstance(data.get("plan"), dict) else state.get("plan")
    if _needs_output_split(updated_plan):
        return {
            **state,
            "plan": updated_plan,
            "validation_errors": list(data.get("validation_errors") or []),
            "validation_warnings": list(data.get("validation_warnings") or []),
            "next_action": "end",
            "status": "failed",
            "error": "output metadata incomplete after split",
        }

    return {
        **state,
        "plan": updated_plan,
        "validation_errors": list(data.get("validation_errors") or []),
        "validation_warnings": list(data.get("validation_warnings") or []),
        "next_action": "preview",
    }


async def _transform_plan(state: PipelineAgentState, runtime: AgentRuntime) -> PipelineAgentState:
    plan_id = str(state.get("plan_id") or "").strip()
    if not plan_id:
        return {**state, "next_action": "end", "status": "failed", "error": "plan_id missing"}

    attempts = int(state.get("transform_attempts") or 0) + 1
    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id=f"pipeline_plan_transform_{attempts}",
        method="POST",
        path=f"/api/v1/pipeline-plans/{plan_id}/transform",
        body={
            "join_plan": state.get("join_hints") or [],
            "cleansing_hints": state.get("cleansing_hints") or [],
            "context_pack": state.get("context_pack") or {},
        },
    )
    if result.get("status") != "success":
        return {**state, "next_action": "end", "status": "failed", "error": result.get("error")}

    payload = result.get("payload")
    data = _api_data(payload)
    updated_plan = data.get("plan") if isinstance(data.get("plan"), dict) else state.get("plan")

    next_action = "preview"
    if _needs_output_split(updated_plan):
        next_action = "split_outputs"

    return {
        **state,
        "plan": updated_plan,
        "validation_errors": list(data.get("validation_errors") or []),
        "validation_warnings": list(data.get("validation_warnings") or []),
        "transform_attempts": attempts,
        "next_action": next_action,
    }


async def _preview_plan(state: PipelineAgentState, runtime: AgentRuntime) -> PipelineAgentState:
    plan_id = str(state.get("plan_id") or "").strip()
    if not plan_id:
        return {**state, "next_action": "end", "status": "failed", "error": "plan_id missing"}

    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id="pipeline_plan_preview",
        method="POST",
        path=f"/api/v1/pipeline-plans/{plan_id}/preview",
        body={
            "node_id": state.get("preview_node_id"),
            "limit": state.get("preview_limit"),
            "include_run_tables": bool(state.get("include_run_tables")),
            "run_table_limit": state.get("preview_limit"),
        },
    )
    if result.get("status") != "success":
        return {**state, "next_action": "end", "status": "failed", "error": result.get("error")}

    payload = result.get("payload")
    api_status = _api_status(payload)
    data = _api_data(payload)

    if api_status == "warning":
        repair_attempts = int(state.get("repair_attempts") or 0)
        max_repairs = int(state.get("max_repairs") or 0)
        if repair_attempts < max_repairs:
            return {
                **state,
                "validation_errors": list(data.get("validation_errors") or []),
                "validation_warnings": list(data.get("validation_warnings") or []),
                "preflight": data.get("preflight") if isinstance(data.get("preflight"), dict) else None,
                "preview": data.get("preview") if isinstance(data.get("preview"), dict) else None,
                "run_tables": data.get("run_tables") if isinstance(data.get("run_tables"), dict) else None,
                "definition_digest": str(data.get("definition_digest") or "").strip() or None,
                "next_action": "repair",
            }
        return {
            **state,
            "validation_errors": list(data.get("validation_errors") or []),
            "validation_warnings": list(data.get("validation_warnings") or []),
            "preflight": data.get("preflight") if isinstance(data.get("preflight"), dict) else None,
            "preview": data.get("preview") if isinstance(data.get("preview"), dict) else None,
            "run_tables": data.get("run_tables") if isinstance(data.get("run_tables"), dict) else None,
            "definition_digest": str(data.get("definition_digest") or "").strip() or None,
            "next_action": "end",
            "status": "failed",
        }

    next_action = "evaluate"
    return {
        **state,
        "preflight": data.get("preflight") if isinstance(data.get("preflight"), dict) else None,
        "preview": data.get("preview") if isinstance(data.get("preview"), dict) else None,
        "run_tables": data.get("run_tables") if isinstance(data.get("run_tables"), dict) else None,
        "definition_digest": str(data.get("definition_digest") or "").strip() or None,
        "next_action": next_action,
    }


async def _evaluate_joins(state: PipelineAgentState, runtime: AgentRuntime) -> PipelineAgentState:
    plan_id = str(state.get("plan_id") or "").strip()
    if not plan_id:
        return {
            **state,
            "next_action": "inspect",
            "status": "running",
            "run_tables": None,
        }

    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id="pipeline_join_evaluate",
        method="POST",
        path=f"/api/v1/pipeline-plans/{plan_id}/evaluate-joins",
        body={
            "run_tables": state.get("run_tables"),
            "definition_digest": state.get("definition_digest"),
        },
    )
    if result.get("status") != "success":
        return {
            **state,
            "next_action": "inspect",
            "join_evaluation": None,
            "join_evaluation_warnings": [],
            "run_tables": None,
            "definition_digest": None,
        }

    payload = result.get("payload")
    data = _api_data(payload)
    return {
        **state,
        "join_evaluation": data.get("evaluations") if isinstance(data.get("evaluations"), list) else None,
        "join_evaluation_warnings": data.get("warnings") if isinstance(data.get("warnings"), list) else None,
        "run_tables": None,
        "definition_digest": None,
        "next_action": "inspect",
    }


async def _inspect_preview(state: PipelineAgentState, runtime: AgentRuntime) -> PipelineAgentState:
    plan_id = str(state.get("plan_id") or "").strip()
    if not plan_id:
        return {**state, "next_action": "end", "status": "failed", "error": "plan_id missing"}

    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id="pipeline_plan_inspect_preview",
        method="POST",
        path=f"/api/v1/pipeline-plans/{plan_id}/inspect-preview",
        body={"preview": state.get("preview")},
    )
    if result.get("status") != "success":
        return {**state, "next_action": "end", "status": "failed", "error": result.get("error")}

    payload = result.get("payload")
    data = _api_data(payload)
    inspector = data.get("inspector") if isinstance(data.get("inspector"), dict) else None
    max_cleansing = int(state.get("max_cleansing") or 0)
    attempts = int(state.get("cleansing_attempts") or 0)

    next_action = "specs"
    if inspector and inspector.get("needs_cleansing") and attempts < max_cleansing:
        next_action = "cleanse"

    return {
        **state,
        "cleansing_inspector": inspector,
        "next_action": next_action,
    }


async def _cleanse_plan(state: PipelineAgentState, runtime: AgentRuntime) -> PipelineAgentState:
    plan_id = str(state.get("plan_id") or "").strip()
    if not plan_id:
        return {**state, "next_action": "end", "status": "failed", "error": "plan_id missing"}

    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id="pipeline_plan_cleanse",
        method="POST",
        path=f"/api/v1/pipeline-plans/{plan_id}/cleanse",
        body={
            "preview": state.get("preview"),
            "inspector": state.get("cleansing_inspector"),
        },
    )
    if result.get("status") != "success":
        return {**state, "next_action": "end", "status": "failed", "error": result.get("error")}

    payload = result.get("payload")
    api_status = _api_status(payload)
    data = _api_data(payload)
    if api_status == "partial":
        return {
            **state,
            "cleansing_actions": list(data.get("actions_applied") or []),
            "validation_errors": list(data.get("validation_errors") or []),
            "validation_warnings": list(data.get("validation_warnings") or []),
            "next_action": "end",
            "status": "failed",
            "error": "cleansing validation failed",
        }

    actions = list(data.get("actions_applied") or [])
    plan = data.get("plan") if isinstance(data.get("plan"), dict) else state.get("plan")
    attempts = int(state.get("cleansing_attempts") or 0) + 1

    if not actions:
        return {
            **state,
            "plan": plan,
            "cleansing_actions": actions,
            "cleansing_attempts": attempts,
            "next_action": "specs",
        }

    return {
        **state,
        "plan": plan,
        "cleansing_actions": actions,
        "cleansing_attempts": attempts,
        "next_action": "preview",
    }


async def _repair_plan(state: PipelineAgentState, runtime: AgentRuntime) -> PipelineAgentState:
    plan_id = str(state.get("plan_id") or "").strip()
    if not plan_id:
        return {**state, "next_action": "end", "status": "failed", "error": "plan_id missing"}

    repair_attempts = int(state.get("repair_attempts") or 0) + 1
    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id=f"pipeline_plan_repair_{repair_attempts}",
        method="POST",
        path=f"/api/v1/pipeline-plans/{plan_id}/repair",
        body={
            "validation_errors": state.get("validation_errors") or [],
            "validation_warnings": state.get("validation_warnings") or [],
            "preflight": state.get("preflight"),
            "preview": state.get("preview"),
        },
    )
    if result.get("status") != "success":
        return {**state, "next_action": "end", "status": "failed", "error": result.get("error")}

    payload = result.get("payload")
    data = _api_data(payload)
    plan_status = str(data.get("status") or "").strip().lower()
    plan_id = str(data.get("plan_id") or "").strip() or plan_id
    plan = data.get("plan") if isinstance(data.get("plan"), dict) else None
    questions = data.get("questions") if isinstance(data.get("questions"), list) else []

    next_action = "preview"
    status = "running"
    validation_errors = list(data.get("validation_errors") or [])
    if plan_status == "clarification_required":
        if plan and _needs_output_split(plan) and _only_output_errors(validation_errors):
            next_action = "split_outputs"
        else:
            next_action = "clarify"
            status = "clarification_required"
    elif plan_status != "success":
        next_action = "end"
        status = "failed"
    elif _needs_output_split(plan):
        next_action = "split_outputs"
    elif int(state.get("max_transform") or 0) > 0 and (state.get("join_hints") or state.get("planner_hints")):
        next_action = "transform"

    return {
        **state,
        "plan_id": plan_id,
        "plan": plan,
        "validation_errors": validation_errors,
        "validation_warnings": list(data.get("validation_warnings") or []),
        "preflight": data.get("preflight") if isinstance(data.get("preflight"), dict) else None,
        "questions": questions,
        "next_action": next_action,
        "status": status,
        "repair_attempts": repair_attempts,
        "run_tables": None,
        "definition_digest": None,
    }


async def _generate_specs(state: PipelineAgentState, runtime: AgentRuntime) -> PipelineAgentState:
    plan_id = str(state.get("plan_id") or "").strip()
    if not plan_id:
        return {**state, "next_action": "end", "status": "failed", "error": "plan_id missing"}

    output_previews = None
    preview = state.get("preview")
    if isinstance(preview, dict) and isinstance(state.get("plan"), dict):
        outputs = state.get("plan", {}).get("outputs")
        if isinstance(outputs, list) and outputs:
            name = str(outputs[0].get("output_name") or "").strip()
            if name:
                output_previews = {name: preview}

    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id="pipeline_plan_specs",
        method="POST",
        path=f"/api/v1/pipeline-plans/{plan_id}/generate-specs",
        body={
            "apply": state.get("apply_specs"),
            "output_bindings": state.get("output_bindings") or {},
            "output_previews": output_previews,
            "auto_sync": state.get("auto_sync"),
            "ontology_branch": state.get("ontology_branch"),
            "dangling_policy": state.get("dangling_policy"),
            "dedupe_policy": state.get("dedupe_policy"),
        },
    )
    if result.get("status") != "success":
        return {**state, "next_action": "end", "status": "failed", "error": result.get("error")}

    payload = result.get("payload")
    data = _api_data(payload)
    return {
        **state,
        "specs": data.get("specs") if isinstance(data.get("specs"), list) else None,
        "next_action": "end",
        "status": "success" if _api_status(payload) != "partial" else "partial",
    }


def build_pipeline_agent_graph(runtime: AgentRuntime):
    graph: StateGraph = StateGraph(PipelineAgentState)

    async def profile(state: PipelineAgentState) -> PipelineAgentState:
        return await _build_context_pack(state, runtime)

    async def route(state: PipelineAgentState) -> PipelineAgentState:
        return await _route_after_profile(state)

    async def join_keys(state: PipelineAgentState) -> PipelineAgentState:
        return await _collect_join_hints(state, runtime)

    async def cleanse_hints(state: PipelineAgentState) -> PipelineAgentState:
        return await _collect_cleansing_hints(state)

    async def plan(state: PipelineAgentState) -> PipelineAgentState:
        return await _compile_plan(state, runtime)

    async def split_outputs(state: PipelineAgentState) -> PipelineAgentState:
        return await _split_outputs(state, runtime)

    async def transform(state: PipelineAgentState) -> PipelineAgentState:
        return await _transform_plan(state, runtime)

    async def preview(state: PipelineAgentState) -> PipelineAgentState:
        return await _preview_plan(state, runtime)

    async def evaluate(state: PipelineAgentState) -> PipelineAgentState:
        return await _evaluate_joins(state, runtime)

    async def inspect(state: PipelineAgentState) -> PipelineAgentState:
        return await _inspect_preview(state, runtime)

    async def cleanse_plan(state: PipelineAgentState) -> PipelineAgentState:
        return await _cleanse_plan(state, runtime)

    async def repair(state: PipelineAgentState) -> PipelineAgentState:
        return await _repair_plan(state, runtime)

    async def specs(state: PipelineAgentState) -> PipelineAgentState:
        return await _generate_specs(state, runtime)

    graph.add_node("profile", profile)
    graph.add_node("route", route)
    graph.add_node("join_keys", join_keys)
    graph.add_node("cleanse_hints", cleanse_hints)
    graph.add_node("plan", plan)
    graph.add_node("transform", transform)
    graph.add_node("split_outputs", split_outputs)
    graph.add_node("preview", preview)
    graph.add_node("evaluate", evaluate)
    graph.add_node("inspect", inspect)
    graph.add_node("cleanse_plan", cleanse_plan)
    graph.add_node("repair", repair)
    graph.add_node("specs", specs)

    graph.set_entry_point("profile")
    graph.add_conditional_edges(
        "profile",
        lambda state: state.get("next_action", "route"),
        {"route": "route", "end": END},
    )
    graph.add_conditional_edges(
        "route",
        lambda state: state.get("next_action", "plan"),
        {"join_keys": "join_keys", "cleanse_hints": "cleanse_hints", "plan": "plan", "end": END},
    )
    graph.add_edge("join_keys", "route")
    graph.add_edge("cleanse_hints", "route")
    graph.add_conditional_edges(
        "plan",
        lambda state: state.get("next_action", "end"),
        {"transform": "transform", "split_outputs": "split_outputs", "preview": "preview", "clarify": END, "end": END},
    )
    graph.add_conditional_edges(
        "transform",
        lambda state: state.get("next_action", "end"),
        {"split_outputs": "split_outputs", "preview": "preview", "end": END},
    )
    graph.add_conditional_edges(
        "split_outputs",
        lambda state: state.get("next_action", "end"),
        {"preview": "preview", "end": END},
    )
    graph.add_conditional_edges(
        "preview",
        lambda state: state.get("next_action", "end"),
        {"repair": "repair", "evaluate": "evaluate", "inspect": "inspect", "specs": "specs", "end": END},
    )
    graph.add_conditional_edges(
        "evaluate",
        lambda state: state.get("next_action", "end"),
        {"inspect": "inspect", "specs": "specs", "end": END},
    )
    graph.add_conditional_edges(
        "inspect",
        lambda state: state.get("next_action", "end"),
        {"cleanse": "cleanse_plan", "specs": "specs", "end": END},
    )
    graph.add_conditional_edges(
        "cleanse_plan",
        lambda state: state.get("next_action", "end"),
        {"preview": "preview", "specs": "specs", "end": END},
    )
    graph.add_conditional_edges(
        "repair",
        lambda state: state.get("next_action", "end"),
        {"split_outputs": "split_outputs", "preview": "preview", "clarify": END, "end": END},
    )
    graph.add_edge("specs", END)
    return graph.compile()


async def run_pipeline_agent_graph(runtime: AgentRuntime, initial_state: PipelineAgentState) -> PipelineAgentState:
    app = build_pipeline_agent_graph(runtime)
    return await app.ainvoke(initial_state)


def build_pipeline_agent_state(
    *,
    goal: str,
    data_scope: Dict[str, Any],
    answers: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
    output_bindings: Optional[Dict[str, Any]],
    preview_node_id: Optional[str],
    preview_limit: int,
    include_run_tables: bool,
    max_repairs: int,
    max_cleansing: int,
    max_transform: int,
    apply_specs: bool,
    auto_sync: bool,
    ontology_branch: Optional[str],
    dangling_policy: str,
    dedupe_policy: str,
    request_headers: Dict[str, str],
    request_id: Optional[str],
    actor: str,
) -> PipelineAgentState:
    return PipelineAgentState(
        run_id=str(uuid4()),
        actor=actor,
        goal=goal,
        data_scope=data_scope,
        answers=answers,
        planner_hints=planner_hints,
        join_hints=None,
        cleansing_hints=None,
        output_bindings=output_bindings,
        preview_node_id=preview_node_id,
        preview_limit=int(preview_limit),
        include_run_tables=bool(include_run_tables),
        max_repairs=int(max_repairs),
        repair_attempts=0,
        max_cleansing=int(max_cleansing),
        cleansing_attempts=0,
        max_transform=int(max_transform),
        transform_attempts=0,
        apply_specs=bool(apply_specs),
        auto_sync=bool(auto_sync),
        ontology_branch=ontology_branch,
        dangling_policy=dangling_policy,
        dedupe_policy=dedupe_policy,
        request_headers=request_headers,
        request_id=request_id,
        context_pack=None,
        plan_id=None,
        plan=None,
        validation_errors=[],
        validation_warnings=[],
        preflight=None,
        preview=None,
        run_tables=None,
        definition_digest=None,
        cleansing_inspector=None,
        cleansing_actions=None,
        join_evaluation=None,
        join_evaluation_warnings=None,
        questions=[],
        specs=None,
        status="running",
        error=None,
        next_action="plan",
    )
