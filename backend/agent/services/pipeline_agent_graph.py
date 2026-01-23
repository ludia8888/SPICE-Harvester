from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict
from uuid import uuid4

from langgraph.graph import END, StateGraph

from agent.models import AgentToolCall
from agent.services.agent_runtime import AgentRuntime


class PipelineAgentState(TypedDict):
    run_id: str
    actor: str
    session_id: Optional[str]
    goal: str
    data_scope: Dict[str, Any]
    answers: Dict[str, Any] | None
    planner_hints: Dict[str, Any] | None
    task_spec: Optional[Dict[str, Any]]
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
    verify_intent_attempts: int
    preview_attempts: int
    join_eval_attempts: int
    preview_inspect_attempts: int
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
    intent_status: Optional[str]
    intent_issues: Optional[List[str]]
    intent_actions: Optional[List[str]]
    intent_warnings: Optional[List[str]]
    questions: List[Dict[str, Any]]
    report: Optional[Dict[str, Any]]
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


def _extract_wiring_feedback(warnings: List[str]) -> List[str]:
    if not warnings:
        return []
    keywords = ("join inputs", "association", "auto-augmented", "rewired", "reordered")
    feedback: List[str] = []
    for warn in warnings:
        text = str(warn).lower()
        if any(keyword in text for keyword in keywords):
            feedback.append(str(warn))
    return feedback


def _build_wiring_hints(warnings: List[str]) -> Optional[Dict[str, Any]]:
    if not warnings:
        return None
    feedback = _extract_wiring_feedback(warnings)
    if not feedback:
        return None
    tags: set[str] = set()
    for warn in feedback:
        text = str(warn).lower()
        if "rewired" in text:
            tags.add("rewired")
        if "reordered" in text:
            tags.add("reordered")
        if "auto-augmented" in text or "auto augmented" in text:
            tags.add("auto_augmented")
        if "association" in text:
            tags.add("association")
    return {"tags": sorted(tags), "warnings": feedback, "count": len(feedback)}


def _score_amount_candidate(
    *,
    column_name: str,
    column_stats: Dict[str, Any],
    column_profile: Dict[str, Any],
    amount_tokens: tuple[str, ...],
) -> tuple[float, List[str]]:
    score = 0.0
    reasons: List[str] = []
    name = (column_name or "").lower()
    if any(token in name for token in amount_tokens):
        score += 0.15
        reasons.append("name token match")

    numeric_stats = column_stats.get("numeric") if isinstance(column_stats, dict) else None
    if isinstance(numeric_stats, dict) and numeric_stats:
        score += 0.45
        reasons.append("numeric stats")
        mean = float(abs(numeric_stats.get("mean") or 0.0))
        max_val = float(numeric_stats.get("max") or 0.0)
        if mean > 0 or max_val > 0:
            score += 0.15
            reasons.append("non-zero values")

    distinct_ratio = float(column_profile.get("distinct_ratio") or 0.0) if isinstance(column_profile, dict) else 0.0
    missing_ratio = float(column_profile.get("missing_ratio") or 0.0) if isinstance(column_profile, dict) else 0.0
    if 0 < distinct_ratio < 0.95:
        score += 0.1
        reasons.append("not id-like")
    if 0 <= missing_ratio < 0.4:
        score += 0.1
        reasons.append("low missingness")

    fmt_profile = column_profile.get("format") if isinstance(column_profile, dict) else None
    digit_ratio = float(fmt_profile.get("digit_ratio") or 0.0) if isinstance(fmt_profile, dict) else 0.0
    if digit_ratio > 0.9 and distinct_ratio > 0.95:
        score -= 0.2
        reasons.append("likely identifier")

    score = max(0.0, min(1.0, score))
    return score, reasons


def _numeric_column_hints(context_pack: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(context_pack, dict):
        return []
    selected = context_pack.get("selected_datasets")
    if not isinstance(selected, list):
        return []
    amount_tokens = ("amount", "value", "price", "total", "payment", "cost", "fare")
    hints: List[Dict[str, Any]] = []
    for item in selected:
        if not isinstance(item, dict):
            continue
        dataset_id = item.get("dataset_id")
        dataset_name = item.get("name")
        columns = item.get("columns") if isinstance(item.get("columns"), list) else []
        stats_by_col = {}
        column_stats = item.get("column_stats")
        if isinstance(column_stats, dict):
            stats_by_col = column_stats.get("columns") if isinstance(column_stats.get("columns"), dict) else {}
        profiles_by_col = item.get("column_profiles") if isinstance(item.get("column_profiles"), dict) else {}
        numeric: List[str] = []
        ranked_amount: List[Dict[str, Any]] = []
        for col in columns:
            if not isinstance(col, dict):
                continue
            col_name = str(col.get("name") or "").strip()
            col_type = str(col.get("type") or "").lower()
            if not col_name:
                continue
            col_stats = stats_by_col.get(col_name) if isinstance(stats_by_col, dict) else {}
            numeric_stats = col_stats.get("numeric") if isinstance(col_stats, dict) else None
            is_numeric_type = any(token in col_type for token in ("integer", "decimal", "float", "double", "number"))
            is_numeric = is_numeric_type or (isinstance(numeric_stats, dict) and numeric_stats)
            if is_numeric:
                numeric.append(col_name)
                score, reasons = _score_amount_candidate(
                    column_name=col_name,
                    column_stats=col_stats or {},
                    column_profile=(profiles_by_col.get(col_name) or {}),
                    amount_tokens=amount_tokens,
                )
                ranked_amount.append(
                    {
                        "column": col_name,
                        "score": round(score, 3),
                        "reasons": reasons[:3],
                    }
                )
        if numeric:
            ranked_amount.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
            amount_candidates = ranked_amount[:8]
            hints.append(
                {
                    "dataset_id": dataset_id,
                    "dataset_name": dataset_name,
                    "numeric_columns": numeric[:12],
                    "amount_candidates": amount_candidates,
                }
            )
    return hints


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


def _allow_specs(state: PipelineAgentState) -> bool:
    task_spec = state.get("task_spec") or {}
    return bool(task_spec.get("allow_specs"))


def _should_transform(state: PipelineAgentState) -> bool:
    if int(state.get("max_transform") or 0) <= 0:
        return False
    hints = state.get("planner_hints") or {}
    return (
        bool(hints.get("validation_errors"))
        or bool(hints.get("intent_feedback"))
    )


def _can_transform(state: PipelineAgentState) -> bool:
    if int(state.get("max_transform") or 0) <= 0:
        return False
    task_spec = state.get("task_spec") or {}
    scope = str(task_spec.get("scope") or "").strip().lower()
    return scope != "report_only"


def _only_output_errors(errors: List[str]) -> bool:
    if not errors:
        return False
    for err in errors:
        if not str(err).strip().lower().startswith("output "):
            return False
    return True


def _transformable_validation_errors(errors: List[str]) -> bool:
    if not errors:
        return False
    fragments = (
        "missing aggregates",
        "missing expression",
        "missing condition",
        "missing join keys",
        "missing join key",
        "missing output",
        "output missing",
        "missing columns",
        "missing column",
        "unknown columns",
        "unknown column",
    )
    for err in errors:
        lowered = str(err or "").strip().lower()
        if not lowered:
            return False
        if not any(fragment in lowered for fragment in fragments):
            return False
    return True


def _needs_join_revision(join_evaluation: Any, warnings: Any) -> bool:
    """
    Heuristic gate to trigger a join-strategy revision loop.

    We keep this conservative to avoid oscillation; the join-key selector
    still must choose only from context_pack candidates.
    """
    evaluations = join_evaluation if isinstance(join_evaluation, list) else []
    for item in evaluations:
        if not isinstance(item, dict):
            continue
        try:
            left_coverage = float(item.get("left_coverage") or 0.0)
            right_coverage = float(item.get("right_coverage") or 0.0)
            explosion_ratio = float(item.get("explosion_ratio") or 0.0)
            output_row_count = int(item.get("output_row_count") or 0)
            left_row_count = int(item.get("left_row_count") or 0)
            right_row_count = int(item.get("right_row_count") or 0)
        except (TypeError, ValueError):
            continue
        # Join evaluation runs on independently sampled inputs, so match-rate based signals are
        # very noisy for high-cardinality keys (e.g., order_id). Avoid "fixing" correct joins.
        #
        # Trigger revision only when we see a strong failure signal:
        # - zero matches in the sampled join output, or
        # - extreme blow-up (many-to-many / wrong key).
        # If one side is already a tiny intermediate table, zero matches is expected with independent sampling.
        # Only treat "zero matches" as actionable when both inputs are reasonably sized.
        if (
            output_row_count <= 0
            and min(left_row_count, right_row_count) >= 200
            and min(left_coverage, right_coverage) <= 0.001
        ):
            return True
        if explosion_ratio >= 10.0:
            return True

    warning_items = warnings if isinstance(warnings, list) else []
    for warn in warning_items:
        text = str(warn or "").strip().lower()
        if not text:
            continue
        if "join input order ambiguous" in text:
            # This can lead to a wrong join when left/right keys differ. Allow a single retry.
            return True
    return False


async def _call_bff(
    *,
    runtime: AgentRuntime,
    state: PipelineAgentState,
    step_id: str,
    method: str,
    path: str,
    tool_id: Optional[str] = None,
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    idempotency_key = f"{state['run_id']}:{step_id}"
    headers = {"Idempotency-Key": idempotency_key}
    session_id = state.get("session_id")
    if session_id:
        headers["X-Agent-Session-ID"] = str(session_id)
    tool_call = AgentToolCall(
        step_id=step_id,
        tool_id=tool_id,
        service="bff",
        method=method,
        path=path,
        body=body,
        query={},
        headers=headers,
        data_scope=state.get("data_scope") or {},
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

    dataset_ids = list(scope.get("dataset_ids") or [])
    dataset_count = len(dataset_ids)
    max_pairs = (dataset_count * (dataset_count - 1)) // 2 if dataset_count > 1 else 0
    body = {
        "db_name": db_name,
        "branch": scope.get("branch"),
        "dataset_ids": dataset_ids,
    }
    if dataset_count:
        body["max_selected_datasets"] = min(20, dataset_count)
        if max_pairs:
            body["max_join_candidates"] = min(50, max(10, max_pairs))
    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id="pipeline_context_pack",
        method="POST",
        path="/api/v1/pipeline-plans/context-pack",
        tool_id="pipeline_plans.context_pack",
        body=body,
    )
    if result.get("status") != "success":
        return {**state, "next_action": "end", "status": "failed", "error": result.get("error")}

    payload = result.get("payload")
    state = {**state, "context_pack": _api_data(payload)}
    numeric_hints = _numeric_column_hints(state.get("context_pack"))
    planner_hints = _merge_planner_hints(
        state.get("planner_hints"),
        {
            "autonomy_level": "high",
            "cardinality_strategy": "prefer_left_on_uncertain",
            "null_strategy": "keep_as_null",
        },
    )
    if numeric_hints:
        planner_hints = _merge_planner_hints(planner_hints, {"numeric_columns": numeric_hints})
    state = {**state, "planner_hints": planner_hints}
    return {**state, "next_action": "route"}


async def _infer_task_spec(state: PipelineAgentState, runtime: AgentRuntime) -> PipelineAgentState:
    if state.get("task_spec") is not None:
        return {**state, "next_action": "route"}
    body = {
        "goal": state.get("goal"),
        "data_scope": state.get("data_scope") or {},
        "context_pack": state.get("context_pack") or {},
    }
    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id="pipeline_task_spec",
        method="POST",
        path="/api/v1/pipeline-plans/task-spec",
        tool_id="pipeline_plans.task_spec",
        body=body,
    )
    if result.get("status") != "success":
        return {**state, "next_action": "end", "status": "failed", "error": result.get("error")}

    payload = result.get("payload")
    data = _api_data(payload)
    spec_status = str(data.get("status") or "").strip().lower()
    task_spec = data.get("task_spec") if isinstance(data.get("task_spec"), dict) else None
    questions = data.get("questions") if isinstance(data.get("questions"), list) else []
    if spec_status == "clarification_required":
        return {
            **state,
            "task_spec": task_spec,
            "questions": questions,
            "status": "clarification_required",
            "next_action": "end",
        }
    return {**state, "task_spec": task_spec, "next_action": "route"}


def _build_null_check_report(state: PipelineAgentState) -> Dict[str, Any]:
    scope = state.get("data_scope") or {}
    wanted_ids = {str(item).strip() for item in (scope.get("dataset_ids") or []) if str(item).strip()}
    pack = state.get("context_pack") or {}
    selected = pack.get("selected_datasets") if isinstance(pack, dict) else None
    datasets: list[dict[str, Any]] = []
    if isinstance(selected, list):
        for ds in selected:
            if not isinstance(ds, dict):
                continue
            ds_id = str(ds.get("dataset_id") or "").strip()
            if wanted_ids and ds_id and ds_id not in wanted_ids:
                continue
            profiles = ds.get("column_profiles") if isinstance(ds.get("column_profiles"), dict) else {}
            columns: list[dict[str, Any]] = []
            for col, prof in profiles.items():
                if not isinstance(prof, dict):
                    continue
                null_ratio = float(prof.get("null_ratio") or 0.0)
                missing_ratio = float(prof.get("missing_ratio") or 0.0)
                if null_ratio <= 0 and missing_ratio <= 0:
                    continue
                columns.append(
                    {
                        "column": str(col),
                        "null_ratio": round(null_ratio, 4),
                        "missing_ratio": round(missing_ratio, 4),
                        "distinct_ratio": prof.get("distinct_ratio"),
                        "duplicate_ratio": prof.get("duplicate_ratio"),
                    }
                )
            columns.sort(key=lambda item: (float(item.get("null_ratio") or 0.0), float(item.get("missing_ratio") or 0.0)), reverse=True)
            datasets.append(
                {
                    "dataset_id": ds_id,
                    "name": ds.get("name"),
                    "branch": ds.get("branch"),
                    "row_count": ds.get("row_count"),
                    "columns_with_nulls": columns,
                }
            )
    return {
        "kind": "null_check",
        "dataset_count": len(datasets),
        "datasets": datasets,
        "notes": ["null/missing ratios are based on safe sample rows from the context pack"],
    }


def _build_profile_report(state: PipelineAgentState) -> Dict[str, Any]:
    scope = state.get("data_scope") or {}
    wanted_ids = {str(item).strip() for item in (scope.get("dataset_ids") or []) if str(item).strip()}
    pack = state.get("context_pack") or {}
    selected = pack.get("selected_datasets") if isinstance(pack, dict) else None
    datasets: list[dict[str, Any]] = []
    if isinstance(selected, list):
        for ds in selected:
            if not isinstance(ds, dict):
                continue
            ds_id = str(ds.get("dataset_id") or "").strip()
            if wanted_ids and ds_id and ds_id not in wanted_ids:
                continue
            columns = ds.get("columns") if isinstance(ds.get("columns"), list) else []
            pk_candidates = ds.get("pk_candidates") if isinstance(ds.get("pk_candidates"), list) else []
            # Keep it compact: top candidates + top null columns.
            profiles = ds.get("column_profiles") if isinstance(ds.get("column_profiles"), dict) else {}
            null_columns: list[dict[str, Any]] = []
            for col, prof in profiles.items():
                if not isinstance(prof, dict):
                    continue
                null_ratio = float(prof.get("null_ratio") or 0.0)
                missing_ratio = float(prof.get("missing_ratio") or 0.0)
                if null_ratio <= 0 and missing_ratio <= 0:
                    continue
                null_columns.append(
                    {
                        "column": str(col),
                        "null_ratio": round(null_ratio, 4),
                        "missing_ratio": round(missing_ratio, 4),
                    }
                )
            null_columns.sort(
                key=lambda item: (float(item.get("null_ratio") or 0.0), float(item.get("missing_ratio") or 0.0)),
                reverse=True,
            )
            datasets.append(
                {
                    "dataset_id": ds_id,
                    "name": ds.get("name"),
                    "branch": ds.get("branch"),
                    "row_count": ds.get("row_count"),
                    "column_count": len(columns),
                    "columns_preview": [
                        str(col.get("name") or "").strip()
                        for col in columns[:25]
                        if isinstance(col, dict) and str(col.get("name") or "").strip()
                    ],
                    "pk_candidates": pk_candidates[:5],
                    "columns_with_nulls": null_columns[:15],
                }
            )
    integration = pack.get("integration_suggestions") if isinstance(pack, dict) else {}
    fk_candidates = []
    if isinstance(integration, dict) and isinstance(integration.get("foreign_key_candidates"), list):
        fk_candidates = integration.get("foreign_key_candidates") or []
    return {
        "kind": "profile",
        "dataset_count": len(datasets),
        "datasets": datasets,
        "foreign_key_candidates": fk_candidates[:25],
        "notes": ["profiles are based on safe sample rows from the context pack"],
    }


def _build_cleansing_suggestion_report(state: PipelineAgentState) -> Dict[str, Any]:
    pack = state.get("context_pack") or {}
    integration = pack.get("integration_suggestions") if isinstance(pack, dict) else {}
    suggestions = []
    if isinstance(integration, dict) and isinstance(integration.get("cleansing_suggestions"), list):
        suggestions = integration.get("cleansing_suggestions") or []
    # Group by dataset_id for readability.
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in suggestions:
        if not isinstance(item, dict):
            continue
        ds_id = str(item.get("dataset_id") or "").strip()
        if not ds_id:
            continue
        grouped.setdefault(ds_id, []).append(
            {
                "column": item.get("column"),
                "suggestion": item.get("suggestion"),
                "evidence": item.get("evidence"),
            }
        )
    datasets = [{"dataset_id": ds_id, "suggestions": items} for ds_id, items in grouped.items()]
    return {
        "kind": "cleanse_suggestions",
        "dataset_count": len(datasets),
        "datasets": datasets,
        "notes": ["suggestions are derived from safe sample rows in the context pack; no data was modified"],
    }


async def _run_report(state: PipelineAgentState) -> PipelineAgentState:
    task_spec = state.get("task_spec") or {}
    intent = str(task_spec.get("intent") or "").strip().lower()
    if intent == "null_check":
        report = _build_null_check_report(state)
    elif intent == "profile":
        report = _build_profile_report(state)
    elif intent == "cleanse":
        report = _build_cleansing_suggestion_report(state)
    else:
        # Default report: still provide a null-oriented summary (safe and generally useful).
        report = _build_null_check_report(state)
        report["kind"] = "report"
        report.setdefault("warnings", []).append(f"unknown report intent: {intent}")
    return {**state, "report": report, "next_action": "end", "status": "success"}


async def _route_after_profile(state: PipelineAgentState) -> PipelineAgentState:
    if state.get("error"):
        return {**state, "next_action": "end", "status": "failed"}
    if state.get("task_spec") is None:
        return {**state, "next_action": "task_spec"}

    task_spec = state.get("task_spec") or {}
    scope = str(task_spec.get("scope") or "").strip().lower()
    if scope == "report_only":
        return {**state, "next_action": "report"}

    intent = str(task_spec.get("intent") or "").strip().lower()
    allow_join = bool(task_spec.get("allow_join"))
    allow_cleansing = bool(task_spec.get("allow_cleansing"))
    dataset_count = len(list((state.get("data_scope") or {}).get("dataset_ids") or []))

    # Skip join selection unless explicitly needed.
    if state.get("join_hints") is None:
        if allow_join and dataset_count > 1 and intent in {"integrate", "prepare_mapping"}:
            return {**state, "next_action": "join_keys"}
        state = {**state, "join_hints": []}

    # Skip cleansing hints unless explicitly needed.
    if state.get("cleansing_hints") is None:
        if allow_cleansing and intent in {"cleanse", "prepare_mapping"}:
            return {**state, "next_action": "cleanse_hints"}
        state = {**state, "cleansing_hints": []}

    return {**state, "next_action": "compile_plan"}


async def _collect_join_hints(state: PipelineAgentState, runtime: AgentRuntime) -> PipelineAgentState:
    dataset_ids = list((state.get("data_scope") or {}).get("dataset_ids") or [])
    dataset_count = len(dataset_ids)
    if dataset_count > 1:
        max_joins = min(12, max(4, dataset_count - 1))
    else:
        max_joins = 4
    body = {
        "goal": state.get("goal"),
        "data_scope": state.get("data_scope") or {},
        "context_pack": state.get("context_pack") or {},
        "max_joins": max_joins,
    }
    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id="pipeline_join_keys",
        method="POST",
        path="/api/v1/pipeline-plans/join-keys",
        tool_id="pipeline_plans.join_keys",
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
    if state.get("task_spec"):
        body["task_spec"] = state.get("task_spec")

    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id="pipeline_plan_compile",
        method="POST",
        path="/api/v1/pipeline-plans/compile",
        tool_id="pipeline_plans.compile",
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
    validation_errors = list(data.get("validation_errors") or [])
    validation_warnings = list(data.get("validation_warnings") or [])
    updated_planner_hints = state.get("planner_hints")
    wiring_hints = _build_wiring_hints(validation_warnings)
    if wiring_hints:
        hints = dict(updated_planner_hints or {})
        hints["wiring_feedback"] = wiring_hints.get("warnings")
        hints["auto_wiring"] = wiring_hints
        updated_planner_hints = hints

    state_for_decision: PipelineAgentState = {**state, "planner_hints": updated_planner_hints}
    allow_specs = _allow_specs(state_for_decision)
    should_transform = _should_transform(state_for_decision)
    can_transform = _can_transform(state_for_decision)

    next_action = "verify_intent"
    status = "running"
    if plan_status == "clarification_required":
        if plan and can_transform and _transformable_validation_errors(validation_errors):
            updated_planner_hints = _merge_planner_hints(updated_planner_hints, {"validation_errors": validation_errors})
            next_action = "transform"
            status = "running"
        elif allow_specs and plan and _needs_output_split(plan) and _only_output_errors(validation_errors):
            next_action = "split_outputs"
        else:
            next_action = "clarify"
            status = "clarification_required"
    elif plan_status != "success":
        next_action = "end"
        status = "failed"
    elif allow_specs and _needs_output_split(plan):
        next_action = "split_outputs"
    elif should_transform:
        next_action = "transform"

    return {
        **state,
        "plan_id": plan_id,
        "plan": plan,
        "validation_errors": validation_errors,
        "validation_warnings": validation_warnings,
        "preflight": data.get("preflight") if isinstance(data.get("preflight"), dict) else None,
        "questions": questions,
        "planner_hints": updated_planner_hints,
        "next_action": next_action,
        "status": status,
    }


async def _split_outputs(state: PipelineAgentState, runtime: AgentRuntime) -> PipelineAgentState:
    if not _allow_specs(state):
        # Specs/mapping not requested; do not run output enrichment.
        return {**state, "next_action": "verify_intent"}

    plan_id = str(state.get("plan_id") or "").strip()
    if not plan_id:
        return {**state, "next_action": "end", "status": "failed", "error": "plan_id missing"}

    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id="pipeline_plan_split_outputs",
        method="POST",
        path=f"/api/v1/pipeline-plans/{plan_id}/split-outputs",
        tool_id="pipeline_plans.split_outputs",
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

    next_action = "verify_intent"
    should_transform = _should_transform(state)
    if should_transform:
        next_action = "transform"

    return {
        **state,
        "plan": updated_plan,
        "validation_errors": list(data.get("validation_errors") or []),
        "validation_warnings": list(data.get("validation_warnings") or []),
        "next_action": next_action,
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
        tool_id="pipeline_plans.transform",
        body={
            "join_plan": state.get("join_hints") or [],
            "cleansing_hints": state.get("cleansing_hints") or [],
            "context_pack": state.get("context_pack") or {},
            "planner_hints": state.get("planner_hints") or {},
        },
    )
    if result.get("status") != "success":
        return {**state, "next_action": "end", "status": "failed", "error": result.get("error")}

    payload = result.get("payload")
    data = _api_data(payload)
    updated_plan = data.get("plan") if isinstance(data.get("plan"), dict) else state.get("plan")
    validation_warnings = list(data.get("validation_warnings") or [])
    updated_planner_hints = state.get("planner_hints")
    wiring_hints = _build_wiring_hints(validation_warnings)
    if wiring_hints:
        hints = dict(updated_planner_hints or {})
        hints["wiring_feedback"] = wiring_hints.get("warnings")
        hints["auto_wiring"] = wiring_hints
        updated_planner_hints = hints

    next_action = "verify_intent"
    if _allow_specs(state) and _needs_output_split(updated_plan):
        next_action = "split_outputs"

    return {
        **state,
        "plan": updated_plan,
        "validation_errors": list(data.get("validation_errors") or []),
        "validation_warnings": validation_warnings,
        "transform_attempts": attempts,
        "planner_hints": updated_planner_hints,
        "next_action": next_action,
    }


async def _verify_intent(state: PipelineAgentState, runtime: AgentRuntime) -> PipelineAgentState:
    plan_id = str(state.get("plan_id") or "").strip()
    if not plan_id:
        return {**state, "next_action": "end", "status": "failed", "error": "plan_id missing"}

    verify_attempts = int(state.get("verify_intent_attempts") or 0) + 1
    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id=f"pipeline_plan_verify_intent_{verify_attempts}",
        method="POST",
        path=f"/api/v1/pipeline-plans/{plan_id}/verify-intent",
        tool_id="pipeline_plans.verify_intent",
        body={
            "join_plan": state.get("join_hints") or [],
            "cleansing_hints": state.get("cleansing_hints") or [],
            "context_pack": state.get("context_pack") or {},
        },
    )
    if result.get("status") != "success":
        return {
            **state,
            "verify_intent_attempts": verify_attempts,
            "next_action": "end",
            "status": "failed",
            "error": result.get("error"),
        }

    payload = result.get("payload")
    data = _api_data(payload)
    intent_status = str(data.get("status") or "").strip().lower()
    missing = data.get("missing_requirements") if isinstance(data.get("missing_requirements"), list) else []
    suggested = data.get("suggested_actions") if isinstance(data.get("suggested_actions"), list) else []
    questions = data.get("questions") if isinstance(data.get("questions"), list) else []
    warnings = data.get("warnings") if isinstance(data.get("warnings"), list) else []

    if intent_status == "pass":
        return {
            **state,
            "verify_intent_attempts": verify_attempts,
            "intent_status": intent_status,
            "intent_issues": missing,
            "intent_actions": suggested,
            "intent_warnings": warnings,
            "next_action": "preview_plan",
        }

    if intent_status == "needs_revision":
        max_transform = int(state.get("max_transform") or 0)
        attempts = int(state.get("transform_attempts") or 0)
        if attempts < max_transform and _can_transform(state):
            planner_hints = dict(state.get("planner_hints") or {})
            planner_hints["intent_feedback"] = {
                "missing_requirements": missing,
                "suggested_actions": suggested,
                "warnings": warnings,
            }
            return {
                **state,
                "verify_intent_attempts": verify_attempts,
                "planner_hints": planner_hints,
                "intent_status": intent_status,
                "intent_issues": missing,
                "intent_actions": suggested,
                "intent_warnings": warnings,
                "next_action": "transform",
            }
        if attempts < max_transform and not _can_transform(state):
            # Scope mismatch: ask the LLM to generate user-facing clarification questions.
            result = await _call_bff(
                runtime=runtime,
                state=state,
                step_id="pipeline_scope_clarify",
                method="POST",
                path="/api/v1/pipeline-plans/clarify-scope",
                tool_id="pipeline_plans.clarify_scope",
                body={
                    "goal": state.get("goal"),
                    "data_scope": state.get("data_scope") or {},
                    "task_spec": state.get("task_spec") or {},
                    "intent_status": intent_status,
                    "missing_requirements": missing,
                    "suggested_actions": suggested,
                    "intent_warnings": warnings,
                    "context_pack": state.get("context_pack") or {},
                },
            )
            if result.get("status") != "success":
                return {
                    **state,
                    "verify_intent_attempts": verify_attempts,
                    "intent_status": intent_status,
                    "intent_issues": missing,
                    "intent_actions": suggested,
                    "intent_warnings": warnings,
                    "next_action": "end",
                    "status": "failed",
                    "error": result.get("error"),
                }

            payload = result.get("payload")
            data = _api_data(payload)
            questions = data.get("questions") if isinstance(data.get("questions"), list) else []
            return {
                **state,
                "verify_intent_attempts": verify_attempts,
                "intent_status": intent_status,
                "intent_issues": missing,
                "intent_actions": suggested,
                "intent_warnings": warnings,
                "questions": questions,
                "next_action": "end",
                "status": "clarification_required",
            }
        return {
            **state,
            "intent_status": intent_status,
            "intent_issues": missing,
            "intent_actions": suggested,
            "intent_warnings": warnings,
            "next_action": "end",
            "status": "failed",
            "error": "intent verification requires revision but max_transform reached",
        }

    if intent_status == "clarification_required":
        return {
            **state,
            "verify_intent_attempts": verify_attempts,
            "intent_status": intent_status,
            "intent_issues": missing,
            "intent_actions": suggested,
            "intent_warnings": warnings,
            "questions": questions,
            "next_action": "end",
            "status": "clarification_required",
        }

    return {
        **state,
        "verify_intent_attempts": verify_attempts,
        "intent_status": intent_status or "unknown",
        "intent_issues": missing,
        "intent_actions": suggested,
        "intent_warnings": warnings,
        "next_action": "end",
        "status": "failed",
        "error": "intent verification returned unknown status",
    }


async def _preview_plan(state: PipelineAgentState, runtime: AgentRuntime) -> PipelineAgentState:
    plan_id = str(state.get("plan_id") or "").strip()
    if not plan_id:
        return {**state, "next_action": "end", "status": "failed", "error": "plan_id missing"}

    requested_limit = int(state.get("preview_limit") or 200)
    preview_limit = min(requested_limit, 200)
    run_table_limit = min(requested_limit, 1000)
    preview_attempts = int(state.get("preview_attempts") or 0) + 1
    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id=f"pipeline_plan_preview_{preview_attempts}",
        method="POST",
        path=f"/api/v1/pipeline-plans/{plan_id}/preview",
        tool_id="pipeline_plans.preview",
        body={
            "node_id": state.get("preview_node_id"),
            "limit": preview_limit,
            "include_run_tables": bool(state.get("include_run_tables")),
            "run_table_limit": run_table_limit,
        },
    )
    if result.get("status") != "success":
        return {
            **state,
            "preview_attempts": preview_attempts,
            "next_action": "end",
            "status": "failed",
            "error": result.get("error"),
        }

    payload = result.get("payload")
    api_status = _api_status(payload)
    data = _api_data(payload)

    if api_status == "warning":
        repair_attempts = int(state.get("repair_attempts") or 0)
        max_repairs = int(state.get("max_repairs") or 0)
        if repair_attempts < max_repairs:
            return {
                **state,
                "preview_attempts": preview_attempts,
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
            "preview_attempts": preview_attempts,
            "validation_errors": list(data.get("validation_errors") or []),
            "validation_warnings": list(data.get("validation_warnings") or []),
            "preflight": data.get("preflight") if isinstance(data.get("preflight"), dict) else None,
            "preview": data.get("preview") if isinstance(data.get("preview"), dict) else None,
            "run_tables": data.get("run_tables") if isinstance(data.get("run_tables"), dict) else None,
            "definition_digest": str(data.get("definition_digest") or "").strip() or None,
            "next_action": "end",
            "status": "failed",
        }

    task_spec = state.get("task_spec") or {}
    next_action = "evaluate" if bool(task_spec.get("allow_join")) else "inspect"
    return {
        **state,
        "preview_attempts": preview_attempts,
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

    join_eval_attempts = int(state.get("join_eval_attempts") or 0) + 1
    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id=f"pipeline_join_evaluate_{join_eval_attempts}",
        method="POST",
        path=f"/api/v1/pipeline-plans/{plan_id}/evaluate-joins",
        tool_id="pipeline_plans.evaluate_joins",
        body={
            "run_tables": state.get("run_tables"),
            "definition_digest": state.get("definition_digest"),
        },
    )
    if result.get("status") != "success":
        return {
            **state,
            "join_eval_attempts": join_eval_attempts,
            "next_action": "inspect",
            "join_evaluation": None,
            "join_evaluation_warnings": [],
            "run_tables": None,
            "definition_digest": None,
        }

    payload = result.get("payload")
    data = _api_data(payload)
    join_evaluation = data.get("evaluations") if isinstance(data.get("evaluations"), list) else None
    join_eval_warnings = data.get("warnings") if isinstance(data.get("warnings"), list) else None

    # Optional join revision loop: if join quality looks very poor, re-run join-key selection
    # with feedback and attempt a transform pass.
    task_spec = state.get("task_spec") or {}
    allow_join = bool(task_spec.get("allow_join"))
    can_transform = _can_transform(state)
    attempts = int(state.get("transform_attempts") or 0)
    max_transform = int(state.get("max_transform") or 0)
    planner_hints = dict(state.get("planner_hints") or {})
    join_revision_attempted = bool(planner_hints.get("join_revision_attempted"))
    should_revise = (
        allow_join
        and can_transform
        and (attempts < max_transform)
        and (not join_revision_attempted)
        and _needs_join_revision(join_evaluation, join_eval_warnings)
    )

    if should_revise:
        dataset_ids = list((state.get("data_scope") or {}).get("dataset_ids") or [])
        dataset_count = len(dataset_ids)
        max_joins = min(12, max(4, dataset_count - 1)) if dataset_count > 1 else 4
        feedback = {
            "current_join_plan": state.get("join_hints") or [],
            "join_evaluation": join_evaluation or [],
            "join_warnings": join_eval_warnings or [],
        }

        join_result = await _call_bff(
            runtime=runtime,
            state=state,
            step_id="pipeline_join_keys_revise",
            method="POST",
            path="/api/v1/pipeline-plans/join-keys",
            tool_id="pipeline_plans.join_keys",
            body={
                "goal": state.get("goal"),
                "data_scope": state.get("data_scope") or {},
                "context_pack": state.get("context_pack") or {},
                "max_joins": max_joins,
                "feedback": feedback,
            },
        )
        if join_result.get("status") == "success":
            join_payload = join_result.get("payload")
            join_data = _api_data(join_payload)
            revised = join_data.get("joins") if isinstance(join_data.get("joins"), list) else []
            if revised:
                planner_hints = _merge_planner_hints(planner_hints, {"join_plan": revised})
                planner_hints["join_revision_attempted"] = True
                planner_hints["join_revision_feedback"] = feedback
                return {
                    **state,
                    "join_eval_attempts": join_eval_attempts,
                    "join_hints": revised,
                    "planner_hints": planner_hints,
                    "join_evaluation": join_evaluation,
                    "join_evaluation_warnings": join_eval_warnings,
                    "run_tables": None,
                    "definition_digest": None,
                    "next_action": "transform",
                }

        # If revision fails, fall back to inspection.
        planner_hints["join_revision_attempted"] = True

    return {
        **state,
        "join_eval_attempts": join_eval_attempts,
        "planner_hints": planner_hints,
        "join_evaluation": join_evaluation,
        "join_evaluation_warnings": join_eval_warnings,
        "run_tables": None,
        "definition_digest": None,
        "next_action": "inspect",
    }


async def _inspect_preview(state: PipelineAgentState, runtime: AgentRuntime) -> PipelineAgentState:
    plan_id = str(state.get("plan_id") or "").strip()
    if not plan_id:
        return {**state, "next_action": "end", "status": "failed", "error": "plan_id missing"}

    inspect_attempts = int(state.get("preview_inspect_attempts") or 0) + 1
    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id=f"pipeline_plan_inspect_preview_{inspect_attempts}",
        method="POST",
        path=f"/api/v1/pipeline-plans/{plan_id}/inspect-preview",
        tool_id="pipeline_plans.inspect_preview",
        body={"preview": state.get("preview")},
    )
    if result.get("status") != "success":
        return {
            **state,
            "preview_inspect_attempts": inspect_attempts,
            "next_action": "end",
            "status": "failed",
            "error": result.get("error"),
        }

    payload = result.get("payload")
    data = _api_data(payload)
    inspector = data.get("inspector") if isinstance(data.get("inspector"), dict) else None
    max_cleansing = int(state.get("max_cleansing") or 0)
    attempts = int(state.get("cleansing_attempts") or 0)

    task_spec = state.get("task_spec") or {}
    allow_cleansing = bool(task_spec.get("allow_cleansing"))
    allow_specs = bool(task_spec.get("allow_specs"))

    next_action = "generate_specs" if allow_specs else "end"
    if allow_cleansing and inspector and inspector.get("needs_cleansing") and attempts < max_cleansing:
        next_action = "cleanse"

    return {
        **state,
        "preview_inspect_attempts": inspect_attempts,
        "cleansing_inspector": inspector,
        "next_action": next_action,
        "status": "success" if next_action == "end" else state.get("status"),
    }


async def _cleanse_plan(state: PipelineAgentState, runtime: AgentRuntime) -> PipelineAgentState:
    plan_id = str(state.get("plan_id") or "").strip()
    if not plan_id:
        return {**state, "next_action": "end", "status": "failed", "error": "plan_id missing"}

    cleanse_attempts = int(state.get("cleansing_attempts") or 0) + 1
    result = await _call_bff(
        runtime=runtime,
        state=state,
        step_id=f"pipeline_plan_cleanse_{cleanse_attempts}",
        method="POST",
        path=f"/api/v1/pipeline-plans/{plan_id}/cleanse",
        tool_id="pipeline_plans.cleanse",
        body={
            "preview": state.get("preview"),
            "inspector": state.get("cleansing_inspector"),
            "planner_hints": state.get("planner_hints"),
        },
    )
    if result.get("status") != "success":
        return {
            **state,
            "cleansing_attempts": cleanse_attempts,
            "next_action": "end",
            "status": "failed",
            "error": result.get("error"),
        }

    payload = result.get("payload")
    api_status = _api_status(payload)
    data = _api_data(payload)
    if api_status == "partial":
        validation_errors = list(data.get("validation_errors") or [])
        validation_warnings = list(data.get("validation_warnings") or [])
        repair_attempts = int(state.get("repair_attempts") or 0)
        max_repairs = int(state.get("max_repairs") or 0)
        if repair_attempts < max_repairs:
            return {
                **state,
                "cleansing_actions": list(data.get("actions_applied") or []),
                "validation_errors": validation_errors,
                "validation_warnings": validation_warnings,
                "preflight": data.get("preflight") if isinstance(data.get("preflight"), dict) else None,
                "next_action": "repair",
            }
        return {
            **state,
            "cleansing_actions": list(data.get("actions_applied") or []),
            "validation_errors": validation_errors,
            "validation_warnings": validation_warnings,
            "next_action": "end",
            "status": "failed",
            "error": "cleansing validation failed",
        }

    actions = list(data.get("actions_applied") or [])
    plan = data.get("plan") if isinstance(data.get("plan"), dict) else state.get("plan")
    attempts = cleanse_attempts

    if not actions:
        return {
            **state,
            "plan": plan,
            "cleansing_actions": actions,
            "cleansing_attempts": attempts,
            "next_action": "generate_specs",
        }

    return {
        **state,
        "plan": plan,
        "cleansing_actions": actions,
        "cleansing_attempts": attempts,
        "next_action": "preview_plan",
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
        tool_id="pipeline_plans.repair",
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

    next_action = "verify_intent"
    status = "running"
    validation_errors = list(data.get("validation_errors") or [])
    validation_warnings = list(data.get("validation_warnings") or [])
    updated_planner_hints = state.get("planner_hints")
    wiring_hints = _build_wiring_hints(validation_warnings)
    if wiring_hints:
        hints = dict(updated_planner_hints or {})
        hints["wiring_feedback"] = wiring_hints.get("warnings")
        hints["auto_wiring"] = wiring_hints
        updated_planner_hints = hints

    state_for_decision: PipelineAgentState = {**state, "planner_hints": updated_planner_hints}
    allow_specs = _allow_specs(state_for_decision)
    should_transform = _should_transform(state_for_decision)
    if plan_status == "clarification_required":
        if allow_specs and plan and _needs_output_split(plan) and _only_output_errors(validation_errors):
            next_action = "split_outputs"
        else:
            next_action = "clarify"
            status = "clarification_required"
    elif plan_status != "success":
        next_action = "end"
        status = "failed"
    elif allow_specs and _needs_output_split(plan):
        next_action = "split_outputs"
    elif should_transform:
        next_action = "transform"

    return {
        **state,
        "plan_id": plan_id,
        "plan": plan,
        "validation_errors": validation_errors,
        "validation_warnings": validation_warnings,
        "preflight": data.get("preflight") if isinstance(data.get("preflight"), dict) else None,
        "questions": questions,
        "planner_hints": updated_planner_hints,
        "next_action": next_action,
        "status": status,
        "repair_attempts": repair_attempts,
        "run_tables": None,
        "definition_digest": None,
    }


async def _generate_specs(state: PipelineAgentState, runtime: AgentRuntime) -> PipelineAgentState:
    if not _allow_specs(state):
        return {**state, "next_action": "end", "status": "success"}

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
        tool_id="pipeline_plans.generate_specs",
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

    async def profiler(state: PipelineAgentState) -> PipelineAgentState:
        return await _build_context_pack(state, runtime)

    async def orchestrator(state: PipelineAgentState) -> PipelineAgentState:
        return await _route_after_profile(state)

    async def scope_guard(state: PipelineAgentState) -> PipelineAgentState:
        return await _infer_task_spec(state, runtime)

    async def reporter(state: PipelineAgentState) -> PipelineAgentState:
        return await _run_report(state)

    async def join_strategist(state: PipelineAgentState) -> PipelineAgentState:
        return await _collect_join_hints(state, runtime)

    async def cleansing_strategist(state: PipelineAgentState) -> PipelineAgentState:
        return await _collect_cleansing_hints(state)

    async def plan_builder(state: PipelineAgentState) -> PipelineAgentState:
        return await _compile_plan(state, runtime)

    async def output_splitter(state: PipelineAgentState) -> PipelineAgentState:
        return await _split_outputs(state, runtime)

    async def plan_transformer(state: PipelineAgentState) -> PipelineAgentState:
        return await _transform_plan(state, runtime)

    async def intent_verifier(state: PipelineAgentState) -> PipelineAgentState:
        return await _verify_intent(state, runtime)

    async def plan_previewer(state: PipelineAgentState) -> PipelineAgentState:
        return await _preview_plan(state, runtime)

    async def join_evaluator(state: PipelineAgentState) -> PipelineAgentState:
        return await _evaluate_joins(state, runtime)

    async def preview_inspector(state: PipelineAgentState) -> PipelineAgentState:
        return await _inspect_preview(state, runtime)

    async def cleanser(state: PipelineAgentState) -> PipelineAgentState:
        return await _cleanse_plan(state, runtime)

    async def repairer(state: PipelineAgentState) -> PipelineAgentState:
        return await _repair_plan(state, runtime)

    async def mapper(state: PipelineAgentState) -> PipelineAgentState:
        return await _generate_specs(state, runtime)

    graph.add_node("profiler", profiler)
    graph.add_node("orchestrator", orchestrator)
    graph.add_node("scope_guard", scope_guard)
    graph.add_node("reporter", reporter)
    graph.add_node("join_strategist", join_strategist)
    graph.add_node("cleansing_strategist", cleansing_strategist)
    graph.add_node("plan_builder", plan_builder)
    graph.add_node("plan_transformer", plan_transformer)
    graph.add_node("intent_verifier", intent_verifier)
    graph.add_node("output_splitter", output_splitter)
    graph.add_node("plan_previewer", plan_previewer)
    graph.add_node("join_evaluator", join_evaluator)
    graph.add_node("preview_inspector", preview_inspector)
    graph.add_node("cleanser", cleanser)
    graph.add_node("repairer", repairer)
    graph.add_node("mapper", mapper)

    graph.set_entry_point("profiler")
    graph.add_conditional_edges(
        "profiler",
        lambda state: state.get("next_action", "route"),
        {"route": "orchestrator", "end": END},
    )
    graph.add_conditional_edges(
        "orchestrator",
        lambda state: state.get("next_action", "compile_plan"),
        {
            "task_spec": "scope_guard",
            "report": "reporter",
            "join_keys": "join_strategist",
            "cleanse_hints": "cleansing_strategist",
            "compile_plan": "plan_builder",
            "end": END,
        },
    )
    graph.add_edge("scope_guard", "orchestrator")
    graph.add_edge("join_strategist", "orchestrator")
    graph.add_edge("cleansing_strategist", "orchestrator")
    graph.add_edge("reporter", END)
    graph.add_conditional_edges(
        "plan_builder",
        lambda state: state.get("next_action", "end"),
        {
            "transform": "plan_transformer",
            "split_outputs": "output_splitter",
            "verify_intent": "intent_verifier",
            "clarify": END,
            "end": END,
        },
    )
    graph.add_conditional_edges(
        "plan_transformer",
        lambda state: state.get("next_action", "end"),
        {"split_outputs": "output_splitter", "verify_intent": "intent_verifier", "end": END},
    )
    graph.add_conditional_edges(
        "output_splitter",
        lambda state: state.get("next_action", "end"),
        {"transform": "plan_transformer", "verify_intent": "intent_verifier", "end": END},
    )
    graph.add_conditional_edges(
        "intent_verifier",
        lambda state: state.get("next_action", "end"),
        {"preview_plan": "plan_previewer", "transform": "plan_transformer", "end": END},
    )
    graph.add_conditional_edges(
        "plan_previewer",
        lambda state: state.get("next_action", "end"),
        {
            "repair": "repairer",
            "evaluate": "join_evaluator",
            "inspect": "preview_inspector",
            "generate_specs": "mapper",
            "end": END,
        },
    )
    graph.add_conditional_edges(
        "join_evaluator",
        lambda state: state.get("next_action", "end"),
        {"transform": "plan_transformer", "inspect": "preview_inspector", "generate_specs": "mapper", "end": END},
    )
    graph.add_conditional_edges(
        "preview_inspector",
        lambda state: state.get("next_action", "end"),
        {"cleanse": "cleanser", "generate_specs": "mapper", "end": END},
    )
    graph.add_conditional_edges(
        "cleanser",
        lambda state: state.get("next_action", "end"),
        {"preview_plan": "plan_previewer", "generate_specs": "mapper", "end": END},
    )
    graph.add_conditional_edges(
        "repairer",
        lambda state: state.get("next_action", "end"),
        {
            "split_outputs": "output_splitter",
            "transform": "plan_transformer",
            "verify_intent": "intent_verifier",
            "clarify": END,
            "end": END,
        },
    )
    graph.add_edge("mapper", END)
    return graph.compile()


async def run_pipeline_agent_graph(runtime: AgentRuntime, initial_state: PipelineAgentState) -> PipelineAgentState:
    app = build_pipeline_agent_graph(runtime)
    return await app.ainvoke(initial_state)


def build_pipeline_agent_state(
    *,
    goal: str,
    data_scope: Dict[str, Any],
    session_id: Optional[str],
    answers: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
    task_spec: Optional[Dict[str, Any]],
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
        session_id=session_id,
        goal=goal,
        data_scope=data_scope,
        answers=answers,
        planner_hints=planner_hints,
        task_spec=task_spec,
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
        verify_intent_attempts=0,
        preview_attempts=0,
        join_eval_attempts=0,
        preview_inspect_attempts=0,
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
        intent_status=None,
        intent_issues=None,
        intent_actions=None,
        intent_warnings=None,
        questions=[],
        report=None,
        specs=None,
        status="running",
        error=None,
        next_action="compile_plan",
    )
