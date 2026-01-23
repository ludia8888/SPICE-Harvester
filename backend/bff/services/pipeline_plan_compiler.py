"""
Pipeline plan compiler (LLM-native pipeline definition planner).

Generates typed PipelinePlan artifacts and validates them before preview.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Literal
from uuid import uuid4

from pydantic import BaseModel, Field, ValidationError, model_validator

from bff.services.pipeline_plan_validation import validate_pipeline_plan
from shared.models.agent_plan_report import PlanCompilationReport
from shared.models.pipeline_plan import PipelinePlan, PipelinePlanAssociation, PipelinePlanDataScope
from shared.models.pipeline_task_spec import PipelineTaskScope, PipelineTaskSpec
from shared.services.audit_log_store import AuditLogStore
from shared.services.dataset_registry import DatasetRegistry
from shared.services.pipeline_task_spec_policy import (
    clamp_task_spec,
    normalize_task_spec as normalize_task_spec_policy,
)
from shared.services.llm_gateway import (
    LLMCallMeta,
    LLMGateway,
    LLMOutputValidationError,
    LLMRequestError,
    LLMUnavailableError,
)
from shared.services.llm_quota import LLMQuotaExceededError, enforce_llm_quota
from shared.services.redis_service import RedisService


class PipelineClarificationQuestion(BaseModel):
    id: str = Field(..., min_length=1, max_length=100)
    question: str = Field(..., min_length=1, max_length=2000)
    required: bool = Field(default=True)
    type: str = Field(default="string", description="string|enum|boolean|number|object")
    options: Optional[List[str]] = None
    default: Optional[Any] = None


class PipelineClarificationPayload(BaseModel):
    questions: List[PipelineClarificationQuestion] = Field(default_factory=list)
    assumptions: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class PipelinePlanDraftEnvelope(BaseModel):
    plan: Dict[str, Any] = Field(default_factory=dict)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    notes: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    questions: List[PipelineClarificationQuestion] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def normalize_envelope(cls, data: Any):  # noqa: ANN001
        if not isinstance(data, dict):
            return data
        if "plan" in data:
            return data
        nested = data.get("definition_json")
        if isinstance(nested, dict) and any(key in nested for key in ("goal", "data_scope", "outputs")):
            normalized = dict(data)
            normalized.pop("definition_json", None)
            normalized["plan"] = nested
            return normalized
        if not any(key in data for key in ("goal", "definition_json", "outputs", "data_scope")):
            return data
        normalized = dict(data)
        plan_payload = {
            key: normalized[key]
            for key in list(normalized.keys())
            if key not in ("confidence", "notes", "warnings", "questions")
        }
        for key in plan_payload.keys():
            normalized.pop(key, None)
        normalized["plan"] = plan_payload
        return normalized


class PipelinePlanBuilderToolCall(BaseModel):
    tool: str = Field(..., min_length=1, max_length=80)
    args: Dict[str, Any] = Field(default_factory=dict)


class PipelinePlanBuilderScriptEnvelope(BaseModel):
    steps: List[PipelinePlanBuilderToolCall] = Field(default_factory=list)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    notes: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    questions: List[PipelineClarificationQuestion] = Field(default_factory=list)


@dataclass(frozen=True)
class PipelinePlanCompileResult:
    status: str  # success|clarification_required|error
    plan_id: str
    plan: Optional[PipelinePlan]
    validation_errors: List[str]
    validation_warnings: List[str]
    questions: List[PipelineClarificationQuestion]
    compilation_report: Optional[PlanCompilationReport] = None
    llm_meta: Optional[LLMCallMeta] = None
    planner_confidence: Optional[float] = None
    planner_notes: Optional[List[str]] = None
    preflight: Optional[Dict[str, Any]] = None


def _build_plan_system_prompt() -> str:
    return (
        "You are a STRICT enterprise pipeline planner for SPICE-Harvester.\n"
        "You MUST output a single JSON object only (no markdown, no commentary).\n"
        "You are NOT executing anything; you are producing a typed pipeline plan.\n"
        "\n"
        "Hard rules:\n"
        "- definition_json must include nodes[] and edges[] (edges may be empty if associations are provided).\n"
        "- nodes MUST only include {id,type,metadata}; edges MUST only include {from,to}.\n"
        "- Do NOT include UI/layout fields (position, x/y), labels, or extra properties.\n"
        "- Use supported operations only; no UDF.\n"
        "- join requires leftKey/rightKey or leftKeys/rightKeys (or joinKey) and allowCrossJoin must be false.\n"
        "- For composite joins, ALWAYS use leftKeys/rightKeys arrays.\n"
        "- join_key_candidates may include composite_group_id; if present, treat that group as a composite join.\n"
        "- If you provide associations, DO NOT try to wire join edges manually; the server will auto-wire.\n"
        "- You MUST still wire non-join transforms with edges (filter/cast/compute/select/drop/rename/groupBy/window).\n"
        "- filter nodes MUST use metadata.expression (not condition).\n"
        "- Edge order matters for join only when you wire edges: first incoming edge is LEFT, second is RIGHT.\n"
        "- Ensure join keys align to the left/right input order if you wire edges.\n"
        "- If you're unsure whether multi-stage transforms are needed (aggregate/window/groupBy), "
        "ask a clarification question in questions and keep the plan minimal.\n"
        "- If planner_hints.autonomy_level is \"high\", decide and proceed unless critical info is missing.\n"
        "- If planner_hints.cardinality_strategy is \"prefer_left_on_uncertain\", "
        "use left join when cardinality_confidence is low or cardinality_note warns about uncertainty.\n"
        "- If planner_hints.null_strategy is provided, default to that when choosing filter/dedupe vs keep nulls.\n"
        "- If planner_hints.numeric_columns is provided, use amount_candidates first for amount/metric calculations.\n"
        "- amount_candidates may include {column, score, reasons} and are ranked (highest score first).\n"
        "- definition_json MUST include at least one output node with type \"output\".\n"
        "- output nodes MUST use type \"output\" (not write_output) and include metadata.outputName or metadata.datasetName.\n"
        "- output node metadata.outputName MUST match outputs[].output_name for the same dataset.\n"
        "- Only use canonical outputs when the user explicitly requests canonical/object/link/ontology-backed results.\n"
        "- For canonical outputs, output_name MUST use canonical_obj_<entity> or canonical_lnk_<left>__<right>.\n"
        "- If the user does NOT ask for canonical outputs, keep output_kind=unknown and avoid canonical_* names.\n"
        "- If you can infer primary keys, include output node metadata.pkColumns and metadata.pkSemantics.\n"
        "- If you can infer data quality rules, include definition_json.expectations (not_null/unique).\n"
        "- Keep the graph minimal; prefer a linear join chain.\n"
        "- output_kind must be one of: object, link, unknown.\n"
        "- object outputs MUST include target_class_id.\n"
        "- link outputs MUST include link_type_id, source_class_id, target_class_id, predicate, cardinality,\n"
        "  source_key_column, target_key_column, and relationship_spec_type (join_table or foreign_key).\n"
        "- If canonical output metadata is missing, ask clarification questions instead of guessing.\n"
        "\n"
        "Output schema:\n"
        "{\n"
        "  \"plan\": {PipelinePlan JSON},\n"
        "  \"confidence\": number (0..1),\n"
        "  \"notes\": string[],\n"
        "  \"warnings\": string[],\n"
        "  \"questions\": [PipelineClarificationQuestion] (optional)\n"
        "}\n"
        "\n"
        "PipelinePlan JSON:\n"
        "{\n"
        "  \"goal\": string,\n"
        "  \"data_scope\": {\"db_name\": string, \"branch\": string, \"dataset_ids\": []},\n"
        "  \"definition_json\": {\"nodes\": [], \"edges\": []},\n"
        "  \"associations\": [\n"
        "    {\n"
        "      \"left_dataset_id\": string?,\n"
        "      \"right_dataset_id\": string?,\n"
        "      \"left_dataset_name\": string?,\n"
        "      \"right_dataset_name\": string?,\n"
        "      \"left_keys\": [string]?,\n"
        "      \"right_keys\": [string]?,\n"
        "      \"join_type\": \"inner|left|right|full\"?\n"
        "    }\n"
        "  ],\n"
        "  \"outputs\": [\n"
        "    {\n"
        "      \"output_name\": string,\n"
        "      \"output_kind\": \"object|link|unknown\",\n"
        "      \"target_class_id\": string?,\n"
        "      \"source_class_id\": string?,\n"
        "      \"link_type_id\": string?,\n"
        "      \"predicate\": string?,\n"
        "      \"cardinality\": string?,\n"
        "      \"source_key_column\": string?,\n"
        "      \"target_key_column\": string?,\n"
        "      \"relationship_spec_type\": \"join_table|foreign_key\"?\n"
        "    }\n"
        "  ],\n"
        "  \"warnings\": []\n"
        "}\n"
        "\n"
        "PipelineClarificationQuestion JSON:\n"
        "{\n"
        "  \"id\": string,\n"
        "  \"question\": string,\n"
        "  \"required\": boolean,\n"
        "  \"type\": \"string|enum|boolean|number|object\",\n"
        "  \"options\": [string]?,\n"
        "  \"default\": any?\n"
        "}\n"
    )


def _build_plan_user_prompt(
    *,
    goal: str,
    data_scope: Optional[PipelinePlanDataScope],
    answers: Optional[Dict[str, Any]],
    context_pack: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
) -> str:
    scope_payload = data_scope.model_dump(mode="json") if data_scope else {}
    answers_payload = answers or {}
    context_payload = context_pack or {}
    return (
        f"Goal:\n{goal}\n\n"
        f"Data scope:\n{json.dumps(scope_payload, ensure_ascii=False)}\n\n"
        f"Clarification answers (if any):\n{json.dumps(answers_payload, ensure_ascii=False)}\n\n"
        f"Planner hints (join/cleansing guidance):\n{json.dumps(planner_hints or {}, ensure_ascii=False)}\n\n"
        f"Pipeline context pack (safe summary):\n{json.dumps(context_payload, ensure_ascii=False)}\n"
    )


_PIPELINE_MCP_ALLOWED_TOOLS = (
    "plan_add_transform",
    "plan_add_join",
    "plan_add_filter",
    "plan_add_compute",
    "plan_add_cast",
    "plan_add_rename",
    "plan_add_select",
    "plan_add_drop",
    "plan_add_dedupe",
    "plan_add_normalize",
    "plan_add_regex_replace",
    "plan_add_output",
    "plan_add_edge",
    "plan_delete_edge",
    "plan_set_node_inputs",
    "plan_update_node_metadata",
    "plan_delete_node",
    "plan_update_output",
)


def _normalize_task_spec(task_spec: Optional[Dict[str, Any]], *, dataset_count: int) -> Optional[PipelineTaskSpec]:
    return normalize_task_spec_policy(task_spec, dataset_count=int(dataset_count or 0))


def _allowed_mcp_tools_for_task(
    *,
    task_spec: Optional[PipelineTaskSpec],
    mode: Literal["build", "repair"],
) -> List[str]:
    """
    Compute a per-request MCP tool allowlist.

    Security contract: tool list is restrictive by default when a task_spec is present.
    """
    base = [
        "plan_add_filter",
        "plan_add_compute",
        "plan_add_cast",
        "plan_add_rename",
        "plan_add_select",
        "plan_add_drop",
        "plan_add_dedupe",
        "plan_add_normalize",
        "plan_add_regex_replace",
        "plan_add_output",
    ]
    if task_spec is None:
        # Backwards-compatible default: allow everything.
        allowed = list(_PIPELINE_MCP_ALLOWED_TOOLS)
        return allowed

    allowed = list(base)
    if bool(task_spec.allow_join):
        allowed.append("plan_add_join")
    if bool(task_spec.allow_advanced_transforms):
        allowed.append("plan_add_transform")

    if mode == "repair":
        # Patch tools are only useful during repair.
        allowed.extend(
            [
                "plan_add_edge",
                "plan_delete_edge",
                "plan_set_node_inputs",
                "plan_update_node_metadata",
                "plan_delete_node",
                "plan_update_output",
            ]
        )

    # Ensure stable order + no duplicates.
    deduped = list(dict.fromkeys([tool for tool in allowed if tool in _PIPELINE_MCP_ALLOWED_TOOLS]))
    return deduped


def _validate_mcp_step_against_task_spec(
    *,
    tool_name: str,
    args: Dict[str, Any],
    task_spec: Optional[PipelineTaskSpec],
    mode: Literal["build", "repair"],
) -> Optional[str]:
    """
    Additional semantic checks that are stricter than the tool allowlist.

    This blocks "join via generic transform/patch" when joins are disallowed.
    """
    if task_spec is None:
        return None

    if task_spec.scope == PipelineTaskScope.report_only:
        return "task_spec.scope=report_only forbids plan compilation"

    allow_join = bool(task_spec.allow_join)
    allow_advanced = bool(task_spec.allow_advanced_transforms)

    if not allow_advanced and tool_name == "plan_add_transform":
        return "plan_add_transform is not allowed for this task"

    if not allow_join:
        if tool_name in {"plan_add_join"}:
            return f"{tool_name} is not allowed (joins disabled)"
        if tool_name == "plan_add_transform":
            op = str(args.get("operation") or "").strip()
            if op.lower() in {"join", "union"}:
                return f"operation={op} is not allowed (joins disabled)"
        if tool_name == "plan_update_node_metadata":
            set_fields = args.get("set")
            if isinstance(set_fields, dict):
                op = set_fields.get("operation")
                if isinstance(op, str) and op.strip().lower() in {"join", "union"}:
                    return f"operation={op} is not allowed (joins disabled)"
                join_keys = {"joinType", "allowCrossJoin", "leftKeys", "rightKeys", "leftKey", "rightKey"}
                if any(key in set_fields for key in join_keys):
                    return "join metadata patch is not allowed (joins disabled)"

    if not allow_advanced and tool_name == "plan_update_node_metadata":
        set_fields = args.get("set")
        if isinstance(set_fields, dict):
            op = set_fields.get("operation")
            if isinstance(op, str) and op.strip().lower() in {"groupby", "aggregate", "pivot", "window", "explode", "sort", "union"}:
                return f"operation={op} is not allowed (advanced transforms disabled)"
    return None


def _build_mcp_script_system_prompt(*, mode: Literal["build", "repair"], allowed_tools: List[str]) -> str:
    """
    Planner prompt for producing a sequence of MCP plan-builder tool calls.

    The planner MUST NOT emit definition_json directly.
    """
    header = "You are a STRICT enterprise pipeline planner for SPICE-Harvester.\n"
    if mode == "repair":
        header = "You are a STRICT enterprise pipeline plan repair planner for SPICE-Harvester.\n"

    rules = (
        "- Use ONLY the allowed tools listed below.\n"
        "- Do NOT invent dataset ids, node ids, or column names; use ONLY what is present in the context pack and current_plan.\n"
        "- Do NOT output definition_json directly.\n"
        "- Cross joins are forbidden.\n"
        "- Prefer plan_add_join for joins. Use plan_add_transform only for advanced ops (groupBy/aggregate/window/pivot/union/sort/explode).\n"
    )
    if mode == "build":
        rules += "- Always end with plan_add_output.\n"
    else:
        rules += (
            "- You will be given current_plan. Apply MINIMAL edits (patch/delete/rewire) instead of rebuilding.\n"
            "- Do NOT add duplicate outputs if current_plan already has a valid output; patch instead.\n"
            "- Use plan_set_node_inputs to fix join LEFT/RIGHT input order.\n"
        )

    return (
        header
        + "Return ONLY a single JSON object. No markdown, no commentary.\n"
        + "You are NOT executing anything; you are producing a list of plan-builder tool calls.\n"
        + "\n"
        + "Hard rules:\n"
        + rules
        + "\n"
        + "Allowed tools:\n"
        + json.dumps(list(allowed_tools or []), ensure_ascii=False)
        + "\n\n"
        + "Tool schemas (args only; server injects `plan` automatically):\n"
        + "- plan_add_join: {left_node_id,right_node_id,left_keys[],right_keys[],join_type?,node_id?}\n"
        + "- plan_add_filter: {input_node_id,expression,node_id?}\n"
        + "- plan_add_compute: {input_node_id,expression,node_id?}\n"
        + "- plan_add_cast: {input_node_id,casts:[{column,type}],node_id?}\n"
        + "- plan_add_rename: {input_node_id,rename:{src:dst},node_id?}\n"
        + "- plan_add_select/drop/dedupe: {input_node_id,columns[],node_id?}\n"
        + "- plan_add_normalize: {input_node_id,columns[],trim?,empty_to_null?,whitespace_to_null?,lowercase?,uppercase?,node_id?}\n"
        + "- plan_add_regex_replace: {input_node_id,rules:[{column,pattern,replacement?,flags?}],node_id?}\n"
        + "- plan_add_transform: {operation,input_node_ids[],metadata?,node_id?}\n"
        + "- plan_add_output: {input_node_id,output_name,output_kind?,node_id?,output_metadata?}\n"
        + "- plan_add_edge: {from_node_id,to_node_id}\n"
        + "- plan_delete_edge: {from_node_id,to_node_id}\n"
        + "- plan_set_node_inputs: {node_id,input_node_ids[]}\n"
        + "- plan_update_node_metadata: {node_id,set?,unset?,replace?}\n"
        + "- plan_delete_node: {node_id}\n"
        + "- plan_update_output: {output_name,set?,unset?,replace?}\n"
        + "\n"
        + "Output schema:\n"
        + "{\n"
        + "  \"steps\": [{\"tool\": string, \"args\": object}],\n"
        + "  \"confidence\": number (0..1),\n"
        + "  \"notes\": string[],\n"
        + "  \"warnings\": string[],\n"
        + "  \"questions\": [PipelineClarificationQuestion] (optional)\n"
        + "}\n"
    )


def _build_mcp_script_user_prompt(
    *,
    goal: str,
    data_scope: Optional[PipelinePlanDataScope],
    answers: Optional[Dict[str, Any]],
    context_pack: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
    input_nodes: List[Dict[str, Any]],
    current_plan: Optional[Dict[str, Any]] = None,
    preflight: Optional[Dict[str, Any]] = None,
    preview: Optional[Dict[str, Any]] = None,
    compilation_report: Optional[Dict[str, Any]] = None,
    prior_errors: Optional[List[str]] = None,
    prior_warnings: Optional[List[str]] = None,
) -> str:
    scope_payload = data_scope.model_dump(mode="json") if data_scope else {}
    payload = {
        "goal": goal,
        "data_scope": scope_payload,
        "answers": answers or {},
        "planner_hints": planner_hints or {},
        "input_nodes": input_nodes,
        "current_plan": current_plan,
        "preflight": preflight,
        "preview": preview,
        "compilation_report": compilation_report,
        "context_pack": context_pack or {},
        "prior_validation_errors": list(prior_errors or []),
        "prior_validation_warnings": list(prior_warnings or []),
    }
    return json.dumps(payload, ensure_ascii=False)


def _build_clarifier_system_prompt() -> str:
    return (
        "You are a strict pipeline plan clarifier.\n"
        "Return ONLY JSON with questions needed to fix validation errors.\n"
        "Do NOT propose a new plan; ask focused questions.\n"
        "Write the questions in the same language as the user's goal.\n"
        "If validation errors mention missing/unknown columns, explicitly mention those column names.\n"
    )


def _build_clarifier_user_prompt(
    *,
    goal: str,
    draft_plan: Dict[str, Any],
    validation_errors: List[str],
    validation_warnings: List[str],
    data_scope: Optional[PipelinePlanDataScope],
    context_pack: Optional[Dict[str, Any]],
    ) -> str:
        scope_payload = data_scope.model_dump(mode="json") if data_scope else {}
        return (
        f"Goal:\n{goal}\n\n"
        f"Data scope:\n{json.dumps(scope_payload, ensure_ascii=False)}\n\n"
        f"Draft plan:\n{json.dumps(draft_plan, ensure_ascii=False)}\n\n"
        f"Validation errors:\n{json.dumps(validation_errors, ensure_ascii=False)}\n\n"
        f"Validation warnings:\n{json.dumps(validation_warnings, ensure_ascii=False)}\n\n"
        f"Context pack:\n{json.dumps(context_pack or {}, ensure_ascii=False)}\n"
    )


def _build_repair_system_prompt() -> str:
    return (
        "You are a STRICT pipeline plan repair agent.\n"
        "Return ONLY JSON following the PipelinePlan output schema.\n"
        "Fix validation/preview errors by adjusting definition_json.\n"
        "If join wiring is unclear, prefer updating associations and keep join edges minimal.\n"
        "Preserve the original data_scope and output intent.\n"
        "Keep changes minimal and deterministic.\n"
    )


def _build_repair_user_prompt(
    *,
    plan: PipelinePlan,
    validation_errors: List[str],
    validation_warnings: List[str],
    preflight: Optional[Dict[str, Any]],
    preview: Optional[Dict[str, Any]],
    context_pack: Optional[Dict[str, Any]],
) -> str:
    return (
        f"Current plan:\n{json.dumps(plan.model_dump(mode='json'), ensure_ascii=False)}\n\n"
        f"Validation errors:\n{json.dumps(validation_errors or [], ensure_ascii=False)}\n\n"
        f"Validation warnings:\n{json.dumps(validation_warnings or [], ensure_ascii=False)}\n\n"
        f"Preflight:\n{json.dumps(preflight or {}, ensure_ascii=False)}\n\n"
        f"Preview:\n{json.dumps(preview or {}, ensure_ascii=False)}\n\n"
        f"Context pack:\n{json.dumps(context_pack or {}, ensure_ascii=False)}\n"
    )


def _fallback_questions_from_errors(errors: List[str]) -> List[PipelineClarificationQuestion]:
    # Do not generate rule-based user-facing questions here.
    # If the LLM clarifier is unavailable, callers should surface validation_errors
    # and let the client decide how to recover (retry, ask user, etc).
    _ = errors
    return []


def _associations_from_join_plan(join_plan: Optional[List[Dict[str, Any]]]) -> List[PipelinePlanAssociation]:
    if not isinstance(join_plan, list):
        return []
    associations: List[PipelinePlanAssociation] = []
    for idx, raw in enumerate(join_plan):
        if not isinstance(raw, dict):
            continue
        normalized = dict(raw)
        if "cardinality" in normalized and "cardinality_hint" not in normalized:
            normalized["cardinality_hint"] = normalized.get("cardinality")
        if "association_id" not in normalized:
            normalized["association_id"] = f"assoc_{idx + 1}"
        try:
            associations.append(PipelinePlanAssociation.model_validate(normalized))
        except Exception:
            continue
    return associations


async def compile_pipeline_plan(
    *,
    goal: str,
    data_scope: Optional[PipelinePlanDataScope],
    answers: Optional[Dict[str, Any]],
    context_pack: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
    task_spec: Optional[Dict[str, Any]],
    actor: str,
    tenant_id: str,
    user_id: Optional[str],
    data_policies: Optional[Dict[str, Any]],
    selected_model: Optional[str],
    allowed_models: Optional[List[str]],
    llm_gateway: LLMGateway,
    redis_service: Optional[RedisService],
    audit_store: Optional[AuditLogStore],
    dataset_registry: DatasetRegistry,
) -> PipelinePlanCompileResult:
    plan_id = str(uuid4())

    dataset_count = len(list(data_scope.dataset_ids or [])) if data_scope else 0
    task_spec_model = normalize_task_spec_policy(task_spec, dataset_count=dataset_count)
    if task_spec_model and task_spec_model.scope == PipelineTaskScope.report_only:
        questions: List[PipelineClarificationQuestion] = []
        for item in list(task_spec_model.questions or []):
            if not isinstance(item, dict):
                continue
            try:
                questions.append(PipelineClarificationQuestion.model_validate(item))
            except Exception:
                continue
        if not questions:
            questions = _fallback_questions_from_errors(["This request is report-only; no pipeline plan will be compiled."])
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=["task_spec.scope=report_only; no pipeline plan will be compiled"],
            validation_warnings=[],
            questions=questions,
            planner_confidence=float(task_spec_model.confidence),
            planner_notes=list(task_spec_model.notes or []),
        )

    system_prompt = _build_plan_system_prompt()
    user_prompt = _build_plan_user_prompt(
        goal=goal,
        data_scope=data_scope,
        answers=answers,
        context_pack=context_pack,
        planner_hints=(
            {**(planner_hints or {}), "task_spec": task_spec_model.model_dump(mode="json")}
            if task_spec_model
            else ({**(planner_hints or {}), "task_spec": task_spec} if task_spec else planner_hints)
        ),
    )
    llm_meta: Optional[LLMCallMeta] = None

    if data_policies and redis_service:
        model_for_quota = str(selected_model or getattr(llm_gateway, "model", "") or "").strip()
        if model_for_quota:
            await enforce_llm_quota(
                redis_service=redis_service,
                tenant_id=tenant_id,
                user_id=user_id,
                model_id=model_for_quota,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                data_policies=data_policies,
            )

    draft = None
    draft_plan_obj: Dict[str, Any] = {}
    last_exc: Exception | None = None
    for attempt in range(2):
        try:
            draft, llm_meta = await llm_gateway.complete_json(
                task="PIPELINE_PLAN_COMPILE_V1",
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=PipelinePlanDraftEnvelope,
                model=selected_model,
                allowed_models=allowed_models,
                redis_service=redis_service,
                audit_store=audit_store,
                audit_partition_key=f"pipeline_plan:{plan_id}",
                audit_actor=actor,
                audit_resource_id=plan_id,
                audit_metadata={
                    "kind": "pipeline_plan_compile",
                    "schema": "PipelinePlan",
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "attempt": attempt + 1,
                },
            )
            draft_plan_obj = dict(draft.plan or {})
            last_exc = None
            break
        except LLMOutputValidationError as exc:
            last_exc = exc
            if attempt < 1:
                continue
            break
        except (LLMUnavailableError, LLMRequestError) as exc:
            last_exc = exc
            break
    if last_exc is not None:
        questions = _fallback_questions_from_errors([str(last_exc)])
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=[str(last_exc)],
            validation_warnings=[],
            questions=questions,
            llm_meta=llm_meta,
        )

    if draft is not None and draft.questions:
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=["planner requested clarification"],
            validation_warnings=[],
            questions=list(draft.questions or []),
            llm_meta=llm_meta,
            planner_confidence=float(draft.confidence),
            planner_notes=draft.notes if draft is not None else None,
        )

    try:
        plan = PipelinePlan.model_validate(draft_plan_obj)
    except ValidationError as exc:
        errors = [str(e.get("msg") or e) for e in exc.errors()]
        questions = _fallback_questions_from_errors(errors)
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=errors,
            validation_warnings=[],
            questions=questions,
            llm_meta=llm_meta,
            planner_confidence=float(draft.confidence) if draft is not None else None,
            planner_notes=draft.notes if draft is not None else None,
        )

    plan = plan.model_copy(
        update={
            "plan_id": plan.plan_id or plan_id,
            "created_by": plan.created_by or actor,
            "data_scope": data_scope or plan.data_scope,
            "task_spec": task_spec_model,
        }
    )
    if not plan.associations and (task_spec_model is None or bool(task_spec_model.allow_join)):
        join_plan = planner_hints.get("join_plan") if isinstance(planner_hints, dict) else None
        derived = _associations_from_join_plan(join_plan)
        if derived:
            plan = plan.model_copy(update={"associations": derived})

    scope_db = str(plan.data_scope.db_name or "").strip()
    scope_branch = str(plan.data_scope.branch or "").strip() or None
    validation = await validate_pipeline_plan(
        plan=plan,
        dataset_registry=dataset_registry,
        db_name=scope_db,
        branch=scope_branch,
        require_output=True,
        context_pack=context_pack,
    )

    if validation.errors:
        questions: List[PipelineClarificationQuestion] = []
        clarification_meta: Optional[LLMCallMeta] = None
        try:
            clarifier_system_prompt = _build_clarifier_system_prompt()
            clarifier_user_prompt = _build_clarifier_user_prompt(
                goal=goal,
                draft_plan=validation.plan.model_dump(mode="json"),
                validation_errors=validation.errors,
                validation_warnings=validation.warnings,
                data_scope=data_scope,
                context_pack=context_pack,
            )
            if data_policies and redis_service:
                model_for_quota = str(selected_model or getattr(llm_gateway, "model", "") or "").strip()
                if model_for_quota:
                    await enforce_llm_quota(
                        redis_service=redis_service,
                        tenant_id=tenant_id,
                        user_id=user_id,
                        model_id=model_for_quota,
                        system_prompt=clarifier_system_prompt,
                        user_prompt=clarifier_user_prompt,
                        data_policies=data_policies,
                    )
            clarification, clarification_meta = await llm_gateway.complete_json(
                task="PIPELINE_PLAN_CLARIFY_V1",
                system_prompt=clarifier_system_prompt,
                user_prompt=clarifier_user_prompt,
                response_model=PipelineClarificationPayload,
                model=selected_model,
                allowed_models=allowed_models,
                redis_service=redis_service,
                audit_store=audit_store,
                audit_partition_key=f"pipeline_plan:{plan_id}",
                audit_actor=actor,
                audit_resource_id=plan_id,
                audit_metadata={"kind": "pipeline_plan_clarify", "tenant_id": tenant_id, "user_id": user_id},
            )
            questions = list(clarification.questions or [])
            llm_meta = clarification_meta or llm_meta
        except LLMQuotaExceededError:
            raise
        except Exception:
            questions = _fallback_questions_from_errors(validation.errors)

        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=validation.plan,
            validation_errors=validation.errors,
            validation_warnings=list(validation.warnings or []),
            questions=questions,
            compilation_report=validation.compilation_report,
            llm_meta=llm_meta,
            planner_confidence=float(draft.confidence) if draft is not None else None,
            planner_notes=draft.notes if draft is not None else None,
            preflight=validation.preflight,
        )

    return PipelinePlanCompileResult(
        status="success",
        plan_id=plan_id,
        plan=validation.plan,
        validation_errors=[],
        validation_warnings=list(validation.warnings or []),
        questions=[],
        compilation_report=validation.compilation_report,
        llm_meta=llm_meta,
        planner_confidence=float(draft.confidence) if draft is not None else None,
        planner_notes=draft.notes if draft is not None else None,
        preflight=validation.preflight,
    )


async def compile_pipeline_plan_mcp(
    *,
    goal: str,
    data_scope: Optional[PipelinePlanDataScope],
    answers: Optional[Dict[str, Any]],
    context_pack: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
    task_spec: Optional[Dict[str, Any]],
    actor: str,
    tenant_id: str,
    user_id: Optional[str],
    data_policies: Optional[Dict[str, Any]],
    selected_model: Optional[str],
    allowed_models: Optional[List[str]],
    llm_gateway: LLMGateway,
    redis_service: Optional[RedisService],
    audit_store: Optional[AuditLogStore],
    dataset_registry: DatasetRegistry,
) -> PipelinePlanCompileResult:
    """
    MCP-based pipeline plan compiler.

    Instead of having the LLM emit PipelinePlan/definition_json directly,
    the LLM emits a sequence of deterministic plan-builder tool calls.
    """
    plan_id = str(uuid4())

    # Lazy import to keep MCP optional in environments where the SDK/servers are not installed.
    try:
        try:
            from mcp.mcp_client import get_mcp_manager  # type: ignore[import-not-found]
        except Exception:  # pragma: no cover
            from backend.mcp.mcp_client import get_mcp_manager  # type: ignore[import-not-found]
    except Exception as exc:
        questions = _fallback_questions_from_errors([f"MCP client unavailable: {exc}"])
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=[f"MCP client unavailable: {exc}"],
            validation_warnings=[],
            questions=questions,
        )

    mcp_manager = get_mcp_manager()

    scope_db = str((data_scope.db_name if data_scope else "") or "").strip()
    scope_branch = str(data_scope.branch or "").strip() or None if data_scope else None

    selected_datasets = []
    if isinstance(context_pack, dict):
        raw_selected = context_pack.get("selected_datasets")
        if isinstance(raw_selected, list):
            selected_datasets = [item for item in raw_selected if isinstance(item, dict)]
    selected_by_id = {
        str(item.get("dataset_id") or "").strip(): item
        for item in selected_datasets
        if str(item.get("dataset_id") or "").strip()
    }

    dataset_ids: list[str] = []
    if data_scope and isinstance(data_scope.dataset_ids, list) and data_scope.dataset_ids:
        dataset_ids = [str(item).strip() for item in data_scope.dataset_ids if str(item).strip()]
    if not dataset_ids and selected_datasets:
        dataset_ids = [str(item.get("dataset_id") or "").strip() for item in selected_datasets if str(item.get("dataset_id") or "").strip()]

    if not scope_db:
        questions = _fallback_questions_from_errors(["data_scope.db_name is required"])
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=["data_scope.db_name is required"],
            validation_warnings=[],
            questions=questions,
        )
    if not dataset_ids:
        questions = _fallback_questions_from_errors(["data_scope.dataset_ids is required (no datasets selected)"])
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=["data_scope.dataset_ids is required (no datasets selected)"],
            validation_warnings=[],
            questions=questions,
        )

    task_spec_model = _normalize_task_spec(task_spec, dataset_count=len(dataset_ids))
    if task_spec_model and task_spec_model.scope == PipelineTaskScope.report_only:
        questions: List[PipelineClarificationQuestion] = []
        for item in list(task_spec_model.questions or []):
            if not isinstance(item, dict):
                continue
            try:
                questions.append(PipelineClarificationQuestion.model_validate(item))
            except Exception:
                continue
        if not questions:
            questions = _fallback_questions_from_errors(["This request is report-only; no pipeline plan will be compiled."])
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=["task_spec.scope=report_only; no pipeline plan will be compiled"],
            validation_warnings=[],
            questions=questions,
            planner_confidence=float(task_spec_model.confidence),
            planner_notes=list(task_spec_model.notes or []),
        )

    def _dataset_info(dataset_id: str) -> tuple[Optional[str], Optional[str], Dict[str, Any]]:
        item = selected_by_id.get(str(dataset_id or "").strip(), None)
        if not isinstance(item, dict):
            return None, None, {}
        name = str(item.get("name") or "").strip() or None
        branch = str(item.get("branch") or "").strip() or None
        return name, branch, item

    async def _call_pipeline_tool(tool: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        payload = await mcp_manager.call_tool("pipeline", tool, arguments)
        if isinstance(payload, dict):
            return payload
        # Defensive fallback: some MCP client implementations return a wrapper object.
        data = getattr(payload, "data", None)
        if isinstance(data, dict):
            return data
        raise RuntimeError(f"Unexpected MCP tool result type: {type(payload)}")

    last_validation_errors: list[str] = []
    last_validation_warnings: list[str] = []
    last_plan: Optional[PipelinePlan] = None
    last_preflight: Optional[Dict[str, Any]] = None
    last_report: Optional[PlanCompilationReport] = None
    last_llm_meta: Optional[LLMCallMeta] = None
    last_confidence: Optional[float] = None
    last_notes: Optional[List[str]] = None
    last_plan_obj: Optional[Dict[str, Any]] = None
    input_nodes_for_prompt: List[Dict[str, Any]] = []

    for attempt in range(2):
        mode: Literal["build", "repair"] = "build" if attempt == 0 else "repair"

        if attempt == 0:
            base = await _call_pipeline_tool(
                "plan_new",
                {
                    "goal": goal,
                    "db_name": scope_db,
                    "branch": scope_branch or "",
                    "dataset_ids": dataset_ids,
                },
            )
            plan_obj = base.get("plan") if isinstance(base, dict) else None
            if not isinstance(plan_obj, dict):
                return PipelinePlanCompileResult(
                    status="clarification_required",
                    plan_id=plan_id,
                    plan=None,
                    validation_errors=["MCP plan_new failed"],
                    validation_warnings=[],
                    questions=_fallback_questions_from_errors(["MCP plan_new failed"]),
                )

            # Add deterministic input nodes for the selected datasets (once).
            input_nodes_for_prompt = []
            for idx, dataset_id in enumerate(dataset_ids):
                dataset_name, dataset_branch, dataset_obj = _dataset_info(dataset_id)
                node_id = f"input_{idx + 1}"
                args: Dict[str, Any] = {
                    "plan": plan_obj,
                    "dataset_id": dataset_id,
                    "node_id": node_id,
                }
                if dataset_name:
                    args["dataset_name"] = dataset_name
                branch_value = dataset_branch or scope_branch
                if branch_value:
                    args["dataset_branch"] = branch_value
                res = await _call_pipeline_tool("plan_add_input", args)
                if isinstance(res, dict) and res.get("error"):
                    return PipelinePlanCompileResult(
                        status="clarification_required",
                        plan_id=plan_id,
                        plan=None,
                        validation_errors=[f"plan_add_input failed: {res.get('error')}"],
                        validation_warnings=[],
                        questions=_fallback_questions_from_errors([f"plan_add_input failed: {res.get('error')}"]),
                    )
                plan_obj = res.get("plan") if isinstance(res, dict) else plan_obj
                columns = [
                    str(col.get("name") or "").strip()
                    for col in (dataset_obj.get("columns") or [])
                    if isinstance(col, dict) and str(col.get("name") or "").strip()
                ]
                input_nodes_for_prompt.append(
                    {
                        "node_id": node_id,
                        "dataset_id": dataset_id,
                        "dataset_name": dataset_name,
                        "dataset_branch": branch_value,
                        "columns": columns[:25],
                    }
                )
        else:
            if last_plan is not None:
                plan_obj = last_plan.model_dump(mode="json")
            elif last_plan_obj is not None:
                plan_obj = last_plan_obj
            else:
                questions = _fallback_questions_from_errors(last_validation_errors or ["planner failed"])
                return PipelinePlanCompileResult(
                    status="clarification_required",
                    plan_id=plan_id,
                    plan=None,
                    validation_errors=last_validation_errors or ["planner failed"],
                    validation_warnings=last_validation_warnings,
                    questions=questions,
                    llm_meta=last_llm_meta,
                    planner_confidence=last_confidence,
                    planner_notes=last_notes,
                )

        input_nodes = input_nodes_for_prompt or []

        allowed_tools = _allowed_mcp_tools_for_task(task_spec=task_spec_model, mode=mode)
        allowed_tool_set = set(allowed_tools)

        system_prompt = _build_mcp_script_system_prompt(mode=mode, allowed_tools=allowed_tools)
        user_prompt = _build_mcp_script_user_prompt(
            goal=goal,
            data_scope=data_scope,
            answers=answers,
            context_pack=context_pack,
            planner_hints={**(planner_hints or {}), "task_spec": (task_spec_model.model_dump(mode="json") if task_spec_model else task_spec)} if (task_spec_model or task_spec) else planner_hints,
            input_nodes=input_nodes,
            current_plan=plan_obj if mode == "repair" else None,
            preflight=last_preflight if mode == "repair" else None,
            compilation_report=last_report.model_dump(mode="json") if (mode == "repair" and last_report) else None,
            prior_errors=last_validation_errors if mode == "repair" else None,
            prior_warnings=last_validation_warnings if mode == "repair" else None,
        )

        if data_policies and redis_service:
            model_for_quota = str(selected_model or getattr(llm_gateway, "model", "") or "").strip()
            if model_for_quota:
                await enforce_llm_quota(
                    redis_service=redis_service,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    model_id=model_for_quota,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    data_policies=data_policies,
                )

        try:
            draft, last_llm_meta = await llm_gateway.complete_json(
                task="PIPELINE_PLAN_MCP_SCRIPT_V1" if attempt == 0 else "PIPELINE_PLAN_MCP_SCRIPT_REPAIR_V1",
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=PipelinePlanBuilderScriptEnvelope,
                model=selected_model,
                allowed_models=allowed_models,
                redis_service=redis_service,
                audit_store=audit_store,
                audit_partition_key=f"pipeline_plan:{plan_id}",
                audit_actor=actor,
                audit_resource_id=plan_id,
                audit_metadata={
                    "kind": "pipeline_plan_mcp_script",
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "attempt": attempt + 1,
                },
            )
        except (LLMUnavailableError, LLMRequestError, LLMOutputValidationError) as exc:
            questions = _fallback_questions_from_errors([str(exc)])
            return PipelinePlanCompileResult(
                status="clarification_required",
                plan_id=plan_id,
                plan=None,
                validation_errors=[str(exc)],
                validation_warnings=[],
                questions=questions,
                llm_meta=last_llm_meta,
            )

        last_confidence = float(draft.confidence) if draft is not None else None
        last_notes = list(draft.notes or []) if draft is not None else None

        if draft.questions:
            return PipelinePlanCompileResult(
                status="clarification_required",
                plan_id=plan_id,
                plan=None,
                validation_errors=["planner requested clarification"],
                validation_warnings=[],
                questions=list(draft.questions or []),
                llm_meta=last_llm_meta,
                planner_confidence=last_confidence,
                planner_notes=last_notes,
            )

        tool_errors: list[str] = []
        tool_warnings: list[str] = list(draft.warnings or [])
        steps = list(draft.steps or [])
        if not steps:
            tool_errors.append("planner returned no steps")

        for step in steps:
            tool_name = str(step.tool or "").strip()
            args = dict(step.args or {})
            if tool_name not in allowed_tool_set:
                tool_errors.append(f"unsupported tool: {tool_name}")
                continue
            policy_error = _validate_mcp_step_against_task_spec(
                tool_name=tool_name,
                args=args,
                task_spec=task_spec_model,
                mode=mode,
            )
            if policy_error:
                tool_errors.append(f"{tool_name}: {policy_error}")
                continue
            args["plan"] = plan_obj
            res = await _call_pipeline_tool(tool_name, args)
            if isinstance(res, dict) and res.get("error"):
                tool_errors.append(f"{tool_name}: {res.get('error')}")
                continue
            if isinstance(res, dict) and isinstance(res.get("warnings"), list):
                tool_warnings.extend([str(item) for item in res.get("warnings") or [] if str(item or "").strip()])
            if isinstance(res, dict) and isinstance(res.get("plan"), dict):
                plan_obj = res["plan"]

        last_plan_obj = plan_obj if isinstance(plan_obj, dict) else last_plan_obj
        if tool_errors:
            last_validation_errors = tool_errors
            last_validation_warnings = tool_warnings
            continue

        try:
            plan_model = PipelinePlan.model_validate(plan_obj)
        except ValidationError as exc:
            last_validation_errors = [str(e.get("msg") or e) for e in exc.errors()]
            last_validation_warnings = tool_warnings
            continue

        plan_model = plan_model.model_copy(
            update={
                "plan_id": plan_model.plan_id or plan_id,
                "created_by": plan_model.created_by or actor,
                "data_scope": data_scope or plan_model.data_scope,
                "task_spec": task_spec_model,
            }
        )
        if not plan_model.associations and (task_spec_model is None or bool(task_spec_model.allow_join)):
            join_plan = planner_hints.get("join_plan") if isinstance(planner_hints, dict) else None
            derived = _associations_from_join_plan(join_plan)
            if derived:
                plan_model = plan_model.model_copy(update={"associations": derived})

        validation = await validate_pipeline_plan(
            plan=plan_model,
            dataset_registry=dataset_registry,
            db_name=scope_db,
            branch=scope_branch,
            require_output=True,
            context_pack=context_pack,
        )

        last_plan = validation.plan
        last_plan_obj = validation.plan.model_dump(mode="json")
        last_preflight = validation.preflight
        last_report = validation.compilation_report
        last_validation_errors = list(validation.errors or [])
        last_validation_warnings = list(tool_warnings) + list(validation.warnings or [])

        if not validation.errors:
            return PipelinePlanCompileResult(
                status="success",
                plan_id=plan_id,
                plan=validation.plan,
                validation_errors=[],
                validation_warnings=last_validation_warnings,
                questions=[],
                compilation_report=validation.compilation_report,
                llm_meta=last_llm_meta,
                planner_confidence=last_confidence,
                planner_notes=last_notes,
                preflight=validation.preflight,
            )

    # Failed after attempts; ask clarifier questions on the last plan (if any).
    if last_plan is None:
        questions = _fallback_questions_from_errors(last_validation_errors or ["planner failed"])
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=last_validation_errors or ["planner failed"],
            validation_warnings=last_validation_warnings,
            questions=questions,
            llm_meta=last_llm_meta,
            planner_confidence=last_confidence,
            planner_notes=last_notes,
        )

    questions: List[PipelineClarificationQuestion] = []
    try:
        clarifier_system_prompt = _build_clarifier_system_prompt()
        clarifier_user_prompt = _build_clarifier_user_prompt(
            goal=goal,
            draft_plan=last_plan.model_dump(mode="json"),
            validation_errors=last_validation_errors,
            validation_warnings=last_validation_warnings,
            data_scope=data_scope,
            context_pack=context_pack,
        )
        if data_policies and redis_service:
            model_for_quota = str(selected_model or getattr(llm_gateway, "model", "") or "").strip()
            if model_for_quota:
                await enforce_llm_quota(
                    redis_service=redis_service,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    model_id=model_for_quota,
                    system_prompt=clarifier_system_prompt,
                    user_prompt=clarifier_user_prompt,
                    data_policies=data_policies,
                )
        clarification, clarification_meta = await llm_gateway.complete_json(
            task="PIPELINE_PLAN_CLARIFY_V1",
            system_prompt=clarifier_system_prompt,
            user_prompt=clarifier_user_prompt,
            response_model=PipelineClarificationPayload,
            model=selected_model,
            allowed_models=allowed_models,
            redis_service=redis_service,
            audit_store=audit_store,
            audit_partition_key=f"pipeline_plan:{plan_id}",
            audit_actor=actor,
            audit_resource_id=plan_id,
            audit_metadata={"kind": "pipeline_plan_clarify", "tenant_id": tenant_id, "user_id": user_id},
        )
        questions = list(clarification.questions or [])
        last_llm_meta = clarification_meta or last_llm_meta
    except Exception:
        questions = _fallback_questions_from_errors(last_validation_errors)

    return PipelinePlanCompileResult(
        status="clarification_required",
        plan_id=plan_id,
        plan=last_plan,
        validation_errors=last_validation_errors,
        validation_warnings=last_validation_warnings,
        questions=questions,
        compilation_report=last_report,
        llm_meta=last_llm_meta,
        planner_confidence=last_confidence,
        planner_notes=last_notes,
        preflight=last_preflight,
    )


async def repair_pipeline_plan(
    *,
    plan: PipelinePlan,
    validation_errors: List[str],
    validation_warnings: List[str],
    preflight: Optional[Dict[str, Any]],
    preview: Optional[Dict[str, Any]],
    context_pack: Optional[Dict[str, Any]],
    actor: str,
    tenant_id: str,
    user_id: Optional[str],
    data_policies: Optional[Dict[str, Any]],
    selected_model: Optional[str],
    allowed_models: Optional[List[str]],
    llm_gateway: LLMGateway,
    redis_service: Optional[RedisService],
    audit_store: Optional[AuditLogStore],
    dataset_registry: DatasetRegistry,
) -> PipelinePlanCompileResult:
    plan_id = str(plan.plan_id or uuid4())
    system_prompt = _build_repair_system_prompt()
    user_prompt = _build_repair_user_prompt(
        plan=plan,
        validation_errors=validation_errors,
        validation_warnings=validation_warnings,
        preflight=preflight,
        preview=preview,
        context_pack=context_pack,
    )
    llm_meta: Optional[LLMCallMeta] = None

    if data_policies and redis_service:
        model_for_quota = str(selected_model or getattr(llm_gateway, "model", "") or "").strip()
        if model_for_quota:
            await enforce_llm_quota(
                redis_service=redis_service,
                tenant_id=tenant_id,
                user_id=user_id,
                model_id=model_for_quota,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                data_policies=data_policies,
            )

    try:
        draft, llm_meta = await llm_gateway.complete_json(
            task="PIPELINE_PLAN_REPAIR_V1",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=PipelinePlanDraftEnvelope,
            model=selected_model,
            allowed_models=allowed_models,
            redis_service=redis_service,
            audit_store=audit_store,
            audit_partition_key=f"pipeline_plan:{plan_id}",
            audit_actor=actor,
            audit_resource_id=plan_id,
            audit_metadata={"kind": "pipeline_plan_repair", "schema": "PipelinePlan", "tenant_id": tenant_id, "user_id": user_id},
        )
        draft_plan_obj = dict(draft.plan or {})
    except (LLMUnavailableError, LLMRequestError, LLMOutputValidationError) as exc:
        questions = _fallback_questions_from_errors([str(exc)])
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=[str(exc)],
            validation_warnings=[],
            questions=questions,
            llm_meta=llm_meta,
        )

    base_plan = plan.model_dump(mode="json")
    if draft_plan_obj:
        merged = dict(base_plan)
        for key, value in draft_plan_obj.items():
            if value is not None:
                merged[key] = value
        draft_plan_obj = merged
    else:
        draft_plan_obj = base_plan

    try:
        repaired_plan = PipelinePlan.model_validate(draft_plan_obj)
    except ValidationError as exc:
        errors = [str(e.get("msg") or e) for e in exc.errors()]
        questions = _fallback_questions_from_errors(errors)
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=errors,
            validation_warnings=[],
            questions=questions,
            llm_meta=llm_meta,
            planner_confidence=float(draft.confidence) if draft is not None else None,
            planner_notes=draft.notes if draft is not None else None,
        )

    repaired_plan = repaired_plan.model_copy(
        update={
            "plan_id": repaired_plan.plan_id or plan_id,
            "created_by": repaired_plan.created_by or actor,
            "data_scope": plan.data_scope,
        }
    )
    if not repaired_plan.associations and plan.associations:
        repaired_plan = repaired_plan.model_copy(update={"associations": list(plan.associations)})

    scope_db = str(repaired_plan.data_scope.db_name or "").strip()
    scope_branch = str(repaired_plan.data_scope.branch or "").strip() or None
    validation = await validate_pipeline_plan(
        plan=repaired_plan,
        dataset_registry=dataset_registry,
        db_name=scope_db,
        branch=scope_branch,
        require_output=True,
        context_pack=context_pack,
    )

    if validation.errors:
        questions = _fallback_questions_from_errors(validation.errors)
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=validation.plan,
            validation_errors=validation.errors,
            validation_warnings=list(validation.warnings or []),
            questions=questions,
            compilation_report=validation.compilation_report,
            llm_meta=llm_meta,
            planner_confidence=float(draft.confidence) if draft is not None else None,
            planner_notes=draft.notes if draft is not None else None,
            preflight=validation.preflight,
        )

    return PipelinePlanCompileResult(
        status="success",
        plan_id=plan_id,
        plan=validation.plan,
        validation_errors=[],
        validation_warnings=list(validation.warnings or []),
        questions=[],
        compilation_report=validation.compilation_report,
        llm_meta=llm_meta,
        planner_confidence=float(draft.confidence) if draft is not None else None,
        planner_notes=draft.notes if draft is not None else None,
        preflight=validation.preflight,
    )


def _extract_input_nodes_for_prompt(
    *,
    plan_obj: Dict[str, Any],
    context_pack: Optional[Dict[str, Any]],
    max_columns: int = 25,
) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    if isinstance(context_pack, dict):
        raw = context_pack.get("selected_datasets")
        if isinstance(raw, list):
            selected = [item for item in raw if isinstance(item, dict)]
    by_id = {
        str(item.get("dataset_id") or "").strip(): item
        for item in selected
        if str(item.get("dataset_id") or "").strip()
    }
    by_name = {
        str(item.get("name") or "").strip().lower(): item
        for item in selected
        if str(item.get("name") or "").strip()
    }

    definition = plan_obj.get("definition_json") if isinstance(plan_obj.get("definition_json"), dict) else {}
    nodes = definition.get("nodes") if isinstance(definition.get("nodes"), list) else []
    input_nodes: List[Dict[str, Any]] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        if str(node.get("type") or "").strip().lower() != "input":
            continue
        node_id = str(node.get("id") or "").strip()
        metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
        dataset_id = str(metadata.get("datasetId") or metadata.get("dataset_id") or "").strip() or None
        dataset_name = str(metadata.get("datasetName") or metadata.get("dataset_name") or "").strip() or None
        branch = str(metadata.get("datasetBranch") or metadata.get("dataset_branch") or "").strip() or None
        ds_obj = by_id.get(dataset_id or "") if dataset_id else None
        if ds_obj is None and dataset_name:
            ds_obj = by_name.get(dataset_name.lower())
        columns = [
            str(col.get("name") or "").strip()
            for col in (ds_obj.get("columns") or [])  # type: ignore[union-attr]
            if isinstance(col, dict) and str(col.get("name") or "").strip()
        ] if isinstance(ds_obj, dict) else []
        input_nodes.append(
            {
                "node_id": node_id,
                "dataset_id": dataset_id,
                "dataset_name": dataset_name,
                "dataset_branch": branch,
                "columns": columns[: max(0, int(max_columns))],
            }
        )
    return input_nodes


async def repair_pipeline_plan_mcp(
    *,
    plan: PipelinePlan,
    validation_errors: List[str],
    validation_warnings: List[str],
    preflight: Optional[Dict[str, Any]],
    preview: Optional[Dict[str, Any]],
    context_pack: Optional[Dict[str, Any]],
    actor: str,
    tenant_id: str,
    user_id: Optional[str],
    data_policies: Optional[Dict[str, Any]],
    selected_model: Optional[str],
    allowed_models: Optional[List[str]],
    llm_gateway: LLMGateway,
    redis_service: Optional[RedisService],
    audit_store: Optional[AuditLogStore],
    dataset_registry: DatasetRegistry,
) -> PipelinePlanCompileResult:
    """
    MCP-based repair loop for an existing PipelinePlan.

    The LLM emits plan-builder patch tool calls (no definition_json).
    """
    plan_id = str(plan.plan_id or uuid4())

    # Lazy import to keep MCP optional in environments where the SDK/servers are not installed.
    try:
        try:
            from mcp.mcp_client import get_mcp_manager  # type: ignore[import-not-found]
        except Exception:  # pragma: no cover
            from backend.mcp.mcp_client import get_mcp_manager  # type: ignore[import-not-found]
    except Exception as exc:
        questions = _fallback_questions_from_errors([f"MCP client unavailable: {exc}"])
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=[f"MCP client unavailable: {exc}"],
            validation_warnings=[],
            questions=questions,
        )

    mcp_manager = get_mcp_manager()

    async def _call_pipeline_tool(tool: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        payload = await mcp_manager.call_tool("pipeline", tool, arguments)
        if isinstance(payload, dict):
            return payload
        data = getattr(payload, "data", None)
        if isinstance(data, dict):
            return data
        raise RuntimeError(f"Unexpected MCP tool result type: {type(payload)}")

    task_spec_model = getattr(plan, "task_spec", None)
    if task_spec_model is not None:
        dataset_count = len(list(plan.data_scope.dataset_ids or [])) if plan.data_scope else 0
        task_spec_model = clamp_task_spec(spec=task_spec_model, dataset_count=dataset_count)
        plan = plan.model_copy(update={"task_spec": task_spec_model})

        scope_value = str(getattr(task_spec_model.scope, "value", task_spec_model.scope) or "").strip().lower()
        if scope_value == "report_only":
            questions = _fallback_questions_from_errors(
                ["task_spec.scope=report_only forbids pipeline plan repair"]
            )
            return PipelinePlanCompileResult(
                status="clarification_required",
                plan_id=plan_id,
                plan=plan,
                validation_errors=["task_spec.scope=report_only forbids pipeline plan repair"],
                validation_warnings=[],
                questions=questions,
            )

    base_obj = plan.model_dump(mode="json")
    input_nodes = _extract_input_nodes_for_prompt(plan_obj=base_obj, context_pack=context_pack)

    scope_db = str(plan.data_scope.db_name or "").strip()
    scope_branch = str(plan.data_scope.branch or "").strip() or None
    if not scope_db:
        questions = _fallback_questions_from_errors(["plan.data_scope.db_name is required"])
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=plan,
            validation_errors=["plan.data_scope.db_name is required"],
            validation_warnings=[],
            questions=questions,
        )

    last_plan: Optional[PipelinePlan] = plan
    last_plan_obj: Dict[str, Any] = base_obj
    last_errors: List[str] = list(validation_errors or [])
    last_warnings: List[str] = list(validation_warnings or [])
    last_llm_meta: Optional[LLMCallMeta] = None
    last_confidence: Optional[float] = None
    last_notes: Optional[List[str]] = None
    last_report: Optional[PlanCompilationReport] = None
    last_preflight: Optional[Dict[str, Any]] = preflight

    allowed_tools = _allowed_mcp_tools_for_task(task_spec=task_spec_model, mode="repair")
    allowed_tool_set = set(allowed_tools)

    for attempt in range(2):
        system_prompt = _build_mcp_script_system_prompt(mode="repair", allowed_tools=allowed_tools)
        user_prompt = _build_mcp_script_user_prompt(
            goal=str(plan.goal or ""),
            data_scope=plan.data_scope,
            answers=None,
            context_pack=context_pack,
            planner_hints={"repair": True},
            input_nodes=input_nodes,
            current_plan=last_plan_obj,
            preflight=last_preflight,
            preview=preview if isinstance(preview, dict) else None,
            compilation_report=last_report.model_dump(mode="json") if last_report else None,
            prior_errors=last_errors,
            prior_warnings=last_warnings,
        )

        if data_policies and redis_service:
            model_for_quota = str(selected_model or getattr(llm_gateway, "model", "") or "").strip()
            if model_for_quota:
                await enforce_llm_quota(
                    redis_service=redis_service,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    model_id=model_for_quota,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    data_policies=data_policies,
                )

        try:
            draft, last_llm_meta = await llm_gateway.complete_json(
                task="PIPELINE_PLAN_MCP_REPAIR_V1",
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=PipelinePlanBuilderScriptEnvelope,
                model=selected_model,
                allowed_models=allowed_models,
                redis_service=redis_service,
                audit_store=audit_store,
                audit_partition_key=f"pipeline_plan:{plan_id}",
                audit_actor=actor,
                audit_resource_id=plan_id,
                audit_metadata={
                    "kind": "pipeline_plan_mcp_repair",
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "attempt": attempt + 1,
                },
            )
        except (LLMUnavailableError, LLMRequestError, LLMOutputValidationError) as exc:
            questions = _fallback_questions_from_errors([str(exc)])
            return PipelinePlanCompileResult(
                status="clarification_required",
                plan_id=plan_id,
                plan=last_plan,
                validation_errors=[str(exc)],
                validation_warnings=[],
                questions=questions,
                llm_meta=last_llm_meta,
            )

        last_confidence = float(draft.confidence) if draft is not None else None
        last_notes = list(draft.notes or []) if draft is not None else None

        if draft.questions:
            return PipelinePlanCompileResult(
                status="clarification_required",
                plan_id=plan_id,
                plan=last_plan,
                validation_errors=["planner requested clarification"],
                validation_warnings=[],
                questions=list(draft.questions or []),
                llm_meta=last_llm_meta,
                planner_confidence=last_confidence,
                planner_notes=last_notes,
                preflight=last_preflight,
            )

        plan_obj = dict(last_plan_obj)
        tool_errors: List[str] = []
        tool_warnings: List[str] = list(draft.warnings or [])
        steps = list(draft.steps or [])
        if not steps:
            tool_errors.append("planner returned no steps")

        for step in steps:
            tool_name = str(step.tool or "").strip()
            if tool_name not in allowed_tool_set:
                tool_errors.append(f"unsupported tool: {tool_name}")
                continue
            args = dict(step.args or {})
            policy_error = _validate_mcp_step_against_task_spec(
                tool_name=tool_name,
                args=args,
                task_spec=task_spec_model,
                mode="repair",
            )
            if policy_error:
                tool_errors.append(f"{tool_name}: {policy_error}")
                continue
            args["plan"] = plan_obj
            res = await _call_pipeline_tool(tool_name, args)
            if isinstance(res, dict) and res.get("error"):
                tool_errors.append(f"{tool_name}: {res.get('error')}")
                continue
            if isinstance(res, dict) and isinstance(res.get("warnings"), list):
                tool_warnings.extend([str(item) for item in res.get("warnings") or [] if str(item or "").strip()])
            if isinstance(res, dict) and isinstance(res.get("plan"), dict):
                plan_obj = res["plan"]

        if tool_errors:
            last_errors = tool_errors
            last_warnings = tool_warnings
            last_plan_obj = plan_obj
            continue

        try:
            repaired = PipelinePlan.model_validate(plan_obj)
        except ValidationError as exc:
            last_errors = [str(e.get("msg") or e) for e in exc.errors()]
            last_warnings = tool_warnings
            last_plan_obj = plan_obj
            continue

        repaired = repaired.model_copy(
            update={
                "plan_id": repaired.plan_id or plan_id,
                "created_by": repaired.created_by or actor,
                "data_scope": plan.data_scope,
                "task_spec": task_spec_model,
            }
        )
        if not repaired.associations and plan.associations:
            repaired = repaired.model_copy(update={"associations": list(plan.associations)})

        validation = await validate_pipeline_plan(
            plan=repaired,
            dataset_registry=dataset_registry,
            db_name=scope_db,
            branch=scope_branch,
            require_output=True,
            context_pack=context_pack,
        )

        last_plan = validation.plan
        last_plan_obj = validation.plan.model_dump(mode="json")
        last_preflight = validation.preflight
        last_report = validation.compilation_report
        last_errors = list(validation.errors or [])
        last_warnings = list(tool_warnings) + list(validation.warnings or [])

        if not validation.errors:
            return PipelinePlanCompileResult(
                status="success",
                plan_id=plan_id,
                plan=validation.plan,
                validation_errors=[],
                validation_warnings=last_warnings,
                questions=[],
                compilation_report=validation.compilation_report,
                llm_meta=last_llm_meta,
                planner_confidence=last_confidence,
                planner_notes=last_notes,
                preflight=validation.preflight,
            )

    # Clarifier fallback.
    questions: List[PipelineClarificationQuestion] = []
    try:
        clarifier_system_prompt = _build_clarifier_system_prompt()
        clarifier_user_prompt = _build_clarifier_user_prompt(
            goal=str(plan.goal or ""),
            draft_plan=last_plan.model_dump(mode="json") if last_plan else last_plan_obj,
            validation_errors=last_errors,
            validation_warnings=last_warnings,
            data_scope=plan.data_scope,
            context_pack=context_pack,
        )
        if data_policies and redis_service:
            model_for_quota = str(selected_model or getattr(llm_gateway, "model", "") or "").strip()
            if model_for_quota:
                await enforce_llm_quota(
                    redis_service=redis_service,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    model_id=model_for_quota,
                    system_prompt=clarifier_system_prompt,
                    user_prompt=clarifier_user_prompt,
                    data_policies=data_policies,
                )
        clarification, clarification_meta = await llm_gateway.complete_json(
            task="PIPELINE_PLAN_CLARIFY_V1",
            system_prompt=clarifier_system_prompt,
            user_prompt=clarifier_user_prompt,
            response_model=PipelineClarificationPayload,
            model=selected_model,
            allowed_models=allowed_models,
            redis_service=redis_service,
            audit_store=audit_store,
            audit_partition_key=f"pipeline_plan:{plan_id}",
            audit_actor=actor,
            audit_resource_id=plan_id,
            audit_metadata={"kind": "pipeline_plan_clarify", "tenant_id": tenant_id, "user_id": user_id},
        )
        questions = list(clarification.questions or [])
        last_llm_meta = clarification_meta or last_llm_meta
    except Exception:
        questions = _fallback_questions_from_errors(last_errors)

    return PipelinePlanCompileResult(
        status="clarification_required",
        plan_id=plan_id,
        plan=last_plan,
        validation_errors=last_errors,
        validation_warnings=last_warnings,
        questions=questions,
        compilation_report=last_report,
        llm_meta=last_llm_meta,
        planner_confidence=last_confidence,
        planner_notes=last_notes,
        preflight=last_preflight,
    )
