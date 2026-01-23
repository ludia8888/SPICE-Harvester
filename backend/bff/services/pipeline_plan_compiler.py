"""
Pipeline plan compiler (LLM-native pipeline definition planner).

Generates typed PipelinePlan artifacts and validates them before preview.
"""

from __future__ import annotations

import json
import re
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
from shared.services.pipeline_plan_builder import add_input, add_join, add_output, add_select, new_plan
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
        "- If planner_hints.join_plan is provided, use ONLY those join keys (do not invent joins/keys).\n"
        "- Prefer plan_add_join for joins. Use plan_add_transform only for advanced ops (groupBy/aggregate/window/pivot/union/sort/explode).\n"
        "- If planner_hints.key_inference provides pk/fk candidates, use them to pick stable join directions and output pkColumns.\n"
        "- If planner_hints.type_inference.join_key_cast_suggestions is provided, add ONLY the minimal casts needed for join keys.\n"
        "- When task_spec.allow_specs is false, output_kind MUST be \"unknown\" (do not emit object/link outputs).\n"
        "- Only set output_kind to object/link when the goal explicitly requests ontology/canonical mapping AND task_spec.allow_specs is true.\n"
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


def _extract_explicit_output_columns_from_goal(goal: str) -> List[str]:
    """
    Best-effort extraction of an explicit output column list from the user's goal.

    We intentionally keep this aligned with the intent verifier heuristics:
    it only triggers on patterns like "... col1, col2 columns" / "... col1, col2 컬럼".
    """
    text = str(goal or "")
    matches = list(
        re.finditer(
            r"(?P<cols>(?:[A-Za-z_][A-Za-z0-9_]*\s*,\s*)*[A-Za-z_][A-Za-z0-9_]*)\s*(?:컬럼|columns?)",
            text,
            flags=re.IGNORECASE,
        )
    )
    if not matches:
        return []
    segment = matches[-1].group("cols") or ""
    cols = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", segment)
    seen: set[str] = set()
    out: List[str] = []
    for c in cols:
        cl = c.lower()
        if cl in seen:
            continue
        seen.add(cl)
        out.append(c)
    return out


def _extract_explicit_output_name_from_goal(goal: str) -> Optional[str]:
    """
    Best-effort extraction of an output dataset name from the goal.

    Examples:
    - "... joined_orders 데이터셋 ..." -> "joined_orders"
    - "... joined_orders dataset ..."  -> "joined_orders"
    """
    text = str(goal or "")
    matches = list(
        re.finditer(
            r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*(?:데이터셋|dataset)",
            text,
            flags=re.IGNORECASE,
        )
    )
    if not matches:
        return None
    name = str(matches[-1].group("name") or "").strip()
    return name or None


def _ensure_plan_has_output(plan_obj: Dict[str, Any], *, goal: str) -> Optional[str]:
    """
    Deterministically ensure the plan has at least one output node + outputs[] entry.

    This protects against LLM omissions (e.g., returning steps but forgetting plan_add_output),
    so the agent can continue without user-facing clarification loops for basic requests.
    """
    if not isinstance(plan_obj, dict):
        return None

    definition = plan_obj.get("definition_json")
    if not isinstance(definition, dict):
        return None
    nodes = definition.get("nodes")
    edges = definition.get("edges")
    if not isinstance(nodes, list) or not isinstance(edges, list):
        return None

    has_output_node = any(
        isinstance(node, dict) and str(node.get("type") or "").strip().lower() == "output" for node in nodes
    )
    outputs = plan_obj.get("outputs")
    has_outputs_entry = isinstance(outputs, list) and bool(outputs)
    if has_output_node and has_outputs_entry:
        return None

    # If we already have an output node, prefer keeping it and only backfill outputs[].
    if has_output_node and not has_outputs_entry:
        output_name = None
        for node in nodes:
            if not isinstance(node, dict):
                continue
            if str(node.get("type") or "").strip().lower() != "output":
                continue
            meta = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
            output_name = str(meta.get("outputName") or meta.get("datasetName") or node.get("id") or "").strip() or None
            if output_name:
                break
        if output_name:
            plan_obj["outputs"] = [{"output_name": output_name, "output_kind": "unknown"}]
            return "compiler: backfilled outputs[] from existing output node"
        return None

    if has_outputs_entry and not has_output_node:
        # validate_pipeline_plan will create output nodes from outputs[], so keep this path minimal.
        return None

    # No outputs and no output node: add one.
    output_name = _extract_explicit_output_name_from_goal(goal) or "output"

    # Pick a sink node as the output input (prefer the terminal transform node).
    node_by_id: Dict[str, Dict[str, Any]] = {}
    node_ids: List[str] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_id = str(node.get("id") or "").strip()
        if not node_id:
            continue
        node_by_id[node_id] = node
        node_ids.append(node_id)
    if not node_ids:
        return None
    outgoing = {str(edge.get("from") or "").strip() for edge in edges if isinstance(edge, dict)}
    sink_ids = [
        node_id
        for node_id in node_ids
        if node_id
        and node_id not in outgoing
        and str(node_by_id.get(node_id, {}).get("type") or "").strip().lower() != "output"
    ]
    attach_to = sink_ids[0] if sink_ids else node_ids[-1]

    try:
        add_output(plan_obj, input_node_id=attach_to, output_name=output_name, output_kind="unknown")
    except Exception:
        return None
    return "compiler: injected missing output node"


def _dedupe_outputs(plan_obj: Dict[str, Any], *, goal: str) -> Optional[str]:
    """
    Deterministically dedupe outputs[] + output nodes by output_name/outputName.

    LLM planners sometimes emit duplicate plan_add_output calls; downstream intent verification and
    execution become ambiguous when multiple output nodes share the same outputName.
    """
    if not isinstance(plan_obj, dict):
        return None
    definition = plan_obj.get("definition_json")
    if not isinstance(definition, dict):
        return None
    nodes = definition.get("nodes")
    edges = definition.get("edges")
    if not isinstance(nodes, list) or not isinstance(edges, list):
        return None

    changed = False

    outputs_raw = plan_obj.get("outputs")
    if isinstance(outputs_raw, list) and outputs_raw:
        seen_names: set[str] = set()
        deduped_outputs: List[Dict[str, Any]] = []
        for item in outputs_raw:
            if not isinstance(item, dict):
                continue
            name = str(item.get("output_name") or "").strip()
            if not name:
                continue
            lowered = name.lower()
            if lowered in seen_names:
                changed = True
                continue
            seen_names.add(lowered)
            deduped_outputs.append(item)
        if deduped_outputs and len(deduped_outputs) != len(outputs_raw):
            plan_obj["outputs"] = deduped_outputs

    required_cols = _extract_explicit_output_columns_from_goal(goal)

    # Collect output nodes grouped by outputName.
    output_nodes_by_name: Dict[str, List[str]] = {}
    node_by_id: Dict[str, Dict[str, Any]] = {}
    node_order: List[str] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_id = str(node.get("id") or "").strip()
        if not node_id:
            continue
        node_by_id[node_id] = node
        node_order.append(node_id)
        if str(node.get("type") or "").strip().lower() != "output":
            continue
        meta = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
        name = str(meta.get("outputName") or "").strip()
        if not name:
            continue
        output_nodes_by_name.setdefault(name, []).append(node_id)

    def _incoming_parent(node_id: str) -> Optional[str]:
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            if str(edge.get("to") or "").strip() != node_id:
                continue
            parent = str(edge.get("from") or "").strip()
            return parent or None
        return None

    def _score_output_node(node_id: str) -> tuple[int, int]:
        """
        Prefer output nodes wired to a select node matching required_cols.
        Secondary tiebreaker: later node order.
        """
        parent_id = _incoming_parent(node_id) or ""
        parent = node_by_id.get(parent_id, {})
        meta = parent.get("metadata") if isinstance(parent.get("metadata"), dict) else {}
        op = str(meta.get("operation") or parent.get("type") or "").strip().lower()
        cols = meta.get("columns")
        colset = {str(c).strip().lower() for c in cols} if isinstance(cols, list) else set()

        score = 0
        if parent_id:
            score += 1
        if op in {"select", "project"}:
            score += 5
            if required_cols and all(str(c).strip().lower() in colset for c in required_cols):
                score += 10
        # Prefer later nodes if scores tie.
        order_idx = node_order.index(node_id) if node_id in node_order else -1
        return score, order_idx

    # Remove duplicate output nodes per outputName.
    to_delete: set[str] = set()
    for name, node_ids in output_nodes_by_name.items():
        if len(node_ids) <= 1:
            continue
        keep = max(node_ids, key=_score_output_node)
        for node_id in node_ids:
            if node_id != keep:
                to_delete.add(node_id)
        changed = True

    if to_delete:
        next_nodes = [
            node
            for node in nodes
            if not (isinstance(node, dict) and str(node.get("id") or "").strip() in to_delete)
        ]
        next_edges = [
            edge
            for edge in edges
            if not (
                isinstance(edge, dict)
                and (str(edge.get("from") or "").strip() in to_delete or str(edge.get("to") or "").strip() in to_delete)
            )
        ]
        definition["nodes"] = next_nodes
        definition["edges"] = next_edges
        plan_obj["definition_json"] = definition

    return "compiler: deduped duplicate output nodes/entries" if changed else None


def _ensure_output_projection_for_explicit_goal_columns(plan_obj: Dict[str, Any], *, goal: str) -> Optional[str]:
    """
    If the goal explicitly lists output columns, ensure a select node directly feeds the (single) output node.

    This is a deterministic hardening step so "simple joins" don't require transform/repair loops just to
    make the output shape explicit for intent verification.
    """
    required_cols = _extract_explicit_output_columns_from_goal(goal)
    if not required_cols:
        return None

    if not isinstance(plan_obj, dict):
        return None
    definition = plan_obj.get("definition_json")
    if not isinstance(definition, dict):
        return None
    nodes = definition.get("nodes")
    edges = definition.get("edges")
    if not isinstance(nodes, list) or not isinstance(edges, list):
        return None

    output_nodes = [
        node
        for node in nodes
        if isinstance(node, dict) and str(node.get("type") or "").strip().lower() == "output"
    ]
    if len(output_nodes) != 1:
        return None

    output_id = str(output_nodes[0].get("id") or "").strip()
    if not output_id:
        return None

    incoming_edges = [
        edge for edge in edges if isinstance(edge, dict) and str(edge.get("to") or "").strip() == output_id
    ]
    if len(incoming_edges) != 1:
        return None

    upstream_id = str(incoming_edges[0].get("from") or "").strip()
    if not upstream_id:
        return None

    node_by_id: Dict[str, Dict[str, Any]] = {
        str(node.get("id") or "").strip(): node
        for node in nodes
        if isinstance(node, dict) and str(node.get("id") or "").strip()
    }
    upstream_node = node_by_id.get(upstream_id)
    if isinstance(upstream_node, dict) and str(upstream_node.get("type") or "").strip().lower() == "transform":
        meta = upstream_node.get("metadata") if isinstance(upstream_node.get("metadata"), dict) else {}
        op = str(meta.get("operation") or "").strip().lower()
        if op in {"select", "project"}:
            next_meta = dict(meta)
            next_meta["operation"] = op
            next_meta["columns"] = list(required_cols)
            upstream_node["metadata"] = next_meta
            return "compiler: updated select columns to match explicit goal output columns"

    try:
        mutation = add_select(plan_obj, input_node_id=upstream_id, columns=list(required_cols))
    except Exception:
        return None

    select_node_id = str(mutation.node_id or "").strip()
    if not select_node_id:
        return None

    # Rewire output to come from the select node.
    next_edges: List[Dict[str, Any]] = []
    seen_edges: set[tuple[str, str]] = set()
    for edge in (definition.get("edges") or []):
        if not isinstance(edge, dict):
            continue
        src = str(edge.get("from") or "").strip()
        dst = str(edge.get("to") or "").strip()
        if not src or not dst:
            continue
        if dst == output_id:
            continue
        key = (src, dst)
        if key in seen_edges:
            continue
        seen_edges.add(key)
        next_edges.append({"from": src, "to": dst})

    key = (select_node_id, output_id)
    if key not in seen_edges:
        next_edges.append({"from": select_node_id, "to": output_id})
    definition["edges"] = next_edges
    plan_obj["definition_json"] = definition
    return "compiler: injected select to match explicit goal output columns"


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

    async def _call_pipeline_tool(tool: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        payload = await mcp_manager.call_tool("pipeline", tool, arguments)
        if isinstance(payload, dict):
            return payload
        structured = getattr(payload, "structuredContent", None)
        if isinstance(structured, dict):
            return structured
        structured = getattr(payload, "structured_content", None)
        if isinstance(structured, dict):
            return structured
        # Defensive fallback: some MCP client implementations return a wrapper object.
        data = getattr(payload, "data", None)
        if isinstance(data, dict):
            return data
        is_error = bool(getattr(payload, "isError", False) or getattr(payload, "is_error", False))
        content = getattr(payload, "content", None)
        if isinstance(content, list) and content:
            if is_error:
                texts: List[str] = []
                for part in content:
                    text = getattr(part, "text", None)
                    if text is None and isinstance(part, dict):
                        text = part.get("text")
                    if isinstance(text, str) and text.strip():
                        texts.append(text.strip())
                return {"error": "\n".join(texts).strip() or f"MCP tool error: {tool}"}
            for part in content:
                text = getattr(part, "text", None)
                if text is None and isinstance(part, dict):
                    text = part.get("text")
                if isinstance(text, str) and text.strip():
                    try:
                        parsed = json.loads(text)
                    except Exception:
                        continue
                    if isinstance(parsed, dict):
                        return parsed
            # Best-effort: return raw text so callers can surface it as an error instead of 500ing.
            if is_error:
                first_text = getattr(content[0], "text", None) if content else None
                if isinstance(first_text, str) and first_text.strip():
                    return {"error": first_text.strip()}
        raise RuntimeError(f"Unexpected MCP tool result type for {tool}: {type(payload)}")

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

    def _trim_type_inference(payload: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return None
        datasets = payload.get("datasets")
        join_casts = payload.get("join_key_cast_suggestions")
        trimmed: Dict[str, Any] = {
            "join_key_cast_suggestions": join_casts if isinstance(join_casts, list) else [],
        }
        if not isinstance(datasets, list):
            return trimmed
        summarized: List[Dict[str, Any]] = []
        for item in datasets[:12]:
            if not isinstance(item, dict):
                continue
            cols = item.get("columns") if isinstance(item.get("columns"), list) else []
            mismatches = [col for col in cols if isinstance(col, dict) and col.get("mismatch")]
            summarized.append(
                {
                    "dataset_id": item.get("dataset_id"),
                    "name": item.get("name"),
                    "mismatched_columns": mismatches[:20],
                }
            )
        trimmed["datasets"] = summarized
        return trimmed

    # Deterministic analysis helpers (PK/FK + type hints) to reduce planner guessing.
    effective_planner_hints = dict(planner_hints or {})
    if isinstance(context_pack, dict):
        # If join_plan is missing (e.g., join-key selection skipped/failed), infer a best-effort join plan
        # from sample-safe context pack candidates so the planner has a concrete candidate space.
        try:
            allow_join = True if task_spec_model is None else bool(task_spec_model.allow_join)
            if allow_join and len(dataset_ids) > 1 and "join_plan" not in effective_planner_hints:
                join_plan_payload = await _call_pipeline_tool(
                    "context_pack_infer_join_plan",
                    {
                        "context_pack": context_pack,
                        "dataset_ids": dataset_ids or None,
                        "max_joins": min(12, max(4, len(dataset_ids) - 1)) if len(dataset_ids) > 1 else 4,
                        "max_edges": 30,
                    },
                )
                join_inf = join_plan_payload.get("inference") if isinstance(join_plan_payload, dict) else None
                if isinstance(join_inf, dict):
                    join_plan = join_inf.get("join_plan")
                    if isinstance(join_plan, list) and join_plan:
                        effective_planner_hints["join_plan"] = join_plan
        except Exception:
            pass

        if "key_inference" not in effective_planner_hints:
            try:
                keys_payload = await _call_pipeline_tool(
                    "context_pack_infer_keys",
                    {
                        "context_pack": context_pack,
                        "max_pk_candidates": 4,
                        "max_fk_candidates": 20,
                    },
                )
                keys_inf = keys_payload.get("inference") if isinstance(keys_payload, dict) else None
                if isinstance(keys_inf, dict):
                    effective_planner_hints["key_inference"] = keys_inf
            except Exception:
                pass
        if "type_inference" not in effective_planner_hints:
            try:
                join_plan_hint = effective_planner_hints.get("join_plan")
                type_args: Dict[str, Any] = {
                    "context_pack": context_pack,
                    "max_columns": 60,
                    "max_samples": 80,
                }
                if isinstance(join_plan_hint, list):
                    type_args["join_plan"] = join_plan_hint
                types_payload = await _call_pipeline_tool("context_pack_infer_types", type_args)
                types_inf = types_payload.get("inference") if isinstance(types_payload, dict) else None
                trimmed = _trim_type_inference(types_inf)
                if trimmed is not None:
                    effective_planner_hints["type_inference"] = trimmed
            except Exception:
                pass

    def _dataset_info(dataset_id: str) -> tuple[Optional[str], Optional[str], Dict[str, Any]]:
        item = selected_by_id.get(str(dataset_id or "").strip(), None)
        if not isinstance(item, dict):
            return None, None, {}
        name = str(item.get("name") or "").strip() or None
        branch = str(item.get("branch") or "").strip() or None
        return name, branch, item

    # Deterministic fast-path: for simple "integrate via join" tasks with an explicit join_plan hint,
    # build the plan without relying on the LLM to pick/track node ids correctly.
    try:
        allow_join = True if task_spec_model is None else bool(task_spec_model.allow_join)
        allow_advanced = True if task_spec_model is None else bool(task_spec_model.allow_advanced_transforms)
        join_plan_hint = effective_planner_hints.get("join_plan") if isinstance(effective_planner_hints, dict) else None
        if allow_join and not allow_advanced and isinstance(join_plan_hint, list) and len(dataset_ids) == 2 and join_plan_hint:
            selected = {str(item).strip() for item in dataset_ids if str(item).strip()}

            chosen_edge: Optional[Dict[str, Any]] = None
            for edge in join_plan_hint:
                if not isinstance(edge, dict):
                    continue
                left_id = str(edge.get("left_dataset_id") or "").strip()
                right_id = str(edge.get("right_dataset_id") or "").strip()
                if left_id and right_id and {left_id, right_id} == selected:
                    chosen_edge = edge
                    break

            if chosen_edge is not None:
                plan_obj = new_plan(goal=goal, db_name=scope_db, branch=scope_branch, dataset_ids=dataset_ids)
                dataset_to_node: Dict[str, str] = {}
                for idx, dataset_id in enumerate(dataset_ids):
                    dataset_name, dataset_branch, _dataset_obj = _dataset_info(dataset_id)
                    node_id = f"input_{idx + 1}"
                    add_input(
                        plan_obj,
                        dataset_id=dataset_id,
                        dataset_name=dataset_name,
                        dataset_branch=dataset_branch or scope_branch,
                        node_id=node_id,
                    )
                    dataset_to_node[str(dataset_id).strip()] = node_id

                left_id = str(chosen_edge.get("left_dataset_id") or "").strip()
                right_id = str(chosen_edge.get("right_dataset_id") or "").strip()
                left_node_id = dataset_to_node.get(left_id)
                right_node_id = dataset_to_node.get(right_id)

                left_keys = chosen_edge.get("left_keys") if isinstance(chosen_edge.get("left_keys"), list) else None
                right_keys = chosen_edge.get("right_keys") if isinstance(chosen_edge.get("right_keys"), list) else None
                if not left_keys:
                    left_col = str(chosen_edge.get("left_column") or "").strip()
                    left_keys = [left_col] if left_col else None
                if not right_keys:
                    right_col = str(chosen_edge.get("right_column") or "").strip()
                    right_keys = [right_col] if right_col else None

                join_type = str(chosen_edge.get("join_type") or "inner").strip().lower() or "inner"
                if left_node_id and right_node_id and left_keys and right_keys and len(left_keys) == len(right_keys):
                    add_join(
                        plan_obj,
                        left_node_id=left_node_id,
                        right_node_id=right_node_id,
                        left_keys=[str(k).strip() for k in left_keys if str(k).strip()],
                        right_keys=[str(k).strip() for k in right_keys if str(k).strip()],
                        join_type=join_type,
                        node_id="join",
                    )

                    output_input_id = "join"
                    required_cols = _extract_explicit_output_columns_from_goal(goal)
                    if required_cols:
                        add_select(plan_obj, input_node_id="join", columns=list(required_cols), node_id="select_1")
                        output_input_id = "select_1"

                    output_name = _extract_explicit_output_name_from_goal(goal) or "output"
                    add_output(plan_obj, input_node_id=output_input_id, output_name=output_name, output_kind="unknown")

                    plan_model = PipelinePlan.model_validate(plan_obj)
                    plan_model = plan_model.model_copy(
                        update={
                            "plan_id": plan_model.plan_id or plan_id,
                            "created_by": plan_model.created_by or actor,
                            "data_scope": data_scope or plan_model.data_scope,
                            "task_spec": task_spec_model,
                        }
                    )

                    if not plan_model.associations and (task_spec_model is None or bool(task_spec_model.allow_join)):
                        derived = _associations_from_join_plan([chosen_edge])
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
                    if not validation.errors:
                        return PipelinePlanCompileResult(
                            status="success",
                            plan_id=plan_id,
                            plan=validation.plan,
                            validation_errors=[],
                            validation_warnings=["compiler: deterministic join fast-path"] + list(validation.warnings or []),
                            questions=[],
                            compilation_report=validation.compilation_report,
                            llm_meta=None,
                            planner_confidence=1.0,
                            planner_notes=["deterministic join fast-path"],
                            preflight=validation.preflight,
                        )
    except Exception:
        pass

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
        # If we couldn't even validate a plan on the first attempt (tool/schema errors),
        # a second "repair" attempt is usually counterproductive. Re-run build instead.
        mode: Literal["build", "repair"] = "build" if attempt == 0 or last_plan is None else "repair"

        if mode == "build":
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
            planner_hints={**(effective_planner_hints or {}), "task_spec": (task_spec_model.model_dump(mode="json") if task_spec_model else task_spec)} if (task_spec_model or task_spec) else (effective_planner_hints or None),
            input_nodes=input_nodes,
            current_plan=plan_obj if mode == "repair" else None,
            preflight=last_preflight if mode == "repair" else None,
            compilation_report=last_report.model_dump(mode="json") if (mode == "repair" and last_report) else None,
            prior_errors=last_validation_errors if attempt > 0 else None,
            prior_warnings=last_validation_warnings if attempt > 0 else None,
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
                task="PIPELINE_PLAN_MCP_SCRIPT_V1" if mode == "build" else "PIPELINE_PLAN_MCP_SCRIPT_REPAIR_V1",
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

            # Guardrail: if specs/mapping are not allowed, never let the planner create ontology-backed outputs.
            if task_spec_model is not None and not bool(task_spec_model.allow_specs):
                if tool_name == "plan_add_output":
                    kind = str(args.get("output_kind") or "").strip().lower()
                    if kind and kind != "unknown":
                        args["output_kind"] = "unknown"
                        meta = args.get("output_metadata")
                        if isinstance(meta, dict):
                            for key in (
                                "target_class_id",
                                "source_class_id",
                                "link_type_id",
                                "predicate",
                                "cardinality",
                                "source_key_column",
                                "target_key_column",
                                "relationship_spec_type",
                            ):
                                meta.pop(key, None)
                            args["output_metadata"] = meta
                if tool_name == "plan_update_output":
                    set_fields = args.get("set")
                    if isinstance(set_fields, dict):
                        kind = str(set_fields.get("output_kind") or "").strip().lower()
                        if kind and kind != "unknown":
                            set_fields["output_kind"] = "unknown"
                        for key in (
                            "target_class_id",
                            "source_class_id",
                            "link_type_id",
                            "predicate",
                            "cardinality",
                            "source_key_column",
                            "target_key_column",
                            "relationship_spec_type",
                        ):
                            set_fields.pop(key, None)
                        args["set"] = set_fields

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
        if isinstance(plan_obj, dict):
            output_warning = _ensure_plan_has_output(plan_obj, goal=goal)
            if output_warning:
                tool_warnings.append(output_warning)
            dedupe_warning = _dedupe_outputs(plan_obj, goal=goal)
            if dedupe_warning:
                tool_warnings.append(dedupe_warning)
            projection_warning = _ensure_output_projection_for_explicit_goal_columns(plan_obj, goal=goal)
            if projection_warning:
                tool_warnings.append(projection_warning)
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
        structured = getattr(payload, "structuredContent", None)
        if isinstance(structured, dict):
            return structured
        structured = getattr(payload, "structured_content", None)
        if isinstance(structured, dict):
            return structured
        data = getattr(payload, "data", None)
        if isinstance(data, dict):
            return data
        is_error = bool(getattr(payload, "isError", False) or getattr(payload, "is_error", False))
        content = getattr(payload, "content", None)
        if isinstance(content, list) and content:
            if is_error:
                texts: List[str] = []
                for part in content:
                    text = getattr(part, "text", None)
                    if text is None and isinstance(part, dict):
                        text = part.get("text")
                    if isinstance(text, str) and text.strip():
                        texts.append(text.strip())
                return {"error": "\n".join(texts).strip() or f"MCP tool error: {tool}"}
            for part in content:
                text = getattr(part, "text", None)
                if text is None and isinstance(part, dict):
                    text = part.get("text")
                if isinstance(text, str) and text.strip():
                    try:
                        parsed = json.loads(text)
                    except Exception:
                        continue
                    if isinstance(parsed, dict):
                        return parsed
            if is_error:
                first_text = getattr(content[0], "text", None) if content else None
                if isinstance(first_text, str) and first_text.strip():
                    return {"error": first_text.strip()}
        raise RuntimeError(f"Unexpected MCP tool result type for {tool}: {type(payload)}")

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
