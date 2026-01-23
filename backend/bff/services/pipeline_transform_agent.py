"""
Pipeline transform agent.

Refines pipeline definition_json using join selections and context pack guidance.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, ValidationError

from bff.services.pipeline_plan_validation import validate_pipeline_plan
from shared.models.pipeline_plan import PipelinePlan, PipelinePlanAssociation
from shared.services.audit_log_store import AuditLogStore
from shared.services.llm_gateway import (
    LLMCallMeta,
    LLMGateway,
    LLMOutputValidationError,
    LLMRequestError,
    LLMUnavailableError,
)
from shared.services.llm_quota import enforce_llm_quota
from shared.services.redis_service import RedisService
from shared.services.pipeline_transform_spec import SUPPORTED_TRANSFORMS

# Reuse the script envelope used by the MCP compiler to avoid duplicating schema.
from bff.services.pipeline_plan_compiler import PipelinePlanBuilderScriptEnvelope
from shared.services.pipeline_task_spec_policy import clamp_task_spec


class PipelineTransformEnvelope(BaseModel):
    definition_json: Dict[str, Any] = Field(default_factory=dict)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    notes: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


@dataclass(frozen=True)
class PipelineTransformResult:
    plan: PipelinePlan
    validation_errors: List[str]
    validation_warnings: List[str]
    confidence: Optional[float]
    notes: List[str]
    warnings: List[str]
    llm_meta: Optional[LLMCallMeta] = None


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


def _build_transform_system_prompt() -> str:
    return (
        "You are a STRICT pipeline transform agent for SPICE-Harvester.\n"
        "Return ONLY JSON with definition_json. No markdown.\n"
        "Keep nodes/edges minimal and deterministic.\n"
        "Do NOT change plan outputs or data_scope.\n"
        "Preserve pkSemantics/pkColumns/expectations unless you are fixing validation errors.\n"
        "Use only supported operations; no UDF.\n"
        "join requires leftKey/rightKey or leftKeys/rightKeys (or joinKey) and allowCrossJoin must be false.\n"
        "For composite joins, ALWAYS use leftKeys/rightKeys arrays.\n"
        "If join candidates include composite_group_id, use that group as a composite join.\n"
        "If associations are provided, you may leave join edges minimal; server auto-wiring will align inputs.\n"
        "You MUST wire non-join transforms with edges (filter/cast/compute/select/drop/rename/groupBy/window).\n"
        "filter nodes MUST use metadata.expression (not condition).\n"
        "Edge order matters for join only when you wire edges: first incoming edge is LEFT, second is RIGHT.\n"
        "Ensure join keys align to the left/right input order if you wire edges.\n"
        "If planner_hints.multi_stage_mode is provided, respect it:\n"
        "- required: include multi-stage chaining if needed by the goal (filter/groupBy/join/compute).\n"
        "- forbid: avoid extra stages beyond joins/cleansing unless the goal explicitly demands it.\n"
        "- If planner_hints.intent_feedback.missing_requirements is non-empty, you MUST address each requirement.\n"
        "- If planner_hints.autonomy_level is \"high\", decide and proceed unless critical info is missing.\n"
        "- If planner_hints.cardinality_strategy is \"prefer_left_on_uncertain\", "
        "use left join when cardinality_confidence is low or cardinality_note warns about uncertainty.\n"
        "- If planner_hints.null_strategy is provided, default to that when choosing filter/dedupe vs keep nulls.\n"
        "- If planner_hints.numeric_columns is provided, choose amount/metric columns from amount_candidates first.\n"
        "- amount_candidates may include {column, score, reasons} and are ranked (highest score first).\n"
        "- If you cannot satisfy a requirement, explain why in warnings.\n"
        "Prefer join/window/aggregate only when needed by the goal; follow join_plan hints.\n"
        "\n"
        f"Supported operations: {', '.join(sorted(SUPPORTED_TRANSFORMS))}\n"
        "\n"
        "Output schema:\n"
        "{\n"
        "  \"definition_json\": {\"nodes\": [], \"edges\": []},\n"
        "  \"confidence\": number (0..1),\n"
        "  \"notes\": string[],\n"
        "  \"warnings\": string[]\n"
        "}\n"
    )


def _build_transform_user_prompt(
    *,
    goal: str,
    data_scope: Dict[str, Any],
    current_definition: Dict[str, Any],
    outputs: List[Dict[str, Any]],
    join_plan: Optional[List[Dict[str, Any]]],
    cleansing_hints: Optional[List[Dict[str, Any]]],
    context_pack: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
    associations: Optional[List[Dict[str, Any]]],
) -> str:
    return (
        f"Goal:\n{goal}\n\n"
        f"Data scope:\n{json.dumps(data_scope or {}, ensure_ascii=False)}\n\n"
        f"Current definition_json:\n{json.dumps(current_definition or {}, ensure_ascii=False)}\n\n"
        f"Outputs:\n{json.dumps(outputs or [], ensure_ascii=False)}\n\n"
        f"Associations:\n{json.dumps(associations or [], ensure_ascii=False)}\n\n"
        f"Join plan:\n{json.dumps(join_plan or [], ensure_ascii=False)}\n\n"
        f"Cleansing hints:\n{json.dumps(cleansing_hints or [], ensure_ascii=False)}\n\n"
        f"Planner hints:\n{json.dumps(planner_hints or {}, ensure_ascii=False)}\n\n"
        f"Context pack:\n{json.dumps(context_pack or {}, ensure_ascii=False)}\n"
    )


async def apply_transform_plan(
    *,
    plan: PipelinePlan,
    join_plan: Optional[List[Dict[str, Any]]],
    cleansing_hints: Optional[List[Dict[str, Any]]],
    context_pack: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
    actor: str,
    tenant_id: str,
    user_id: Optional[str],
    data_policies: Optional[Dict[str, Any]],
    selected_model: Optional[str],
    allowed_models: Optional[List[str]],
    llm_gateway: LLMGateway,
    redis_service: Optional[RedisService],
    audit_store: Optional[AuditLogStore],
    db_name: str,
    branch: Optional[str],
    dataset_registry: Any,
) -> PipelineTransformResult:
    system_prompt = _build_transform_system_prompt()
    user_prompt = _build_transform_user_prompt(
        goal=str(plan.goal or ""),
        data_scope=plan.data_scope.model_dump(mode="json"),
        current_definition=plan.definition_json,
        outputs=[output.model_dump(mode="json") for output in (plan.outputs or [])],
        associations=[assoc.model_dump(mode="json") for assoc in (plan.associations or [])],
        join_plan=join_plan,
        cleansing_hints=cleansing_hints,
        context_pack=context_pack,
        planner_hints=planner_hints,
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
            task="PIPELINE_TRANSFORM_PLAN_V1",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=PipelineTransformEnvelope,
            model=selected_model,
            allowed_models=allowed_models,
            redis_service=redis_service,
            audit_store=audit_store,
            audit_partition_key=f"pipeline_plan:{plan.plan_id or uuid4()}",
            audit_actor=actor,
            audit_resource_id=str(plan.plan_id or uuid4()),
            audit_metadata={"kind": "pipeline_transform_plan", "tenant_id": tenant_id, "user_id": user_id},
        )
    except (LLMUnavailableError, LLMRequestError, LLMOutputValidationError) as exc:
        raise exc

    try:
        definition_json = dict(draft.definition_json or {})
    except ValidationError as exc:
        raise LLMOutputValidationError(str(exc)) from exc

    updated_plan = plan.model_copy(update={"definition_json": definition_json})
    if not updated_plan.associations:
        derived = _associations_from_join_plan(join_plan)
        if derived:
            updated_plan = updated_plan.model_copy(update={"associations": derived})
    validation = await validate_pipeline_plan(
        plan=updated_plan,
        dataset_registry=dataset_registry,
        db_name=db_name,
        branch=branch,
        require_output=True,
        context_pack=context_pack,
    )

    return PipelineTransformResult(
        plan=validation.plan,
        validation_errors=list(validation.errors or []),
        validation_warnings=list(validation.warnings or []),
        confidence=float(draft.confidence) if draft is not None else None,
        notes=list(draft.notes or []),
        warnings=list(draft.warnings or []),
        llm_meta=llm_meta,
    )


def _build_transform_mcp_system_prompt(*, allowed_tools: List[str]) -> str:
    return (
        "You are a STRICT pipeline transform agent for SPICE-Harvester.\n"
        "Return ONLY a single JSON object. No markdown, no commentary.\n"
        "You are NOT executing anything; you are producing a list of plan-builder tool calls.\n"
        "\n"
        "Hard rules:\n"
        "- Use ONLY the allowed tools listed below.\n"
        "- Do NOT invent dataset ids, node ids, or column names.\n"
        "- Do NOT output definition_json directly.\n"
        "- Do NOT change plan.outputs or plan.data_scope.\n"
        "- Keep changes minimal and deterministic.\n"
        "- Prefer patching existing nodes/edges over rebuilding.\n"
        "- Cross joins are forbidden.\n"
        "\n"
        "Allowed tools:\n"
        + json.dumps(list(allowed_tools or []), ensure_ascii=False)
        + "\n\n"
        "Tool schemas (args only; server injects `plan` automatically):\n"
        "- plan_add_join: {left_node_id,right_node_id,left_keys[],right_keys[],join_type?,node_id?}\n"
        "- plan_add_filter: {input_node_id,expression,node_id?}\n"
        "- plan_add_compute: {input_node_id,expression,node_id?}\n"
        "- plan_add_cast: {input_node_id,casts:[{column,type}],node_id?}\n"
        "- plan_add_rename: {input_node_id,rename:{src:dst},node_id?}\n"
        "- plan_add_select/drop/dedupe: {input_node_id,columns[],node_id?}\n"
        "- plan_add_normalize: {input_node_id,columns[],trim?,empty_to_null?,whitespace_to_null?,lowercase?,uppercase?,node_id?}\n"
        "- plan_add_regex_replace: {input_node_id,rules:[{column,pattern,replacement?,flags?}],node_id?}\n"
        "- plan_add_transform: {operation,input_node_ids[],metadata?,node_id?}\n"
        "- plan_add_edge: {from_node_id,to_node_id}\n"
        "- plan_delete_edge: {from_node_id,to_node_id}\n"
        "- plan_set_node_inputs: {node_id,input_node_ids[]}\n"
        "- plan_update_node_metadata: {node_id,set?,unset?,replace?}\n"
        "- plan_delete_node: {node_id}\n"
        "\n"
        "Output schema:\n"
        "{\n"
        "  \"steps\": [{\"tool\": string, \"args\": object}],\n"
        "  \"confidence\": number (0..1),\n"
        "  \"notes\": string[],\n"
        "  \"warnings\": string[],\n"
        "  \"questions\": [{id,question,required,type,options?,default?}] (optional)\n"
        "}\n"
    )


def _build_transform_mcp_user_prompt(
    *,
    goal: str,
    data_scope: Dict[str, Any],
    current_plan: Dict[str, Any],
    join_plan: Optional[List[Dict[str, Any]]],
    cleansing_hints: Optional[List[Dict[str, Any]]],
    context_pack: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
) -> str:
    payload = {
        "goal": goal,
        "data_scope": data_scope or {},
        "current_plan": current_plan or {},
        "join_plan": join_plan or [],
        "cleansing_hints": cleansing_hints or [],
        "planner_hints": planner_hints or {},
        "context_pack": context_pack or {},
    }
    return json.dumps(payload, ensure_ascii=False)


def _is_output_node(plan_obj: Dict[str, Any], node_id: str) -> bool:
    definition = plan_obj.get("definition_json") if isinstance(plan_obj.get("definition_json"), dict) else {}
    nodes = definition.get("nodes") if isinstance(definition.get("nodes"), list) else []
    target = str(node_id or "").strip()
    if not target:
        return False
    for node in nodes:
        if not isinstance(node, dict):
            continue
        if str(node.get("id") or "").strip() != target:
            continue
        return str(node.get("type") or "").strip().lower() == "output"
    return False


async def apply_transform_plan_mcp(
    *,
    plan: PipelinePlan,
    join_plan: Optional[List[Dict[str, Any]]],
    cleansing_hints: Optional[List[Dict[str, Any]]],
    context_pack: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
    actor: str,
    tenant_id: str,
    user_id: Optional[str],
    data_policies: Optional[Dict[str, Any]],
    selected_model: Optional[str],
    allowed_models: Optional[List[str]],
    llm_gateway: LLMGateway,
    redis_service: Optional[RedisService],
    audit_store: Optional[AuditLogStore],
    db_name: str,
    branch: Optional[str],
    dataset_registry: Any,
) -> PipelineTransformResult:
    """
    MCP-based transform agent.

    The LLM emits a patch script (plan builder tool calls) instead of definition_json.
    """
    # Lazy import to keep MCP optional in environments where the SDK/servers are not installed.
    try:
        try:
            from mcp.mcp_client import get_mcp_manager  # type: ignore[import-not-found]
        except Exception:  # pragma: no cover
            from backend.mcp.mcp_client import get_mcp_manager  # type: ignore[import-not-found]
    except Exception as exc:
        raise LLMRequestError(f"MCP client unavailable: {exc}") from exc

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

    original_outputs = list(plan.outputs or [])
    original_associations = list(plan.associations or [])

    task_spec_model = getattr(plan, "task_spec", None)
    if task_spec_model is not None:
        dataset_count = len(list(plan.data_scope.dataset_ids or [])) if plan.data_scope else 0
        task_spec_model = clamp_task_spec(spec=task_spec_model, dataset_count=dataset_count)
        plan = plan.model_copy(update={"task_spec": task_spec_model})
    plan_obj = plan.model_dump(mode="json")

    allow_join = True if task_spec_model is None else bool(task_spec_model.allow_join)
    allow_advanced = True if task_spec_model is None else bool(task_spec_model.allow_advanced_transforms)

    allowed_tools = [
        "plan_add_filter",
        "plan_add_compute",
        "plan_add_cast",
        "plan_add_rename",
        "plan_add_select",
        "plan_add_drop",
        "plan_add_dedupe",
        "plan_add_normalize",
        "plan_add_regex_replace",
        "plan_add_edge",
        "plan_delete_edge",
        "plan_set_node_inputs",
        "plan_update_node_metadata",
        "plan_delete_node",
    ]
    if allow_join:
        allowed_tools.append("plan_add_join")
    if allow_advanced:
        allowed_tools.append("plan_add_transform")
    allowed_tool_set = set(allowed_tools)

    system_prompt = _build_transform_mcp_system_prompt(allowed_tools=allowed_tools)
    user_prompt = _build_transform_mcp_user_prompt(
        goal=str(plan.goal or ""),
        data_scope=plan.data_scope.model_dump(mode="json"),
        current_plan=plan_obj,
        join_plan=join_plan,
        cleansing_hints=cleansing_hints,
        context_pack=context_pack,
        planner_hints=planner_hints,
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
            task="PIPELINE_TRANSFORM_MCP_SCRIPT_V1",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=PipelinePlanBuilderScriptEnvelope,
            model=selected_model,
            allowed_models=allowed_models,
            redis_service=redis_service,
            audit_store=audit_store,
            audit_partition_key=f"pipeline_plan:{plan.plan_id or uuid4()}",
            audit_actor=actor,
            audit_resource_id=str(plan.plan_id or uuid4()),
            audit_metadata={"kind": "pipeline_transform_mcp_script", "tenant_id": tenant_id, "user_id": user_id},
        )
    except (LLMUnavailableError, LLMRequestError, LLMOutputValidationError) as exc:
        raise exc

    tool_errors: List[str] = []
    tool_warnings: List[str] = list(draft.warnings or [])
    steps = list(draft.steps or [])
    if not steps:
        tool_errors.append("planner returned no steps")

    advanced_ops = {"groupby", "aggregate", "pivot", "window", "explode", "sort", "union"}
    join_meta_keys = {"joinType", "allowCrossJoin", "leftKeys", "rightKeys", "leftKey", "rightKey", "joinKey"}

    def _policy_error(tool_name: str, args: Dict[str, Any]) -> Optional[str]:
        if task_spec_model is None:
            return None
        scope_value = str(getattr(task_spec_model.scope, "value", task_spec_model.scope) or "").strip().lower()
        if scope_value == "report_only":
            return "task_spec.scope=report_only forbids plan transforms"
        if not allow_advanced and tool_name == "plan_add_transform":
            return "plan_add_transform is not allowed for this task"
        if not allow_join and tool_name == "plan_add_join":
            return "plan_add_join is not allowed for this task"
        if tool_name == "plan_add_transform":
            op = str(args.get("operation") or "").strip().lower()
            if not allow_join and op in {"join", "union"}:
                return f"operation={op} is not allowed (joins disabled)"
            if not allow_advanced and op in advanced_ops:
                return f"operation={op} is not allowed (advanced transforms disabled)"
        if tool_name == "plan_update_node_metadata":
            set_fields = args.get("set")
            if isinstance(set_fields, dict):
                op = set_fields.get("operation")
                if isinstance(op, str):
                    op_norm = op.strip().lower()
                    if not allow_join and op_norm in {"join", "union"}:
                        return f"operation={op} is not allowed (joins disabled)"
                    if not allow_advanced and op_norm in advanced_ops:
                        return f"operation={op} is not allowed (advanced transforms disabled)"
                if not allow_join and any(key in set_fields for key in join_meta_keys):
                    return "join metadata patch is not allowed (joins disabled)"
        return None

    for step in steps:
        tool_name = str(step.tool or "").strip()
        if tool_name not in allowed_tool_set:
            tool_errors.append(f"unsupported tool: {tool_name}")
            continue
        args = dict(step.args or {})

        policy_error = _policy_error(tool_name, args)
        if policy_error:
            tool_errors.append(f"{tool_name}: {policy_error}")
            continue

        # Enforce "do not change outputs" at the tool boundary.
        if tool_name in {"plan_add_output", "plan_update_output"}:
            tool_errors.append(f"{tool_name} is forbidden for transform")
            continue
        if tool_name == "plan_delete_node":
            node_id = str(args.get("node_id") or "").strip()
            if _is_output_node(plan_obj, node_id):
                tool_errors.append("plan_delete_node cannot target output nodes")
                continue
        if tool_name == "plan_update_node_metadata":
            node_id = str(args.get("node_id") or "").strip()
            if _is_output_node(plan_obj, node_id):
                tool_errors.append("plan_update_node_metadata cannot target output nodes")
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
        return PipelineTransformResult(
            plan=plan,
            validation_errors=tool_errors,
            validation_warnings=tool_warnings,
            confidence=float(draft.confidence) if draft is not None else None,
            notes=list(draft.notes or []),
            warnings=tool_warnings,
            llm_meta=llm_meta,
        )

    try:
        updated_plan = PipelinePlan.model_validate(plan_obj)
    except ValidationError as exc:
        raise LLMOutputValidationError(str(exc)) from exc

    # Preserve outputs/data_scope and associations contract explicitly.
    updated_plan = updated_plan.model_copy(update={"outputs": original_outputs, "data_scope": plan.data_scope})
    if not updated_plan.associations and original_associations:
        updated_plan = updated_plan.model_copy(update={"associations": original_associations})

    validation = await validate_pipeline_plan(
        plan=updated_plan,
        dataset_registry=dataset_registry,
        db_name=db_name,
        branch=branch,
        require_output=True,
        context_pack=context_pack,
    )

    return PipelineTransformResult(
        plan=validation.plan,
        validation_errors=list(validation.errors or []),
        validation_warnings=list(tool_warnings) + list(validation.warnings or []),
        confidence=float(draft.confidence) if draft is not None else None,
        notes=list(draft.notes or []),
        warnings=list(tool_warnings),
        llm_meta=llm_meta,
    )
