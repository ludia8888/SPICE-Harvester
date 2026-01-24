"""
Experimental: Autonomous MCP planner loop.

Goal: Give the LLM high autonomy to *iteratively* call MCP tools (observe -> act),
instead of emitting a one-shot "script" that requires heuristics for drift/repair.

This intentionally keeps prompts token-light by:
- storing large objects (plan/context_pack) server-side
- sending the LLM compact summaries + the last tool observation
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

from bff.services.pipeline_plan_models import PipelineClarificationQuestion, PipelinePlanCompileResult
from bff.services.pipeline_plan_validation import validate_pipeline_plan
from shared.models.pipeline_plan import PipelinePlan, PipelinePlanDataScope
from shared.services.audit_log_store import AuditLogStore
from shared.services.dataset_registry import DatasetRegistry
from shared.services.llm_gateway import (
    LLMCallMeta,
    LLMGateway,
    LLMOutputValidationError,
    LLMRequestError,
    LLMUnavailableError,
)
from shared.services.llm_quota import enforce_llm_quota
from shared.services.redis_service import RedisService

logger = logging.getLogger(__name__)


class AutonomousPlannerDecision(BaseModel):
    action: Literal["call_tool", "finish", "clarify"] = Field(default="call_tool")
    tool: Optional[str] = Field(default=None, max_length=200)
    args: Dict[str, Any] = Field(default_factory=dict)
    questions: List[PipelineClarificationQuestion] = Field(default_factory=list)
    notes: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)

    @field_validator("args", mode="before")
    @classmethod
    def _coerce_args(cls, v):  # noqa: ANN001
        # gpt-5 can emit nulls for empty objects; treat as empty dict.
        return {} if v is None else v

    @field_validator("questions", "notes", "warnings", mode="before")
    @classmethod
    def _coerce_lists(cls, v):  # noqa: ANN001
        # gpt-5 can emit nulls for empty arrays; treat as empty list.
        return [] if v is None else v

    @model_validator(mode="after")
    def _validate_action(self) -> "AutonomousPlannerDecision":
        if self.action == "call_tool":
            if not str(self.tool or "").strip():
                raise ValueError("tool is required when action=call_tool")
        if self.action == "clarify":
            if not self.questions:
                raise ValueError("questions is required when action=clarify")
        return self


@dataclass
class _PlannerState:
    db_name: str
    branch: Optional[str]
    dataset_ids: List[str]
    goal: str

    context_pack: Optional[Dict[str, Any]] = None
    key_inference: Optional[Dict[str, Any]] = None
    type_inference: Optional[Dict[str, Any]] = None
    join_plan: Optional[List[Dict[str, Any]]] = None

    plan_obj: Optional[Dict[str, Any]] = None
    input_nodes: Dict[str, str] = field(default_factory=dict)
    last_observation: Optional[Dict[str, Any]] = None


_AUTONOMOUS_ALLOWED_TOOLS: tuple[str, ...] = (
    # Context pack + deterministic analysis
    "context_pack_build",
    "context_pack_null_report",
    "context_pack_infer_keys",
    "context_pack_infer_types",
    "context_pack_infer_join_plan",
    # Plan builder
    "plan_new",
    "plan_add_input",
    "plan_add_join",
    "plan_add_transform",
    "plan_add_group_by",
    "plan_add_window",
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
    # Validation / preview
    "plan_validate_structure",
    "plan_validate",
    "plan_preview",
    "preview_inspect",
    "plan_evaluate_joins",
)


def _trim_key_inference(value: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    pk = value.get("primary_keys")
    if not isinstance(pk, list):
        pk = []
    fk = value.get("foreign_keys")
    if not isinstance(fk, list):
        fk = []
    pk_out: List[Dict[str, Any]] = []
    for item in pk[:12]:
        if not isinstance(item, dict):
            continue
        pk_candidates = item.get("pk_candidates")
        if not isinstance(pk_candidates, list):
            pk_candidates = []
        pk_out.append(
            {
                "dataset_id": item.get("dataset_id"),
                "name": item.get("name"),
                "row_count": item.get("row_count"),
                "best_pk": item.get("best_pk"),
                "pk_candidates": pk_candidates[:3],
            }
        )
    fk_out = [item for item in fk[:20] if isinstance(item, dict)]
    return {"primary_keys": pk_out, "foreign_keys": fk_out, "notes": value.get("notes")}


def _trim_type_inference(value: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    datasets = value.get("datasets")
    if not isinstance(datasets, list):
        datasets = []
    join_casts = value.get("join_key_cast_suggestions")
    if not isinstance(join_casts, list):
        join_casts = []
    summarized: List[Dict[str, Any]] = []
    for ds in datasets[:12]:
        if not isinstance(ds, dict):
            continue
        cols = ds.get("columns")
        if not isinstance(cols, list):
            cols = []
        mismatches = [col for col in cols if isinstance(col, dict) and col.get("mismatch")]
        summarized.append(
            {
                "dataset_id": ds.get("dataset_id"),
                "name": ds.get("name"),
                "mismatched_columns": mismatches[:20],
            }
        )
    return {
        "datasets": summarized,
        "join_key_cast_suggestions": [item for item in join_casts[:20] if isinstance(item, dict)],
        "notes": value.get("notes"),
    }


def _trim_join_plan(value: Optional[List[Dict[str, Any]]]) -> Optional[List[Dict[str, Any]]]:
    if not isinstance(value, list):
        return None
    out: List[Dict[str, Any]] = []
    for item in value[:20]:
        if not isinstance(item, dict):
            continue
        out.append(item)
    return out


def _summarize_context_pack(pack: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(pack, dict):
        return {}

    overview = pack.get("datasets_overview")
    if not isinstance(overview, list):
        overview = []
    selected = pack.get("selected_datasets")
    if not isinstance(selected, list):
        selected = []

    def _ds_overview(item: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(item, dict):
            return None
        cols_preview = item.get("columns_preview")
        if not isinstance(cols_preview, list):
            cols_preview = []
        cols_preview = [str(c) for c in cols_preview if isinstance(c, str)]
        return {
            "dataset_id": item.get("dataset_id"),
            "name": item.get("name"),
            "branch": item.get("branch"),
            "column_count": item.get("column_count"),
            "columns_preview": cols_preview[:30],
        }

    def _ds_selected(item: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(item, dict):
            return None
        cols = item.get("columns")
        if not isinstance(cols, list):
            cols = []
        col_items: List[Dict[str, Any]] = []
        for col in cols[:60]:
            if not isinstance(col, dict):
                continue
            name = str(col.get("name") or "").strip()
            if not name:
                continue
            col_items.append({"name": name, "type": col.get("type") or col.get("data_type")})
        return {
            "dataset_id": item.get("dataset_id"),
            "name": item.get("name"),
            "branch": item.get("branch"),
            "row_count": item.get("row_count"),
            "columns": col_items,
        }

    overview_out = [d for d in (_ds_overview(item) for item in overview) if d]
    selected_out = [d for d in (_ds_selected(item) for item in selected) if d]
    return {
        "db_name": pack.get("db_name"),
        "branch": pack.get("branch"),
        "datasets_overview": overview_out[:30],
        "selected_datasets": selected_out[:12],
        "integration_suggestions_keys": sorted(list((pack.get("integration_suggestions") or {}).keys()))
        if isinstance(pack.get("integration_suggestions"), dict)
        else [],
    }


def _summarize_plan(plan_obj: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(plan_obj, dict):
        return {}
    definition = plan_obj.get("definition_json")
    if not isinstance(definition, dict):
        definition = {}
    nodes = definition.get("nodes")
    edges = definition.get("edges")
    if not isinstance(nodes, list):
        nodes = []
    if not isinstance(edges, list):
        edges = []

    node_out: List[Dict[str, Any]] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_id = str(node.get("id") or "").strip()
        node_type = str(node.get("type") or "").strip()
        meta = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
        op = str(meta.get("operation") or "").strip() or None

        summary_meta: Dict[str, Any] = {}
        op_l = (op or "").lower()
        if op_l == "join":
            for key in ("joinType", "leftKeys", "rightKeys"):
                if key in meta:
                    summary_meta[key] = meta.get(key)
        elif op_l == "groupby":
            for key in ("groupBy", "aggregates"):
                if key in meta:
                    summary_meta[key] = meta.get(key)
        elif op_l == "window":
            if "window" in meta:
                summary_meta["window"] = meta.get("window")
        elif op_l in {"filter", "compute"}:
            if "expression" in meta:
                expr = str(meta.get("expression") or "")
                summary_meta["expression"] = expr[:2000]
        elif op_l in {"select", "drop", "dedupe", "normalize"}:
            if "columns" in meta:
                summary_meta["columns"] = meta.get("columns")
        elif op_l == "cast":
            if "casts" in meta:
                summary_meta["casts"] = meta.get("casts")
        elif op_l == "rename":
            if "rename" in meta:
                summary_meta["rename"] = meta.get("rename")
        elif op_l == "regexreplace":
            if "rules" in meta:
                summary_meta["rules"] = meta.get("rules")
        elif node_type.lower() == "input":
            for key in ("datasetId", "datasetName", "datasetBranch"):
                if key in meta:
                    summary_meta[key] = meta.get(key)
        elif node_type.lower() == "output":
            if "outputName" in meta:
                summary_meta["outputName"] = meta.get("outputName")

        node_out.append(
            {
                "id": node_id,
                "type": node_type,
                "operation": op,
                "meta": summary_meta,
            }
        )

    edge_out: List[Dict[str, Any]] = []
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        src = str(edge.get("from") or "").strip()
        dst = str(edge.get("to") or "").strip()
        if src and dst:
            edge_out.append({"from": src, "to": dst})

    outputs = plan_obj.get("outputs")
    if not isinstance(outputs, list):
        outputs = []
    outputs_out: List[Dict[str, Any]] = []
    for out in outputs:
        if not isinstance(out, dict):
            continue
        outputs_out.append(
            {
                "output_name": out.get("output_name") or out.get("outputName"),
                "output_kind": out.get("output_kind") or out.get("outputKind"),
            }
        )

    return {"nodes": node_out[:80], "edges": edge_out[:120], "outputs": outputs_out[:20]}


def _plan_status(plan_obj: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(plan_obj, dict):
        return {"initialized": False}
    definition = plan_obj.get("definition_json")
    if not isinstance(definition, dict):
        definition = {}
    nodes = definition.get("nodes")
    edges = definition.get("edges")
    if not isinstance(nodes, list):
        nodes = []
    if not isinstance(edges, list):
        edges = []
    outputs = plan_obj.get("outputs")
    if not isinstance(outputs, list):
        outputs = []
    return {
        "initialized": True,
        "node_count": len(nodes),
        "edge_count": len(edges),
        "output_count": len(outputs),
    }


def _summarize_input_nodes(state: _PlannerState) -> List[Dict[str, Any]]:
    # Provide stable node ids for each selected dataset so the LLM can reference inputs deterministically.
    by_id: Dict[str, Dict[str, Any]] = {}
    if isinstance(state.context_pack, dict):
        selected = state.context_pack.get("selected_datasets")
        if isinstance(selected, list):
            for item in selected:
                if not isinstance(item, dict):
                    continue
                ds_id = str(item.get("dataset_id") or "").strip()
                if not ds_id:
                    continue
                by_id[ds_id] = item

    out: List[Dict[str, Any]] = []
    for ds_id in state.dataset_ids:
        meta = by_id.get(ds_id) or {}
        out.append(
            {
                "dataset_id": ds_id,
                "dataset_name": meta.get("name"),
                "dataset_branch": meta.get("branch") or state.branch,
                "node_id": state.input_nodes.get(ds_id),
            }
        )
    return out


def _build_system_prompt(*, allowed_tools: List[str]) -> str:
    tool_lines = "\n".join([f"- {name}" for name in allowed_tools])
    return (
        "You are an autonomous data-engineering agent for SPICE-Harvester.\n"
        "You can iteratively call tools to inspect datasets, infer keys/types, and build/validate/preview a pipeline plan.\n"
        "\n"
        "Core rules:\n"
        "- Return ONLY JSON matching the schema. No markdown, no extra text.\n"
        "- Minimize assumptions; if a requirement is ambiguous, ask a small number of questions.\n"
        "- Prefer tool use over guessing. Use preview/evaluate tools to validate joins/aggregations.\n"
        "- Keep outputs consistent with the goal. Do not invent extra outputs unless requested.\n"
        "- Do not finish until the plan is VALID (use plan_validate or finish only when it will pass validation).\n"
        "- Never propose destructive/write operations. This flow is planning + sample-safe preview only.\n"
        "\n"
        "Common patterns:\n"
        "- join: plan_add_join(left_node_id,right_node_id,left_keys=[...],right_keys=[...],join_type='left|inner')\n"
        "- compute: plan_add_compute(input_node_id, expression=\"line_revenue = qty * unit_price\")\n"
        "- group by / aggregate: plan_add_group_by(input_node_id, group_by=[...], aggregates=[{\"column\":\"price\",\"op\":\"sum\",\"alias\":\"total_revenue\"}])\n"
        "- window rank: plan_add_window(input_node_id, partition_by=[...], order_by=[\"-total_revenue\"])  # adds row_number\n"
        "- top-N: plan_add_filter(input_node_id, expression=\"row_number <= N\")\n"
        "- output: plan_add_output(input_node_id, output_name=\"result\")\n"
        "\n"
        "Available tools:\n"
        f"{tool_lines}\n"
        "\n"
        "Tool usage notes:\n"
        "- context_pack_build returns a sample-based summary; keep max_sample_rows small unless you need more evidence.\n"
        "- plan_new creates an EMPTY plan (no nodes/edges/outputs). You MUST add input nodes and outputs.\n"
        "- plan_* tools operate on the current in-memory plan/context pack held by the server.\n"
        "- plan_preview is sample-safe; keep limits small and iterate.\n"
        "\n"
        "Schema:\n"
        "{\n"
        "  \"action\": \"call_tool|finish|clarify\",\n"
        "  \"tool\": \"string (required when action=call_tool)\",\n"
        "  \"args\": {\"...\": \"...\"},\n"
        "  \"questions\": [PipelineClarificationQuestion],\n"
        "  \"notes\": string[],\n"
        "  \"warnings\": string[],\n"
        "  \"confidence\": number (0..1)\n"
        "}\n"
    )


def _build_user_prompt(*, snapshot: Dict[str, Any]) -> str:
    return (
        "Current snapshot (authoritative):\n"
        f"{json.dumps(snapshot or {}, ensure_ascii=False)}\n"
        "\n"
        "Choose the next action.\n"
    )


async def compile_pipeline_plan_mcp_autonomous(
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
    Experimental autonomous compiler:
    - LLM iteratively chooses tools based on server-provided state summaries + observations.
    - Server stores full plan/context_pack; LLM sees compact summaries only (token-aware).
    """
    plan_id = str(uuid4())

    if not data_scope or not str(data_scope.db_name or "").strip():
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=["data_scope.db_name is required"],
            validation_warnings=[],
            questions=[
                PipelineClarificationQuestion(
                    id="db_name",
                    question="Which database (db_name) should I use?",
                    required=True,
                    type="string",
                )
            ],
        )

    dataset_ids = [str(item).strip() for item in (data_scope.dataset_ids or []) if str(item).strip()]
    if not dataset_ids:
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=["data_scope.dataset_ids is required"],
            validation_warnings=[],
            questions=[
                PipelineClarificationQuestion(
                    id="dataset_ids",
                    question="Which dataset_ids should I use to satisfy the goal?",
                    required=True,
                    type="string",
                )
            ],
        )

    # Lazy import to keep MCP optional in environments where the SDK/servers are not installed.
    try:
        try:
            from mcp.mcp_client import get_mcp_manager  # type: ignore[import-not-found]
        except Exception:  # pragma: no cover
            from backend.mcp.mcp_client import get_mcp_manager  # type: ignore[import-not-found]
    except Exception as exc:
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=[f"MCP client unavailable: {exc}"],
            validation_warnings=[],
            questions=[],
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

    state = _PlannerState(
        db_name=str(data_scope.db_name or "").strip(),
        branch=str(data_scope.branch or "").strip() or None,
        dataset_ids=dataset_ids,
        goal=str(goal or "").strip(),
        context_pack=context_pack if isinstance(context_pack, dict) else None,
    )

    max_steps = 18
    llm_meta: Optional[LLMCallMeta] = None
    tool_warnings: List[str] = []
    tool_errors: List[str] = []
    notes: List[str] = []

    # Deterministic bootstrap: create an empty plan and stable input nodes BEFORE the LLM loop.
    # This avoids wasted iterations on mechanical setup (and prevents plan_new reset loops).
    try:
        base = await _call_pipeline_tool(
            "plan_new",
            {
                "goal": state.goal,
                "db_name": state.db_name,
                "branch": state.branch,
                "dataset_ids": state.dataset_ids,
            },
        )
        plan = base.get("plan") if isinstance(base, dict) else None
        state.plan_obj = plan if isinstance(plan, dict) else None
        if not isinstance(state.plan_obj, dict):
            state.last_observation = {"error": "plan_new failed", "details": base}
        else:
            for idx, dataset_id in enumerate(state.dataset_ids):
                node_id = f"input_{idx + 1}"
                args_input: Dict[str, Any] = {
                    "plan": state.plan_obj,
                    "dataset_id": dataset_id,
                    "node_id": node_id,
                }
                try:
                    ds = await dataset_registry.get_dataset(dataset_id=dataset_id)
                except Exception:
                    ds = None
                if ds:
                    if str(ds.name or "").strip():
                        args_input["dataset_name"] = str(ds.name)
                    branch_value = str(ds.branch or "").strip() or (state.branch or None)
                    if branch_value:
                        args_input["dataset_branch"] = branch_value
                elif state.branch:
                    args_input["dataset_branch"] = state.branch

                res = await _call_pipeline_tool("plan_add_input", args_input)
                if isinstance(res, dict) and res.get("error"):
                    tool_errors.append(f"plan_add_input failed: {res.get('error')}")
                    state.last_observation = {"error": "plan_add_input failed", "details": res, "dataset_id": dataset_id}
                    break
                if isinstance(res, dict) and isinstance(res.get("plan"), dict):
                    state.plan_obj = res["plan"]
                state.input_nodes[dataset_id] = node_id
            if not state.last_observation:
                state.last_observation = {
                    "status": "bootstrapped",
                    "plan_status": _plan_status(state.plan_obj),
                    "input_nodes": _summarize_input_nodes(state),
                }
    except Exception as exc:
        tool_errors.append(f"bootstrap failed: {exc}")
        state.last_observation = {"error": f"bootstrap failed: {exc}"}

    allowed_tools = list(_AUTONOMOUS_ALLOWED_TOOLS)
    if isinstance(state.plan_obj, dict):
        # The plan is already initialized server-side; exposing plan_new encourages reset loops.
        allowed_tools = [t for t in allowed_tools if t != "plan_new"]
    allowed_tool_set = set(allowed_tools)
    system_prompt = _build_system_prompt(allowed_tools=allowed_tools)

    for step_idx in range(max_steps):
        # If the LLM claims it's done, validate and (if invalid) keep iterating for repair.
        # This is intentionally server-side (mechanical) so we don't leak large plan objects.
        snapshot: Dict[str, Any] = {
            "goal": state.goal,
            "data_scope": {"db_name": state.db_name, "branch": state.branch, "dataset_ids": state.dataset_ids},
            "answers": answers or None,
            "planner_hints": planner_hints or None,
            "task_spec": task_spec or None,
            "context_pack_summary": _summarize_context_pack(state.context_pack),
            "key_inference": _trim_key_inference(state.key_inference),
            "type_inference": _trim_type_inference(state.type_inference),
            "join_plan": _trim_join_plan(state.join_plan),
            "input_nodes": _summarize_input_nodes(state),
            "plan_status": _plan_status(state.plan_obj),
            "plan_summary": _summarize_plan(state.plan_obj),
            "last_observation": state.last_observation,
        }
        user_prompt = _build_user_prompt(snapshot=snapshot)

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
            decision, llm_meta = await llm_gateway.complete_json(
                task="PIPELINE_PLAN_MCP_AUTONOMOUS_STEP_V1",
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=AutonomousPlannerDecision,
                model=selected_model,
                allowed_models=allowed_models,
                redis_service=redis_service,
                audit_store=audit_store,
                audit_partition_key=f"pipeline_plan:{plan_id}",
                audit_actor=actor,
                audit_resource_id=plan_id,
                audit_metadata={
                    "kind": "pipeline_plan_mcp_autonomous",
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "step": step_idx + 1,
                },
            )
        except (LLMUnavailableError, LLMRequestError, LLMOutputValidationError) as exc:
            return PipelinePlanCompileResult(
                status="clarification_required",
                plan_id=plan_id,
                plan=None,
                validation_errors=[str(exc)],
                validation_warnings=tool_warnings,
                questions=[],
                llm_meta=llm_meta,
            )

        notes.extend([str(n) for n in (decision.notes or []) if str(n or "").strip()])
        tool_warnings.extend([str(w) for w in (decision.warnings or []) if str(w or "").strip()])

        if decision.action == "clarify":
            return PipelinePlanCompileResult(
                status="clarification_required",
                plan_id=plan_id,
                plan=None,
                validation_errors=["planner requested clarification"],
                validation_warnings=tool_warnings,
                questions=list(decision.questions or []),
                llm_meta=llm_meta,
                planner_confidence=float(decision.confidence),
                planner_notes=notes,
            )

        if decision.action == "finish":
            if not isinstance(state.plan_obj, dict):
                state.last_observation = {"error": "cannot finish: plan is not initialized"}
                continue
            try:
                plan_model = PipelinePlan.model_validate(state.plan_obj)
            except Exception as exc:
                state.last_observation = {"status": "invalid", "errors": [str(exc)]}
                continue
            validation = await validate_pipeline_plan(
                plan=plan_model,
                dataset_registry=dataset_registry,
                db_name=str(plan_model.data_scope.db_name or ""),
                branch=str(plan_model.data_scope.branch or "") or None,
                require_output=True,
                context_pack=state.context_pack,
            )
            state.plan_obj = validation.plan.model_dump(mode="json")
            if not validation.errors:
                break
            state.last_observation = {
                "status": "invalid",
                "errors": list(validation.errors or [])[:20],
                "warnings": list(validation.warnings or [])[:20],
                "preflight": validation.preflight,
            }
            continue

        tool_name = str(decision.tool or "").strip()
        if tool_name not in allowed_tool_set:
            state.last_observation = {"error": f"unsupported tool: {tool_name}"}
            tool_errors.append(f"unsupported tool: {tool_name}")
            continue

        args = dict(decision.args or {})

        # Dispatch: keep server-side state; only store big objects here.
        try:
            if tool_name == "context_pack_build":
                payload = await _call_pipeline_tool(
                    tool_name,
                    {
                        "db_name": state.db_name,
                        "branch": state.branch,
                        "dataset_ids": state.dataset_ids,
                        "max_datasets_overview": int(args.get("max_datasets_overview") or 20),
                        "max_selected_datasets": int(args.get("max_selected_datasets") or 9),
                        "max_sample_rows": int(args.get("max_sample_rows") or 10),
                        "max_join_candidates": int(args.get("max_join_candidates") or 12),
                        "max_pk_candidates": int(args.get("max_pk_candidates") or 6),
                    },
                )
                pack = payload.get("context_pack") if isinstance(payload, dict) else None
                if isinstance(pack, dict):
                    state.context_pack = pack
                    state.last_observation = {"status": "success", "context_pack_summary": _summarize_context_pack(pack)}
                else:
                    state.last_observation = payload
                continue

            if tool_name.startswith("context_pack_"):
                if not isinstance(state.context_pack, dict):
                    state.last_observation = {"error": "context_pack is not available; call context_pack_build first"}
                    continue
                if tool_name == "context_pack_null_report":
                    requested = int(args.get("max_columns") or 20)
                    args["max_columns"] = max(1, min(requested, 30))
                # Always use the server-side context pack. If the model provides `context_pack` in args,
                # ignore it to prevent partial/invalid packs from overriding state.
                args.pop("context_pack", None)
                payload = await _call_pipeline_tool(tool_name, {**args, "context_pack": state.context_pack})
                if tool_name == "context_pack_infer_keys":
                    state.key_inference = payload.get("inference") if isinstance(payload.get("inference"), dict) else None
                    state.last_observation = {
                        "status": payload.get("status"),
                        "inference": _trim_key_inference(state.key_inference),
                    }
                    continue
                if tool_name == "context_pack_infer_types":
                    state.type_inference = payload.get("inference") if isinstance(payload.get("inference"), dict) else None
                    state.last_observation = {
                        "status": payload.get("status"),
                        "inference": _trim_type_inference(state.type_inference),
                    }
                    continue
                if tool_name == "context_pack_infer_join_plan":
                    inf = payload.get("inference") if isinstance(payload.get("inference"), dict) else None
                    jp = inf.get("join_plan") if isinstance(inf, dict) else None
                    state.join_plan = jp if isinstance(jp, list) else None
                    state.last_observation = {
                        "status": payload.get("status"),
                        "inference": {"join_plan": _trim_join_plan(state.join_plan), "warnings": inf.get("warnings") if isinstance(inf, dict) else None},
                    }
                    continue
                if tool_name == "context_pack_null_report":
                    report = payload.get("report") if isinstance(payload, dict) else None
                    if isinstance(report, dict):
                        datasets = report.get("datasets") if isinstance(report.get("datasets"), list) else []
                        trimmed_datasets: List[Dict[str, Any]] = []
                        for ds in datasets[:12]:
                            if not isinstance(ds, dict):
                                continue
                            cols = ds.get("columns_with_nulls") if isinstance(ds.get("columns_with_nulls"), list) else []
                            trimmed_datasets.append({**ds, "columns_with_nulls": cols[:20]})
                        state.last_observation = {"status": payload.get("status"), "report": {**report, "datasets": trimmed_datasets}}
                    else:
                        state.last_observation = payload
                    continue
                state.last_observation = payload
                continue

            if tool_name == "plan_new":
                if isinstance(state.plan_obj, dict):
                    # Avoid "reset loops": once a plan exists, repeatedly calling plan_new just discards progress.
                    state.last_observation = {
                        "status": "noop",
                        "message": "plan is already initialized; do not call plan_new again. Next: add inputs/joins/transforms/outputs.",
                        "plan_status": _plan_status(state.plan_obj),
                    }
                    continue
                payload = await _call_pipeline_tool(
                    tool_name,
                    {
                        "goal": state.goal,
                        "db_name": state.db_name,
                        "branch": state.branch,
                        "dataset_ids": state.dataset_ids,
                    },
                )
                plan = payload.get("plan") if isinstance(payload, dict) else None
                state.plan_obj = plan if isinstance(plan, dict) else None
                if state.plan_obj:
                    state.last_observation = {
                        "status": "success",
                        "plan_status": _plan_status(state.plan_obj),
                        "next_suggested_tools": ["plan_add_input", "plan_add_join", "plan_add_transform", "plan_add_output"],
                    }
                else:
                    state.last_observation = payload
                continue

            # All other plan tools require a plan.
            if not isinstance(state.plan_obj, dict):
                state.last_observation = {"error": "plan is not initialized; call plan_new first"}
                continue

            # Token hygiene: clamp preview size in tool results that we echo to the LLM snapshot.
            if tool_name == "plan_preview":
                requested = int(args.get("limit") or 50)
                args["limit"] = max(1, min(requested, 50))

            # Always use the server-side plan. Never allow `plan` in args to override state.
            args.pop("plan", None)
            payload = await _call_pipeline_tool(tool_name, {**args, "plan": state.plan_obj})
            if isinstance(payload, dict) and isinstance(payload.get("plan"), dict):
                state.plan_obj = payload["plan"]

            if tool_name == "plan_preview" and isinstance(payload, dict) and isinstance(payload.get("preview"), dict):
                preview = payload.get("preview") if isinstance(payload.get("preview"), dict) else {}
                rows = preview.get("rows")
                if isinstance(rows, list):
                    preview = dict(preview)
                    preview["rows"] = rows[:5]
                state.last_observation = {"status": payload.get("status"), "preview": preview, "warnings": payload.get("warnings")}
            else:
                # Token hygiene: do not echo back the full plan/context objects to the LLM.
                observation: Dict[str, Any] = dict(payload) if isinstance(payload, dict) else {"result": payload}
                observation.pop("plan", None)
                if isinstance(observation.get("evaluations"), list):
                    observation["evaluations"] = observation.get("evaluations")[:8]
                if isinstance(observation.get("warnings"), list):
                    observation["warnings"] = observation.get("warnings")[:20]
                if isinstance(observation.get("errors"), list):
                    observation["errors"] = observation.get("errors")[:20]
                state.last_observation = observation

        except Exception as exc:
            logger.warning("autonomous tool failed tool=%s err=%s", tool_name, exc)
            state.last_observation = {"error": str(exc)}
            tool_errors.append(f"{tool_name}: {exc}")
            continue

    if not isinstance(state.plan_obj, dict):
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=tool_errors or ["planner did not produce a plan"],
            validation_warnings=tool_warnings,
            questions=[],
            llm_meta=llm_meta,
            planner_notes=notes or None,
        )

    try:
        plan_model = PipelinePlan.model_validate(state.plan_obj)
    except ValidationError as exc:
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=None,
            validation_errors=[str(exc)],
            validation_warnings=tool_warnings,
            questions=[],
            llm_meta=llm_meta,
            planner_notes=notes or None,
        )

    validation = await validate_pipeline_plan(
        plan=plan_model,
        dataset_registry=dataset_registry,
        db_name=str(plan_model.data_scope.db_name or ""),
        branch=str(plan_model.data_scope.branch or "") or None,
        require_output=True,
        context_pack=state.context_pack,
    )

    if validation.errors:
        return PipelinePlanCompileResult(
            status="clarification_required",
            plan_id=plan_id,
            plan=validation.plan,
            validation_errors=list(validation.errors or []),
            validation_warnings=list(tool_warnings) + list(validation.warnings or []),
            questions=[],
            llm_meta=llm_meta,
            planner_notes=notes or None,
            planner_confidence=None,
            preflight=validation.preflight,
        )

    return PipelinePlanCompileResult(
        status="success",
        plan_id=plan_id,
        plan=validation.plan,
        validation_errors=[],
        validation_warnings=list(tool_warnings) + list(validation.warnings or []),
        questions=[],
        compilation_report=validation.compilation_report,
        llm_meta=llm_meta,
        planner_notes=notes or None,
        preflight=validation.preflight,
    )
