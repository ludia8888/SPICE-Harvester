"""
Autonomous Pipeline Agent loop (single agent + tools).

This is a *single* LLM loop that iteratively:
  BuildPrompt -> Inference -> ToolRequested -> ExecuteTool -> AppendObservation -> ...

No multi-agent routing, no sub-agent handoffs, and no server-side semantic rewrites.
The LLM must explicitly call tools (MCP) to inspect data and mutate the in-memory plan.

This loop can either:
- Produce an analysis/report (e.g., null checks) without creating a plan, or
- Build a validated pipeline plan + preview.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator, model_validator

from bff.services.pipeline_plan_models import PipelineClarificationQuestion
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
from shared.services.pipeline_plan_registry import PipelinePlanRegistry
from shared.services.redis_service import RedisService
from shared.utils.llm_safety import mask_pii, stable_json_dumps

logger = logging.getLogger(__name__)


class AutonomousPipelineAgentToolCall(BaseModel):
    tool: str = Field(default="", max_length=200)
    args: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("args", mode="before")
    @classmethod
    def _coerce_args(cls, v):  # noqa: ANN001
        # Some models can emit nulls for empty objects; treat as empty dict.
        return {} if v is None else v


class AutonomousPipelineAgentDecision(BaseModel):
    action: Literal["call_tool", "finish", "clarify"] = Field(default="call_tool")
    # Preferred: multiple tool calls per inference to reduce round-trips.
    tool_calls: List[AutonomousPipelineAgentToolCall] = Field(default_factory=list, max_length=12)
    # Legacy single-call shape (kept for backward compatibility with older mocks/models).
    tool: Optional[str] = Field(default=None, max_length=200)
    args: Dict[str, Any] = Field(default_factory=dict)
    questions: List[PipelineClarificationQuestion] = Field(default_factory=list)
    notes: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)

    @field_validator("args", mode="before")
    @classmethod
    def _coerce_args(cls, v):  # noqa: ANN001
        # Some models can emit nulls for empty objects; treat as empty dict.
        return {} if v is None else v

    @field_validator("tool_calls", mode="before")
    @classmethod
    def _coerce_tool_calls(cls, v):  # noqa: ANN001
        # Some models can emit nulls for empty arrays; treat as empty list.
        return [] if v is None else v

    @field_validator("questions", mode="before")
    @classmethod
    def _coerce_questions(cls, v):  # noqa: ANN001
        # Some models emit questions as a list of strings; coerce into the structured shape.
        if v is None:
            return []
        if not isinstance(v, list):
            return v
        out: List[Any] = []
        for idx, item in enumerate(v):
            if isinstance(item, str):
                text = item.strip()
                if text:
                    out.append(
                        {
                            "id": f"q{idx + 1}",
                            "question": text,
                            "required": True,
                            "type": "string",
                        }
                    )
                continue
            out.append(item)
        return out

    @field_validator("notes", "warnings", mode="before")
    @classmethod
    def _coerce_lists(cls, v):  # noqa: ANN001
        # Some models can emit nulls for empty arrays; treat as empty list.
        return [] if v is None else v

    @model_validator(mode="after")
    def _validate_action(self) -> "AutonomousPipelineAgentDecision":
        if self.action == "call_tool":
            has_batch = bool(self.tool_calls)
            has_single = bool(str(self.tool or "").strip())
            if not (has_batch or has_single):
                raise ValueError("tool_calls or tool is required when action=call_tool")
            for idx, call in enumerate(self.tool_calls or []):
                if not str(call.tool or "").strip():
                    raise ValueError(f"tool_calls[{idx}].tool is required when action=call_tool")
        if self.action == "clarify":
            if not self.questions:
                raise ValueError("questions is required when action=clarify")
        return self


@dataclass
class _AgentState:
    db_name: str
    branch: Optional[str]
    dataset_ids: List[str]
    goal: str
    principal_id: str
    principal_type: str
    pipeline_id: Optional[str] = None

    # Deterministic (tool-produced) context.
    context_pack: Optional[Dict[str, Any]] = None
    null_report: Optional[Dict[str, Any]] = None
    key_inference: Optional[Dict[str, Any]] = None
    type_inference: Optional[Dict[str, Any]] = None
    join_plan: Optional[List[Dict[str, Any]]] = None

    # Plan (optional): created only when the LLM explicitly calls plan_new.
    plan_obj: Optional[Dict[str, Any]] = None
    last_observation: Optional[Dict[str, Any]] = None
    # Append-only JSONL prompt log to enable provider-side prefix caching.
    prompt_items: List[str] = field(default_factory=list)


_PIPELINE_AGENT_ALLOWED_TOOLS: tuple[str, ...] = (
    # Context pack + deterministic analysis
    "context_pack_build",
    "context_pack_null_report",
    "context_pack_infer_keys",
    "context_pack_infer_types",
    "context_pack_infer_join_plan",
    # Plan builder (mutating, but in-memory only)
    "plan_new",
    "plan_add_input",
    "plan_add_join",
    "plan_add_transform",
    "plan_add_group_by",
    "plan_add_group_by_expr",
    "plan_add_window",
    "plan_add_window_expr",
    "plan_add_filter",
    "plan_add_compute",
    "plan_add_compute_column",
    "plan_add_compute_assignments",
    "plan_add_cast",
    "plan_add_rename",
    "plan_add_select",
    "plan_add_select_expr",
    "plan_add_drop",
    "plan_add_dedupe",
    "plan_add_normalize",
    "plan_add_regex_replace",
    "plan_add_output",
    "plan_add_edge",
    "plan_delete_edge",
    "plan_set_node_inputs",
    "plan_update_node_metadata",
    "plan_configure_input_read",
    "plan_delete_node",
    "plan_update_output",
    # Validation / preview
    "plan_validate_structure",
    "plan_validate",
    "plan_preview",
    "preview_inspect",
    "plan_evaluate_joins",
    # Refutation gate (claim-based hard failures with witnesses only)
    "plan_refute_claims",
    # Pipeline execution (Spark worker via control plane). Use only when the user explicitly asked
    # to materialize outputs/build/deploy (these can write to storage / create datasets).
    "pipeline_create_from_plan",
    "pipeline_update_from_plan",
    "pipeline_preview_wait",
    "pipeline_build_wait",
    "pipeline_deploy_promote_build",
)


def _trim_null_report(report: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(report, dict):
        return None
    datasets = report.get("datasets")
    if not isinstance(datasets, list):
        datasets = []
    trimmed: List[Dict[str, Any]] = []
    for ds in datasets[:12]:
        if not isinstance(ds, dict):
            continue
        cols = ds.get("columns")
        if not isinstance(cols, list):
            cols = []
        trimmed.append(
            {
                "dataset_id": ds.get("dataset_id"),
                "name": ds.get("name"),
                "row_count": ds.get("row_count"),
                "columns": [c for c in cols[:30] if isinstance(c, dict)],
            }
        )
    return {"datasets": trimmed, "notes": report.get("notes")}


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
        if isinstance(item, dict):
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


def _build_system_prompt(*, allowed_tools: List[str]) -> str:
    tool_lines = "\n".join([f"- {name}" for name in allowed_tools])
    return (
        "You are a single autonomous data-engineering agent for SPICE-Harvester.\n"
        "You can iteratively call tools to inspect datasets (profiling/nulls/keys/types) and optionally build a pipeline plan.\n"
        "The user prompt is an append-only JSONL log (one JSON object per line). The newest lines are the most recent state/observations.\n"
        "\n"
        "Core rules:\n"
        "- Return ONLY JSON matching the schema. No markdown, no extra text.\n"
        "- Do NOT invent data; prefer tool observations over guessing.\n"
        "- Tool args MUST NOT include raw `context_pack` or `plan` objects; the server stores them.\n"
        "- Respect scope: if the user asked ONLY for analysis (e.g., null check), do NOT build a plan.\n"
        "- If planner_hints.require_plan=true, you MUST build and validate a plan (do NOT finish with report-only).\n"
        "- If the user asked for a derived dataset/result, build a plan via plan_* tools and ensure it validates.\n"
        "- No silent server-side rewrites exist; any plan changes must be explicit tool calls.\n"
        "- Latency: prefer batching multiple tool calls in a single response via `tool_calls` (max 12 per step).\n"
        "- If batching plan edits, use deterministic `node_id` for every node you will reference later in the same batch.\n"
        "- Do NOT ask the user to increase internal step/tool-call limits. If you're close to finishing, use the most direct remaining tool calls to complete.\n"
        "- Do NOT write full SQL queries. Spark SQL *expressions* are allowed inside these tools:\n"
        "  - plan_add_filter(expression=...)\n"
        "  - plan_add_compute(expression=...) (legacy)\n"
        "  - plan_add_compute_column(target_column=..., formula=...)\n"
        "  - plan_add_compute_assignments(assignments=[{column,expression}, ...])\n"
        "  - plan_add_select_expr(expressions=[...])\n"
        "  - plan_add_group_by_expr(aggregate_expressions=[...])\n"
        "  - plan_add_window_expr(expressions=[{column,expr}, ...])\n"
        "- `plan_add_transform` is operation+metadata (NOT SQL).\n"
        "- `plan_preview` runs a lightweight deterministic executor and does NOT match full Spark SQL semantics.\n"
        "  If you use non-trivial Spark SQL (cast/case/regexp/date funcs/etc), validate with Spark via `pipeline_preview_wait` after materializing the pipeline.\n"
        "- Default to read-only (analysis + plan + preview). Only if the user explicitly asked to materialize outputs/build/deploy should you call `pipeline_*` tools.\n"
        "- Deterministic inference tools (keys/types/join-plan) produce hypotheses. Treat them as evidence/suggestions, not ground truth.\n"
        "- If you want hard gating on ETL assumptions, attach `claims` to node.metadata (list of {id, kind, severity, spec}).\n"
        "  For CAST_LOSSLESS claims, include spec.allowed_normalization (e.g., [\"trim\",\"lowercase\"]).\n"
        "  You can call `plan_refute_claims` to find counterexamples (witness-based). A PASS means 'not refuted', never 'proven correct'.\n"
        "\n"
        "Common patterns:\n"
        "- null check: context_pack_null_report(dataset_ids=[...])\n"
        "- infer PK/FK: context_pack_infer_keys()\n"
        "- infer casts: context_pack_infer_types(join_plan=...)\n"
        "- join: plan_add_join(left_node_id,right_node_id,left_keys=[...],right_keys=[...],join_type='left|inner')\n"
        "- ingest permissive: plan_configure_input_read(node_id, mode='PERMISSIVE', corrupt_record_column='_corrupt_record')\n"
        "- compute: plan_add_compute_column(input_node_id, target_column=\"revenue\", formula=\"qty * unit_price\")\n"
        "- compute many: plan_add_compute_assignments(input_node_id, assignments=[{\"column\":\"x\",\"expression\":\"...\"}, ...])\n"
        "- select expr: plan_add_select_expr(input_node_id, expressions=[\"col\", \"sum(price) as total\"])  # Spark selectExpr\n"
        "- group by / aggregate: plan_add_group_by(input_node_id, group_by=[...], aggregates=[{\"column\":\"price\",\"op\":\"sum\",\"alias\":\"total\"}])\n"
        "- group by expr: plan_add_group_by_expr(input_node_id, group_by=[...], aggregate_expressions=[\"approx_percentile(price, 0.5) as p50\", ...])\n"
        "- window expr: plan_add_window_expr(input_node_id, expressions=[{\"column\":\"rn\",\"expr\":\"row_number() over (partition by k order by ts desc)\"}])\n"
        "- top-N: plan_add_filter(input_node_id, expression=\"row_number <= N\")\n"
        "- output: plan_add_output(input_node_id, output_name=\"result\")\n"
        "- refute claims: plan_refute_claims()  # optional; server will also run it on finish when a plan exists\n"
        "- materialize pipeline: pipeline_create_from_plan(name=\"...\", location=\"team/...\"), then pipeline_preview_wait(...), pipeline_build_wait(...)\n"
        "- deploy from build: pipeline_deploy_promote_build(pipeline_id, build_job_id, node_id, db_name, dataset_name)  # requires approve\n"
        "\n"
        "Available tools:\n"
        f"{tool_lines}\n"
        "\n"
        "Schema:\n"
        "{\n"
        "  \"action\": \"call_tool|finish|clarify\",\n"
        "  \"tool_calls\": [{\"tool\": \"string\", \"args\": {\"...\": \"...\"}}],\n"
        "  \"tool\": \"string (legacy single tool; optional if tool_calls is set)\",\n"
        "  \"args\": {\"...\": \"...\"},\n"
        "  \"questions\": [PipelineClarificationQuestion],\n"
        "  \"notes\": string[],\n"
        "  \"warnings\": string[],\n"
        "  \"confidence\": number (0..1)\n"
        "}\n"
    )


def _prompt_text(items: List[str]) -> str:
    if not items:
        return ""
    # Keep a trailing newline so each next append preserves exact prefix matches.
    return "\n".join([str(item) for item in items]) + "\n"


def _build_prompt_header(
    *,
    state: _AgentState,
    answers: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
    task_spec: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "type": "header",
        "goal": state.goal,
        "data_scope": {"db_name": state.db_name, "branch": state.branch, "dataset_ids": state.dataset_ids},
        "answers": answers or None,
        "planner_hints": planner_hints or None,
        "task_spec": task_spec or None,
        "context_pack_summary": _summarize_context_pack(state.context_pack),
    }


def _build_compaction_snapshot(
    *,
    state: _AgentState,
    answers: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
    task_spec: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    # Deterministic compaction snapshot: derived from server-side state, not from model text.
    return {
        "goal": state.goal,
        "data_scope": {"db_name": state.db_name, "branch": state.branch, "dataset_ids": state.dataset_ids},
        "answers": answers or None,
        "planner_hints": planner_hints or None,
        "task_spec": task_spec or None,
        "context_pack_summary": _summarize_context_pack(state.context_pack),
        "null_report": _trim_null_report(state.null_report),
        "key_inference": _trim_key_inference(state.key_inference),
        "type_inference": _trim_type_inference(state.type_inference),
        "join_plan": _trim_join_plan(state.join_plan),
        "plan_status": _plan_status(state.plan_obj),
        "plan_summary": _summarize_plan(state.plan_obj),
        "last_observation": state.last_observation,
    }


def _maybe_compact_prompt_items(
    *,
    state: _AgentState,
    answers: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
    task_spec: Optional[Dict[str, Any]],
    max_chars: int,
) -> None:
    """
    Deterministic compaction: when the append-only log grows too large, replace it with:
    - a fresh header, plus
    - a compact authoritative snapshot derived from current server-side state.
    """
    if max_chars <= 0:
        return
    current = _prompt_text(state.prompt_items)
    if len(current) <= max_chars:
        return
    snapshot = _build_compaction_snapshot(state=state, answers=answers, planner_hints=planner_hints, task_spec=task_spec)
    state.prompt_items = [
        stable_json_dumps(_build_prompt_header(state=state, answers=answers, planner_hints=planner_hints, task_spec=task_spec)),
        stable_json_dumps(
            {
                "type": "compaction",
                "reason": "prompt_too_large",
                "snapshot": snapshot,
                "note": "Older detailed log was compacted. Treat snapshot as authoritative and continue.",
            }
        ),
    ]


def _mask_tool_observation(payload: Dict[str, Any]) -> Dict[str, Any]:
    # Ensure we never echo raw PII back into the prompt snapshot.
    return mask_pii(payload or {}, max_string_chars=200)


def _is_internal_budget_clarification(questions: List[PipelineClarificationQuestion]) -> bool:
    for q in questions or []:
        text = f"{q.id} {q.question}".lower()
        if any(token in text for token in ("step", "steps", "budget", "limit", "quota", "스텝", "단계", "한도")):
            return True
    return False


async def run_pipeline_agent_mcp_autonomous(
    *,
    goal: str,
    data_scope: PipelinePlanDataScope,
    answers: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
    task_spec: Optional[Dict[str, Any]] = None,
    context_pack: Optional[Dict[str, Any]] = None,
    persist_plan: bool = True,
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
    plan_registry: PipelinePlanRegistry,
) -> Dict[str, Any]:
    """
    Returns a payload compatible with the existing UI expectations for `/agent/pipeline-runs`.

    The payload is NOT an ApiResponse envelope; the router should wrap it.
    """
    run_id = str(uuid4())
    plan_id: Optional[str] = None

    db_name = str(data_scope.db_name or "").strip()
    if not db_name:
        return {
            "run_id": run_id,
            "status": "clarification_required",
            "plan_id": plan_id,
            "plan": None,
            "preview": None,
            "report": None,
            "questions": [
                {
                    "id": "db_name",
                    "question": "Which database (db_name) should I use?",
                    "required": True,
                    "type": "string",
                }
            ],
            "validation_errors": ["data_scope.db_name is required"],
            "validation_warnings": [],
        }

    dataset_ids = [str(item).strip() for item in (data_scope.dataset_ids or []) if str(item).strip()]
    if not dataset_ids:
        return {
            "run_id": run_id,
            "status": "clarification_required",
            "plan_id": plan_id,
            "plan": None,
            "preview": None,
            "report": None,
            "questions": [
                {
                    "id": "dataset_ids",
                    "question": "Which dataset_ids should I use to satisfy the goal?",
                    "required": True,
                    "type": "string",
                }
            ],
            "validation_errors": ["data_scope.dataset_ids is required"],
            "validation_warnings": [],
        }

    # Lazy import: MCP is optional in some envs.
    try:
        try:
            from mcp.mcp_client import get_mcp_manager  # type: ignore[import-not-found]
        except Exception:  # pragma: no cover
            from backend.mcp.mcp_client import get_mcp_manager  # type: ignore[import-not-found]
    except Exception as exc:
        return {
            "run_id": run_id,
            "status": "clarification_required",
            "plan_id": plan_id,
            "plan": None,
            "preview": None,
            "report": None,
            "questions": [],
            "validation_errors": [f"MCP client unavailable: {exc}"],
            "validation_warnings": [],
        }

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
            texts: List[str] = []
            for part in content:
                text = getattr(part, "text", None)
                if text is None and isinstance(part, dict):
                    text = part.get("text")
                if isinstance(text, str) and text.strip():
                    texts.append(text.strip())
            if is_error:
                return {"error": "\n".join(texts).strip() or f"MCP tool error: {tool}"}
            for text in texts:
                try:
                    parsed = json.loads(text)
                except Exception:
                    continue
                if isinstance(parsed, dict):
                    return parsed
            if texts:
                return {"result": texts[0]}
        raise RuntimeError(f"Unexpected MCP tool result type for {tool}: {type(payload)}")

    principal_id = str(user_id or actor or "system").strip() or "system"
    principal_type = "user"
    state = _AgentState(
        db_name=db_name,
        branch=str(data_scope.branch or "").strip() or None,
        dataset_ids=dataset_ids,
        goal=str(goal or "").strip(),
        principal_id=principal_id,
        principal_type=principal_type,
    )
    if isinstance(context_pack, dict) and context_pack:
        state.context_pack = dict(context_pack)
        state.last_observation = {"status": "bootstrapped", "context_pack": "provided"}

    # Scale iteration budget by dataset count (multi-way joins take more tool calls).
    max_steps = min(60, 24 + max(0, len(state.dataset_ids) - 1) * 8)
    llm_meta: Optional[LLMCallMeta] = None
    notes: List[str] = []
    tool_warnings: List[str] = []
    tool_errors: List[str] = []

    allowed_tools = list(_PIPELINE_AGENT_ALLOWED_TOOLS)
    system_prompt = _build_system_prompt(allowed_tools=allowed_tools)
    max_tool_calls_per_step = 12
    # Keep the user prompt under the LLM gateway hard cap to avoid server-side truncation.
    prompt_char_limit = int(max(1, int(getattr(llm_gateway, "max_prompt_chars", 20000) or 20000) * 0.9))

    async def _execute_tool_call(*, tool_name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a single MCP tool call and mutate agent state.

        This is separated so the loop can execute a *batch* of tool calls per LLM step.
        """
        if tool_name not in allowed_tools:
            state.last_observation = {"error": f"tool not allowed: {tool_name}"}
            return state.last_observation

        try:
            if tool_name == "context_pack_build":
                if isinstance(state.context_pack, dict):
                    meaningful = {k for k in args.keys() if k not in {"db_name", "branch", "dataset_ids"}}
                    if not meaningful:
                        state.last_observation = {
                            "status": "noop",
                            "message": "context_pack is already built; do not call context_pack_build again. Next: run analysis tools (context_pack_null_report / infer_keys / infer_types / infer_join_plan) or finish if you already have enough evidence.",
                            "context_pack_summary": _summarize_context_pack(state.context_pack),
                        }
                        return state.last_observation
                # Never allow the model to override request scope.
                payload = await _call_pipeline_tool(
                    tool_name,
                    {
                        **args,
                        "db_name": state.db_name,
                        "branch": state.branch,
                        "dataset_ids": state.dataset_ids,
                    },
                )
                pack = payload.get("context_pack") if isinstance(payload, dict) else None
                state.context_pack = pack if isinstance(pack, dict) else state.context_pack
                observation = dict(payload) if isinstance(payload, dict) else {"result": payload}
                observation.pop("context_pack", None)
                state.last_observation = _mask_tool_observation(observation)
                return state.last_observation

            # All remaining context_pack_* tools require a context pack.
            if tool_name.startswith("context_pack_") and tool_name != "context_pack_build":
                if not isinstance(state.context_pack, dict):
                    state.last_observation = {"error": "context_pack missing; call context_pack_build first"}
                    return state.last_observation
                # Always use the server-side context pack. If the model provides `context_pack` in args,
                # ignore it to prevent partial/invalid packs from overriding state.
                args.pop("context_pack", None)
                if tool_name == "context_pack_null_report" and isinstance(state.null_report, dict):
                    # If we already computed a null report that covers the requested datasets,
                    # avoid tool loops and prompt the model to finish.
                    requested_ids = args.get("dataset_ids")
                    requested: Optional[set[str]] = None
                    if isinstance(requested_ids, list):
                        requested = {str(item).strip() for item in requested_ids if str(item).strip()}
                    reported_ids: set[str] = set()
                    datasets = state.null_report.get("datasets")
                    if isinstance(datasets, list):
                        for item in datasets:
                            if not isinstance(item, dict):
                                continue
                            ds_id = str(item.get("dataset_id") or "").strip()
                            if ds_id:
                                reported_ids.add(ds_id)
                    other_args = {k for k in args.keys() if k != "dataset_ids"}
                    if not other_args and (requested is None or requested.issubset(reported_ids)):
                        state.last_observation = {
                            "status": "noop",
                            "message": "null report already computed; finish to return it",
                            "report": _trim_null_report(state.null_report),
                        }
                        return state.last_observation
                if tool_name == "context_pack_infer_keys" and isinstance(state.key_inference, dict):
                    other_args = {k for k in args.keys() if k != "dataset_ids"}
                    if not other_args:
                        state.last_observation = {
                            "status": "noop",
                            "message": "key inference already computed; use it (or finish) instead of re-running",
                            "inference": _trim_key_inference(state.key_inference),
                        }
                        return state.last_observation
                if tool_name == "context_pack_infer_types" and isinstance(state.type_inference, dict):
                    other_args = {k for k in args.keys() if k != "dataset_ids"}
                    if not other_args:
                        state.last_observation = {
                            "status": "noop",
                            "message": "type inference already computed; use it (or finish) instead of re-running",
                            "inference": _trim_type_inference(state.type_inference),
                        }
                        return state.last_observation
                if tool_name == "context_pack_infer_join_plan" and isinstance(state.join_plan, list):
                    other_args = {k for k in args.keys() if k != "dataset_ids"}
                    if not other_args:
                        state.last_observation = {
                            "status": "noop",
                            "message": "join plan already inferred; proceed to build joins (or finish) instead of re-running",
                            "join_plan": _trim_join_plan(state.join_plan),
                        }
                        return state.last_observation
                payload = await _call_pipeline_tool(tool_name, {**args, "context_pack": state.context_pack})
                if isinstance(payload, dict) and payload.get("error"):
                    state.last_observation = _mask_tool_observation(payload)
                    return state.last_observation
                if tool_name == "context_pack_null_report":
                    report = payload.get("report") if isinstance(payload, dict) else None
                    state.null_report = report if isinstance(report, dict) else state.null_report
                    state.last_observation = _mask_tool_observation(
                        {
                            "status": payload.get("status") if isinstance(payload, dict) else None,
                            "report": _trim_null_report(state.null_report),
                        }
                    )
                elif tool_name == "context_pack_infer_keys":
                    inf = payload.get("inference") if isinstance(payload, dict) else None
                    state.key_inference = inf if isinstance(inf, dict) else state.key_inference
                    state.last_observation = _mask_tool_observation(
                        {
                            "status": payload.get("status") if isinstance(payload, dict) else None,
                            "inference": _trim_key_inference(state.key_inference),
                        }
                    )
                elif tool_name == "context_pack_infer_types":
                    inf = payload.get("inference") if isinstance(payload, dict) else None
                    state.type_inference = inf if isinstance(inf, dict) else state.type_inference
                    state.last_observation = _mask_tool_observation(
                        {
                            "status": payload.get("status") if isinstance(payload, dict) else None,
                            "inference": _trim_type_inference(state.type_inference),
                        }
                    )
                elif tool_name == "context_pack_infer_join_plan":
                    inf = payload.get("join_plan") if isinstance(payload, dict) else None
                    state.join_plan = inf if isinstance(inf, list) else state.join_plan
                    state.last_observation = _mask_tool_observation(
                        {
                            "status": payload.get("status") if isinstance(payload, dict) else None,
                            "join_plan": _trim_join_plan(state.join_plan),
                        }
                    )
                else:
                    state.last_observation = _mask_tool_observation(payload if isinstance(payload, dict) else {"result": payload})
                return state.last_observation

            if tool_name == "plan_new":
                if isinstance(state.plan_obj, dict):
                    state.last_observation = {
                        "status": "noop",
                        "message": "plan already exists; do not call plan_new again. Continue editing the current plan.",
                        "plan_status": _plan_status(state.plan_obj),
                    }
                    return state.last_observation
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
                observation = dict(payload) if isinstance(payload, dict) else {"result": payload}
                observation.pop("plan", None)
                state.last_observation = _mask_tool_observation(
                    {
                        **observation,
                        "plan_status": _plan_status(state.plan_obj),
                        "next_suggested_tools": ["plan_add_input", "plan_add_join", "plan_add_transform", "plan_add_output"],
                    }
                )
                return state.last_observation

            if tool_name.startswith("pipeline_"):
                # Pipeline execution tools (Spark worker via control plane). These can write/build/deploy,
                # so the model should only call them when the user explicitly requested materialization.
                if tool_name in {"pipeline_create_from_plan", "pipeline_update_from_plan"}:
                    if not isinstance(state.plan_obj, dict):
                        state.last_observation = {"error": "plan is not initialized; call plan_new first"}
                        return state.last_observation
                    args.pop("plan", None)
                    payload = await _call_pipeline_tool(
                        tool_name,
                        {
                            **args,
                            "plan": state.plan_obj,
                            "principal_id": state.principal_id,
                            "principal_type": state.principal_type,
                        },
                    )
                else:
                    # These tools should not receive the full plan object.
                    args.pop("plan", None)
                    payload = await _call_pipeline_tool(
                        tool_name,
                        {
                            **args,
                            "principal_id": state.principal_id,
                            "principal_type": state.principal_type,
                            # Helpful for audit/logging; endpoints don't require it for pipeline_id-based ops.
                            "db_name": state.db_name,
                        },
                    )

                # Capture pipeline_id if tool returned it, so subsequent steps can reference it without re-parsing.
                if isinstance(payload, dict):
                    pipeline_id = None
                    if isinstance(payload.get("pipeline"), dict):
                        pipeline_id = str((payload.get("pipeline") or {}).get("pipeline_id") or "").strip() or None
                    if not pipeline_id:
                        pipeline_id = str(payload.get("pipeline_id") or "").strip() or None
                    if pipeline_id:
                        state.pipeline_id = pipeline_id

                # Keep observations small; do not embed definition_json.
                state.last_observation = _mask_tool_observation(payload if isinstance(payload, dict) else {"result": payload})
                return state.last_observation

            # Plan tools require a plan.
            if not isinstance(state.plan_obj, dict):
                state.last_observation = {"error": "plan is not initialized; call plan_new first"}
                return state.last_observation

            if tool_name == "plan_preview":
                requested = int(args.get("limit") or 50)
                args["limit"] = max(1, min(requested, 200))

            # Always use the server-side plan. Never allow `plan` in args to override state.
            args.pop("plan", None)
            payload = await _call_pipeline_tool(tool_name, {**args, "plan": state.plan_obj})
            if isinstance(payload, dict) and isinstance(payload.get("plan"), dict):
                state.plan_obj = payload["plan"]

            # Keep tool observation small; never echo the full plan back.
            if tool_name == "plan_preview" and isinstance(payload, dict) and isinstance(payload.get("preview"), dict):
                preview = dict(payload.get("preview") or {})
                rows = preview.get("rows")
                if isinstance(rows, list):
                    preview["rows"] = rows[:5]
                state.last_observation = _mask_tool_observation(
                    {
                        "status": payload.get("status"),
                        "preview": preview,
                        "warnings": payload.get("warnings"),
                        "plan_status": _plan_status(state.plan_obj),
                    }
                )
            else:
                observation = dict(payload) if isinstance(payload, dict) else {"result": payload}
                observation.pop("plan", None)
                if isinstance(observation.get("evaluations"), list):
                    observation["evaluations"] = observation.get("evaluations")[:8]
                if isinstance(observation.get("warnings"), list):
                    observation["warnings"] = observation.get("warnings")[:20]
                if isinstance(observation.get("errors"), list):
                    observation["errors"] = observation.get("errors")[:20]
                state.last_observation = _mask_tool_observation({**observation, "plan_status": _plan_status(state.plan_obj)})

            return state.last_observation

        except Exception as exc:
            logger.warning("pipeline agent autonomous tool failed tool=%s err=%s", tool_name, exc)
            state.last_observation = {"error": str(exc)}
            tool_errors.append(f"{tool_name}: {exc}")
            return state.last_observation

    # Deterministic bootstrap: fetch a compact context pack once so the LLM starts with evidence.
    # This is safe (read-only) and avoids wasting an iteration on "call context_pack_build".
    try:
        dataset_count = len(state.dataset_ids)
        max_pairs = (dataset_count * (dataset_count - 1)) // 2 if dataset_count > 1 else 0
        bootstrap_args: Dict[str, Any] = {
            "db_name": state.db_name,
            "branch": state.branch,
            "dataset_ids": state.dataset_ids,
            "max_selected_datasets": min(12, max(1, dataset_count)),
            "max_sample_rows": 30,
        }
        if max_pairs:
            bootstrap_args["max_join_candidates"] = min(30, max(10, max_pairs))
        if not isinstance(state.context_pack, dict):
            pack_payload = await _call_pipeline_tool("context_pack_build", bootstrap_args)
            pack = pack_payload.get("context_pack") if isinstance(pack_payload, dict) else None
            if isinstance(pack, dict):
                state.context_pack = pack
                state.last_observation = {"status": "bootstrapped", "context_pack": "ready"}
    except Exception as exc:
        logger.warning("pipeline agent context_pack bootstrap failed err=%s", exc)

    # Initialize append-only prompt log with a stable header (enables prefix caching across iterations).
    state.prompt_items = [
        stable_json_dumps(_build_prompt_header(state=state, answers=answers, planner_hints=planner_hints, task_spec=task_spec))
    ]
    if state.last_observation:
        state.prompt_items.append(
            stable_json_dumps({"type": "bootstrap_observation", "observation": _mask_tool_observation(state.last_observation)})
        )

    for step_idx in range(max_steps):
        step_state: Dict[str, Any] = {
            "type": "state",
            "step": step_idx + 1,
            "analysis_status": {
                "has_context_pack": isinstance(state.context_pack, dict),
                "has_null_report": isinstance(state.null_report, dict),
                "has_key_inference": isinstance(state.key_inference, dict),
                "has_type_inference": isinstance(state.type_inference, dict),
                "has_join_plan": isinstance(state.join_plan, list),
            },
            "plan_status": _plan_status(state.plan_obj),
            "plan_summary": _summarize_plan(state.plan_obj),
            "last_observation": state.last_observation,
        }
        state.prompt_items.append(stable_json_dumps(step_state))
        _maybe_compact_prompt_items(
            state=state,
            answers=answers,
            planner_hints=planner_hints,
            task_spec=task_spec,
            max_chars=prompt_char_limit,
        )
        user_prompt = _prompt_text(state.prompt_items)

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
                task="PIPELINE_AGENT_AUTONOMOUS_STEP_V1",
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=AutonomousPipelineAgentDecision,
                model=selected_model,
                allowed_models=allowed_models,
                redis_service=redis_service,
                audit_store=audit_store,
                audit_partition_key=f"pipeline_agent:{run_id}",
                audit_actor=actor,
                audit_resource_id=run_id,
                audit_metadata={
                    "kind": "pipeline_agent_autonomous",
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "step": step_idx + 1,
                },
            )
        except (LLMUnavailableError, LLMRequestError, LLMOutputValidationError) as exc:
            tool_errors.append(str(exc))
            break

        # Append the decision to the prompt log so the next iteration benefits from prefix caching.
        state.prompt_items.append(
            stable_json_dumps(
                {
                    "type": "decision",
                    "step": step_idx + 1,
                    "decision": decision.model_dump(mode="json"),
                }
            )
        )

        notes.extend([str(n) for n in (decision.notes or []) if str(n or "").strip()])
        tool_warnings.extend([str(w) for w in (decision.warnings or []) if str(w or "").strip()])

        if decision.action == "clarify":
            if _is_internal_budget_clarification(list(decision.questions or [])):
                state.last_observation = {"error": "internal step budget cannot be changed; proceed using available tools"}
                continue
            return {
                "run_id": run_id,
                "status": "clarification_required",
                "plan_id": plan_id,
                "plan": None,
                "preview": None,
                "report": state.null_report,
                "questions": [q.model_dump(mode="json") for q in (decision.questions or [])],
                "validation_errors": tool_errors or ["planner requested clarification"],
                "validation_warnings": tool_warnings,
                "llm": (
                    {
                        "provider": llm_meta.provider,
                        "model": llm_meta.model,
                        "cache_hit": llm_meta.cache_hit,
                        "latency_ms": llm_meta.latency_ms,
                    }
                    if llm_meta
                    else None
                ),
            }

        if decision.action == "finish":
            if isinstance(state.plan_obj, dict):
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
                if validation.errors:
                    state.last_observation = {
                        "status": "invalid",
                        "errors": list(validation.errors or []),
                        "warnings": list(validation.warnings or []),
                    }
                    continue

                # Refutation gate: only blocks on concrete counterexamples (witnesses).
                # PASS means "not refuted", never "proven correct".
                state.prompt_items.append(
                    stable_json_dumps(
                        {
                            "type": "tool_call",
                            "step": step_idx + 1,
                            "tool": "plan_refute_claims",
                            "args": {"sample_limit": 400},
                        }
                    )
                )
                refute_observation = await _execute_tool_call(tool_name="plan_refute_claims", args={"sample_limit": 400})
                state.prompt_items.append(
                    stable_json_dumps(
                        {
                            "type": "tool_output",
                            "step": step_idx + 1,
                            "tool": "plan_refute_claims",
                            "output": refute_observation,
                        }
                    )
                )
                if isinstance(refute_observation, dict):
                    refute_errors = refute_observation.get("errors")
                    if refute_observation.get("status") == "invalid" or (isinstance(refute_errors, list) and refute_errors):
                        state.last_observation = refute_observation
                        continue
                    refute_warnings = refute_observation.get("warnings")
                    if isinstance(refute_warnings, list):
                        tool_warnings.extend([str(w) for w in refute_warnings if str(w or "").strip()])

                # Persist the plan so the existing preview endpoint can be used from the UI.
                if not plan_id:
                    plan_id = str(uuid4())
                if persist_plan:
                    await plan_registry.upsert_plan(
                        plan_id=plan_id,
                        tenant_id=tenant_id,
                        status="COMPILED",
                        goal=str(validation.plan.goal or ""),
                        db_name=str(validation.plan.data_scope.db_name or ""),
                        branch=str(validation.plan.data_scope.branch or "") or None,
                        plan=validation.plan.model_dump(mode="json"),
                        created_by=actor,
                    )

                return {
                    "run_id": run_id,
                    "status": "success",
                    "plan_id": plan_id,
                    "plan": validation.plan.model_dump(mode="json"),
                    # Preview is generated by the caller (router) to stay consistent with BFF preview semantics.
                    "preview": None,
                    "report": state.null_report,
                    "questions": [],
                    "validation_errors": [],
                    "validation_warnings": list(tool_warnings) + list(validation.warnings or []),
                    "preflight": validation.preflight,
                    "planner": {"confidence": float(decision.confidence), "notes": notes},
                    "llm": (
                        {
                            "provider": llm_meta.provider,
                            "model": llm_meta.model,
                            "cache_hit": llm_meta.cache_hit,
                            "latency_ms": llm_meta.latency_ms,
                        }
                        if llm_meta
                        else None
                    ),
                }

            if isinstance(state.null_report, dict):
                return {
                    "run_id": run_id,
                    "status": "success",
                    "plan_id": plan_id,
                    "plan": None,
                    "preview": None,
                    "report": state.null_report,
                    "questions": [],
                    "validation_errors": [],
                    "validation_warnings": list(tool_warnings),
                    "planner": {"confidence": float(decision.confidence), "notes": notes},
                    "llm": (
                        {
                            "provider": llm_meta.provider,
                            "model": llm_meta.model,
                            "cache_hit": llm_meta.cache_hit,
                            "latency_ms": llm_meta.latency_ms,
                        }
                        if llm_meta
                        else None
                    ),
                }

            state.last_observation = {"error": "cannot finish: produce a report or a validated plan first"}
            continue

        # call_tool
        tool_calls = list(decision.tool_calls or [])
        if not tool_calls:
            tool_calls = [AutonomousPipelineAgentToolCall(tool=str(decision.tool or ""), args=dict(decision.args or {}))]

        executed_tools: List[str] = []
        stop_reason: Optional[str] = None
        for call in tool_calls[:max_tool_calls_per_step]:
            tool_name = str(call.tool or "").strip()
            args = dict(call.args or {})
            if not tool_name:
                state.last_observation = {"error": "tool name missing"}
                stop_reason = "missing_tool"
                break

            executed_tools.append(tool_name)
            state.prompt_items.append(stable_json_dumps({"type": "tool_call", "step": step_idx + 1, "tool": tool_name, "args": args}))
            observation = await _execute_tool_call(tool_name=tool_name, args=args)
            state.prompt_items.append(
                stable_json_dumps({"type": "tool_output", "step": step_idx + 1, "tool": tool_name, "output": observation})
            )
            if not isinstance(observation, dict):
                stop_reason = "invalid_observation"
                break
            errors = observation.get("errors")
            if observation.get("error"):
                stop_reason = "tool_error"
                break
            if observation.get("status") == "invalid":
                stop_reason = "invalid_status"
                break
            if isinstance(errors, list) and errors:
                stop_reason = "validation_errors"
                break

        if executed_tools:
            augmented = dict(state.last_observation or {})
            augmented["executed_tools"] = executed_tools
            augmented["executed_tool_count"] = len(executed_tools)
            if stop_reason:
                augmented["batch_stopped"] = stop_reason
            state.last_observation = augmented
            state.prompt_items.append(
                stable_json_dumps(
                    {
                        "type": "batch_summary",
                        "step": step_idx + 1,
                        "executed_tools": executed_tools,
                        "stop_reason": stop_reason,
                    }
                )
            )
        continue

    # Loop exhausted / LLM error. Return best-effort result.
    if isinstance(state.plan_obj, dict):
        try:
            plan_model = PipelinePlan.model_validate(state.plan_obj)
        except Exception as exc:
            return {
                "run_id": run_id,
                "status": "failed",
                "plan_id": plan_id,
                "plan": None,
                "preview": None,
                "report": state.null_report,
                "questions": [],
                "validation_errors": tool_errors or [str(exc)],
                "validation_warnings": tool_warnings,
                "llm": (
                    {
                        "provider": llm_meta.provider,
                        "model": llm_meta.model,
                        "cache_hit": llm_meta.cache_hit,
                        "latency_ms": llm_meta.latency_ms,
                    }
                    if llm_meta
                    else None
                ),
            }

        validation = await validate_pipeline_plan(
            plan=plan_model,
            dataset_registry=dataset_registry,
            db_name=str(plan_model.data_scope.db_name or ""),
            branch=str(plan_model.data_scope.branch or "") or None,
            # If we have a plan, prefer treating "valid + has output" as success even if the
            # model didn't explicitly emit action=finish (loop exhaustion, etc).
            require_output=True,
            context_pack=state.context_pack,
        )
        final_status = "success" if (not validation.errors and not tool_errors) else "partial"
        if not plan_id:
            plan_id = str(uuid4())
        if persist_plan:
            await plan_registry.upsert_plan(
                plan_id=plan_id,
                tenant_id=tenant_id,
                status="DRAFT" if validation.errors else "COMPILED",
                goal=str(validation.plan.goal or ""),
                db_name=str(validation.plan.data_scope.db_name or ""),
                branch=str(validation.plan.data_scope.branch or "") or None,
                plan=validation.plan.model_dump(mode="json"),
                created_by=actor,
            )
        validation_errors_out = list(validation.errors or [])
        if not validation_errors_out and tool_errors:
            validation_errors_out = tool_errors[:1]
        return {
            "run_id": run_id,
            "status": final_status,
            "plan_id": plan_id,
            "plan": validation.plan.model_dump(mode="json"),
            "preview": None,
            "report": state.null_report,
            "questions": [],
            "validation_errors": validation_errors_out,
            "validation_warnings": list(tool_warnings) + list(validation.warnings or []),
            "preflight": validation.preflight,
            "planner": {"confidence": None, "notes": notes or None},
            "llm": (
                {
                    "provider": llm_meta.provider,
                    "model": llm_meta.model,
                    "cache_hit": llm_meta.cache_hit,
                    "latency_ms": llm_meta.latency_ms,
                }
                if llm_meta
                else None
            ),
        }

    return {
        "run_id": run_id,
        "status": "failed",
        "plan_id": plan_id,
        "plan": None,
        "preview": None,
        "report": state.null_report,
        "questions": [],
        "validation_errors": tool_errors or ["pipeline agent did not complete"],
        "validation_warnings": tool_warnings,
        "planner": {"confidence": None, "notes": notes or None},
        "llm": (
            {
                "provider": llm_meta.provider,
                "model": llm_meta.model,
                "cache_hit": llm_meta.cache_hit,
                "latency_ms": llm_meta.latency_ms,
            }
            if llm_meta
            else None
        ),
    }
