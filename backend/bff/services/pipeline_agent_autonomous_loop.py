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
from shared.services.core.audit_log_store import AuditLogStore
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.agent.llm_gateway import (
    LLMCallMeta,
    LLMGateway,
    LLMOutputValidationError,
    LLMRequestError,
    LLMUnavailableError,
)
from shared.services.agent.llm_quota import enforce_llm_quota
from shared.services.registries.pipeline_plan_registry import PipelinePlanRegistry
from shared.services.storage.redis_service import RedisService
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
    # Keep this high enough for complex ETL edits, but low enough to avoid truncated/invalid JSON
    # responses from the LLM (which can end the run prematurely).
    # NOTE: Do not hard-cap the list length here. If the model emits too many tool calls, the
    # runner will deterministically execute only the first N (see max_tool_calls_per_step).
    tool_calls: List[AutonomousPipelineAgentToolCall] = Field(default_factory=list)
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


def _tool_alias(tool_name: str) -> str:
    """
    Convert a tool name into a short alias for intra-batch reference resolution.

    Example:
      plan_add_group_by_expr -> group_by_expr
      pipeline_build_wait    -> build_wait
      ontology_add_property  -> add_property
    """
    name = str(tool_name or "").strip()
    for prefix in ("plan_add_", "plan_update_", "plan_", "pipeline_", "ontology_"):
        if name.startswith(prefix):
            return name[len(prefix) :]
    return name


def _resolve_ref_path(value: Any, path: List[str]) -> Any:
    cur = value
    for key in path:
        if isinstance(cur, dict) and key in cur:
            cur = cur[key]
            continue
        raise KeyError(key)
    return cur


def _drop_none_values(value: Any) -> Any:
    """
    MCP tools validate inputs via JSON schema where `null` is frequently invalid for optional
    string fields (e.g., {"branch": null} fails when the schema says {"type":"string"}).

    Use explicit `unset` semantics for plan patching instead of passing nulls.
    """
    if isinstance(value, dict):
        return {k: _drop_none_values(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_drop_none_values(v) for v in value if v is not None]
    return value


def _resolve_batch_placeholders(value: Any, *, last: Optional[Dict[str, Any]], last_by_alias: Dict[str, Dict[str, Any]]) -> Any:
    """
    Resolve lightweight placeholders inside a *single batch* of tool calls.

    Supported forms (strings only, must start with '$'):
      - $last.<field>                      -> from the most recent tool output in the batch
      - $last.<tool_alias>.<field>         -> from the most recent output for that alias

    This enables batched tool calls where later calls need IDs returned by earlier calls
    without forcing extra LLM round-trips.
    """
    if isinstance(value, dict):
        return {k: _resolve_batch_placeholders(v, last=last, last_by_alias=last_by_alias) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_batch_placeholders(v, last=last, last_by_alias=last_by_alias) for v in value]
    if not isinstance(value, str):
        return value
    if not value.startswith("$"):
        return value

    # Only treat *entire-string* placeholders as refs to avoid mutating Spark SQL expressions.
    raw = value.strip()
    if not raw.startswith("$last."):
        return value

    parts = raw.split(".")
    # parts[0] == "$last"
    if len(parts) < 2:
        return value

    base: Optional[Dict[str, Any]] = None
    path: List[str] = []
    if len(parts) == 2:
        base = last
        path = [parts[1]]
    else:
        alias = parts[1]
        base = last_by_alias.get(alias)
        path = parts[2:]
    if base is None:
        raise KeyError(raw)
    return _resolve_ref_path(base, path)


@dataclass
class _AgentState:
    db_name: str
    branch: Optional[str]
    dataset_ids: List[str]
    goal: str
    principal_id: str
    principal_type: str
    pipeline_id: Optional[str] = None
    last_build_job_id: Optional[str] = None
    last_build_artifact_id: Optional[str] = None
    # Pipeline execution progress (Spark worker). Keyed by output node_id where applicable.
    pipeline_progress: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    pipeline_events: List[Dict[str, Any]] = field(default_factory=list)

    # Deterministic (tool-produced) context - simplified (context_pack removed).
    null_report: Optional[Dict[str, Any]] = None
    key_inference: Optional[Dict[str, Any]] = None
    type_inference: Optional[Dict[str, Any]] = None
    join_plan: Optional[List[Dict[str, Any]]] = None

    # Plan (optional): created only when the LLM explicitly calls plan_new.
    plan_obj: Optional[Dict[str, Any]] = None
    last_observation: Optional[Dict[str, Any]] = None
    # Append-only JSONL prompt log to enable provider-side prefix caching.
    prompt_items: List[str] = field(default_factory=list)

    # Ontology state (integrated from ontology agent)
    ontology_session_id: Optional[str] = None
    working_ontology: Optional[Dict[str, Any]] = None
    schema_inference: Optional[Dict[str, Any]] = None
    mapping_suggestions: Optional[Dict[str, Any]] = None


_PIPELINE_AGENT_ALLOWED_TOOLS: tuple[str, ...] = (
    # Plan builder (mutating, but in-memory only)
    "plan_new",
    "plan_reset",
    "plan_add_input",
    "plan_add_external_input",
    "plan_add_join",
    "plan_add_transform",
    "plan_add_sort",
    "plan_add_explode",
    "plan_add_union",
    "plan_add_pivot",
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
    "plan_update_settings",
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
    # ==================== Ontology Tools (integrated) ====================
    # Initialization
    "ontology_new",
    "ontology_load",
    "ontology_reset",
    # Class metadata
    "ontology_set_class_meta",
    "ontology_set_abstract",
    # Property management
    "ontology_add_property",
    "ontology_update_property",
    "ontology_remove_property",
    "ontology_set_primary_key",
    # Relationship management
    "ontology_add_relationship",
    "ontology_update_relationship",
    "ontology_remove_relationship",
    # Schema inference
    "ontology_infer_schema_from_data",
    "ontology_suggest_mappings",
    # Validation
    "ontology_validate",
    "ontology_check_relationships",
    "ontology_check_circular_refs",
    # Query
    "ontology_list_classes",
    "ontology_get_class",
    "ontology_search_classes",
    # Save
    "ontology_create",
    "ontology_update",
    "ontology_preview",
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


def _summarize_pipeline_progress(state: _AgentState) -> Dict[str, Any]:
    if not state.pipeline_id and not state.pipeline_progress:
        return {}
    nodes_out: List[Dict[str, Any]] = []
    for node_id, prog in list(state.pipeline_progress.items())[:20]:
        if not isinstance(prog, dict):
            continue

        def _stage(name: str) -> Optional[Dict[str, Any]]:
            item = prog.get(name)
            if not isinstance(item, dict):
                return None
            out: Dict[str, Any] = {}
            for key in ("status", "job_id", "build_job_id", "artifact_id", "dataset_name", "deployed_commit_id", "code"):
                if key in item and item.get(key) is not None:
                    out[key] = item.get(key)
            return out or None

        entry: Dict[str, Any] = {"node_id": node_id}
        for stage_name in ("preview", "build", "deploy"):
            stage = _stage(stage_name)
            if stage:
                entry[stage_name] = stage
        nodes_out.append(entry)

    return {
        "pipeline_id": state.pipeline_id,
        "last_build_job_id": state.last_build_job_id,
        "last_build_artifact_id": state.last_build_artifact_id,
        "nodes": nodes_out,
        "recent_events": state.pipeline_events[-10:],
    }


def _record_pipeline_event(*, state: _AgentState, tool_name: str, args: Dict[str, Any], observation: Dict[str, Any]) -> None:
    status_value = str(observation.get("status") or "").strip().lower() or None
    node_id = str(args.get("node_id") or "").strip() or None
    dataset_name = str(args.get("dataset_name") or "").strip() or None
    build_job_id = str(args.get("build_job_id") or "").strip() or None

    event: Dict[str, Any] = {
        "tool": tool_name,
        "status": status_value,
        "pipeline_id": state.pipeline_id,
    }
    if node_id:
        event["node_id"] = node_id
    if dataset_name:
        event["dataset_name"] = dataset_name
    if build_job_id:
        event["build_job_id"] = build_job_id
    for key in ("job_id", "artifact_id", "deployed_commit_id", "code"):
        if observation.get(key) is not None:
            event[key] = observation.get(key)
    if observation.get("error"):
        event["error"] = observation.get("error")
    if isinstance(observation.get("errors"), list) and observation.get("errors"):
        event["errors"] = list(observation.get("errors") or [])[:10]
    if isinstance(observation.get("warnings"), list) and observation.get("warnings"):
        event["warnings"] = list(observation.get("warnings") or [])[:10]

    state.pipeline_events.append(_mask_tool_observation(event))
    # Keep history bounded to avoid prompt bloat.
    if len(state.pipeline_events) > 80:
        state.pipeline_events = state.pipeline_events[-40:]

    if not node_id:
        # pipeline_build_wait can return output info for multiple output nodes even when node_id was omitted.
        if tool_name == "pipeline_build_wait":
            out = observation.get("output") if isinstance(observation.get("output"), dict) else None
            items = out.get("outputs") if isinstance(out, dict) and isinstance(out.get("outputs"), list) else []
            for item in items:
                if not isinstance(item, dict):
                    continue
                nid = str(item.get("node_id") or item.get("nodeId") or "").strip()
                if not nid:
                    continue
                prog = state.pipeline_progress.get(nid)
                if not isinstance(prog, dict):
                    prog = {}
                    state.pipeline_progress[nid] = prog
                prog["build"] = {
                    "tool": tool_name,
                    "status": status_value,
                    "pipeline_id": state.pipeline_id,
                    "node_id": nid,
                    "job_id": observation.get("job_id"),
                    "artifact_id": observation.get("artifact_id"),
                    "dataset_name": item.get("dataset_name") or item.get("datasetName"),
                }
        return

    prog = state.pipeline_progress.get(node_id)
    if not isinstance(prog, dict):
        prog = {}
        state.pipeline_progress[node_id] = prog

    stage: Optional[str] = None
    if tool_name == "pipeline_preview_wait":
        stage = "preview"
    elif tool_name == "pipeline_build_wait":
        stage = "build"
    elif tool_name == "pipeline_deploy_promote_build":
        stage = "deploy"
    if stage:
        prog[stage] = event


def _pipeline_has_unresolved_status(state: _AgentState) -> Optional[Dict[str, Any]]:
    """
    If pipeline execution was attempted, ensure we don't incorrectly report success while
    preview/build/deploy are pending or failed.
    """
    if not state.pipeline_progress:
        return None

    for node_id, prog in state.pipeline_progress.items():
        if not isinstance(prog, dict):
            continue
        for stage in ("preview", "build", "deploy"):
            item = prog.get(stage)
            if not isinstance(item, dict):
                continue
            if item.get("error") or (isinstance(item.get("errors"), list) and item.get("errors")):
                return {"issue": "pipeline_stage_error", "node_id": node_id, "stage": stage}
            status_value = str(item.get("status") or "").strip().lower()
            if status_value and status_value != "success":
                return {"issue": "pipeline_stage_not_success", "node_id": node_id, "stage": stage, "status": status_value}
    return None


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
        "You can iteratively call tools to inspect datasets (profiling/nulls/keys/types), build pipeline plans, and create/modify ontology schemas.\n"
        "The user prompt is an append-only JSONL log (one JSON object per line). The newest lines are the most recent state/observations.\n"
        "\n"
        "Core rules:\n"
        "- Return ONLY JSON matching the schema. No markdown, no extra text.\n"
        "- Do NOT invent data; prefer tool observations over guessing.\n"
        "- Tool args MUST NOT include raw `plan` objects; the server stores them.\n"
        "- Respect scope: if the user asked ONLY for analysis (e.g., null check), do NOT build a plan.\n"
        "- If planner_hints.require_plan=true, you MUST build and validate a plan (do NOT finish with report-only).\n"
        "- If the user asked for a derived dataset/result, build a plan via plan_* tools and ensure it validates.\n"
        "- No silent server-side rewrites exist; any plan changes must be explicit tool calls.\n"
        "- Latency: prefer batching multiple tool calls in a single response via `tool_calls` (target 8-12; max 20 executed per step).\n"
        "- Keep `tool_calls` batches modest (8-12) to avoid truncated/invalid JSON.\n"
        "- If you output >20 tool_calls, the runtime will execute only the first 20 and report the rest as skipped.\n"
        "- Always use deterministic `node_id` when adding nodes (plan_add_input/compute/join/groupBy/window/output),\n"
        "  so later edits don't depend on auto-generated ids like input_3.\n"
        "- If you need to restart planning from scratch, call plan_new again (it replaces any existing plan and clears pipeline state).\n"
        "- In batched tool calls, you MAY reference earlier tool outputs using placeholders (entire string only):\n"
        "  - $last.<field>\n"
        "  - $last.<tool_alias>.<field>\n"
        "  Where tool_alias is the tool name without common prefixes (e.g., plan_add_group_by_expr -> group_by_expr).\n"
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
        "Pipeline patterns:\n"
        "- join: plan_add_join(left_node_id,right_node_id,left_keys=[...],right_keys=[...],join_type='left|inner')\n"
        "- union: plan_add_union(left_node_id,right_node_id,union_mode='strict|common_only|pad')\n"
        "- ingest permissive: plan_configure_input_read(node_id, mode='PERMISSIVE', corrupt_record_column='_corrupt_record')\n"
        "- external input (jdbc/kafka): plan_add_external_input(read={\"format\":\"jdbc\",\"options\":{...},\"options_env\":{...}})\n"
        "- compute: plan_add_compute_column(input_node_id, target_column=\"revenue\", formula=\"qty * unit_price\")\n"
        "- compute many: plan_add_compute_assignments(input_node_id, assignments=[{\"column\":\"x\",\"expression\":\"...\"}, ...])\n"
        "- select expr: plan_add_select_expr(input_node_id, expressions=[\"col\", \"sum(price) as total\"])  # Spark selectExpr\n"
        "- group by / aggregate: plan_add_group_by(input_node_id, group_by=[...], aggregates=[{\"column\":\"price\",\"op\":\"sum\",\"alias\":\"total\"}])\n"
        "- group by expr: plan_add_group_by_expr(input_node_id, group_by=[...], aggregate_expressions=[\"approx_percentile(price, 0.5) as p50\", ...])\n"
        "- window expr: plan_add_window_expr(input_node_id, expressions=[{\"column\":\"rn\",\"expr\":\"row_number() over (partition by k order by ts desc)\"}])\n"
        "- sort: plan_add_sort(input_node_id, columns=[\"-total\", \"customer_id\"])  # prefix '-' for DESC\n"
        "- explode: plan_add_explode(input_node_id, column=\"items\")\n"
        "- pivot: plan_add_pivot(input_node_id, index=[\"customer_id\"], columns=\"category\", values=\"amount\", agg=\"sum\")\n"
        "- top-N: plan_add_filter(input_node_id, expression=\"row_number <= N\")\n"
        "- spark conf / cast mode: plan_update_settings(set={\"spark_conf\": {\"spark.sql.ansi.enabled\":\"true\"}, \"cast_mode\":\"STRICT\"})\n"
        "- patch node metadata: plan_update_node_metadata(node_id=\"...\", set={...})\n"
        "- output: plan_add_output(input_node_id, output_name=\"result\")\n"
        "- refute claims: plan_refute_claims()  # optional; server will also run it on finish when a plan exists\n"
        "- materialize pipeline: pipeline_create_from_plan(name=\"...\", location=\"team/...\"), then pipeline_preview_wait(...), pipeline_build_wait(...)\n"
        "- deploy from build: pipeline_deploy_promote_build(pipeline_id, build_job_id, node_id, db_name, dataset_name)  # requires approve\n"
        "  - If deploy returns status='replay_required', retry with replay_on_deploy=true OR change dataset_name.\n"
        "  (runtime convenience: after a successful create/build in this run, omitting pipeline_id/build_job_id will use the latest values)\n"
        "\n"
        "Ontology patterns (use ontology_session_id from header):\n"
        "- ontology_new: class_id (required), label (required), description (optional)\n"
        "- ontology_add_property: name (required), type (required, e.g. xsd:string, xsd:integer, xsd:dateTime), label (required), required, primary_key, title_key\n"
        "- ontology_add_relationship: predicate (required), target (required), label (required), cardinality (1:1, 1:n, n:1, n:m)\n"
        "- ontology_infer_schema_from_data: columns (required, string[]), data (required, array of arrays)\n"
        "- Create new class: ontology_new -> ontology_add_property (multiple) -> ontology_validate -> ontology_preview -> finish\n"
        "- Modify existing class: ontology_load -> ontology_add_property/ontology_update_property -> ontology_validate -> ontology_update\n"
        "- Do NOT call ontology_create/ontology_update unless explicitly asked to save to database.\n"
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
    header: Dict[str, Any] = {
        "type": "header",
        "goal": state.goal,
        "data_scope": {"db_name": state.db_name, "branch": state.branch, "dataset_ids": state.dataset_ids},
        "answers": answers or None,
        "planner_hints": planner_hints or None,
        "task_spec": task_spec or None,
    }
    # Include ontology session_id for ontology tools
    if state.ontology_session_id:
        header["ontology_session_id"] = state.ontology_session_id
    return header


def _summarize_ontology(ontology: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Create a summary of the working ontology for compaction."""
    if not isinstance(ontology, dict):
        return None
    return {
        "id": ontology.get("id"),
        "label": ontology.get("label"),
        "parent_class": ontology.get("parent_class"),
        "abstract": ontology.get("abstract", False),
        "property_count": len(ontology.get("properties") or []),
        "relationship_count": len(ontology.get("relationships") or []),
        "properties": [
            {"name": p.get("name"), "type": p.get("type"), "primary_key": p.get("primary_key", False)}
            for p in (ontology.get("properties") or [])[:20]
        ],
        "relationships": [
            {"predicate": r.get("predicate"), "target": r.get("target")}
            for r in (ontology.get("relationships") or [])[:10]
        ],
    }


def _build_compaction_snapshot(
    *,
    state: _AgentState,
    answers: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
    task_spec: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    # Deterministic compaction snapshot: derived from server-side state, not from model text.
    snapshot: Dict[str, Any] = {
        "goal": state.goal,
        "data_scope": {"db_name": state.db_name, "branch": state.branch, "dataset_ids": state.dataset_ids},
        "answers": answers or None,
        "planner_hints": planner_hints or None,
        "task_spec": task_spec or None,
        "null_report": _trim_null_report(state.null_report),
        "key_inference": _trim_key_inference(state.key_inference),
        "type_inference": _trim_type_inference(state.type_inference),
        "join_plan": _trim_join_plan(state.join_plan),
        "plan_status": _plan_status(state.plan_obj),
        "plan_summary": _summarize_plan(state.plan_obj),
        "last_observation": state.last_observation,
    }
    # Include ontology state if present
    if state.ontology_session_id:
        snapshot["ontology_session_id"] = state.ontology_session_id
    if state.working_ontology:
        snapshot["ontology_summary"] = _summarize_ontology(state.working_ontology)
    if state.schema_inference:
        snapshot["has_schema_inference"] = True
    if state.mapping_suggestions:
        snapshot["has_mapping_suggestions"] = True
    return snapshot


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

    # Check if this is an ontology-only task (no datasets needed)
    goal_lower = str(goal or "").lower()
    is_ontology_only_task = (
        not dataset_ids
        and any(kw in goal_lower for kw in (
            "ontology", "온톨로지", "클래스", "class", "스키마", "schema",
            "property", "속성", "relationship", "관계",
        ))
    )

    # Require dataset_ids only for pipeline tasks (not ontology-only)
    if not dataset_ids and not is_ontology_only_task:
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
            from mcp_servers.mcp_client import get_mcp_manager  # type: ignore[import-not-found]
        except Exception:  # pragma: no cover
            from backend.mcp_servers.mcp_client import get_mcp_manager  # type: ignore[import-not-found]
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

    async def _call_ontology_tool(tool: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Call an ontology MCP tool."""
        payload = await mcp_manager.call_tool("ontology", tool, arguments)
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
    ontology_session_id = f"ontology_{run_id}"
    state = _AgentState(
        db_name=db_name,
        branch=str(data_scope.branch or "").strip() or None,
        dataset_ids=dataset_ids,
        goal=str(goal or "").strip(),
        principal_id=principal_id,
        principal_type=principal_type,
        ontology_session_id=ontology_session_id,
    )

    # Scale iteration budget by dataset count (multi-way joins take more tool calls).
    max_steps = min(60, 24 + max(0, len(state.dataset_ids) - 1) * 8)
    llm_meta: Optional[LLMCallMeta] = None
    notes: List[str] = []
    tool_warnings: List[str] = []
    tool_errors: List[str] = []
    consecutive_llm_failures = 0

    allowed_tools = list(_PIPELINE_AGENT_ALLOWED_TOOLS)
    system_prompt = _build_system_prompt(allowed_tools=allowed_tools)
    max_tool_calls_per_step = 20
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
            args = _drop_none_values(dict(args or {}))

            if tool_name == "plan_new":
                # Treat plan_new as "start over" even if a plan already exists.
                # Models often re-issue plan_new to restart planning. If we noop here,
                # subsequent plan_add_* calls can collide on deterministic node_ids and
                # permanently wedge the run. Resetting is deterministic and semantics-preserving.
                had_plan = isinstance(state.plan_obj, dict)
                scoped: Dict[str, Any] = {
                    "goal": state.goal,
                    "db_name": state.db_name,
                    "dataset_ids": state.dataset_ids,
                }
                if state.branch:
                    scoped["branch"] = state.branch
                payload = await _call_pipeline_tool(
                    tool_name,
                    scoped,
                )
                plan = payload.get("plan") if isinstance(payload, dict) else None
                state.plan_obj = plan if isinstance(plan, dict) else None
                if had_plan or state.pipeline_id:
                    # Starting a new plan implies a new pipeline lifecycle in this run.
                    state.pipeline_id = None
                    state.last_build_job_id = None
                    state.last_build_artifact_id = None
                    state.pipeline_progress = {}
                    state.pipeline_events = []
                observation = dict(payload) if isinstance(payload, dict) else {"result": payload}
                observation.pop("plan", None)
                state.last_observation = _mask_tool_observation(
                    {
                        **observation,
                        "plan_status": _plan_status(state.plan_obj),
                        "plan_replaced": bool(had_plan),
                        "next_suggested_tools": ["plan_add_input", "plan_add_join", "plan_add_transform", "plan_add_output"],
                    }
                )
                return state.last_observation

            if tool_name == "plan_reset":
                if not isinstance(state.plan_obj, dict):
                    state.last_observation = {"error": "plan is not initialized; call plan_new first"}
                    return state.last_observation
                payload = await _call_pipeline_tool(
                    tool_name,
                    {"plan": state.plan_obj},
                )
                plan = payload.get("plan") if isinstance(payload, dict) else None
                state.plan_obj = plan if isinstance(plan, dict) else None
                observation = dict(payload) if isinstance(payload, dict) else {"result": payload}
                observation.pop("plan", None)
                state.last_observation = _mask_tool_observation({**observation, "plan_status": _plan_status(state.plan_obj)})
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
                    # Convenience: bind pipeline execution tools to the most recently-created pipeline/build
                    # in this run when the model omits identifiers.
                    pipeline_id_arg = str(args.get("pipeline_id") or "").strip()
                    if not pipeline_id_arg and state.pipeline_id:
                        args["pipeline_id"] = state.pipeline_id
                    if tool_name == "pipeline_deploy_promote_build":
                        build_job_id_arg = str(args.get("build_job_id") or "").strip()
                        if not build_job_id_arg and state.last_build_job_id:
                            args["build_job_id"] = state.last_build_job_id
                        artifact_id_arg = str(args.get("artifact_id") or "").strip()
                        if not artifact_id_arg and state.last_build_artifact_id:
                            args["artifact_id"] = state.last_build_artifact_id
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
                    if tool_name == "pipeline_build_wait":
                        job_id = str(payload.get("job_id") or "").strip()
                        if job_id:
                            state.last_build_job_id = job_id
                        artifact_id = str(payload.get("artifact_id") or "").strip()
                        if artifact_id:
                            state.last_build_artifact_id = artifact_id
                    _record_pipeline_event(state=state, tool_name=tool_name, args=args, observation=payload)

                # Keep observations small; do not embed definition_json.
                state.last_observation = _mask_tool_observation(payload if isinstance(payload, dict) else {"result": payload})
                return state.last_observation

            # ==================== Ontology Tools ====================
            if tool_name.startswith("ontology_"):
                # Always inject session_id for ontology tools
                if "session_id" not in args:
                    args["session_id"] = state.ontology_session_id

                # For tools that need db_name/branch, inject if not provided
                if tool_name in {
                    "ontology_load", "ontology_list_classes", "ontology_get_class",
                    "ontology_search_classes", "ontology_create", "ontology_update",
                    "ontology_check_relationships", "ontology_check_circular_refs",
                    "ontology_suggest_mappings",
                }:
                    if "db_name" not in args or not args["db_name"]:
                        args["db_name"] = state.db_name
                    if "branch" not in args or not args["branch"]:
                        args["branch"] = state.branch or "main"

                payload = await _call_ontology_tool(tool_name, args)

                # Track working ontology from preview
                if tool_name == "ontology_preview" and isinstance(payload, dict):
                    ont = payload.get("ontology")
                    if isinstance(ont, dict):
                        state.working_ontology = ont

                # Track schema inference results
                if tool_name == "ontology_infer_schema_from_data" and isinstance(payload, dict):
                    state.schema_inference = payload

                # Track mapping suggestions
                if tool_name == "ontology_suggest_mappings" and isinstance(payload, dict):
                    state.mapping_suggestions = payload

                state.last_observation = _mask_tool_observation(
                    payload if isinstance(payload, dict) else {"result": payload}
                )
                return state.last_observation

            # Plan tools require a plan.
            if not isinstance(state.plan_obj, dict):
                state.last_observation = {"error": "plan is not initialized; call plan_new first"}
                return state.last_observation

            if tool_name == "preview_inspect" and not isinstance(args.get("preview"), dict):
                # Convenience: let the model inspect the most recent preview without threading it through.
                last = state.last_observation if isinstance(state.last_observation, dict) else {}
                preview = last.get("preview") if isinstance(last.get("preview"), dict) else None
                if isinstance(preview, dict):
                    args["preview"] = preview
                else:
                    state.last_observation = {"error": "preview_inspect requires preview; call plan_preview or pipeline_preview_wait first"}
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
                "has_null_report": isinstance(state.null_report, dict),
                "has_key_inference": isinstance(state.key_inference, dict),
                "has_type_inference": isinstance(state.type_inference, dict),
                "has_join_plan": isinstance(state.join_plan, list),
            },
            "pipeline_status": _summarize_pipeline_progress(state),
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
            consecutive_llm_failures = 0
        except LLMOutputValidationError as exc:
            # Do not abort the run on a single malformed JSON response. Treat it as an observation and
            # let the model try again with a smaller/cleaner response.
            consecutive_llm_failures += 1
            tool_errors.append(str(exc))
            state.last_observation = {
                "error": "llm_output_invalid_json",
                "detail": str(exc),
                "hint": "Return ONLY valid JSON. Keep tool_calls <= 20 and avoid huge payloads.",
            }
            state.prompt_items.append(
                stable_json_dumps(
                    {
                        "type": "llm_error",
                        "step": step_idx + 1,
                        "error_kind": "output_validation",
                        "detail": str(exc)[:500],
                    }
                )
            )
            if consecutive_llm_failures >= 3:
                break
            continue
        except LLMRequestError as exc:
            # Transient upstream errors/timeouts: allow a couple of retries inside the loop so
            # the UI doesn't get a hard failure for a single 5xx.
            consecutive_llm_failures += 1
            tool_errors.append(str(exc))
            state.last_observation = {"error": "llm_request_failed", "detail": str(exc)}
            state.prompt_items.append(
                stable_json_dumps(
                    {
                        "type": "llm_error",
                        "step": step_idx + 1,
                        "error_kind": "request_error",
                        "detail": str(exc)[:500],
                    }
                )
            )
            if consecutive_llm_failures >= 3:
                break
            continue
        except LLMUnavailableError as exc:
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

                # If the model attempted pipeline execution, do not allow finishing while
                # Spark preview/build/deploy are pending or failed.
                pipeline_issue = _pipeline_has_unresolved_status(state)
                if pipeline_issue:
                    state.last_observation = {
                        "error": "cannot finish: pipeline execution is not successful yet",
                        "pipeline_issue": pipeline_issue,
                        "pipeline_status": _summarize_pipeline_progress(state),
                    }
                    continue

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
                    "pipeline_id": state.pipeline_id,
                    "last_build_job_id": state.last_build_job_id,
                    "last_build_artifact_id": state.last_build_artifact_id,
                    "pipeline_status": _summarize_pipeline_progress(state),
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
        batch_last: Optional[Dict[str, Any]] = None
        batch_last_by_alias: Dict[str, Dict[str, Any]] = {}
        truncated_tool_calls = max(0, len(tool_calls) - max_tool_calls_per_step)
        for call in tool_calls[:max_tool_calls_per_step]:
            tool_name = str(call.tool or "").strip()
            try:
                args = _resolve_batch_placeholders(
                    dict(call.args or {}),
                    last=batch_last,
                    last_by_alias=batch_last_by_alias,
                )
            except Exception as exc:
                state.last_observation = {"error": f"failed to resolve batch tool refs for {tool_name}: {exc}"}
                stop_reason = "bad_batch_ref"
                break
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
            batch_last = observation
            batch_last_by_alias[_tool_alias(tool_name)] = observation
            errors = observation.get("errors")
            if observation.get("error"):
                logger.info(
                    "pipeline agent batch stopping: tool_error tool=%s step=%s observation=%s",
                    tool_name,
                    step_idx + 1,
                    _mask_tool_observation(dict(observation)),
                )
                stop_reason = "tool_error"
                break
            if observation.get("status") == "invalid":
                preflight_blocking = None
                preflight_obj = observation.get("preflight")
                if isinstance(preflight_obj, dict):
                    blocking = preflight_obj.get("blocking_errors")
                    if isinstance(blocking, list) and blocking:
                        # Log a small, PII-masked witness list to make invalid preflight debuggable.
                        preflight_blocking = _mask_tool_observation({"blocking_errors": blocking[:5]})
                logger.info(
                    "pipeline agent batch stopping: invalid_status tool=%s step=%s errors=%s warnings=%s preflight_blocking=%s",
                    tool_name,
                    step_idx + 1,
                    (observation.get("errors") if isinstance(observation.get("errors"), list) else None),
                    (observation.get("warnings") if isinstance(observation.get("warnings"), list) else None),
                    preflight_blocking,
                )
                stop_reason = "invalid_status"
                break
            if tool_name.startswith("pipeline_"):
                # Pipeline execution tools can be long-running and stateful (queued/running).
                # If a pipeline tool doesn't report success, stop the batch so the model can react
                # (wait/poll/repair) instead of blindly continuing with dependent actions.
                status_value = str(observation.get("status") or "").strip().lower()
                if status_value and status_value != "success":
                    logger.info(
                        "pipeline agent batch stopping: pipeline_status tool=%s step=%s status=%s",
                        tool_name,
                        step_idx + 1,
                        status_value,
                    )
                    stop_reason = f"pipeline_status_{status_value}"
                    break
            if isinstance(errors, list) and errors:
                logger.info(
                    "pipeline agent batch stopping: validation_errors tool=%s step=%s errors=%s",
                    tool_name,
                    step_idx + 1,
                    errors[:20],
                )
                stop_reason = "validation_errors"
                break

        if executed_tools:
            augmented = dict(state.last_observation or {})
            augmented["executed_tools"] = executed_tools
            augmented["executed_tool_count"] = len(executed_tools)
            if truncated_tool_calls:
                augmented["tool_calls_truncated"] = truncated_tool_calls
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
        )
        pipeline_issue = _pipeline_has_unresolved_status(state)
        final_status = "success" if (not validation.errors and not tool_errors and not pipeline_issue) else "partial"
        if pipeline_issue and isinstance(pipeline_issue, dict):
            status_value = str(pipeline_issue.get("status") or "").strip().lower()
            if pipeline_issue.get("issue") == "pipeline_stage_error" or status_value in {"failed", "timeout", "conflict", "invalid"}:
                final_status = "failed"
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
        if pipeline_issue and not validation_errors_out:
            validation_errors_out = [f"pipeline execution incomplete ({pipeline_issue})"]
        return {
            "run_id": run_id,
            "status": final_status,
            "plan_id": plan_id,
            "plan": validation.plan.model_dump(mode="json"),
            "preview": None,
            "report": state.null_report,
            "pipeline_id": state.pipeline_id,
            "last_build_job_id": state.last_build_job_id,
            "last_build_artifact_id": state.last_build_artifact_id,
            "pipeline_status": _summarize_pipeline_progress(state),
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
