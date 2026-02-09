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
from shared.services.storage.event_store import EventStore
from shared.utils.llm_safety import mask_pii, stable_json_dumps
from shared.config.model_context_limits import PromptBudget, get_model_context_config

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
    action: Literal["call_tool", "finish", "clarify", "respond"] = Field(default="call_tool")
    # Chain-of-thought reasoning (internal, not shown to user directly).
    reasoning: Optional[str] = Field(default=None, max_length=4000)
    # Natural language message to the user (shown directly in chat).
    message: Optional[str] = Field(default=None, max_length=4000)
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
        if self.action == "respond":
            if not str(self.message or "").strip():
                raise ValueError("message is required when action=respond")
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

    # Enterprise context management
    knowledge_ledger: Dict[str, Any] = field(default_factory=dict)
    tool_call_hashes: Dict[str, int] = field(default_factory=dict)  # hash(tool+args) → step_idx


def _build_agent_context_snapshot(state: _AgentState) -> Dict[str, Any]:
    """Serialize key agent state for persistence across sessions (clarification resume)."""
    ctx: Dict[str, Any] = {}
    if state.knowledge_ledger:
        ctx["knowledge_ledger"] = state.knowledge_ledger
    if state.null_report:
        ctx["null_report"] = state.null_report
    if state.key_inference:
        ctx["key_inference"] = state.key_inference
    if state.type_inference:
        ctx["type_inference"] = state.type_inference
    if isinstance(state.join_plan, list):
        ctx["join_plan"] = state.join_plan
    if state.ontology_session_id:
        ctx["ontology_session_id"] = state.ontology_session_id
    if state.working_ontology:
        ctx["working_ontology"] = state.working_ontology
    if state.schema_inference:
        ctx["schema_inference"] = state.schema_inference
    if state.mapping_suggestions:
        ctx["mapping_suggestions"] = state.mapping_suggestions
    if state.tool_call_hashes:
        ctx["tool_call_hashes"] = {str(k): v for k, v in state.tool_call_hashes.items()}
    return ctx


# ==================== Trimming Constants ====================
# Consistent limits for tool response trimming to prevent context overflow
AGENT_TRIM_PREVIEW_ROWS = 10  # Rows in preview results
AGENT_TRIM_EVALUATIONS = 10  # Max evaluations to return
AGENT_TRIM_WARNINGS = 30  # Max warnings to return
AGENT_TRIM_ERRORS = 30  # Max errors to return
AGENT_TRIM_EVENTS = 50  # Max pipeline events to keep (before compaction)
AGENT_TRIM_EVENTS_COMPACT = 30  # Events to keep after compaction

# Prompt item importance scores for graduated compression (lower = pruned first)
ITEM_PRIORITY: Dict[str, int] = {
    "state": 1,
    "tool_output_ref": 1,
    "batch_summary": 2,
    "tool_call": 2,
    "tool_output": 3,
    "llm_error": 4,
    "bootstrap_observation": 5,
}


# ==================== Enterprise Context Management ====================


def _extract_knowledge(tool_name: str, observation: Dict[str, Any], ledger: Dict[str, Any]) -> None:
    """Extract key structural facts from tool results into the knowledge ledger.

    The ledger survives all compression levels, ensuring the model never forgets
    critical schema information, column names, or foreign key relationships.
    """
    if not isinstance(observation, dict) or observation.get("error"):
        return

    if tool_name == "dataset_sample":
        ds_id = str(observation.get("dataset_id") or "").strip()
        if ds_id:
            ledger.setdefault("dataset_schemas", {})[ds_id] = {
                "columns": list(observation.get("columns") or []),
                "row_count": observation.get("row_count"),
                "sample_row": (observation.get("rows") or [])[:1],
            }

    elif tool_name == "dataset_profile":
        ds_id = str(observation.get("dataset_id") or "").strip()
        if ds_id:
            cols_summary = []
            for col in (observation.get("columns") or []):
                if isinstance(col, dict):
                    cols_summary.append({
                        "name": col.get("name"),
                        "type": col.get("inferred_type") or col.get("type"),
                        "null_ratio": col.get("null_ratio"),
                        "distinct": col.get("distinct_count"),
                    })
            ledger.setdefault("dataset_profiles", {})[ds_id] = cols_summary

    elif tool_name == "dataset_list":
        datasets = []
        for ds in (observation.get("datasets") or []):
            if isinstance(ds, dict):
                datasets.append({
                    "id": ds.get("dataset_id"),
                    "name": ds.get("dataset_name"),
                    "row_count": ds.get("row_count"),
                })
        if datasets:
            ledger["available_datasets"] = datasets

    elif tool_name == "detect_foreign_keys":
        fks = observation.get("patterns")  # MCP handler returns "patterns", not "foreign_keys"
        if isinstance(fks, list) and fks:
            existing = ledger.get("foreign_keys") or []
            # Deduplicate by (source_column, target_dataset_id) for multi-call accumulation
            seen = {(p.get("source_column"), p.get("target_dataset_id"))
                    for p in existing if isinstance(p, dict)}
            for p in fks:
                key = (p.get("source_column"), p.get("target_dataset_id"))
                if key not in seen:
                    existing.append(p)
                    seen.add(key)
            ledger["foreign_keys"] = existing

    elif tool_name == "ontology_infer_schema_from_data":
        # Keep inferred types and property suggestions
        inferred = observation.get("inferred_properties") or observation.get("properties")
        if isinstance(inferred, list):
            ledger["inferred_schema"] = inferred[:50]

    elif tool_name == "plan_validate":
        # Keep validation errors/warnings so they're not lost after compaction
        errors = observation.get("errors")
        warnings = observation.get("warnings")
        if isinstance(errors, list) and errors:
            ledger["last_validation_errors"] = errors[:10]
        if isinstance(warnings, list) and warnings:
            ledger["last_validation_warnings"] = warnings[:10]


def _summarize_tool_output(tool_name: str, observation: Dict[str, Any]) -> Dict[str, Any]:
    """Summarize large tool outputs before appending to prompt_items.

    Called AFTER _extract_knowledge, so the full data is already safely stored
    in the knowledge ledger.  The summarized version goes into the prompt
    for the model to see without blowing the context budget.
    """
    if not isinstance(observation, dict):
        return observation

    if tool_name == "dataset_sample":
        rows = observation.get("rows") or []
        return {
            "status": observation.get("status"),
            "dataset_id": observation.get("dataset_id"),
            "columns": observation.get("columns"),
            "row_count": observation.get("row_count"),
            "sample_rows_shown": len(rows),
            "first_row": rows[0] if rows else None,
            "last_row": rows[-1] if len(rows) > 1 else None,
        }

    if tool_name == "dataset_profile":
        columns = observation.get("columns") or []
        return {
            "status": observation.get("status"),
            "dataset_id": observation.get("dataset_id"),
            "column_count": len(columns),
            "columns": [
                {
                    "name": c.get("name"),
                    "type": c.get("inferred_type") or c.get("type"),
                    "null_ratio": c.get("null_ratio"),
                    "distinct": c.get("distinct_count"),
                }
                for c in columns[:30]
                if isinstance(c, dict)
            ],
        }

    if tool_name == "data_query":
        rows = observation.get("rows") or []
        return {
            "status": observation.get("status"),
            "row_count": len(rows),
            "columns": observation.get("columns"),
            "first_3_rows": rows[:3],
        }

    if tool_name == "plan_preview":
        rows = observation.get("rows") or observation.get("preview") or []
        if isinstance(rows, list):
            return {
                "status": observation.get("status"),
                "row_count": len(rows),
                "columns": observation.get("columns"),
                "first_3_rows": rows[:3],
            }

    # Generic fallback: if serialized output exceeds 5K, keep only essential keys
    serialized = stable_json_dumps(observation)
    if len(serialized) > 5000:
        essential = {}
        for k in ("status", "error", "errors", "warnings", "plan", "node_id",
                   "pipeline_id", "dataset_id", "class_id", "session_id"):
            if k in observation:
                essential[k] = observation[k]
        essential["_truncated"] = True
        essential["_original_chars"] = len(serialized)
        return essential

    return observation


_DEDUP_EXEMPT_PREFIXES = ("ontology_", "objectify_")


def _deduplicate_tool_call(
    state: _AgentState, tool_name: str, args: Dict[str, Any], step_idx: int,
) -> Optional[int]:
    """Return the step index of a previous identical call, or None if unique.

    Ontology and objectify tools are exempt from dedup because they mutate
    session state and must be re-callable after resume.
    """
    if tool_name.startswith(_DEDUP_EXEMPT_PREFIXES):
        return None
    key = f"{tool_name}:{stable_json_dumps(args)}"
    h = hash(key)
    existing = state.tool_call_hashes.get(h)
    if existing is not None:
        return existing
    state.tool_call_hashes[h] = step_idx
    return None


def _dedup_evict_on_failure(
    state: _AgentState, tool_name: str, args: Dict[str, Any],
) -> None:
    """Remove a tool call from the dedup cache so the LLM can retry it.

    Called when a tool call fails (returns an error observation).  Without this,
    the LLM's subsequent retry with identical args would be treated as a
    duplicate and silently skipped — making it impossible to recover from
    transient failures (e.g. ``pipeline_create_from_plan`` failing because a
    required field was missing, then succeeding after the LLM sets that field).
    """
    key = f"{tool_name}:{stable_json_dumps(args)}"
    state.tool_call_hashes.pop(hash(key), None)


# ── Knowledge-ledger cache (Layer 2 safety net for resume) ──

_KNOWLEDGE_CACHE_MAP: Dict[str, tuple] = {
    "dataset_sample":      ("dataset_schemas",   lambda a: str(a.get("dataset_id") or "").strip()),
    "dataset_profile":     ("dataset_profiles",  lambda a: str(a.get("dataset_id") or "").strip()),
    "detect_foreign_keys": ("foreign_keys",      lambda a: str(a.get("dataset_id") or "").strip()),
    "dataset_list":        ("available_datasets", lambda a: "__all__"),
}


def _check_knowledge_cache(
    ledger: Dict[str, Any], tool_name: str, args: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Return a synthetic cached observation if the knowledge ledger already has
    the data this tool would produce.  Returns ``None`` on cache miss.

    The caller must check ``args.get("force_refresh")`` **before** calling this.
    """
    entry = _KNOWLEDGE_CACHE_MAP.get(tool_name)
    if not entry:
        return None
    section_key, key_fn = entry
    lookup_key = key_fn(args)
    if not lookup_key:
        return None
    section = ledger.get(section_key)
    if section is None:
        return None

    if tool_name == "dataset_list":
        if isinstance(section, list) and section:
            return {"status": "success", "source": "cached", "datasets": section}
        return None

    if tool_name == "detect_foreign_keys":
        if isinstance(section, list):
            return {"status": "success", "source": "cached", "patterns": section}
        return None

    # dataset_sample / dataset_profile — dict keyed by dataset_id
    if isinstance(section, dict) and lookup_key in section:
        return {"status": "success", "source": "cached", "dataset_id": lookup_key, **section[lookup_key]}
    return None


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
    # ==================== Objectify Tools (Dataset -> Ontology Instances) ====================
    "objectify_suggest_mapping",
    "objectify_create_mapping_spec",
    "objectify_list_mapping_specs",
    "objectify_run",
    "objectify_get_status",
    "objectify_wait",
    # Object type registration (REQUIRED before objectify_run)
    "ontology_register_object_type",
    # Instance query for objectify verification
    "ontology_query_instances",
    # FK detection and link type creation
    "detect_foreign_keys",
    "create_link_type_from_fk",
    # Incremental objectify
    "trigger_incremental_objectify",
    "get_objectify_watermark",
    # Dataset tools
    "dataset_get_by_name",
    "dataset_get_latest_version",
    "dataset_validate_columns",
    "dataset_list",
    "dataset_sample",
    "dataset_profile",
    "data_query",
    # ==================== Debugging Tools ====================
    "debug_get_errors",
    "debug_get_execution_log",
    "debug_inspect_node",
    "debug_explain_failure",
    "debug_dry_run",
)


def _calculate_trim_limit(total_count: int, default_limit: int, min_limit: int = 3) -> int:
    """
    Dynamically calculate trim limit based on data size.

    Rules:
    - If total <= default_limit: keep all
    - If total > default_limit: use default_limit
    - Never go below min_limit
    """
    if total_count <= default_limit:
        return total_count
    return max(min_limit, default_limit)


def _trim_null_report(report: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(report, dict):
        return None
    datasets = report.get("datasets")
    if not isinstance(datasets, list):
        datasets = []

    # Dynamic limit based on actual data size
    total = len(datasets)
    dataset_limit = _calculate_trim_limit(total, default_limit=12)
    # Adjust column limit based on dataset count
    column_limit = 30 if total <= 6 else 20 if total <= 12 else 15

    trimmed: List[Dict[str, Any]] = []
    for ds in datasets[:dataset_limit]:
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
                "columns": [c for c in cols[:column_limit] if isinstance(c, dict)],
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

    # Dynamic limits based on data size
    total_datasets = len(pk)
    pk_limit = _calculate_trim_limit(total_datasets, default_limit=12)
    fk_limit = _calculate_trim_limit(len(fk), default_limit=20)
    # Reduce candidates if many datasets
    pk_candidates_limit = 3 if total_datasets <= 5 else 2

    pk_out: List[Dict[str, Any]] = []
    for item in pk[:pk_limit]:
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
                "pk_candidates": pk_candidates[:pk_candidates_limit],
            }
        )
    fk_out = [item for item in fk[:fk_limit] if isinstance(item, dict)]
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

    # Dynamic limits based on data size
    total = len(datasets)
    ds_limit = _calculate_trim_limit(total, default_limit=12)
    mismatch_limit = 20 if total <= 5 else 15 if total <= 10 else 10
    cast_limit = _calculate_trim_limit(len(join_casts), default_limit=20)

    summarized: List[Dict[str, Any]] = []
    for ds in datasets[:ds_limit]:
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
                "mismatched_columns": mismatches[:mismatch_limit],
            }
        )
    return {
        "datasets": summarized,
        "join_key_cast_suggestions": [item for item in join_casts[:cast_limit] if isinstance(item, dict)],
        "notes": value.get("notes"),
    }


def _trim_join_plan(value: Optional[List[Dict[str, Any]]]) -> Optional[List[Dict[str, Any]]]:
    if not isinstance(value, list):
        return None
    total = len(value)
    limit = _calculate_trim_limit(total, default_limit=20)
    out: List[Dict[str, Any]] = []
    for item in value[:limit]:
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
    # Keep history bounded to avoid prompt bloat (linear trim - always keep most recent)
    if len(state.pipeline_events) > AGENT_TRIM_EVENTS:
        state.pipeline_events = state.pipeline_events[-AGENT_TRIM_EVENTS_COMPACT:]

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
        "You are SPICE-Harvester's autonomous data-engineering agent.\n"
        "You help users build data pipelines, analyze datasets, design ontology schemas, "
        "and transform data — all through natural conversation in Korean.\n"
        "\n"
        "## How to respond\n"
        "Return JSON matching the schema below.\n"
        "- Use `reasoning` to think step-by-step before acting (internal, not shown to user).\n"
        "- Use `message` to communicate naturally with the user in Korean.\n"
        "- `action='respond'`: conversational reply (greeting, explanation, analysis summary). Requires `message`.\n"
        "- `action='call_tool'`: inspect data, build plans, or execute operations.\n"
        "- `action='finish'`: a concrete artifact (plan, report, ontology) is ready. Include `message` to explain results.\n"
        "- `action='clarify'`: you genuinely cannot proceed without user input.\n"
        "\n"
        "## Core principles\n"
        "- Understand the user's intent before acting. Ask yourself: what do they really want?\n"
        "- Explore data first (dataset_list, dataset_sample, profiling), then design the pipeline.\n"
        "- Batch multiple tool calls per step via `tool_calls[]` to reduce latency.\n"
        "- Treat deterministic inference tools (keys/types/join-plan) as hypotheses, not ground truth.\n"
        "- Default to read-only. Only materialize (pipeline_*) when the user explicitly asks.\n"
        "- Do NOT invent data; prefer tool observations over guessing.\n"
        "- Tool args MUST NOT include raw `plan` objects; the server stores them.\n"
        "- Use deterministic `node_id` when adding nodes so later edits don't depend on auto-generated ids.\n"
        "\n"
        "## Resumed session — use existing knowledge\n"
        "When the prompt header contains `resumed_knowledge`, you are resuming a previous session.\n"
        "The knowledge ledger already has data from prior tool calls. Do NOT re-fetch:\n"
        "- `resumed_knowledge.dataset_schemas[dataset_id]` exists → skip `dataset_sample`\n"
        "- `resumed_knowledge.dataset_profiles[dataset_id]` exists → skip `dataset_profile`\n"
        "- `resumed_knowledge.foreign_keys` has patterns → skip `detect_foreign_keys`\n"
        "- `resumed_knowledge.available_datasets` exists → skip `dataset_list`\n"
        "Focus on answering clarification or building/editing the plan with existing knowledge.\n"
        "Exception: pass `force_refresh: true` in args to bypass cache if data may have changed.\n"
        "\n"
        "## Clarification framework — when to ask vs proceed\n"
        "\n"
        "Your users are DOMAIN EXPERTS, not engineers. They know their business deeply but may not\n"
        "know technical terms like 'grain', 'cardinality', or 'inner join'. Your job:\n"
        "1. Infer everything the data alone can tell you (PROCEED autonomously).\n"
        "2. Ask ONLY what the data cannot tell you — domain meaning, business rules, naming preferences.\n"
        "3. Frame every question in plain Korean with concrete options derived from the data.\n"
        "\n"
        "### What the data CAN tell you (→ PROCEED, never ask)\n"
        "- Column data types (string, int, float, date, boolean)\n"
        "- Null ratios, distinct counts, value distributions\n"
        "- Foreign key relationships (matching column names + values)\n"
        "- Join keys (columns with matching names and overlapping values)\n"
        "- Schema structure (column list, row counts)\n"
        "- Uniqueness (candidate primary keys)\n"
        "\n"
        "### What the data CANNOT tell you (→ CLARIFY, must ask)\n"
        "\n"
        "#### 1. Business entity boundaries\n"
        "Data alone cannot determine how the user mentally models their domain.\n"
        "- Example: 'customer_address' columns — is Address part of Customer, or its own entity?\n"
        "- Example: orders + order_items — should the canonical entity be at order or item level?\n"
        "- How to ask: '주문 데이터에 주문(order)과 주문항목(order_item)이 있습니다. "
        "분석의 기본 단위를 어떤 것으로 할까요? (1) 주문 단위 — 주문 1건당 1행 "
        "(2) 주문항목 단위 — 상품 1건당 1행'\n"
        "\n"
        "#### 2. Relationship meaning and direction\n"
        "FK detection finds connections but cannot determine their business meaning.\n"
        "- Example: customer_id links customers↔orders, but is the relationship 'placed_by' or 'received'?\n"
        "- Example: product_id appears in both orders and reviews — direct link or through order?\n"
        "- How to ask: 'Customer와 Order가 customer_id로 연결됩니다. "
        "이 관계를 어떻게 부르면 좋을까요? (1) \"주문함\" — 고객이 주문을 함 "
        "(2) \"구매 이력\" — 고객의 구매 기록 (3) 직접 입력'\n"
        "\n"
        "#### 3. Ambiguous column purpose\n"
        "Column names can be vague — the same name means different things in different domains.\n"
        "- Example: 'value' — price? score? quantity? weight?\n"
        "- Example: 'status' — order status? payment status? shipping status?\n"
        "- Example: 'type' — customer type? product category? transaction type?\n"
        "- How to ask: '\"value\" 컬럼의 값이 12.50, 89.99 등입니다. "
        "이 값은 무엇을 의미하나요? (1) 가격/금액 (2) 평점/점수 (3) 수량 (4) 직접 입력'\n"
        "\n"
        "#### 4. Naming preferences (Object Types, properties)\n"
        "Technical names from data don't reflect business terminology.\n"
        "- Example: table 'olist_customers' — Object Type name should be 'Customer'? '고객'? 'Buyer'?\n"
        "- How to ask: '이 데이터의 Object Type 이름을 제안합니다: "
        "Customer, Order, Product, Seller. 이 이름이 맞나요? 변경하고 싶은 항목이 있으면 알려주세요.'\n"
        "\n"
        "#### 5. Join strategy for pipelines\n"
        "The correct join type depends on business rules, not data structure.\n"
        "- Example: orders LEFT JOIN payments — keep orders without payments, or only matched orders?\n"
        "- How to ask: '주문(Order)과 결제(Payment) 데이터를 결합합니다. "
        "결제 기록이 없는 주문도 포함할까요? (1) 네, 모든 주문 포함 (미결제 포함) "
        "(2) 아니오, 결제된 주문만'\n"
        "\n"
        "#### 6. Primary key / title key selection\n"
        "Uniqueness detection finds candidates, but business identity is a domain choice.\n"
        "- Example: both 'customer_id' and 'email' are unique — which is the business identifier?\n"
        "- How to ask: '고객을 식별하는 기본 키로 무엇을 사용할까요? "
        "(1) customer_id (내부 ID) (2) email (이메일 주소) "
        "그리고 목록에서 표시할 이름은? (1) customer_name (2) email'\n"
        "\n"
        "#### 7. Data quality decisions\n"
        "When data has significant quality issues, handling strategy is a business decision.\n"
        "- Example: 30% null in 'review_score' — drop rows? fill with default? ignore column?\n"
        "- How to ask: '\"review_score\" 컬럼에 30%의 빈 값이 있습니다. "
        "어떻게 처리할까요? (1) 빈 값이 있는 행 제외 (2) 기본값(예: 0)으로 채움 "
        "(3) 이 컬럼 제외 (4) 그대로 유지'\n"
        "\n"
        "#### 8. Final design confirmation (mandatory)\n"
        "Before any irreversible registration, always present the complete design and get confirmation.\n"
        "- Pipeline plan: show node count, join strategy, output structure\n"
        "- Ontology schema: show Object Types, properties, relationships\n"
        "- How to ask: present a concrete proposal with all decisions summarized, then ask '이 구조로 진행할까요?'\n"
        "\n"
        "### Clarification question format rules\n"
        "- ALWAYS explain WHY you are asking each question — state the concrete consequence of the choice.\n"
        "  Example: '결제수단 목록이 문자열일 경우, 값이 없을 때 어떻게 둘지 결정해야 리포트/대시보드 오류를 막을 수 있습니다.'\n"
        "  Bad: '결제수단 빈 값 처리를 선택해 주세요' (no reason given)\n"
        "- ALWAYS provide concrete `options` (2-4 choices) derived from actual data observations.\n"
        "- ALWAYS show the data evidence that led to your question (sample values, counts, patterns).\n"
        "- NEVER use technical jargon — translate to plain Korean business language.\n"
        "- NEVER ask about things you can determine from the data alone.\n"
        "- NEVER ask multiple unrelated questions at once — group related decisions together.\n"
        "- If the user's original request implies a clear preference, use it as the default and confirm.\n"
        "- Set `type='enum'` with `options` when possible so the user can click, not type.\n"
        "\n"
        "## Tool batching & placeholders\n"
        "- In batched tool calls, reference earlier outputs: $last.<field> or $last.<tool_alias>.<field>\n"
        "- Max 20 tool calls executed per step; excess calls are skipped.\n"
        "\n"
        "## Tool parameter quick-reference\n"
        "These are the **exact** parameter names each tool expects. Do NOT invent alternatives.\n"
        "\n"
        "### plan_add_input (add a dataset source)\n"
        "  Required: `dataset_id` (string).  Optional: `dataset_name`, `dataset_branch`, `node_id`.\n"
        "\n"
        "### plan_add_join\n"
        "  Required: `left_node_id`, `right_node_id`, `left_keys` (list), `right_keys` (list), `join_type` (inner|left|right|full|cross).\n"
        "  Optional: `node_id`, `join_hints`, `broadcast_left`, `broadcast_right`.\n"
        "  Note: Do NOT use `left_input` or `right_input` — the correct names are `left_node_id` / `right_node_id`.\n"
        "\n"
        "### plan_add_group_by (aggregation)\n"
        "  Required: `input_node_id`, `aggregates` (list of `{column, op, alias?}`).\n"
        "  Optional: `group_by` (list), `operation` (groupBy|aggregate), `node_id`.\n"
        "  Valid `op` values: count, sum, avg, min, max, first, last, collect_list, collect_set, count_distinct.\n"
        "  Note: Do NOT use `aggregations` or `required_columns`. The parameter is `aggregates`.\n"
        "\n"
        "### plan_add_group_by_expr (SQL expression-based aggregation)\n"
        "  Required: `input_node_id`, `aggregate_expressions` (list of SQL strings).\n"
        "  Optional: `group_by` (list), `operation`, `node_id`.\n"
        "\n"
        "### plan_add_select\n"
        "  Required: `input_node_id`, `columns` (list of column names to keep).\n"
        "  Optional: `node_id`.\n"
        "\n"
        "### plan_add_filter\n"
        "  Required: `input_node_id`, `expression` (Spark SQL boolean expression).\n"
        "  Optional: `node_id`.\n"
        "\n"
        "### plan_add_compute\n"
        "  Required: `input_node_id`, `expression` (string) + `alias` (new column name).\n"
        "  OR: `input_node_id`, `computations` (list of `{alias, expression}`).\n"
        "  Optional: `node_id`.\n"
        "\n"
        "### dataset_sample\n"
        "  Required: `dataset_id`.  Optional: `limit` (default 20, max 50), `columns` (list).\n"
        "\n"
        "### dataset_profile\n"
        "  Required: `dataset_id`.  Optional: `columns` (list).\n"
        "\n"
        "### ontology_infer_schema_from_data\n"
        "  Required: `columns` (list of column name strings), `data` (list of row arrays).\n"
        "  Optional: `class_name` (string).\n"
        "  IMPORTANT: Pass raw sample data rows, NOT dataset_ids.\n"
        "  Example: `{\"columns\": [\"id\", \"name\", \"email\"], \"data\": [[\"1\", \"Alice\", \"a@b.com\"], [\"2\", \"Bob\", \"b@c.com\"]]}`\n"
        "  Tip: First call `dataset_sample` to get rows, then extract columns+data from the result.\n"
        "\n"
        "### ontology_register_object_type\n"
        "  Required: `db_name`, `class_id`, `dataset_id`, `primary_key` (list), `title_key` (list).\n"
        "  Optional: `branch` (default: main).\n"
        "\n"
        "### detect_foreign_keys (FK relationship detection)\n"
        "  Required: `dataset_id` (string — SINGULAR, one dataset ID).\n"
        "  Optional: `confidence_threshold` (number, default 0.6).\n"
        "  This tool analyzes ONE source dataset and finds FK relationships to ALL other datasets in the DB.\n"
        "  For full bidirectional coverage, call it ONCE PER DATASET using batched `tool_calls`.\n"
        "  ❌ Do NOT use `dataset_ids` (plural/array) — the parameter is `dataset_id` (singular string).\n"
        "\n"
        "## Spark SQL notes\n"
        "- Spark SQL *expressions* are allowed in filter/compute/select_expr/group_by_expr/window_expr tools.\n"
        "- `plan_preview` is a lightweight Python executor, NOT full Spark. For complex SQL, validate via `pipeline_preview_wait`.\n"
        "\n"
        "## Ontology & Objectify workflow\n"
        "When the user asks to design an ontology or map data to ontology instances, follow this workflow:\n"
        "\n"
        "### Phase 1: Schema inference (autonomous)\n"
        "1. Call `dataset_sample` for each dataset to get sample rows.\n"
        "2. Call `dataset_profile` to understand column types, nulls, and distributions.\n"
        "3. Call `ontology_infer_schema_from_data` with the sample data:\n"
        "   - Extract `columns` (list of column names) and `data` (list of row arrays) from `dataset_sample` result.\n"
        "   - Do NOT pass `dataset_ids` — this tool requires raw data, not references.\n"
        "4. Call `detect_foreign_keys` ONCE PER DATASET with singular `dataset_id` (string, not array).\n"
        "   - Each call analyzes one source dataset against all other datasets in the DB.\n"
        "   - For N datasets, make N separate calls to get full bidirectional FK coverage.\n"
        "   - Use batched `tool_calls` to issue all N calls in a single step.\n"
        "- These are mechanical steps — proceed autonomously.\n"
        "\n"
        "### Phase 2: Domain modeling (clarification required)\n"
        "Apply the clarification framework above. You MUST use `action='clarify'` for:\n"
        "- Entity boundaries (§1), relationship meaning (§2), ambiguous columns (§3),\n"
        "  naming preferences (§4), PK/title key (§6), and final design confirmation (§8).\n"
        "Present your inferred schema as a concrete proposal with options, then ask for confirmation.\n"
        "\n"
        "### Phase 3: Registration & mapping (autonomous after approval)\n"
        "After user confirms the design, create ONE class at a time:\n"
        "For EACH class (e.g. Customer, Order, Payment):\n"
        "  a) `ontology_new(class_id='Customer', label='Customer')` — creates empty class in memory\n"
        "  b) `ontology_add_property(...)` for each column — add all properties\n"
        "  c) `ontology_set_primary_key(primary_key=['customer_unique_id'])` — set PK\n"
        "  d) `ontology_add_relationship(...)` — add relationships if any\n"
        "  e) `ontology_validate()` — check consistency\n"
        "  f) `ontology_create()` — persist to TerminusDB\n"
        "  g) `ontology_register_object_type(class_id='Customer', dataset_id='...', primary_key=['customer_unique_id'], title_key=['customer_unique_id'])` — register for objectify\n"
        "Repeat a-g for each class before moving to objectify.\n"
        "\n"
        "IMPORTANT tool signatures:\n"
        "- `ontology_new(class_id='Customer', label='Customer')` — session_id is auto-injected.\n"
        "- `ontology_add_property(name='customer_id', type='string', label='Customer ID')` — type: 'string'|'integer'|'float'|'boolean'|'datetime'|'date'. Optional: required (bool), primary_key (bool), title_key (bool).\n"
        "- `ontology_set_primary_key(property_name='customer_id')` — sets ONE property as PK. For composite PK, call twice with clear_existing=false on the second call.\n"
        "- `ontology_add_relationship(predicate='hasOrders', target='Order', label='Has Orders', cardinality='1:n')` — cardinality: '1:1'|'1:n'|'n:1'|'n:m'. All 4 args required.\n"
        "- `ontology_create()` — db_name/branch auto-injected. Persists to TerminusDB.\n"
        "- `ontology_register_object_type(class_id='Customer', dataset_id='...', primary_key=['customer_unique_id'], title_key=['customer_unique_id'])` — db_name auto-injected.\n"
        "\n"
        "After ALL classes are registered:\n"
        "  h) `objectify_suggest_mapping(class_id='...', dataset_id='...')` → review suggestions\n"
        "  i) `objectify_create_mapping_spec(...)` — persist mapping\n"
        "  j) `objectify_run(...)` → `objectify_wait(...)` → `ontology_query_instances(...)` to verify\n"
        "\n"
        "### Phase 4: Link types (autonomous)\n"
        "- Use `create_link_type_from_fk` with FK patterns from Phase 1.\n"
        "- Link types connect Object Type instances via foreign key relationships.\n"
        "\n"
        f"## Available tools\n{tool_lines}\n"
        "\n"
        "## Response schema\n"
        "{\n"
        '  "reasoning": "your internal thought process (optional)",\n'
        '  "action": "call_tool|finish|clarify|respond",\n'
        '  "message": "natural language message to user in Korean (optional, required for respond)",\n'
        '  "tool_calls": [{"tool": "string", "args": {...}}],\n'
        '  "tool": "string (legacy single tool; optional if tool_calls is set)",\n'
        '  "args": {"...": "..."},\n'
        '  "questions": [PipelineClarificationQuestion],\n'
        '  "notes": string[],\n'
        '  "warnings": string[],\n'
        '  "confidence": number (0..1)\n'
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
    # Include restored context so LLM knows it's resuming a previous session
    if state.plan_obj:
        header["resumed_plan"] = {
            "plan_status": _plan_status(state.plan_obj),
            "plan_summary": _summarize_plan(state.plan_obj),
        }
    if state.knowledge_ledger:
        header["resumed_knowledge"] = state.knowledge_ledger
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
    # Knowledge ledger survives ALL compression levels
    if state.knowledge_ledger:
        snapshot["knowledge_ledger"] = state.knowledge_ledger
    return snapshot


def _get_item_type(item_json: str) -> str:
    """Fast extraction of item type from serialized JSON without full parse."""
    # Items are stored as stable_json_dumps; "type" is always near the start.
    try:
        obj = json.loads(item_json)
        return str(obj.get("type") or "unknown") if isinstance(obj, dict) else "unknown"
    except Exception:
        return "unknown"


def _reshrink_tool_output(item_json: str, max_chars: int) -> str:
    """Re-summarize a tool_output item to fit within max_chars."""
    try:
        obj = json.loads(item_json)
    except Exception:
        return item_json[:max_chars]
    if not isinstance(obj, dict):
        return item_json[:max_chars]
    output = obj.get("output")
    if isinstance(output, dict):
        # Keep only essential keys from the output
        shrunk = {}
        for k in ("status", "error", "errors", "node_id", "dataset_id", "pipeline_id",
                   "columns", "class_id", "session_id"):
            if k in output:
                shrunk[k] = output[k]
        shrunk["_reshrunk"] = True
        obj["output"] = shrunk
    result = stable_json_dumps(obj)
    return result[:max_chars] if len(result) > max_chars else result


def _progressive_compress_prompt_items(
    *,
    state: _AgentState,
    answers: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
    task_spec: Optional[Dict[str, Any]],
    target_chars: int,
    preserve_recent_n: int = 10,
) -> List[str]:
    """Five-level graduated compression with importance scoring.

    Levels applied in order until prompt fits within target_chars:
      L1 – Remove tool_output_ref items (duplicate references)
      L2 – Re-summarize tool_output items to 1K chars each
      L3 – Merge consecutive state items (keep only latest per 3-run)
      L4 – Priority-scored removal (lowest priority first)
      L5 – Full snapshot + knowledge_ledger (last resort)
    """
    items = list(state.prompt_items)
    if len(items) <= 2:
        return items

    def _total_len(lst: List[str]) -> int:
        return sum(len(s) for s in lst)

    if _total_len(items) <= target_chars:
        return items

    # Protect header (index 0) and recent N items throughout L1-L4
    header = items[:1]
    if len(items) > preserve_recent_n + 1:
        protected_tail = items[-preserve_recent_n:]
        middle = items[1:-preserve_recent_n]
    else:
        protected_tail = items[1:]
        middle = []

    # --- Level 1: Drop tool_output_ref items ---
    middle = [m for m in middle if _get_item_type(m) != "tool_output_ref"]
    if _total_len(header + middle + protected_tail) <= target_chars:
        return header + middle + protected_tail

    # --- Level 2: Re-shrink tool_output items ---
    for i, m in enumerate(middle):
        if _get_item_type(m) == "tool_output" and len(m) > 1000:
            middle[i] = _reshrink_tool_output(m, max_chars=1000)
    if _total_len(header + middle + protected_tail) <= target_chars:
        return header + middle + protected_tail

    # --- Level 3: Merge consecutive state items (keep 1 per 3) ---
    merged: List[str] = []
    state_run: List[str] = []
    for m in middle:
        if _get_item_type(m) == "state":
            state_run.append(m)
            if len(state_run) >= 3:
                merged.append(state_run[-1])  # keep latest of the 3
                state_run = []
        else:
            if state_run:
                merged.append(state_run[-1])
                state_run = []
            merged.append(m)
    if state_run:
        merged.append(state_run[-1])
    middle = merged
    if _total_len(header + middle + protected_tail) <= target_chars:
        return header + middle + protected_tail

    # --- Level 4: Priority-scored removal (lowest first) ---
    scored = [(ITEM_PRIORITY.get(_get_item_type(m), 3), i, m) for i, m in enumerate(middle)]
    scored.sort(key=lambda x: (x[0], x[1]))  # lowest priority & oldest first
    budget = target_chars - _total_len(header) - _total_len(protected_tail)
    kept: List[str] = []
    used = 0
    # Take items in reverse priority (highest first) until budget exhausted
    for _, _, m in reversed(scored):
        if used + len(m) <= budget:
            kept.append(m)
            used += len(m)
    # Restore original order
    kept_set = set(id(k) for k in kept)
    middle = [m for m in middle if id(m) in kept_set]
    if _total_len(header + middle + protected_tail) <= target_chars:
        return header + middle + protected_tail

    # --- Level 5: Full snapshot + knowledge_ledger ---
    snapshot = _build_compaction_snapshot(
        state=state,
        answers=answers,
        planner_hints=planner_hints,
        task_spec=task_spec,
    )
    keep_recent = min(3, len(protected_tail))
    recent = protected_tail[-keep_recent:] if keep_recent > 0 else []
    return [
        stable_json_dumps(_build_prompt_header(
            state=state,
            answers=answers,
            planner_hints=planner_hints,
            task_spec=task_spec,
        )),
        stable_json_dumps({
            "type": "compaction",
            "reason": "graduated_L5",
            "snapshot": snapshot,
            "preserved_recent_count": keep_recent,
            "note": "Graduated compression exhausted; snapshot + knowledge_ledger is authoritative.",
        }),
    ] + recent


def _compute_prompt_hash(text: str) -> str:
    """Compute a short hash of prompt text for identification."""
    import hashlib
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


async def _log_pre_compression_state(
    *,
    run_id: str,
    prompt_items: List[str],
    compression_reason: str,
    audit_store: Optional[AuditLogStore] = None,
    event_store: Optional[EventStore] = None,
) -> None:
    """
    Save pre-compression state for debugging.

    Storage:
    - Metadata to AuditLogStore (PostgreSQL)
    - Full prompt to EventStore (S3) if available

    Args:
        run_id: Pipeline run identifier
        prompt_items: Current prompt items before compression
        compression_reason: Why compression is happening
        audit_store: Optional audit log store
        event_store: Optional event store for S3 storage
    """
    from datetime import datetime, timezone

    occurred_at = datetime.now(timezone.utc)
    full_prompt = "\n".join(prompt_items)
    prompt_hash = _compute_prompt_hash(full_prompt)

    # Metadata for audit log
    metadata = {
        "run_id": run_id,
        "prompt_item_count": len(prompt_items),
        "total_chars": len(full_prompt),
        "prompt_hash": prompt_hash,
        "compression_reason": compression_reason,
        "occurred_at": occurred_at.isoformat(),
    }

    # Log to AuditLogStore
    if audit_store:
        try:
            await audit_store.log(
                partition_key=f"pipeline_agent:{run_id}",
                actor="pipeline_agent",
                action="PROMPT_PRE_COMPRESSION",
                status="success",
                resource_type="prompt_snapshot",
                resource_id=f"compression:{run_id}:{prompt_hash}",
                metadata=metadata,
            )
        except Exception as e:
            logger.debug(f"Pre-compression audit log failed (non-fatal): {e}")

    # Store full prompt to S3 via EventStore pattern
    if event_store:
        try:
            import aioboto3

            if not hasattr(event_store, "session") or event_store.session is None:
                event_store.session = aioboto3.Session()

            s3_key = (
                f"prompt_snapshots/{occurred_at.year:04d}/{occurred_at.month:02d}/"
                f"{run_id}/pre_compression_{prompt_hash}.jsonl"
            )

            # Use EventStore's S3 client configuration
            s3_kwargs = {}
            if hasattr(event_store, "_s3_client_kwargs"):
                s3_kwargs = event_store._s3_client_kwargs()
            elif hasattr(event_store, "endpoint_url"):
                s3_kwargs = {
                    "service_name": "s3",
                    "endpoint_url": event_store.endpoint_url,
                    "aws_access_key_id": getattr(event_store, "access_key", None),
                    "aws_secret_access_key": getattr(event_store, "secret_key", None),
                }

            if s3_kwargs:
                async with event_store.session.client(**s3_kwargs) as s3:
                    bucket_name = getattr(event_store, "bucket_name", "spice-event-store")
                    await s3.put_object(
                        Bucket=bucket_name,
                        Key=s3_key,
                        Body=full_prompt.encode("utf-8"),
                        ContentType="application/x-jsonlines",
                        Metadata={
                            "run-id": run_id,
                            "prompt-hash": prompt_hash,
                            "item-count": str(len(prompt_items)),
                        },
                    )
                logger.debug(f"Pre-compression prompt saved to S3: {s3_key}")
        except Exception as e:
            logger.debug(f"Pre-compression S3 save failed (non-fatal): {e}")


async def _maybe_compact_prompt_items(
    *,
    state: _AgentState,
    answers: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
    task_spec: Optional[Dict[str, Any]],
    max_chars: int,
    run_id: str = "",
    audit_store: Optional[AuditLogStore] = None,
    event_store: Optional[EventStore] = None,
) -> None:
    """
    Improved compaction with progressive compression and pre-compression logging.

    Uses progressive compression (removing oldest items first) before falling back
    to full snapshot-based compaction. Logs pre-compression state for debugging.

    Args:
        state: Current agent state (prompt_items will be modified)
        answers: User answers for clarification
        planner_hints: Hints for the planner
        task_spec: Task specification
        max_chars: Maximum allowed characters
        run_id: Pipeline run identifier for logging
        audit_store: Optional audit log store for metadata
        event_store: Optional event store for S3 storage
    """
    if max_chars <= 0:
        return

    current = _prompt_text(state.prompt_items)
    if len(current) <= max_chars:
        return

    # Log pre-compression state for debugging
    if run_id and (audit_store or event_store):
        await _log_pre_compression_state(
            run_id=run_id,
            prompt_items=state.prompt_items,
            compression_reason=f"prompt_size_{len(current)}_exceeds_{max_chars}",
            audit_store=audit_store,
            event_store=event_store,
        )

    # Calculate target (70% of max to leave room for growth)
    target_chars = int(max_chars * 0.7)

    # Progressive compression
    state.prompt_items = _progressive_compress_prompt_items(
        state=state,
        answers=answers,
        planner_hints=planner_hints,
        task_spec=task_spec,
        target_chars=target_chars,
    )


def _mask_tool_observation(payload: Dict[str, Any]) -> Dict[str, Any]:
    # Ensure we never echo raw PII back into the prompt snapshot.
    return mask_pii(payload or {}, max_string_chars=200)


def _is_internal_budget_clarification(questions: List[PipelineClarificationQuestion]) -> bool:
    for q in questions or []:
        text = f"{q.id} {q.question}".lower()
        if any(token in text for token in ("step", "steps", "budget", "limit", "quota", "스텝", "단계", "한도")):
            return True
    return False


# ---------------------------------------------------------------------------
# Module-level MCP tool caller (replaces 4 near-identical nested closures)
# ---------------------------------------------------------------------------

async def _call_mcp_tool(
    mcp_manager: Any,
    server: str,
    tool: str,
    arguments: Dict[str, Any],
) -> Dict[str, Any]:
    """Call an MCP tool on *server* ("pipeline" or "ontology") and normalise the response.

    Handles all known response shapes from the MCP SDK:
    - plain dict
    - object with structuredContent / structured_content / data attribute
    - object with content list (text parts, possibly JSON-encoded)
    - error flag
    Includes ``_parse_warnings`` for non-JSON text fallbacks so callers can
    inspect parsing issues.
    """
    payload = await mcp_manager.call_tool(server, tool, arguments)
    if isinstance(payload, dict):
        return payload

    # Structured-content attribute variants
    structured = getattr(payload, "structuredContent", None)
    if isinstance(structured, dict):
        return structured
    structured = getattr(payload, "structured_content", None)
    if isinstance(structured, dict):
        return structured

    # Generic .data wrapper
    data = getattr(payload, "data", None)
    if isinstance(data, dict):
        return data

    # Content-list (text parts) – the most common MCP SDK shape
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
        parse_errors: List[str] = []
        for text in texts:
            try:
                parsed = json.loads(text)
            except Exception as parse_exc:
                parse_errors.append(f"JSON parse error: {parse_exc}")
                continue
            if isinstance(parsed, dict):
                return parsed
        if parse_errors:
            logger.warning("%s MCP tool %s returned non-JSON: %s", server, tool, parse_errors)
        if texts:
            return {"result": texts[0], "_parse_warnings": parse_errors if parse_errors else None}

    raise RuntimeError(f"Unexpected MCP tool result type for {tool}: {type(payload)}")


# ---------------------------------------------------------------------------
# LLM meta helper (replaces 10+ identical inline blocks)
# ---------------------------------------------------------------------------

def _build_llm_meta_dict(llm_meta: Optional["LLMCallMeta"]) -> Optional[Dict[str, Any]]:
    """Convert an LLMCallMeta to a plain dict (or None). Used in every return block."""
    if llm_meta is None:
        return None
    return {
        "provider": llm_meta.provider,
        "model": llm_meta.model,
        "cache_hit": llm_meta.cache_hit,
        "latency_ms": llm_meta.latency_ms,
    }


# ---------------------------------------------------------------------------
# Module-level tool dispatcher (replaces ~480-line nested _execute_tool_call)
# ---------------------------------------------------------------------------

async def _execute_tool_call(
    *,
    tool_name: str,
    args: Dict[str, Any],
    state: "_AgentState",
    mcp_manager: Any,
    allowed_tools: List[str],
    planner_hints: Optional[Dict[str, Any]],
    tool_errors: List[str],
    tool_warnings: List[str],
    step_idx: int,
) -> Dict[str, Any]:
    """Execute a single MCP tool call and mutate *state*.

    Extracted to module level so both the streaming and non-streaming loops
    can share the same dispatch logic (previously duplicated/simplified in
    the streaming path).
    """
    if tool_name not in allowed_tools:
        state.last_observation = {"error": f"tool not allowed: {tool_name}"}
        return state.last_observation

    try:
        args = _drop_none_values(dict(args or {}))
        args.pop("force_refresh", None)  # virtual param for cache bypass

        # ==================== plan_new ====================
        if tool_name == "plan_new":
            had_plan = isinstance(state.plan_obj, dict)
            had_pipeline = bool(state.pipeline_id)
            had_analysis = any([
                state.null_report, state.key_inference, state.type_inference,
                state.join_plan, state.schema_inference, state.mapping_suggestions,
            ])
            scoped: Dict[str, Any] = {
                "goal": state.goal,
                "db_name": state.db_name,
                "dataset_ids": state.dataset_ids,
            }
            if state.branch:
                scoped["branch"] = state.branch
            payload = await _call_mcp_tool(mcp_manager, "pipeline", tool_name, scoped)
            plan = payload.get("plan") if isinstance(payload, dict) else None
            state.plan_obj = plan if isinstance(plan, dict) else None

            # Reset ALL related state when creating a new plan
            reset_warnings: List[str] = []
            if had_plan:
                reset_warnings.append("Previous plan was discarded")
            if had_pipeline:
                reset_warnings.append(f"Previous pipeline_id={state.pipeline_id} was discarded")
            if had_analysis:
                reset_warnings.append("Previous analysis data (null_report, key_inference, etc.) was discarded")

            # Reset pipeline state
            state.pipeline_id = None
            state.last_build_job_id = None
            state.last_build_artifact_id = None
            state.pipeline_progress = {}
            state.pipeline_events = []
            # Reset analysis state
            state.null_report = None
            state.key_inference = None
            state.type_inference = None
            state.join_plan = None
            state.schema_inference = None
            state.mapping_suggestions = None

            observation = dict(payload) if isinstance(payload, dict) else {"result": payload}
            observation.pop("plan", None)
            suggested_tools = ["plan_add_input"]
            if state.dataset_ids and len(state.dataset_ids) > 1:
                suggested_tools.append("plan_add_join")
            if planner_hints and planner_hints.get("ontology_mode"):
                suggested_tools = ["ontology_new", "ontology_infer_schema_from_data"]
            elif planner_hints and planner_hints.get("objectify_mode"):
                suggested_tools = ["objectify_suggest_mapping", "objectify_create_mapping_spec", "objectify_run"]
            else:
                suggested_tools.extend(["plan_add_transform", "plan_add_output"])
            state.last_observation = _mask_tool_observation(
                {
                    **observation,
                    "plan_status": _plan_status(state.plan_obj),
                    "plan_replaced": bool(had_plan),
                    "reset_warnings": reset_warnings if reset_warnings else None,
                    "next_suggested_tools": suggested_tools,
                }
            )
            return state.last_observation

        # ==================== plan_reset ====================
        if tool_name == "plan_reset":
            if not isinstance(state.plan_obj, dict):
                state.last_observation = {"error": "plan is not initialized; call plan_new first"}
                return state.last_observation
            payload = await _call_mcp_tool(mcp_manager, "pipeline", tool_name, {"plan": state.plan_obj})
            plan = payload.get("plan") if isinstance(payload, dict) else None
            state.plan_obj = plan if isinstance(plan, dict) else None
            observation = dict(payload) if isinstance(payload, dict) else {"result": payload}
            observation.pop("plan", None)
            state.last_observation = _mask_tool_observation({**observation, "plan_status": _plan_status(state.plan_obj)})
            return state.last_observation

        # ==================== Pipeline Execution Tools ====================
        if tool_name.startswith("pipeline_"):
            if tool_name in {"pipeline_create_from_plan", "pipeline_update_from_plan"}:
                if not isinstance(state.plan_obj, dict):
                    state.last_observation = {"error": "plan is not initialized; call plan_new first"}
                    return state.last_observation
                args.pop("plan", None)
                payload = await _call_mcp_tool(
                    mcp_manager, "pipeline", tool_name,
                    {
                        **args,
                        "plan": state.plan_obj,
                        "principal_id": state.principal_id,
                        "principal_type": state.principal_type,
                    },
                )
            else:
                args.pop("plan", None)
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
                payload = await _call_mcp_tool(
                    mcp_manager, "pipeline", tool_name,
                    {
                        **args,
                        "principal_id": state.principal_id,
                        "principal_type": state.principal_type,
                        "db_name": state.db_name,
                    },
                )

            # Capture pipeline_id if returned
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

            state.last_observation = _mask_tool_observation(payload if isinstance(payload, dict) else {"result": payload})
            return state.last_observation

        # ==================== Ontology Tools ====================
        if tool_name.startswith("ontology_") and tool_name not in {
            "ontology_register_object_type",
            "ontology_query_instances",
        }:
            if "session_id" not in args:
                args["session_id"] = state.ontology_session_id
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

            payload = await _call_mcp_tool(mcp_manager, "ontology", tool_name, args)

            # Auto-fallback: 409 Conflict → load seq → ontology_update
            if tool_name == "ontology_create" and isinstance(payload, dict):
                error_msg = str(payload.get("error") or "")
                if "409" in error_msg or "Conflict" in error_msg:
                    logger.info("ontology_create got 409 Conflict, loading seq then ontology_update")
                    # Load current class to get expected_seq
                    class_id = args.get("class_id") or (state.working_ontology or {}).get("id", "")
                    load_args = {
                        "session_id": args.get("session_id", state.ontology_session_id),
                        "db_name": args.get("db_name", state.db_name),
                        "class_id": class_id,
                        "branch": args.get("branch", state.branch or "main"),
                    }
                    load_result = await _call_mcp_tool(mcp_manager, "ontology", "ontology_load", load_args)
                    seq = None
                    if isinstance(load_result, dict):
                        seq = load_result.get("seq") or load_result.get("expected_seq")
                        ont = load_result.get("ontology")
                        if isinstance(ont, dict) and seq is None:
                            seq = ont.get("_seq") or ont.get("seq")
                    update_args = dict(args)
                    if seq is not None:
                        update_args["expected_seq"] = seq
                        logger.info("409 fallback: loaded seq=%s for class=%s", seq, class_id)
                    payload = await _call_mcp_tool(mcp_manager, "ontology", "ontology_update", update_args)
                    if isinstance(payload, dict) and payload.get("status") == "success":
                        logger.info("ontology_update succeeded after 409 fallback")

            if tool_name == "ontology_preview" and isinstance(payload, dict):
                ont = payload.get("ontology")
                if isinstance(ont, dict):
                    state.working_ontology = ont
            if tool_name == "ontology_infer_schema_from_data" and isinstance(payload, dict):
                state.schema_inference = payload
            if tool_name == "ontology_suggest_mappings" and isinstance(payload, dict):
                state.mapping_suggestions = payload

            if tool_name in {"ontology_create", "ontology_update", "ontology_add_property",
                             "ontology_add_relationship", "ontology_set_primary_key"}:
                logger.info("ontology tool executed: %s status=%s", tool_name,
                           payload.get("status") if isinstance(payload, dict) else "unknown")

            state.last_observation = _mask_tool_observation(
                payload if isinstance(payload, dict) else {"result": payload}
            )
            return state.last_observation

        # ==================== Objectify Tools ====================
        if tool_name.startswith("objectify_"):
            payload = await _call_mcp_tool(mcp_manager, "pipeline", tool_name, args)
            if tool_name in {"objectify_create_mapping_spec", "objectify_run"}:
                logger.info("objectify tool executed: %s status=%s", tool_name,
                           payload.get("status") if isinstance(payload, dict) else "unknown")
            state.last_observation = _mask_tool_observation(
                payload if isinstance(payload, dict) else {"result": payload}
            )
            return state.last_observation

        # ==================== Dataset Lookup Tools ====================
        if tool_name in {"dataset_get_by_name", "dataset_get_latest_version", "dataset_validate_columns", "dataset_list", "dataset_sample", "dataset_profile", "data_query"}:
            payload = await _call_mcp_tool(mcp_manager, "pipeline", tool_name, args)
            state.last_observation = _mask_tool_observation(
                payload if isinstance(payload, dict) else {"result": payload}
            )
            return state.last_observation

        # ==================== Ontology Query Tools ====================
        if tool_name == "ontology_query_instances":
            payload = await _call_mcp_tool(mcp_manager, "pipeline", tool_name, args)
            state.last_observation = _mask_tool_observation(
                payload if isinstance(payload, dict) else {"result": payload}
            )
            return state.last_observation

        # ==================== Object Type Registration ====================
        if tool_name == "ontology_register_object_type":
            payload = await _call_mcp_tool(mcp_manager, "pipeline", tool_name, args)
            if isinstance(payload, dict):
                logger.info("ontology_register_object_type executed: status=%s class_id=%s",
                           payload.get("status"), args.get("class_id"))
            state.last_observation = _mask_tool_observation(
                payload if isinstance(payload, dict) else {"result": payload}
            )
            return state.last_observation

        # ==================== Debugging Tools ====================
        if tool_name == "debug_get_errors":
            include_warnings = args.get("include_warnings", True)
            limit = int(args.get("limit") or 50)
            errors_list = list(tool_errors)[-limit:] if tool_errors else []
            warnings_list = list(tool_warnings)[-limit:] if include_warnings and tool_warnings else []
            state.last_observation = {
                "status": "success",
                "errors": errors_list,
                "warnings": warnings_list,
                "error_count": len(tool_errors),
                "warning_count": len(tool_warnings) if include_warnings else 0,
            }
            return state.last_observation

        if tool_name == "debug_get_execution_log":
            limit = int(args.get("limit") or 20)
            step_filter = args.get("step")
            log_entries: List[Dict[str, Any]] = []
            for item in state.prompt_items:
                try:
                    parsed = json.loads(item)
                    item_type = parsed.get("type")
                    item_step = parsed.get("step")
                    if item_type in {"tool_call", "tool_output", "tool_calls", "tool_outputs"}:
                        if step_filter is None or item_step == step_filter:
                            entry: Dict[str, Any] = {
                                "type": item_type,
                                "step": item_step,
                            }
                            if item_type in {"tool_call", "tool_calls"}:
                                entry["tool"] = parsed.get("tool") or parsed.get("tools")
                            if item_type in {"tool_output", "tool_outputs"}:
                                obs = parsed.get("observation") or parsed.get("observations")
                                if isinstance(obs, dict):
                                    entry["status"] = obs.get("status")
                                    entry["error"] = obs.get("error")
                                elif isinstance(obs, list):
                                    entry["count"] = len(obs)
                            log_entries.append(entry)
                except (json.JSONDecodeError, TypeError):
                    continue
            state.last_observation = {
                "status": "success",
                "log": log_entries[-limit:],
                "total_entries": len(log_entries),
                "current_step": step_idx + 1,
            }
            return state.last_observation

        if tool_name == "debug_explain_failure":
            diagnosis: List[Dict[str, str]] = []
            for error in list(tool_errors)[-10:]:
                error_lower = str(error).lower()
                if "not found" in error_lower or "does not exist" in error_lower:
                    diagnosis.append({
                        "error": str(error),
                        "cause": "Resource not found",
                        "fix": "Check spelling and verify the resource exists",
                    })
                elif "type" in error_lower and ("mismatch" in error_lower or "incompatible" in error_lower):
                    diagnosis.append({
                        "error": str(error),
                        "cause": "Type mismatch",
                        "fix": "Add a cast operation to convert types",
                    })
                elif "join" in error_lower:
                    diagnosis.append({
                        "error": str(error),
                        "cause": "Join key issue",
                        "fix": "Verify join keys exist and have compatible types",
                    })
                elif "permission" in error_lower or "denied" in error_lower:
                    diagnosis.append({
                        "error": str(error),
                        "cause": "Permission denied",
                        "fix": "Check user permissions for the resource",
                    })
                elif "timeout" in error_lower:
                    diagnosis.append({
                        "error": str(error),
                        "cause": "Operation timed out",
                        "fix": "Reduce data size or increase timeout",
                    })
                else:
                    diagnosis.append({
                        "error": str(error),
                        "cause": "Unknown",
                        "fix": "Review error details and check logs",
                    })
            state.last_observation = {
                "status": "success",
                "diagnosis": diagnosis,
                "total_errors": len(tool_errors),
                "total_warnings": len(tool_warnings),
                "suggestion": "Use debug_get_errors to see all errors, debug_get_execution_log to trace tool calls",
            }
            return state.last_observation

        if tool_name == "debug_inspect_node":
            plan_to_inspect = args.get("plan") or state.plan_obj
            if not isinstance(plan_to_inspect, dict):
                state.last_observation = {"error": "No plan available; call plan_new first or provide plan argument"}
                return state.last_observation
            payload = await _call_mcp_tool(mcp_manager, "pipeline", tool_name, {**args, "plan": plan_to_inspect})
            state.last_observation = _mask_tool_observation(payload if isinstance(payload, dict) else {"result": payload})
            return state.last_observation

        if tool_name == "debug_dry_run":
            plan_to_validate = args.get("plan") or state.plan_obj
            if not isinstance(plan_to_validate, dict):
                state.last_observation = {"error": "No plan available; call plan_new first or provide plan argument"}
                return state.last_observation
            payload = await _call_mcp_tool(mcp_manager, "pipeline", tool_name, {**args, "plan": plan_to_validate})
            state.last_observation = _mask_tool_observation(payload if isinstance(payload, dict) else {"result": payload})
            return state.last_observation

        # ==================== FK Detection & Link Type Tools ====================
        if tool_name in {"detect_foreign_keys", "create_link_type_from_fk"}:
            if "db_name" not in args or not args.get("db_name"):
                args["db_name"] = state.db_name
            if "branch" not in args or not args.get("branch"):
                args["branch"] = state.branch or "main"
            if tool_name == "detect_foreign_keys" and "dataset_ids" in args:
                raw_ids = args.pop("dataset_ids")
                id_list = raw_ids if isinstance(raw_ids, list) else [raw_ids]
                all_patterns: list = []
                for did in id_list:
                    per_args = {**args, "dataset_id": str(did).strip()}
                    sub_payload = await _call_mcp_tool(mcp_manager, "pipeline", tool_name, per_args)
                    if isinstance(sub_payload, dict) and sub_payload.get("status") == "success":
                        all_patterns.extend(sub_payload.get("patterns") or [])
                state.last_observation = {
                    "status": "success",
                    "db_name": args.get("db_name"),
                    "patterns_found": len(all_patterns),
                    "patterns": all_patterns,
                    "datasets_analyzed": len(id_list),
                }
            else:
                payload = await _call_mcp_tool(mcp_manager, "pipeline", tool_name, args)
                state.last_observation = payload if isinstance(payload, dict) else {"result": payload}
            return state.last_observation

        # ==================== Default Plan Tools ====================
        if not isinstance(state.plan_obj, dict):
            state.last_observation = {"error": "plan is not initialized; call plan_new first"}
            return state.last_observation

        if tool_name == "preview_inspect" and not isinstance(args.get("preview"), dict):
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

        args.pop("plan", None)
        payload = await _call_mcp_tool(mcp_manager, "pipeline", tool_name, {**args, "plan": state.plan_obj})
        if isinstance(payload, dict) and isinstance(payload.get("plan"), dict):
            state.plan_obj = payload["plan"]

        if tool_name == "plan_preview" and isinstance(payload, dict) and isinstance(payload.get("preview"), dict):
            preview = dict(payload.get("preview") or {})
            rows = preview.get("rows")
            if isinstance(rows, list):
                preview["rows"] = rows[:AGENT_TRIM_PREVIEW_ROWS]
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
                observation["evaluations"] = observation.get("evaluations")[:AGENT_TRIM_EVALUATIONS]
            if isinstance(observation.get("warnings"), list):
                observation["warnings"] = observation.get("warnings")[:AGENT_TRIM_WARNINGS]
            if isinstance(observation.get("errors"), list):
                observation["errors"] = observation.get("errors")[:AGENT_TRIM_ERRORS]
            state.last_observation = _mask_tool_observation({**observation, "plan_status": _plan_status(state.plan_obj)})

        return state.last_observation

    except Exception as exc:
        exc_type = type(exc).__name__
        logger.warning("pipeline agent autonomous tool failed tool=%s err=%s type=%s", tool_name, exc, exc_type, exc_info=True)
        state.last_observation = {
            "error": str(exc),
            "error_type": exc_type,
            "tool": tool_name,
            "recoverable": exc_type in {
                "TimeoutError", "ConnectionError", "HTTPError",
                "ClosedResourceError", "BrokenResourceError", "EndOfStream",
            },
        }
        tool_errors.append(f"{tool_name} [{exc_type}]: {exc}")
        return state.last_observation


# ---------------------------------------------------------------------------
# StreamEvent dataclass (used by both streaming and core loop)
# ---------------------------------------------------------------------------

@dataclass
class StreamEvent:
    """SSE event container. ``event_type`` maps to the SSE ``event:`` field."""
    event_type: str  # start, thinking, tool_start, tool_end, plan_update, preview_update, ontology_update, clarification, error, complete
    data: Dict[str, Any]


# ---------------------------------------------------------------------------
# Unified core loop (async generator → StreamEvent)
# ---------------------------------------------------------------------------

_THINKING_MESSAGES = [
    "요청을 분석하고 있습니다...",
    "데이터 구조를 파악하고 있습니다...",
    "최적의 변환 방법을 찾고 있습니다...",
    "파이프라인 구조를 설계하고 있습니다...",
    "노드 연결 관계를 검토하고 있습니다...",
]


async def _run_agent_core(
    *,
    goal: str,
    data_scope: "PipelinePlanDataScope",
    answers: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
    task_spec: Optional[Dict[str, Any]] = None,
    resume_plan_id: Optional[str] = None,
    persist_plan: bool = True,
    actor: str,
    tenant_id: str,
    user_id: Optional[str],
    data_policies: Optional[Dict[str, Any]],
    selected_model: Optional[str],
    allowed_models: Optional[List[str]],
    llm_gateway: "LLMGateway",
    redis_service: Optional["RedisService"],
    audit_store: Optional["AuditLogStore"],
    event_store: Optional["EventStore"] = None,
    dataset_registry: "DatasetRegistry",
    plan_registry: "PipelinePlanRegistry",
    streaming: bool = False,
) -> "AsyncGenerator[StreamEvent, None]":
    """Single autonomous loop that serves both streaming and non-streaming callers.

    When *streaming* is True, intermediate events (thinking, tool_start, tool_end,
    plan_update, preview_update, ontology_update) are yielded.  Terminal events
    (complete, clarification, error) are **always** yielded.

    Non-streaming callers collect only the terminal event's ``.data`` dict.
    """
    run_id = str(uuid4())
    plan_id: Optional[str] = resume_plan_id or None

    # ── Early validation ──
    db_name = str(data_scope.db_name or "").strip()
    if not db_name:
        yield StreamEvent(
            event_type="error" if streaming else "complete",
            data={
                "run_id": run_id,
                "status": "clarification_required",
                "plan_id": plan_id,
                "plan": None,
                "preview": None,
                "report": None,
                "questions": [
                    {"id": "db_name", "question": "Which database (db_name) should I use?", "required": True, "type": "string"}
                ],
                "validation_errors": ["data_scope.db_name is required"],
                "validation_warnings": [],
            },
        )
        return

    dataset_ids = [str(item).strip() for item in (data_scope.dataset_ids or []) if str(item).strip()]
    goal_lower = str(goal or "").lower()
    is_ontology_only_task = (
        not dataset_ids
        and any(kw in goal_lower for kw in (
            "ontology", "온톨로지", "클래스", "class", "스키마", "schema",
            "property", "속성", "relationship", "관계",
        ))
    )
    if not dataset_ids and not is_ontology_only_task:
        yield StreamEvent(
            event_type="error" if streaming else "complete",
            data={
                "run_id": run_id,
                "status": "clarification_required",
                "plan_id": plan_id,
                "plan": None,
                "preview": None,
                "report": None,
                "questions": [
                    {"id": "dataset_ids", "question": "Which dataset_ids should I use to satisfy the goal?", "required": True, "type": "string"}
                ],
                "validation_errors": ["data_scope.dataset_ids is required"],
                "validation_warnings": [],
            },
        )
        return

    # ── MCP client ──
    try:
        try:
            from mcp_servers.mcp_client import get_mcp_manager  # type: ignore[import-not-found]
        except Exception:
            from backend.mcp_servers.mcp_client import get_mcp_manager  # type: ignore[import-not-found]
    except Exception as exc:
        yield StreamEvent(
            event_type="error" if streaming else "complete",
            data={
                "run_id": run_id,
                "status": "clarification_required",
                "plan_id": plan_id,
                "plan": None,
                "preview": None,
                "report": None,
                "questions": [],
                "validation_errors": [f"MCP client unavailable: {exc}"],
                "validation_warnings": [],
            },
        )
        return

    mcp_manager = get_mcp_manager()

    if streaming:
        yield StreamEvent(
            event_type="start",
            data={"run_id": run_id, "goal": goal, "db_name": db_name, "dataset_ids": dataset_ids},
        )

    # ── State init (non-streaming baseline + streaming extras) ──
    principal_id = str(user_id or actor or "system").strip() or "system"
    principal_type = "user" if user_id else "system"
    ontology_session_id = f"ontology_{run_id}"
    state = _AgentState(
        db_name=db_name,
        branch=str(data_scope.branch or "").strip() or "main",
        dataset_ids=dataset_ids,
        goal=str(goal or "").strip(),
        principal_id=principal_id,
        principal_type=principal_type,
        ontology_session_id=ontology_session_id,
    )

    # ── Resume from previous run (plan_id supplied by client) ──
    if resume_plan_id and plan_registry:
        try:
            saved = await plan_registry.get_plan(plan_id=resume_plan_id, tenant_id=tenant_id)
            if saved and isinstance(saved.plan, dict):
                saved_plan = dict(saved.plan)
                agent_ctx = saved_plan.pop("_agent_context", None)
                state.plan_obj = saved_plan if saved_plan.get("definition_json") else None
                if isinstance(agent_ctx, dict):
                    state.knowledge_ledger = agent_ctx.get("knowledge_ledger") or {}
                    state.null_report = agent_ctx.get("null_report")
                    state.key_inference = agent_ctx.get("key_inference")
                    state.type_inference = agent_ctx.get("type_inference")
                    state.join_plan = agent_ctx.get("join_plan")
                    if agent_ctx.get("ontology_session_id"):
                        state.ontology_session_id = agent_ctx["ontology_session_id"]
                    state.working_ontology = agent_ctx.get("working_ontology")
                    state.schema_inference = agent_ctx.get("schema_inference")
                    state.mapping_suggestions = agent_ctx.get("mapping_suggestions")
                    saved_hashes = agent_ctx.get("tool_call_hashes")
                    if isinstance(saved_hashes, dict):
                        state.tool_call_hashes = {int(k): v for k, v in saved_hashes.items()}
                logger.info(
                    "Resumed agent state from plan_id=%s (plan_obj=%s, knowledge_keys=%d)",
                    resume_plan_id,
                    "yes" if state.plan_obj else "no",
                    len(state.knowledge_ledger),
                )
        except Exception as exc:
            logger.warning("Failed to load plan_id=%s for resume: %s", resume_plan_id, exc)

    # Dynamic step budget (non-streaming's formula)
    max_steps = min(60, 24 + max(0, len(state.dataset_ids) - 1) * 8)
    max_tool_calls_per_step = 20
    llm_meta: Optional[LLMCallMeta] = None
    notes: List[str] = []
    tool_warnings: List[str] = []
    tool_errors: List[str] = []
    consecutive_llm_failures = 0

    # Streaming dedup guards
    _consecutive_dup_steps = 0
    _MAX_CONSECUTIVE_DUP_STEPS = 3

    allowed_tools = list(_PIPELINE_AGENT_ALLOWED_TOOLS)
    system_prompt = _build_system_prompt(allowed_tools=allowed_tools)

    # Dynamic prompt limit
    model_id = str(selected_model or getattr(llm_gateway, "model", "") or "").strip()
    model_config = get_model_context_config(model_id)
    gateway_limit = int(getattr(llm_gateway, "max_prompt_chars", 0) or 0)
    if gateway_limit > 0:
        prompt_char_limit = int(min(gateway_limit, model_config.safe_prompt_chars) * 0.9)
    else:
        system_chars = len(system_prompt)
        prompt_char_limit = int((model_config.safe_prompt_chars - system_chars) * 0.85)
    prompt_budget = PromptBudget(total_chars=prompt_char_limit)

    # ── Prompt log ──
    state.prompt_items = [
        stable_json_dumps(_build_prompt_header(state=state, answers=answers, planner_hints=planner_hints, task_spec=task_spec))
    ]
    if state.last_observation:
        state.prompt_items.append(
            stable_json_dumps({"type": "bootstrap_observation", "observation": _mask_tool_observation(state.last_observation)})
        )

    # ── Main loop ──
    for step_idx in range(max_steps):
        # Step state → prompt
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
        await _maybe_compact_prompt_items(
            state=state, answers=answers, planner_hints=planner_hints,
            task_spec=task_spec, max_chars=prompt_char_limit,
            run_id=run_id, audit_store=audit_store, event_store=event_store,
        )
        user_prompt = _prompt_text(state.prompt_items)

        # Quota enforcement
        if data_policies and redis_service:
            model_for_quota = str(selected_model or getattr(llm_gateway, "model", "") or "").strip()
            if model_for_quota:
                await enforce_llm_quota(
                    redis_service=redis_service, tenant_id=tenant_id, user_id=user_id,
                    model_id=model_for_quota, system_prompt=system_prompt,
                    user_prompt=user_prompt, data_policies=data_policies,
                )

        if streaming:
            yield StreamEvent(
                event_type="thinking",
                data={
                    "run_id": run_id, "step": step_idx + 1, "max_steps": max_steps,
                    "message": _THINKING_MESSAGES[step_idx % len(_THINKING_MESSAGES)],
                },
            )

        # ── LLM call with 3× retry ──
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
            consecutive_llm_failures += 1
            tool_errors.append(str(exc))
            state.last_observation = {
                "error": "llm_output_invalid_json",
                "detail": str(exc),
                "hint": "Return ONLY valid JSON. Keep tool_calls <= 20 and avoid huge payloads.",
            }
            state.prompt_items.append(
                stable_json_dumps({"type": "llm_error", "step": step_idx + 1, "error_kind": "output_validation", "detail": str(exc)[:500]})
            )
            if consecutive_llm_failures >= 3:
                break
            continue
        except LLMRequestError as exc:
            consecutive_llm_failures += 1
            tool_errors.append(str(exc))
            state.last_observation = {"error": "llm_request_failed", "detail": str(exc)}
            state.prompt_items.append(
                stable_json_dumps({"type": "llm_error", "step": step_idx + 1, "error_kind": "request_error", "detail": str(exc)[:500]})
            )
            if consecutive_llm_failures >= 3:
                break
            continue
        except LLMUnavailableError as exc:
            tool_errors.append(str(exc))
            if streaming:
                yield StreamEvent(event_type="error", data={"error": f"LLM unavailable: {exc}", "run_id": run_id, "step": step_idx + 1})
            break

        # Append decision to prompt log
        state.prompt_items.append(
            stable_json_dumps({"type": "decision", "step": step_idx + 1, "decision": decision.model_dump(mode="json")})
        )
        notes.extend([str(n) for n in (decision.notes or []) if str(n or "").strip()])
        tool_warnings.extend([str(w) for w in (decision.warnings or []) if str(w or "").strip()])

        # ── ACTION: clarify ──
        if decision.action == "clarify":
            if _is_internal_budget_clarification(list(decision.questions or [])):
                state.last_observation = {"error": "internal step budget cannot be changed; proceed using available tools"}
                continue

            # ── Save DRAFT plan + agent context for session resume ──
            if not plan_id:
                plan_id = str(uuid4())
            if persist_plan and plan_registry:
                draft_plan: Dict[str, Any] = {}
                if isinstance(state.plan_obj, dict):
                    draft_plan = dict(state.plan_obj)
                else:
                    draft_plan = {
                        "goal": state.goal,
                        "data_scope": {"db_name": state.db_name, "branch": state.branch, "dataset_ids": state.dataset_ids},
                    }
                draft_plan["_agent_context"] = _build_agent_context_snapshot(state)
                try:
                    await plan_registry.upsert_plan(
                        plan_id=plan_id,
                        tenant_id=tenant_id,
                        status="DRAFT",
                        goal=state.goal,
                        db_name=state.db_name,
                        branch=state.branch,
                        plan=draft_plan,
                        created_by=actor,
                    )
                    logger.info("Saved DRAFT plan_id=%s for clarification resume", plan_id)
                except Exception as exc:
                    logger.warning("Failed to save DRAFT plan_id=%s: %s", plan_id, exc)

            yield StreamEvent(
                event_type="clarification",
                data={
                    "run_id": run_id,
                    "status": "clarification_required",
                    "plan_id": plan_id,
                    "plan": state.plan_obj,
                    "preview": None,
                    "report": state.null_report,
                    "questions": [q.model_dump(mode="json") for q in (decision.questions or [])],
                    "validation_errors": tool_errors or ["planner requested clarification"],
                    "validation_warnings": tool_warnings,
                    "llm": _build_llm_meta_dict(llm_meta),
                },
            )
            return

        # ── ACTION: respond ──
        if decision.action == "respond":
            yield StreamEvent(
                event_type="complete",
                data={
                    "run_id": run_id,
                    "status": "success",
                    "message": str(decision.message or ""),
                    "reasoning": str(decision.reasoning or "") if decision.reasoning else None,
                    "plan_id": plan_id,
                    "plan": state.plan_obj if isinstance(state.plan_obj, dict) else None,
                    "preview": None,
                    "report": state.null_report,
                    "questions": [],
                    "validation_errors": [],
                    "validation_warnings": list(tool_warnings),
                    "planner": {"confidence": float(decision.confidence), "notes": notes},
                    "llm": _build_llm_meta_dict(llm_meta),
                },
            )
            return

        # ── ACTION: finish ──
        # If LLM says "finish" but also provides tool_calls, it actually
        # wants to call tools first.  Override to call_tool so the tools
        # execute before we terminate.
        if decision.action == "finish" and decision.tool_calls:
            logger.info("finish+tool_calls override → call_tool (step=%d tools=%s)",
                        step_idx + 1, [tc.tool for tc in decision.tool_calls])
            decision.action = "call_tool"

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

                # Refutation gate
                state.prompt_items.append(
                    stable_json_dumps({"type": "tool_call", "step": step_idx + 1, "tool": "plan_refute_claims", "args": {"sample_limit": 400}})
                )
                refute_observation = await _execute_tool_call(
                    tool_name="plan_refute_claims", args={"sample_limit": 400},
                    state=state, mcp_manager=mcp_manager, allowed_tools=allowed_tools,
                    planner_hints=planner_hints, tool_errors=tool_errors,
                    tool_warnings=tool_warnings, step_idx=step_idx,
                )
                state.prompt_items.append(
                    stable_json_dumps({"type": "tool_output", "step": step_idx + 1, "tool": "plan_refute_claims", "output": refute_observation})
                )
                if isinstance(refute_observation, dict):
                    refute_errors = refute_observation.get("errors")
                    if refute_observation.get("status") == "invalid" or (isinstance(refute_errors, list) and refute_errors):
                        refute_error_msgs = refute_errors if isinstance(refute_errors, list) else []
                        tool_warnings.extend([f"refutation: {str(e)}" for e in refute_error_msgs[:5]])
                    refute_warnings = refute_observation.get("warnings")
                    if isinstance(refute_warnings, list):
                        tool_warnings.extend([str(w) for w in refute_warnings if str(w or "").strip()])

                # Pipeline status check
                pipeline_issue = _pipeline_has_unresolved_status(state)
                if pipeline_issue:
                    tool_warnings.append(
                        f"pipeline status: {pipeline_issue.get('issue', 'unresolved')} "
                        f"node={pipeline_issue.get('node_id', '?')} stage={pipeline_issue.get('stage', '?')}"
                    )

                # Persist plan
                if not plan_id:
                    plan_id = str(uuid4())
                if persist_plan:
                    await plan_registry.upsert_plan(
                        plan_id=plan_id, tenant_id=tenant_id, status="COMPILED",
                        goal=str(validation.plan.goal or ""),
                        db_name=str(validation.plan.data_scope.db_name or ""),
                        branch=str(validation.plan.data_scope.branch or "") or None,
                        plan=validation.plan.model_dump(mode="json"),
                        created_by=actor,
                    )

                yield StreamEvent(
                    event_type="complete",
                    data={
                        "run_id": run_id,
                        "status": "success",
                        "message": str(decision.message or "") or None,
                        "reasoning": str(decision.reasoning or "") if decision.reasoning else None,
                        "plan_id": plan_id,
                        "plan": validation.plan.model_dump(mode="json"),
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
                        "llm": _build_llm_meta_dict(llm_meta),
                    },
                )
                return

            # Report-only finish
            if isinstance(state.null_report, dict):
                yield StreamEvent(
                    event_type="complete",
                    data={
                        "run_id": run_id,
                        "status": "success",
                        "message": str(decision.message or "") or None,
                        "reasoning": str(decision.reasoning or "") if decision.reasoning else None,
                        "plan_id": plan_id,
                        "plan": None,
                        "preview": None,
                        "report": state.null_report,
                        "questions": [],
                        "validation_errors": [],
                        "validation_warnings": list(tool_warnings),
                        "planner": {"confidence": float(decision.confidence), "notes": notes},
                        "llm": _build_llm_meta_dict(llm_meta),
                    },
                )
                return

            # Ontology-only finish
            if isinstance(state.working_ontology, dict) or isinstance(state.schema_inference, dict):
                final_ontology = state.working_ontology
                if not final_ontology and state.ontology_session_id:
                    preview_result = await _execute_tool_call(
                        tool_name="ontology_preview",
                        args={"session_id": state.ontology_session_id},
                        state=state, mcp_manager=mcp_manager, allowed_tools=allowed_tools,
                        planner_hints=planner_hints, tool_errors=tool_errors,
                        tool_warnings=tool_warnings, step_idx=step_idx,
                    )
                    if isinstance(preview_result, dict) and preview_result.get("status") == "success":
                        final_ontology = preview_result.get("ontology")

                yield StreamEvent(
                    event_type="complete",
                    data={
                        "run_id": run_id,
                        "status": "success",
                        "message": str(decision.message or "") or "온톨로지 설계가 완료되었습니다.",
                        "reasoning": str(decision.reasoning or "") if decision.reasoning else None,
                        "plan_id": None,
                        "plan": None,
                        "preview": None,
                        "report": None,
                        "ontology": final_ontology,
                        "schema_inference": state.schema_inference,
                        "mapping_suggestions": state.mapping_suggestions,
                        "questions": [],
                        "validation_errors": [],
                        "validation_warnings": list(tool_warnings),
                        "planner": {"confidence": float(decision.confidence), "notes": notes},
                        "llm": _build_llm_meta_dict(llm_meta),
                    },
                )
                return

            # Message-only finish
            if str(decision.message or "").strip():
                yield StreamEvent(
                    event_type="complete",
                    data={
                        "run_id": run_id,
                        "status": "success",
                        "message": str(decision.message or ""),
                        "reasoning": str(decision.reasoning or "") if decision.reasoning else None,
                        "plan_id": plan_id,
                        "plan": None,
                        "preview": None,
                        "report": None,
                        "questions": [],
                        "validation_errors": [],
                        "validation_warnings": list(tool_warnings),
                        "planner": {"confidence": float(decision.confidence), "notes": notes},
                        "llm": _build_llm_meta_dict(llm_meta),
                    },
                )
                return
            state.last_observation = {"error": "cannot finish: produce a report, a validated plan, or a response message"}
            continue

        # ── ACTION: call_tool ──
        tool_calls = list(decision.tool_calls or [])
        if not tool_calls:
            tool_calls = [AutonomousPipelineAgentToolCall(tool=str(decision.tool or ""), args=dict(decision.args or {}))]

        executed_tools: List[str] = []
        stop_reason: Optional[str] = None
        batch_last: Optional[Dict[str, Any]] = None
        batch_last_by_alias: Dict[str, Dict[str, Any]] = {}
        truncated_tool_calls = max(0, len(tool_calls) - max_tool_calls_per_step)
        _step_all_dup = True

        for call in tool_calls[:max_tool_calls_per_step]:
            tool_name = str(call.tool or "").strip()
            if not tool_name:
                if not streaming:
                    state.last_observation = {"error": "tool name missing"}
                    stop_reason = "missing_tool"
                    break
                continue

            try:
                args = _resolve_batch_placeholders(
                    dict(call.args or {}), last=batch_last, last_by_alias=batch_last_by_alias,
                )
            except Exception as exc:
                state.last_observation = {"error": f"failed to resolve batch tool refs for {tool_name}: {exc}"}
                stop_reason = "bad_batch_ref"
                break

            # ── Pre-execution dedup ──
            _pre_dup = _deduplicate_tool_call(state, tool_name, args, step_idx)
            if _pre_dup is not None:
                state.prompt_items.append(
                    stable_json_dumps({"type": "tool_output_ref", "step": step_idx + 1, "tool": tool_name,
                                       "ref_step": _pre_dup, "note": "Duplicate call skipped"})
                )
                if streaming:
                    yield StreamEvent(
                        event_type="tool_end",
                        data={
                            "run_id": run_id, "step": step_idx + 1, "tool": tool_name,
                            "success": True, "error": None,
                            "observation": {"note": f"Duplicate of step {_pre_dup + 1}, skipped"},
                        },
                    )
                continue

            # ── Knowledge ledger cache (Layer 2 — resume safety net) ──
            if not args.get("force_refresh") and state.knowledge_ledger:
                cached_obs = _check_knowledge_cache(state.knowledge_ledger, tool_name, args)
                if cached_obs is not None:
                    state.prompt_items.append(
                        stable_json_dumps({"type": "tool_call", "step": step_idx + 1, "tool": tool_name, "args": args})
                    )
                    state.prompt_items.append(
                        stable_json_dumps({"type": "tool_output", "step": step_idx + 1, "tool": tool_name, "output": cached_obs})
                    )
                    state.last_observation = cached_obs
                    executed_tools.append(tool_name)
                    batch_last = cached_obs
                    batch_last_by_alias[_tool_alias(tool_name)] = cached_obs
                    if streaming:
                        yield StreamEvent(
                            event_type="tool_start",
                            data={
                                "run_id": run_id, "step": step_idx + 1, "max_steps": max_steps,
                                "tool": tool_name,
                                "args": {k: v for k, v in args.items() if k != "plan"},
                                "reasoning": decision.notes[0] if decision.notes else None,
                                "cached": True,
                            },
                        )
                        yield StreamEvent(
                            event_type="tool_end",
                            data={
                                "run_id": run_id, "step": step_idx + 1, "tool": tool_name,
                                "success": True, "error": None,
                                "observation": _mask_tool_observation(cached_obs),
                                "cached": True,
                            },
                        )
                    logger.info(
                        "Knowledge cache hit: tool=%s dataset=%s step=%d",
                        tool_name, args.get("dataset_id", "N/A"), step_idx + 1,
                    )
                    continue

            _step_all_dup = False

            # ── Consecutive dup guard ──
            if _consecutive_dup_steps >= _MAX_CONSECUTIVE_DUP_STEPS:
                state.prompt_items.append(stable_json_dumps({
                    "type": "system_hint",
                    "message": (
                        "⚠️ You have been repeating the same tool calls. All data has already been explored. "
                        "You MUST now either: (1) action='clarify' to ask the user questions, "
                        "or (2) action='call_tool' with a NEW tool you haven't called yet, "
                        "or (3) action='finish' with a plan. Do NOT repeat dataset_sample/dataset_profile."
                    ),
                }))
                _consecutive_dup_steps = 0

            if streaming:
                yield StreamEvent(
                    event_type="tool_start",
                    data={
                        "run_id": run_id, "step": step_idx + 1, "max_steps": max_steps,
                        "tool": tool_name,
                        "args": {k: v for k, v in args.items() if k != "plan"},
                        "reasoning": decision.notes[0] if decision.notes else None,
                    },
                )

            executed_tools.append(tool_name)
            state.prompt_items.append(stable_json_dumps({"type": "tool_call", "step": step_idx + 1, "tool": tool_name, "args": args}))

            # Execute
            observation = await _execute_tool_call(
                tool_name=tool_name, args=args,
                state=state, mcp_manager=mcp_manager, allowed_tools=allowed_tools,
                planner_hints=planner_hints, tool_errors=tool_errors,
                tool_warnings=tool_warnings, step_idx=step_idx,
            )

            # ── Evict failed calls from dedup cache so LLM can retry ──
            _tool_failed = isinstance(observation, dict) and observation.get("error")
            if _tool_failed:
                _dedup_evict_on_failure(state, tool_name, args)

            # Knowledge + summarize
            if isinstance(observation, dict):
                _extract_knowledge(tool_name, observation, state.knowledge_ledger)
            summarized = _summarize_tool_output(tool_name, observation) if isinstance(observation, dict) else observation
            state.prompt_items.append(
                stable_json_dumps({"type": "tool_output", "step": step_idx + 1, "tool": tool_name, "output": summarized})
            )

            if streaming:
                tool_error = observation.get("error") if isinstance(observation, dict) else None
                yield StreamEvent(
                    event_type="tool_end",
                    data={
                        "run_id": run_id, "step": step_idx + 1, "tool": tool_name,
                        "success": not tool_error, "error": tool_error,
                        "observation": _mask_tool_observation(observation) if isinstance(observation, dict) else observation,
                    },
                )

                # plan_update event
                if tool_name.startswith("plan_") and isinstance(state.plan_obj, dict) and not (isinstance(observation, dict) and observation.get("error")):
                    definition = state.plan_obj.get("definition_json", {})
                    nodes = definition.get("nodes", [])
                    edges = definition.get("edges", [])
                    yield StreamEvent(
                        event_type="plan_update",
                        data={
                            "run_id": run_id, "step": step_idx + 1, "tool": tool_name,
                            "plan": {
                                "definition_json": {"nodes": nodes, "edges": edges},
                                "outputs": state.plan_obj.get("outputs", []),
                            },
                            "node_count": len(nodes), "edge_count": len(edges),
                        },
                    )

                # preview_update event
                if tool_name == "plan_preview" and isinstance(observation, dict) and not observation.get("error"):
                    preview_data = observation.get("preview") or observation.get("result") or {}
                    node_id = args.get("node_id")
                    if not node_id and isinstance(state.plan_obj, dict):
                        definition = state.plan_obj.get("definition_json", {})
                        nodes = definition.get("nodes", [])
                        if isinstance(nodes, list) and nodes:
                            last_node = nodes[-1] if isinstance(nodes[-1], dict) else None
                            node_id = str((last_node or {}).get("id") or "").strip() or node_id
                    preview_columns: list = []
                    preview_rows: list = []
                    if isinstance(preview_data, dict):
                        raw_columns = preview_data.get("columns", [])
                        if isinstance(raw_columns, list):
                            preview_columns = [
                                {"name": str(c.get("name") or c) if isinstance(c, dict) else str(c),
                                 "type": str(c.get("type", "string")) if isinstance(c, dict) else "string"}
                                for c in raw_columns
                            ]
                        raw_rows = preview_data.get("rows", preview_data.get("data", []))
                        if isinstance(raw_rows, list):
                            preview_rows = raw_rows[:100]
                    yield StreamEvent(
                        event_type="preview_update",
                        data={
                            "run_id": run_id, "step": step_idx + 1, "node_id": node_id,
                            "preview_status": observation.get("status"),
                            "preview_policy": observation.get("preview_policy") if isinstance(observation.get("preview_policy"), dict) else None,
                            "hint": observation.get("hint"),
                            "preview": {"columns": preview_columns, "rows": preview_rows, "row_count": len(preview_rows)},
                        },
                    )

                # ontology_update event
                if (tool_name.startswith("ontology_") or tool_name.startswith("objectify_")) and isinstance(observation, dict) and not observation.get("error"):
                    ontology_event_data: Dict[str, Any] = {"run_id": run_id, "step": step_idx + 1, "tool": tool_name}
                    if isinstance(state.working_ontology, dict):
                        ontology_event_data["ontology"] = state.working_ontology
                    if isinstance(state.schema_inference, dict):
                        ontology_event_data["schema_inference"] = state.schema_inference
                    if isinstance(state.mapping_suggestions, dict):
                        ontology_event_data["mapping_suggestions"] = state.mapping_suggestions
                    if tool_name in {"objectify_run", "objectify_wait"} and isinstance(observation, dict):
                        ontology_event_data["objectify_status"] = {
                            "job_id": observation.get("job_id"),
                            "status": observation.get("status"),
                            "instances_created": observation.get("instances_created"),
                        }
                    yield StreamEvent(event_type="ontology_update", data=ontology_event_data)

            # ── Batch stop conditions (non-streaming's richer logic) ──
            if not isinstance(observation, dict):
                stop_reason = "invalid_observation"
                break
            batch_last = observation
            batch_last_by_alias[_tool_alias(tool_name)] = observation
            errors = observation.get("errors")
            if observation.get("error"):
                error_text = str(observation.get("error") or "").strip()
                if error_text.startswith("tool not allowed:"):
                    continue
                stop_reason = "tool_error"
                break
            if observation.get("status") == "invalid":
                stop_reason = "invalid_status"
                break
            if tool_name.startswith("pipeline_"):
                status_value = str(observation.get("status") or "").strip().lower()
                if status_value and status_value != "success":
                    stop_reason = f"pipeline_status_{status_value}"
                    break
            if isinstance(errors, list) and errors:
                stop_reason = "validation_errors"
                break

        # Batch summary
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
                stable_json_dumps({"type": "batch_summary", "step": step_idx + 1, "executed_tools": executed_tools, "stop_reason": stop_reason})
            )

        # Dup step counter
        if _step_all_dup:
            _consecutive_dup_steps += 1
        else:
            _consecutive_dup_steps = 0

        continue

    # ── Loop exhausted / LLM error. Return best-effort result. ──
    if isinstance(state.plan_obj, dict):
        try:
            plan_model = PipelinePlan.model_validate(state.plan_obj)
        except Exception as exc:
            yield StreamEvent(
                event_type="complete",
                data={
                    "run_id": run_id, "status": "failed", "plan_id": plan_id,
                    "plan": None, "preview": None, "report": state.null_report,
                    "questions": [], "validation_errors": tool_errors or [str(exc)],
                    "validation_warnings": tool_warnings,
                    "llm": _build_llm_meta_dict(llm_meta),
                },
            )
            return

        validation = await validate_pipeline_plan(
            plan=plan_model, dataset_registry=dataset_registry,
            db_name=str(plan_model.data_scope.db_name or ""),
            branch=str(plan_model.data_scope.branch or "") or None,
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
                plan_id=plan_id, tenant_id=tenant_id,
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
        yield StreamEvent(
            event_type="complete",
            data={
                "run_id": run_id, "status": final_status, "plan_id": plan_id,
                "plan": validation.plan.model_dump(mode="json"),
                "preview": None, "report": state.null_report,
                "pipeline_id": state.pipeline_id,
                "last_build_job_id": state.last_build_job_id,
                "last_build_artifact_id": state.last_build_artifact_id,
                "pipeline_status": _summarize_pipeline_progress(state),
                "questions": [], "validation_errors": validation_errors_out,
                "validation_warnings": list(tool_warnings) + list(validation.warnings or []),
                "preflight": validation.preflight,
                "planner": {"confidence": None, "notes": notes or None},
                "llm": _build_llm_meta_dict(llm_meta),
            },
        )
        return

    # Loop exhausted but ontology result exists
    if isinstance(state.working_ontology, dict) or isinstance(state.schema_inference, dict):
        yield StreamEvent(
            event_type="complete",
            data={
                "run_id": run_id, "status": "partial", "plan_id": None,
                "plan": None, "preview": None, "report": None,
                "ontology": state.working_ontology,
                "schema_inference": state.schema_inference,
                "mapping_suggestions": state.mapping_suggestions,
                "questions": [], "validation_errors": [],
                "validation_warnings": tool_warnings,
                "planner": {"confidence": None, "notes": notes or None},
                "llm": _build_llm_meta_dict(llm_meta),
            },
        )
        return

    yield StreamEvent(
        event_type="complete",
        data={
            "run_id": run_id, "status": "failed", "plan_id": plan_id,
            "plan": None, "preview": None, "report": state.null_report,
            "questions": [], "validation_errors": tool_errors or ["pipeline agent did not complete"],
            "validation_warnings": tool_warnings,
            "planner": {"confidence": None, "notes": notes or None},
            "llm": _build_llm_meta_dict(llm_meta),
        },
    )


async def run_pipeline_agent_mcp_autonomous(
    *,
    goal: str,
    data_scope: PipelinePlanDataScope,
    answers: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
    task_spec: Optional[Dict[str, Any]] = None,
    resume_plan_id: Optional[str] = None,
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
    event_store: Optional[EventStore] = None,
    dataset_registry: DatasetRegistry,
    plan_registry: PipelinePlanRegistry,
) -> Dict[str, Any]:
    """Non-streaming wrapper: collects the terminal event from the unified core loop."""
    result: Dict[str, Any] = {"run_id": "", "status": "failed"}
    async for event in _run_agent_core(
        goal=goal, data_scope=data_scope, answers=answers,
        planner_hints=planner_hints, task_spec=task_spec,
        resume_plan_id=resume_plan_id,
        persist_plan=persist_plan, actor=actor, tenant_id=tenant_id,
        user_id=user_id, data_policies=data_policies,
        selected_model=selected_model, allowed_models=allowed_models,
        llm_gateway=llm_gateway, redis_service=redis_service,
        audit_store=audit_store, event_store=event_store,
        dataset_registry=dataset_registry, plan_registry=plan_registry,
        streaming=False,
    ):
        if event.event_type in ("complete", "clarification", "error"):
            result = event.data
    return result


async def run_pipeline_agent_streaming(
    *,
    goal: str,
    data_scope: PipelinePlanDataScope,
    answers: Optional[Dict[str, Any]],
    planner_hints: Optional[Dict[str, Any]],
    task_spec: Optional[Dict[str, Any]] = None,
    resume_plan_id: Optional[str] = None,
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
    event_store: Optional[EventStore] = None,
    dataset_registry: DatasetRegistry,
    plan_registry: PipelinePlanRegistry,
):
    """SSE streaming wrapper: yields all events from the unified core loop."""
    async for event in _run_agent_core(
        goal=goal, data_scope=data_scope, answers=answers,
        planner_hints=planner_hints, task_spec=task_spec,
        resume_plan_id=resume_plan_id,
        persist_plan=persist_plan, actor=actor, tenant_id=tenant_id,
        user_id=user_id, data_policies=data_policies,
        selected_model=selected_model, allowed_models=allowed_models,
        llm_gateway=llm_gateway, redis_service=redis_service,
        audit_store=audit_store, event_store=event_store,
        dataset_registry=dataset_registry, plan_registry=plan_registry,
        streaming=True,
    ):
        yield event
