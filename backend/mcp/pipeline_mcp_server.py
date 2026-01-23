#!/usr/bin/env python3
"""
Pipeline MCP Server

Exposes deterministic "plan builder" + profiling/preview utilities as MCP tools,
so an internal planner can assemble PipelinePlan artifacts via tool calls.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from mcp.server import InitializationOptions, Server
from mcp.server.stdio import stdio_server
from mcp.types import ServerCapabilities, Tool, ToolsCapability

# Import paths depend on whether we run from source (repo layout) or from a container image.
# - repo layout: <repo>/backend/mcp -> add <repo>/backend
# - container layout: /app/backend/mcp -> add /app (bff lives at /app/bff)
_this_file = Path(__file__).resolve()
_backend_root = _this_file.parents[1]
_repo_root = _this_file.parents[2] if len(_this_file.parents) > 2 else _backend_root
for _path in (str(_backend_root), str(_repo_root)):
    if _path and _path not in sys.path:
        sys.path.append(_path)

from bff.services.pipeline_context_pack import build_pipeline_context_pack  # noqa: E402
from bff.services.pipeline_join_evaluator import evaluate_pipeline_joins  # noqa: E402
from bff.services.pipeline_plan_validation import validate_pipeline_plan  # noqa: E402
from shared.models.pipeline_plan import PipelinePlan  # noqa: E402
from shared.services.dataset_profile_registry import DatasetProfileRegistry  # noqa: E402
from shared.services.dataset_registry import DatasetRegistry  # noqa: E402
from shared.services.pipeline_executor import PipelineExecutor  # noqa: E402
from shared.services.pipeline_preview_inspector import inspect_preview  # noqa: E402
from shared.services.pipeline_plan_builder import (  # noqa: E402
    PipelinePlanBuilderError,
    add_edge,
    add_cast,
    add_compute,
    add_dedupe,
    add_drop,
    add_filter,
    add_input,
    add_join,
    add_normalize,
    add_output,
    add_rename,
    add_regex_replace,
    add_select,
    add_transform,
    delete_edge,
    delete_node,
    new_plan,
    set_node_inputs,
    update_node_metadata,
    update_output,
    validate_structure,
)
from shared.services.pipeline_type_inference import (  # noqa: E402
    common_join_key_type,
    infer_xsd_type_with_confidence,
    normalize_declared_type,
)
from shared.utils.llm_safety import mask_pii  # noqa: E402

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def _build_null_report_from_context_pack(
    context_pack: Dict[str, Any],
    *,
    dataset_ids: Optional[List[str]] = None,
    max_columns: int = 50,
) -> Dict[str, Any]:
    wanted = {str(item).strip() for item in (dataset_ids or []) if str(item).strip()}
    selected = context_pack.get("selected_datasets") if isinstance(context_pack, dict) else None
    datasets: list[dict[str, Any]] = []
    if isinstance(selected, list):
        for ds in selected:
            if not isinstance(ds, dict):
                continue
            ds_id = str(ds.get("dataset_id") or "").strip()
            if wanted and ds_id and ds_id not in wanted:
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
            columns.sort(
                key=lambda item: (
                    float(item.get("null_ratio") or 0.0),
                    float(item.get("missing_ratio") or 0.0),
                ),
                reverse=True,
            )
            columns = columns[: max(0, int(max_columns or 0))]
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


def _coerce_context_pack(context_pack: Any) -> Dict[str, Any]:
    return context_pack if isinstance(context_pack, dict) else {}


def _filter_selected_datasets(context_pack: Dict[str, Any], *, dataset_ids: Optional[List[str]]) -> List[Dict[str, Any]]:
    selected = context_pack.get("selected_datasets")
    if not isinstance(selected, list):
        return []
    wanted = {str(item).strip() for item in (dataset_ids or []) if str(item).strip()}
    out: List[Dict[str, Any]] = []
    for item in selected:
        if not isinstance(item, dict):
            continue
        ds_id = str(item.get("dataset_id") or "").strip()
        if wanted and ds_id and ds_id not in wanted:
            continue
        out.append(item)
    return out


def _context_pack_key_inference(
    context_pack: Dict[str, Any],
    *,
    dataset_ids: Optional[List[str]] = None,
    max_pk_candidates: int = 6,
    max_fk_candidates: int = 25,
) -> Dict[str, Any]:
    selected = _filter_selected_datasets(context_pack, dataset_ids=dataset_ids)
    pk: List[Dict[str, Any]] = []
    for ds in selected:
        pk_candidates = ds.get("pk_candidates") if isinstance(ds.get("pk_candidates"), list) else []
        pk.append(
            {
                "dataset_id": ds.get("dataset_id"),
                "name": ds.get("name"),
                "row_count": ds.get("row_count"),
                "pk_candidates": [
                    item
                    for item in pk_candidates[: max(0, int(max_pk_candidates))]
                    if isinstance(item, dict)
                ],
            }
        )

    suggestions = context_pack.get("integration_suggestions")
    if not isinstance(suggestions, dict):
        suggestions = {}
    fk_candidates = suggestions.get("foreign_key_candidates")
    if not isinstance(fk_candidates, list):
        fk_candidates = []
    fk_candidates = [item for item in fk_candidates if isinstance(item, dict)]
    fk_candidates.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)

    return {
        "primary_keys": pk,
        "foreign_keys": fk_candidates[: max(0, int(max_fk_candidates))],
        "notes": ["inference is sample-based; validate keys before using for canonical mappings"],
    }


def _extract_column_type(dataset: Dict[str, Any], column_name: str) -> Optional[str]:
    columns = dataset.get("columns")
    if not isinstance(columns, list):
        return None
    for col in columns:
        if not isinstance(col, dict):
            continue
        name = str(col.get("name") or "").strip()
        if not name or name != column_name:
            continue
        raw_type = col.get("type") or col.get("data_type")
        if raw_type:
            return normalize_declared_type(raw_type)
    return None


def _context_pack_type_inference(
    context_pack: Dict[str, Any],
    *,
    dataset_ids: Optional[List[str]] = None,
    max_columns: int = 60,
    max_samples: int = 80,
    join_plan: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    selected = _filter_selected_datasets(context_pack, dataset_ids=dataset_ids)
    per_dataset: List[Dict[str, Any]] = []
    inferred_by_ds: Dict[str, Dict[str, str]] = {}

    for ds in selected:
        ds_id = str(ds.get("dataset_id") or "").strip()
        sample_rows = ds.get("sample_rows")
        if not isinstance(sample_rows, list):
            sample_rows = []
        columns = ds.get("columns")
        if not isinstance(columns, list):
            columns = []

        col_items: List[Dict[str, Any]] = []
        inferred_types: Dict[str, str] = {}
        for col in columns[: max(0, int(max_columns))]:
            if not isinstance(col, dict):
                continue
            name = str(col.get("name") or "").strip()
            if not name:
                continue
            declared = normalize_declared_type(col.get("type") or col.get("data_type") or "xsd:string")
            values: List[Any] = []
            for row in sample_rows:
                if not isinstance(row, dict):
                    continue
                values.append(row.get(name))
            result = infer_xsd_type_with_confidence(values, max_samples=max_samples)
            inferred = normalize_declared_type(result.suggested_type)
            inferred_types[name] = inferred
            mismatch = declared != inferred and float(result.confidence or 0.0) >= 0.95
            col_items.append(
                {
                    "column": name,
                    "declared_type": declared,
                    "suggested_type": inferred,
                    "confidence": float(result.confidence),
                    "sample_size": int(result.sample_size),
                    "ratios": result.ratios,
                    "mismatch": bool(mismatch),
                }
            )

        inferred_by_ds[ds_id] = inferred_types
        per_dataset.append(
            {
                "dataset_id": ds.get("dataset_id"),
                "name": ds.get("name"),
                "column_count": len(columns),
                "columns": col_items,
            }
        )

    # Suggest minimal casts for join keys when types disagree.
    join_casts: List[Dict[str, Any]] = []
    plan_items = [item for item in (join_plan or []) if isinstance(item, dict)]
    if not plan_items:
        suggestions = context_pack.get("integration_suggestions")
        if not isinstance(suggestions, dict):
            suggestions = {}
        fk_candidates = suggestions.get("foreign_key_candidates")
        if not isinstance(fk_candidates, list):
            fk_candidates = []
        plan_items = [item for item in fk_candidates[:6] if isinstance(item, dict)]

    for item in plan_items:
        left_id = str(item.get("left_dataset_id") or item.get("child_dataset_id") or "").strip()
        right_id = str(item.get("right_dataset_id") or item.get("parent_dataset_id") or "").strip()
        left_col = str(item.get("left_column") or item.get("child_column") or "").strip()
        right_col = str(item.get("right_column") or item.get("parent_column") or "").strip()
        if not left_id or not right_id or not left_col or not right_col:
            continue

        left_ds = next((ds for ds in selected if str(ds.get("dataset_id") or "").strip() == left_id), None)
        right_ds = next((ds for ds in selected if str(ds.get("dataset_id") or "").strip() == right_id), None)
        if not left_ds or not right_ds:
            continue

        left_type = _extract_column_type(left_ds, left_col) or inferred_by_ds.get(left_id, {}).get(left_col) or "xsd:string"
        right_type = _extract_column_type(right_ds, right_col) or inferred_by_ds.get(right_id, {}).get(right_col) or "xsd:string"
        common = common_join_key_type(left_type, right_type)
        if common == normalize_declared_type(left_type) == normalize_declared_type(right_type):
            continue
        for ds_id, col_name, declared in (
            (left_id, left_col, left_type),
            (right_id, right_col, right_type),
        ):
            if normalize_declared_type(declared) == common:
                continue
            join_casts.append(
                {
                    "dataset_id": ds_id,
                    "column": col_name,
                    "type": common,
                    "reason": f"join key type alignment ({left_type} vs {right_type})",
                }
            )

    deduped: Dict[tuple[str, str, str], Dict[str, Any]] = {}
    for cast in join_casts:
        key = (str(cast.get("dataset_id") or ""), str(cast.get("column") or ""), str(cast.get("type") or ""))
        if not all(key):
            continue
        deduped[key] = cast

    return {
        "datasets": per_dataset,
        "join_key_cast_suggestions": list(deduped.values()),
        "notes": ["type inference is sample-based; casts are suggestions only"],
    }


class PipelineMCPServer:
    def __init__(self) -> None:
        self.server = Server("pipeline-mcp-server")
        self._dataset_registry: Optional[DatasetRegistry] = None
        self._profile_registry: Optional[DatasetProfileRegistry] = None
        self._setup_handlers()

    async def _ensure_registries(self) -> tuple[DatasetRegistry, DatasetProfileRegistry]:
        if self._dataset_registry is None:
            self._dataset_registry = DatasetRegistry()
            await self._dataset_registry.initialize()
        if self._profile_registry is None:
            self._profile_registry = DatasetProfileRegistry()
            await self._profile_registry.initialize()
        return self._dataset_registry, self._profile_registry

    def _setup_handlers(self) -> None:
        @self.server.list_tools()
        async def list_tools() -> List[Tool]:
            tool_specs: List[Dict[str, Any]] = [
                {
                    "name": "context_pack_build",
                    "description": "Build a safe pipeline context pack (schemas/samples + join/pk/fk/cast/cleansing hints).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_name": {"type": "string"},
                            "branch": {"type": "string"},
                            "dataset_ids": {"type": "array", "items": {"type": "string"}},
                            "max_datasets_overview": {"type": "integer"},
                            "max_selected_datasets": {"type": "integer"},
                            "max_sample_rows": {"type": "integer"},
                            "max_join_candidates": {"type": "integer"},
                            "max_pk_candidates": {"type": "integer"},
                        },
                        "required": ["db_name"],
                    },
                },
                {
                    "name": "plan_new",
                    "description": "Create a new PipelinePlan JSON (empty nodes/edges).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "goal": {"type": "string"},
                            "db_name": {"type": "string"},
                            "branch": {"type": "string"},
                            "dataset_ids": {"type": "array", "items": {"type": "string"}},
                        },
                        "required": ["goal", "db_name"],
                    },
                },
                {
                    "name": "plan_add_input",
                    "description": "Add an input node for a dataset.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "dataset_id": {"type": "string"},
                            "dataset_name": {"type": "string"},
                            "dataset_branch": {"type": "string"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan"],
                    },
                },
                {
                    "name": "plan_add_transform",
                    "description": "Add a generic transform node (operation + metadata) with edges from input_node_ids.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "operation": {"type": "string"},
                            "input_node_ids": {"type": "array", "items": {"type": "string"}},
                            "metadata": {"type": "object"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "operation", "input_node_ids"],
                    },
                },
                {
                    "name": "plan_add_join",
                    "description": "Add a join transform node (LEFT then RIGHT edge order). Cross joins are rejected.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "left_node_id": {"type": "string"},
                            "right_node_id": {"type": "string"},
                            "left_keys": {"type": "array", "items": {"type": "string"}},
                            "right_keys": {"type": "array", "items": {"type": "string"}},
                            "join_type": {"type": "string"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "left_node_id", "right_node_id", "left_keys", "right_keys"],
                    },
                },
                {
                    "name": "plan_add_filter",
                    "description": "Add a filter transform node.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "expression": {"type": "string"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "expression"],
                    },
                },
                {
                    "name": "plan_add_compute",
                    "description": "Add a compute transform node (expression).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "expression": {"type": "string"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "expression"],
                    },
                },
                {
                    "name": "plan_add_cast",
                    "description": "Add a cast transform node. casts=[{column,type}].",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "casts": {"type": "array", "items": {"type": "object"}},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "casts"],
                    },
                },
                {
                    "name": "plan_add_rename",
                    "description": "Add a rename transform node. rename={src:dst}.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "rename": {"type": "object"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "rename"],
                    },
                },
                {
                    "name": "plan_add_select",
                    "description": "Add a select transform node.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "columns": {"type": "array", "items": {"type": "string"}},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "columns"],
                    },
                },
                {
                    "name": "plan_add_drop",
                    "description": "Add a drop transform node.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "columns": {"type": "array", "items": {"type": "string"}},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "columns"],
                    },
                },
                {
                    "name": "plan_add_dedupe",
                    "description": "Add a dedupe transform node.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "columns": {"type": "array", "items": {"type": "string"}},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "columns"],
                    },
                },
                {
                    "name": "plan_add_normalize",
                    "description": "Add a normalize transform node.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "columns": {"type": "array", "items": {"type": "string"}},
                            "trim": {"type": "boolean"},
                            "empty_to_null": {"type": "boolean"},
                            "whitespace_to_null": {"type": "boolean"},
                            "lowercase": {"type": "boolean"},
                            "uppercase": {"type": "boolean"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "columns"],
                    },
                },
                {
                    "name": "plan_add_regex_replace",
                    "description": "Add a regexReplace transform node. rules=[{column,pattern,replacement,flags?}].",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "rules": {"type": "array", "items": {"type": "object"}},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "input_node_id", "rules"],
                    },
                },
                {
                    "name": "plan_add_output",
                    "description": "Add an output node + outputs[] entry.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "input_node_id": {"type": "string"},
                            "output_name": {"type": "string"},
                            "output_kind": {"type": "string"},
                            "node_id": {"type": "string"},
                            "output_metadata": {"type": "object"},
                        },
                        "required": ["plan", "input_node_id", "output_name"],
                    },
                },
                {
                    "name": "plan_add_edge",
                    "description": "Add an edge from->to (idempotent). Incoming edge order can matter for joins.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "from_node_id": {"type": "string"},
                            "to_node_id": {"type": "string"},
                        },
                        "required": ["plan", "from_node_id", "to_node_id"],
                    },
                },
                {
                    "name": "plan_delete_edge",
                    "description": "Delete edges from->to (no-op if not found).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "from_node_id": {"type": "string"},
                            "to_node_id": {"type": "string"},
                        },
                        "required": ["plan", "from_node_id", "to_node_id"],
                    },
                },
                {
                    "name": "plan_set_node_inputs",
                    "description": "Replace all incoming edges to node_id with input_node_ids (in order).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "node_id": {"type": "string"},
                            "input_node_ids": {"type": "array", "items": {"type": "string"}},
                        },
                        "required": ["plan", "node_id", "input_node_ids"],
                    },
                },
                {
                    "name": "plan_update_node_metadata",
                    "description": "Patch node.metadata (merge by default, replace if requested).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "node_id": {"type": "string"},
                            "set": {"type": "object"},
                            "unset": {"type": "array", "items": {"type": "string"}},
                            "replace": {"type": "boolean"},
                        },
                        "required": ["plan", "node_id"],
                    },
                },
                {
                    "name": "plan_delete_node",
                    "description": "Delete a node and any incident edges; for output nodes also removes outputs[] entry.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "node_id": {"type": "string"},
                        },
                        "required": ["plan", "node_id"],
                    },
                },
                {
                    "name": "plan_update_output",
                    "description": "Patch outputs[] entry (by output_name); keeps output node metadata.outputName in sync if renamed.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "output_name": {"type": "string"},
                            "set": {"type": "object"},
                            "unset": {"type": "array", "items": {"type": "string"}},
                            "replace": {"type": "boolean"},
                        },
                        "required": ["plan", "output_name"],
                    },
                },
                {
                    "name": "plan_validate_structure",
                    "description": "Validate plan.definition_json structure without resolving dataset schemas.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {"plan": {"type": "object"}},
                        "required": ["plan"],
                    },
                },
                {
                    "name": "plan_validate",
                    "description": "Validate a PipelinePlan using dataset-aware preflight rules.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "require_output": {"type": "boolean"},
                        },
                        "required": ["plan"],
                    },
                },
                {
                    "name": "plan_preview",
                    "description": "Preview a plan via the deterministic PipelineExecutor (sample-safe).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "node_id": {"type": "string"},
                            "limit": {"type": "integer"},
                        },
                        "required": ["plan"],
                    },
                },
                {
                    "name": "preview_inspect",
                    "description": "Inspect a preview sample and propose cleansing suggestions (deterministic).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "preview": {"type": "object"},
                        },
                        "required": ["preview"],
                    },
                },
                {
                    "name": "plan_evaluate_joins",
                    "description": "Evaluate join nodes in a plan (coverage/explosion) using sample-safe execution.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "plan": {"type": "object"},
                            "node_filter": {"type": "string"},
                            "run_tables": {"type": "object"},
                        },
                        "required": ["plan"],
                    },
                },
                {
                    "name": "context_pack_null_report",
                    "description": "Generate a null/missing report from a context pack (no plan changes).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "context_pack": {"type": "object"},
                            "dataset_ids": {"type": "array", "items": {"type": "string"}},
                            "max_columns": {"type": "integer"},
                        },
                        "required": ["context_pack"],
                    },
                },
                {
                    "name": "context_pack_infer_keys",
                    "description": "Infer PK/FK candidates from a context pack (deterministic, sample-based).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "context_pack": {"type": "object"},
                            "dataset_ids": {"type": "array", "items": {"type": "string"}},
                            "max_pk_candidates": {"type": "integer"},
                            "max_fk_candidates": {"type": "integer"},
                        },
                        "required": ["context_pack"],
                    },
                },
                {
                    "name": "context_pack_infer_types",
                    "description": "Infer column types + join-key cast suggestions from a context pack (deterministic, sample-based).",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "context_pack": {"type": "object"},
                            "dataset_ids": {"type": "array", "items": {"type": "string"}},
                            "max_columns": {"type": "integer"},
                            "max_samples": {"type": "integer"},
                            "join_plan": {"type": "array", "items": {"type": "object"}},
                        },
                        "required": ["context_pack"],
                    },
                },
            ]
            return [Tool(**spec) for spec in tool_specs]

        @self.server.call_tool()
        async def call_tool(name: str, arguments: Dict[str, Any]) -> Any:
            try:
                if name == "context_pack_build":
                    dataset_registry, profile_registry = await self._ensure_registries()
                    db_name = str(arguments.get("db_name") or "").strip()
                    if not db_name:
                        raise PipelinePlanBuilderError("db_name is required")
                    pack = await build_pipeline_context_pack(
                        db_name=db_name,
                        branch=str(arguments.get("branch") or "").strip() or None,
                        dataset_ids=arguments.get("dataset_ids"),
                        dataset_registry=dataset_registry,
                        profile_registry=profile_registry,
                        max_datasets_overview=int(arguments.get("max_datasets_overview") or 20),
                        max_selected_datasets=int(arguments.get("max_selected_datasets") or 6),
                        max_sample_rows=int(arguments.get("max_sample_rows") or 20),
                        max_join_candidates=int(arguments.get("max_join_candidates") or 10),
                        max_pk_candidates=int(arguments.get("max_pk_candidates") or 6),
                    )
                    return {"context_pack": pack}

                if name == "plan_new":
                    plan = new_plan(
                        goal=str(arguments.get("goal") or ""),
                        db_name=str(arguments.get("db_name") or ""),
                        branch=str(arguments.get("branch") or "").strip() or None,
                        dataset_ids=arguments.get("dataset_ids"),
                    )
                    return {"plan": plan}

                if name == "plan_add_input":
                    plan = arguments.get("plan") or {}
                    result = add_input(
                        plan,
                        dataset_id=arguments.get("dataset_id"),
                        dataset_name=arguments.get("dataset_name"),
                        dataset_branch=arguments.get("dataset_branch"),
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_join":
                    plan = arguments.get("plan") or {}
                    result = add_join(
                        plan,
                        left_node_id=str(arguments.get("left_node_id") or ""),
                        right_node_id=str(arguments.get("right_node_id") or ""),
                        left_keys=arguments.get("left_keys") or [],
                        right_keys=arguments.get("right_keys") or [],
                        join_type=str(arguments.get("join_type") or "inner"),
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_transform":
                    plan = arguments.get("plan") or {}
                    result = add_transform(
                        plan,
                        operation=str(arguments.get("operation") or ""),
                        input_node_ids=arguments.get("input_node_ids") or [],
                        metadata=arguments.get("metadata") or {},
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_filter":
                    plan = arguments.get("plan") or {}
                    result = add_filter(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        expression=str(arguments.get("expression") or ""),
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_compute":
                    plan = arguments.get("plan") or {}
                    result = add_compute(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        expression=str(arguments.get("expression") or ""),
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_cast":
                    plan = arguments.get("plan") or {}
                    result = add_cast(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        casts=arguments.get("casts") or [],
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_rename":
                    plan = arguments.get("plan") or {}
                    result = add_rename(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        rename=arguments.get("rename") or {},
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_select":
                    plan = arguments.get("plan") or {}
                    result = add_select(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        columns=arguments.get("columns") or [],
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_drop":
                    plan = arguments.get("plan") or {}
                    result = add_drop(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        columns=arguments.get("columns") or [],
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_dedupe":
                    plan = arguments.get("plan") or {}
                    result = add_dedupe(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        columns=arguments.get("columns") or [],
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_normalize":
                    plan = arguments.get("plan") or {}
                    result = add_normalize(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        columns=arguments.get("columns") or [],
                        trim=bool(arguments.get("trim", True)),
                        empty_to_null=bool(arguments.get("empty_to_null", True)),
                        whitespace_to_null=bool(arguments.get("whitespace_to_null", True)),
                        lowercase=bool(arguments.get("lowercase", False)),
                        uppercase=bool(arguments.get("uppercase", False)),
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_regex_replace":
                    plan = arguments.get("plan") or {}
                    result = add_regex_replace(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        rules=arguments.get("rules") or [],
                        node_id=arguments.get("node_id"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_output":
                    plan = arguments.get("plan") or {}
                    result = add_output(
                        plan,
                        input_node_id=str(arguments.get("input_node_id") or ""),
                        output_name=str(arguments.get("output_name") or ""),
                        output_kind=str(arguments.get("output_kind") or "unknown"),
                        node_id=arguments.get("node_id"),
                        output_metadata=arguments.get("output_metadata"),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_add_edge":
                    plan = arguments.get("plan") or {}
                    result = add_edge(
                        plan,
                        from_node_id=str(arguments.get("from_node_id") or ""),
                        to_node_id=str(arguments.get("to_node_id") or ""),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_delete_edge":
                    plan = arguments.get("plan") or {}
                    result = delete_edge(
                        plan,
                        from_node_id=str(arguments.get("from_node_id") or ""),
                        to_node_id=str(arguments.get("to_node_id") or ""),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_set_node_inputs":
                    plan = arguments.get("plan") or {}
                    result = set_node_inputs(
                        plan,
                        node_id=str(arguments.get("node_id") or ""),
                        input_node_ids=arguments.get("input_node_ids") or [],
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_update_node_metadata":
                    plan = arguments.get("plan") or {}
                    result = update_node_metadata(
                        plan,
                        node_id=str(arguments.get("node_id") or ""),
                        set_fields=arguments.get("set"),
                        unset_fields=arguments.get("unset"),
                        replace=bool(arguments.get("replace", False)),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_delete_node":
                    plan = arguments.get("plan") or {}
                    result = delete_node(
                        plan,
                        node_id=str(arguments.get("node_id") or ""),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_update_output":
                    plan = arguments.get("plan") or {}
                    result = update_output(
                        plan,
                        output_name=str(arguments.get("output_name") or ""),
                        set_fields=arguments.get("set"),
                        unset_fields=arguments.get("unset"),
                        replace=bool(arguments.get("replace", False)),
                    )
                    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

                if name == "plan_validate_structure":
                    plan = arguments.get("plan") or {}
                    errors, warnings = validate_structure(plan)
                    return {"errors": errors, "warnings": warnings}

                if name == "plan_validate":
                    plan_obj = arguments.get("plan") or {}
                    try:
                        plan = PipelinePlan.model_validate(plan_obj)
                    except Exception as exc:
                        return {"status": "invalid", "errors": [str(exc)], "warnings": []}

                    db_name = str(plan.data_scope.db_name or "").strip()
                    if not db_name:
                        return {"status": "invalid", "errors": ["plan.data_scope.db_name is required"], "warnings": []}

                    dataset_registry, _ = await self._ensure_registries()
                    validation = await validate_pipeline_plan(
                        plan=plan,
                        dataset_registry=dataset_registry,
                        db_name=db_name,
                        branch=str(plan.data_scope.branch or "") or None,
                        require_output=bool(arguments.get("require_output", False)),
                        context_pack=None,
                    )
                    payload = {
                        "status": "success" if not validation.errors else "invalid",
                        "plan": validation.plan.model_dump(mode="json"),
                        "errors": list(validation.errors or []),
                        "warnings": list(validation.warnings or []),
                        "preflight": validation.preflight,
                        "compilation_report": validation.compilation_report.model_dump(mode="json"),
                    }
                    return payload

                if name == "plan_preview":
                    plan_obj = arguments.get("plan") or {}
                    # Validate shape early to avoid opaque executor errors.
                    errors, warnings = validate_structure(plan_obj)
                    if errors:
                        return {"status": "invalid", "errors": errors, "warnings": warnings}

                    try:
                        plan = PipelinePlan.model_validate(plan_obj)
                    except Exception as exc:
                        return {"status": "invalid", "errors": [str(exc)], "warnings": warnings}

                    db_name = str(plan.data_scope.db_name or "").strip()
                    if not db_name:
                        return {"status": "invalid", "errors": ["plan.data_scope.db_name is required"], "warnings": warnings}

                    dataset_registry, _ = await self._ensure_registries()
                    executor = PipelineExecutor(dataset_registry)
                    limit = int(arguments.get("limit") or 50)
                    node_id = str(arguments.get("node_id") or "").strip() or None

                    definition = dict(plan.definition_json or {})
                    preview_meta = dict(definition.get("__preview_meta__") or {})
                    preview_meta.setdefault("branch", str(plan.data_scope.branch or "") or "main")
                    preview_meta["sample_limit"] = max(1, min(limit, 200))
                    definition["__preview_meta__"] = preview_meta

                    preview = await executor.preview(definition=definition, db_name=db_name, node_id=node_id, limit=limit)
                    preview_masked = mask_pii(preview)
                    return {"status": "success", "preview": preview_masked, "warnings": warnings}

                if name == "preview_inspect":
                    preview = arguments.get("preview") or {}
                    if not isinstance(preview, dict):
                        return {"error": "preview must be an object"}
                    inspection = inspect_preview(preview)
                    return {"status": "success", "inspector": mask_pii(inspection)}

                if name == "plan_evaluate_joins":
                    plan_obj = arguments.get("plan") or {}
                    try:
                        plan = PipelinePlan.model_validate(plan_obj)
                    except Exception as exc:
                        return {"status": "invalid", "errors": [str(exc)], "warnings": []}

                    db_name = str(plan.data_scope.db_name or "").strip()
                    if not db_name:
                        return {"status": "invalid", "errors": ["plan.data_scope.db_name is required"], "warnings": []}

                    dataset_registry, _ = await self._ensure_registries()
                    evaluations, warnings = await evaluate_pipeline_joins(
                        definition_json=dict(plan.definition_json or {}),
                        db_name=db_name,
                        dataset_registry=dataset_registry,
                        node_filter=str(arguments.get("node_filter") or "").strip() or None,
                        run_tables=arguments.get("run_tables"),
                    )
                    return {
                        "status": "success",
                        "evaluations": [asdict(item) for item in evaluations],
                        "warnings": list(warnings or []),
                    }

                if name == "context_pack_null_report":
                    pack = arguments.get("context_pack") or {}
                    if not isinstance(pack, dict):
                        return {"error": "context_pack must be an object"}
                    report = _build_null_report_from_context_pack(
                        pack,
                        dataset_ids=arguments.get("dataset_ids"),
                        max_columns=int(arguments.get("max_columns") or 50),
                    )
                    return {"status": "success", "report": report}

                if name == "context_pack_infer_keys":
                    pack = arguments.get("context_pack") or {}
                    if not isinstance(pack, dict):
                        return {"error": "context_pack must be an object"}
                    inference = _context_pack_key_inference(
                        pack,
                        dataset_ids=arguments.get("dataset_ids"),
                        max_pk_candidates=int(arguments.get("max_pk_candidates") or 6),
                        max_fk_candidates=int(arguments.get("max_fk_candidates") or 25),
                    )
                    return {"status": "success", "inference": inference}

                if name == "context_pack_infer_types":
                    pack = arguments.get("context_pack") or {}
                    if not isinstance(pack, dict):
                        return {"error": "context_pack must be an object"}
                    join_plan = arguments.get("join_plan") if isinstance(arguments.get("join_plan"), list) else None
                    inference = _context_pack_type_inference(
                        pack,
                        dataset_ids=arguments.get("dataset_ids"),
                        max_columns=int(arguments.get("max_columns") or 60),
                        max_samples=int(arguments.get("max_samples") or 80),
                        join_plan=join_plan,
                    )
                    return {"status": "success", "inference": inference}

                return {"error": f"Unknown tool: {name}"}

            except PipelinePlanBuilderError as exc:
                return {"error": str(exc)}
            except Exception as exc:
                logger.exception("pipeline_mcp tool failed name=%s", name)
                return {"error": str(exc)}

    async def run(self) -> None:
        async with stdio_server() as (read_stream, write_stream):
            # MCP Python SDK (>=1.0) requires explicit initialization options.
            init = InitializationOptions(
                server_name="pipeline-mcp-server",
                server_version=os.environ.get("SPICE_VERSION", "0.1.0"),
                capabilities=ServerCapabilities(tools=ToolsCapability()),
            )
            await self.server.run(read_stream, write_stream, init)


async def main() -> None:
    server = PipelineMCPServer()
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())
