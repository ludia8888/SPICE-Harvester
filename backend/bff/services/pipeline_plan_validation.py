"""
Pipeline plan validation against pipeline definition rules + preflight.

Uses existing pipeline definition validation/preflight utilities and returns
machine-readable diagnostics for planners/repair loops.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from shared.models.agent_plan_report import PlanCompilationReport, PlanDiagnostic, PlanDiagnosticSeverity
from shared.models.pipeline_plan import PipelinePlan
from shared.models.pipeline_task_spec import PipelineTaskSpec
from shared.services.dataset_registry import DatasetRegistry
from shared.services.pipeline_dataset_utils import normalize_dataset_selection, resolve_dataset_version
from shared.services.pipeline_graph_utils import build_incoming, normalize_edges, normalize_nodes, topological_sort
from shared.services.pipeline_preflight_utils import (
    SchemaInfo,
    _apply_cast,
    _apply_compute,
    _apply_drop,
    _apply_group_by,
    _apply_join,
    _apply_rename,
    _apply_select,
    _apply_union,
    _apply_window,
    _normalize_column_list,
    _schema_for_input,
)
from shared.services.pipeline_transform_spec import normalize_operation, normalize_union_mode, resolve_join_spec
from shared.services.pipeline_task_spec_policy import clamp_task_spec, validate_plan_against_task_spec
from shared.utils.canonical_json import sha256_canonical_json_prefixed

from bff.routers import pipeline as pipeline_router


@dataclass(frozen=True)
class PipelinePlanValidationResult:
    plan: PipelinePlan
    errors: List[str]
    warnings: List[str]
    preflight: Dict[str, Any]
    compilation_report: PlanCompilationReport


@dataclass(frozen=True)
class InputNodeInfo:
    node_id: str
    dataset_id: Optional[str]
    dataset_name: Optional[str]
    row_count: Optional[int]
    column_profiles: Dict[str, Any]


def _ensure_output_nodes(definition_json: Dict[str, Any], outputs: List[Any]) -> Dict[str, Any]:
    nodes_raw = definition_json.get("nodes")
    if not isinstance(nodes_raw, list) or not nodes_raw:
        return definition_json
    nodes = [dict(node) for node in nodes_raw if isinstance(node, dict)]
    edges = normalize_edges(definition_json.get("edges"))
    node_by_id = normalize_nodes(nodes)
    if not node_by_id:
        return definition_json
    existing_ids = set(node_by_id.keys())
    output_nodes = [node for node in nodes if node.get("type") == "output"]
    outgoing = {edge["from"] for edge in edges if edge.get("from")}
    sink_ids = [
        node_id
        for node_id, node in node_by_id.items()
        if node.get("type") != "output" and node_id not in outgoing
    ]
    if not sink_ids:
        sink_ids = [list(node_by_id.keys())[-1]]
    if not sink_ids:
        return definition_json

    node_order = [node.get("id") for node in nodes if node.get("id")]
    ordered_sinks = [node_id for node_id in node_order if node_id in set(sink_ids)] or sink_ids
    updated = False

    def attach_output(output_id: str, *, index: int) -> None:
        nonlocal updated
        if not output_id:
            return
        if any(edge.get("to") == output_id for edge in edges):
            return
        attach_to = ordered_sinks[index] if index < len(ordered_sinks) else ordered_sinks[-1]
        if attach_to and attach_to != output_id:
            edges.append({"from": attach_to, "to": output_id})
            updated = True

    if output_nodes:
        for idx, output_node in enumerate(output_nodes):
            attach_output(str(output_node.get("id") or "").strip(), index=idx)

        existing_names: set[str] = set()
        for node in output_nodes:
            node_id = str(node.get("id") or "").strip()
            metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
            name = str(metadata.get("outputName") or node.get("title") or node_id or "").strip()
            if name:
                existing_names.add(name)

        for idx, output in enumerate(outputs):
            output_name = str(getattr(output, "output_name", "") or "").strip()
            if not output_name or output_name in existing_names:
                continue
            base_id = re.sub(r"[^A-Za-z0-9_]+", "_", f"output_{output_name}").strip("_") or "output"
            output_id = pipeline_router._unique_node_id(base_id, existing_ids)
            existing_ids.add(output_id)
            nodes.append({"id": output_id, "type": "output", "metadata": {"outputName": output_name}})
            attach_output(output_id, index=idx)
        if not updated:
            return definition_json
    else:
        if not outputs:
            return definition_json
        for idx, output in enumerate(outputs):
            output_name = str(getattr(output, "output_name", "") or "").strip()
            if not output_name:
                continue
            base_id = re.sub(r"[^A-Za-z0-9_]+", "_", f"output_{output_name}").strip("_") or "output"
            output_id = pipeline_router._unique_node_id(base_id, existing_ids)
            existing_ids.add(output_id)
            nodes.append({"id": output_id, "type": "output", "metadata": {"outputName": output_name}})
            attach_output(output_id, index=idx)
        if not updated:
            return definition_json
    updated_definition = dict(definition_json)
    updated_definition["nodes"] = nodes
    updated_definition["edges"] = edges
    return updated_definition


def _normalize_join_nodes(definition_json: Dict[str, Any]) -> Dict[str, Any]:
    nodes_raw = definition_json.get("nodes")
    if not isinstance(nodes_raw, list) or not nodes_raw:
        return definition_json
    nodes = [dict(node) for node in nodes_raw if isinstance(node, dict)]
    updated = False
    for node in nodes:
        if str(node.get("type") or "").strip().lower() != "join":
            continue
        node["type"] = "transform"
        metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
        if str(metadata.get("operation") or "").strip().lower() != "join":
            metadata = dict(metadata)
            metadata["operation"] = "join"
            node["metadata"] = metadata
        updated = True
    if not updated:
        return definition_json
    updated_definition = dict(definition_json)
    updated_definition["nodes"] = nodes
    return updated_definition


def _normalize_aggregate_nodes(definition_json: Dict[str, Any]) -> Dict[str, Any]:
    nodes_raw = definition_json.get("nodes")
    if not isinstance(nodes_raw, list) or not nodes_raw:
        return definition_json
    nodes = [dict(node) for node in nodes_raw if isinstance(node, dict)]
    updated = False
    for node in nodes:
        metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
        operation = normalize_operation(metadata.get("operation") or node.get("type"))
        if str(operation or "").lower() not in {"groupby", "aggregate"}:
            continue
        if node.get("type") != "transform":
            node["type"] = "transform"
            updated = True
        if metadata.get("operation") is None:
            metadata = dict(metadata)
            metadata["operation"] = "groupBy" if str(operation).lower() == "groupby" else "aggregate"
            node["metadata"] = metadata
            updated = True
        group_by = metadata.get("groupBy")
        if not group_by:
            group_by = metadata.get("groupByKeys") or metadata.get("group_by")
            if group_by:
                metadata = dict(metadata)
                metadata["groupBy"] = group_by
                node["metadata"] = metadata
                updated = True
        aggregates = metadata.get("aggregates") or metadata.get("aggregations")
        if not isinstance(aggregates, list) or not aggregates:
            continue
        normalized_list: List[Dict[str, Any]] = []
        for agg in aggregates:
            if not isinstance(agg, dict):
                normalized_list.append(agg)
                continue
            column = agg.get("column") or agg.get("field") or agg.get("col")
            op = agg.get("op") or agg.get("operation") or agg.get("agg") or agg.get("func")
            alias = agg.get("alias") or agg.get("name")
            normalized = dict(agg)
            if column and not normalized.get("column"):
                normalized["column"] = column
            if op and not normalized.get("op"):
                normalized["op"] = op
            if alias and not normalized.get("alias"):
                normalized["alias"] = alias
            normalized_list.append(normalized)
        if normalized_list != aggregates:
            updated = True
            metadata = dict(metadata)
            metadata["aggregates"] = normalized_list
            node["metadata"] = metadata
    if not updated:
        return definition_json
    updated_definition = dict(definition_json)
    updated_definition["nodes"] = nodes
    return updated_definition


def _normalize_filter_nodes(definition_json: Dict[str, Any]) -> Dict[str, Any]:
    nodes_raw = definition_json.get("nodes")
    if not isinstance(nodes_raw, list) or not nodes_raw:
        return definition_json
    nodes = [dict(node) for node in nodes_raw if isinstance(node, dict)]
    updated = False
    for node in nodes:
        metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
        operation = normalize_operation(metadata.get("operation") or node.get("type"))
        if str(operation or "").lower() != "filter":
            continue
        if node.get("type") != "transform":
            node["type"] = "transform"
            updated = True
        if metadata.get("operation") is None:
            metadata = dict(metadata)
            metadata["operation"] = "filter"
            node["metadata"] = metadata
            updated = True
        if metadata.get("expression") or metadata.get("predicate"):
            continue
        condition = metadata.get("condition")
        if not condition:
            continue
        metadata = dict(metadata)
        metadata["expression"] = condition
        node["metadata"] = metadata
        updated = True
    if not updated:
        return definition_json
    updated_definition = dict(definition_json)
    updated_definition["nodes"] = nodes
    return updated_definition


def _edge_source(edge: Dict[str, Any]) -> str:
    return str(edge.get("from") or edge.get("source") or "").strip()


def _edge_target(edge: Dict[str, Any]) -> str:
    return str(edge.get("to") or edge.get("target") or "").strip()


def _prune_invalid_edges(definition_json: Dict[str, Any]) -> tuple[Dict[str, Any], List[str]]:
    nodes_raw = definition_json.get("nodes")
    if not isinstance(nodes_raw, list):
        return definition_json, []
    nodes_list = [dict(node) for node in nodes_raw if isinstance(node, dict)]
    node_by_id = normalize_nodes(nodes_list)
    edges = normalize_edges(definition_json.get("edges"))
    if not node_by_id or not edges:
        return definition_json, []

    pruned = 0
    cleaned: List[Dict[str, Any]] = []
    for edge in edges:
        source = _edge_source(edge)
        target = _edge_target(edge)
        if not source or not target:
            pruned += 1
            continue
        if source == target:
            pruned += 1
            continue
        source_node = node_by_id.get(source)
        target_node = node_by_id.get(target)
        if not source_node or not target_node:
            pruned += 1
            continue
        if str(target_node.get("type") or "").strip().lower() == "input":
            pruned += 1
            continue
        if str(source_node.get("type") or "").strip().lower() == "output":
            pruned += 1
            continue
        cleaned.append({"from": source, "to": target})

    if pruned <= 0:
        return definition_json, []
    updated_definition = dict(definition_json)
    updated_definition["edges"] = cleaned
    return updated_definition, [f"pruned {pruned} invalid edges"]


def _slugify_dataset_name(value: Optional[str]) -> str:
    raw = str(value or "").strip().lower()
    raw = re.sub(r"[^a-z0-9_]+", "_", raw)
    raw = re.sub(r"_+", "_", raw).strip("_")
    for suffix in ("_dataset", "_data", "_table"):
        if raw.endswith(suffix):
            raw = raw[: -len(suffix)].rstrip("_")
            break
    return raw or "dataset"


def _normalize_key_list(value: Any) -> List[str]:
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    output: List[str] = []
    for item in items:
        if item is None:
            continue
        raw = str(item).strip()
        if not raw:
            continue
        parts = [part.strip() for part in raw.replace("+", ",").split(",") if part.strip()]
        output.extend(parts if parts else [raw])
    return [item for item in output if item]


_EXPR_IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_EXPR_IGNORE_TOKENS = {
    "and",
    "or",
    "not",
    "null",
    "true",
    "false",
    "is",
    "in",
    "between",
    "like",
    "ilike",
    "case",
    "when",
    "then",
    "else",
    "end",
}


def _strip_string_literals(expression: str) -> str:
    expression = re.sub(r"'[^']*'", "", expression)
    expression = re.sub(r'"[^"]*"', "", expression)
    return expression


def _extract_unknown_columns(expression: str, *, columns: List[str]) -> List[str]:
    if not expression:
        return []
    cleaned = _strip_string_literals(expression)
    candidates = _EXPR_IDENTIFIER_RE.findall(cleaned)
    if not candidates:
        return []
    column_set = set(columns)
    unknown: List[str] = []
    for token in candidates:
        lower = token.lower()
        if lower in _EXPR_IGNORE_TOKENS:
            continue
        if f"{token}(" in cleaned:
            continue
        if token not in column_set:
            unknown.append(token)
    return sorted(set(unknown))


def _index_context_pack(context_pack: Optional[Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str], List[Dict[str, Any]]]:
    datasets_by_id: Dict[str, Dict[str, Any]] = {}
    dataset_id_by_name: Dict[str, str] = {}
    join_candidates: List[Dict[str, Any]] = []
    if isinstance(context_pack, dict):
        selected = context_pack.get("selected_datasets")
        if isinstance(selected, list):
            for item in selected:
                if not isinstance(item, dict):
                    continue
                dataset_id = str(item.get("dataset_id") or "").strip()
                if dataset_id:
                    datasets_by_id[dataset_id] = item
                name = str(item.get("name") or "").strip().lower()
                if name and dataset_id:
                    dataset_id_by_name[name] = dataset_id
        suggestions = context_pack.get("integration_suggestions") if isinstance(context_pack, dict) else None
        if isinstance(suggestions, dict):
            candidates = suggestions.get("join_key_candidates")
            if isinstance(candidates, list):
                join_candidates = [cand for cand in candidates if isinstance(cand, dict)]
    return datasets_by_id, dataset_id_by_name, join_candidates


def _dataset_ref_key(dataset_id: Optional[str], dataset_name: Optional[str]) -> Optional[str]:
    if dataset_id:
        return dataset_id
    if dataset_name:
        return dataset_name.strip().lower() or None
    return None


def _match_join_candidate(
    candidates: List[Dict[str, Any]],
    *,
    left_dataset_id: Optional[str],
    right_dataset_id: Optional[str],
    left_keys: List[str],
    right_keys: List[str],
) -> Tuple[Optional[Dict[str, Any]], bool]:
    if not left_dataset_id or not right_dataset_id:
        return None, False
    left_keys_norm = [key.lower() for key in left_keys]
    right_keys_norm = [key.lower() for key in right_keys]

    def _candidate_keys(candidate: Dict[str, Any], side: str) -> List[str]:
        cols = candidate.get(f"{side}_columns")
        if isinstance(cols, list) and cols:
            return [str(item).strip().lower() for item in cols if str(item).strip()]
        col = candidate.get(f"{side}_column")
        return [item.lower() for item in _normalize_key_list(col)]

    for candidate in candidates:
        cand_left_id = str(candidate.get("left_dataset_id") or "").strip()
        cand_right_id = str(candidate.get("right_dataset_id") or "").strip()
        if not cand_left_id or not cand_right_id:
            continue
        cand_left_keys = _candidate_keys(candidate, "left")
        cand_right_keys = _candidate_keys(candidate, "right")
        if cand_left_id == left_dataset_id and cand_right_id == right_dataset_id:
            if cand_left_keys == left_keys_norm and cand_right_keys == right_keys_norm:
                return candidate, False
            if set(cand_left_keys) == set(left_keys_norm) and set(cand_right_keys) == set(right_keys_norm):
                return candidate, False
        if cand_left_id == right_dataset_id and cand_right_id == left_dataset_id:
            if cand_left_keys == right_keys_norm and cand_right_keys == left_keys_norm:
                return candidate, True
            if set(cand_left_keys) == set(right_keys_norm) and set(cand_right_keys) == set(left_keys_norm):
                return candidate, True
    return None, False


def _average_distinct_ratio(
    *,
    column_profiles: Dict[str, Any],
    keys: List[str],
) -> Optional[float]:
    if not keys or not isinstance(column_profiles, dict):
        return None
    ratios: List[float] = []
    for key in keys:
        profile = column_profiles.get(key) if isinstance(column_profiles, dict) else None
        if isinstance(profile, dict) and profile.get("distinct_ratio") is not None:
            try:
                ratios.append(float(profile.get("distinct_ratio")))
            except Exception:
                continue
    if not ratios:
        return None
    return sum(ratios) / float(len(ratios))


def _swap_join_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    updated = dict(metadata)
    left_keys = updated.get("leftKeys") or updated.get("left_keys")
    right_keys = updated.get("rightKeys") or updated.get("right_keys")
    left_key = updated.get("leftKey") or updated.get("left_key")
    right_key = updated.get("rightKey") or updated.get("right_key")
    updated["leftKeys"], updated["rightKeys"] = right_keys, left_keys
    updated["leftKey"], updated["rightKey"] = right_key, left_key
    return updated


def _select_semantic_order(
    *,
    left_info: InputNodeInfo,
    right_info: InputNodeInfo,
    left_keys: List[str],
    right_keys: List[str],
    join_type: str,
    join_candidates: List[Dict[str, Any]],
) -> Tuple[Optional[bool], Optional[str]]:
    join_type = str(join_type or "inner").strip().lower()
    if join_type not in {"inner"}:
        return None, None
    candidate, swapped = _match_join_candidate(
        join_candidates,
        left_dataset_id=left_info.dataset_id,
        right_dataset_id=right_info.dataset_id,
        left_keys=left_keys,
        right_keys=right_keys,
    )
    if candidate:
        hint = str(candidate.get("cardinality_hint") or "").strip().upper()
        confidence = float(candidate.get("cardinality_confidence") or 0.0)
        if hint in {"N:1", "1:N"} and confidence >= 0.6:
            left_is_n = hint == "N:1"
            if swapped:
                left_is_n = not left_is_n
            if not left_is_n:
                return True, f"cardinality hint {hint}"
            return False, f"cardinality hint {hint}"
    left_distinct = _average_distinct_ratio(column_profiles=left_info.column_profiles, keys=left_keys)
    right_distinct = _average_distinct_ratio(column_profiles=right_info.column_profiles, keys=right_keys)
    if left_distinct is not None and right_distinct is not None:
        if left_distinct + 0.1 < right_distinct:
            return False, "distinct_ratio lower on left"
        if right_distinct + 0.1 < left_distinct:
            return True, "distinct_ratio lower on right"
    if left_info.row_count and right_info.row_count:
        if left_info.row_count >= right_info.row_count * 1.5:
            return False, "row_count higher on left"
        if right_info.row_count >= left_info.row_count * 1.5:
            return True, "row_count higher on right"
    return None, None


def _reorder_edges_for_node(
    edges: List[Dict[str, Any]],
    node_id: str,
    ordered_sources: List[str],
) -> List[Dict[str, Any]]:
    node_edges: List[Dict[str, Any]] = []
    other_edges: List[Dict[str, Any]] = []
    for edge in edges:
        if _edge_target(edge) == node_id:
            node_edges.append(edge)
        else:
            other_edges.append(edge)

    edge_by_source: Dict[str, Dict[str, Any]] = {}
    for edge in node_edges:
        source = _edge_source(edge)
        if source and source not in edge_by_source:
            edge_by_source[source] = edge

    updated = list(other_edges)
    for source in ordered_sources:
        edge = edge_by_source.get(source)
        if edge:
            updated.append(edge)

    used_sources = {source for source in ordered_sources if source in edge_by_source}
    for edge in node_edges:
        if _edge_source(edge) in used_sources:
            continue
        updated.append(edge)
    return updated


def _schema_has_keys(schema: SchemaInfo, keys: List[str]) -> bool:
    if not keys:
        return False
    return all(key in schema.columns for key in keys)


def _regex_flags(value: Any) -> int:
    if value is None:
        return 0
    text = str(value or "")
    flags = 0
    if "i" in text:
        flags |= re.IGNORECASE
    if "m" in text:
        flags |= re.MULTILINE
    if "s" in text:
        flags |= re.DOTALL
    return flags


def _normalize_regex_rules(metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
    rules: List[Dict[str, Any]] = []
    raw_rules = metadata.get("rules")
    if isinstance(raw_rules, list) and raw_rules:
        for rule in raw_rules:
            if not isinstance(rule, dict):
                continue
            column = str(rule.get("column") or "").strip()
            pattern = str(rule.get("pattern") or "").strip()
            if not column or not pattern:
                continue
            rules.append(
                {
                    "column": column,
                    "pattern": pattern,
                    "flags": rule.get("flags"),
                }
            )
        return rules
    pattern = str(metadata.get("pattern") or "").strip()
    if not pattern:
        return rules
    columns = metadata.get("columns") or []
    for col in columns if isinstance(columns, list) else [columns]:
        col_name = str(col or "").strip()
        if not col_name:
            continue
        rules.append({"column": col_name, "pattern": pattern, "flags": metadata.get("flags")})
    return rules


def _replace_join_edges(
    edges: List[Dict[str, Any]],
    *,
    node_id: str,
    sources: List[str],
) -> List[Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []
    sources_set = set(sources)
    for edge in edges:
        if _edge_target(edge) == node_id:
            continue
        if _edge_source(edge) == node_id and _edge_target(edge) in sources_set:
            continue
        cleaned.append(edge)
    for source in sources:
        cleaned.append({"from": source, "to": node_id})
    return cleaned


def _is_acyclic(nodes: Dict[str, Dict[str, Any]], edges: List[Dict[str, Any]]) -> bool:
    order = topological_sort(nodes, edges, include_unordered=False)
    return len(order) == len(nodes)


def _select_join_sources(
    *,
    node_id: str,
    order: List[str],
    nodes: Dict[str, Dict[str, Any]],
    available_schemas: Dict[str, SchemaInfo],
    incoming_ids: List[str],
    left_keys: List[str],
    right_keys: List[str],
) -> Optional[List[str]]:
    if not left_keys or not right_keys:
        return None

    candidates = [
        cand_id
        for cand_id in order
        if cand_id != node_id
        and cand_id in available_schemas
        and str((nodes.get(cand_id) or {}).get("type") or "").strip().lower() != "output"
    ]

    def _pick_candidate(keys: List[str], *, exclude: Optional[set[str]] = None) -> Optional[str]:
        exclude = exclude or set()
        preferred = [cand_id for cand_id in incoming_ids if cand_id in available_schemas]
        for cand_id in preferred + candidates:
            if cand_id in exclude:
                continue
            if _schema_has_keys(available_schemas[cand_id], keys):
                return cand_id
        return None

    left_source = _pick_candidate(left_keys)
    right_source = _pick_candidate(right_keys, exclude={left_source} if left_source else set())
    if not left_source or not right_source or left_source == right_source:
        return None
    return [left_source, right_source]


async def _apply_logical_associations(
    *,
    definition_json: Dict[str, Any],
    associations: List[Any],
    dataset_registry: DatasetRegistry,
    db_name: str,
    branch: Optional[str],
    context_pack: Optional[Dict[str, Any]],
) -> tuple[Dict[str, Any], List[str]]:
    if not associations:
        return definition_json, []
    nodes_raw = definition_json.get("nodes")
    if not isinstance(nodes_raw, list) or not nodes_raw:
        return definition_json, []
    nodes = [dict(node) for node in nodes_raw if isinstance(node, dict)]
    edges = normalize_edges(definition_json.get("edges"))
    node_by_id = normalize_nodes(nodes)
    if not node_by_id:
        return definition_json, []

    datasets_by_id, dataset_id_by_name, join_candidates = _index_context_pack(context_pack)
    input_nodes: Dict[str, InputNodeInfo] = {}
    node_by_dataset_id: Dict[str, str] = {}
    node_by_dataset_name: Dict[str, str] = {}
    input_info_by_key: Dict[str, InputNodeInfo] = {}

    for node_id, node in node_by_id.items():
        if str(node.get("type") or "").strip().lower() != "input":
            continue
        metadata = node.get("metadata") or {}
        selection = normalize_dataset_selection(metadata, default_branch=branch or "main")
        resolution = await resolve_dataset_version(
            dataset_registry,
            db_name=db_name,
            selection=selection,
        )
        dataset = resolution.dataset
        version = resolution.version
        dataset_id = str(getattr(dataset, "dataset_id", "") or selection.dataset_id or "").strip() or None
        dataset_name = str(getattr(dataset, "name", "") or selection.dataset_name or "").strip() or None
        row_count = getattr(version, "row_count", None) if version else None
        if dataset_id and dataset_id in datasets_by_id:
            profile = datasets_by_id[dataset_id]
            if row_count is None:
                row_count = profile.get("row_count")
            column_profiles = profile.get("column_profiles") if isinstance(profile.get("column_profiles"), dict) else {}
        else:
            column_profiles = {}
        input_nodes[node_id] = InputNodeInfo(
            node_id=node_id,
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            row_count=row_count if isinstance(row_count, int) else None,
            column_profiles=column_profiles,
        )
        if dataset_id:
            node_by_dataset_id[dataset_id] = node_id
        if dataset_name:
            node_by_dataset_name[dataset_name.lower()] = node_id

    warnings: List[str] = []
    updated_edges = list(edges)
    updated_nodes = list(nodes)
    existing_ids = set(node_by_id.keys())
    nodes_changed = False
    join_nodes = [
        node_id
        for node_id, node in node_by_id.items()
        if normalize_operation((node.get("metadata") or {}).get("operation") or node.get("type")) == "join"
    ]
    unused_join_nodes = [node_id for node_id in join_nodes]

    chain_anchor: Optional[str] = None
    chain_refs: set[str] = set()

    def _assoc_value(assoc: Any, key: str) -> Any:
        if isinstance(assoc, dict):
            return assoc.get(key)
        return getattr(assoc, key, None)

    async def _ensure_input_node(
        *,
        dataset_id: Optional[str],
        dataset_name: Optional[str],
    ) -> Optional[InputNodeInfo]:
        if dataset_id and dataset_id in node_by_dataset_id:
            return input_nodes.get(node_by_dataset_id[dataset_id])
        if dataset_name:
            name_key = dataset_name.lower()
            if name_key in node_by_dataset_name:
                return input_nodes.get(node_by_dataset_name[name_key])

        resolved_name = dataset_name
        resolved_branch = branch or None
        if dataset_id:
            try:
                dataset = await dataset_registry.get_dataset(dataset_id=str(dataset_id))
            except Exception:
                dataset = None
            if dataset:
                resolved_name = str(getattr(dataset, "name", "") or resolved_name or "").strip() or resolved_name
                resolved_branch = getattr(dataset, "branch", None) or resolved_branch
        if not resolved_name and dataset_id and dataset_id in datasets_by_id:
            resolved_name = str(datasets_by_id[dataset_id].get("name") or "").strip() or resolved_name

        base_id = _slugify_dataset_name(resolved_name) if resolved_name else "dataset"
        if dataset_id:
            suffix = dataset_id.replace("-", "")[:6]
            if suffix and base_id != "dataset":
                base_id = base_id
            elif suffix:
                base_id = f"{base_id}_{suffix}"
        node_id = pipeline_router._unique_node_id(base_id, existing_ids)
        existing_ids.add(node_id)

        metadata: Dict[str, Any] = {"dataset_id": dataset_id} if dataset_id else {}
        if resolved_name:
            metadata["dataset_name"] = resolved_name
        if resolved_branch:
            metadata["dataset_branch"] = resolved_branch

        node = {"id": node_id, "type": "input", "metadata": metadata}
        updated_nodes.append(node)
        node_by_id[node_id] = node
        row_count = None
        column_profiles: Dict[str, Any] = {}
        if dataset_id and dataset_id in datasets_by_id:
            profile = datasets_by_id[dataset_id]
            row_count = profile.get("row_count")
            if isinstance(profile.get("column_profiles"), dict):
                column_profiles = profile.get("column_profiles") or {}
        info = InputNodeInfo(
            node_id=node_id,
            dataset_id=dataset_id,
            dataset_name=resolved_name,
            row_count=row_count if isinstance(row_count, int) else None,
            column_profiles=column_profiles,
        )
        input_nodes[node_id] = info
        key = _dataset_ref_key(dataset_id, resolved_name)
        if dataset_id:
            node_by_dataset_id[dataset_id] = node_id
            input_info_by_key[dataset_id] = info
        elif key:
            node_by_dataset_name[key] = node_id
            input_info_by_key[key] = info
        return info

    async def _resolve_input_node(assoc: Any, *, side: str) -> Optional[InputNodeInfo]:
        dataset_id = str(_assoc_value(assoc, f"{side}_dataset_id") or "").strip() or None
        dataset_name = str(_assoc_value(assoc, f"{side}_dataset_name") or "").strip() or None
        node_id = node_by_dataset_id.get(dataset_id) if dataset_id else None
        if not node_id and dataset_name:
            node_id = node_by_dataset_name.get(dataset_name.lower())
        if not node_id and dataset_name:
            mapped_id = dataset_id_by_name.get(dataset_name.lower())
            if mapped_id:
                node_id = node_by_dataset_id.get(mapped_id)
        if node_id:
            return input_nodes.get(node_id)
        return await _ensure_input_node(dataset_id=dataset_id, dataset_name=dataset_name)

    for idx, assoc in enumerate(associations):
        left_info = await _resolve_input_node(assoc, side="left")
        right_info = await _resolve_input_node(assoc, side="right")
        if not left_info or not right_info:
            warnings.append(f"association {idx + 1}: dataset mapping failed")
            continue
        left_keys = _normalize_key_list(_assoc_value(assoc, "left_keys"))
        right_keys = _normalize_key_list(_assoc_value(assoc, "right_keys"))
        if not left_keys or not right_keys:
            warnings.append(f"association {idx + 1}: missing join keys")
            continue
        join_type = str(_assoc_value(assoc, "join_type") or "inner").strip().lower()

        join_node_id = None
        if unused_join_nodes:
            join_node_id = unused_join_nodes.pop(0)
        if not join_node_id:
            base_id = f"join_{idx + 1}"
            join_node_id = pipeline_router._unique_node_id(base_id, existing_ids)
            existing_ids.add(join_node_id)
            updated_nodes.append({"id": join_node_id, "type": "transform", "metadata": {}})
            node_by_id[join_node_id] = updated_nodes[-1]
            nodes_changed = True

        join_node = node_by_id[join_node_id]
        metadata = join_node.get("metadata") if isinstance(join_node.get("metadata"), dict) else {}
        metadata = dict(metadata)
        metadata["operation"] = "join"
        metadata["joinType"] = join_type
        metadata["allowCrossJoin"] = False
        metadata["leftKeys"] = left_keys
        metadata["rightKeys"] = right_keys
        if _assoc_value(assoc, "association_id"):
            metadata["associationId"] = str(_assoc_value(assoc, "association_id"))
        join_node["type"] = "transform"
        join_node["metadata"] = metadata
        nodes_changed = True

        left_ref = _dataset_ref_key(left_info.dataset_id, left_info.dataset_name)
        right_ref = _dataset_ref_key(right_info.dataset_id, right_info.dataset_name)
        left_input = left_info.node_id
        right_input = right_info.node_id

        if chain_anchor and left_ref and right_ref:
            if left_ref in chain_refs and right_ref not in chain_refs:
                left_input = chain_anchor
                right_input = right_info.node_id
            elif right_ref in chain_refs and left_ref not in chain_refs:
                left_input = left_info.node_id
                right_input = chain_anchor
            elif right_ref in chain_refs and left_ref in chain_refs:
                warnings.append(f"association {idx + 1}: both sides already in join chain")
            else:
                warnings.append(f"association {idx + 1}: disjoint join chain detected; starting new chain")
                chain_anchor = None
                chain_refs = set()

        if left_input in input_nodes and right_input in input_nodes:
            swap, reason = _select_semantic_order(
                left_info=input_nodes[left_input],
                right_info=input_nodes[right_input],
                left_keys=left_keys,
                right_keys=right_keys,
                join_type=join_type,
                join_candidates=join_candidates,
            )
            if swap is True:
                left_input, right_input = right_input, left_input
                metadata = _swap_join_metadata(metadata)
                join_node["metadata"] = metadata
                nodes_changed = True
                warnings.append(f"association {idx + 1}: semantic join order swapped ({reason})")

        before_edges = list(updated_edges)
        updated_edges = _replace_join_edges(updated_edges, node_id=str(join_node_id), sources=[left_input, right_input])
        if updated_edges != before_edges:
            warnings.append(
                f"association {idx + 1}: wired join node {join_node_id} ({left_input} -> {right_input})"
            )
        chain_anchor = join_node_id
        if left_ref:
            chain_refs.add(left_ref)
        if right_ref:
            chain_refs.add(right_ref)

    if not warnings and updated_edges == edges and not nodes_changed:
        return definition_json, []
    updated_definition = dict(definition_json)
    updated_definition["nodes"] = updated_nodes
    updated_definition["edges"] = updated_edges
    return updated_definition, warnings


async def _align_join_inputs(
    *,
    definition_json: Dict[str, Any],
    dataset_registry: DatasetRegistry,
    db_name: str,
    branch: Optional[str],
    context_pack: Optional[Dict[str, Any]],
) -> tuple[Dict[str, Any], List[str], List[str]]:
    nodes_raw = definition_json.get("nodes")
    if not isinstance(nodes_raw, list):
        return definition_json, [], []
    nodes_list = [dict(node) for node in nodes_raw if isinstance(node, dict)]
    nodes = normalize_nodes(nodes_list)
    edges = normalize_edges(definition_json.get("edges"))
    if not nodes:
        return definition_json, [], []

    datasets_by_id, dataset_id_by_name, join_candidates = _index_context_pack(context_pack)
    incoming = build_incoming(edges)
    order = topological_sort(nodes, edges, include_unordered=True)
    schema_by_node: Dict[str, SchemaInfo] = {}
    node_sources: Dict[str, set[str]] = {}
    input_schemas: Dict[str, SchemaInfo] = {}
    input_info: Dict[str, InputNodeInfo] = {}
    input_info_by_key: Dict[str, InputNodeInfo] = {}
    warnings: List[str] = []
    column_errors: List[str] = []
    updated_edges = list(edges)
    nodes_changed = False

    for node_id, node in nodes.items():
        node_type = str(node.get("type") or "").strip().lower()
        if node_type != "input":
            continue
        metadata = node.get("metadata") or {}
        selection = normalize_dataset_selection(metadata, default_branch=branch or "main")
        resolution = await resolve_dataset_version(
            dataset_registry,
            db_name=db_name,
            selection=selection,
        )
        dataset = resolution.dataset
        version = resolution.version
        input_schemas[node_id] = _schema_for_input(resolution.dataset, resolution.version)
        dataset_id = str(getattr(dataset, "dataset_id", "") or selection.dataset_id or "").strip() or None
        dataset_name = str(getattr(dataset, "name", "") or selection.dataset_name or "").strip() or None
        row_count = getattr(version, "row_count", None) if version else None
        if dataset_id and dataset_id in datasets_by_id:
            profile = datasets_by_id[dataset_id]
            if row_count is None:
                row_count = profile.get("row_count")
            column_profiles = profile.get("column_profiles") if isinstance(profile.get("column_profiles"), dict) else {}
        else:
            column_profiles = {}
        info = InputNodeInfo(
            node_id=node_id,
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            row_count=row_count if isinstance(row_count, int) else None,
            column_profiles=column_profiles,
        )
        input_info[node_id] = info
        if dataset_id:
            input_info_by_key[dataset_id] = info
            node_sources[node_id] = {dataset_id}
        elif dataset_name:
            key = dataset_name.lower()
            input_info_by_key[key] = info
            node_sources[node_id] = {key}
        else:
            node_sources[node_id] = set()

    schema_by_node.update(input_schemas)

    def _info_for_node(node_id: str) -> Optional[InputNodeInfo]:
        if node_id in input_info:
            return input_info[node_id]
        sources = node_sources.get(node_id) or set()
        if len(sources) == 1:
            key = next(iter(sources))
            return input_info_by_key.get(key)
        return None

    for node_id in order:
        node = nodes.get(node_id) or {}
        node_type = str(node.get("type") or "transform").strip().lower()
        metadata = node.get("metadata") or {}
        input_ids = incoming.get(node_id, [])
        inputs = [schema_by_node[in_id] for in_id in input_ids if in_id in schema_by_node]
        sources = set().union(*(node_sources.get(in_id, set()) for in_id in input_ids)) if input_ids else set()

        if node_type == "input":
            if node_id in input_schemas:
                schema_by_node[node_id] = input_schemas[node_id]
            continue

        if node_type == "output":
            schema_by_node[node_id] = inputs[0] if inputs else SchemaInfo(columns=[], type_map={})
            node_sources[node_id] = sources
            continue

        operation = normalize_operation(metadata.get("operation") or node_type)
        if operation == "join":
            join_spec = resolve_join_spec(metadata)
            if not (join_spec.allow_cross_join and join_spec.join_type == "cross"):
                left_keys = list(join_spec.left_keys or [])
                right_keys = list(join_spec.right_keys or [])
                left_key = join_spec.left_key or join_spec.right_key
                right_key = join_spec.right_key or join_spec.left_key
                if left_key and not left_keys:
                    left_keys = [left_key]
                if right_key and not right_keys:
                    right_keys = [right_key]

                if left_keys and right_keys:
                    selected = _select_join_sources(
                        node_id=str(node_id),
                        order=order,
                        nodes=nodes,
                        available_schemas=schema_by_node,
                        incoming_ids=list(input_ids),
                        left_keys=left_keys,
                        right_keys=right_keys,
                    )
                    if selected and (len(input_ids) != 2 or input_ids != selected):
                        candidate_edges = _replace_join_edges(updated_edges, node_id=str(node_id), sources=selected)
                        if _is_acyclic(nodes, candidate_edges):
                            updated_edges = candidate_edges
                            incoming = build_incoming(updated_edges)
                            input_ids = incoming.get(node_id, [])
                            inputs = [schema_by_node[in_id] for in_id in input_ids if in_id in schema_by_node]
                            warnings.append(f"join inputs rewired for node {node_id}")

            if len(inputs) >= 2:
                if not (join_spec.allow_cross_join and join_spec.join_type == "cross"):
                    left_keys = list(join_spec.left_keys or [])
                    right_keys = list(join_spec.right_keys or [])
                    left_key = join_spec.left_key or join_spec.right_key
                    right_key = join_spec.right_key or join_spec.left_key
                    if left_key and not left_keys:
                        left_keys = [left_key]
                    if right_key and not right_keys:
                        right_keys = [right_key]

                    if left_keys and right_keys:
                        left_in_left = _schema_has_keys(inputs[0], left_keys)
                        right_in_right = _schema_has_keys(inputs[1], right_keys)
                        left_in_right = _schema_has_keys(inputs[1], left_keys)
                        right_in_left = _schema_has_keys(inputs[0], right_keys)
                        if not left_in_left and not right_in_right and left_in_right and right_in_left:
                            ordered_sources = [str(input_ids[1]), str(input_ids[0])]
                            updated_edges = _reorder_edges_for_node(updated_edges, str(node_id), ordered_sources)
                            incoming = build_incoming(updated_edges)
                            input_ids = incoming.get(node_id, [])
                            inputs = [schema_by_node[in_id] for in_id in input_ids if in_id in schema_by_node]
                            warnings.append(f"join inputs reordered for node {node_id}")
                        else:
                            left_info = _info_for_node(str(input_ids[0])) if len(input_ids) >= 2 else None
                            right_info = _info_for_node(str(input_ids[1])) if len(input_ids) >= 2 else None
                            if left_info and right_info:
                                swap, reason = _select_semantic_order(
                                    left_info=left_info,
                                    right_info=right_info,
                                    left_keys=left_keys,
                                    right_keys=right_keys,
                                    join_type=join_spec.join_type,
                                    join_candidates=join_candidates,
                                )
                                if swap is True:
                                    ordered_sources = [str(input_ids[1]), str(input_ids[0])]
                                    updated_edges = _reorder_edges_for_node(updated_edges, str(node_id), ordered_sources)
                                    incoming = build_incoming(updated_edges)
                                    input_ids = incoming.get(node_id, [])
                                    inputs = [schema_by_node[in_id] for in_id in input_ids if in_id in schema_by_node]
                                    node["metadata"] = _swap_join_metadata(metadata)
                                    nodes_changed = True
                                    warnings.append(
                                        f"join inputs reordered for node {node_id} (semantic: {reason})"
                                    )

                schema_by_node[node_id] = _apply_join(inputs[0], inputs[1])
                node_sources[node_id] = sources
            else:
                schema_by_node[node_id] = SchemaInfo(columns=[], type_map={})
                node_sources[node_id] = sources
            continue

        if operation == "union" and len(inputs) >= 2:
            union_mode = normalize_union_mode(metadata)
            schema_by_node[node_id] = _apply_union(inputs[0], inputs[1], union_mode)
            node_sources[node_id] = sources
            continue

        if not inputs:
            schema_by_node[node_id] = SchemaInfo(columns=[], type_map={})
            node_sources[node_id] = sources
            continue

        base = inputs[0]
        if operation == "filter":
            expression = str(metadata.get("expression") or metadata.get("predicate") or "").strip()
            unknown = _extract_unknown_columns(expression, columns=list(base.columns))
            if unknown:
                column_errors.append(f"filter unknown columns on node {node_id}: {', '.join(unknown)}")
        if operation == "regexReplace":
            rules = _normalize_regex_rules(metadata)
            missing_cols = [rule["column"] for rule in rules if rule.get("column") not in base.columns]
            if missing_cols:
                column_errors.append(
                    f"regexReplace missing columns on node {node_id}: {', '.join(sorted(set(missing_cols)))}"
                )
            for rule in rules:
                pattern = str(rule.get("pattern") or "").strip()
                if not pattern:
                    continue
                try:
                    re.compile(pattern, flags=_regex_flags(rule.get("flags")))
                except re.error:
                    column_errors.append(
                        f"regexReplace invalid pattern on node {node_id}: {pattern}"
                    )
        if operation == "select":
            schema_by_node[node_id] = _apply_select(base, _normalize_column_list(metadata.get("columns") or []))
        elif operation == "drop":
            schema_by_node[node_id] = _apply_drop(base, _normalize_column_list(metadata.get("columns") or []))
        elif operation == "rename":
            rename_map = metadata.get("rename") or {}
            schema_by_node[node_id] = _apply_rename(base, rename_map if isinstance(rename_map, dict) else {})
        elif operation == "cast":
            casts = metadata.get("casts") or []
            schema_by_node[node_id] = _apply_cast(base, casts if isinstance(casts, list) else [])
        elif operation == "compute":
            schema_by_node[node_id] = _apply_compute(base, metadata.get("expression") or "")
        elif operation in {"groupBy", "aggregate"}:
            group_by = _normalize_column_list(metadata.get("groupBy") or metadata.get("groupByKeys") or [])
            missing_group = [col for col in group_by if col not in base.columns]
            if missing_group:
                column_errors.append(
                    f"groupBy missing columns on node {node_id}: {', '.join(missing_group)}"
                )
            aggregates = metadata.get("aggregates") or metadata.get("aggregations") or []
            for agg in aggregates if isinstance(aggregates, list) else []:
                if not isinstance(agg, dict):
                    continue
                column = str(agg.get("column") or agg.get("field") or "").strip()
                op = str(agg.get("op") or agg.get("operation") or "").strip().lower()
                if not column or op == "count":
                    continue
                if column not in base.columns:
                    column_errors.append(f"aggregate missing column {column} on node {node_id}")
            schema_by_node[node_id] = _apply_group_by(base, group_by, aggregates if isinstance(aggregates, list) else [])
        elif operation == "window":
            schema_by_node[node_id] = _apply_window(base)
        else:
            schema_by_node[node_id] = base
        node_sources[node_id] = sources

    if not warnings and updated_edges == edges and not nodes_changed and not column_errors:
        return definition_json, [], []
    updated_definition = dict(definition_json)
    updated_definition["edges"] = updated_edges
    if nodes_changed:
        updated_definition["nodes"] = nodes_list
    return updated_definition, warnings, column_errors

async def validate_pipeline_plan(
    *,
    plan: PipelinePlan,
    dataset_registry: DatasetRegistry,
    db_name: str,
    branch: Optional[str],
    require_output: bool = True,
    context_pack: Optional[Dict[str, Any]] = None,
    task_spec: Optional[PipelineTaskSpec] = None,
) -> PipelinePlanValidationResult:
    errors: List[str] = []
    warnings: List[str] = []

    # Enforce TaskSpec policy (overreach guardrails) as early as possible.
    effective_spec = task_spec or getattr(plan, "task_spec", None)
    if effective_spec is not None:
        dataset_count = len(list(plan.data_scope.dataset_ids or [])) if plan.data_scope else 0
        effective_spec = clamp_task_spec(spec=effective_spec, dataset_count=dataset_count)
        plan = plan.model_copy(update={"task_spec": effective_spec})
        policy_errors, policy_warnings = validate_plan_against_task_spec(plan=plan, task_spec=effective_spec)
        errors.extend(policy_errors)
        warnings.extend(policy_warnings)

    for output in plan.outputs or []:
        kind = str(getattr(output.output_kind, "value", output.output_kind) or "unknown").strip().lower()
        if kind == "object":
            if not output.target_class_id:
                errors.append(f"output {output.output_name}: target_class_id is required for object outputs")
        elif kind == "link":
            missing: List[str] = []
            if not output.link_type_id:
                missing.append("link_type_id")
            if not output.source_class_id:
                missing.append("source_class_id")
            if not output.target_class_id:
                missing.append("target_class_id")
            if not output.predicate:
                missing.append("predicate")
            if not output.cardinality:
                missing.append("cardinality")
            if not output.source_key_column:
                missing.append("source_key_column")
            if not output.target_key_column:
                missing.append("target_key_column")
            if not output.relationship_spec_type:
                missing.append("relationship_spec_type")
            if missing:
                errors.append(
                    f"output {output.output_name}: missing required link metadata ({', '.join(missing)})"
                )
        else:
            warnings.append(f"output {output.output_name}: output_kind is unknown")

    definition_json = dict(plan.definition_json or {})
    canonical_output_names = {
        output.output_name
        for output in (plan.outputs or [])
        if str(getattr(output.output_kind, "value", output.output_kind) or "").strip().lower() in {"object", "link"}
        and str(output.output_name or "").strip()
    }
    normalized = _normalize_join_nodes(definition_json)
    normalized = _normalize_aggregate_nodes(normalized)
    normalized = _normalize_filter_nodes(normalized)
    normalized, assoc_warnings = await _apply_logical_associations(
        definition_json=normalized,
        associations=list(plan.associations or []),
        dataset_registry=dataset_registry,
        db_name=db_name,
        branch=branch,
        context_pack=context_pack,
    )
    normalized = await pipeline_router._augment_definition_with_casts(
        definition_json=normalized,
        db_name=db_name,
        branch=branch,
        dataset_registry=dataset_registry,
    )
    normalized, edge_warnings = _prune_invalid_edges(normalized)
    normalized, join_warnings, column_errors = await _align_join_inputs(
        definition_json=normalized,
        dataset_registry=dataset_registry,
        db_name=db_name,
        branch=branch,
        context_pack=context_pack,
    )
    normalized = _ensure_output_nodes(normalized, plan.outputs or [])
    normalized = pipeline_router._augment_definition_with_canonical_contract(
        definition_json=normalized,
        branch=branch,
        canonical_output_names=canonical_output_names or None,
    )
    if normalized != definition_json:
        warnings.append("definition_json auto-augmented with associations/casts/canonical contract/join alignment")
    warnings.extend(assoc_warnings)
    warnings.extend(edge_warnings)
    warnings.extend(join_warnings)
    errors.extend(column_errors)

    nodes = normalize_nodes(normalized.get("nodes"))
    edges = normalize_edges(normalized.get("edges"))
    if nodes and edges and not _is_acyclic(nodes, edges):
        errors.append("pipeline graph has cycles; remove circular joins/edges")

    errors.extend(
        pipeline_router._validate_pipeline_definition(
            definition_json=normalized,
            require_output=require_output,
        )
    )

    preflight = await pipeline_router._run_pipeline_preflight(
        definition_json=normalized,
        db_name=db_name,
        branch=branch,
        dataset_registry=dataset_registry,
    )
    if preflight.get("has_blocking_errors"):
        errors.append("pipeline preflight has blocking errors")

    updated_plan = plan.model_copy(update={"definition_json": normalized})
    digest = sha256_canonical_json_prefixed(updated_plan.model_dump(mode="json"))

    diagnostics: List[PlanDiagnostic] = []
    for message in errors:
        diagnostics.append(
            PlanDiagnostic(
                code="PIPELINE_PLAN_INVALID",
                severity=PlanDiagnosticSeverity.error,
                message=str(message),
            )
        )
    for message in warnings:
        diagnostics.append(
            PlanDiagnostic(
                code="PIPELINE_PLAN_WARNING",
                severity=PlanDiagnosticSeverity.warning,
                message=str(message),
            )
        )

    status = "success" if not errors else "error"
    report = PlanCompilationReport(
        plan_id=str(updated_plan.plan_id or ""),
        status=status,
        plan_digest=digest,
        diagnostics=diagnostics,
    )

    return PipelinePlanValidationResult(
        plan=updated_plan,
        errors=errors,
        warnings=warnings,
        preflight=preflight,
        compilation_report=report,
    )
