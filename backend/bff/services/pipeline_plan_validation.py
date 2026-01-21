"""
Pipeline plan validation against pipeline definition rules + preflight.

Uses existing pipeline definition validation/preflight utilities and returns
machine-readable diagnostics for planners/repair loops.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from shared.models.agent_plan_report import PlanCompilationReport, PlanDiagnostic, PlanDiagnosticSeverity
from shared.models.pipeline_plan import PipelinePlan
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
from shared.utils.canonical_json import sha256_canonical_json_prefixed

from bff.routers import pipeline as pipeline_router


@dataclass(frozen=True)
class PipelinePlanValidationResult:
    plan: PipelinePlan
    errors: List[str]
    warnings: List[str]
    preflight: Dict[str, Any]
    compilation_report: PlanCompilationReport


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


def _edge_source(edge: Dict[str, Any]) -> str:
    return str(edge.get("from") or edge.get("source") or "").strip()


def _edge_target(edge: Dict[str, Any]) -> str:
    return str(edge.get("to") or edge.get("target") or "").strip()


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


async def _align_join_inputs(
    *,
    definition_json: Dict[str, Any],
    dataset_registry: DatasetRegistry,
    db_name: str,
    branch: Optional[str],
) -> tuple[Dict[str, Any], List[str]]:
    nodes = normalize_nodes(definition_json.get("nodes"))
    edges = normalize_edges(definition_json.get("edges"))
    if not nodes or not edges:
        return definition_json, []

    incoming = build_incoming(edges)
    order = topological_sort(nodes, edges, include_unordered=True)
    schema_by_node: Dict[str, SchemaInfo] = {}
    input_schemas: Dict[str, SchemaInfo] = {}
    warnings: List[str] = []
    updated_edges = list(edges)

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
        input_schemas[node_id] = _schema_for_input(resolution.dataset, resolution.version)

    schema_by_node.update(input_schemas)

    for node_id in order:
        node = nodes.get(node_id) or {}
        node_type = str(node.get("type") or "transform").strip().lower()
        metadata = node.get("metadata") or {}
        input_ids = incoming.get(node_id, [])
        inputs = [schema_by_node[in_id] for in_id in input_ids if in_id in schema_by_node]

        if node_type == "input":
            if node_id in input_schemas:
                schema_by_node[node_id] = input_schemas[node_id]
            continue

        if node_type == "output":
            schema_by_node[node_id] = inputs[0] if inputs else SchemaInfo(columns=[], type_map={})
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

                schema_by_node[node_id] = _apply_join(inputs[0], inputs[1])
            else:
                schema_by_node[node_id] = SchemaInfo(columns=[], type_map={})
            continue

        if operation == "union" and len(inputs) >= 2:
            union_mode = normalize_union_mode(metadata)
            schema_by_node[node_id] = _apply_union(inputs[0], inputs[1], union_mode)
            continue

        if not inputs:
            schema_by_node[node_id] = SchemaInfo(columns=[], type_map={})
            continue

        base = inputs[0]
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
            group_by = _normalize_column_list(metadata.get("groupBy") or [])
            aggregates = metadata.get("aggregates") or []
            schema_by_node[node_id] = _apply_group_by(base, group_by, aggregates if isinstance(aggregates, list) else [])
        elif operation == "window":
            schema_by_node[node_id] = _apply_window(base)
        else:
            schema_by_node[node_id] = base

    if not warnings:
        return definition_json, []
    updated_definition = dict(definition_json)
    updated_definition["edges"] = updated_edges
    return updated_definition, warnings

async def validate_pipeline_plan(
    *,
    plan: PipelinePlan,
    dataset_registry: DatasetRegistry,
    db_name: str,
    branch: Optional[str],
    require_output: bool = True,
) -> PipelinePlanValidationResult:
    errors: List[str] = []
    warnings: List[str] = []

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
    normalized = await pipeline_router._augment_definition_with_casts(
        definition_json=normalized,
        db_name=db_name,
        branch=branch,
        dataset_registry=dataset_registry,
    )
    normalized, join_warnings = await _align_join_inputs(
        definition_json=normalized,
        dataset_registry=dataset_registry,
        db_name=db_name,
        branch=branch,
    )
    normalized = _ensure_output_nodes(normalized, plan.outputs or [])
    normalized = pipeline_router._augment_definition_with_canonical_contract(
        definition_json=normalized,
        branch=branch,
        canonical_output_names=canonical_output_names or None,
    )
    if normalized != definition_json:
        warnings.append("definition_json auto-augmented with casts/canonical contract/join alignment")
    warnings.extend(join_warnings)

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
