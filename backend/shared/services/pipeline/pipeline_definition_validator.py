from __future__ import annotations

from dataclasses import dataclass
from typing import AbstractSet, Any, Dict, List, Optional

from shared.services.pipeline.pipeline_graph_utils import build_incoming, normalize_edges, normalize_nodes
from shared.services.pipeline.pipeline_transform_spec import normalize_operation, normalize_union_mode, resolve_join_spec

_ALLOWED_UNION_MODES: frozenset[str] = frozenset({"strict", "common_only", "pad_missing_nulls", "pad"})


def normalize_transform_metadata(metadata: Any) -> Dict[str, Any]:
    if not isinstance(metadata, dict):
        return {}
    normalized = dict(metadata)

    if not normalized.get("columns"):
        fields = normalized.get("fields")
        if isinstance(fields, list):
            normalized["columns"] = fields

    if not normalized.get("groupBy"):
        for key in ("groupKeys", "keys"):
            value = normalized.get(key)
            if isinstance(value, list):
                normalized["groupBy"] = value
                break

    if not normalized.get("aggregates"):
        aggregations = normalized.get("aggregations")
        if isinstance(aggregations, list):
            normalized["aggregates"] = aggregations

    aggregates = normalized.get("aggregates")
    if isinstance(aggregates, list):
        normalized_items: list[dict[str, Any]] = []
        for item in aggregates:
            if not isinstance(item, dict):
                continue
            column = item.get("column") or item.get("field") or item.get("name")
            op = item.get("op") or item.get("function") or item.get("agg")
            alias = item.get("alias") or item.get("as")
            updated = dict(item)
            if column and not updated.get("column"):
                updated["column"] = column
            if op and not updated.get("op"):
                updated["op"] = op
            if alias and not updated.get("alias"):
                updated["alias"] = alias
            normalized_items.append(updated)
        normalized["aggregates"] = normalized_items

    return normalized


@dataclass(frozen=True)
class PipelineDefinitionValidationPolicy:
    supported_ops: AbstractSet[str]
    require_output: bool = True
    normalize_metadata: bool = True
    udf_error_message_template: Optional[str] = None


@dataclass
class PipelineDefinitionValidationResult:
    errors: List[str]
    nodes: Dict[str, Dict[str, Any]]
    edges: List[Dict[str, Any]]
    incoming: Dict[str, List[str]]


def validate_pipeline_definition(
    definition_json: Dict[str, Any],
    *,
    policy: PipelineDefinitionValidationPolicy,
) -> PipelineDefinitionValidationResult:
    errors: list[str] = []
    nodes_raw = definition_json.get("nodes")
    if not isinstance(nodes_raw, list) or not nodes_raw:
        errors.append("Pipeline has no nodes")
        return PipelineDefinitionValidationResult(errors=errors, nodes={}, edges=[], incoming={})

    nodes = normalize_nodes(nodes_raw)
    if policy.normalize_metadata:
        for node in nodes.values():
            node["metadata"] = normalize_transform_metadata(node.get("metadata"))

    edges = normalize_edges(definition_json.get("edges"))
    node_ids = set(nodes.keys())
    for edge in edges:
        if edge["from"] not in node_ids or edge["to"] not in node_ids:
            errors.append(f"Pipeline edge references missing node: {edge['from']}->{edge['to']}")

    has_output = any(node.get("type") == "output" for node in nodes.values())
    if policy.require_output and not has_output:
        errors.append("Pipeline has no output node")

    incoming = build_incoming(edges)

    for node_id, node in nodes.items():
        if node.get("type") != "transform":
            continue
        metadata = node.get("metadata") or {}
        operation = normalize_operation(metadata.get("operation"))
        if not operation:
            if len(incoming.get(node_id, [])) >= 2:
                errors.append(f"transform node {node_id} has multiple inputs but no operation")
            continue

        if operation == "udf" and policy.udf_error_message_template:
            errors.append(policy.udf_error_message_template.format(operation=operation, node_id=node_id))
            continue

        if operation not in policy.supported_ops:
            errors.append(f"Unsupported operation '{operation}' on node {node_id}")
            continue

        if operation == "filter" and not str(metadata.get("expression") or "").strip():
            errors.append(f"{operation} missing expression on node {node_id}")

        if operation == "compute":
            has_expression = bool(str(metadata.get("expression") or "").strip())
            target = metadata.get("targetColumn") or metadata.get("target_column") or metadata.get("target")
            formula = metadata.get("formula") or metadata.get("expr")
            has_target_formula = bool(str(target or "").strip()) and bool(str(formula or "").strip())
            assignments = metadata.get("assignments") or metadata.get("computedColumns") or metadata.get("computed_columns")
            has_assignments = isinstance(assignments, list) and any(
                isinstance(item, dict)
                and str(item.get("column") or item.get("target") or item.get("name") or "").strip()
                and str(item.get("expression") or item.get("expr") or item.get("formula") or "").strip()
                for item in assignments
            )
            if not (has_expression or has_target_formula or has_assignments):
                errors.append(f"compute missing expression/targetColumn/formula/assignments on node {node_id}")

        if operation in {"select", "drop", "sort", "dedupe", "explode", "normalize"}:
            columns = metadata.get("columns") or []
            expressions = metadata.get("expressions") or metadata.get("selectExpr") or metadata.get("select_expr")
            if operation == "select":
                if not columns and not (isinstance(expressions, list) and expressions):
                    errors.append(f"{operation} missing columns/expressions on node {node_id}")
            else:
                if not columns:
                    errors.append(f"{operation} missing columns on node {node_id}")

        if operation == "rename":
            rename_map = metadata.get("rename") or {}
            if not rename_map:
                errors.append(f"rename missing mapping on node {node_id}")

        if operation == "cast":
            casts = metadata.get("casts") or []
            if not casts:
                errors.append(f"cast missing columns on node {node_id}")

        if operation in {"groupBy", "aggregate"}:
            aggregates = metadata.get("aggregates") or []
            expr_items = metadata.get("aggregateExpressions") or metadata.get("aggExpressions") or metadata.get("aggregate_expressions")
            has_exprs = isinstance(expr_items, list) and len(expr_items) > 0
            has_aggs = isinstance(aggregates, list) and any(
                isinstance(item, dict) and item.get("column") and item.get("op") for item in aggregates
            )
            if not (has_aggs or has_exprs):
                errors.append(f"{operation} missing aggregates/aggregateExpressions on node {node_id}")

        if operation == "join":
            if len(incoming.get(node_id, [])) < 2:
                errors.append(f"join requires two inputs on node {node_id}")
            join_spec = resolve_join_spec(metadata)
            left_keys = list(join_spec.left_keys or [])
            right_keys = list(join_spec.right_keys or [])
            if join_spec.left_key and not left_keys:
                left_keys = [join_spec.left_key]
            if join_spec.right_key and not right_keys:
                right_keys = [join_spec.right_key]
            if not join_spec.allow_cross_join and (not left_keys or not right_keys):
                errors.append(f"join requires leftKey/rightKey or leftKeys/rightKeys (or joinKey) on node {node_id}")
            if left_keys and right_keys and len(left_keys) != len(right_keys):
                errors.append(f"join requires leftKeys/rightKeys of the same length on node {node_id}")
            if join_spec.allow_cross_join and not left_keys and not right_keys:
                if join_spec.join_type != "cross":
                    errors.append(f"join allowCrossJoin requires joinType='cross' on node {node_id}")

        if operation == "union":
            if len(incoming.get(node_id, [])) < 2:
                errors.append(f"union requires two inputs on node {node_id}")
            union_mode = normalize_union_mode(metadata)
            if union_mode not in _ALLOWED_UNION_MODES:
                errors.append(f"union has invalid unionMode '{union_mode}' on node {node_id}")

        if operation == "pivot":
            pivot_meta = metadata.get("pivot") or {}
            index_cols = pivot_meta.get("index") or []
            columns_col = pivot_meta.get("columns")
            values_col = pivot_meta.get("values")
            if not index_cols or not columns_col or not values_col:
                errors.append(f"pivot missing fields on node {node_id}")

        if operation == "window":
            window_meta = metadata.get("window") or {}
            order_by = window_meta.get("orderBy") or []
            expressions = window_meta.get("expressions") if isinstance(window_meta.get("expressions"), list) else None
            if not order_by and not (isinstance(expressions, list) and expressions):
                errors.append(f"window missing orderBy/expressions on node {node_id}")

    return PipelineDefinitionValidationResult(errors=errors, nodes=nodes, edges=edges, incoming=incoming)

