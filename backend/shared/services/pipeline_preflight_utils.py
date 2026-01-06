from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from shared.services.pipeline_dataset_utils import normalize_dataset_selection, resolve_dataset_version
from shared.services.pipeline_graph_utils import build_incoming, normalize_edges, normalize_nodes, topological_sort
from shared.services.pipeline_schema_utils import normalize_schema_contract, normalize_schema_type
from shared.services.pipeline_transform_spec import normalize_operation, normalize_union_mode, resolve_join_spec
from shared.services.pipeline_type_utils import infer_xsd_type_from_values


@dataclass(frozen=True)
class SchemaInfo:
    columns: List[str]
    type_map: Dict[str, Optional[str]]
    dynamic_columns: bool = False


def _normalize_column_list(raw: Any) -> List[str]:
    if not isinstance(raw, list):
        return []
    output: List[str] = []
    for item in raw:
        if isinstance(item, dict):
            name = str(item.get("name") or item.get("column") or "").strip()
            if name:
                output.append(name)
        else:
            value = str(item).strip()
            if value:
                output.append(value)
    return output


def _extract_schema_columns(schema: Any) -> Tuple[List[str], Dict[str, Optional[str]]]:
    if not isinstance(schema, dict):
        return [], {}
    if isinstance(schema.get("columns"), list):
        columns: List[str] = []
        type_map: Dict[str, Optional[str]] = {}
        for col in schema["columns"]:
            if isinstance(col, dict):
                name = str(col.get("name") or col.get("column") or "").strip()
                if not name:
                    continue
                columns.append(name)
                raw_type = col.get("type") or col.get("data_type") or col.get("dtype")
                if raw_type:
                    type_map[name] = normalize_schema_type(raw_type)
            elif isinstance(col, str):
                name = col.strip()
                if name:
                    columns.append(name)
        return columns, type_map
    if isinstance(schema.get("fields"), list):
        columns = []
        type_map: Dict[str, Optional[str]] = {}
        for col in schema["fields"]:
            if not isinstance(col, dict):
                continue
            name = str(col.get("name") or "").strip()
            if not name:
                continue
            columns.append(name)
            raw_type = col.get("type")
            if raw_type:
                type_map[name] = normalize_schema_type(raw_type)
        return columns, type_map
    if isinstance(schema.get("properties"), dict):
        columns = []
        type_map = {}
        for key, value in schema["properties"].items():
            name = str(key).strip()
            if not name:
                continue
            columns.append(name)
            if isinstance(value, dict) and value.get("type"):
                type_map[name] = normalize_schema_type(value.get("type"))
        return columns, type_map
    return [], {}


def _extract_sample_rows(sample: Any) -> List[Dict[str, Any]]:
    if not isinstance(sample, dict):
        return []
    rows = sample.get("rows")
    if isinstance(rows, list):
        if rows and isinstance(rows[0], dict):
            return rows  # type: ignore[return-value]
        columns, _ = _extract_schema_columns(sample)
        output: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, list):
                continue
            output.append(
                {
                    (columns[idx] if idx < len(columns) else f"col_{idx}"): value
                    for idx, value in enumerate(row)
                }
            )
        return output
    data_rows = sample.get("data")
    if isinstance(data_rows, list) and data_rows and isinstance(data_rows[0], dict):
        return data_rows  # type: ignore[return-value]
    return []


def _infer_types_from_rows(rows: List[Dict[str, Any]], columns: List[str]) -> Dict[str, str]:
    inferred: Dict[str, str] = {}
    if not rows or not columns:
        return inferred
    for col in columns:
        values = [row.get(col) for row in rows if row.get(col) is not None]
        inferred[col] = infer_xsd_type_from_values(values)
    return inferred


def _merge_types(
    base: Dict[str, Optional[str]],
    extra: Dict[str, Optional[str]],
) -> Dict[str, Optional[str]]:
    output = dict(base)
    for key, value in extra.items():
        if not output.get(key) and value:
            output[key] = value
    return output


def _schema_for_input(dataset: Any, version: Any) -> SchemaInfo:
    columns, type_map = _extract_schema_columns(getattr(dataset, "schema_json", None) or {})
    sample_columns, sample_types = _extract_schema_columns(getattr(version, "sample_json", None) or {})
    sample_rows = _extract_sample_rows(getattr(version, "sample_json", None) or {})

    if not columns:
        columns = sample_columns
    if not columns and sample_rows:
        seen: set[str] = set()
        ordered: List[str] = []
        for row in sample_rows:
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    ordered.append(key)
        columns = ordered

    merged_types = _merge_types(type_map, sample_types)
    if sample_rows and columns:
        inferred = _infer_types_from_rows(sample_rows, columns)
        merged_types = _merge_types(merged_types, inferred)

    return SchemaInfo(columns=columns, type_map=merged_types)


def _apply_select(schema: SchemaInfo, columns: List[str]) -> SchemaInfo:
    normalized = [col for col in columns if col in schema.columns]
    type_map = {col: schema.type_map.get(col) for col in normalized}
    return SchemaInfo(columns=normalized, type_map=type_map, dynamic_columns=schema.dynamic_columns)


def _apply_drop(schema: SchemaInfo, columns: List[str]) -> SchemaInfo:
    drop_set = set(columns)
    normalized = [col for col in schema.columns if col not in drop_set]
    type_map = {col: schema.type_map.get(col) for col in normalized}
    return SchemaInfo(columns=normalized, type_map=type_map, dynamic_columns=schema.dynamic_columns)


def _apply_rename(schema: SchemaInfo, rename_map: Dict[str, Any]) -> SchemaInfo:
    normalized: List[str] = []
    type_map: Dict[str, Optional[str]] = {}
    for col in schema.columns:
        mapped = str(rename_map.get(col) or col).strip()
        if not mapped:
            continue
        normalized.append(mapped)
        type_map[mapped] = schema.type_map.get(col)
    return SchemaInfo(columns=normalized, type_map=type_map, dynamic_columns=schema.dynamic_columns)


def _apply_cast(schema: SchemaInfo, casts: List[Dict[str, Any]]) -> SchemaInfo:
    type_map = dict(schema.type_map)
    for cast in casts:
        column = cast.get("column")
        raw_type = cast.get("type")
        if not column or not raw_type:
            continue
        col_name = str(column)
        type_map[col_name] = normalize_schema_type(raw_type)
    return SchemaInfo(columns=list(schema.columns), type_map=type_map, dynamic_columns=schema.dynamic_columns)


def _apply_compute(schema: SchemaInfo, expression: str) -> SchemaInfo:
    expression = str(expression or "").strip()
    if not expression:
        return schema
    target = None
    if "=" in expression:
        target = expression.split("=", 1)[0].strip()
    else:
        target = "computed"
    if not target:
        return schema
    columns = list(schema.columns)
    if target not in columns:
        columns.append(target)
    type_map = dict(schema.type_map)
    if target not in type_map:
        type_map[target] = None
    return SchemaInfo(columns=columns, type_map=type_map, dynamic_columns=schema.dynamic_columns)


def _apply_group_by(schema: SchemaInfo, group_by: List[str], aggregates: List[Dict[str, Any]]) -> SchemaInfo:
    grouped = [col for col in group_by if col in schema.columns]
    type_map: Dict[str, Optional[str]] = {col: schema.type_map.get(col) for col in grouped}
    output = list(grouped)
    for agg in aggregates:
        column = str(agg.get("column") or "").strip()
        op = str(agg.get("op") or "").strip().lower()
        if not column or not op:
            continue
        alias = str(agg.get("alias") or f"{op}_{column}")
        if not alias:
            continue
        output.append(alias)
        if op == "count":
            type_map[alias] = "xsd:integer"
        elif op in {"sum", "avg"}:
            type_map[alias] = "xsd:decimal"
        elif op in {"min", "max"}:
            type_map[alias] = schema.type_map.get(column)
        else:
            type_map[alias] = None
    return SchemaInfo(columns=output, type_map=type_map, dynamic_columns=schema.dynamic_columns)


def _apply_window(schema: SchemaInfo) -> SchemaInfo:
    columns = list(schema.columns)
    if "row_number" not in columns:
        columns.append("row_number")
    type_map = dict(schema.type_map)
    type_map.setdefault("row_number", "xsd:integer")
    return SchemaInfo(columns=columns, type_map=type_map, dynamic_columns=schema.dynamic_columns)


def _apply_join(left: SchemaInfo, right: SchemaInfo) -> SchemaInfo:
    columns = list(left.columns)
    type_map: Dict[str, Optional[str]] = dict(left.type_map)
    left_set = set(left.columns)
    for col in right.columns:
        mapped = f"right_{col}" if col in left_set else col
        columns.append(mapped)
        type_map[mapped] = right.type_map.get(col)
    return SchemaInfo(
        columns=columns,
        type_map=type_map,
        dynamic_columns=left.dynamic_columns or right.dynamic_columns,
    )


def _apply_union(left: SchemaInfo, right: SchemaInfo, mode: str) -> SchemaInfo:
    normalized_mode = (mode or "strict").strip().lower()
    left_set = set(left.columns)
    right_set = set(right.columns)
    if normalized_mode == "common_only":
        columns = [col for col in left.columns if col in right_set]
    elif normalized_mode in {"pad_missing_nulls", "pad"}:
        columns = list(left.columns) + [col for col in right.columns if col not in left_set]
    else:
        columns = list(left.columns)

    type_map: Dict[str, Optional[str]] = {}
    for col in columns:
        left_type = left.type_map.get(col)
        right_type = right.type_map.get(col)
        if left_type and right_type and left_type != right_type:
            type_map[col] = None
        else:
            type_map[col] = left_type or right_type
    return SchemaInfo(
        columns=columns,
        type_map=type_map,
        dynamic_columns=left.dynamic_columns or right.dynamic_columns,
    )


def _schema_empty(schema: SchemaInfo) -> bool:
    return not schema.columns


def _column_type(schema: SchemaInfo, column: str) -> Optional[str]:
    value = schema.type_map.get(column)
    if value:
        return normalize_schema_type(value)
    return None


async def compute_pipeline_preflight(
    *,
    definition: Dict[str, Any],
    db_name: str,
    dataset_registry: Any,
    branch: Optional[str] = None,
) -> Dict[str, Any]:
    nodes = normalize_nodes(definition.get("nodes"))
    edges = normalize_edges(definition.get("edges"))
    incoming = build_incoming(edges)
    order = topological_sort(nodes, edges, include_unordered=True)

    schema_by_node: Dict[str, SchemaInfo] = {}
    issues: List[Dict[str, Any]] = []

    for node_id in order:
        node = nodes.get(node_id) or {}
        node_type = str(node.get("type") or "transform").strip().lower()
        metadata = node.get("metadata") or {}
        input_ids = incoming.get(node_id, [])
        inputs = [schema_by_node[in_id] for in_id in input_ids if in_id in schema_by_node]

        if node_type == "input":
            selection = normalize_dataset_selection(metadata, default_branch=branch or "main")
            resolution = await resolve_dataset_version(
                dataset_registry,
                db_name=db_name,
                selection=selection,
            )
            schema_by_node[node_id] = _schema_for_input(resolution.dataset, resolution.version)
            continue

        if node_type == "output":
            schema_by_node[node_id] = inputs[0] if inputs else SchemaInfo(columns=[], type_map={})
            continue

        operation = normalize_operation(metadata.get("operation"))

        if operation == "join" and len(inputs) >= 2:
            join_spec = resolve_join_spec(metadata)
            if join_spec.allow_cross_join and join_spec.join_type == "cross":
                schema_by_node[node_id] = _apply_join(inputs[0], inputs[1])
                continue
            left_key = join_spec.left_key or join_spec.right_key
            right_key = join_spec.right_key or join_spec.left_key
            if not left_key or not right_key:
                issues.append(
                    {
                        "kind": "join_key_missing",
                        "severity": "error",
                        "node_id": node_id,
                        "message": "join requires leftKey/rightKey (or joinKey)",
                    }
                )
            elif _schema_empty(inputs[0]) or _schema_empty(inputs[1]) or inputs[0].dynamic_columns or inputs[1].dynamic_columns:
                issues.append(
                    {
                        "kind": "join_preflight_skipped",
                        "severity": "warning",
                        "node_id": node_id,
                        "message": "join preflight skipped (missing schema metadata)",
                        "left_node_id": input_ids[0] if len(input_ids) > 0 else None,
                        "right_node_id": input_ids[1] if len(input_ids) > 1 else None,
                    }
                )
            else:
                left_missing = left_key not in inputs[0].columns
                right_missing = right_key not in inputs[1].columns
                if left_missing or right_missing:
                    issues.append(
                        {
                            "kind": "join_key_missing",
                            "severity": "error",
                            "node_id": node_id,
                            "message": "join key missing in input schema",
                            "left": {
                                "node_id": input_ids[0] if len(input_ids) > 0 else None,
                                "key": left_key,
                                "missing": left_missing,
                            },
                            "right": {
                                "node_id": input_ids[1] if len(input_ids) > 1 else None,
                                "key": right_key,
                                "missing": right_missing,
                            },
                        }
                    )
                else:
                    left_type = _column_type(inputs[0], left_key)
                    right_type = _column_type(inputs[1], right_key)
                    if left_type and right_type and left_type != right_type:
                        suggested_casts = [
                            {
                                "side": "left",
                                "node_id": input_ids[0] if len(input_ids) > 0 else None,
                                "column": left_key,
                                "target_type": right_type,
                                "operation": "cast",
                            },
                            {
                                "side": "right",
                                "node_id": input_ids[1] if len(input_ids) > 1 else None,
                                "column": right_key,
                                "target_type": left_type,
                                "operation": "cast",
                            },
                        ]
                        issues.append(
                            {
                                "kind": "join_key_type_mismatch",
                                "severity": "error",
                                "node_id": node_id,
                                "message": "join key types differ",
                                "left": {
                                    "node_id": input_ids[0] if len(input_ids) > 0 else None,
                                    "key": left_key,
                                    "type": left_type,
                                },
                                "right": {
                                    "node_id": input_ids[1] if len(input_ids) > 1 else None,
                                    "key": right_key,
                                    "type": right_type,
                                },
                                "suggested_casts": suggested_casts,
                            }
                        )
                    elif not left_type or not right_type:
                        issues.append(
                            {
                                "kind": "join_key_type_unknown",
                                "severity": "warning",
                                "node_id": node_id,
                                "message": "join key type unavailable for comparison",
                                "left": {
                                    "node_id": input_ids[0] if len(input_ids) > 0 else None,
                                    "key": left_key,
                                    "type": left_type,
                                },
                                "right": {
                                    "node_id": input_ids[1] if len(input_ids) > 1 else None,
                                    "key": right_key,
                                    "type": right_type,
                                },
                            }
                        )
            schema_by_node[node_id] = _apply_join(inputs[0], inputs[1])
            continue

        if operation == "union" and len(inputs) >= 2:
            union_mode = normalize_union_mode(metadata)
            left_schema = inputs[0]
            right_schema = inputs[1]
            left_set = set(left_schema.columns)
            right_set = set(right_schema.columns)

            if _schema_empty(left_schema) or _schema_empty(right_schema) or left_schema.dynamic_columns or right_schema.dynamic_columns:
                issues.append(
                    {
                        "kind": "union_preflight_skipped",
                        "severity": "warning",
                        "node_id": node_id,
                        "message": "union preflight skipped (missing schema metadata)",
                        "left_node_id": input_ids[0] if len(input_ids) > 0 else None,
                        "right_node_id": input_ids[1] if len(input_ids) > 1 else None,
                    }
                )
            else:
                common = left_set & right_set
                type_mismatches: List[Dict[str, Any]] = []
                for col in sorted(common):
                    left_type = _column_type(left_schema, col)
                    right_type = _column_type(right_schema, col)
                    if left_type and right_type and left_type != right_type:
                        type_mismatches.append(
                            {"column": col, "left_type": left_type, "right_type": right_type}
                        )

                if union_mode == "strict":
                    missing_left = sorted(right_set - left_set)
                    missing_right = sorted(left_set - right_set)
                    if missing_left or missing_right:
                        issues.append(
                            {
                                "kind": "union_strict_schema_mismatch",
                                "severity": "error",
                                "node_id": node_id,
                                "message": "union strict schema mismatch",
                                "missing_in_left": missing_left,
                                "missing_in_right": missing_right,
                                "left_node_id": input_ids[0] if len(input_ids) > 0 else None,
                                "right_node_id": input_ids[1] if len(input_ids) > 1 else None,
                            }
                        )
                    if type_mismatches:
                        issues.append(
                            {
                                "kind": "union_strict_type_mismatch",
                                "severity": "error",
                                "node_id": node_id,
                                "message": "union strict type mismatch",
                                "type_mismatches": type_mismatches,
                                "left_node_id": input_ids[0] if len(input_ids) > 0 else None,
                                "right_node_id": input_ids[1] if len(input_ids) > 1 else None,
                            }
                        )
                elif union_mode in {"pad_missing_nulls", "pad"}:
                    missing_left = sorted(right_set - left_set)
                    missing_right = sorted(left_set - right_set)
                    if missing_left or missing_right:
                        issues.append(
                            {
                                "kind": "union_padding",
                                "severity": "info",
                                "node_id": node_id,
                                "message": "union will pad missing columns with nulls",
                                "padded_in_left": missing_left,
                                "padded_in_right": missing_right,
                                "left_node_id": input_ids[0] if len(input_ids) > 0 else None,
                                "right_node_id": input_ids[1] if len(input_ids) > 1 else None,
                            }
                        )
                    if type_mismatches:
                        issues.append(
                            {
                                "kind": "union_type_mismatch",
                                "severity": "warning",
                                "node_id": node_id,
                                "message": "union type mismatch (pad mode)",
                                "type_mismatches": type_mismatches,
                                "left_node_id": input_ids[0] if len(input_ids) > 0 else None,
                                "right_node_id": input_ids[1] if len(input_ids) > 1 else None,
                            }
                        )
                elif union_mode == "common_only":
                    if not common:
                        issues.append(
                            {
                                "kind": "union_no_common_columns",
                                "severity": "error",
                                "node_id": node_id,
                                "message": "union common_only has no shared columns",
                                "left_node_id": input_ids[0] if len(input_ids) > 0 else None,
                                "right_node_id": input_ids[1] if len(input_ids) > 1 else None,
                            }
                        )
                    if type_mismatches:
                        issues.append(
                            {
                                "kind": "union_type_mismatch",
                                "severity": "warning",
                                "node_id": node_id,
                                "message": "union type mismatch (common_only)",
                                "type_mismatches": type_mismatches,
                                "left_node_id": input_ids[0] if len(input_ids) > 0 else None,
                                "right_node_id": input_ids[1] if len(input_ids) > 1 else None,
                            }
                        )

            schema_by_node[node_id] = _apply_union(left_schema, right_schema, union_mode)
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
        elif operation == "pivot":
            pivot_meta = metadata.get("pivot") or {}
            if isinstance(pivot_meta, dict):
                index_cols = _normalize_column_list(pivot_meta.get("index") or [])
            else:
                index_cols = []
            schema_by_node[node_id] = SchemaInfo(
                columns=index_cols,
                type_map={col: base.type_map.get(col) for col in index_cols},
                dynamic_columns=True,
            )
        elif operation == "window":
            schema_by_node[node_id] = _apply_window(base)
        else:
            schema_by_node[node_id] = base

    contract_specs = normalize_schema_contract(
        definition.get("schemaContract") or definition.get("schema_contract")
    )
    if contract_specs:
        output_nodes = [
            node_id for node_id, node in nodes.items() if str(node.get("type") or "").lower() == "output"
        ]
        output_node_id = output_nodes[-1] if output_nodes else (order[-1] if order else None)
        output_schema = schema_by_node.get(output_node_id) if output_node_id else None
        if not output_schema or _schema_empty(output_schema) or output_schema.dynamic_columns:
            issues.append(
                {
                    "kind": "schema_contract_preflight_skipped",
                    "severity": "warning",
                    "node_id": output_node_id,
                    "message": "schema contract preflight skipped (missing output schema)",
                }
            )
        else:
            for spec in contract_specs:
                column = spec.column
                expected_type = spec.expected_type
                actual_type = _column_type(output_schema, column)
                if column not in output_schema.columns:
                    if spec.required:
                        issues.append(
                            {
                                "kind": "schema_contract_missing_column",
                                "severity": "error",
                                "node_id": output_node_id,
                                "message": "schema contract missing required column",
                                "column": column,
                                "suggested_actions": [
                                    {"action": "update_contract", "column": column},
                                    {"action": "add_transform", "operation": "compute", "column": column},
                                ],
                            }
                        )
                    continue
                if expected_type:
                    if actual_type and actual_type != expected_type:
                        issues.append(
                            {
                                "kind": "schema_contract_type_mismatch",
                                "severity": "error",
                                "node_id": output_node_id,
                                "message": "schema contract type mismatch",
                                "column": column,
                                "expected_type": expected_type,
                                "actual_type": actual_type,
                                "suggested_actions": [
                                    {
                                        "action": "insert_cast",
                                        "operation": "cast",
                                        "column": column,
                                        "target_type": expected_type,
                                    },
                                    {"action": "update_contract", "column": column},
                                ],
                            }
                        )
                    elif not actual_type:
                        issues.append(
                            {
                                "kind": "schema_contract_type_unknown",
                                "severity": "warning",
                                "node_id": output_node_id,
                                "message": "schema contract type unknown for output column",
                                "column": column,
                                "expected_type": expected_type,
                            }
                        )

    blocking_errors = [issue for issue in issues if issue.get("severity") == "error"]
    return {
        "issues": issues,
        "blocking_errors": blocking_errors,
        "has_blocking_errors": bool(blocking_errors),
    }
