from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from shared.services.pipeline.pipeline_dataset_utils import normalize_dataset_selection, resolve_dataset_version
from shared.services.pipeline.pipeline_graph_utils import build_incoming, normalize_edges, normalize_nodes, topological_sort
from shared.services.pipeline.pipeline_schema_utils import normalize_schema_contract, normalize_schema_type
from shared.services.pipeline.pipeline_transform_spec import (
    normalize_operation,
    normalize_union_mode,
    resolve_join_spec,
    resolve_stream_join_spec,
)
from shared.services.pipeline.pipeline_type_utils import infer_xsd_type_from_values
from shared.utils.schema_columns import extract_schema_columns


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
                # Spark may strip a UTF-8 BOM prefix from CSV headers; normalize consistently.
                output.append(name.lstrip("\ufeff") or name)
        else:
            value = str(item).strip()
            if value:
                output.append(value.lstrip("\ufeff") or value)
    return output


def _extract_schema_columns(schema: Any) -> Tuple[List[str], Dict[str, Optional[str]]]:
    columns: List[str] = []
    type_map: Dict[str, Optional[str]] = {}
    for col in extract_schema_columns(schema, strip_bom=True, dedupe=True):
        name = str(col.get("name") or "").strip()
        if not name:
            continue
        columns.append(name)
        raw_type = col.get("type")
        if raw_type:
            type_map[name] = normalize_schema_type(raw_type)
    return columns, type_map


def _extract_sample_rows(sample: Any) -> List[Dict[str, Any]]:
    if not isinstance(sample, dict):
        return []
    rows = sample.get("rows")
    if isinstance(rows, list):
        if rows and isinstance(rows[0], dict):
            output: List[Dict[str, Any]] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                normalized: Dict[str, Any] = {}
                for key, value in row.items():
                    name = (str(key) or "").lstrip("\ufeff") or str(key)
                    if name in normalized:
                        base = name
                        idx = 1
                        while f"{base}__{idx}" in normalized:
                            idx += 1
                        name = f"{base}__{idx}"
                    normalized[name] = value
                output.append(normalized)
            return output
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
        output: List[Dict[str, Any]] = []
        for row in data_rows:
            if not isinstance(row, dict):
                continue
            normalized: Dict[str, Any] = {}
            for key, value in row.items():
                name = (str(key) or "").lstrip("\ufeff") or str(key)
                if name in normalized:
                    base = name
                    idx = 1
                    while f"{base}__{idx}" in normalized:
                        idx += 1
                    name = f"{base}__{idx}"
                normalized[name] = value
            output.append(normalized)
        return output
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


def _apply_compute(schema: SchemaInfo, metadata: Any) -> SchemaInfo:
    """
    Compute transforms can be represented in a few deterministic forms:

    - legacy: {"expression": "new_col = ..."}
    - preferred single column: {"targetColumn": "new_col", "formula": "..."}
    - preferred multi column: {"assignments": [{"column": "c1", "expression": "..."}, ...]}

    Preflight does NOT evaluate Spark SQL; it only propagates *column existence* so downstream
    join/filter/contract checks can validate against produced schemas.
    """
    if isinstance(metadata, dict):
        # Multi-column assignments (preferred).
        assignments = metadata.get("assignments")
        if isinstance(assignments, list) and assignments:
            columns = list(schema.columns)
            type_map = dict(schema.type_map)
            for item in assignments:
                if not isinstance(item, dict):
                    continue
                col = str(item.get("column") or item.get("target") or item.get("name") or "").strip()
                if not col:
                    continue
                if col not in columns:
                    columns.append(col)
                if col not in type_map:
                    type_map[col] = None
            return SchemaInfo(columns=columns, type_map=type_map, dynamic_columns=schema.dynamic_columns)

        # Single-column assignment (preferred).
        target = str(metadata.get("targetColumn") or metadata.get("target_column") or "").strip()
        formula = str(metadata.get("formula") or "").strip()
        if target and formula:
            columns = list(schema.columns)
            if target not in columns:
                columns.append(target)
            type_map = dict(schema.type_map)
            if target not in type_map:
                type_map[target] = None
            return SchemaInfo(columns=columns, type_map=type_map, dynamic_columns=schema.dynamic_columns)

        # Legacy "expression" string.
        metadata = metadata.get("expression") or ""

    expression = str(metadata or "").strip()
    if not expression:
        return schema

    # Best-effort: handle "a = ..." shape.
    target = expression.split("=", 1)[0].strip() if "=" in expression else "computed"
    if not target:
        return schema

    columns = list(schema.columns)
    if target not in columns:
        columns.append(target)
    type_map = dict(schema.type_map)
    if target not in type_map:
        type_map[target] = None
    return SchemaInfo(columns=columns, type_map=type_map, dynamic_columns=schema.dynamic_columns)


def _apply_geospatial(schema: SchemaInfo, metadata: Dict[str, Any]) -> SchemaInfo:
    geo = metadata.get("geospatial") if isinstance(metadata.get("geospatial"), dict) else metadata
    mode = str(geo.get("mode") or "").strip().lower()
    output_col = str(geo.get("outputColumn") or geo.get("output_column") or "").strip()
    if not output_col:
        if mode == "distance":
            output_col = "distance_km"
        elif mode == "geohash":
            output_col = "geohash"
        else:
            output_col = "point"
    columns = list(schema.columns)
    if output_col not in columns:
        columns.append(output_col)
    type_map = dict(schema.type_map)
    if mode == "distance":
        type_map[output_col] = "xsd:decimal"
    else:
        type_map[output_col] = "xsd:string"
    return SchemaInfo(columns=columns, type_map=type_map, dynamic_columns=schema.dynamic_columns)


def _apply_pattern_mining(schema: SchemaInfo, metadata: Dict[str, Any]) -> SchemaInfo:
    pattern_meta = metadata.get("patternMining") if isinstance(metadata.get("patternMining"), dict) else metadata
    output_col = str(pattern_meta.get("outputColumn") or pattern_meta.get("output_column") or "").strip()
    if not output_col:
        output_col = "pattern_match"
    mode = str(pattern_meta.get("matchMode") or pattern_meta.get("match_mode") or "contains").strip().lower()
    columns = list(schema.columns)
    if output_col not in columns:
        columns.append(output_col)
    type_map = dict(schema.type_map)
    if mode == "extract":
        type_map[output_col] = "xsd:string"
    elif mode == "count":
        type_map[output_col] = "xsd:integer"
    else:
        type_map[output_col] = "xsd:boolean"
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


def _parse_sql_alias(expr: str) -> Optional[str]:
    """
    Best-effort alias extraction for Spark SQL expressions.

    Supports the most common deterministic form:
      "<expr> AS <alias>"
    """
    raw = str(expr or "").strip()
    if not raw:
        return None
    lower = raw.lower()
    if " as " not in lower:
        return None
    before, _sep, _after = lower.rpartition(" as ")
    alias = raw[len(before) + 4 :].strip()
    if alias.startswith("`") and alias.endswith("`") and len(alias) >= 2:
        alias = alias[1:-1].replace("``", "`").strip()
    return alias or None


def _apply_group_by_expr(schema: SchemaInfo, group_by: List[str], expressions: List[Any]) -> SchemaInfo:
    """
    Schema propagation for Spark `groupBy(...).agg(expr(...).alias(...))` via `aggregateExpressions`.

    If an expression does not have an explicit alias, we conservatively mark the output as dynamic
    to avoid hard preflight failures on downstream selects.
    """
    grouped = [col for col in group_by if col in schema.columns]
    type_map: Dict[str, Optional[str]] = {col: schema.type_map.get(col) for col in grouped}
    output = list(grouped)
    existing: set[str] = set(output)

    for item in expressions or []:
        alias: Optional[str] = None
        if isinstance(item, str):
            alias = _parse_sql_alias(item)
        elif isinstance(item, dict):
            alias = str(item.get("alias") or item.get("as") or "").strip() or None
            if not alias:
                alias = _parse_sql_alias(str(item.get("expr") or item.get("expression") or ""))
        else:
            continue

        if not alias:
            return SchemaInfo(columns=output, type_map=type_map, dynamic_columns=True)

        name = alias
        if name in existing:
            base = name
            idx = 1
            while f"{base}__{idx}" in existing:
                idx += 1
            name = f"{base}__{idx}"
        existing.add(name)
        output.append(name)
        type_map.setdefault(name, None)

    return SchemaInfo(columns=output, type_map=type_map, dynamic_columns=schema.dynamic_columns)


def _apply_window(schema: SchemaInfo, metadata: Any = None) -> SchemaInfo:
    """
    Window transforms can either be declarative metadata (partition/order + row_number)
    or a list of Spark SQL expressions that add new columns.

    Preflight does NOT evaluate Spark SQL; it only propagates produced column names.
    """
    columns = list(schema.columns)
    type_map = dict(schema.type_map)

    if isinstance(metadata, dict):
        window_meta = metadata.get("window") if isinstance(metadata.get("window"), dict) else {}
        expressions = None
        if isinstance(window_meta, dict) and isinstance(window_meta.get("expressions"), list):
            expressions = window_meta.get("expressions")
        elif isinstance(metadata.get("expressions"), list):
            expressions = metadata.get("expressions")

        if isinstance(expressions, list) and expressions:
            for item in expressions:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("column") or item.get("name") or "").strip().lstrip("\ufeff")
                if not name:
                    continue
                if name not in columns:
                    columns.append(name)
                type_map.setdefault(name, None)
            return SchemaInfo(columns=columns, type_map=type_map, dynamic_columns=schema.dynamic_columns)

        output_col = str(window_meta.get("outputColumn") or "row_number").strip().lstrip("\ufeff") if isinstance(window_meta, dict) else ""
        output_col = output_col or "row_number"
        if output_col not in columns:
            columns.append(output_col)
        type_map.setdefault(output_col, "xsd:integer")
        return SchemaInfo(columns=columns, type_map=type_map, dynamic_columns=schema.dynamic_columns)

    if "row_number" not in columns:
        columns.append("row_number")
    type_map.setdefault("row_number", "xsd:integer")
    return SchemaInfo(columns=columns, type_map=type_map, dynamic_columns=schema.dynamic_columns)


def _apply_join(
    left: SchemaInfo,
    right: SchemaInfo,
    *,
    left_keys: Optional[List[str]] = None,
    right_keys: Optional[List[str]] = None,
) -> SchemaInfo:
    """
    Apply the deterministic join output naming policy.

    - Left columns are preserved.
    - Right columns that collide with left are prefixed with `right_`.
    - If join keys are same-named, Spark join de-dupes those key columns; we do NOT add a second
      `right_<key>` column for them.

    This must match the Spark runtime join behavior to prevent preflight/schema mismatches.
    """
    left_key_list = [str(k).strip().lstrip("\ufeff") for k in (left_keys or []) if str(k).strip()]
    right_key_list = [str(k).strip().lstrip("\ufeff") for k in (right_keys or []) if str(k).strip()]
    same_key_names = bool(left_key_list) and len(left_key_list) == len(right_key_list) and all(
        lk == rk for lk, rk in zip(left_key_list, right_key_list)
    )
    join_key_keep = set(right_key_list) if same_key_names else set()

    columns = list(left.columns)
    type_map: Dict[str, Optional[str]] = dict(left.type_map)
    left_set = set(left.columns)
    existing: set[str] = set(columns)

    for col in right.columns:
        if col in left_set:
            if col in join_key_keep:
                # Same-named join keys are de-duped by Spark `on=[...]`.
                continue
            mapped = f"right_{col}"
        else:
            mapped = col

        # Ensure uniqueness deterministically.
        if mapped in existing:
            base = mapped
            idx = 1
            while f"{base}__{idx}" in existing:
                idx += 1
            mapped = f"{base}__{idx}"

        columns.append(mapped)
        existing.add(mapped)
        type_map[mapped] = right.type_map.get(col)

    return SchemaInfo(
        columns=columns,
        type_map=type_map,
        dynamic_columns=left.dynamic_columns or right.dynamic_columns,
    )


def _apply_select_expr(schema: SchemaInfo, expressions: List[str]) -> Tuple[SchemaInfo, List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Best-effort schema propagation for Spark `selectExpr`.

    Returns:
    - output schema
    - missing column references (deterministic hard errors)
    - normalized column refs (BOM-stripped)
    """
    missing: List[Dict[str, Any]] = []
    normalized: List[Dict[str, Any]] = []

    # If the select uses star expansion, treat output as dynamic to avoid false claims.
    for raw in expressions:
        text = str(raw or "").strip()
        if text == "*" or text.endswith(".*"):
            return (
                SchemaInfo(columns=list(schema.columns), type_map=dict(schema.type_map), dynamic_columns=True),
                missing,
                normalized,
            )

    out_cols: List[str] = []
    out_types: Dict[str, Optional[str]] = {}
    existing: set[str] = set()

    for raw in expressions:
        expr = str(raw or "").strip()
        if not expr:
            continue

        # Handle a strict subset of expressions deterministically:
        # - `src`
        # - `src AS dst`
        # with optional backticks around identifiers.
        src = None
        dst = None
        upper = expr.upper()
        if " AS " in upper:
            parts = expr.split(" AS ", 1) if " AS " in expr else expr.split(" as ", 1)
            if len(parts) == 2:
                src = parts[0].strip()
                dst = parts[1].strip()
        else:
            src = expr
            dst = None

        def _unquote_ident(value: Optional[str]) -> Optional[str]:
            if value is None:
                return None
            text = str(value).strip()
            if text.startswith("`") and text.endswith("`") and len(text) >= 2:
                inner = text[1:-1].replace("``", "`")
                return inner.strip()
            return text

        src_name = _unquote_ident(src)
        dst_name = _unquote_ident(dst) if dst else None

        # If src is not a simple identifier, we can't validate it; only propagate alias if present.
        if src_name and ("(" in src_name or ")" in src_name or " " in src_name or "." in src_name):
            if dst_name:
                name = dst_name
                if name in existing:
                    base = name
                    idx = 1
                    while f"{base}__{idx}" in existing:
                        idx += 1
                    name = f"{base}__{idx}"
                out_cols.append(name)
                existing.add(name)
                out_types[name] = None
            continue

        if not src_name:
            continue

        resolved_src = src_name
        if resolved_src not in schema.columns:
            stripped = resolved_src.lstrip("\ufeff")
            if stripped != resolved_src and stripped in schema.columns:
                normalized.append({"original": resolved_src, "resolved": stripped})
                resolved_src = stripped
            else:
                missing.append({"column": resolved_src})
                # Still keep going so output schema is computed for LLM feedback.

        out_name = dst_name or src_name
        if out_name in existing:
            base = out_name
            idx = 1
            while f"{base}__{idx}" in existing:
                idx += 1
            out_name = f"{base}__{idx}"
        out_cols.append(out_name)
        existing.add(out_name)
        out_types[out_name] = schema.type_map.get(resolved_src)

    return SchemaInfo(columns=out_cols, type_map=out_types, dynamic_columns=schema.dynamic_columns), missing, normalized


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
    # Spark CSV reader may strip a BOM from the first header column; be forgiving in preflight.
    stripped = str(column or "").lstrip("\ufeff")
    if stripped and stripped != column:
        value = schema.type_map.get(stripped)
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

        if operation in {"join", "streamJoin"} and len(inputs) >= 2:
            join_spec = resolve_join_spec(metadata)
            if join_spec.allow_cross_join and join_spec.join_type == "cross":
                schema_by_node[node_id] = _apply_join(inputs[0], inputs[1])
                continue
            left_keys = list(join_spec.left_keys or [])
            right_keys = list(join_spec.right_keys or [])
            left_key = join_spec.left_key or join_spec.right_key
            right_key = join_spec.right_key or join_spec.left_key
            if left_key and not left_keys:
                left_keys = [left_key]
            if right_key and not right_keys:
                right_keys = [right_key]
            if not left_keys or not right_keys:
                issues.append(
                    {
                        "kind": "join_key_missing",
                        "severity": "error",
                        "node_id": node_id,
                        "message": f"{operation} requires leftKey/rightKey or leftKeys/rightKeys (or joinKey)",
                    }
                )
            elif len(left_keys) != len(right_keys):
                issues.append(
                    {
                        "kind": "join_key_count_mismatch",
                        "severity": "error",
                        "node_id": node_id,
                        "message": f"{operation} requires leftKeys/rightKeys of the same length",
                        "left_keys": left_keys,
                        "right_keys": right_keys,
                    }
                )
            if operation == "streamJoin":
                try:
                    stream_spec = resolve_stream_join_spec(metadata)
                except ValueError as exc:
                    issues.append(
                        {
                            "kind": "stream_join_invalid",
                            "severity": "error",
                            "node_id": node_id,
                            "message": str(exc),
                        }
                    )
                else:
                    if stream_spec.strategy not in {"dynamic", "left_lookup", "static"}:
                        issues.append(
                            {
                                "kind": "stream_join_strategy_invalid",
                                "severity": "error",
                                "node_id": node_id,
                                "message": f"streamJoin strategy must be dynamic|left_lookup|static (got: {stream_spec.strategy})",
                            }
                        )
                    elif stream_spec.strategy == "dynamic":
                        missing_fields: List[str] = []
                        if not str(stream_spec.left_event_time_column or "").strip():
                            missing_fields.append("leftEventTimeColumn")
                        if not str(stream_spec.right_event_time_column or "").strip():
                            missing_fields.append("rightEventTimeColumn")
                        if stream_spec.allowed_lateness_seconds is None:
                            missing_fields.append("allowedLatenessSeconds")
                        if missing_fields:
                            issues.append(
                                {
                                    "kind": "stream_join_required_field_missing",
                                    "severity": "error",
                                    "node_id": node_id,
                                    "message": f"streamJoin dynamic missing required fields: {', '.join(missing_fields)}",
                                }
                            )
                        elif stream_spec.allowed_lateness_seconds < 0:
                            issues.append(
                                {
                                    "kind": "stream_join_allowed_lateness_invalid",
                                    "severity": "error",
                                    "node_id": node_id,
                                    "message": "streamJoin allowedLatenessSeconds must be >= 0",
                                }
                            )
                        elif _schema_empty(inputs[0]) or _schema_empty(inputs[1]) or inputs[0].dynamic_columns or inputs[1].dynamic_columns:
                            issues.append(
                                {
                                    "kind": "stream_join_dynamic_preflight_skipped",
                                    "severity": "warning",
                                    "node_id": node_id,
                                    "message": "streamJoin dynamic event-time check skipped (missing schema metadata)",
                                }
                            )
                        else:
                            left_cols = set(str(col).lstrip("\ufeff") for col in inputs[0].columns or [])
                            right_cols = set(str(col).lstrip("\ufeff") for col in inputs[1].columns or [])
                            left_col = str(stream_spec.left_event_time_column or "").lstrip("\ufeff")
                            right_col = str(stream_spec.right_event_time_column or "").lstrip("\ufeff")
                            missing_schema_fields: List[str] = []
                            if left_col and left_col not in left_cols:
                                missing_schema_fields.append(f"left:{left_col}")
                            if right_col and right_col not in right_cols:
                                missing_schema_fields.append(f"right:{right_col}")
                            if missing_schema_fields:
                                issues.append(
                                    {
                                        "kind": "stream_join_event_time_missing_in_schema",
                                        "severity": "error",
                                        "node_id": node_id,
                                        "message": "streamJoin dynamic event-time columns missing in input schema",
                                        "missing": missing_schema_fields,
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
                left_cols = set(inputs[0].columns or [])
                right_cols = set(inputs[1].columns or [])
                left_cols_stripped = {str(col).lstrip("\ufeff") for col in left_cols}
                right_cols_stripped = {str(col).lstrip("\ufeff") for col in right_cols}

                left_missing: List[str] = []
                right_missing: List[str] = []
                normalized: List[Dict[str, Any]] = []
                for key in left_keys:
                    if key in left_cols:
                        continue
                    stripped = str(key).lstrip("\ufeff")
                    if stripped in left_cols_stripped:
                        # Be forgiving when schema columns contain a UTF-8 BOM prefix (Spark may strip it at read time).
                        normalized.append({"side": "left", "original": key, "resolved": stripped})
                        continue
                    left_missing.append(key)
                for key in right_keys:
                    if key in right_cols:
                        continue
                    stripped = str(key).lstrip("\ufeff")
                    if stripped in right_cols_stripped:
                        normalized.append({"side": "right", "original": key, "resolved": stripped})
                        continue
                    right_missing.append(key)
                if left_missing or right_missing:
                    issues.append(
                        {
                            "kind": "join_key_missing",
                            "severity": "error",
                            "node_id": node_id,
                            "message": "join key missing in input schema",
                            "left": {
                                "node_id": input_ids[0] if len(input_ids) > 0 else None,
                                "keys": left_keys,
                                "missing": left_missing,
                            },
                            "right": {
                                "node_id": input_ids[1] if len(input_ids) > 1 else None,
                                "keys": right_keys,
                                "missing": right_missing,
                            },
                            # Provide a small, deterministic witness surface so the agent can repair
                            # without guessing (avoid semantic/heuristic routing).
                            "left_available_columns": list(inputs[0].columns or [])[:60],
                            "right_available_columns": list(inputs[1].columns or [])[:60],
                        }
                    )
                else:
                    if normalized:
                        issues.append(
                            {
                                "kind": "join_key_normalized",
                                "severity": "warning",
                                "node_id": node_id,
                                "message": "join key normalized by stripping UTF-8 BOM prefix",
                                "normalized": normalized,
                            }
                        )
                    mismatches: List[Dict[str, Any]] = []
                    unknowns: List[Dict[str, Any]] = []
                    suggested_casts: List[Dict[str, Any]] = []
                    for left_key_item, right_key_item in zip(left_keys, right_keys):
                        left_type = _column_type(inputs[0], left_key_item)
                        right_type = _column_type(inputs[1], right_key_item)
                        if left_type and right_type and left_type != right_type:
                            mismatches.append(
                                {
                                    "left_key": left_key_item,
                                    "right_key": right_key_item,
                                    "left_type": left_type,
                                    "right_type": right_type,
                                }
                            )
                            suggested_casts.extend(
                                [
                                    {
                                        "side": "left",
                                        "node_id": input_ids[0] if len(input_ids) > 0 else None,
                                        "column": left_key_item,
                                        "target_type": right_type,
                                        "operation": "cast",
                                    },
                                    {
                                        "side": "right",
                                        "node_id": input_ids[1] if len(input_ids) > 1 else None,
                                        "column": right_key_item,
                                        "target_type": left_type,
                                        "operation": "cast",
                                    },
                                ]
                            )
                        elif not left_type or not right_type:
                            unknowns.append(
                                {
                                    "left_key": left_key_item,
                                    "right_key": right_key_item,
                                    "left_type": left_type,
                                    "right_type": right_type,
                                }
                            )
                    if mismatches:
                        issues.append(
                            {
                                "kind": "join_key_type_mismatch",
                                "severity": "error",
                                "node_id": node_id,
                                "message": "join key types differ",
                                "left": {
                                    "node_id": input_ids[0] if len(input_ids) > 0 else None,
                                    "keys": left_keys,
                                },
                                "right": {
                                    "node_id": input_ids[1] if len(input_ids) > 1 else None,
                                    "keys": right_keys,
                                },
                                "mismatches": mismatches,
                                "suggested_casts": suggested_casts,
                            }
                        )
                    elif unknowns:
                        issues.append(
                            {
                                "kind": "join_key_type_unknown",
                                "severity": "warning",
                                "node_id": node_id,
                                "message": "join key type unavailable for comparison",
                                "left": {
                                    "node_id": input_ids[0] if len(input_ids) > 0 else None,
                                    "keys": left_keys,
                                },
                                "right": {
                                    "node_id": input_ids[1] if len(input_ids) > 1 else None,
                                    "keys": right_keys,
                                },
                                "unknowns": unknowns,
                            }
                        )
            schema_by_node[node_id] = _apply_join(inputs[0], inputs[1], left_keys=left_keys, right_keys=right_keys)
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
            exprs = metadata.get("expressions") or metadata.get("selectExpr") or metadata.get("select_expr")
            if isinstance(exprs, list) and exprs:
                schema, missing, normalized = _apply_select_expr(base, [str(item) for item in exprs])
                if missing and not base.dynamic_columns:
                    issues.append(
                        {
                            "kind": "select_expr_missing_column",
                            "severity": "error",
                            "node_id": node_id,
                            "message": "selectExpr references missing column(s)",
                            "missing": missing,
                            "input_node_id": input_ids[0] if len(input_ids) > 0 else None,
                            "available_columns": list(base.columns or [])[:80],
                        }
                    )
                if normalized:
                    issues.append(
                        {
                            "kind": "select_expr_normalized",
                            "severity": "warning",
                            "node_id": node_id,
                            "message": "selectExpr column normalized by stripping UTF-8 BOM prefix",
                            "normalized": normalized,
                            "input_node_id": input_ids[0] if len(input_ids) > 0 else None,
                        }
                    )
                schema_by_node[node_id] = schema
            else:
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
            schema_by_node[node_id] = _apply_compute(base, metadata)
        elif operation == "split":
            schema_by_node[node_id] = base
        elif operation == "geospatial":
            schema_by_node[node_id] = _apply_geospatial(base, metadata)
        elif operation == "patternMining":
            schema_by_node[node_id] = _apply_pattern_mining(base, metadata)
        elif operation == "regexReplace":
            schema_by_node[node_id] = base
        elif operation in {"groupBy", "aggregate"}:
            group_by = _normalize_column_list(metadata.get("groupBy") or [])
            expr_items = (
                metadata.get("aggregateExpressions")
                or metadata.get("aggExpressions")
                or metadata.get("aggregate_expressions")
            )
            if isinstance(expr_items, list) and expr_items:
                schema_by_node[node_id] = _apply_group_by_expr(base, group_by, expr_items)
            else:
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
            schema_by_node[node_id] = _apply_window(base, metadata)
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


async def compute_schema_by_node(
    *,
    definition: Dict[str, Any],
    db_name: str,
    dataset_registry: Any,
    branch: Optional[str] = None,
) -> Dict[str, SchemaInfo]:
    nodes = normalize_nodes(definition.get("nodes"))
    edges = normalize_edges(definition.get("edges"))
    incoming = build_incoming(edges)
    order = topological_sort(nodes, edges, include_unordered=True)

    schema_by_node: Dict[str, SchemaInfo] = {}
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

        if operation in {"join", "streamJoin"} and len(inputs) >= 2:
            join_spec = resolve_join_spec(metadata)
            left_keys = list(join_spec.left_keys or [])
            right_keys = list(join_spec.right_keys or [])
            left_key = join_spec.left_key or join_spec.right_key
            right_key = join_spec.right_key or join_spec.left_key
            if left_key and not left_keys:
                left_keys = [left_key]
            if right_key and not right_keys:
                right_keys = [right_key]
            if operation == "streamJoin":
                try:
                    resolve_stream_join_spec(metadata)
                except ValueError:
                    pass
            schema_by_node[node_id] = _apply_join(inputs[0], inputs[1], left_keys=left_keys, right_keys=right_keys)
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
            exprs = metadata.get("expressions") or metadata.get("selectExpr") or metadata.get("select_expr")
            if isinstance(exprs, list) and exprs:
                schema_by_node[node_id] = _apply_select_expr(base, [str(item) for item in exprs])[0]
            else:
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
            schema_by_node[node_id] = _apply_compute(base, metadata)
        elif operation == "split":
            schema_by_node[node_id] = base
        elif operation == "geospatial":
            schema_by_node[node_id] = _apply_geospatial(base, metadata)
        elif operation == "patternMining":
            schema_by_node[node_id] = _apply_pattern_mining(base, metadata)
        elif operation == "regexReplace":
            schema_by_node[node_id] = base
        elif operation in {"groupBy", "aggregate"}:
            group_by = _normalize_column_list(metadata.get("groupBy") or [])
            expr_items = (
                metadata.get("aggregateExpressions")
                or metadata.get("aggExpressions")
                or metadata.get("aggregate_expressions")
            )
            if isinstance(expr_items, list) and expr_items:
                schema_by_node[node_id] = _apply_group_by_expr(base, group_by, expr_items)
            else:
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
            schema_by_node[node_id] = _apply_window(base, metadata)
        else:
            schema_by_node[node_id] = base

    return schema_by_node
