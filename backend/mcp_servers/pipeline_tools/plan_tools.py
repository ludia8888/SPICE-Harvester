from __future__ import annotations

import logging
from dataclasses import asdict
from typing import Any, Awaitable, Callable, Dict, List

from bff.services.pipeline_join_evaluator import evaluate_pipeline_joins
from bff.services.pipeline_plan_validation import validate_pipeline_plan
from shared.models.pipeline_plan import PipelinePlan
from shared.services.pipeline.pipeline_claim_refuter import refute_pipeline_plan_claims
from shared.services.pipeline.pipeline_executor import PipelineExecutor
from shared.services.pipeline.pipeline_preview_policy import evaluate_preview_policy
from shared.services.pipeline.pipeline_plan_builder import (
    PipelinePlanBuilderError,
    add_cast,
    add_compute,
    add_compute_assignments,
    add_compute_column,
    add_dedupe,
    add_drop,
    add_edge,
    add_explode,
    add_external_input,
    add_filter,
    add_group_by_expr,
    add_join,
    add_normalize,
    add_output,
    add_pivot,
    add_regex_replace,
    add_rename,
    add_select,
    add_select_expr,
    add_sort,
    add_transform,
    add_union,
    add_window_expr,
    configure_input_read,
    delete_edge,
    delete_node,
    new_plan,
    reset_plan,
    set_node_inputs,
    update_node_metadata,
    update_output,
    update_settings,
    validate_structure,
    add_input,
)
from shared.utils.llm_safety import mask_pii

from ..pipeline_mcp_helpers import normalize_aggregates, normalize_string_list

logger = logging.getLogger(__name__)

ToolHandler = Callable[[Any, Dict[str, Any]], Awaitable[Any]]


async def _plan_new(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = new_plan(
        goal=str(arguments.get("goal") or ""),
        db_name=str(arguments.get("db_name") or ""),
        branch=str(arguments.get("branch") or "").strip() or None,
        dataset_ids=arguments.get("dataset_ids"),
    )
    return {"plan": plan}


async def _plan_reset(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    result = reset_plan(plan)
    return {"plan": result.plan, "warnings": list(result.warnings)}


async def _plan_add_input(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    result = add_input(
        plan,
        dataset_id=arguments.get("dataset_id"),
        dataset_name=arguments.get("dataset_name"),
        dataset_branch=arguments.get("dataset_branch"),
        read=arguments.get("read") if isinstance(arguments.get("read"), dict) else None,
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_external_input(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    read_obj = arguments.get("read")
    if not isinstance(read_obj, dict) or not read_obj:
        return {"status": "invalid", "errors": ["read must be a non-empty object"]}
    result = add_external_input(
        plan,
        read=read_obj,
        source_name=arguments.get("source_name") or arguments.get("sourceName"),
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_configure_input_read(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    patch: Dict[str, Any] = {}
    read_obj = arguments.get("read")
    if isinstance(read_obj, dict):
        patch.update(read_obj)
    if arguments.get("format"):
        patch["format"] = str(arguments.get("format")).strip()
    options_obj = arguments.get("options")
    if isinstance(options_obj, dict) and options_obj:
        patch["options"] = dict(options_obj)
    options_env_obj = arguments.get("options_env")
    if isinstance(options_env_obj, dict) and options_env_obj:
        patch["options_env"] = dict(options_env_obj)
    schema_obj = arguments.get("schema")
    if isinstance(schema_obj, list) and schema_obj:
        patch["schema"] = schema_obj
    if arguments.get("mode") is not None:
        patch["mode"] = str(arguments.get("mode"))
    if arguments.get("corrupt_record_column") is not None:
        patch["corrupt_record_column"] = str(arguments.get("corrupt_record_column"))
    if "header" in arguments:
        patch["header"] = bool(arguments.get("header"))
    if "infer_schema" in arguments:
        patch["infer_schema"] = bool(arguments.get("infer_schema"))
    result = configure_input_read(
        plan,
        node_id=str(arguments.get("node_id") or ""),
        read=patch,
        replace=bool(arguments.get("replace") or False),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_join(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    left_keys = (
        arguments.get("left_keys")
        or arguments.get("leftKeys")
        or arguments.get("left_columns")
        or arguments.get("leftColumns")
    )
    right_keys = (
        arguments.get("right_keys")
        or arguments.get("rightKeys")
        or arguments.get("right_columns")
        or arguments.get("rightColumns")
    )
    if not left_keys:
        left_keys = (
            arguments.get("left_column")
            or arguments.get("leftColumn")
            or arguments.get("left_key")
            or arguments.get("leftKey")
            or arguments.get("join_key")
            or arguments.get("joinKey")
        )
    if not right_keys:
        right_keys = (
            arguments.get("right_column")
            or arguments.get("rightColumn")
            or arguments.get("right_key")
            or arguments.get("rightKey")
            or arguments.get("join_key")
            or arguments.get("joinKey")
        )
    join_type = str(arguments.get("join_type") or arguments.get("joinType") or "").strip().lower()
    valid_join_types = {"inner", "left", "right", "full", "cross"}
    if not join_type:
        return {"status": "invalid", "errors": ["join_type is required. Specify one of: inner, left, right, full, cross"]}
    if join_type not in valid_join_types:
        return {"status": "invalid", "errors": [f"Invalid join_type '{join_type}'. Must be one of: {', '.join(sorted(valid_join_types))}"]}
    # Validate key count match
    left_keys_list = normalize_string_list(left_keys)
    right_keys_list = normalize_string_list(right_keys)
    if len(left_keys_list) != len(right_keys_list):
        return {
            "status": "invalid",
            "errors": [
                f"Join key count mismatch: left_keys has {len(left_keys_list)} keys, "
                f"right_keys has {len(right_keys_list)} keys. They must be equal."
            ],
            "left_keys": left_keys_list,
            "right_keys": right_keys_list,
        }
    result = add_join(
        plan,
        left_node_id=str(arguments.get("left_node_id") or ""),
        right_node_id=str(arguments.get("right_node_id") or ""),
        left_keys=left_keys_list,
        right_keys=right_keys_list,
        join_type=join_type,
        join_hints=arguments.get("join_hints") if isinstance(arguments.get("join_hints"), dict) else None,
        broadcast_left=bool(arguments.get("broadcast_left") or False),
        broadcast_right=bool(arguments.get("broadcast_right") or False),
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_group_by(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    input_node_id = str(arguments.get("input_node_id") or "")
    group_by = normalize_string_list(arguments.get("group_by") or arguments.get("groupBy") or [])
    aggregates, agg_warnings = normalize_aggregates(arguments.get("aggregates") or arguments.get("aggregations") or [])
    if not aggregates:
        if agg_warnings:
            raise PipelinePlanBuilderError(f"aggregates is required (items: {{column,op,alias?}}). Parse warnings: {agg_warnings}")
        raise PipelinePlanBuilderError("aggregates is required (items: {column,op,alias?})")
    op = str(arguments.get("operation") or "groupBy").strip() or "groupBy"
    if op not in {"groupBy", "aggregate"}:
        op = "groupBy"
    result = add_transform(
        plan,
        operation=op,
        input_node_ids=[input_node_id],
        metadata={"groupBy": group_by, "aggregates": aggregates},
        node_id=arguments.get("node_id"),
    )
    all_warnings = list(result.warnings) + agg_warnings
    return {"plan": result.plan, "node_id": result.node_id, "warnings": all_warnings}


async def _plan_add_group_by_expr(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    input_node_id = str(arguments.get("input_node_id") or "")
    group_by = normalize_string_list(arguments.get("group_by") or arguments.get("groupBy") or [])
    exprs = arguments.get("aggregate_expressions") or arguments.get("aggregateExpressions") or []
    op = str(arguments.get("operation") or "groupBy").strip() or "groupBy"
    if op not in {"groupBy", "aggregate"}:
        op = "groupBy"
    result = add_group_by_expr(
        plan,
        input_node_id=input_node_id,
        group_by=group_by,
        aggregate_expressions=exprs if isinstance(exprs, list) else [exprs],
        operation=op,
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_window(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    input_node_id = str(arguments.get("input_node_id") or "")
    window = arguments.get("window") if isinstance(arguments.get("window"), dict) else None
    if window is None:
        partition_by = normalize_string_list(arguments.get("partition_by") or arguments.get("partitionBy") or [])
        order_by_raw = arguments.get("order_by") or arguments.get("orderBy") or []
        order_by = order_by_raw if isinstance(order_by_raw, list) else [order_by_raw]
        window = {"partitionBy": partition_by, "orderBy": order_by}
    result = add_transform(
        plan,
        operation="window",
        input_node_ids=[input_node_id],
        metadata={"window": window},
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_window_expr(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    input_node_id = str(arguments.get("input_node_id") or "")
    expressions = arguments.get("expressions") or []
    result = add_window_expr(
        plan,
        input_node_id=input_node_id,
        expressions=expressions if isinstance(expressions, list) else [expressions],
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_transform(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    operation = str(arguments.get("operation") or "").strip()
    input_node_ids = arguments.get("input_node_ids")
    if not input_node_ids:
        single = arguments.get("input_node_id") or arguments.get("inputNodeId")
        input_node_ids = [single] if single else []
    metadata = arguments.get("metadata") or {}
    if not isinstance(metadata, dict):
        metadata = {}
    transform_agg_warnings: List[str] = []
    if not metadata and operation in {"groupBy", "aggregate"}:
        group_by = normalize_string_list(arguments.get("group_by") or arguments.get("groupBy") or [])
        aggregates, transform_agg_warnings = normalize_aggregates(arguments.get("aggregates") or arguments.get("aggregations") or [])
        metadata = {"groupBy": group_by, "aggregates": aggregates}
    if not metadata and operation == "window":
        window = arguments.get("window") if isinstance(arguments.get("window"), dict) else None
        if window is None:
            partition_by = normalize_string_list(arguments.get("partition_by") or arguments.get("partitionBy") or [])
            order_by_raw = arguments.get("order_by") or arguments.get("orderBy") or []
            order_by = order_by_raw if isinstance(order_by_raw, list) else [order_by_raw]
            window = {"partitionBy": partition_by, "orderBy": order_by}
        metadata = {"window": window}
    result = add_transform(
        plan,
        operation=operation,
        input_node_ids=input_node_ids or [],
        metadata=metadata,
        node_id=arguments.get("node_id"),
    )
    all_warnings = list(result.warnings) + transform_agg_warnings
    return {"plan": result.plan, "node_id": result.node_id, "warnings": all_warnings}


async def _plan_add_sort(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    input_node_id = str(arguments.get("input_node_id") or "")
    cols = arguments.get("columns") or []
    columns = cols if isinstance(cols, list) else [cols]
    result = add_sort(
        plan,
        input_node_id=input_node_id,
        columns=columns,
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_explode(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    input_node_id = str(arguments.get("input_node_id") or "")
    column = str(arguments.get("column") or "").strip()
    result = add_explode(
        plan,
        input_node_id=input_node_id,
        column=column,
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_union(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    result = add_union(
        plan,
        left_node_id=str(arguments.get("left_node_id") or ""),
        right_node_id=str(arguments.get("right_node_id") or ""),
        union_mode=str(arguments.get("union_mode") or "strict"),
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_pivot(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    input_node_id = str(arguments.get("input_node_id") or "")
    index = arguments.get("index") or []
    if not isinstance(index, list):
        index = [index]
    columns_val = str(arguments.get("columns") or "").strip()
    values_val = str(arguments.get("values") or "").strip()
    # Validate required fields
    pivot_errors: List[str] = []
    if not input_node_id:
        pivot_errors.append("input_node_id is required")
    if not columns_val:
        pivot_errors.append("columns is required (column to pivot on)")
    if not values_val:
        pivot_errors.append("values is required (column to aggregate)")
    if pivot_errors:
        return {"status": "invalid", "errors": pivot_errors}
    result = add_pivot(
        plan,
        input_node_id=input_node_id,
        index=[str(item) for item in index],
        columns=columns_val,
        values=values_val,
        agg=str(arguments.get("agg") or "sum"),
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_filter(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    result = add_filter(
        plan,
        input_node_id=str(arguments.get("input_node_id") or ""),
        expression=str(arguments.get("expression") or ""),
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_compute(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    input_node_id = str(arguments.get("input_node_id") or "")
    expr = str(arguments.get("expression") or "").strip()

    # Common LLM shape: {"computations":[{"alias":"x","expression":"a*b"}, ...]}
    if not expr:
        raw = (
            arguments.get("computations")
            or arguments.get("computes")
            or arguments.get("compute")
            or arguments.get("expressions")
            or []
        )
        items = raw if isinstance(raw, list) else [raw]
        assignments: list[dict[str, Any]] = []
        for item in items:
            if isinstance(item, str) and item.strip():
                text = item.strip()
                if "=" in text:
                    left, right = [part.strip() for part in text.split("=", 1)]
                    if left and right:
                        assignments.append({"column": left, "expression": right})
                continue
            if not isinstance(item, dict):
                continue
            alias = (
                item.get("alias")
                or item.get("new_column")
                or item.get("newColumn")
                or item.get("column")
                or item.get("name")
            )
            formula = item.get("expression") or item.get("formula") or item.get("expr")
            alias_value = str(alias or "").strip()
            formula_value = str(formula or "").strip()
            if not alias_value or not formula_value:
                continue
            assignments.append({"column": alias_value, "expression": formula_value})

        if assignments:
            result = add_compute_assignments(
                plan,
                input_node_id=input_node_id,
                assignments=assignments,
                node_id=arguments.get("node_id"),
            )
            return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

    # Another common shape: expression="a*b", alias="x" (no assignment in expression).
    alias = str(
        arguments.get("alias")
        or arguments.get("new_column")
        or arguments.get("newColumn")
        or ""
    ).strip()
    if expr and alias and "=" not in expr:
        result = add_compute_column(
            plan,
            input_node_id=input_node_id,
            target_column=alias,
            formula=expr,
            node_id=arguments.get("node_id"),
        )
        return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}

    result = add_compute(
        plan,
        input_node_id=input_node_id,
        expression=expr,
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_compute_column(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    result = add_compute_column(
        plan,
        input_node_id=str(arguments.get("input_node_id") or ""),
        target_column=str(arguments.get("target_column") or ""),
        formula=str(arguments.get("formula") or ""),
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_compute_assignments(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    result = add_compute_assignments(
        plan,
        input_node_id=str(arguments.get("input_node_id") or ""),
        assignments=arguments.get("assignments") or [],
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_cast(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    result = add_cast(
        plan,
        input_node_id=str(arguments.get("input_node_id") or ""),
        casts=arguments.get("casts") or [],
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_rename(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    # Be forgiving: LLMs often pass rename maps as a list of {from,to} objects or
    # use alternate field names like `mappings` / `renames`.
    raw_rename = arguments.get("rename")
    if raw_rename is None:
        raw_rename = arguments.get("renames")
    if raw_rename is None:
        raw_rename = arguments.get("mappings")

    rename_map: Dict[str, str] = {}
    if isinstance(raw_rename, dict):
        # Accept both mapping-form {"old":"new"} and single-pair form {"from":"old","to":"new"}.
        keys = {str(k) for k in raw_rename.keys() if k is not None}
        if ({"from", "to"} <= keys) or ({"src", "dst"} <= keys) or ({"source", "target"} <= keys):
            src = str(
                raw_rename.get("from")
                or raw_rename.get("src")
                or raw_rename.get("source")
                or ""
            ).strip()
            dst = str(
                raw_rename.get("to")
                or raw_rename.get("dst")
                or raw_rename.get("target")
                or ""
            ).strip()
            if src and dst:
                rename_map[src] = dst
        else:
            for k, v in raw_rename.items():
                src = str(k or "").strip()
                dst = str(v or "").strip()
                if src and dst:
                    rename_map[src] = dst
    elif isinstance(raw_rename, list):
        for item in raw_rename:
            if not isinstance(item, dict):
                continue
            src = str(item.get("from") or item.get("src") or item.get("source") or "").strip()
            dst = str(item.get("to") or item.get("dst") or item.get("target") or "").strip()
            if src and dst:
                rename_map[src] = dst

    result = add_rename(
        plan,
        input_node_id=str(arguments.get("input_node_id") or ""),
        rename=rename_map,
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_select(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    result = add_select(
        plan,
        input_node_id=str(arguments.get("input_node_id") or ""),
        columns=arguments.get("columns") or [],
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_select_expr(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    result = add_select_expr(
        plan,
        input_node_id=str(arguments.get("input_node_id") or ""),
        expressions=arguments.get("expressions") or [],
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_drop(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    result = add_drop(
        plan,
        input_node_id=str(arguments.get("input_node_id") or ""),
        columns=arguments.get("columns") or [],
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_dedupe(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    result = add_dedupe(
        plan,
        input_node_id=str(arguments.get("input_node_id") or ""),
        columns=arguments.get("columns") or [],
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_normalize(_server: Any, arguments: Dict[str, Any]) -> Any:
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


async def _plan_add_regex_replace(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    result = add_regex_replace(
        plan,
        input_node_id=str(arguments.get("input_node_id") or ""),
        rules=arguments.get("rules") or [],
        node_id=arguments.get("node_id"),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_add_output(_server: Any, arguments: Dict[str, Any]) -> Any:
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


async def _plan_add_edge(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    result = add_edge(
        plan,
        from_node_id=str(arguments.get("from_node_id") or ""),
        to_node_id=str(arguments.get("to_node_id") or ""),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_delete_edge(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    result = delete_edge(
        plan,
        from_node_id=str(arguments.get("from_node_id") or ""),
        to_node_id=str(arguments.get("to_node_id") or ""),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_set_node_inputs(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    result = set_node_inputs(
        plan,
        node_id=str(arguments.get("node_id") or ""),
        input_node_ids=arguments.get("input_node_ids") or [],
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_update_node_metadata(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    # Models often guess "metadata"/"meta" instead of the canonical "set" param.
    set_fields = arguments.get("set")
    if set_fields is None:
        candidate = arguments.get("metadata")
        if isinstance(candidate, dict):
            set_fields = candidate
    if set_fields is None:
        candidate = arguments.get("meta")
        if isinstance(candidate, dict):
            set_fields = candidate
    result = update_node_metadata(
        plan,
        node_id=str(arguments.get("node_id") or ""),
        set_fields=set_fields,
        unset_fields=arguments.get("unset"),
        replace=bool(arguments.get("replace", False)),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_update_settings(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    set_fields = arguments.get("set")
    if set_fields is None:
        candidate = arguments.get("settings")
        if isinstance(candidate, dict):
            set_fields = candidate
    result = update_settings(
        plan,
        set_fields=set_fields,
        unset_fields=arguments.get("unset"),
        replace=bool(arguments.get("replace", False)),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_delete_node(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    result = delete_node(
        plan,
        node_id=str(arguments.get("node_id") or ""),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_update_output(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    output_name = str(arguments.get("output_name") or "").strip()
    if not output_name:
        # Convenience: allow selecting the output entry by output node id, since most other
        # plan patch tools are node_id-based and LLMs commonly supply node_id.
        node_id = str(arguments.get("node_id") or "").strip()
        if node_id:
            definition = plan.get("definition_json")
            nodes = definition.get("nodes") if isinstance(definition, dict) else None
            if isinstance(nodes, list):
                for node in nodes:
                    if not isinstance(node, dict):
                        continue
                    if str(node.get("id") or "").strip() != node_id:
                        continue
                    if str(node.get("type") or "").strip().lower() != "output":
                        continue
                    meta = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
                    output_name = str(meta.get("outputName") or meta.get("output_name") or "").strip()
                    break
    if not output_name:
        return {"error": "output_name is required (or provide node_id of an output node)."}
    result = update_output(
        plan,
        output_name=output_name,
        set_fields=arguments.get("set"),
        unset_fields=arguments.get("unset"),
        replace=bool(arguments.get("replace", False)),
    )
    return {"plan": result.plan, "node_id": result.node_id, "warnings": list(result.warnings)}


async def _plan_validate_structure(_server: Any, arguments: Dict[str, Any]) -> Any:
    plan = arguments.get("plan") or {}
    errors, warnings = validate_structure(plan)
    return {"errors": errors, "warnings": warnings}


async def _plan_validate(server: Any, arguments: Dict[str, Any]) -> Any:
    plan_obj = arguments.get("plan") or {}
    try:
        plan = PipelinePlan.model_validate(plan_obj)
    except Exception as exc:
        return {"status": "invalid", "errors": [str(exc)], "warnings": []}

    db_name = str(plan.data_scope.db_name or "").strip()
    if not db_name:
        return {"status": "invalid", "errors": ["plan.data_scope.db_name is required"], "warnings": []}

    dataset_registry, _ = await server._ensure_registries()
    validation = await validate_pipeline_plan(
        plan=plan,
        dataset_registry=dataset_registry,
        db_name=db_name,
        branch=str(plan.data_scope.branch or "") or None,
        require_output=bool(arguments.get("require_output", False)),
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


async def _plan_preview(server: Any, arguments: Dict[str, Any]) -> Any:
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

    dataset_registry, _ = await server._ensure_registries()

    # Inject StorageService so preview can read actual artifacts from S3/lakeFS
    storage_service = None
    try:
        pipeline_registry = await server._ensure_pipeline_registry()
        storage_service = await pipeline_registry.get_lakefs_storage()
    except Exception as exc:
        logger.debug("plan_preview: lakeFS storage unavailable, falling back to sample_json: %s", exc)

    executor = PipelineExecutor(dataset_registry, storage_service=storage_service)
    limit = int(arguments.get("limit") or 50)
    node_id = str(arguments.get("node_id") or "").strip() or None

    definition = dict(plan.definition_json or {})
    preview_meta = dict(definition.get("__preview_meta__") or {})
    preview_meta.setdefault("branch", str(plan.data_scope.branch or "") or "main")
    preview_meta["sample_limit"] = max(1, min(limit, 200))
    definition["__preview_meta__"] = preview_meta

    preview_policy = evaluate_preview_policy(definition)
    policy_level = str(preview_policy.get("level") or "allow").strip().lower()
    if policy_level == "deny":
        return {
            "status": "preview_denied",
            "error": "Plan preview denied by policy.",
            "preview_policy": preview_policy,
            "preview": {
                "row_count": 0,
                "columns": [],
                "rows": [],
            },
            "hint": "Fix the denied operations (see preview_policy) before preview/build/deploy.",
            "warnings": list(warnings or []),
        }
    if policy_level == "require_spark":
        return {
            "status": "requires_spark_preview",
            "preview_policy": preview_policy,
            "preview": {
                "row_count": 0,
                "columns": [],
                "rows": [],
            },
            "hint": (
                "Plan preview is not reliable for this plan. Save/materialize the pipeline first, "
                "then use Spark-backed preview (pipeline_preview_wait) or Build."
            ),
            "warnings": list(warnings or []),
        }

    try:
        preview = await executor.preview(definition=definition, db_name=db_name, node_id=node_id, limit=limit)
    except Exception as exc:
        # Plan preview runs in a lightweight Python executor that cannot support all Spark SQL
        # expressions. Return explicit error status instead of hiding as warning.
        logger.warning("plan_preview failed: %s", exc, exc_info=True)
        return {
            "status": "preview_failed",
            "error": str(exc),
            "error_type": type(exc).__name__,
            "preview_policy": preview_policy,
            "preview": {
                "row_count": 0,
                "columns": [],
                "rows": [],
            },
            "hint": (
                "Plan preview failed. Save/materialize the pipeline first, then use Spark-backed preview "
                "(pipeline_preview_wait) or Build."
            ),
            "warnings": list(warnings or []),
        }

    preview_masked = mask_pii(preview)
    return {"status": "success", "preview": preview_masked, "preview_policy": preview_policy, "warnings": warnings}


async def _plan_refute_claims(server: Any, arguments: Dict[str, Any]) -> Any:
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

    dataset_registry, _ = await server._ensure_registries()
    report = await refute_pipeline_plan_claims(
        plan=plan,
        dataset_registry=dataset_registry,
        run_tables=arguments.get("run_tables"),
        sample_limit=int(arguments.get("sample_limit") or 400),
        max_output_rows=int(arguments.get("max_output_rows") or 20000),
        max_hard_failures=int(arguments.get("max_hard_failures") or 5),
        max_soft_warnings=int(arguments.get("max_soft_warnings") or 20),
    )
    if warnings:
        merged = list(report.get("warnings") or []) if isinstance(report, dict) else []
        merged.extend([w for w in warnings if w])
        if isinstance(report, dict):
            report["warnings"] = merged[:40]
    return report


async def _plan_evaluate_joins(server: Any, arguments: Dict[str, Any]) -> Any:
    plan_obj = arguments.get("plan") or {}
    try:
        plan = PipelinePlan.model_validate(plan_obj)
    except Exception as exc:
        return {"status": "invalid", "errors": [str(exc)], "warnings": []}

    db_name = str(plan.data_scope.db_name or "").strip()
    if not db_name:
        return {"status": "invalid", "errors": ["plan.data_scope.db_name is required"], "warnings": []}

    dataset_registry, _ = await server._ensure_registries()
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


PLAN_TOOL_HANDLERS: Dict[str, ToolHandler] = {
    "plan_new": _plan_new,
    "plan_reset": _plan_reset,
    "plan_add_input": _plan_add_input,
    "plan_add_external_input": _plan_add_external_input,
    "plan_configure_input_read": _plan_configure_input_read,
    "plan_add_join": _plan_add_join,
    "plan_add_group_by": _plan_add_group_by,
    "plan_add_group_by_expr": _plan_add_group_by_expr,
    "plan_add_window": _plan_add_window,
    "plan_add_window_expr": _plan_add_window_expr,
    "plan_add_transform": _plan_add_transform,
    "plan_add_sort": _plan_add_sort,
    "plan_add_explode": _plan_add_explode,
    "plan_add_union": _plan_add_union,
    "plan_add_pivot": _plan_add_pivot,
    "plan_add_filter": _plan_add_filter,
    "plan_add_compute": _plan_add_compute,
    "plan_add_compute_column": _plan_add_compute_column,
    "plan_add_compute_assignments": _plan_add_compute_assignments,
    "plan_add_cast": _plan_add_cast,
    "plan_add_rename": _plan_add_rename,
    "plan_add_select": _plan_add_select,
    "plan_add_select_expr": _plan_add_select_expr,
    "plan_add_drop": _plan_add_drop,
    "plan_add_dedupe": _plan_add_dedupe,
    "plan_add_normalize": _plan_add_normalize,
    "plan_add_regex_replace": _plan_add_regex_replace,
    "plan_add_output": _plan_add_output,
    "plan_add_edge": _plan_add_edge,
    "plan_delete_edge": _plan_delete_edge,
    "plan_set_node_inputs": _plan_set_node_inputs,
    "plan_update_node_metadata": _plan_update_node_metadata,
    "plan_update_settings": _plan_update_settings,
    "plan_delete_node": _plan_delete_node,
    "plan_update_output": _plan_update_output,
    "plan_validate_structure": _plan_validate_structure,
    "plan_validate": _plan_validate,
    "plan_preview": _plan_preview,
    "plan_refute_claims": _plan_refute_claims,
    "plan_evaluate_joins": _plan_evaluate_joins,
}


def build_plan_tool_handlers() -> Dict[str, ToolHandler]:
    return dict(PLAN_TOOL_HANDLERS)

