"""Spark transform engine for the pipeline worker.

This module implements transform execution as a small Strategy registry so the
`PipelineWorker` remains focused on orchestration (composition root).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

try:
    from pyspark.sql import DataFrame  # type: ignore
    from pyspark.sql import functions as F  # type: ignore
    from pyspark.sql.window import Window  # type: ignore

    _PYSPARK_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover
    # Keep the module importable in environments without pyspark.
    _PYSPARK_AVAILABLE = False
    DataFrame = Any  # type: ignore[assignment,misc]
    F = None  # type: ignore[assignment]
    Window = None  # type: ignore[assignment]

from shared.services.pipeline.pipeline_parameter_utils import apply_parameters
from shared.services.pipeline.pipeline_transform_spec import normalize_operation, normalize_union_mode, resolve_join_spec

logger = logging.getLogger(__name__)

ApplyCastsFn = Callable[[DataFrame, List[Dict[str, Any]]], DataFrame]


@dataclass(frozen=True)
class _SparkTransformContext:
    metadata: Dict[str, Any]
    inputs: List[DataFrame]
    parameters: Dict[str, Any]
    apply_casts: ApplyCastsFn


SparkTransformHandler = Callable[[_SparkTransformContext], DataFrame]


def apply_spark_transform(
    *,
    metadata: Dict[str, Any],
    inputs: List[DataFrame],
    parameters: Dict[str, Any],
    apply_casts: ApplyCastsFn,
) -> DataFrame:
    if not inputs:
        raise ValueError("Spark transform requires at least one input.")
    operation = normalize_operation(metadata.get("operation"))
    if operation == "udf":
        raise ValueError(
            "udf operation is not supported in Spark execution. Remove this transform or replace it with built-in expressions."
        )

    if not _PYSPARK_AVAILABLE or F is None:
        raise RuntimeError("pyspark is required to execute Spark transforms.")

    handler = _HANDLERS.get(operation)
    if handler is None:
        return inputs[0]
    return handler(
        _SparkTransformContext(
            metadata=metadata,
            inputs=inputs,
            parameters=parameters,
            apply_casts=apply_casts,
        )
    )


def _clean_col_name(value: Any) -> str:
    return str(value or "").strip().lstrip("\ufeff")


def _clean_expr(value: Any, parameters: Dict[str, Any]) -> str:
    return apply_parameters(str(value or ""), parameters).replace("\ufeff", "")


def _regex_inline_flags(value: Any) -> str:
    text = str(value or "")
    inline = ""
    if "i" in text:
        inline += "i"
    if "m" in text:
        inline += "m"
    if "s" in text:
        inline += "s"
    return inline


def _resolve_join_col(df: DataFrame, col_name: str) -> str:
    """Resolve join column names defensively (handles UTF-8 BOM artifacts)."""
    if col_name in df.columns:
        return col_name
    stripped = col_name.lstrip("\ufeff")
    if stripped != col_name and stripped in df.columns:
        return stripped
    bom = f"\ufeff{col_name}"
    if bom in df.columns:
        return bom
    return col_name


def _apply_join(ctx: _SparkTransformContext) -> DataFrame:
    if len(ctx.inputs) < 2:
        return ctx.inputs[0]

    join_spec = resolve_join_spec(ctx.metadata)
    join_type = join_spec.join_type
    allow_cross_join = join_spec.allow_cross_join
    left_key = join_spec.left_key
    right_key = join_spec.right_key
    left_keys = list(join_spec.left_keys or [])
    right_keys = list(join_spec.right_keys or [])
    if left_key and not left_keys:
        left_keys = [left_key]
    if right_key and not right_keys:
        right_keys = [right_key]

    left = ctx.inputs[0]
    right = ctx.inputs[1]

    hints = ctx.metadata.get("joinHints") or ctx.metadata.get("join_hints") or ctx.metadata.get("hints") or {}
    if isinstance(hints, dict):
        left_hint = hints.get("left") or hints.get("leftHint") or hints.get("left_hint")
        right_hint = hints.get("right") or hints.get("rightHint") or hints.get("right_hint")
        if left_hint:
            left = left.hint(str(left_hint))
        if right_hint:
            right = right.hint(str(right_hint))
    if ctx.metadata.get("broadcastLeft") or ctx.metadata.get("broadcast_left"):
        left = left.hint("broadcast")
    if ctx.metadata.get("broadcastRight") or ctx.metadata.get("broadcast_right"):
        right = right.hint("broadcast")

    if left_keys and right_keys:
        if len(left_keys) != len(right_keys):
            raise ValueError("Join requires leftKeys/rightKeys of the same length.")

        resolved_left_keys = [_resolve_join_col(left, key) for key in left_keys]
        resolved_right_keys = [_resolve_join_col(right, key) for key in right_keys]
        same_key_names = all(lk == rk for lk, rk in zip(resolved_left_keys, resolved_right_keys))

        # Deterministically avoid duplicate column names in join output by renaming
        # colliding right-side columns. This matches preflight's `right_` prefixing
        # logic, so plans can rely on stable column names without Spark ambiguity.
        left_set = set(left.columns)
        right_df = right
        right_col_map: Dict[str, str] = {}
        right_cols: set[str] = set(right.columns)
        join_key_keep = set(resolved_right_keys) if same_key_names else set()
        for col in list(right.columns):
            if col in left_set and col not in join_key_keep:
                new = f"right_{col}"
                if new in right_cols or new in left_set:
                    base = new
                    idx = 1
                    while f"{base}__{idx}" in right_cols or f"{base}__{idx}" in left_set:
                        idx += 1
                    new = f"{base}__{idx}"
                right_df = right_df.withColumnRenamed(col, new)
                right_col_map[col] = new
                right_cols.discard(col)
                right_cols.add(new)

        mapped_right_keys = [right_col_map.get(rk, rk) for rk in resolved_right_keys]

        if same_key_names:
            return left.join(right_df, on=resolved_left_keys, how=join_type)

        conditions = [left[lk] == right_df[rk] for lk, rk in zip(resolved_left_keys, mapped_right_keys)]
        join_expr = conditions[0]
        for cond in conditions[1:]:
            join_expr = join_expr & cond
        return left.join(right_df, join_expr, how=join_type)

    if allow_cross_join:
        if join_type != "cross":
            logger.warning("allowCrossJoin enabled but joinType=%s; forcing cross join", join_type)
        try:
            return left.crossJoin(right)
        except Exception:
            return left.join(right, how="cross")

    raise ValueError(
        "Join requires leftKey/rightKey or leftKeys/rightKeys (or joinKey). Cross join requires allowCrossJoin=true."
    )


def _apply_filter(ctx: _SparkTransformContext) -> DataFrame:
    expr = _clean_expr(ctx.metadata.get("expression"), ctx.parameters)
    if expr:
        return ctx.inputs[0].filter(expr)
    return ctx.inputs[0]


def _apply_compute(ctx: _SparkTransformContext) -> DataFrame:
    df = ctx.inputs[0]

    assignments = ctx.metadata.get("assignments") or ctx.metadata.get("computedColumns") or ctx.metadata.get("computed_columns")
    if isinstance(assignments, list) and assignments:
        out = df
        for item in assignments:
            if not isinstance(item, dict):
                continue
            target = _clean_col_name(item.get("column") or item.get("target") or item.get("name"))
            formula = str(item.get("expression") or item.get("expr") or item.get("formula") or "").strip()
            if not target or not formula:
                continue
            formula_text = _clean_expr(formula, ctx.parameters)
            if not formula_text:
                continue
            out = out.withColumn(target, F.expr(formula_text))
        return out

    target = (
        ctx.metadata.get("targetColumn")
        or ctx.metadata.get("target_column")
        or ctx.metadata.get("target")
        or ctx.metadata.get("column")
    )
    formula = ctx.metadata.get("formula") or ctx.metadata.get("expr")
    if target and formula:
        target_text = _clean_col_name(target)
        formula_text = _clean_expr(formula, ctx.parameters).strip()
        if target_text and formula_text:
            return df.withColumn(target_text, F.expr(formula_text))

    expr = _clean_expr(ctx.metadata.get("expression"), ctx.parameters).strip()
    if not expr:
        return df
    if "=" in expr:
        target_candidate, formula_candidate = [part.strip() for part in expr.split("=", 1)]
        target_clean = target_candidate.strip().lstrip("\ufeff")
        if target_clean.startswith("`") and target_clean.endswith("`") and len(target_clean) > 1:
            target_clean = target_clean[1:-1].replace("``", "`")
        if target_clean and target_clean not in df.columns:
            return df.withColumn(target_clean, F.expr(formula_candidate.replace("\ufeff", "")))
    return df.withColumn("computed", F.expr(expr))


def _apply_normalize(ctx: _SparkTransformContext) -> DataFrame:
    columns = ctx.metadata.get("columns") or []
    if not isinstance(columns, list) or not columns:
        return ctx.inputs[0]

    df = ctx.inputs[0]
    selected = [_clean_col_name(col) for col in columns if str(col or "").strip()]
    if not selected:
        return df

    trim = bool(ctx.metadata.get("trim", True))
    empty_to_null = bool(ctx.metadata.get("emptyToNull", ctx.metadata.get("empty_to_null", True)))
    whitespace_to_null = bool(ctx.metadata.get("whitespaceToNull", ctx.metadata.get("whitespace_to_null", True)))
    lowercase = bool(ctx.metadata.get("lowercase", False))
    uppercase = bool(ctx.metadata.get("uppercase", False))

    dtypes = dict(df.dtypes)
    for col_name in selected:
        if dtypes.get(col_name) != "string":
            continue
        expr = F.col(col_name)
        if trim:
            expr = F.trim(expr)
        if lowercase:
            expr = F.lower(expr)
        if uppercase:
            expr = F.upper(expr)
        if whitespace_to_null:
            expr = F.when(F.trim(expr) == "", F.lit(None)).otherwise(expr)
        if empty_to_null:
            expr = F.when(expr == "", F.lit(None)).otherwise(expr)
        df = df.withColumn(col_name, expr)
    return df


def _apply_explode(ctx: _SparkTransformContext) -> DataFrame:
    columns = ctx.metadata.get("columns") or []
    if not columns:
        return ctx.inputs[0]
    column = _clean_col_name(columns[0])
    if not column:
        return ctx.inputs[0]
    return ctx.inputs[0].withColumn(column, F.explode(F.col(column)))


def _apply_select(ctx: _SparkTransformContext) -> DataFrame:
    expressions = ctx.metadata.get("expressions") or ctx.metadata.get("selectExpr") or ctx.metadata.get("select_expr")
    if isinstance(expressions, list) and expressions:
        resolved = [_clean_expr(item, ctx.parameters) for item in expressions if str(item or "").strip()]
        if resolved:
            return ctx.inputs[0].selectExpr(*resolved)
    columns = ctx.metadata.get("columns") or []
    if columns:
        cleaned = [_clean_col_name(col) for col in columns if str(col or "").strip()]
        return ctx.inputs[0].select(*[F.col(col) for col in cleaned])
    return ctx.inputs[0]


def _apply_drop(ctx: _SparkTransformContext) -> DataFrame:
    columns = ctx.metadata.get("columns") or []
    if not columns:
        return ctx.inputs[0]
    cleaned = [_clean_col_name(col) for col in columns if str(col or "").strip()]
    if not cleaned:
        return ctx.inputs[0]
    return ctx.inputs[0].drop(*cleaned)


def _apply_rename(ctx: _SparkTransformContext) -> DataFrame:
    rename_map = ctx.metadata.get("rename") or {}
    if not isinstance(rename_map, dict) or not rename_map:
        return ctx.inputs[0]

    df = ctx.inputs[0]
    existing_set = set(df.columns)
    for raw_src, raw_dst in rename_map.items():
        src = str(raw_src)
        dst = str(raw_dst).lstrip("\ufeff")

        if src not in existing_set:
            stripped = src.lstrip("\ufeff")
            if stripped != src and stripped in existing_set:
                src = stripped
        if src not in existing_set:
            continue
        if src == dst:
            continue

        if dst in existing_set:
            if str(raw_src).startswith("\ufeff") and dst == str(raw_src).lstrip("\ufeff"):
                df = df.drop(src)
                existing_set.remove(src)
                continue

            base = dst
            suffix = 1
            while dst in existing_set:
                dst = f"{base}__{suffix}"
                suffix += 1

        df = df.withColumnRenamed(src, dst)
        existing_set.remove(src)
        existing_set.add(dst)

    return df


def _apply_cast(ctx: _SparkTransformContext) -> DataFrame:
    casts = ctx.metadata.get("casts") or []
    if not casts:
        return ctx.inputs[0]
    return ctx.apply_casts(ctx.inputs[0], casts)


def _apply_regex_replace(ctx: _SparkTransformContext) -> DataFrame:
    df = ctx.inputs[0]

    rules = ctx.metadata.get("rules") or []
    if rules:
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            column = _clean_col_name(rule.get("column"))
            pattern = str(rule.get("pattern") or "").strip()
            if not column or not pattern:
                continue
            inline = _regex_inline_flags(rule.get("flags"))
            if inline:
                pattern = f"(?{inline}){pattern}"
            replacement = str(rule.get("replacement") or "")
            df = df.withColumn(column, F.regexp_replace(F.col(column), pattern, replacement))
        return df

    pattern = str(ctx.metadata.get("pattern") or "").strip()
    columns = ctx.metadata.get("columns") or []
    if pattern and columns:
        inline = _regex_inline_flags(ctx.metadata.get("flags"))
        if inline:
            pattern = f"(?{inline}){pattern}"
        replacement = str(ctx.metadata.get("replacement") or "")
        for col in columns:
            col_name = _clean_col_name(col)
            if not col_name:
                continue
            df = df.withColumn(col_name, F.regexp_replace(F.col(col_name), pattern, replacement))
        return df

    return df


def _apply_dedupe(ctx: _SparkTransformContext) -> DataFrame:
    subset = ctx.metadata.get("columns") or []
    if subset:
        cleaned = [_clean_col_name(col) for col in subset if str(col or "").strip()]
        return ctx.inputs[0].dropDuplicates(cleaned)
    return ctx.inputs[0].dropDuplicates()


def _parse_sort_specs(items: Any) -> List[Any]:
    specs: list[Any] = []
    if not isinstance(items, list):
        return specs
    for item in items:
        if isinstance(item, str):
            col = item.strip()
            if not col:
                continue
            direction = "asc"
            if col.startswith("-"):
                direction = "desc"
                col = col[1:].strip()
            col = col.lstrip("\ufeff")
            if not col:
                continue
            specs.append(F.col(col).desc() if direction == "desc" else F.col(col))
            continue
        if isinstance(item, dict):
            col = _clean_col_name(item.get("column") or item.get("name"))
            if not col:
                continue
            direction = str(item.get("direction") or item.get("dir") or "asc").strip().lower()
            if direction not in {"asc", "desc"}:
                direction = "asc"
            specs.append(F.col(col).desc() if direction == "desc" else F.col(col))
            continue
    return specs


def _apply_sort(ctx: _SparkTransformContext) -> DataFrame:
    columns = ctx.metadata.get("columns") or []
    specs = _parse_sort_specs(columns)
    if specs:
        return ctx.inputs[0].sort(*specs)
    return ctx.inputs[0]


def _apply_union(ctx: _SparkTransformContext) -> DataFrame:
    if len(ctx.inputs) < 2:
        return ctx.inputs[0]

    union_mode = normalize_union_mode(ctx.metadata)
    left = ctx.inputs[0]
    right = ctx.inputs[1]
    left_cols = list(left.columns)
    right_cols = list(right.columns)
    left_set = set(left_cols)
    right_set = set(right_cols)
    if union_mode == "strict":
        if left_set != right_set:
            missing_left = sorted(right_set - left_set)
            missing_right = sorted(left_set - right_set)
            raise ValueError(
                "union schema mismatch (strict): "
                f"missing_in_left={missing_left} missing_in_right={missing_right}"
            )
        return left.unionByName(right)
    if union_mode == "common_only":
        common = [col for col in left_cols if col in right_set]
        if not common:
            raise ValueError("union has no common columns")
        return left.select(*common).unionByName(right.select(*common))
    if union_mode in {"pad_missing_nulls", "pad"}:
        all_cols = left_cols + [col for col in right_cols if col not in left_set]

        def align(df: DataFrame, present: set[str]) -> DataFrame:
            return df.select(*[(F.col(col) if col in present else F.lit(None).alias(col)) for col in all_cols])

        return align(left, left_set).unionByName(align(right, right_set))
    raise ValueError(f"Invalid unionMode: {union_mode}")


def _apply_group_by(ctx: _SparkTransformContext) -> DataFrame:
    group_by_raw = ctx.metadata.get("groupBy") or []
    group_by = [
        _clean_col_name(col)
        for col in (group_by_raw if isinstance(group_by_raw, list) else [])
        if str(col or "").strip()
    ]
    group_cols = [F.col(col) for col in group_by] if group_by else []

    agg_exprs: list[Any] = []
    expr_items = ctx.metadata.get("aggregateExpressions") or ctx.metadata.get("aggExpressions") or ctx.metadata.get("aggregate_expressions")
    if isinstance(expr_items, list) and expr_items:
        for item in expr_items:
            if isinstance(item, str):
                expr_text = _clean_expr(item, ctx.parameters).strip()
                if not expr_text:
                    continue
                alias: Optional[str] = None
                lower = expr_text.lower()
                if " as " in lower:
                    parts = lower.rsplit(" as ", 1)
                    if len(parts) == 2 and parts[1].strip():
                        alias = expr_text[len(parts[0]) + 4 :].strip()
                        expr_text = expr_text[: len(parts[0])].strip()
                col_expr = F.expr(expr_text)
                if alias:
                    col_expr = col_expr.alias(alias)
                agg_exprs.append(col_expr)
                continue
            if not isinstance(item, dict):
                continue
            expr_text = str(item.get("expr") or item.get("expression") or "").strip()
            if not expr_text:
                continue
            expr_text = _clean_expr(expr_text, ctx.parameters)
            alias = str(item.get("alias") or item.get("as") or "").strip() or None
            col_expr = F.expr(expr_text)
            if alias:
                col_expr = col_expr.alias(alias)
            agg_exprs.append(col_expr)

    if not agg_exprs:
        aggregates = ctx.metadata.get("aggregates") or []
        for agg in aggregates:
            if not isinstance(agg, dict):
                continue
            col_name = _clean_col_name(agg.get("column"))
            op = str(agg.get("op") or "").lower()
            alias = agg.get("alias") or f"{op}_{col_name}"
            if not col_name or not op:
                continue
            base_col = F.col(col_name)
            if op == "count":
                agg_exprs.append(F.count(base_col).alias(alias))
            elif op == "sum":
                agg_exprs.append(F.sum(base_col).alias(alias))
            elif op == "avg":
                agg_exprs.append(F.avg(base_col).alias(alias))
            elif op == "min":
                agg_exprs.append(F.min(base_col).alias(alias))
            elif op == "max":
                agg_exprs.append(F.max(base_col).alias(alias))

    if group_cols and agg_exprs:
        return ctx.inputs[0].groupBy(*group_cols).agg(*agg_exprs)
    if agg_exprs:
        return ctx.inputs[0].agg(*agg_exprs)
    return ctx.inputs[0]


def _apply_pivot(ctx: _SparkTransformContext) -> DataFrame:
    pivot_meta = ctx.metadata.get("pivot") or {}
    index_cols_raw = pivot_meta.get("index") or []
    index_cols = [
        _clean_col_name(col)
        for col in (index_cols_raw if isinstance(index_cols_raw, list) else [])
        if str(col or "").strip()
    ]
    columns_col = _clean_col_name(pivot_meta.get("columns")) or None
    values_col = _clean_col_name(pivot_meta.get("values")) or None
    agg = str(pivot_meta.get("agg") or "sum").lower()
    if index_cols and columns_col and values_col:
        base = ctx.inputs[0].groupBy(*[F.col(c) for c in index_cols]).pivot(columns_col)
        if agg == "count":
            return base.count()
        if agg == "avg":
            return base.avg(values_col)
        if agg == "min":
            return base.min(values_col)
        if agg == "max":
            return base.max(values_col)
        return base.sum(values_col)
    return ctx.inputs[0]


def _apply_window(ctx: _SparkTransformContext) -> DataFrame:
    window_meta = ctx.metadata.get("window") or {}
    expressions = (
        window_meta.get("expressions")
        if isinstance(window_meta.get("expressions"), list)
        else ctx.metadata.get("expressions")
    )
    if isinstance(expressions, list) and expressions:
        df = ctx.inputs[0]
        for item in expressions:
            if not isinstance(item, dict):
                continue
            name = _clean_col_name(item.get("column") or item.get("name"))
            expr_text = str(item.get("expr") or item.get("expression") or "").strip()
            if not name or not expr_text:
                continue
            expr_text = _clean_expr(expr_text, ctx.parameters)
            df = df.withColumn(name, F.expr(expr_text))
        return df

    partition_by_raw = window_meta.get("partitionBy") or []
    partition_by = [
        _clean_col_name(col)
        for col in (partition_by_raw if isinstance(partition_by_raw, list) else [])
        if str(col or "").strip()
    ]
    order_by = window_meta.get("orderBy") or []
    output_column = _clean_col_name(window_meta.get("outputColumn") or "row_number") or "row_number"
    specs = _parse_sort_specs(order_by)
    if not specs:
        return ctx.inputs[0]
    if partition_by:
        window_spec = Window.partitionBy(*partition_by).orderBy(*specs)
    else:
        window_spec = Window.orderBy(*specs)
    return ctx.inputs[0].withColumn(output_column, F.row_number().over(window_spec))


_HANDLERS: Dict[str, SparkTransformHandler] = {
    "join": _apply_join,
    "filter": _apply_filter,
    "compute": _apply_compute,
    "normalize": _apply_normalize,
    "explode": _apply_explode,
    "select": _apply_select,
    "drop": _apply_drop,
    "rename": _apply_rename,
    "cast": _apply_cast,
    "regexReplace": _apply_regex_replace,
    "dedupe": _apply_dedupe,
    "sort": _apply_sort,
    "union": _apply_union,
    "groupBy": _apply_group_by,
    "aggregate": _apply_group_by,
    "pivot": _apply_pivot,
    "window": _apply_window,
}

