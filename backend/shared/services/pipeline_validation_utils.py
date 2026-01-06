from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from shared.services.pipeline_definition_utils import split_expectation_columns
from shared.services.pipeline_schema_utils import (
    normalize_expectations,
    normalize_number,
    normalize_schema_checks,
    normalize_schema_contract,
    normalize_schema_type,
    normalize_value_list,
)


@dataclass(frozen=True)
class TableOps:
    columns: set[str]
    type_map: Dict[str, str]
    total_count: Callable[[], int]
    has_null: Callable[[str], bool]
    has_empty: Callable[[str], bool]
    unique_count: Callable[[Sequence[str]], int]
    min_max: Callable[[str], Tuple[Optional[float], Optional[float]]]
    regex_mismatch: Callable[[str, str], bool]
    in_set_mismatch: Callable[[str, Sequence[Any]], bool]


def _format_error(prefix: str, message: str) -> str:
    return f"{prefix}{message}" if prefix else message


def validate_schema_checks(
    ops: TableOps,
    checks: Any,
    *,
    error_prefix: str = "",
) -> List[str]:
    specs = normalize_schema_checks(checks)
    if not specs:
        return []
    errors: List[str] = []

    for check in specs:
        rule = check.rule
        column = check.column
        value = check.value
        if rule in {"required", "exists"}:
            if column not in ops.columns:
                errors.append(_format_error(error_prefix, f"missing column {column}"))
        if rule in {"type", "dtype"}:
            expected = normalize_schema_type(value)
            if column not in ops.columns:
                errors.append(_format_error(error_prefix, f"missing column {column}"))
            elif expected:
                actual = normalize_schema_type(ops.type_map.get(column))
                if actual and actual != expected:
                    errors.append(_format_error(error_prefix, f"{column} type {actual} != {expected}"))
        if rule in {"not_null", "non_null"}:
            if column not in ops.columns:
                errors.append(_format_error(error_prefix, f"missing column {column}"))
            elif ops.has_null(column):
                errors.append(_format_error(error_prefix, f"{column} has nulls"))
        if rule == "min":
            threshold = normalize_number(value)
            if threshold is not None:
                min_value, _ = ops.min_max(column)
                if min_value is not None and min_value < threshold:
                    errors.append(_format_error(error_prefix, f"{column} min < {threshold}"))
        if rule == "max":
            threshold = normalize_number(value)
            if threshold is not None:
                _, max_value = ops.min_max(column)
                if max_value is not None and max_value > threshold:
                    errors.append(_format_error(error_prefix, f"{column} max > {threshold}"))
        if rule == "regex":
            pattern = str(value or "").strip()
            if pattern:
                try:
                    re.compile(pattern)
                except re.error:
                    errors.append(_format_error(error_prefix, f"{column} regex invalid"))
                    continue
                if ops.regex_mismatch(column, pattern):
                    errors.append(_format_error(error_prefix, f"{column} regex mismatch"))
    return errors


def validate_expectations(
    ops: TableOps,
    expectations: Iterable[Dict[str, Any]],
) -> List[str]:
    errors: List[str] = []
    specs = normalize_expectations(list(expectations))
    if not specs:
        return errors

    total_count: Optional[int] = None

    def get_total() -> int:
        nonlocal total_count
        if total_count is None:
            total_count = ops.total_count()
        return total_count

    for exp in specs:
        rule = exp.rule
        columns = split_expectation_columns(exp.column)
        value = exp.value
        if rule == "row_count_min" and value is not None:
            if get_total() < int(value):
                errors.append(f"row_count_min failed: {value}")
        if rule == "row_count_max" and value is not None:
            if get_total() > int(value):
                errors.append(f"row_count_max failed: {value}")
        if rule in {"not_null", "non_null"} and columns:
            for column in columns:
                if ops.has_null(column):
                    errors.append(f"not_null failed: {column}")
        if rule == "non_empty" and len(columns) == 1:
            column = columns[0]
            if ops.has_empty(column):
                errors.append(f"non_empty failed: {column}")
        if rule == "unique" and columns:
            if ops.unique_count(columns) != get_total():
                errors.append(f"unique failed: {','.join(columns)}")
        if rule in {"min", "max"} and len(columns) == 1:
            column = columns[0]
            threshold = normalize_number(value)
            if threshold is None:
                continue
            min_value, max_value = ops.min_max(column)
            if rule == "min" and min_value is not None and min_value < threshold:
                errors.append(f"min failed: {column} < {threshold}")
            if rule == "max" and max_value is not None and max_value > threshold:
                errors.append(f"max failed: {column} > {threshold}")
        if rule == "regex" and len(columns) == 1 and value:
            column = columns[0]
            pattern = str(value)
            try:
                re.compile(pattern)
            except re.error:
                errors.append(f"regex invalid: {pattern}")
                continue
            if ops.regex_mismatch(column, pattern):
                errors.append(f"regex failed: {column}")
        if rule == "in_set" and len(columns) == 1:
            column = columns[0]
            allowed = normalize_value_list(value)
            if allowed:
                if ops.in_set_mismatch(column, allowed):
                    errors.append(f"in_set failed: {column}")
    return errors


def validate_schema_contract(ops: TableOps, contract: Any) -> List[str]:
    specs = normalize_schema_contract(contract)
    if not specs:
        return []
    errors: List[str] = []
    for item in specs:
        column = item.column
        if column not in ops.columns:
            if item.required:
                errors.append(f"schema contract missing column: {column}")
            continue
        if item.expected_type:
            actual = normalize_schema_type(ops.type_map.get(column))
            if actual and actual != item.expected_type:
                errors.append(
                    f"schema contract type mismatch: {column} {actual} != {item.expected_type}"
                )
    return errors
