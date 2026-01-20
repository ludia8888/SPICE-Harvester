"""
Preview inspector for cleansing suggestions.

Analyzes preview samples (columns/rows/stats) to surface data quality issues
and propose safe cleansing actions.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional


_CAST_THRESHOLD = 0.9
_WHITESPACE_THRESHOLD = 0.05
_EMPTY_THRESHOLD = 0.1


def _iter_values(rows: Iterable[Dict[str, Any]], column: str) -> List[Any]:
    values: List[Any] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if column not in row:
            continue
        value = row.get(column)
        if value is None:
            continue
        if isinstance(value, str) and value == "":
            continue
        values.append(value)
    return values


def _ratio(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(count / float(total), 4)


def _is_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"true", "false"}
    return False


def _is_int(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, str):
        try:
            int(value.strip())
            return True
        except Exception:
            return False
    return False


def _is_decimal(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, float):
        return True
    if isinstance(value, str):
        try:
            float(value.strip())
            return True
        except Exception:
            return False
    return False


def _is_datetime(value: Any) -> bool:
    if isinstance(value, datetime):
        return True
    if isinstance(value, str):
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            return True
        except Exception:
            return False
    return False


def _parseability(values: List[Any]) -> Dict[str, float]:
    total = len(values)
    if total <= 0:
        return {
            "xsd:boolean": 0.0,
            "xsd:integer": 0.0,
            "xsd:decimal": 0.0,
            "xsd:dateTime": 0.0,
        }
    return {
        "xsd:boolean": _ratio(sum(1 for value in values if _is_bool(value)), total),
        "xsd:integer": _ratio(sum(1 for value in values if _is_int(value)), total),
        "xsd:decimal": _ratio(sum(1 for value in values if _is_decimal(value)), total),
        "xsd:dateTime": _ratio(sum(1 for value in values if _is_datetime(value)), total),
    }


def _column_type_map(columns: Any) -> Dict[str, str]:
    output: Dict[str, str] = {}
    if not isinstance(columns, list):
        return output
    for col in columns:
        if isinstance(col, dict):
            name = str(col.get("name") or "").strip()
            if not name:
                continue
            output[name] = str(col.get("type") or "xsd:string")
        elif isinstance(col, str):
            output[str(col)] = "xsd:string"
    return output


def inspect_preview(preview: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(preview, dict):
        return {"columns": {}, "issues": [], "suggestions": [], "needs_cleansing": False}

    rows = preview.get("rows") if isinstance(preview.get("rows"), list) else []
    columns = preview.get("columns") if isinstance(preview.get("columns"), list) else []
    column_stats = preview.get("column_stats") if isinstance(preview.get("column_stats"), dict) else {}
    cast_stats = preview.get("cast_stats") if isinstance(preview.get("cast_stats"), dict) else {}
    type_map = _column_type_map(columns)
    sample_row_count = int(column_stats.get("sample_row_count") or len(rows) or 0)

    stats_by_col = column_stats.get("columns") if isinstance(column_stats.get("columns"), dict) else {}
    issues: List[Dict[str, Any]] = []
    suggestions: List[Dict[str, Any]] = []
    report_columns: Dict[str, Dict[str, Any]] = {}

    for col_name, inferred_type in type_map.items():
        raw_stats = stats_by_col.get(col_name) if isinstance(stats_by_col, dict) else {}
        null_count = int(raw_stats.get("null_count") or 0)
        empty_count = int(raw_stats.get("empty_count") or 0)
        whitespace_count = int(raw_stats.get("whitespace_count") or 0)
        distinct_count = int(raw_stats.get("distinct_count") or 0)
        non_null = max(0, sample_row_count - null_count - empty_count)

        values = _iter_values(rows, col_name)
        parseability = _parseability(values)
        cast_failure = None
        if isinstance(cast_stats.get(col_name), dict):
            cast_failure = cast_stats[col_name].get("failure_rate")

        distinct_ratio = _ratio(distinct_count, non_null) if non_null else 0.0
        null_ratio = _ratio(null_count, sample_row_count)
        empty_ratio = _ratio(empty_count, sample_row_count)
        whitespace_ratio = _ratio(whitespace_count, sample_row_count)

        report_columns[col_name] = {
            "type": inferred_type,
            "null_ratio": null_ratio,
            "empty_ratio": empty_ratio,
            "whitespace_ratio": whitespace_ratio,
            "distinct_ratio": distinct_ratio,
            "parseability": parseability,
            "cast_failure_rate": cast_failure,
        }

        if whitespace_ratio >= _WHITESPACE_THRESHOLD or empty_ratio >= _EMPTY_THRESHOLD:
            suggestions.append(
                {
                    "column": col_name,
                    "operation": "normalize",
                    "trim": True,
                    "empty_to_null": True,
                    "whitespace_to_null": True,
                    "reason": "high whitespace/empty ratio",
                }
            )
            issues.append(
                {
                    "column": col_name,
                    "issue": "high_empty_or_whitespace",
                    "severity": "warning",
                    "evidence": {"empty_ratio": empty_ratio, "whitespace_ratio": whitespace_ratio},
                }
            )

        if inferred_type == "xsd:string":
            best_type = None
            best_score = 0.0
            for target, score in parseability.items():
                if score >= _CAST_THRESHOLD and score > best_score:
                    best_type = target
                    best_score = score
            if best_type:
                suggestions.append(
                    {
                        "column": col_name,
                        "operation": "cast",
                        "target_type": best_type,
                        "confidence": best_score,
                        "reason": "high parseability for target type",
                    }
                )

    return {
        "sample_row_count": sample_row_count,
        "column_types": type_map,
        "columns": report_columns,
        "issues": issues,
        "suggestions": suggestions,
        "needs_cleansing": bool(suggestions),
    }
