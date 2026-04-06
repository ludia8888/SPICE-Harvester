"""
Preview inspector for cleansing suggestions.

Analyzes preview samples (columns/rows/stats) to surface data quality issues
and propose safe cleansing actions.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional

from shared.services.pipeline.pipeline_math_utils import safe_ratio
from shared.services.pipeline.pipeline_type_predicates import (
    preview_is_bool as _is_bool,
    preview_is_datetime as _is_datetime,
    preview_is_decimal as _is_decimal,
    preview_is_int as _is_int,
)

_CAST_THRESHOLD = 0.9
_WHITESPACE_THRESHOLD = 0.05
_EMPTY_THRESHOLD = 0.1
_DUPLICATE_ROW_THRESHOLD = 0.1
_CASE_VARIATION_THRESHOLD = 0.15
_REGEX_CLEANSE_THRESHOLD = 0.2
_EMAIL_HINTS = ("email", "e_mail")
_PHONE_HINTS = ("phone", "mobile", "tel", "cell")
_POSTAL_HINTS = ("zip", "postal", "postcode")

# Enterprise Enhancement (2026-01): Columns that should preserve whitespace
# These patterns indicate semantic whitespace that should NOT be normalized
_PRESERVE_WHITESPACE_PATTERNS = (
    "code", "script", "json", "xml", "html", "yaml", "yml",
    "query", "sql", "expression", "formula", "template",
    "content", "body", "text", "description", "comment", "note",
    "address", "snippet", "source", "raw",
)

# Enterprise Enhancement (2026-01): Table name patterns that should NOT be deduped
# Event/log tables intentionally have duplicate-looking rows
_NO_DEDUPE_TABLE_PATTERNS = (
    "log", "event", "audit", "history", "trace", "metric",
    "transaction", "activity", "record", "journal", "change",
)


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
def _looks_like_id(name: str) -> bool:
    lowered = str(name or "").strip().lower()
    if not lowered:
        return False
    if lowered == "id":
        return True
    if lowered.endswith("_id"):
        return True
    if lowered.endswith("id") and len(lowered) <= 6:
        return True
    return False


def _domain_hint(name: str) -> Optional[str]:
    lowered = str(name or "").strip().lower()
    if not lowered:
        return None
    if any(token in lowered for token in _EMAIL_HINTS):
        return "email"
    if any(token in lowered for token in _PHONE_HINTS):
        return "phone"
    if any(token in lowered for token in _POSTAL_HINTS):
        return "postal"
    return None


def _should_preserve_whitespace(column_name: str) -> bool:
    """
    Enterprise Enhancement (2026-01):
    Detect columns that should preserve whitespace (code, JSON, text content, etc.)
    These columns should NOT receive whitespace normalization suggestions.
    """
    lowered = str(column_name or "").strip().lower()
    if not lowered:
        return False
    return any(pattern in lowered for pattern in _PRESERVE_WHITESPACE_PATTERNS)


def _is_event_or_log_table(table_name: Optional[str], column_names: List[str]) -> bool:
    """
    Enterprise Enhancement (2026-01):
    Detect event/log tables that should NOT receive dedupe suggestions.
    Event tables intentionally have repeated entries.

    Detection heuristics:
    1. Table name contains log/event/audit patterns
    2. Has timestamp + event_type columns (typical event table schema)
    """
    # Check table name
    if table_name:
        lowered = str(table_name).strip().lower()
        if any(pattern in lowered for pattern in _NO_DEDUPE_TABLE_PATTERNS):
            return True

    # Check column patterns that suggest event/log table
    lowered_cols = [str(col).lower() for col in column_names]

    # Event tables typically have: timestamp + event_type/action
    has_timestamp = any(
        "timestamp" in col or "created_at" in col or "occurred_at" in col or "event_time" in col
        for col in lowered_cols
    )
    has_event_type = any(
        "event_type" in col or "action" in col or "event_name" in col or "activity" in col
        for col in lowered_cols
    )

    if has_timestamp and has_event_type:
        return True

    return False


def _row_key(row: Dict[str, Any], columns: List[str]) -> tuple:
    return tuple(str(row.get(col)) for col in columns)


def _case_variation_ratio(values: List[Any]) -> float:
    originals = [str(v) for v in values if str(v).strip()]
    if len(originals) <= 1:
        return 0.0
    lowered = [v.lower() for v in originals]
    unique_originals = len(set(originals))
    unique_lowered = len(set(lowered))
    if unique_originals <= 0:
        return 0.0
    return round(1.0 - (unique_lowered / float(unique_originals)), 4)


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
        "xsd:boolean": safe_ratio(sum(1 for value in values if _is_bool(value)), total),
        "xsd:integer": safe_ratio(sum(1 for value in values if _is_int(value)), total),
        "xsd:decimal": safe_ratio(sum(1 for value in values if _is_decimal(value)), total),
        "xsd:dateTime": safe_ratio(sum(1 for value in values if _is_datetime(value)), total),
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


def inspect_preview(
    preview: Dict[str, Any],
    *,
    table_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Analyze preview data and suggest cleansing operations.

    Enterprise Enhancement (2026-01):
    - Added table_name parameter to detect event/log tables
    - Protects columns with semantic whitespace (code, JSON, etc.)
    - Includes parseability warning when cast may lose data
    - Suppresses dedupe suggestions for event/log tables

    Args:
        preview: Preview data with rows, columns, and stats
        table_name: Optional table name for context-aware suggestions
    """
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

        distinct_ratio = safe_ratio(distinct_count, non_null) if non_null else 0.0
        duplicate_ratio = round(max(0.0, 1.0 - distinct_ratio), 4) if non_null else 0.0
        null_ratio = safe_ratio(null_count, sample_row_count)
        empty_ratio = safe_ratio(empty_count, sample_row_count)
        whitespace_ratio = safe_ratio(whitespace_count, sample_row_count)
        case_variation = _case_variation_ratio(values) if inferred_type == "xsd:string" else 0.0

        report_columns[col_name] = {
            "type": inferred_type,
            "null_ratio": null_ratio,
            "empty_ratio": empty_ratio,
            "whitespace_ratio": whitespace_ratio,
            "distinct_ratio": distinct_ratio,
            "duplicate_ratio": duplicate_ratio,
            "parseability": parseability,
            "cast_failure_rate": cast_failure,
            "case_variation_ratio": case_variation,
        }

        if whitespace_ratio >= _WHITESPACE_THRESHOLD or empty_ratio >= _EMPTY_THRESHOLD:
            # Enterprise Enhancement: Protect columns with semantic whitespace
            if _should_preserve_whitespace(col_name):
                issues.append(
                    {
                        "column": col_name,
                        "issue": "whitespace_in_semantic_column",
                        "severity": "info",
                        "evidence": {
                            "empty_ratio": empty_ratio,
                            "whitespace_ratio": whitespace_ratio,
                            "note": (
                                "Column name suggests semantic whitespace (code/JSON/text). "
                                "Normalization suggestion suppressed to prevent data corruption."
                            ),
                        },
                    }
                )
            else:
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
                # Enterprise Enhancement: Calculate failure count and add warning
                total_values = len(values)
                failure_count = int(total_values * (1.0 - best_score))

                cast_suggestion: Dict[str, Any] = {
                    "column": col_name,
                    "operation": "cast",
                    "target_type": best_type,
                    "confidence": best_score,
                    "reason": "high parseability for target type",
                    # Enterprise: Add explicit failure information
                    "parseability_ratio": best_score,
                    "estimated_failures": failure_count,
                    "total_values_checked": total_values,
                }

                # Enterprise: Add warning if not 100% parseable
                if best_score < 1.0:
                    cast_suggestion["data_loss_warning"] = (
                        f"WARNING: {failure_count} of {total_values} values ({(1-best_score)*100:.1f}%) "
                        f"will fail to parse and become NULL. Review unparseable values before casting."
                    )
                    cast_suggestion["recommendation"] = (
                        "Consider filtering or handling unparseable values first"
                        if failure_count > 10
                        else "Low failure count; cast is likely safe"
                    )

                suggestions.append(cast_suggestion)
            if case_variation >= _CASE_VARIATION_THRESHOLD:
                suggestions.append(
                    {
                        "column": col_name,
                        "operation": "normalize",
                        "lowercase": True,
                        "reason": "case variations detected",
                    }
                )
            domain = _domain_hint(col_name)
            if domain == "phone":
                noisy = sum(1 for value in values if re.search(r"[^0-9+]", str(value)))
                if safe_ratio(noisy, len(values)) >= _REGEX_CLEANSE_THRESHOLD:
                    suggestions.append(
                        {
                            "column": col_name,
                            "operation": "regexReplace",
                            "pattern": r"[^0-9+]+",
                            "replacement": "",
                            "reason": "phone formatting characters detected",
                        }
                    )
            elif domain == "postal":
                noisy = sum(1 for value in values if re.search(r"[\s-]", str(value)))
                if safe_ratio(noisy, len(values)) >= _REGEX_CLEANSE_THRESHOLD:
                    suggestions.append(
                        {
                            "column": col_name,
                            "operation": "regexReplace",
                            "pattern": r"[\s-]+",
                            "replacement": "",
                            "reason": "postal code separators detected",
                        }
                    )
            elif domain == "email":
                noisy = sum(1 for value in values if re.search(r"\s", str(value)))
                if safe_ratio(noisy, len(values)) >= _REGEX_CLEANSE_THRESHOLD:
                    suggestions.append(
                        {
                            "column": col_name,
                            "operation": "regexReplace",
                            "pattern": r"\s+",
                            "replacement": "",
                            "reason": "whitespace detected in emails",
                        }
                    )

    if rows and columns:
        column_names: List[str] = []
        for col in columns:
            if isinstance(col, dict):
                name = str(col.get("name") or "").strip()
            else:
                name = str(col or "").strip()
            if name:
                column_names.append(name)
        if column_names:
            unique_rows = len({_row_key(row, column_names) for row in rows if isinstance(row, dict)})
            duplicate_ratio = safe_ratio(len(rows) - unique_rows, len(rows))
            if duplicate_ratio >= _DUPLICATE_ROW_THRESHOLD:
                # Enterprise Enhancement: Detect event/log tables
                is_event_table = _is_event_or_log_table(table_name, column_names)

                if is_event_table:
                    # Don't suggest dedupe for event/log tables - duplicates are intentional
                    issues.append(
                        {
                            "issue": "duplicate_rows_in_event_table",
                            "severity": "info",
                            "evidence": {
                                "duplicate_ratio": round(duplicate_ratio, 4),
                                "note": (
                                    "Table appears to be an event/log table where duplicates "
                                    "represent legitimate repeated events. Dedupe suggestion suppressed."
                                ),
                                "table_name": table_name,
                            },
                        }
                    )
                else:
                    preferred = [
                        name
                        for name in column_names
                        if _looks_like_id(name)
                        and report_columns.get(name, {}).get("distinct_ratio", 0.0) >= 0.9
                        and report_columns.get(name, {}).get("null_ratio", 1.0) <= 0.05
                    ]
                    candidates = preferred or [
                        name
                        for name in column_names
                        if report_columns.get(name, {}).get("distinct_ratio", 0.0) >= 0.9
                        and report_columns.get(name, {}).get("null_ratio", 1.0) <= 0.05
                    ]
                    suggestions.append(
                        {
                            "operation": "dedupe",
                            "columns": candidates[:3] if candidates else [],
                            "reason": "duplicate rows detected in preview",
                            "duplicate_ratio": round(duplicate_ratio, 4),
                        }
                    )
                    issues.append(
                        {
                            "issue": "duplicate_rows",
                            "severity": "warning",
                            "evidence": {"duplicate_ratio": round(duplicate_ratio, 4)},
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
