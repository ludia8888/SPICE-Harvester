"""
Pipeline Profiler - lightweight column statistics for Pipeline Builder previews.

This module is intentionally sample-based (it profiles the preview sample rows),
so it can run without additional Spark actions and without reading full datasets.
"""

from __future__ import annotations

import json
from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


def _safe_stringify(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    try:
        return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)
    except Exception:
        return str(value)


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return float(raw)
        except Exception:
            return None
    return None


def _compute_histogram(values: Sequence[float], *, bins: int = 10) -> List[Dict[str, Any]]:
    if not values:
        return []
    resolved_bins = max(1, int(bins))
    if resolved_bins > len(values):
        resolved_bins = len(values)

    minimum = float(min(values))
    maximum = float(max(values))
    if minimum == maximum:
        return [{"start": minimum, "end": maximum, "count": int(len(values))}]

    width = (maximum - minimum) / resolved_bins
    if width <= 0:
        return [{"start": minimum, "end": maximum, "count": int(len(values))}]

    counts = [0 for _ in range(resolved_bins)]
    for value in values:
        idx = int((float(value) - minimum) / width)
        if idx >= resolved_bins:
            idx = resolved_bins - 1
        if idx < 0:
            idx = 0
        counts[idx] += 1

    histogram: List[Dict[str, Any]] = []
    for i in range(resolved_bins):
        start = minimum + width * i
        end = minimum + width * (i + 1)
        histogram.append({"start": float(start), "end": float(end), "count": int(counts[i])})
    return histogram


def _normalize_columns(columns: Any) -> List[Dict[str, Any]]:
    if isinstance(columns, list):
        output: List[Dict[str, Any]] = []
        for col in columns:
            if isinstance(col, str) and col.strip():
                output.append({"name": col.strip()})
            elif isinstance(col, dict):
                name = str(col.get("name") or col.get("key") or "").strip()
                if name:
                    output.append(dict(col))
        return output
    return []


def compute_column_stats(
    *,
    rows: Sequence[Dict[str, Any]],
    columns: Any,
    max_top_values: int = 10,
) -> Dict[str, Any]:
    """
    Returns a dict payload that is safe to JSON serialize:
    {
      "sample_row_count": <int>,
      "columns": {
        "<col>": {
           "type": "...",
           "null_count": <int>,
           "empty_count": <int>,
           "whitespace_count": <int>,
           "distinct_count": <int>,
           "top_values": [{"value": "...", "count": <int>}],
           "numeric": {"min": <float>, "max": <float>, "mean": <float>}  # if applicable
        }
      }
    }
    """
    column_defs = _normalize_columns(columns)
    column_names = [str(col.get("name") or col.get("key") or "").strip() for col in column_defs]
    column_names = [name for name in column_names if name]

    stats: Dict[str, Any] = {"sample_row_count": len(rows), "columns": {}}
    if not column_names:
        return stats

    for col in column_defs:
        name = str(col.get("name") or col.get("key") or "").strip()
        if not name:
            continue

        type_hint = col.get("type")

        values = [row.get(name) for row in rows]
        null_count = sum(1 for value in values if value is None)
        empty_count = sum(1 for value in values if isinstance(value, str) and value == "")
        whitespace_count = sum(
            1
            for value in values
            if isinstance(value, str) and value != "" and value.strip() == ""
        )

        normalized_non_null = [
            _safe_stringify(value)
            for value in values
            if value is not None and not (isinstance(value, str) and value == "")
        ]
        distinct_count = len(set(normalized_non_null))

        top_counter = Counter(normalized_non_null)
        top_values = [
            {"value": value, "count": int(count)}
            for value, count in top_counter.most_common(max(0, int(max_top_values)))
        ]

        numeric_values = [_coerce_float(value) for value in values]
        numeric_values = [value for value in numeric_values if value is not None]
        numeric_payload = None
        if numeric_values:
            numeric_payload = {
                "min": float(min(numeric_values)),
                "max": float(max(numeric_values)),
                "mean": float(sum(numeric_values) / len(numeric_values)),
                "histogram": _compute_histogram(numeric_values, bins=10),
            }

        payload: Dict[str, Any] = {
            "type": type_hint,
            "null_count": int(null_count),
            "empty_count": int(empty_count),
            "whitespace_count": int(whitespace_count),
            "distinct_count": int(distinct_count),
            "top_values": top_values,
        }
        if numeric_payload:
            payload["numeric"] = numeric_payload

        stats["columns"][name] = payload

    return stats
