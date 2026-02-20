"""Lightweight type hints for structure analysis (Data Island detection).

These are intentionally simple and heuristic-based.  They do NOT provide
high-confidence type inference -- they only need to distinguish "typed data"
from "header/label text" for table boundary detection.

This replaces the legacy PatternBasedTypeDetector dependency and aligns with
Palantir Foundry's approach where precise type inference is NOT done at
upload time (inferSchema=False; all columns default to STRING).
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Any, List, Sequence

from shared.models.common import DataType
from shared.models.type_inference import ColumnAnalysisResult, TypeInferenceResult

_NUMBER_RE = re.compile(r"^[+-]?\d[\d,]*\.?\d*$")
_DATE_RE = re.compile(
    r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}"  # YYYY-MM-DD or YYYY/MM/DD
    r"|^\d{1,2}[-/]\d{1,2}[-/]\d{2,4}$"  # MM-DD-YYYY or DD/MM/YYYY
)
_BOOL_LITERALS = {"true", "false", "yes", "no", "y", "n", "1", "0", "on", "off"}


@lru_cache(maxsize=20000)
def infer_single_value_type(text: str, include_complex_types: bool = False) -> str:
    """Return a rough XSD type hint for a single cell value.

    Used only for structure analysis cell scoring (Data Island detection).
    """
    stripped = text.strip()
    if not stripped:
        return DataType.STRING.value
    lower = stripped.lower()
    if lower in _BOOL_LITERALS:
        return "xsd:boolean"
    cleaned = stripped.replace(",", "")
    if _NUMBER_RE.match(cleaned):
        return "xsd:decimal" if "." in stripped else "xsd:integer"
    if _DATE_RE.match(stripped):
        return "xsd:dateTime"
    return DataType.STRING.value


def infer_column_type_hint(
    values: Sequence[Any],
    *,
    column_name: str | None = None,
    include_complex_types: bool = False,
) -> ColumnAnalysisResult:
    """Lightweight column analysis that mimics the old FunnelTypeInferenceService API.

    For structure analysis: returns a confidence score based on how consistently
    the values share a single type.  For column schema: always returns xsd:string
    (Foundry-style inferSchema=False).
    """
    non_empty = [str(v).strip() for v in values if v is not None and str(v).strip()]
    if not non_empty:
        return ColumnAnalysisResult(
            column_name=column_name or "",
            inferred_type=TypeInferenceResult(
                type=DataType.STRING.value,
                confidence=0.0,
                reason="No non-empty values",
            ),
            total_count=len(values),
            non_empty_count=0,
            sample_values=[],
        )

    types = [infer_single_value_type(v, include_complex_types) for v in non_empty]
    from collections import Counter

    counter = Counter(types)
    dominant_type, dominant_count = counter.most_common(1)[0]
    confidence = dominant_count / len(types)

    return ColumnAnalysisResult(
        column_name=column_name or "",
        inferred_type=TypeInferenceResult(
            type=dominant_type,
            confidence=round(confidence, 3),
            reason=f"{dominant_count}/{len(types)} values are {dominant_type}",
        ),
        total_count=len(values),
        non_empty_count=len(non_empty),
        sample_values=non_empty[:5],
    )


def infer_columns_for_table(
    headers: List[str],
    rows: List[List[Any]],
    *,
    include_complex_types: bool = False,
) -> List[ColumnAnalysisResult]:
    """Foundry-style: all columns default to xsd:string."""
    cols: List[List[Any]] = [[] for _ in headers]
    for row in rows:
        for i, _h in enumerate(headers):
            if i < len(row):
                cols[i].append(row[i])
    return [
        infer_column_type_hint(
            col, column_name=headers[i], include_complex_types=include_complex_types
        )
        for i, col in enumerate(cols)
    ]
