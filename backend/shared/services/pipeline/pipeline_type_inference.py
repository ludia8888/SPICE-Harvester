from __future__ import annotations

"""
Deterministic type inference helpers.

These utilities are intentionally sample-based and side-effect free so they can be used by:
- MCP analysis tools (context pack type hints)
- planners (to decide minimal casts for joins)
- unit tests
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from shared.services.pipeline.pipeline_schema_utils import normalize_schema_type
from shared.services.pipeline.pipeline_type_utils import (
    parse_datetime_text,
    parse_decimal_text,
    parse_int_text,
)


def _is_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"true", "false"}
    return False


def _is_datetime(value: Any) -> bool:
    if isinstance(value, datetime):
        return True
    if isinstance(value, str):
        return parse_datetime_text(value, allow_ambiguous=False) is not None
    return False


def _is_int(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, str):
        return parse_int_text(value) is not None
    return False


def _is_decimal(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, float):
        return True
    if isinstance(value, int):
        return True
    if isinstance(value, str):
        return parse_decimal_text(value) is not None
    return False


def _sample_non_null(values: Iterable[Any], *, max_samples: int) -> List[Any]:
    resolved_max = max(0, int(max_samples))
    sample: List[Any] = []
    for value in values:
        if value in (None, ""):
            continue
        if isinstance(value, str) and not value.strip():
            continue
        sample.append(value)
        if resolved_max and len(sample) >= resolved_max:
            break
    return sample


@dataclass(frozen=True)
class TypeInferenceResult:
    suggested_type: str
    confidence: float
    sample_size: int
    ratios: Dict[str, float]


def infer_xsd_type_with_confidence(values: Iterable[Any], *, max_samples: int = 80) -> TypeInferenceResult:
    """
    Best-effort inference with a confidence score based on parse rates.

    Notes:
    - This is intentionally permissive (ratio-based), unlike infer_xsd_type_from_values (all-or-nothing).
    - suggested_type is always a normalized xsd:* string.
    """
    sample = _sample_non_null(values, max_samples=max_samples)
    if not sample:
        return TypeInferenceResult(
            suggested_type="xsd:string",
            confidence=0.0,
            sample_size=0,
            ratios={"bool": 0.0, "int": 0.0, "decimal": 0.0, "datetime": 0.0},
        )

    total = float(len(sample))
    bool_ratio = sum(1 for v in sample if _is_bool(v)) / total
    int_ratio = sum(1 for v in sample if _is_int(v)) / total
    decimal_ratio = sum(1 for v in sample if _is_decimal(v)) / total
    datetime_ratio = sum(1 for v in sample if _is_datetime(v)) / total
    ratios = {
        "bool": round(bool_ratio, 3),
        "int": round(int_ratio, 3),
        "decimal": round(decimal_ratio, 3),
        "datetime": round(datetime_ratio, 3),
    }

    # Choose the most specific type that is "very likely".
    threshold = 0.95
    if bool_ratio >= threshold:
        return TypeInferenceResult("xsd:boolean", round(bool_ratio, 3), len(sample), ratios)
    if int_ratio >= threshold:
        return TypeInferenceResult("xsd:integer", round(int_ratio, 3), len(sample), ratios)
    if decimal_ratio >= threshold:
        return TypeInferenceResult("xsd:decimal", round(decimal_ratio, 3), len(sample), ratios)
    if datetime_ratio >= threshold:
        return TypeInferenceResult("xsd:dateTime", round(datetime_ratio, 3), len(sample), ratios)

    # Otherwise, treat as string and expose the strongest ratio as a weak signal.
    best = max(bool_ratio, int_ratio, decimal_ratio, datetime_ratio)
    return TypeInferenceResult("xsd:string", round(best, 3), len(sample), ratios)


def common_join_key_type(left: Any, right: Any) -> str:
    """
    Pick a safe common type for join keys.

    We bias to xsd:string when types disagree because it preserves information and avoids precision loss.
    """
    left_norm = normalize_schema_type(left)
    right_norm = normalize_schema_type(right)
    if left_norm == right_norm:
        return left_norm
    if {left_norm, right_norm}.issubset({"xsd:integer", "xsd:decimal"}):
        return "xsd:string"
    return "xsd:string"


def normalize_declared_type(value: Any) -> str:
    normalized = normalize_schema_type(value)
    return normalized or "xsd:string"

