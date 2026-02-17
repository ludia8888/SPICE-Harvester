from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from shared.errors.external_codes import ExternalErrorCode
from shared.services.pipeline.pipeline_type_utils import (
    parse_datetime_text_with_ambiguity,
    parse_decimal_text,
    parse_int_text,
)

_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

# Rough KR + intl-ish phone patterns (fallback-only; Funnel remains source of truth).
_PHONE_PATTERNS: Tuple[re.Pattern[str], ...] = (
    # KR mobile: 010-1234-5678 / 01012345678
    re.compile(r"^(?:\+?82[- ]?)?(?:0)?1[016789][- ]?\d{3,4}[- ]?\d{4}$"),
    # KR landline: 02-123-4567 / 031-123-4567 (len varies)
    re.compile(r"^(?:\+?82[- ]?)?(?:0)?\d{1,2}[- ]?\d{3,4}[- ]?\d{4}$"),
    # US-ish: (415) 555-2671 / 415-555-2671 / +1 415 555 2671
    re.compile(r"^(?:\+?1[- ]?)?\(?\d{3}\)?[- ]?\d{3}[- ]?\d{4}$"),
)

_BOOL_LITERALS = {
    "true",
    "false",
    "yes",
    "no",
    "y",
    "n",
    "1",
    "0",
    "on",
    "off",
    # Korean
    "참",
    "거짓",
    "예",
    "아니오",
}


def _is_non_empty(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _sample_non_empty(values: Iterable[Any], *, max_samples: int = 200) -> List[Any]:
    resolved_max = max(0, int(max_samples))
    output: List[Any] = []
    for value in values:
        if not _is_non_empty(value):
            continue
        output.append(value)
        if resolved_max and len(output) >= resolved_max:
            break
    return output


def _phone_ratio(values: Sequence[str]) -> float:
    if not values:
        return 0.0
    matched = 0
    for v in values:
        s = v.strip()
        if not s:
            continue
        if any(p.match(s) for p in _PHONE_PATTERNS):
            matched += 1
    return matched / float(len(values))


def _email_ratio(values: Sequence[str]) -> float:
    if not values:
        return 0.0
    matched = 0
    for v in values:
        s = v.strip()
        if not s:
            continue
        if _EMAIL_RE.match(s):
            matched += 1
    return matched / float(len(values))


def _bool_ratio(values: Sequence[Any]) -> float:
    if not values:
        return 0.0
    matched = 0
    for v in values:
        if isinstance(v, bool):
            matched += 1
            continue
        if isinstance(v, str) and v.strip().lower() in _BOOL_LITERALS:
            matched += 1
    return matched / float(len(values))


def _int_ratio(values: Sequence[Any]) -> float:
    if not values:
        return 0.0
    matched = 0
    for v in values:
        if isinstance(v, bool):
            continue
        if isinstance(v, int):
            matched += 1
            continue
        if isinstance(v, str) and parse_int_text(v) is not None:
            matched += 1
    return matched / float(len(values))


def _decimal_ratio(values: Sequence[Any]) -> float:
    if not values:
        return 0.0
    matched = 0
    for v in values:
        if isinstance(v, bool):
            continue
        if isinstance(v, (int, float)):
            matched += 1
            continue
        if isinstance(v, str) and parse_decimal_text(v) is not None:
            matched += 1
    return matched / float(len(values))


def _date_datetime_ratios(values: Sequence[Any]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    date_like = 0
    datetime_like = 0
    total = 0
    for v in values:
        if isinstance(v, datetime):
            total += 1
            if v.hour or v.minute or v.second or v.microsecond:
                datetime_like += 1
            else:
                date_like += 1
            continue
        if not isinstance(v, str):
            continue
        text = v.strip()
        if not text:
            continue
        parsed = parse_datetime_text_with_ambiguity(text, allow_ambiguous=True)
        if not parsed:
            continue
        total += 1
        format_used = str(parsed.format_used or "")
        has_time = any(token in format_used for token in ("%H", "%M", "%S"))
        if not has_time:
            # ISO-8601 might hide format; look at raw as a fallback.
            has_time = bool(re.search(r"[T\\s]\\d{1,2}:\\d{2}", text))
        if has_time:
            datetime_like += 1
        else:
            date_like += 1
    if total <= 0:
        return 0.0, 0.0
    return date_like / float(total), datetime_like / float(total)


def infer_type_fallback(
    values: Sequence[Any],
    *,
    column_name: Optional[str] = None,
    include_complex_types: bool = True,
) -> Dict[str, Any]:
    """
    Deterministic, dependency-free(ish) inference used when Funnel is unavailable.

    Returns a Funnel-compatible TypeInferenceResult dict:
      { "type": "...", "confidence": 0..1, "reason": "...", "metadata": {...} }
    """
    sample = _sample_non_empty(values, max_samples=120)
    if not sample:
        return {
            "type": "xsd:string",
            "confidence": 0.0,
            "reason": "No non-empty samples available; defaulting to string",
            "metadata": {"fallback": True},
        }

    # Convert to strings once for pattern checks.
    str_sample = [str(v).strip() for v in sample]

    phone_ratio = _phone_ratio(str_sample) if include_complex_types else 0.0
    email_ratio = _email_ratio(str_sample) if include_complex_types else 0.0
    bool_ratio = _bool_ratio(sample)
    int_ratio = _int_ratio(sample)
    decimal_ratio = _decimal_ratio(sample)
    date_ratio, datetime_ratio = _date_datetime_ratios(sample)

    ratios = {
        "phone": round(phone_ratio, 3),
        "email": round(email_ratio, 3),
        "bool": round(bool_ratio, 3),
        "int": round(int_ratio, 3),
        "decimal": round(decimal_ratio, 3),
        "date": round(date_ratio, 3),
        "datetime": round(datetime_ratio, 3),
    }

    # Conservative thresholds: avoid misclassifying IDs/phone-like strings as numbers.
    strong = 0.95
    medium = 0.85

    if include_complex_types and email_ratio >= strong:
        return {
            "type": "email",
            "confidence": round(email_ratio, 3),
            "reason": f"{int(email_ratio * len(sample))}/{len(sample)} samples match email pattern",
            "metadata": {"fallback": True, "ratios": ratios},
        }

    if include_complex_types and phone_ratio >= medium:
        return {
            "type": "phone",
            "confidence": round(phone_ratio, 3),
            "reason": f"{int(phone_ratio * len(sample))}/{len(sample)} samples match phone pattern",
            "metadata": {"fallback": True, "ratios": ratios},
        }

    if bool_ratio >= strong:
        return {
            "type": "xsd:boolean",
            "confidence": round(bool_ratio, 3),
            "reason": f"{int(bool_ratio * len(sample))}/{len(sample)} samples parse as boolean",
            "metadata": {"fallback": True, "ratios": ratios},
        }

    if int_ratio >= strong:
        return {
            "type": "xsd:integer",
            "confidence": round(int_ratio, 3),
            "reason": f"{int(int_ratio * len(sample))}/{len(sample)} samples parse as integer",
            "metadata": {"fallback": True, "ratios": ratios},
        }

    if decimal_ratio >= strong:
        return {
            "type": "xsd:decimal",
            "confidence": round(decimal_ratio, 3),
            "reason": f"{int(decimal_ratio * len(sample))}/{len(sample)} samples parse as decimal",
            "metadata": {"fallback": True, "ratios": ratios},
        }

    # Date/time: choose the stronger signal if it's dominant.
    # If both are weak, keep string and expose ratios for transparency.
    if datetime_ratio >= strong and datetime_ratio >= date_ratio:
        return {
            "type": "xsd:dateTime",
            "confidence": round(datetime_ratio, 3),
            "reason": f"{int(datetime_ratio * len(sample))}/{len(sample)} samples parse as datetime",
            "metadata": {"fallback": True, "ratios": ratios},
        }

    if date_ratio >= strong:
        return {
            "type": "xsd:date",
            "confidence": round(date_ratio, 3),
            "reason": f"{int(date_ratio * len(sample))}/{len(sample)} samples parse as date",
            "metadata": {"fallback": True, "ratios": ratios},
        }

    best = max(bool_ratio, int_ratio, decimal_ratio, datetime_ratio, date_ratio, phone_ratio, email_ratio)
    return {
        "type": "xsd:string",
        "confidence": round(best, 3),
        "reason": "Mixed/unreliable patterns; defaulting to string",
        "metadata": {"fallback": True, "ratios": ratios, "column_name": column_name or ""},
    }


def build_funnel_analysis_fallback(
    *,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    include_complex_types: bool = True,
    error: Optional[str] = None,
    stage: str = "bff",
) -> Dict[str, Any]:
    """
    Build a Funnel-compatible analysis payload from sample rows without calling Funnel.

    Intended usage:
    - CSV ingest preview when Funnel is down
    - /datasets/.../funnel-analysis recompute when Funnel is down
    - Google Sheets registration preview when Funnel is down
    """
    safe_columns = [str(c) for c in (columns or []) if str(c)]
    safe_rows: List[List[Any]] = [
        list(row) if isinstance(row, (list, tuple)) else [] for row in (rows or [])
    ]

    column_results: List[Dict[str, Any]] = []
    for col_index, col_name in enumerate(safe_columns):
        values = [row[col_index] if col_index < len(row) else None for row in safe_rows]
        total_count = len(values)
        non_empty = [v for v in values if _is_non_empty(v)]
        non_empty_count = len(non_empty)
        null_count = total_count - non_empty_count
        unique_count = len({str(v) for v in non_empty}) if non_empty else 0
        null_ratio = (null_count / total_count) if total_count else 0.0
        unique_ratio = (unique_count / non_empty_count) if non_empty_count else 0.0

        inferred_type = infer_type_fallback(
            values,
            column_name=col_name,
            include_complex_types=include_complex_types,
        )
        column_results.append(
            {
                "column_name": col_name,
                "inferred_type": inferred_type,
                "total_count": total_count,
                "non_empty_count": non_empty_count,
                "sample_values": [v for v in non_empty[:10]],
                "null_count": null_count,
                "unique_count": unique_count,
                "null_ratio": round(null_ratio, 6),
                "unique_ratio": round(unique_ratio, 6),
                "risk_flags": [],
            }
        )

    risk_summary: List[Dict[str, Any]] = []
    if error:
        risk_summary.append(
            {
                "code": ExternalErrorCode.TABULAR_ANALYSIS_UNAVAILABLE.value,
                "severity": "warning",
                "message": "Tabular analysis runtime unavailable; used local fallback type inference.",
                "column": None,
                "evidence": {
                    "error": str(error),
                    "sample_rows": len(safe_rows),
                    "sample_cols": len(safe_columns),
                },
                "suggested_actions": [
                    "Retry tabular analysis when the runtime is healthy.",
                    "Verify inferred types before running production objectify/pipelines.",
                ],
                "is_suggestion": True,
                "stage": stage,
            }
        )

    return {
        "columns": column_results,
        "analysis_metadata": {
            "source": "fallback",
            "sample_rows": len(safe_rows),
            "sample_cols": len(safe_columns),
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "risk_summary": risk_summary,
        "risk_policy": {"stage": "funnel", "suggestion_only": True, "hard_gate": False},
    }
