"""
LLM safety utilities (domain-neutral).

Goals:
- Minimize data sent to LLM (truncate / sample).
- Mask likely PII before leaving the system.
- Provide stable digests (hashes) for audit/caching without storing raw payloads.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, List, Optional, Union

from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode


_EMAIL_RE = re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")
# Loose phone matcher (international/local); we mask long digit runs to reduce PII leakage.
_LONG_DIGIT_RE = re.compile(r"\b\+?\d[\d\s().-]{7,}\d\b")
_UUID_RE = re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b")
# Date/time patterns that should NOT be masked as phone numbers / PII numbers.
# ISO-8601 datetime (2018-01-01T12:34:56, 2018-01-01 12:34:56.789+09:00, etc.)
_DATETIME_RE = re.compile(
    r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b"
)
# ISO-8601 date only (2018-01-01)
_DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
# US/EU date formats (01/31/2018, 31.01.2018)
_DATE_SLASH_RE = re.compile(r"\b\d{2}/\d{2}/\d{4}\b")
_DATE_DOT_RE = re.compile(r"\b\d{2}\.\d{2}\.\d{4}\b")
# Decimal numbers (123.45, -0.99) — not PII
_DECIMAL_NUM_RE = re.compile(r"\b[+-]?\d+\.\d+\b")


def sha256_hex(value: Union[str, bytes]) -> str:
    data = value.encode("utf-8") if isinstance(value, str) else value
    return hashlib.sha256(data).hexdigest()


def stable_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True, default=str)


def digest_for_audit(obj: Any) -> str:
    return sha256_hex(stable_json_dumps(obj))


def truncate_text(text: str, *, max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 1)] + "…"


def _mask_email(text: str) -> str:
    def _repl(match: re.Match[str]) -> str:
        raw = match.group(0)
        digest = sha256_hex(raw)[:10]
        return f"<email:{digest}>"

    return _EMAIL_RE.sub(_repl, text)


def _mask_long_digits(text: str) -> str:
    placeholders: dict[str, str] = {}

    def _protect(match: re.Match[str]) -> str:
        key = f"__PH_{len(placeholders)}__"
        placeholders[key] = match.group(0)
        return key

    def _repl(match: re.Match[str]) -> str:
        raw = match.group(0)
        digits = re.sub(r"\D+", "", raw)
        if len(digits) < 8:
            return raw
        digest = sha256_hex(digits)[:10]
        return f"<number:{digest}>"

    # Protect non-PII patterns that look like long digit sequences:
    # UUIDs, ISO datetimes, dates, US/EU dates, and decimal numbers.
    protected = _UUID_RE.sub(_protect, text)
    protected = _DATETIME_RE.sub(_protect, protected)
    protected = _DATE_RE.sub(_protect, protected)
    protected = _DATE_SLASH_RE.sub(_protect, protected)
    protected = _DATE_DOT_RE.sub(_protect, protected)
    protected = _DECIMAL_NUM_RE.sub(_protect, protected)
    masked = _LONG_DIGIT_RE.sub(_repl, protected)
    for key, value in placeholders.items():
        masked = masked.replace(key, value)
    return masked


def mask_pii_text(text: str, *, max_chars: Optional[int] = None) -> str:
    out = text
    out = _mask_email(out)
    out = _mask_long_digits(out)
    if max_chars is not None:
        out = truncate_text(out, max_chars=max_chars)
    return out


def mask_pii(obj: Any, *, max_string_chars: int = 200) -> Any:
    """
    Recursively mask likely PII in a JSON-like structure.

    Notes:
    - This is intentionally conservative (masking *likely* PII).
    - Do not treat this as a formal DLP system.
    """

    if obj is None:
        return None
    if isinstance(obj, (bool, int, float)):
        return obj
    if isinstance(obj, str):
        return mask_pii_text(obj, max_chars=max_string_chars)
    if isinstance(obj, (list, tuple)):
        return [mask_pii(v, max_string_chars=max_string_chars) for v in obj]
    if isinstance(obj, dict):
        return {str(k): mask_pii(v, max_string_chars=max_string_chars) for k, v in obj.items()}
    # Fallback: stringize
    return mask_pii_text(str(obj), max_chars=max_string_chars)


def sample_items(items: List[Any], *, max_items: int) -> List[Any]:
    if max_items <= 0:
        return []
    return items[:max_items]


def build_agent_error_response(
    error_message: str,
    *,
    error_code: Optional[str] = None,
    recoverable: bool = True,
    hint: Optional[str] = None,
    suggested_action: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build a standard error response format for the Agent.

    This format helps the Agent understand:
    - What went wrong
    - Whether it can recover
    - What to do next
    """
    payload = build_error_envelope(
        service_name="shared",
        message=error_message,
        detail=error_message,
        code=ErrorCode.INTERNAL_ERROR,
        category=ErrorCategory.INTERNAL,
        status_code=500,
        external_code=str(error_code) if error_code else None,
        context=context,
        prefer_status_code=True,
    )
    # Back-compat for agent consumers that look for `error` in tool observations.
    payload["error"] = error_message
    payload["recoverable"] = bool(recoverable)
    if hint:
        payload["hint"] = hint
    if suggested_action:
        payload["suggested_action"] = suggested_action
    return payload


# ==============================================================================
# Raw Data-Centric Agent Support (2026-01)
# ==============================================================================

_URL_RE = re.compile(r"(?i)^https?://")
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ISO_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}")
_US_DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")
_EU_DATE_RE = re.compile(r"^\d{2}\.\d{2}\.\d{4}$")
_DECIMAL_RE = re.compile(r"^[+-]?\d+\.\d+$")
_INTEGER_RE = re.compile(r"^[+-]?\d+$")
# "USR-001", "INV00123", "12345" (>=3 digits) are treated as sequential-ish identifiers.
_SEQUENTIAL_ID_RE = re.compile(r"^[A-Za-z]{0,8}[-_]?(\d{3,})$")
_CODE_SHORT_RE = re.compile(r"^[A-Z]{2,3}$")
_IDENTIFIER_LIKE_RE = re.compile(r"^[A-Z][A-Z0-9_]{2,}$")


def detect_value_pattern(value: Any) -> str:
    """
    Best-effort value pattern detection for raw-data reasoning.

    This intentionally uses lightweight heuristics (no schema required) so we can produce
    stable, explainable observations for an Agent without reading full raw datasets.
    """
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "decimal"

    text = str(value).strip()
    if text == "":
        return "empty_string"

    if _UUID_RE.match(text):
        return "uuid"
    if _EMAIL_RE.search(text):
        return "email"
    if _URL_RE.match(text):
        return "url"
    if _ISO_DATETIME_RE.match(text):
        return "iso_datetime"
    if _ISO_DATE_RE.match(text):
        return "iso_date"
    if _US_DATE_RE.match(text):
        return "us_date_format"
    if _EU_DATE_RE.match(text):
        return "eu_date_format"

    lowered = text.lower()
    if lowered in {"true", "false", "yes", "no", "y", "n", "t", "f"}:
        return "boolean_string"

    if _DECIMAL_RE.match(text):
        return "decimal_string"

    if _SEQUENTIAL_ID_RE.match(text):
        return "sequential_id"

    if _INTEGER_RE.match(text):
        # Keep short ints as integers; longer runs are more likely identifiers.
        digits = re.sub(r"[+-]", "", text)
        return "integer_string" if len(digits) <= 2 else "sequential_id"

    if _CODE_SHORT_RE.match(text):
        return "code_short"

    if _IDENTIFIER_LIKE_RE.match(text):
        return "identifier_like"

    return "free_text"


def extract_column_value_patterns(values: List[Any], *, max_samples: int = 200) -> Dict[str, Any]:
    """
    Summarize observed value patterns for a column.

    Returns a compact, JSON-safe structure that can be safely embedded into prompts.
    """
    sample = values[: max(0, int(max_samples))]
    total = len(sample)
    if total == 0:
        return {
            "total_analyzed": 0,
            "null_ratio": 0.0,
            "dominant_pattern": None,
            "is_homogeneous": False,
            "pattern_distribution": {},
            "observations": [],
        }

    null_count = sum(1 for v in sample if v is None)
    non_null = [v for v in sample if v is not None]
    patterns: Dict[str, int] = {}
    examples: Dict[str, List[str]] = {}

    for v in non_null:
        pat = detect_value_pattern(v)
        patterns[pat] = patterns.get(pat, 0) + 1
        ex_list = examples.setdefault(pat, [])
        if len(ex_list) < 3:
            ex_list.append(str(v))

    dominant: Optional[str] = None
    dominant_count = 0
    for pat, cnt in patterns.items():
        if cnt > dominant_count:
            dominant = pat
            dominant_count = cnt

    non_null_total = len(non_null)
    dominant_ratio = (dominant_count / non_null_total) if non_null_total else 0.0
    is_homogeneous = bool(non_null_total) and (len(patterns) == 1 or dominant_ratio >= 0.9)

    distribution: Dict[str, Any] = {}
    for pat, cnt in sorted(patterns.items(), key=lambda kv: (-kv[1], kv[0])):
        distribution[pat] = {
            "count": cnt,
            "ratio": round(cnt / non_null_total, 4) if non_null_total else 0.0,
            "examples": examples.get(pat, [])[:3],
        }

    observations: List[str] = []
    if dominant == "uuid":
        observations.append("UUID-like values detected (likely identifier).")
    elif dominant == "sequential_id":
        observations.append("Sequential ID-like values detected (likely identifier).")
    elif dominant in {"iso_date", "iso_datetime", "us_date_format", "eu_date_format"}:
        observations.append("Date/time-like values detected.")
    elif dominant == "email":
        observations.append("Email-like values detected (PII risk).")
    elif dominant == "boolean_string":
        observations.append("Boolean-like strings detected.")

    return {
        "total_analyzed": total,
        "null_ratio": round(null_count / total, 4) if total else 0.0,
        "dominant_pattern": dominant,
        "is_homogeneous": is_homogeneous,
        "pattern_distribution": distribution,
        "observations": observations,
    }


def build_column_semantic_observations(column_name: str, values: List[Any]) -> Dict[str, Any]:
    """
    Produce higher-level semantic hints for a column based on:
    - Column name heuristics (id, created_at, status, email, is_*)
    - Value pattern extraction
    """
    name = str(column_name or "").strip()
    lowered = name.lower()
    patterns = extract_column_value_patterns(values)
    dominant = patterns.get("dominant_pattern")

    semantic_hints: List[str] = []
    name_observations: List[str] = []
    cross_observations: List[str] = []

    if lowered == "id" or lowered.endswith("_id") or lowered.endswith("id"):
        semantic_hints.append("possible_foreign_key")
        name_observations.append("Name suggests an ID column (possible FK/reference).")
        if dominant in {"uuid", "sequential_id"}:
            semantic_hints.append("likely_identifier")
            cross_observations.append("✓ Name suggests ID and values match common identifier formats.")

    if any(token in lowered for token in ("created_at", "updated_at", "timestamp", "time", "date", "_at")):
        semantic_hints.append("likely_temporal")
        name_observations.append("Name suggests temporal meaning (date/time).")
        if dominant in {"iso_date", "iso_datetime", "us_date_format", "eu_date_format"}:
            cross_observations.append("✓ Name and values both look like date/time.")

    if any(token in lowered for token in ("email", "phone", "ssn", "social", "address")):
        semantic_hints.append("possibly_sensitive")
        name_observations.append("Name suggests potentially sensitive data (PII).")
        if dominant in {"email"}:
            cross_observations.append("✓ Detected values match sensitive patterns (email).")

    if lowered.startswith("is_") or lowered.startswith("has_") or lowered.startswith("flag") or lowered.endswith("_flag"):
        semantic_hints.append("likely_boolean")
        name_observations.append("Name suggests boolean flag semantics.")
        if dominant in {"boolean_string"}:
            cross_observations.append("✓ Name suggests boolean and values look boolean-like.")

    if any(token in lowered for token in ("status", "state", "type", "category", "tier", "level")):
        semantic_hints.append("likely_categorical")
        name_observations.append("Name suggests categorical/enum semantics.")

    combined_observations: List[str] = []
    combined_observations.extend(name_observations)
    combined_observations.extend(patterns.get("observations") or [])
    combined_observations.extend(cross_observations)

    return {
        "column_name": name,
        "semantic_hints": sorted(set(semantic_hints)),
        "value_patterns": patterns,
        "name_observations": name_observations,
        "cross_observations": cross_observations,
        "combined_observations": combined_observations,
    }


def _normalize_value_for_overlap(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().lower()
    return text if text != "" else None


def build_relationship_observations(
    left_column: str,
    right_column: str,
    left_values: List[Any],
    right_values: List[Any],
    *,
    max_examples: int = 5,
) -> Dict[str, Any]:
    """
    Compare two columns' values to produce FK/relationship hints.

    The intent is to give an Agent compact, defensible signals:
    overlap, containment, uniqueness, null ratios, and example mismatches.
    """
    left_total = len(left_values)
    right_total = len(right_values)
    left_norm = [_normalize_value_for_overlap(v) for v in left_values]
    right_norm = [_normalize_value_for_overlap(v) for v in right_values]

    left_nulls = sum(1 for v in left_norm if v is None)
    right_nulls = sum(1 for v in right_norm if v is None)

    left_non_null = [v for v in left_norm if v is not None]
    right_non_null = [v for v in right_norm if v is not None]

    left_set = set(left_non_null)
    right_set = set(right_non_null)

    intersection = left_set & right_set
    intersection_count = len(intersection)

    left_containment = intersection_count / len(left_set) if left_set else 0.0
    right_containment = intersection_count / len(right_set) if right_set else 0.0

    left_unique_ratio = len(left_set) / len(left_non_null) if left_non_null else 0.0
    right_unique_ratio = len(right_set) / len(right_non_null) if right_non_null else 0.0

    left_only = sorted(left_set - right_set)
    right_only = sorted(right_set - left_set)

    observations: List[str] = []
    if intersection_count == 0 and left_set and right_set:
        observations.append("NO VALUE OVERLAP: columns share no common values.")
    else:
        observations.append(
            f"Overlap: intersection={intersection_count}, left_containment={left_containment:.2f}, right_containment={right_containment:.2f}."
        )

    if left_unique_ratio >= 0.9 and right_unique_ratio < 0.7:
        observations.append("Potential one-to-many relationship: left looks unique, right has duplicates.")

    agent_instruction = (
        "Use domain knowledge to confirm whether this relationship represents an FK/PK link, "
        "and validate join keys, null handling, and expected cardinality before enforcing constraints."
    )

    return {
        "left_column": str(left_column or ""),
        "right_column": str(right_column or ""),
        "null_metrics": {
            "left_null_ratio": round(left_nulls / left_total, 4) if left_total else 0.0,
            "right_null_ratio": round(right_nulls / right_total, 4) if right_total else 0.0,
        },
        "uniqueness_metrics": {
            "left_unique_ratio": round(left_unique_ratio, 4),
            "right_unique_ratio": round(right_unique_ratio, 4),
        },
        "overlap_metrics": {
            "intersection_count": intersection_count,
            "left_containment": round(left_containment, 4),
            "right_containment": round(right_containment, 4),
        },
        "non_matching_examples": {
            "left_only": left_only[: max(0, int(max_examples))],
            "right_only": right_only[: max(0, int(max_examples))],
        },
        "observations": observations,
        "agent_instruction": agent_instruction,
    }
