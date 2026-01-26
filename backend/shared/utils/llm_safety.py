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
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union


_EMAIL_RE = re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")
# Loose phone matcher (international/local); we mask long digit runs to reduce PII leakage.
_LONG_DIGIT_RE = re.compile(r"\b\+?\d[\d\s().-]{7,}\d\b")
_UUID_RE = re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b")


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

    def _protect_uuid(match: re.Match[str]) -> str:
        key = f"__UUID_{len(placeholders)}__"
        placeholders[key] = match.group(0)
        return key

    def _repl(match: re.Match[str]) -> str:
        raw = match.group(0)
        digits = re.sub(r"\D+", "", raw)
        if len(digits) < 8:
            return raw
        digest = sha256_hex(digits)[:10]
        return f"<number:{digest}>"

    protected = _UUID_RE.sub(_protect_uuid, text)
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


# ==============================================================================
# Enterprise Enhancement (2026-01): Standard Response Formats for LLM Agent
# ==============================================================================

def build_sample_disclaimer(
    *,
    sample_size: Optional[int] = None,
    total_size: Optional[int] = None,
    coverage_ratio: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Build a standard disclaimer for sample-based results.

    This helps the Agent understand that results are based on sampled data
    and may not reflect the full dataset characteristics.
    """
    disclaimer: Dict[str, Any] = {
        "mode": "sample_based",
        "disclaimer": "Results are based on sampled data and do not guarantee correctness on full dataset",
    }

    if sample_size is not None:
        disclaimer["sample_size"] = sample_size
    if total_size is not None:
        disclaimer["total_size"] = total_size
    if coverage_ratio is not None:
        disclaimer["coverage_ratio"] = round(coverage_ratio, 4)

    # Add severity based on coverage
    if coverage_ratio is not None:
        if coverage_ratio < 0.001:
            disclaimer["confidence_level"] = "very_low"
            disclaimer["warning"] = (
                "CRITICAL: Sample covers less than 0.1% of data. "
                "Results may be severely unrepresentative."
            )
        elif coverage_ratio < 0.01:
            disclaimer["confidence_level"] = "low"
            disclaimer["warning"] = (
                "Sample covers less than 1% of data. "
                "Results should be treated as rough estimates."
            )
        elif coverage_ratio < 0.1:
            disclaimer["confidence_level"] = "medium"
        else:
            disclaimer["confidence_level"] = "high"

    return disclaimer


def build_confidence_envelope(
    *,
    primary_value: Any,
    confidence_score: float,
    alternative_values: Optional[List[Any]] = None,
    warnings: Optional[List[str]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Wrap a value with confidence metadata for the Agent.

    This standard format helps the Agent understand:
    - How confident we are in the primary value
    - What alternative interpretations exist
    - What warnings should be considered
    """
    envelope: Dict[str, Any] = {
        "value": primary_value,
        "confidence": round(min(1.0, max(0.0, confidence_score)), 3),
    }

    # Classify confidence level
    if confidence_score >= 0.9:
        envelope["confidence_level"] = "high"
    elif confidence_score >= 0.7:
        envelope["confidence_level"] = "medium"
    elif confidence_score >= 0.5:
        envelope["confidence_level"] = "low"
    else:
        envelope["confidence_level"] = "very_low"

    if alternative_values:
        envelope["alternatives"] = alternative_values

    if warnings:
        envelope["warnings"] = warnings

    if metadata:
        envelope["metadata"] = metadata

    return envelope


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
    response: Dict[str, Any] = {
        "status": "error",
        "error": error_message,
        "recoverable": recoverable,
    }

    if error_code:
        response["error_code"] = error_code

    if hint:
        response["hint"] = hint

    if suggested_action:
        response["suggested_action"] = suggested_action

    if context:
        response["context"] = context

    return response


def build_ambiguity_warning(
    value_type: str,
    primary_interpretation: str,
    alternative_interpretation: str,
    *,
    raw_value: Optional[str] = None,
    resolution_hint: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build a standard warning for ambiguous data interpretations.

    Examples:
    - Locale ambiguity: "1.234" could be 1.234 or 1234
    - Date format ambiguity: "01/02/2024" could be Jan 2 or Feb 1
    """
    warning: Dict[str, Any] = {
        "type": "ambiguity",
        "category": value_type,
        "primary_interpretation": primary_interpretation,
        "alternative_interpretation": alternative_interpretation,
        "is_ambiguous": True,
    }

    if raw_value:
        warning["raw_value"] = raw_value

    if resolution_hint:
        warning["resolution_hint"] = resolution_hint
    else:
        warning["resolution_hint"] = (
            f"Verify the intended interpretation for {value_type}. "
            "Consider using explicit format specifications."
        )

    return warning


# ==============================================================================
# Raw Data-Centric Agent Support (2026-01)
# ==============================================================================
# Philosophy: Let LLM see raw data and make its own judgments.
# System provides OBSERVATIONS, not RECOMMENDATIONS.
# LLM's pattern recognition and domain knowledge should drive decisions.

# Pattern detection regexes
_UUID_PATTERN = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_SEQUENTIAL_ID_PATTERN = re.compile(r"^[A-Za-z]{0,5}[-_]?\d{3,}$")  # USR-001, INV00123
_NUMERIC_STRING_PATTERN = re.compile(r"^-?\d+\.?\d*$")
_ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2})?")
_US_DATE_PATTERN = re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}$")
_EU_DATE_PATTERN = re.compile(r"^\d{1,2}\.\d{1,2}\.\d{2,4}$")
_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_PHONE_PATTERN = re.compile(r"^[\d\s\-\(\)\+\.]{7,}$")
_URL_PATTERN = re.compile(r"^https?://")
_JSON_LIKE_PATTERN = re.compile(r"^\s*[\[\{]")
_BOOLEAN_VALUES = {"true", "false", "yes", "no", "1", "0", "t", "f", "y", "n"}


def detect_value_pattern(value: Any) -> Optional[str]:
    """
    Detect the semantic pattern of a single value.

    Returns pattern name or None if no recognizable pattern.
    This helps LLM understand what kind of data it's looking at.
    """
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "decimal"

    text = str(value).strip()
    if not text:
        return "empty"

    # Check patterns in order of specificity
    if _UUID_PATTERN.match(text):
        return "uuid"
    if _ISO_DATE_PATTERN.match(text):
        return "iso_datetime" if "T" in text else "iso_date"
    if _US_DATE_PATTERN.match(text):
        return "us_date_format"  # MM/DD/YYYY - ambiguous
    if _EU_DATE_PATTERN.match(text):
        return "eu_date_format"  # DD.MM.YYYY - ambiguous
    if _EMAIL_PATTERN.match(text):
        return "email"
    if _URL_PATTERN.match(text):
        return "url"
    if _JSON_LIKE_PATTERN.match(text):
        return "json_or_array"
    if _SEQUENTIAL_ID_PATTERN.match(text):
        return "sequential_id"  # USR-001, INV00123
    if text.lower() in _BOOLEAN_VALUES:
        return "boolean_string"
    if _NUMERIC_STRING_PATTERN.match(text):
        if "." in text:
            return "decimal_string"
        return "integer_string"
    if _PHONE_PATTERN.match(text) and sum(c.isdigit() for c in text) >= 7:
        return "phone_like"
    if len(text) <= 3 and text.isupper():
        return "code_short"  # USA, KR, USD
    if "_" in text or text.isupper():
        return "identifier_like"  # ENUM_VALUE, STATUS_ACTIVE

    return "free_text"


def extract_column_value_patterns(
    values: Iterable[Any],
    *,
    max_samples: int = 100,
    min_pattern_ratio: float = 0.1,
) -> Dict[str, Any]:
    """
    Analyze values in a column and extract pattern distribution.

    Returns structured observations (NOT recommendations) that help
    LLM understand the data characteristics.

    Example output:
    {
        "total_analyzed": 100,
        "pattern_distribution": {
            "uuid": {"count": 95, "ratio": 0.95, "examples": ["abc-123-...", ...]},
            "null": {"count": 5, "ratio": 0.05, "examples": []},
        },
        "dominant_pattern": "uuid",
        "is_homogeneous": True,
        "observations": [
            "95% of values follow UUID format",
            "Column appears to contain unique identifiers"
        ]
    }
    """
    # Collect samples
    samples: List[Any] = []
    for v in values:
        samples.append(v)
        if len(samples) >= max_samples:
            break

    if not samples:
        return {
            "total_analyzed": 0,
            "pattern_distribution": {},
            "dominant_pattern": None,
            "is_homogeneous": False,
            "observations": ["No values to analyze"],
        }

    # Detect patterns
    pattern_counts: Dict[str, int] = {}
    pattern_examples: Dict[str, List[str]] = {}

    for v in samples:
        pattern = detect_value_pattern(v)
        if pattern:
            pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1
            if pattern not in pattern_examples:
                pattern_examples[pattern] = []
            if len(pattern_examples[pattern]) < 3:  # Keep up to 3 examples
                example = str(v) if v is not None else "null"
                if len(example) > 50:
                    example = example[:47] + "..."
                pattern_examples[pattern].append(example)

    total = len(samples)
    distribution: Dict[str, Dict[str, Any]] = {}
    for pattern, count in sorted(pattern_counts.items(), key=lambda x: -x[1]):
        ratio = count / total
        if ratio >= min_pattern_ratio or pattern in ("null", "empty"):
            distribution[pattern] = {
                "count": count,
                "ratio": round(ratio, 3),
                "examples": pattern_examples.get(pattern, []),
            }

    # Find dominant pattern (excluding null/empty)
    non_null_patterns = {
        k: v for k, v in pattern_counts.items() if k not in ("null", "empty")
    }
    dominant = max(non_null_patterns.items(), key=lambda x: x[1])[0] if non_null_patterns else None
    dominant_ratio = pattern_counts.get(dominant, 0) / total if dominant else 0

    # Generate observations (not recommendations!)
    observations: List[str] = []

    if dominant:
        observations.append(f"{dominant_ratio*100:.0f}% of values match '{dominant}' pattern")

    null_ratio = pattern_counts.get("null", 0) / total
    empty_ratio = pattern_counts.get("empty", 0) / total
    if null_ratio > 0.1:
        observations.append(f"{null_ratio*100:.0f}% of values are NULL")
    if empty_ratio > 0.1:
        observations.append(f"{empty_ratio*100:.0f}% of values are empty strings")

    if dominant == "uuid":
        observations.append("Values appear to be UUIDs (system-generated identifiers)")
    elif dominant == "sequential_id":
        observations.append("Values appear to be sequential IDs (e.g., USR-001, INV00123)")
    elif dominant == "iso_date" or dominant == "iso_datetime":
        observations.append("Values appear to be ISO format dates/timestamps")
    elif dominant in ("us_date_format", "eu_date_format"):
        observations.append(
            f"Values use {'US (MM/DD)' if dominant == 'us_date_format' else 'EU (DD.MM)'} "
            "date format - VERIFY locale interpretation"
        )
    elif dominant == "boolean_string":
        observations.append("Values appear to be boolean (true/false/yes/no)")

    is_homogeneous = dominant_ratio >= 0.9 if dominant else False
    if is_homogeneous:
        observations.append("Column values are highly homogeneous (single pattern)")
    elif len(non_null_patterns) > 3:
        observations.append("Column has mixed value patterns - may need cleansing")

    return {
        "total_analyzed": total,
        "pattern_distribution": distribution,
        "dominant_pattern": dominant,
        "dominant_ratio": round(dominant_ratio, 3) if dominant else 0,
        "is_homogeneous": is_homogeneous,
        "null_ratio": round(null_ratio, 3),
        "observations": observations,
    }


def build_column_semantic_observations(
    column_name: str,
    values: Iterable[Any],
    *,
    max_samples: int = 100,
) -> Dict[str, Any]:
    """
    Build semantic observations about a column based on name AND values.

    This combines:
    1. Column name analysis (semantic hints from naming conventions)
    2. Value pattern analysis (what the actual data looks like)

    Returns observations that help LLM make informed decisions.
    """
    name_lower = column_name.lower().strip()

    # Name-based observations
    name_observations: List[str] = []
    semantic_hints: List[str] = []

    # ID-like column detection
    if name_lower == "id" or name_lower.endswith("_id") or name_lower.endswith("id"):
        name_observations.append("Column name suggests this is an identifier")
        semantic_hints.append("likely_identifier")

    # Timestamp detection
    if any(t in name_lower for t in ("created", "updated", "modified", "timestamp", "_at", "date", "time")):
        name_observations.append("Column name suggests temporal data")
        semantic_hints.append("likely_temporal")

    # Foreign key hint
    if name_lower.endswith("_id") and name_lower != "id":
        base_name = name_lower[:-3]  # Remove "_id"
        name_observations.append(f"Column may reference '{base_name}' table (FK pattern)")
        semantic_hints.append("possible_foreign_key")

    # Status/type enum detection
    if any(t in name_lower for t in ("status", "type", "category", "state", "kind", "level")):
        name_observations.append("Column name suggests categorical/enum values")
        semantic_hints.append("likely_categorical")

    # Flag/boolean detection
    if any(t in name_lower for t in ("is_", "has_", "can_", "flag", "enabled", "active", "deleted")):
        name_observations.append("Column name suggests boolean flag")
        semantic_hints.append("likely_boolean")

    # PII-sensitive columns
    if any(t in name_lower for t in ("email", "phone", "address", "ssn", "password", "secret", "token")):
        name_observations.append("Column name suggests PII/sensitive data - handle with care")
        semantic_hints.append("possibly_sensitive")

    # Content columns (not good for keys)
    if any(t in name_lower for t in ("description", "comment", "note", "text", "content", "body", "message")):
        name_observations.append("Column name suggests free-text content (not suitable for keys)")
        semantic_hints.append("free_text_content")

    # Value-based analysis
    value_analysis = extract_column_value_patterns(values, max_samples=max_samples)

    # Cross-reference name hints with actual values
    cross_observations: List[str] = []

    dominant = value_analysis.get("dominant_pattern")
    if "likely_identifier" in semantic_hints:
        if dominant == "uuid":
            cross_observations.append("Name suggests ID, values confirm UUID format ✓")
        elif dominant == "sequential_id":
            cross_observations.append("Name suggests ID, values confirm sequential ID format ✓")
        elif dominant == "integer_string" or dominant == "integer":
            cross_observations.append("Name suggests ID, values are integers (auto-increment style)")
        elif dominant == "free_text":
            cross_observations.append("WARNING: Name suggests ID but values look like free text")

    if "likely_temporal" in semantic_hints:
        if dominant in ("iso_date", "iso_datetime"):
            cross_observations.append("Name suggests temporal, values confirm ISO date format ✓")
        elif dominant in ("us_date_format", "eu_date_format"):
            cross_observations.append(
                "Name suggests temporal, but date format is ambiguous - verify locale"
            )
        elif dominant not in ("iso_date", "iso_datetime", "us_date_format", "eu_date_format"):
            cross_observations.append("WARNING: Name suggests temporal but values don't look like dates")

    if "likely_boolean" in semantic_hints:
        if dominant == "boolean" or dominant == "boolean_string":
            cross_observations.append("Name suggests boolean, values confirm ✓")
        elif dominant == "integer_string" and value_analysis.get("pattern_distribution", {}).get("integer_string", {}).get("examples", []):
            examples = value_analysis["pattern_distribution"]["integer_string"]["examples"]
            if all(e in ("0", "1") for e in examples):
                cross_observations.append("Name suggests boolean, values are 0/1 (boolean integers) ✓")

    return {
        "column_name": column_name,
        "name_observations": name_observations,
        "semantic_hints": semantic_hints,
        "value_analysis": value_analysis,
        "cross_observations": cross_observations,
        "combined_observations": name_observations + value_analysis.get("observations", []) + cross_observations,
    }


def build_raw_data_display(
    rows: List[Dict[str, Any]],
    columns: List[str],
    *,
    max_rows: int = 20,
    max_value_length: int = 80,
) -> Dict[str, Any]:
    """
    Build a structured display of raw data for LLM analysis.

    This is designed to help LLM see actual data patterns clearly.
    Includes:
    - First N rows (to see typical values)
    - Value length distribution
    - Unique value examples per column
    """
    display_rows = rows[:max_rows]

    # Truncate long values for display
    def truncate(v: Any) -> str:
        if v is None:
            return "<null>"
        s = str(v)
        if len(s) > max_value_length:
            return s[: max_value_length - 3] + "..."
        return s

    formatted_rows: List[Dict[str, str]] = []
    for row in display_rows:
        formatted_rows.append({col: truncate(row.get(col)) for col in columns})

    # Collect unique values per column (for LLM to see variety)
    unique_per_column: Dict[str, List[str]] = {}
    for col in columns:
        seen: set[str] = set()
        examples: List[str] = []
        for row in rows:
            v = row.get(col)
            s = truncate(v)
            if s not in seen and len(examples) < 8:
                seen.add(s)
                examples.append(s)
        unique_per_column[col] = examples

    return {
        "row_count_shown": len(formatted_rows),
        "total_rows_available": len(rows),
        "columns": columns,
        "sample_rows": formatted_rows,
        "unique_value_examples": unique_per_column,
        "display_note": (
            f"Showing {len(formatted_rows)} of {len(rows)} rows. "
            "Examine patterns in values to understand data semantics."
        ),
    }


def build_relationship_observations(
    left_column: str,
    right_column: str,
    left_values: Iterable[Any],
    right_values: Iterable[Any],
    *,
    max_samples: int = 200,
) -> Dict[str, Any]:
    """
    Build observations about potential relationship between two columns.

    Returns OBSERVATIONS (not recommendations) to help LLM decide:
    - Whether these columns should be joined
    - What the relationship cardinality might be
    - What issues might arise

    The LLM should make the final judgment based on these observations
    combined with its domain knowledge.
    """
    # Collect samples
    left_list: List[Any] = []
    right_list: List[Any] = []

    for v in left_values:
        left_list.append(v)
        if len(left_list) >= max_samples:
            break

    for v in right_values:
        right_list.append(v)
        if len(right_list) >= max_samples:
            break

    # Normalize for comparison
    def normalize(v: Any) -> Optional[str]:
        if v is None:
            return None
        s = str(v).strip().lower()
        return s if s else None

    left_normalized = [normalize(v) for v in left_list]
    right_normalized = [normalize(v) for v in right_list]

    left_non_null = [v for v in left_normalized if v is not None]
    right_non_null = [v for v in right_normalized if v is not None]

    left_set = set(left_non_null)
    right_set = set(right_non_null)

    # Calculate overlap
    intersection = left_set & right_set
    left_only = left_set - right_set
    right_only = right_set - left_set

    # Containment ratios
    left_in_right = len(intersection) / len(left_set) if left_set else 0
    right_in_left = len(intersection) / len(right_set) if right_set else 0

    # Uniqueness in each column
    left_unique_ratio = len(left_set) / len(left_non_null) if left_non_null else 0
    right_unique_ratio = len(right_set) / len(right_non_null) if right_non_null else 0

    # NULL ratios
    left_null_ratio = (len(left_list) - len(left_non_null)) / len(left_list) if left_list else 0
    right_null_ratio = (len(right_list) - len(right_non_null)) / len(right_list) if right_list else 0

    # Build observations
    observations: List[str] = []

    # Overlap observations
    if not intersection:
        observations.append("NO VALUE OVERLAP found between columns in sample")
        observations.append("These columns may not be joinable, or sample is unrepresentative")
    elif left_in_right >= 0.95 and right_in_left >= 0.95:
        observations.append(f"Very high mutual overlap ({left_in_right*100:.0f}%/{right_in_left*100:.0f}%)")
        observations.append("Columns appear to contain matching value sets")
    elif left_in_right >= 0.8:
        observations.append(f"{left_in_right*100:.0f}% of left values exist in right")
        observations.append("Left column values are mostly contained in right")
    elif right_in_left >= 0.8:
        observations.append(f"{right_in_left*100:.0f}% of right values exist in left")
        observations.append("Right column values are mostly contained in left")
    else:
        observations.append(f"Partial overlap: {left_in_right*100:.0f}% left→right, {right_in_left*100:.0f}% right→left")

    # Uniqueness observations
    if left_unique_ratio >= 0.99:
        observations.append(f"Left column ({left_column}) has unique values (possible PK)")
    elif left_unique_ratio >= 0.9:
        observations.append(f"Left column ({left_column}) has high uniqueness ({left_unique_ratio*100:.0f}%)")
    elif left_unique_ratio < 0.5:
        observations.append(f"Left column ({left_column}) has many duplicates ({(1-left_unique_ratio)*100:.0f}%)")

    if right_unique_ratio >= 0.99:
        observations.append(f"Right column ({right_column}) has unique values (possible PK)")
    elif right_unique_ratio >= 0.9:
        observations.append(f"Right column ({right_column}) has high uniqueness ({right_unique_ratio*100:.0f}%)")
    elif right_unique_ratio < 0.5:
        observations.append(f"Right column ({right_column}) has many duplicates ({(1-right_unique_ratio)*100:.0f}%)")

    # NULL observations
    if left_null_ratio > 0.3:
        observations.append(f"WARNING: Left column has {left_null_ratio*100:.0f}% NULL values")
    if right_null_ratio > 0.3:
        observations.append(f"WARNING: Right column has {right_null_ratio*100:.0f}% NULL values")

    # Cardinality hint (as observation, not conclusion)
    cardinality_observation = None
    if left_unique_ratio >= 0.95 and right_unique_ratio >= 0.95:
        cardinality_observation = "Both columns have unique values → possible 1:1 relationship"
    elif left_unique_ratio >= 0.95 and right_unique_ratio < 0.5:
        cardinality_observation = "Left unique, right has duplicates → possible 1:N (left is parent)"
    elif right_unique_ratio >= 0.95 and left_unique_ratio < 0.5:
        cardinality_observation = "Right unique, left has duplicates → possible N:1 (right is parent)"
    elif left_unique_ratio < 0.5 and right_unique_ratio < 0.5:
        cardinality_observation = "Both columns have duplicates → possible N:M relationship"

    if cardinality_observation:
        observations.append(cardinality_observation)

    # Non-matching value examples (for debugging)
    left_only_examples = list(left_only)[:5]
    right_only_examples = list(right_only)[:5]

    return {
        "left_column": left_column,
        "right_column": right_column,
        "sample_sizes": {
            "left": len(left_list),
            "right": len(right_list),
        },
        "overlap_metrics": {
            "intersection_count": len(intersection),
            "left_containment": round(left_in_right, 3),
            "right_containment": round(right_in_left, 3),
            "left_only_count": len(left_only),
            "right_only_count": len(right_only),
        },
        "uniqueness_metrics": {
            "left_unique_ratio": round(left_unique_ratio, 3),
            "right_unique_ratio": round(right_unique_ratio, 3),
        },
        "null_metrics": {
            "left_null_ratio": round(left_null_ratio, 3),
            "right_null_ratio": round(right_null_ratio, 3),
        },
        "observations": observations,
        "non_matching_examples": {
            "left_only": left_only_examples,
            "right_only": right_only_examples,
        },
        "agent_instruction": (
            "Based on these observations and your domain knowledge, "
            "determine if these columns represent a valid join relationship "
            "and what the appropriate join type should be."
        ),
    }
