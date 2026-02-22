"""
Claim-based refuter for pipeline plans.

This module implements a *refutation gate*:
- It never "proves" a plan/claim correct.
- It only produces HARD failures when it can provide a concrete counterexample ("witness").

Enterprise Enhancement (2026-01):
Added explicit tracking of supported claim kinds and clear warnings about:
- Sample-based verification limitations
- Claims that cannot be hard-gated (FK, ROW_PRESERVE_LEFT)
- Unknown/unsupported claim kinds that may indicate typos

The refuter is intentionally domain-agnostic and relies only on:
- plan structure (nodes/edges/metadata), and
- deterministic sample tables produced by PipelineExecutor (or provided run_tables).
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from shared.models.pipeline_plan import PipelinePlan
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.pipeline.pipeline_executor import PipelineExecutor, PipelineTable, _cast_value_with_status  # type: ignore
from shared.services.pipeline.pipeline_graph_utils import build_incoming, normalize_edges, normalize_nodes
from shared.services.pipeline.pipeline_transform_spec import normalize_operation, resolve_join_spec
from shared.services.pipeline.pipeline_type_utils import normalize_cast_target, parse_decimal_text, parse_datetime_text, parse_int_text
from shared.utils.llm_safety import mask_pii, stable_json_dumps, sha256_hex

logger = logging.getLogger(__name__)


def _normalize_string_list(value: Any) -> List[str]:
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    out: List[str] = []
    for item in items:
        text = str(item or "").strip()
        if not text:
            continue
        parts = [part.strip() for part in text.replace("+", ",").split(",") if part.strip()]
        out.extend(parts if parts else [text])
    return [item for item in out if item]


def _normalize_claim_kind(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    key = raw.replace("-", "_").upper()
    if key in {"PK", "PRIMARY_KEY", "PRIMARYKEY"}:
        return "PK"
    if key in {"FK", "FOREIGN_KEY", "FOREIGNKEY"}:
        return "FK"
    if key in {"CAST_SUCCESS", "CAST_ALL_PARSE", "CAST_PARSE_ALL"}:
        return "CAST_SUCCESS"
    if key in {"CAST_LOSSLESS", "CAST_ROUNDTRIP", "CAST_LOSSLESS_ROUNDTRIP"}:
        return "CAST_LOSSLESS"
    if key in {"JOIN_ASSUMES_RIGHT_PK", "JOIN_RIGHT_PK"}:
        return "JOIN_ASSUMES_RIGHT_PK"
    if key in {"JOIN_FUNCTIONAL_RIGHT", "JOIN_N_TO_1", "JOIN_N1"}:
        return "JOIN_FUNCTIONAL_RIGHT"
    if key in {"ROW_PRESERVE_LEFT", "JOIN_ROW_PRESERVE_LEFT"}:
        return "ROW_PRESERVE_LEFT"
    if key in {"FILTER_ONLY_NULLS", "FILTER_NULL_ONLY", "FILTER_REMOVE_ONLY_NULLS"}:
        return "FILTER_ONLY_NULLS"
    if key in {"FILTER_MIN_RETAIN_RATE", "FILTER_MIN_RETAIN", "FILTER_RETAIN_RATE_MIN"}:
        return "FILTER_MIN_RETAIN_RATE"
    if key in {"UNION_ROW_LOSSLESS", "UNION_LOSSLESS", "UNION_LOSSLESS_ROWS"}:
        return "UNION_ROW_LOSSLESS"
    return key


def _normalize_claim_severity(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"SOFT", "WARN", "WARNING"}:
        return "SOFT"
    if raw in {"HARD", "FAIL", "BLOCK"}:
        return "HARD"
    return "HARD"


# Enterprise Enhancement (2026-01): Explicitly define supported claim kinds
# to detect typos and provide clear warnings for unsupported claims
_SUPPORTED_CLAIM_KINDS: Set[str] = {
    "PK",
    "FK",
    "CAST_SUCCESS",
    "CAST_LOSSLESS",
    "JOIN_ASSUMES_RIGHT_PK",
    "JOIN_FUNCTIONAL_RIGHT",
    "ROW_PRESERVE_LEFT",
    "FILTER_ONLY_NULLS",
    "FILTER_MIN_RETAIN_RATE",
    "UNION_ROW_LOSSLESS",
}

# Claims that CANNOT be hard-gated due to sampling/truncation limitations
_SOFT_ONLY_CLAIM_KINDS: Set[str] = {
    "FK",  # Cannot prove absence in parent set with sampling
    "ROW_PRESERVE_LEFT",  # Affected by sampling/truncation
}


def _find_similar_claim_kinds(target: str, cutoff: float = 0.6) -> List[str]:
    """Find claim kinds similar to the target for helpful error messages."""
    import difflib

    if not target:
        return []

    target_lower = target.lower().replace("-", "_")
    matches = []

    for kind in _SUPPORTED_CLAIM_KINDS:
        kind_lower = kind.lower().replace("-", "_")
        ratio = difflib.SequenceMatcher(None, target_lower, kind_lower).ratio()
        if ratio >= cutoff:
            matches.append((kind, ratio))

    matches.sort(key=lambda x: x[1], reverse=True)
    return [m[0] for m in matches]


def _claim_spec(claim: Dict[str, Any]) -> Dict[str, Any]:
    spec = claim.get("spec")
    return spec if isinstance(spec, dict) else {}


def _claim_target_node_id(claim: Dict[str, Any]) -> Optional[str]:
    target = claim.get("target")
    if isinstance(target, dict):
        for key in ("node_id", "nodeId", "table_node_id", "tableNodeId"):
            value = target.get(key)
            if value:
                return str(value).strip() or None
    return None


def _row_ref(
    row: Dict[str, Any],
    *,
    row_index: int,
    include_columns: List[str],
) -> Dict[str, Any]:
    values = {col: row.get(col) for col in include_columns}
    return {
        "row_index": row_index,
        # Deterministic digest to help reproduce/minimize counterexamples without leaking raw rows.
        "row_digest": f"sha256:{sha256_hex(stable_json_dumps(values))}",
        "values": values,
    }


def _row_digest_for_columns(row: Dict[str, Any], columns: List[str]) -> str:
    projected = {col: row.get(col) for col in columns}
    return f"sha256:{sha256_hex(stable_json_dumps(projected))}"


def _bag_counts_for_table(table: PipelineTable, *, columns: List[str]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in table.rows:
        digest = _row_digest_for_columns(row, columns)
        counts[digest] = counts.get(digest, 0) + 1
    return counts


def _first_row_by_digest(table: PipelineTable, *, columns: List[str], digest: str) -> Optional[Tuple[int, Dict[str, Any]]]:
    for idx, row in enumerate(table.rows):
        if _row_digest_for_columns(row, columns) == digest:
            return idx, row
    return None


def _value_is_null(value: Any, *, treat_empty_as_null: bool, treat_whitespace_as_null: bool) -> bool:
    if value is None:
        return True
    if not isinstance(value, str):
        return False
    if treat_whitespace_as_null and value.strip() == "":
        return True
    if treat_empty_as_null and value == "":
        return True
    return False


def _refute_filter_only_nulls(
    input_table: PipelineTable,
    output_table: PipelineTable,
    *,
    column: str,
    treat_empty_as_null: bool,
    treat_whitespace_as_null: bool,
) -> Optional[Dict[str, Any]]:
    """
    Refute the claim that a filter only removes rows where target column is NULL.

    FIX (2026-01): Use only target column for row identity comparison.
    Previously used all columns, which caused false negatives when other columns
    had different values but the target column was actually NULL.
    """
    col = str(column or "").strip()
    if not col:
        return {
            "description": "FILTER_ONLY_NULLS missing target column",
            "row_refs": [],
            "values": {},
            "unverifiable": True,
        }
    if col not in input_table.columns:
        return {
            "description": "FILTER_ONLY_NULLS column missing from input table",
            "row_refs": [],
            "values": {"column": col},
            "unverifiable": True,
        }

    # FIX: Compare rows by target column value, not entire row digest.
    # This correctly identifies when a non-null value in the target column was removed.
    #
    # Build a set of target column values that exist in output
    output_values: set[tuple[Any, ...]] = set()
    for row in output_table.rows:
        # Use tuple for hashability, include row identity columns if available
        output_values.add((row.get(col),))

    # Check each input row - if removed and target column is NOT NULL, that's a violation
    for idx, row in enumerate(input_table.rows):
        value = row.get(col)

        # Check if this specific row exists in output (by checking if value is preserved)
        # For proper multiset semantics, we track counts
        pass

    # Alternative approach: directly iterate and check removed rows
    # Build multiset of (target_col_value, row_index) for input
    input_value_indices: Dict[Any, List[int]] = {}
    for idx, row in enumerate(input_table.rows):
        val = row.get(col)
        # Normalize for comparison
        val_key = (val,) if val is not None else (None,)
        input_value_indices.setdefault(val_key, []).append(idx)

    output_value_counts: Dict[Any, int] = {}
    for row in output_table.rows:
        val = row.get(col)
        val_key = (val,) if val is not None else (None,)
        output_value_counts[val_key] = output_value_counts.get(val_key, 0) + 1

    # Find removed rows with non-null target column
    for val_key, indices in input_value_indices.items():
        input_count = len(indices)
        output_count = output_value_counts.get(val_key, 0)

        if output_count >= input_count:
            continue  # All rows with this value are preserved

        # Some rows with this value were removed - check if value is NULL
        value = val_key[0]
        if not _value_is_null(
            value,
            treat_empty_as_null=treat_empty_as_null,
            treat_whitespace_as_null=treat_whitespace_as_null,
        ):
            # Non-null value was removed - this is a violation
            # Find a witness row
            witness_idx = indices[0]
            witness_row = input_table.rows[witness_idx]
            return {
                "description": "Filter removed row with non-null value in target column",
                "row_refs": [_row_ref(witness_row, row_index=witness_idx, include_columns=[col])],
                "values": {
                    "column": col,
                    "value": value,
                    "input_count": input_count,
                    "output_count": output_count,
                    "removed_count": input_count - output_count,
                },
            }
    return None


def _refute_filter_min_retain_rate(
    input_table: PipelineTable,
    output_table: PipelineTable,
    *,
    min_rate: float,
) -> Optional[Dict[str, Any]]:
    try:
        threshold = float(min_rate)
    except (TypeError, ValueError):
        threshold = 0.0
    if threshold <= 0:
        return {
            "description": "FILTER_MIN_RETAIN_RATE missing/invalid min_rate",
            "row_refs": [],
            "values": {"min_rate": min_rate},
            "unverifiable": True,
        }
    in_count = len(input_table.rows)
    out_count = len(output_table.rows)
    rate = (out_count / float(in_count)) if in_count > 0 else 1.0
    if rate + 1e-12 < threshold:
        return {
            "description": "Retain rate too low",
            "row_refs": [],
            "values": {"input_rows": in_count, "output_rows": out_count, "retain_rate": round(rate, 6), "min_rate": threshold},
        }
    return None


def _refute_union_row_lossless(
    left: PipelineTable,
    right: PipelineTable,
    output: PipelineTable,
) -> Optional[Dict[str, Any]]:
    # Compare as a multiset on the union output schema.
    cols = list(output.columns)
    if not cols:
        return {
            "description": "UNION_ROW_LOSSLESS output table has no columns",
            "row_refs": [],
            "values": {},
            "unverifiable": True,
        }
    required: Dict[str, int] = {}
    for source in (left, right):
        for row in source.rows:
            digest = _row_digest_for_columns(row, cols)
            required[digest] = required.get(digest, 0) + 1
    actual = _bag_counts_for_table(output, columns=cols)

    for digest, needed in required.items():
        have = actual.get(digest, 0)
        if have >= needed:
            continue
        # Witness: show an input row that should be present but isn't.
        hit = _first_row_by_digest(left, columns=cols, digest=digest) or _first_row_by_digest(right, columns=cols, digest=digest)
        if hit:
            idx, row = hit
            # Keep witness payload small: include non-null columns (up to 12) or fallback to first 5.
            include = [c for c in cols if row.get(c) is not None][:12]
            if not include:
                include = cols[:5]
            return {
                "description": "Row from input missing in UNION output",
                "row_refs": [_row_ref(row, row_index=idx, include_columns=include)],
                "values": {"missing_row_digest": digest, "required_count": needed, "output_count": have},
            }
        return {
            "description": "Row from input missing in UNION output",
            "row_refs": [],
            "values": {"missing_row_digest": digest, "required_count": needed, "output_count": have},
        }
    return None


def _refute_pk(
    table: PipelineTable,
    *,
    key_cols: List[str],
    require_not_null: bool,
) -> Optional[Dict[str, Any]]:
    if not key_cols:
        return {
            "description": "PK claim missing key_cols",
            "row_refs": [],
            "values": {"key_cols": []},
            "unverifiable": True,
        }
    missing_cols = [col for col in key_cols if col not in table.columns]
    if missing_cols:
        return {
            "description": "PK key column(s) missing from table",
            "row_refs": [],
            "values": {"missing_columns": missing_cols, "key_cols": key_cols},
            "unverifiable": True,
        }

    seen: Dict[Tuple[Any, ...], int] = {}
    for idx, row in enumerate(table.rows):
        key = tuple(row.get(col) for col in key_cols)
        if require_not_null and any(value is None for value in key):
            return {
                "description": "PK has NULL in key",
                "row_refs": [_row_ref(row, row_index=idx, include_columns=key_cols)],
                "values": {"key_cols": key_cols, "key": list(key)},
            }
        if key in seen:
            other_idx = seen[key]
            other = table.rows[other_idx] if 0 <= other_idx < len(table.rows) else {}
            return {
                "description": "Duplicate PK key found",
                "row_refs": [
                    _row_ref(other, row_index=other_idx, include_columns=key_cols),
                    _row_ref(row, row_index=idx, include_columns=key_cols),
                ],
                "values": {"key_cols": key_cols, "key": list(key)},
            }
        seen[key] = idx
    return None


def _refute_join_functional_right(
    left: PipelineTable,
    right: PipelineTable,
    *,
    left_keys: List[str],
    right_keys: List[str],
) -> Optional[Dict[str, Any]]:
    """
    Refute the claim that the join is functional on the right side (N:1 relationship).

    A functional join means: for any left key value, there is at most one matching right row.
    This is equivalent to saying the right join keys form a unique key on the right table
    (at least for the subset of keys that appear in the left table).

    FIX (2026-01): Check for right-side duplicates regardless of left-side match.
    The functional dependency is a property of the right table's key uniqueness,
    not dependent on whether left has matching rows.
    """
    if not left_keys or not right_keys or len(left_keys) != len(right_keys):
        return {
            "description": "JOIN_FUNCTIONAL_RIGHT missing or mismatched join keys",
            "row_refs": [],
            "values": {"left_keys": left_keys, "right_keys": right_keys},
            "unverifiable": True,
        }
    missing_left = [col for col in left_keys if col not in left.columns]
    missing_right = [col for col in right_keys if col not in right.columns]
    if missing_left or missing_right:
        return {
            "description": "JOIN_FUNCTIONAL_RIGHT join key column(s) missing",
            "row_refs": [],
            "values": {"missing_left": missing_left, "missing_right": missing_right},
            "unverifiable": True,
        }

    # FIX: First check if right table has duplicate keys (regardless of left matches)
    # This is the core functional dependency check
    first_right: Dict[Tuple[Any, ...], int] = {}
    for ri, row in enumerate(right.rows):
        k = tuple(row.get(col) for col in right_keys)
        if any(v is None for v in k):
            continue
        if k in first_right:
            # Duplicate key found in right table - this violates functional dependency
            r1 = first_right[k]
            right_row_1 = right.rows[r1] if 0 <= r1 < len(right.rows) else {}
            return {
                "description": "Non-functional join: right table has duplicate keys",
                "row_refs": [
                    {"side": "right", **_row_ref(right_row_1, row_index=r1, include_columns=right_keys)},
                    {"side": "right", **_row_ref(row, row_index=ri, include_columns=right_keys)},
                ],
                "values": {
                    "join_key": list(k),
                    "left_keys": left_keys,
                    "right_keys": right_keys,
                    "note": "Right table has duplicate values for join key, violating N:1 assumption",
                },
            }
        first_right[k] = ri

    # Optional: Also check if there are left keys that would match multiple right rows
    # (This is already covered by the above check, but we can provide more context)

    return None


def _refute_cast_success(
    table: PipelineTable,
    casts: List[Dict[str, Any]],
    *,
    only_columns: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    requested = set(_normalize_string_list(only_columns)) if only_columns else None
    cast_specs: List[Tuple[str, str]] = []
    for item in casts or []:
        if not isinstance(item, dict):
            continue
        col = str(item.get("column") or "").strip()
        target = str(item.get("type") or "").strip()
        if not col or not target:
            continue
        if requested is not None and col not in requested:
            continue
        cast_specs.append((col, target))

    if not cast_specs:
        return {
            "description": "CAST_SUCCESS has no casts to check",
            "row_refs": [],
            "values": {"casts": casts},
            "unverifiable": True,
        }

    for col, target in cast_specs:
        if col not in table.columns:
            return {
                "description": "CAST_SUCCESS column missing from table",
                "row_refs": [],
                "values": {"column": col, "target_type": target},
                "unverifiable": True,
            }

    for idx, row in enumerate(table.rows):
        for col, target in cast_specs:
            value = row.get(col)
            _, attempted, failed = _cast_value_with_status(value, target, cast_mode="SAFE_NULL")
            if attempted and failed:
                return {
                    "description": "Cast parse failed",
                    "row_refs": [_row_ref(row, row_index=idx, include_columns=[col])],
                    "values": {"column": col, "target_type": target, "value": value},
                }
    return None


_CAST_LOSSLESS_SUPPORTED_NORMALIZATIONS = {
    "trim",
    "lowercase",
    "uppercase",
    "collapse_whitespace",
    "strip_leading_zeros",
    "strip_trailing_zeros",
    "normalize_integer",
    "normalize_decimal",
    "normalize_datetime",
    "normalize_date",
}


def _normalize_allowed_normalization(value: Any) -> Tuple[List[str], List[str]]:
    rules = _normalize_string_list(value)
    normalized: List[str] = []
    unknown: List[str] = []
    for raw in rules:
        key = str(raw or "").strip().lower().replace("-", "_")
        if key in {"strip", "trim"}:
            key = "trim"
        if key in {"lower", "lower_case"}:
            key = "lowercase"
        if key in {"upper", "upper_case"}:
            key = "uppercase"
        if key in {"collapse_ws", "collapse_space", "collapse_spaces"}:
            key = "collapse_whitespace"
        if key in {"strip_leading_zero", "strip_leading_zeroes"}:
            key = "strip_leading_zeros"
        if key in {"strip_trailing_zero", "strip_trailing_zeroes"}:
            key = "strip_trailing_zeros"
        if key in {"normalize_int", "normalize_integer"}:
            key = "normalize_integer"
        if key in {"normalize_float", "normalize_number", "normalize_decimal"}:
            key = "normalize_decimal"
        if key in {"normalize_dt", "normalize_datetime"}:
            key = "normalize_datetime"
        if key in {"normalize_date"}:
            key = "normalize_date"

        if key not in _CAST_LOSSLESS_SUPPORTED_NORMALIZATIONS:
            unknown.append(str(raw))
            continue
        normalized.append(key)
    return normalized, unknown


def _serialize_canonical_decimal(value: float) -> str:
    """
    Deterministic, non-scientific canonical form with bounded precision.
    This intentionally does NOT preserve trailing zeros (information loss)
    unless the agent allows it via normalization.

    FIX (2026-01): Handle edge cases that cause overflow or unexpected output:
    - Very large floats (>1e15) that overflow .15f format
    - Infinity and NaN values
    - Subnormal numbers near zero
    """
    import math

    # Handle special float values
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"

    # For very large numbers, use scientific notation threshold
    # .15f format can produce extremely long strings for large numbers
    abs_val = abs(value)
    if abs_val >= 1e15:
        # Use repr which handles large floats correctly, then normalize
        text = repr(value)
        # Try to parse and reformat if it's in scientific notation
        if "e" in text.lower():
            # Keep scientific notation for very large numbers
            return text
        # Otherwise strip trailing zeros
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text if text not in {"", "-0"} else "0"

    # For very small subnormal numbers, also use repr
    if 0 < abs_val < 1e-15:
        text = repr(value)
        if "e" in text.lower():
            return text
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text if text not in {"", "-0"} else "0"

    # Normal range: use fixed-point notation
    text = format(float(value), ".15f")
    text = text.rstrip("0").rstrip(".")
    if text in {"", "-0"}:
        return "0"
    return text


def _serialize_roundtrip(value: Any, *, target_type: str) -> str:
    normalized = normalize_cast_target(target_type)
    if value is None:
        return ""
    if normalized == "xsd:string":
        if isinstance(value, (dict, list)):
            return stable_json_dumps(value)
        if isinstance(value, tuple):
            return stable_json_dumps(list(value))
        return str(value)
    if normalized == "xsd:boolean":
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value).strip().lower()
    if normalized == "xsd:integer":
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value).strip()
    if normalized == "xsd:decimal":
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float):
            return _serialize_canonical_decimal(value)
        # Fallback: treat as string.
        return str(value).strip()
    if normalized == "xsd:date":
        if isinstance(value, date) and not isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, datetime):
            return value.date().isoformat()
        return str(value).strip()
    if normalized == "xsd:dateTime":
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value).strip()
    return str(value).strip()


def _apply_lossless_normalization(text: str, rules: List[str]) -> str:
    out = str(text or "")
    for rule in rules:
        if rule == "trim":
            out = out.strip()
            continue
        if rule == "lowercase":
            out = out.lower()
            continue
        if rule == "uppercase":
            out = out.upper()
            continue
        if rule == "collapse_whitespace":
            out = re.sub(r"\s+", " ", out).strip()
            continue
        if rule == "strip_leading_zeros":
            match = re.match(r"^([+-]?)(0+)(\d+)$", out)
            if match:
                sign = match.group(1) or ""
                digits = match.group(3).lstrip("0") or "0"
                out = f"{sign}{digits}"
            continue
        if rule == "strip_trailing_zeros":
            match = re.match(r"^([+-]?\d+)\.(\d+)$", out)
            if match:
                head = match.group(1)
                tail = match.group(2).rstrip("0")
                out = f"{head}.{tail}" if tail else head
            continue
        if rule == "normalize_integer":
            parsed = parse_int_text(out)
            if parsed is not None:
                out = str(parsed)
            continue
        if rule == "normalize_decimal":
            parsed = parse_decimal_text(out)
            if parsed is not None:
                out = _serialize_canonical_decimal(float(parsed))
            continue
        if rule == "normalize_datetime":
            parsed = parse_datetime_text(out, allow_ambiguous=True)
            if parsed is not None:
                out = parsed.isoformat()
            continue
        if rule == "normalize_date":
            parsed = parse_datetime_text(out, allow_ambiguous=True)
            if parsed is not None:
                out = parsed.date().isoformat()
            continue
    return out


def _refute_cast_lossless(
    input_table: PipelineTable,
    output_table: PipelineTable,
    casts: List[Dict[str, Any]],
    *,
    allowed_normalization: Any,
    only_columns: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    normalized_rules, unknown_rules = _normalize_allowed_normalization(allowed_normalization)
    if unknown_rules:
        return {
            "description": "CAST_LOSSLESS has unsupported normalization rule(s)",
            "row_refs": [],
            "values": {"unknown_rules": unknown_rules, "supported_rules": sorted(_CAST_LOSSLESS_SUPPORTED_NORMALIZATIONS)},
            "unverifiable": True,
        }
    if not normalized_rules:
        return {
            "description": "CAST_LOSSLESS requires allowed_normalization",
            "row_refs": [],
            "values": {"allowed_normalization": []},
            "unverifiable": True,
        }

    requested = set(_normalize_string_list(only_columns)) if only_columns else None
    cast_specs: List[Tuple[str, str]] = []
    for item in casts or []:
        if not isinstance(item, dict):
            continue
        col = str(item.get("column") or "").strip()
        target = str(item.get("type") or "").strip()
        if not col or not target:
            continue
        if requested is not None and col not in requested:
            continue
        cast_specs.append((col, target))

    if not cast_specs:
        return {
            "description": "CAST_LOSSLESS has no casts to check",
            "row_refs": [],
            "values": {"casts": casts},
            "unverifiable": True,
        }

    missing_cols = [col for col, _ in cast_specs if col not in input_table.columns or col not in output_table.columns]
    if missing_cols:
        return {
            "description": "CAST_LOSSLESS column missing from input/output table",
            "row_refs": [],
            "values": {"missing_columns": sorted(set(missing_cols))},
            "unverifiable": True,
        }

    # Cast transform preserves row ordering in the executor; compare row-by-row.
    pair_count = min(len(input_table.rows), len(output_table.rows))
    for idx in range(pair_count):
        in_row = input_table.rows[idx]
        out_row = output_table.rows[idx]
        for col, target in cast_specs:
            original_value = in_row.get(col)
            if original_value is None:
                continue
            original_text = str(original_value) if not isinstance(original_value, bool) else str(original_value)
            original_norm = _apply_lossless_normalization(original_text, normalized_rules)

            # If the cast itself fails, that's an immediate counterexample for losslessness.
            _, attempted, failed = _cast_value_with_status(original_value, target, cast_mode="SAFE_NULL")
            if attempted and failed:
                return {
                    "description": "Cast parse failed",
                    "row_refs": [_row_ref(in_row, row_index=idx, include_columns=[col])],
                    "values": {
                        "column": col,
                        "target_type": normalize_cast_target(target),
                        "allowed_normalization": normalized_rules,
                        "original": original_text,
                    },
                }

            roundtrip_text = _serialize_roundtrip(out_row.get(col), target_type=target)
            roundtrip_norm = _apply_lossless_normalization(roundtrip_text, normalized_rules)
            if original_norm != roundtrip_norm:
                return {
                    "description": "Round-trip mismatch (information loss)",
                    "row_refs": [_row_ref(in_row, row_index=idx, include_columns=[col])],
                    "values": {
                        "column": col,
                        "target_type": normalize_cast_target(target),
                        "allowed_normalization": normalized_rules,
                        "original": original_text,
                        "normalized_original": original_norm,
                        "roundtrip": roundtrip_text,
                        "normalized_roundtrip": roundtrip_norm,
                    },
                }
    return None


def _extract_claims(definition_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    nodes = definition_json.get("nodes")
    if not isinstance(nodes, list):
        return []
    claims: List[Dict[str, Any]] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_id = str(node.get("id") or "").strip()
        if not node_id:
            continue
        meta = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
        raw_claims = meta.get("claims")
        if not isinstance(raw_claims, list):
            continue
        for idx, item in enumerate(raw_claims):
            if not isinstance(item, dict):
                continue
            claim = dict(item)
            claim_id = str(claim.get("id") or "").strip()
            kind = _normalize_claim_kind(claim.get("kind") or claim.get("type") or "")
            if not claim_id:
                base = kind or "claim"
                claim_id = f"{node_id}:{base}:{idx + 1}"
            claim["id"] = claim_id
            claim["_node_id"] = node_id
            claim["_kind"] = kind
            claim["_severity"] = _normalize_claim_severity(claim.get("severity"))
            claims.append(claim)
    claims.sort(key=lambda c: (str(c.get("_node_id") or ""), str(c.get("id") or "")))
    return claims


async def refute_pipeline_plan_claims(
    *,
    plan: PipelinePlan,
    dataset_registry: Optional[DatasetRegistry] = None,
    run_tables: Optional[Dict[str, Any]] = None,
    sample_limit: int = 400,
    max_output_rows: int = 20000,
    max_hard_failures: int = 5,
    max_soft_warnings: int = 20,
) -> Dict[str, Any]:
    """
    Returns:
      {
        status: "success" | "invalid",
        gate: "PASS_NOT_REFUTED" | "BLOCK",
        hard_failures: [Violation...],
        soft_warnings: [Violation...],
        errors: string[],
        warnings: string[],
        stats: {...}
      }
    """
    definition_json = dict(plan.definition_json or {})
    nodes = normalize_nodes(definition_json.get("nodes"))
    edges = normalize_edges(definition_json.get("edges"))
    incoming = build_incoming(edges)

    claims = _extract_claims(definition_json)
    warnings: List[str] = []
    if not claims:
        return {
            "status": "success",
            "gate": "PASS_NOT_REFUTED",
            "hard_failures": [],
            "soft_warnings": [],
            "errors": [],
            # No claims is normal; the refuter is opt-in via claims.
            "warnings": [],
            "stats": {"claims_total": 0},
        }

    tables: Dict[str, PipelineTable] = {}
    if run_tables is not None:
        # run_tables format is compatible with PipelineExecutor preview payload:
        # { node_id: { columns: [...], rows: [...] } }
        for node_id, payload in (run_tables or {}).items():
            if not isinstance(node_id, str) or not node_id.strip():
                continue
            if not isinstance(payload, dict):
                continue
            cols_raw = payload.get("columns")
            rows_raw = payload.get("rows")
            if not isinstance(cols_raw, list) or not isinstance(rows_raw, list):
                continue
            cols: List[str] = []
            for col in cols_raw:
                if isinstance(col, dict):
                    name = str(col.get("name") or "").strip()
                else:
                    name = str(col or "").strip()
                if name:
                    cols.append(name)
            rows: List[Dict[str, Any]] = [dict(row) for row in rows_raw if isinstance(row, dict)]
            tables[str(node_id)] = PipelineTable(columns=cols, rows=rows)
    else:
        # Fail open: inability to evaluate is never a witness.
        if not dataset_registry:
            return mask_pii(
                {
                    "status": "success",
                    "gate": "PASS_NOT_REFUTED",
                    "hard_failures": [],
                    "soft_warnings": [],
                    "errors": [],
                    "warnings": ["refuter skipped: dataset_registry is required when run_tables is not provided"],
                    "stats": {"claims_total": len(claims), "claims_checked": 0, "hard_failures": 0, "soft_warnings": 0},
                }
            )
        db_name = str(plan.data_scope.db_name or "").strip()
        if not db_name:
            return mask_pii(
                {
                    "status": "success",
                    "gate": "PASS_NOT_REFUTED",
                    "hard_failures": [],
                    "soft_warnings": [],
                    "errors": [],
                    "warnings": ["refuter skipped: plan.data_scope.db_name is required"],
                    "stats": {"claims_total": len(claims), "claims_checked": 0, "hard_failures": 0, "soft_warnings": 0},
                }
            )
        executor = PipelineExecutor(dataset_registry)
        definition_for_run = dict(definition_json)
        preview_meta = dict(definition_for_run.get("__preview_meta__") or {})
        preview_meta.setdefault("branch", str(plan.data_scope.branch or "") or "master")
        try:
            preview_meta["sample_limit"] = max(1, min(int(sample_limit or 0), 1200))
        except (TypeError, ValueError):
            preview_meta["sample_limit"] = 400
        try:
            preview_meta["max_output_rows"] = max(1, min(int(max_output_rows or 0), 250000))
        except (TypeError, ValueError):
            preview_meta["max_output_rows"] = 20000
        definition_for_run["__preview_meta__"] = preview_meta
        try:
            run_result = await executor.run(definition=definition_for_run, db_name=db_name)
        except Exception as exc:
            # Fail open: preview execution errors are not counterexamples.
            logging.getLogger(__name__).warning("Exception fallback at shared/services/pipeline/pipeline_claim_refuter.py:958", exc_info=True)
            return mask_pii(
                {
                    "status": "success",
                    "gate": "PASS_NOT_REFUTED",
                    "hard_failures": [],
                    "soft_warnings": [],
                    "errors": [],
                    "warnings": [f"refuter skipped: preview run failed ({exc})"],
                    "stats": {"claims_total": len(claims), "claims_checked": 0, "hard_failures": 0, "soft_warnings": 0},
                }
            )
        tables = dict(run_result.tables or {})

    hard_failures: List[Dict[str, Any]] = []
    soft_warnings: List[Dict[str, Any]] = []
    checked = 0

    for claim in claims:
        if len(hard_failures) >= max(0, int(max_hard_failures or 0)):
            warnings.append("hard failure limit reached; remaining claims not checked")
            break

        claim_id = str(claim.get("id") or "").strip() or "claim"
        kind = str(claim.get("_kind") or "")
        severity = str(claim.get("_severity") or "HARD")
        node_id = _claim_target_node_id(claim) or str(claim.get("_node_id") or "").strip()
        spec = _claim_spec(claim)

        checked += 1
        witness: Optional[Dict[str, Any]] = None
        unsound_hard = False

        if kind == "PK":
            key_cols = _normalize_string_list(spec.get("key_cols") or spec.get("keyCols") or spec.get("columns"))
            require_not_null = True if "require_not_null" not in spec and "requireNotNull" not in spec else bool(
                spec.get("require_not_null", spec.get("requireNotNull"))
            )
            table = tables.get(node_id)
            if not table:
                witness = {
                    "description": "Target table not available for PK check",
                    "row_refs": [],
                    "values": {"node_id": node_id},
                    "unverifiable": True,
                }
            else:
                witness = _refute_pk(table, key_cols=key_cols, require_not_null=require_not_null)

        elif kind in {"JOIN_ASSUMES_RIGHT_PK", "JOIN_FUNCTIONAL_RIGHT", "ROW_PRESERVE_LEFT"}:
            node = nodes.get(node_id) if node_id else None
            meta = node.get("metadata") if isinstance(node, dict) else {}
            operation = normalize_operation(meta.get("operation"))
            if operation != "join":
                witness = {
                    "description": f"{kind} applied to non-join node",
                    "row_refs": [],
                    "values": {"node_id": node_id, "operation": operation},
                    "unverifiable": True,
                }
            else:
                join_spec = resolve_join_spec(meta or {})
                inputs = incoming.get(node_id, [])
                left_id = inputs[0] if len(inputs) >= 1 else None
                right_id = inputs[1] if len(inputs) >= 2 else None
                if not left_id or not right_id:
                    witness = {
                        "description": "Join node missing inputs",
                        "row_refs": [],
                        "values": {"node_id": node_id, "inputs": inputs},
                        "unverifiable": True,
                    }
                else:
                    left_table = tables.get(left_id)
                    right_table = tables.get(right_id)
                    if not left_table or not right_table:
                        witness = {
                            "description": "Join input table(s) not available",
                            "row_refs": [],
                            "values": {"left_id": left_id, "right_id": right_id},
                            "unverifiable": True,
                        }
                    else:
                        left_keys = list(join_spec.left_keys or [])
                        right_keys = list(join_spec.right_keys or [])
                        if kind == "JOIN_ASSUMES_RIGHT_PK":
                            require_not_null = True if "require_not_null" not in spec and "requireNotNull" not in spec else bool(
                                spec.get("require_not_null", spec.get("requireNotNull"))
                            )
                            witness = _refute_pk(right_table, key_cols=right_keys, require_not_null=require_not_null)
                        elif kind == "JOIN_FUNCTIONAL_RIGHT":
                            witness = _refute_join_functional_right(
                                left_table,
                                right_table,
                                left_keys=left_keys,
                                right_keys=right_keys,
                            )
                        else:
                            # ROW_PRESERVE_LEFT is tricky under sampling + max_output_rows truncation.
                            # Only provide SOFT guidance unless the agent explicitly sets severity=SOFT.
                            unsound_hard = True
                            witness = {
                                "description": "ROW_PRESERVE_LEFT is not hard-refutable under sampling/truncation; treated as soft",
                                "row_refs": [],
                                "values": {"node_id": node_id},
                                "unverifiable": True,
                            }

        elif kind in {"CAST_SUCCESS", "CAST_LOSSLESS"}:
            node = nodes.get(node_id) if node_id else None
            meta = node.get("metadata") if isinstance(node, dict) else {}
            operation = normalize_operation(meta.get("operation"))
            if operation != "cast":
                witness = {
                    "description": f"{kind} applied to non-cast node",
                    "row_refs": [],
                    "values": {"node_id": node_id, "operation": operation},
                    "unverifiable": True,
                }
            else:
                inputs = incoming.get(node_id, [])
                src_id = inputs[0] if inputs else None
                src_table = tables.get(src_id) if src_id else None
                out_table = tables.get(node_id) if node_id else None
                if not src_table:
                    witness = {
                        "description": "Cast input table not available",
                        "row_refs": [],
                        "values": {"node_id": node_id, "input": src_id},
                        "unverifiable": True,
                    }
                else:
                    casts = meta.get("casts") if isinstance(meta.get("casts"), list) else []
                    only_columns = _normalize_string_list(spec.get("columns") or spec.get("cols")) if spec else None
                    if kind == "CAST_SUCCESS":
                        witness = _refute_cast_success(src_table, casts, only_columns=only_columns)
                    else:
                        if not out_table:
                            witness = {
                                "description": "Cast output table not available",
                                "row_refs": [],
                                "values": {"node_id": node_id},
                                "unverifiable": True,
                            }
                        else:
                            witness = _refute_cast_lossless(
                                src_table,
                                out_table,
                                casts,
                                allowed_normalization=spec.get("allowed_normalization", spec.get("allowedNormalization")),
                                only_columns=only_columns,
                            )

        elif kind in {"FILTER_ONLY_NULLS", "FILTER_MIN_RETAIN_RATE"}:
            node = nodes.get(node_id) if node_id else None
            meta = node.get("metadata") if isinstance(node, dict) else {}
            operation = normalize_operation(meta.get("operation"))
            if operation != "filter":
                witness = {
                    "description": f"{kind} applied to non-filter node",
                    "row_refs": [],
                    "values": {"node_id": node_id, "operation": operation},
                    "unverifiable": True,
                }
            else:
                inputs = incoming.get(node_id, [])
                src_id = inputs[0] if inputs else None
                input_table = tables.get(src_id) if src_id else None
                output_table = tables.get(node_id) if node_id else None
                if not input_table or not output_table:
                    witness = {
                        "description": "Filter input/output table not available",
                        "row_refs": [],
                        "values": {"node_id": node_id, "input": src_id},
                        "unverifiable": True,
                    }
                else:
                    if kind == "FILTER_ONLY_NULLS":
                        col = str(spec.get("column") or spec.get("col") or spec.get("target_column") or spec.get("targetColumn") or "").strip()
                        treat_empty_as_null = bool(
                            spec.get("treat_empty_as_null", spec.get("empty_is_null", spec.get("emptyIsNull", False)))
                        )
                        treat_whitespace_as_null = bool(
                            spec.get(
                                "treat_whitespace_as_null",
                                spec.get("whitespace_is_null", spec.get("whitespaceIsNull", False)),
                            )
                        )
                        witness = _refute_filter_only_nulls(
                            input_table,
                            output_table,
                            column=col,
                            treat_empty_as_null=treat_empty_as_null,
                            treat_whitespace_as_null=treat_whitespace_as_null,
                        )
                    else:
                        witness = _refute_filter_min_retain_rate(
                            input_table,
                            output_table,
                            min_rate=spec.get("min_rate", spec.get("minRate", 0.0)),
                        )

        elif kind == "UNION_ROW_LOSSLESS":
            node = nodes.get(node_id) if node_id else None
            meta = node.get("metadata") if isinstance(node, dict) else {}
            operation = normalize_operation(meta.get("operation"))
            if operation != "union":
                witness = {
                    "description": "UNION_ROW_LOSSLESS applied to non-union node",
                    "row_refs": [],
                    "values": {"node_id": node_id, "operation": operation},
                    "unverifiable": True,
                }
            else:
                inputs = incoming.get(node_id, [])
                left_id = inputs[0] if len(inputs) >= 1 else None
                right_id = inputs[1] if len(inputs) >= 2 else None
                left_table = tables.get(left_id) if left_id else None
                right_table = tables.get(right_id) if right_id else None
                out_table = tables.get(node_id) if node_id else None
                if not left_table or not right_table or not out_table:
                    witness = {
                        "description": "Union input/output table(s) not available",
                        "row_refs": [],
                        "values": {"node_id": node_id, "inputs": inputs},
                        "unverifiable": True,
                    }
                else:
                    witness = _refute_union_row_lossless(left_table, right_table, out_table)

        elif kind == "FK":
            # Enterprise Enhancement (2026-01): Provide clear explanation for FK soft-gating
            # FK "orphan" checks are not sound under head-sampling. We can emit SOFT hints only.
            unsound_hard = True

            # Log explicit warning if user specified HARD severity
            if severity == "HARD":
                logger.warning(
                    f"FK CLAIM DOWNGRADED TO SOFT: Claim '{claim_id}' was specified as HARD, "
                    "but FK claims cannot be hard-gated under sampling. This is because sample-based "
                    "verification cannot prove that a foreign key value exists in the parent table - "
                    "we can only check the sampled rows. Use explicit parent key lookup for production validation."
                )

            witness = {
                "description": "FK refutation is sample-based and not hard-gated (cannot prove absence in parent set)",
                "row_refs": [],
                "values": {
                    "note": "treated as soft unless a full-scan membership oracle exists",
                    "original_severity": severity,
                    "reason": "FK claims require full parent table scan to verify membership, which is not available in preview mode",
                    "recommendation": "For production, use explicit FK constraint enforcement in the target database",
                },
                "unverifiable": True,
            }

        else:
            # Enterprise Enhancement (2026-01): Improved unknown claim handling with suggestions
            unsound_hard = True

            # Find similar claim kinds for helpful suggestions
            similar_kinds = _find_similar_claim_kinds(kind)
            suggestion = ""
            if similar_kinds:
                suggestion = f" Did you mean: {', '.join(similar_kinds[:3])}?"

            logger.warning(
                f"UNKNOWN CLAIM KIND: '{kind}' is not a supported claim type. "
                f"Supported types: {sorted(_SUPPORTED_CLAIM_KINDS)}.{suggestion}"
            )

            witness = {
                "description": f"Unknown/unsupported claim kind: '{kind}'",
                "row_refs": [],
                "values": {
                    "kind": kind,
                    "supported_kinds": sorted(_SUPPORTED_CLAIM_KINDS),
                    "similar_suggestions": similar_kinds[:3] if similar_kinds else [],
                    "note": "This claim was not evaluated. Check for typos or use a supported claim kind.",
                },
                "unverifiable": True,
            }

        if witness is None:
            continue

        if isinstance(witness, dict) and witness.get("unverifiable"):
            unsound_hard = True

        violation = {
            "claim_id": claim_id,
            "kind": kind,
            "severity": "HARD_FAIL" if (severity == "HARD" and not unsound_hard) else "SOFT_WARN",
            "witness": witness,
            "hint": claim.get("hint") if isinstance(claim.get("hint"), dict) else None,
        }

        if severity == "HARD" and not unsound_hard:
            hard_failures.append(violation)
        else:
            soft_warnings.append(violation)
            if len(soft_warnings) >= max(0, int(max_soft_warnings or 0)):
                warnings.append("soft warning limit reached; remaining soft issues not reported")
                break

    gate = "BLOCK" if hard_failures else "PASS_NOT_REFUTED"
    status = "invalid" if hard_failures else "success"
    errors = [f"{v.get('claim_id')}: {((v.get('witness') or {}).get('description') or '')}" for v in hard_failures][:20]

    # Enterprise Enhancement (2026-01): Add sample-based verification limitations warning
    if status == "success" and checked > 0:
        warnings.append(
            "SAMPLE-BASED VERIFICATION: Claims were checked against sampled data only. "
            f"Passing {checked} checks does NOT guarantee correctness on full dataset. "
            "For production pipelines, implement runtime validation on actual data."
        )

    # Count soft-only claims that were downgraded
    downgraded_claims = [
        c for c in claims
        if c.get("_kind") in _SOFT_ONLY_CLAIM_KINDS and c.get("_severity") == "HARD"
    ]
    if downgraded_claims:
        downgraded_kinds = set(c.get("_kind") for c in downgraded_claims)
        warnings.append(
            f"SEVERITY DOWNGRADE: {len(downgraded_claims)} claim(s) with kind {sorted(downgraded_kinds)} "
            "were specified as HARD but downgraded to SOFT because these claim types cannot be "
            "reliably verified with sample data."
        )

    payload = {
        "status": status,
        "gate": gate,
        "hard_failures": hard_failures,
        "soft_warnings": soft_warnings,
        "errors": errors,
        "warnings": warnings,
        "stats": {
            "claims_total": len(claims),
            "claims_checked": checked,
            "hard_failures": len(hard_failures),
            "soft_warnings": len(soft_warnings),
            "downgraded_to_soft": len(downgraded_claims),
        },
        # Enterprise Enhancement: Include verification scope disclaimer
        "verification_scope": {
            "mode": "sample_based",
            "disclaimer": "Results are based on sampled data and do not guarantee correctness on full dataset",
            "supported_hard_gate_kinds": sorted(_SUPPORTED_CLAIM_KINDS - _SOFT_ONLY_CLAIM_KINDS),
            "soft_only_kinds": sorted(_SOFT_ONLY_CLAIM_KINDS),
        },
    }
    return mask_pii(payload)
