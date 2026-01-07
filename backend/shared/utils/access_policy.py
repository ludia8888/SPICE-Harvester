"""
Access policy evaluation helpers (row/column masking).
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple


_SUPPORTED_OPS = {
    "eq",
    "ne",
    "gt",
    "gte",
    "lt",
    "lte",
    "in",
    "not_in",
    "contains",
    "is_null",
    "not_null",
}


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _coerce_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    return None


def _match_rule(value: Any, *, op: str, expected: Any) -> bool:
    op = op.lower()
    if op not in _SUPPORTED_OPS:
        return False
    if op == "is_null":
        return value is None
    if op == "not_null":
        return value is not None
    if op == "eq":
        return value == expected
    if op == "ne":
        return value != expected
    if op == "gt":
        return value is not None and expected is not None and value > expected
    if op == "gte":
        return value is not None and expected is not None and value >= expected
    if op == "lt":
        return value is not None and expected is not None and value < expected
    if op == "lte":
        return value is not None and expected is not None and value <= expected
    if op == "in":
        return value in _as_list(expected)
    if op == "not_in":
        return value not in _as_list(expected)
    if op == "contains":
        if value is None:
            return False
        if isinstance(value, (list, tuple, set)):
            return expected in value
        return str(expected) in str(value)
    return False


def _match_filters(
    row: Dict[str, Any],
    *,
    filters: List[Dict[str, Any]],
    operator: str,
) -> bool:
    if not filters:
        return True
    operator = operator.lower()
    if operator not in {"and", "or"}:
        operator = "and"

    matches: List[bool] = []
    for rule in filters:
        if not isinstance(rule, dict):
            matches.append(False)
            continue
        field = str(rule.get("field") or "").strip()
        op = str(rule.get("op") or rule.get("operator") or "").strip().lower()
        expected = rule.get("value")
        if not field or not op:
            matches.append(False)
            continue
        value = row.get(field)
        matches.append(_match_rule(value, op=op, expected=expected))

    if operator == "or":
        return any(matches)
    return all(matches)


def _apply_mask(
    row: Dict[str, Any],
    *,
    columns: Iterable[str],
    mask_value: Any,
) -> Tuple[Dict[str, Any], int]:
    masked = 0
    output = dict(row)
    for col in columns:
        key = str(col or "").strip()
        if not key:
            continue
        if key in output:
            output[key] = mask_value
            masked += 1
    return output, masked


def apply_access_policy(
    rows: List[Dict[str, Any]],
    *,
    policy: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Apply a row/column policy to result rows.

    Policy schema (minimal):
      {
        "row_filters": [{"field": "status", "op": "eq", "value": "active"}],
        "filter_operator": "and",
        "filter_mode": "allow",  # allow|deny
        "mask_columns": ["email", "phone"],
        "mask_value": null
      }
    """

    if not rows:
        return rows, {"filtered": 0, "masked_fields": 0}

    filters = policy.get("row_filters") or policy.get("filters") or []
    if not isinstance(filters, list):
        filters = []
    operator = str(policy.get("filter_operator") or "and").strip().lower()
    mode = str(policy.get("filter_mode") or "allow").strip().lower()
    if mode not in {"allow", "deny"}:
        mode = "allow"

    mask_columns = policy.get("mask_columns") or []
    if isinstance(mask_columns, str):
        mask_columns = [c.strip() for c in mask_columns.split(",") if c.strip()]
    if not isinstance(mask_columns, list):
        mask_columns = []
    mask_value = policy.get("mask_value")
    if _coerce_bool(mask_value) is None and mask_value is None:
        mask_value = None

    filtered_rows: List[Dict[str, Any]] = []
    filtered_out = 0
    masked_fields = 0

    for row in rows:
        if not isinstance(row, dict):
            continue
        matches = _match_filters(row, filters=filters, operator=operator)
        allowed = matches if mode == "allow" else not matches
        if not allowed:
            filtered_out += 1
            continue
        masked_row, masked = _apply_mask(row, columns=mask_columns, mask_value=mask_value)
        masked_fields += masked
        filtered_rows.append(masked_row)

    return filtered_rows, {"filtered": filtered_out, "masked_fields": masked_fields}

