"""Pipeline plan preview utilities (BFF).

Extracted from `bff.routers.pipeline_plans_ops` to isolate preview-specific
helpers (digesting + sanitization) from routing concerns.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

from shared.security.input_sanitizer import InputSanitizer, SecurityViolationError, sanitize_input
from shared.utils.canonical_json import sha256_canonical_json_prefixed

_PREVIEW_SANITIZER = InputSanitizer()
_SYS_TS_EXPR_RE = re.compile(r"^\\s*(?P<col>_sys_ingested_at|_sys_valid_from)\\s*=\\s*to_timestamp\\('.*'\\)\\s*$")

# Join sampling needs to be large enough to avoid false "empty join" results,
# but small enough to keep preview runs lightweight in dev.
_JOIN_SAMPLE_MIN_ROWS = 800
_JOIN_SAMPLE_MULTI_MIN_ROWS = 5000
_PREVIEW_MAX_OUTPUT_ROWS = 20000


def _definition_has_join(definition_json: Dict[str, Any]) -> bool:
    nodes = definition_json.get("nodes")
    if not isinstance(nodes, list):
        return False
    for node in nodes:
        if not isinstance(node, dict):
            continue
        if str(node.get("type") or "").strip().lower() != "transform":
            continue
        metadata = node.get("metadata")
        if not isinstance(metadata, dict):
            continue
        if str(metadata.get("operation") or "").strip().lower() == "join":
            return True
    return False


def _definition_join_count(definition_json: Dict[str, Any]) -> int:
    nodes = definition_json.get("nodes")
    if not isinstance(nodes, list):
        return 0
    count = 0
    for node in nodes:
        if not isinstance(node, dict):
            continue
        if str(node.get("type") or "").strip().lower() != "transform":
            continue
        metadata = node.get("metadata")
        if not isinstance(metadata, dict):
            continue
        if str(metadata.get("operation") or "").strip().lower() == "join":
            count += 1
    return count


def _serialize_run_tables(run_tables: Dict[str, Any], *, limit: int) -> Dict[str, Dict[str, Any]]:
    payload: Dict[str, Dict[str, Any]] = {}
    resolved_limit = max(0, int(limit))
    for node_id, table in (run_tables or {}).items():
        if not isinstance(node_id, str) or not node_id.strip():
            continue
        if not isinstance(table, dict):
            continue
        columns = table.get("columns")
        rows = table.get("rows")
        if not isinstance(columns, list) or not isinstance(rows, list):
            continue
        payload[node_id] = {
            "columns": columns,
            "rows": rows[:resolved_limit] if resolved_limit else rows,
        }
    return payload


def _normalize_definition_for_digest(definition_json: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(definition_json, dict):
        return {}
    normalized = dict(definition_json)
    if "__preview_meta__" in normalized:
        normalized.pop("__preview_meta__", None)
    nodes_raw = normalized.get("nodes")
    if not isinstance(nodes_raw, list):
        return normalized
    updated = False
    nodes: List[Any] = []
    for node in nodes_raw:
        if not isinstance(node, dict):
            nodes.append(node)
            continue
        metadata = node.get("metadata")
        if not isinstance(metadata, dict):
            nodes.append(node)
            continue
        operation = str(metadata.get("operation") or "").strip().lower()
        if operation != "compute":
            nodes.append(node)
            continue
        expression = metadata.get("expression")
        if not isinstance(expression, str):
            nodes.append(node)
            continue
        match = _SYS_TS_EXPR_RE.match(expression)
        if not match:
            nodes.append(node)
            continue
        col = match.group("col")
        next_metadata = dict(metadata)
        next_metadata["expression"] = f"{col} = to_timestamp('__SYS_TIME__')"
        next_node = dict(node)
        next_node["metadata"] = next_metadata
        nodes.append(next_node)
        updated = True
    if updated:
        normalized["nodes"] = nodes
    return normalized


def _definition_digest(definition_json: Dict[str, Any]) -> str:
    normalized = _normalize_definition_for_digest(definition_json)
    return sha256_canonical_json_prefixed(normalized)


def _sanitize_label_dict_with_limits(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise SecurityViolationError(f"Expected dict, got {type(payload)}")
    if len(payload) > _PREVIEW_SANITIZER.max_dict_keys:
        raise SecurityViolationError(
            f"Too many keys in dict: {len(payload)} > {_PREVIEW_SANITIZER.max_dict_keys}"
        )
    return _PREVIEW_SANITIZER.sanitize_label_dict(payload)


def _sanitize_preview_columns(columns: list[Any]) -> list[Any]:
    if not isinstance(columns, list):
        raise SecurityViolationError(f"Expected list, got {type(columns)}")
    if len(columns) > _PREVIEW_SANITIZER.max_list_items:
        raise SecurityViolationError(
            f"Too many items in list: {len(columns)} > {_PREVIEW_SANITIZER.max_list_items}"
        )
    sanitized: list[Any] = []
    for col in columns:
        if isinstance(col, dict):
            sanitized.append(sanitize_input(col))
        elif isinstance(col, str):
            sanitized.append(_PREVIEW_SANITIZER.sanitize_label_key(col))
        else:
            sanitized.append(_PREVIEW_SANITIZER.sanitize_any(col))
    return sanitized


def _sanitize_preview_rows(rows: list[Any]) -> list[Any]:
    if not isinstance(rows, list):
        raise SecurityViolationError(f"Expected list, got {type(rows)}")
    if len(rows) > _PREVIEW_SANITIZER.max_list_items:
        raise SecurityViolationError(
            f"Too many items in list: {len(rows)} > {_PREVIEW_SANITIZER.max_list_items}"
        )
    sanitized: list[Any] = []
    for row in rows:
        if isinstance(row, dict):
            sanitized.append(_sanitize_label_dict_with_limits(row))
        else:
            sanitized.append(_PREVIEW_SANITIZER.sanitize_any(row))
    return sanitized


def _sanitize_preview_table(table: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(table, dict):
        raise SecurityViolationError(f"Expected dict, got {type(table)}")
    if len(table) > _PREVIEW_SANITIZER.max_dict_keys:
        raise SecurityViolationError(
            f"Too many keys in dict: {len(table)} > {_PREVIEW_SANITIZER.max_dict_keys}"
        )
    sanitized: Dict[str, Any] = {}
    for key, value in table.items():
        if not isinstance(key, str):
            raise SecurityViolationError("Table keys must be strings")
        clean_key = _PREVIEW_SANITIZER.sanitize_field_name(key)
        if key == "columns" and isinstance(value, list):
            sanitized[clean_key] = _sanitize_preview_columns(value)
        elif key == "rows" and isinstance(value, list):
            sanitized[clean_key] = _sanitize_preview_rows(value)
        elif key in {"column_stats", "cast_stats"} and isinstance(value, dict):
            sanitized[clean_key] = _sanitize_label_dict_with_limits(value)
        else:
            sanitized[clean_key] = _PREVIEW_SANITIZER.sanitize_any(value)
    return sanitized


def _sanitize_preview_tables(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    if not isinstance(payload, dict):
        raise SecurityViolationError(f"Expected dict, got {type(payload)}")
    if len(payload) > _PREVIEW_SANITIZER.max_dict_keys:
        raise SecurityViolationError(
            f"Too many keys in dict: {len(payload)} > {_PREVIEW_SANITIZER.max_dict_keys}"
        )
    sanitized: Dict[str, Dict[str, Any]] = {}
    for key, value in payload.items():
        if not isinstance(key, str):
            raise SecurityViolationError("Preview table keys must be strings")
        clean_key = _PREVIEW_SANITIZER.sanitize_label_key(key)
        sanitized[clean_key] = _sanitize_preview_table(value)
    return sanitized
