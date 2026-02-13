"""Pipeline Builder schema helpers.

Schema coercion/diffing and PK resolution.
Extracted from `bff.routers.pipeline_ops`.
"""


from typing import Any, Dict, List, Optional

from shared.services.pipeline.pipeline_definition_utils import resolve_pk_columns
from shared.services.pipeline.pipeline_graph_utils import normalize_nodes
from shared.services.pipeline.pipeline_schema_utils import normalize_schema_type


def _normalize_schema_column_type(value: Any) -> str:
    return normalize_schema_type(value)


def _coerce_schema_columns(raw: Any) -> list[dict[str, str]]:
    if not raw:
        return []
    if isinstance(raw, dict) and "columns" in raw:
        raw = raw.get("columns")
    if not isinstance(raw, list):
        return []
    output: list[dict[str, str]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or item.get("column") or "").strip()
        if not name:
            continue
        output.append({"name": name, "type": _normalize_schema_column_type(item.get("type"))})
    return output


def _detect_breaking_schema_changes(*, previous_schema: Any, next_columns: Any) -> list[dict[str, str]]:
    previous_cols = _coerce_schema_columns(previous_schema)
    next_cols = _coerce_schema_columns(next_columns)
    if not previous_cols or not next_cols:
        return []

    prev_map = {item["name"]: item.get("type") or "" for item in previous_cols}
    next_map = {item["name"]: item.get("type") or "" for item in next_cols}

    breaking: list[dict[str, str]] = []
    for name, prev_type in prev_map.items():
        if name not in next_map:
            breaking.append({"kind": "MISSING_COLUMN", "column": name})
            continue
        next_type = next_map.get(name) or ""
        if prev_type and next_type and prev_type != next_type:
            breaking.append({"kind": "TYPE_CHANGED", "column": name, "from": prev_type, "to": next_type})
    return breaking



def _resolve_output_pk_columns(
    *,
    definition_json: Dict[str, Any],
    node_id: Optional[str],
    output_name: Optional[str],
) -> List[str]:
    if not isinstance(definition_json, dict):
        return []
    nodes = normalize_nodes(definition_json.get("nodes"))
    node = nodes.get(node_id or "")
    output_metadata = node.get("metadata") if isinstance(node, dict) else {}
    if not isinstance(output_metadata, dict):
        output_metadata = {}
    declared_outputs = definition_json.get("outputs") if isinstance(definition_json.get("outputs"), list) else []
    return resolve_pk_columns(
        definition=definition_json,
        output_metadata=output_metadata,
        output_name=output_name,
        output_node_id=node_id,
        declared_outputs=declared_outputs,
    )

