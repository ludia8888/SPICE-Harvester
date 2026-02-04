"""Objectify helpers (BFF).

This module acts as a small Facade over helper logic shared by the objectify
endpoint routers. Keeping logic here supports router composition (Composite
pattern) and prevents individual routers from becoming monoliths.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from shared.services.pipeline.pipeline_schema_utils import normalize_schema_type
from shared.utils.import_type_normalization import normalize_import_target_type
from shared.utils.schema_type_compatibility import is_type_compatible as _is_type_compatible
from shared.utils.schema_hash import compute_schema_hash

_ALLOWED_SOURCE_TYPES = {
    "xsd:string",
    "xsd:integer",
    "xsd:decimal",
    "xsd:boolean",
    "xsd:date",
    "xsd:dateTime",
    "xsd:time",
}


def _match_output_name(output: Dict[str, Any], name: str) -> bool:
    if not name:
        return False
    target = name.strip()
    if not target:
        return False
    for key in ("output_name", "dataset_name", "node_id"):
        candidate = str(output.get(key) or "").strip()
        if candidate and candidate == target:
            return True
    return False


def _compute_schema_hash_from_sample(sample_json: Any) -> Optional[str]:
    if not isinstance(sample_json, dict):
        return None
    columns = sample_json.get("columns")
    if not isinstance(columns, list) or not columns:
        return None
    return compute_schema_hash(columns)


def _extract_schema_columns(schema: Any) -> List[str]:
    if not isinstance(schema, dict):
        return []
    columns = schema.get("columns")
    if isinstance(columns, list):
        names: List[str] = []
        for col in columns:
            if isinstance(col, dict):
                name = str(col.get("name") or col.get("column") or "").strip()
            else:
                name = str(col).strip()
            if name:
                names.append(name)
        return names
    fields = schema.get("fields")
    if isinstance(fields, list):
        names = []
        for col in fields:
            if not isinstance(col, dict):
                continue
            name = str(col.get("name") or "").strip()
            if name:
                names.append(name)
        return names
    props = schema.get("properties")
    if isinstance(props, dict):
        return [str(key).strip() for key in props.keys() if str(key).strip()]
    return []


def _extract_schema_types(schema: Any) -> Dict[str, str]:
    if not isinstance(schema, dict):
        return {}
    columns = schema.get("columns")
    if isinstance(columns, list):
        output: Dict[str, str] = {}
        for col in columns:
            if isinstance(col, dict):
                name = str(col.get("name") or col.get("column") or "").strip()
                raw_type = col.get("type") or col.get("data_type") or col.get("datatype")
            else:
                name = str(col).strip()
                raw_type = None
            if name:
                output[name] = normalize_schema_type(raw_type)
        return output
    fields = schema.get("fields")
    if isinstance(fields, list):
        output = {}
        for col in fields:
            if not isinstance(col, dict):
                continue
            name = str(col.get("name") or "").strip()
            if name:
                output[name] = normalize_schema_type(col.get("type"))
        return output
    props = schema.get("properties")
    if isinstance(props, dict):
        return {str(key).strip(): normalize_schema_type(val) for key, val in props.items() if str(key).strip()}
    return {}


def _normalize_mapping_pair(item: Any) -> Optional[tuple[str, str]]:
    if not isinstance(item, dict):
        return None
    source = str(item.get("source_field") or "").strip()
    target = str(item.get("target_field") or "").strip()
    if not source or not target:
        return None
    return source, target


def _build_mapping_change_summary(
    previous_mappings: List[Dict[str, Any]],
    new_mappings: List[Dict[str, Any]],
) -> Dict[str, Any]:
    previous_pairs = {pair for pair in (_normalize_mapping_pair(item) for item in previous_mappings) if pair}
    new_pairs = {pair for pair in (_normalize_mapping_pair(item) for item in new_mappings) if pair}
    added_pairs = sorted(new_pairs - previous_pairs)
    removed_pairs = sorted(previous_pairs - new_pairs)

    previous_by_target: Dict[str, set[str]] = {}
    for source, target in previous_pairs:
        previous_by_target.setdefault(target, set()).add(source)

    new_by_target: Dict[str, set[str]] = {}
    for source, target in new_pairs:
        new_by_target.setdefault(target, set()).add(source)

    changed_targets: List[Dict[str, Any]] = []
    for target in sorted(set(previous_by_target) | set(new_by_target)):
        previous_sources = previous_by_target.get(target, set())
        new_sources = new_by_target.get(target, set())
        if previous_sources != new_sources:
            changed_targets.append(
                {
                    "target_field": target,
                    "previous_sources": sorted(previous_sources),
                    "new_sources": sorted(new_sources),
                }
            )

    impacted_targets = {target for _, target in added_pairs + removed_pairs}
    impacted_targets.update(item["target_field"] for item in changed_targets)

    impacted_sources = {source for source, _ in added_pairs + removed_pairs}
    for item in changed_targets:
        impacted_sources.update(item["previous_sources"])
        impacted_sources.update(item["new_sources"])

    return {
        "added": [{"source_field": source, "target_field": target} for source, target in added_pairs],
        "removed": [{"source_field": source, "target_field": target} for source, target in removed_pairs],
        "changed_targets": changed_targets,
        "impacted_targets": sorted(impacted_targets),
        "impacted_sources": sorted(impacted_sources),
        "counts": {
            "added": len(added_pairs),
            "removed": len(removed_pairs),
            "changed_targets": len(changed_targets),
        },
        "has_changes": bool(added_pairs or removed_pairs or changed_targets),
    }


def _unwrap_data_payload(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    if isinstance(payload, dict):
        return payload
    return {}


def _extract_ontology_fields(payload: Any) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    data = _unwrap_data_payload(payload)
    properties = data.get("properties") if isinstance(data, dict) else None
    relationships = data.get("relationships") if isinstance(data, dict) else None

    prop_map: Dict[str, Dict[str, Any]] = {}
    if isinstance(properties, list):
        for prop in properties:
            if not isinstance(prop, dict):
                continue
            name = str(prop.get("name") or "").strip()
            if not name:
                continue
            prop_map[name] = prop

    rel_map: Dict[str, Dict[str, Any]] = {}
    if isinstance(relationships, list):
        for rel in relationships:
            if not isinstance(rel, dict):
                continue
            predicate = str(rel.get("predicate") or rel.get("name") or "").strip()
            if predicate:
                rel_map[predicate] = rel

    return prop_map, rel_map


def _resolve_import_type(raw_type: Any) -> Optional[str]:
    if not raw_type:
        return None
    raw = str(raw_type).strip()
    if not raw:
        return None
    lowered = raw.lower()
    if lowered.startswith("xsd:"):
        return raw
    if lowered.startswith("sys:"):
        return raw
    supported = {
        "string",
        "text",
        "integer",
        "int",
        "long",
        "decimal",
        "number",
        "float",
        "double",
        "boolean",
        "bool",
        "date",
        "datetime",
        "timestamp",
        "email",
        "url",
        "uri",
        "uuid",
        "ip",
        "phone",
        "json",
        "array",
        "struct",
        "object",
        "vector",
        "geopoint",
        "geoshape",
        "marking",
        "cipher",
        "attachment",
        "media",
        "time_series",
        "timeseries",
    }
    if lowered not in supported:
        return None
    return normalize_import_target_type(lowered)
