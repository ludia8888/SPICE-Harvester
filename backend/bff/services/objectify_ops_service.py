"""Objectify helper utilities (BFF).

Extracted from `bff.routers.objectify_ops` to keep routers thin and to
centralize shared logic behind a small service facade.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from bff.services.link_types_mapping_service import extract_ontology_properties, extract_ontology_relationships
from shared.services.pipeline.pipeline_schema_utils import normalize_schema_type
from shared.utils.import_type_normalization import resolve_import_type
from shared.utils.payload_utils import unwrap_data_payload
from shared.utils.schema_type_compatibility import is_type_compatible
from shared.utils.schema_columns import (
    extract_schema_column_names as _extract_schema_column_names_raw,
    extract_schema_type_map as _extract_schema_type_map_raw,
)
from shared.utils.schema_hash import compute_schema_hash_from_sample
from shared.utils.objectify_outputs import match_output_name as _match_output_name_raw

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
    return _match_output_name_raw(output, name)


def _compute_schema_hash_from_sample(sample_json: Any) -> Optional[str]:
    return compute_schema_hash_from_sample(sample_json)


def _extract_schema_columns(schema: Any) -> List[str]:
    return _extract_schema_column_names_raw(schema)


def _extract_schema_types(schema: Any) -> Dict[str, str]:
    return _extract_schema_type_map_raw(schema, normalizer=normalize_schema_type)


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


def _extract_ontology_fields(payload: Any) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    return extract_ontology_properties(payload), extract_ontology_relationships(payload)


def _resolve_import_type(raw_type: Any) -> Optional[str]:
    return resolve_import_type(raw_type)


def _is_type_compatible(source_type: str, target_type: str) -> bool:
    return is_type_compatible(source_type, target_type)


def _unwrap_data_payload(payload: Any) -> Dict[str, Any]:
    return unwrap_data_payload(payload)
