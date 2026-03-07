"""Shared helpers for reading ontology property/relationship collections."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from shared.utils.payload_utils import unwrap_data_payload


def extract_ontology_properties(payload: Any) -> Dict[str, Dict[str, Any]]:
    data = unwrap_data_payload(payload)
    props = data.get("properties") if isinstance(data, dict) else None
    output: Dict[str, Dict[str, Any]] = {}
    if isinstance(props, list):
        for prop in props:
            if not isinstance(prop, dict):
                continue
            name = str(prop.get("name") or prop.get("id") or "").strip()
            if name:
                output[name] = prop
    return output


def extract_ontology_relationships(payload: Any) -> Dict[str, Dict[str, Any]]:
    data = unwrap_data_payload(payload)
    rels = data.get("relationships") if isinstance(data, dict) else None
    output: Dict[str, Dict[str, Any]] = {}
    if isinstance(rels, list):
        for rel in rels:
            if not isinstance(rel, dict):
                continue
            predicate = str(rel.get("predicate") or rel.get("name") or "").strip()
            if predicate:
                output[predicate] = rel
    return output


def extract_ontology_fields(payload: Any) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    return extract_ontology_properties(payload), extract_ontology_relationships(payload)


def list_ontology_properties(payload: Any) -> List[Dict[str, Any]]:
    return list(extract_ontology_properties(payload).values())
