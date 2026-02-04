"""
Ontology router helpers (BFF).

This module acts as a small Facade over repeated normalization / transformation
logic used by multiple ontology subrouters.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from shared.utils.id_generator import generate_simple_id

logger = logging.getLogger(__name__)


def _localized_to_string(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        # Prefer English for IDs (domain-neutral), then Korean, then any translation.
        return (
            str(value.get("en") or "").strip()
            or str(value.get("ko") or "").strip()
            or next((str(v).strip() for v in value.values() if v and str(v).strip()), "")
        )
    return str(value).strip() if value is not None else ""


def _transform_properties_for_oms(data: Dict[str, Any], *, log_conversions: bool = False) -> None:
    properties = data.get("properties")
    if not isinstance(properties, list):
        return

    for prop in properties:
        if not isinstance(prop, dict):
            continue

        if "name" not in prop and "label" in prop:
            prop["name"] = generate_simple_id(
                _localized_to_string(prop.get("label")),
                use_timestamp_for_korean=False,
            )

        if prop.get("type") == "STRING":
            prop["type"] = "xsd:string"
        elif prop.get("type") == "INTEGER":
            prop["type"] = "xsd:integer"
        elif prop.get("type") == "DECIMAL":
            prop["type"] = "xsd:decimal"
        elif prop.get("type") == "BOOLEAN":
            prop["type"] = "xsd:boolean"
        elif prop.get("type") == "DATETIME":
            prop["type"] = "xsd:dateTime"

        if prop.get("type") == "link" and "target" in prop:
            prop["linkTarget"] = prop.pop("target")
            if log_conversions:
                logger.info(
                    "🔧 Converted property '%s' target -> linkTarget: %s",
                    prop.get("name"),
                    prop.get("linkTarget"),
                )

        if prop.get("type") == "array" and "items" in prop:
            items = prop["items"]
            if isinstance(items, dict) and items.get("type") == "link" and "target" in items:
                items["linkTarget"] = items.pop("target")
                if log_conversions:
                    logger.info(
                        "🔧 Converted array property '%s' items target -> linkTarget: %s",
                        prop.get("name"),
                        items.get("linkTarget"),
                    )


def _normalize_mapping_type(type_value: Any) -> str:
    if not type_value:
        return "xsd:string"
    t = str(type_value).strip()
    if not t:
        return "xsd:string"
    if t.startswith("xsd:"):
        return t

    t_lower = t.lower()

    # Funnel complex types → base XSD types (domain-neutral)
    if t_lower == "money":
        return "xsd:decimal"
    if t_lower in {"integer", "int"}:
        return "xsd:integer"
    if t_lower in {"decimal", "number", "float", "double"}:
        return "xsd:decimal"
    if t_lower in {"boolean", "bool"}:
        return "xsd:boolean"
    if t_lower in {"datetime", "timestamp"}:
        return "xsd:dateTime"
    if t_lower == "date":
        # Note: "date" may be a complex type representing date/time in upstream.
        return "xsd:dateTime"
    return "xsd:string"


def _build_source_schema_from_preview(preview: Dict[str, Any]) -> List[Dict[str, Any]]:
    inferred = preview.get("inferred_schema") or []
    out: List[Dict[str, Any]] = []

    for col in inferred:
        if not isinstance(col, dict):
            continue
        col_name = col.get("column_name") or col.get("name")
        if not col_name:
            continue
        inferred_type = col.get("inferred_type") or {}
        type_id = None
        conf = None
        if isinstance(inferred_type, dict):
            type_id = inferred_type.get("type")
            conf = inferred_type.get("confidence")
        if not type_id:
            type_id = col.get("type")

        field: Dict[str, Any] = {
            "name": str(col_name),
            "type": _normalize_mapping_type(type_id),
        }
        if conf is not None:
            field["confidence"] = conf
        if type_id is not None:
            field["original_type"] = type_id
        out.append(field)

    # Fallback: no inferred_schema → columns only
    if not out:
        for name in preview.get("columns") or []:
            out.append({"name": str(name), "type": "xsd:string"})

    return out


def _build_sample_data_from_preview(preview: Dict[str, Any]) -> List[Dict[str, Any]]:
    columns = [str(c) for c in (preview.get("columns") or [])]
    rows = preview.get("sample_data") or []
    sample: List[Dict[str, Any]] = []

    for row in rows:
        if not isinstance(row, list):
            continue
        sample.append({columns[i]: row[i] if i < len(row) else None for i in range(len(columns))})

    return sample


def _build_target_schema_from_ontology(
    ontology: Dict[str, Any],
    *,
    include_relationships: bool,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    raw_props = ontology.get("properties") or []
    for prop in raw_props:
        if isinstance(prop, dict):
            name = prop.get("name") or prop.get("id")
            if not name:
                continue
            out.append(
                {
                    "name": str(name),
                    "type": _normalize_mapping_type(prop.get("type")),
                    "label": prop.get("label"),
                }
            )
        elif isinstance(prop, str):
            out.append({"name": prop, "type": "xsd:string"})

    if include_relationships:
        raw_rels = ontology.get("relationships") or []
        for rel in raw_rels:
            if not isinstance(rel, dict):
                continue
            predicate = rel.get("predicate") or rel.get("name")
            if not predicate:
                continue
            out.append(
                {
                    "name": str(predicate),
                    "type": "link",
                    "label": rel.get("label"),
                    "target": rel.get("target"),
                }
            )

    # Keep stable order but remove duplicates by name
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for f in out:
        name = f.get("name")
        if not name or name in seen:
            continue
        seen.add(name)
        deduped.append(f)
    return deduped


def _normalize_target_schema_for_mapping(
    target_schema: List[Dict[str, Any]],
    *,
    include_relationships: bool,
) -> List[Dict[str, Any]]:
    """
    Normalize a client-provided target schema to the shape expected by MappingSuggestionService.

    This keeps behavior domain-neutral and does not require OMS.
    """
    out: List[Dict[str, Any]] = []
    for f in target_schema or []:
        if not isinstance(f, dict):
            continue
        name = f.get("name") or f.get("id") or f.get("predicate")
        if not name:
            continue
        t = f.get("type") or "xsd:string"
        norm = {**f, "name": str(name), "type": _normalize_mapping_type(t)}
        out.append(norm)

    if not include_relationships:
        out = [f for f in out if str(f.get("type") or "") != "link"]

    # Keep stable order but remove duplicates by name
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for f in out:
        name = f.get("name")
        if not name or name in seen:
            continue
        seen.add(name)
        deduped.append(f)
    return deduped


def _extract_target_field_types(ontology: Dict[str, Any]) -> Dict[str, str]:
    types: Dict[str, str] = {}

    for prop in ontology.get("properties") or []:
        if not isinstance(prop, dict):
            continue
        name = prop.get("name") or prop.get("id")
        if not name:
            continue
        types[str(name)] = str(prop.get("type") or "xsd:string")

    for rel in ontology.get("relationships") or []:
        if not isinstance(rel, dict):
            continue
        predicate = rel.get("predicate") or rel.get("name")
        if not predicate:
            continue
        types[str(predicate)] = "link"

    return types


def _normalize_import_target_type(type_value: Any) -> str:
    """
    Normalize user-provided target field type for import.

    This is intentionally conservative and domain-neutral.
    """
    from shared.utils.import_type_normalization import normalize_import_target_type

    return normalize_import_target_type(type_value)


def _extract_target_field_types_from_import_schema(
    target_schema: List[Any],
) -> Dict[str, str]:
    types: Dict[str, str] = {}
    for f in target_schema:
        name = (getattr(f, "name", "") or "").strip()
        if not name:
            continue
        types[name] = _normalize_import_target_type(getattr(f, "type", None))
    return types

