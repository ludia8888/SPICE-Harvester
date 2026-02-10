"""
Relationship extraction from instance payload using ontology schema.

Shared library used by:
- objectify_worker: extract relationships during ES indexing
- instance_worker: extract relationships for CRUD operations (Phase 2)

No TerminusDB dependency — accepts ontology data directly.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set, Union

logger = logging.getLogger(__name__)


def _normalize_ref(ref: Any) -> str:
    """Normalize a relationship reference to TargetClass/instance_id format."""
    if not isinstance(ref, str):
        raise ValueError(f"Relationship ref must be a string, got {type(ref).__name__}")

    candidate = ref.strip()
    if "/" not in candidate:
        raise ValueError(
            f"Invalid relationship ref '{ref}'. Expected '<TargetClass>/<instance_id>'"
        )

    target_class, target_instance_id = candidate.split("/", 1)
    if not target_class.strip():
        raise ValueError("Relationship target class is empty")
    if not target_instance_id.strip():
        raise ValueError("Relationship instance id is empty")
    return f"{target_class}/{target_instance_id}"


def _expects_many(cardinality: Optional[Any]) -> Optional[bool]:
    """Determine if a relationship cardinality expects multiple values."""
    if cardinality is None:
        return None
    try:
        card = str(cardinality).strip()
    except Exception:
        return None
    if not card:
        return None
    return card.endswith(":n") or card.endswith(":m")


def _extract_from_relationship_list(
    rel_list: list,
    payload: Dict[str, Any],
    relationships: Dict[str, Union[str, List[str]]],
    known_fields: Set[str],
) -> None:
    """Extract relationships from a list of relationship definitions (OntologyResponse or OMS dict)."""
    for rel in rel_list:
        if not isinstance(rel, dict):
            field_name = getattr(rel, "predicate", None) or getattr(rel, "name", None)
            cardinality = getattr(rel, "cardinality", None)
        else:
            field_name = rel.get("predicate") or rel.get("name")
            cardinality = rel.get("cardinality")

        if field_name:
            known_fields.add(str(field_name))
        if not field_name or field_name not in payload:
            continue

        value = payload[field_name]
        wants_many = _expects_many(cardinality)

        if wants_many is True:
            items = value if isinstance(value, list) else [value]
            normalized: List[str] = []
            for item in items:
                normalized.append(_normalize_ref(item))
            seen: Set[str] = set()
            unique = [v for v in normalized if not (v in seen or seen.add(v))]  # type: ignore[func-returns-value]
            relationships[str(field_name)] = unique
        else:
            if isinstance(value, list):
                if len(value) == 0:
                    continue
                if len(value) != 1:
                    raise ValueError(
                        f"Relationship '{field_name}' expects a single ref (cardinality={cardinality}), "
                        f"got {len(value)} items"
                    )
                value = value[0]
            relationships[str(field_name)] = _normalize_ref(value)


def _extract_from_terminus_schema(
    schema: Dict[str, Any],
    payload: Dict[str, Any],
    relationships: Dict[str, Union[str, List[str]]],
    known_fields: Set[str],
) -> None:
    """Extract relationships from TerminusDB-style schema (properties with @class)."""
    for key, value_def in schema.items():
        if isinstance(value_def, dict) and "@class" in value_def:
            known_fields.add(str(key))
        if isinstance(value_def, dict) and "@class" in value_def and key in payload:
            value = payload[key]
            wants_many = str(value_def.get("@type") or "").strip() == "Set"

            if wants_many:
                items = value if isinstance(value, list) else [value]
                normalized: List[str] = []
                for item in items:
                    normalized.append(_normalize_ref(item))
                seen: Set[str] = set()
                unique = [v for v in normalized if not (v in seen or seen.add(v))]  # type: ignore[func-returns-value]
                relationships[str(key)] = unique
            else:
                if isinstance(value, list):
                    if len(value) == 0:
                        continue
                    if len(value) != 1:
                        raise ValueError(
                            f"Relationship '{key}' expects a single ref, got {len(value)} items"
                        )
                    value = value[0]
                relationships[str(key)] = _normalize_ref(value)


def _pattern_fallback(
    payload: Dict[str, Any],
    relationships: Dict[str, Union[str, List[str]]],
) -> None:
    """Fallback: detect relationships by field name patterns."""
    patterns = ("_by", "_to", "_ref", "contains", "linked")
    for key, value in payload.items():
        if key in relationships:
            continue

        if isinstance(value, str) and "/" in value:
            if any(pattern in key for pattern in patterns):
                relationships[key] = _normalize_ref(value)
            continue

        if isinstance(value, list) and value:
            if any(pattern in key for pattern in patterns):
                normalized: List[str] = [_normalize_ref(item) for item in value]
                seen: Set[str] = set()
                unique = [v for v in normalized if not (v in seen or seen.add(v))]  # type: ignore[func-returns-value]
                relationships[key] = unique


def extract_relationships(
    payload: Dict[str, Any],
    *,
    ontology_data: Any = None,
    rel_map: Optional[Dict[str, Dict[str, Any]]] = None,
    allow_pattern_fallback: bool = True,
) -> Dict[str, Union[str, List[str]]]:
    """
    Extract relationship fields from an instance payload.

    Args:
        payload: The instance data dictionary.
        ontology_data: Ontology schema data (OntologyResponse pydantic model, OMS dict,
                       or TerminusDB schema dict). Optional if rel_map is provided.
        rel_map: Pre-parsed relationship map from _extract_ontology_fields().
                 Keys are predicate names, values are relationship definition dicts.
                 When provided, ontology_data is ignored.
        allow_pattern_fallback: If True, also detect relationships by field name patterns.

    Returns:
        Dict mapping predicate names to TargetClass/instance_id references.
        Single refs are strings, multi-refs are lists of strings.
    """
    relationships: Dict[str, Union[str, List[str]]] = {}
    known_fields: Set[str] = set()

    if rel_map is not None:
        # Fast path: use pre-parsed relationship map from objectify_worker
        rel_list = [{"predicate": k, **v} for k, v in rel_map.items()]
        _extract_from_relationship_list(rel_list, payload, relationships, known_fields)
    elif ontology_data is not None:
        try:
            # OntologyResponse (pydantic) shape
            rel_list = getattr(ontology_data, "relationships", None)
            if isinstance(rel_list, list):
                _extract_from_relationship_list(rel_list, payload, relationships, known_fields)
            elif isinstance(ontology_data, dict):
                if "relationships" in ontology_data and isinstance(ontology_data.get("relationships"), list):
                    _extract_from_relationship_list(
                        ontology_data["relationships"], payload, relationships, known_fields
                    )
                else:
                    _extract_from_terminus_schema(ontology_data, payload, relationships, known_fields)
        except Exception as e:
            logger.warning("Could not extract relationships from ontology data: %s", e)

    if allow_pattern_fallback:
        _pattern_fallback(payload, relationships)

    return relationships
