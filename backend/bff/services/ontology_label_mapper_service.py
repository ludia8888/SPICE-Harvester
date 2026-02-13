"""Ontology label mapper helpers (BFF).

Centralizes LabelMapper interactions for ontology payloads to avoid duplicated
registration and label→id conversion logic across services/routers.
"""

from __future__ import annotations

from typing import Any, Dict

from shared.utils.label_mapper import LabelMapper
from shared.observability.tracing import trace_external_call


@trace_external_call("bff.ontology_label_mapper.map_relationship_targets")
async def map_relationship_targets(
    *,
    mapper: LabelMapper,
    db_name: str,
    ontology_dict: Dict[str, Any],
    lang: str,
) -> None:
    """Convert relationship targets from labels → class ids when mappings exist."""
    relationships = ontology_dict.get("relationships") or []
    if not isinstance(relationships, list):
        return
    for rel in relationships:
        if not isinstance(rel, dict):
            continue
        target = rel.get("target")
        if not isinstance(target, str) or not target.strip():
            continue
        mapped = await mapper.get_class_id(db_name, target, lang)
        if mapped:
            rel["target"] = mapped


@trace_external_call("bff.ontology_label_mapper.register_ontology_label_mappings")
async def register_ontology_label_mappings(
    *,
    mapper: LabelMapper,
    db_name: str,
    class_id: str,
    ontology_dict: Dict[str, Any],
) -> None:
    """Register class/property/relationship label mappings for a created ontology."""
    await mapper.register_class(
        db_name,
        class_id,
        ontology_dict.get("label", ""),
        ontology_dict.get("description"),
    )

    for prop in ontology_dict.get("properties", []) or []:
        if isinstance(prop, dict):
            await mapper.register_property(
                db_name,
                class_id,
                prop.get("name", ""),
                prop.get("label", ""),
            )

    for rel in ontology_dict.get("relationships", []) or []:
        if isinstance(rel, dict):
            await mapper.register_relationship(
                db_name,
                rel.get("predicate", ""),
                rel.get("label", ""),
            )

