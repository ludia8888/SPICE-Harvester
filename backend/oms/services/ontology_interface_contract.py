"""
Interface contract validation for ontology classes.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from oms.services.ontology_health_issue_registry import build_object_type_ref


def extract_interface_refs(metadata: Dict[str, Any]) -> List[str]:
    if not metadata:
        return []
    refs: List[str] = []
    for key in ("interfaces", "interface_refs", "interfaceRefs", "interfaceRef", "interface"):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            refs.append(value.strip())
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, str) and item.strip():
                    refs.append(item.strip())
    seen = set()
    ordered = []
    for ref in refs:
        if ref not in seen:
            seen.add(ref)
            ordered.append(ref)
    return ordered


def strip_interface_prefix(value: str) -> str:
    if not value:
        return value
    for prefix in ("interface:", "interfaces:"):
        if value.startswith(prefix):
            return value[len(prefix) :].strip()
    return value.strip()


def collect_interface_contract_issues(
    *,
    ontology_id: str,
    metadata: Dict[str, Any],
    properties: List[Any],
    relationships: List[Any],
    interface_index: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    interface_refs = extract_interface_refs(metadata)
    if not interface_refs:
        return []

    property_map = build_property_map(properties or [])
    relationship_map = build_relationship_map(relationships or [])

    issues: List[Dict[str, Any]] = []
    for raw_ref in interface_refs:
        interface_id = strip_interface_prefix(raw_ref)
        interface_resource = interface_index.get(interface_id)
        resource_ref = build_object_type_ref(ontology_id)
        if not interface_resource:
            issues.append(
                {
                    "code": "IFACE_NOT_FOUND",
                    "severity": "ERROR",
                    "resource_ref": resource_ref,
                    "details": {"interface_ref": raw_ref, "interface_id": interface_id},
                    "suggested_fix": "Create the interface resource or remove the reference.",
                    "message": f"Interface '{raw_ref}' not found for ontology '{ontology_id}'",
                }
            )
            continue

        interface_spec = interface_resource.get("spec") or {}
        required_properties = extract_required_entries(
            interface_spec.get("required_properties"),
            name_keys=("name",),
        )
        required_relationships = extract_required_entries(
            interface_spec.get("required_relationships"),
            name_keys=("predicate", "name"),
        )

        for name, entry in required_properties:
            actual = property_map.get(name)
            if not actual:
                issues.append(
                    {
                        "code": "IFACE_MISSING_PROPERTY",
                        "severity": "ERROR",
                        "resource_ref": resource_ref,
                        "details": {"missing_fields": [name], "interface_id": interface_id},
                        "suggested_fix": "Add the missing property or update the interface contract.",
                        "message": (
                            f"Ontology '{ontology_id}' missing property '{name}' "
                            f"required by interface '{interface_id}'"
                        ),
                    }
                )
                continue
            required_type = extract_entry_value(
                entry, keys=("type", "type_ref", "typeRef", "value_type_ref", "valueTypeRef")
            )
            if required_type:
                actual_type = extract_property_type(actual)
                if (
                    actual_type
                    and normalize_reference_value(actual_type)
                    != normalize_reference_value(required_type)
                ):
                    issues.append(
                        {
                            "code": "IFACE_PROPERTY_TYPE_MISMATCH",
                            "severity": "ERROR",
                            "resource_ref": resource_ref,
                            "details": {
                                "field": name,
                                "interface_id": interface_id,
                                "expected": {"type": required_type},
                                "actual": {"type": actual_type},
                            },
                            "suggested_fix": "Align the property type with the interface requirement.",
                            "message": (
                                f"Ontology '{ontology_id}' property '{name}' type '{actual_type}' "
                                f"does not match interface '{interface_id}' requirement '{required_type}'"
                            ),
                        }
                    )

        for predicate, entry in required_relationships:
            actual = relationship_map.get(predicate)
            if not actual:
                issues.append(
                    {
                        "code": "IFACE_MISSING_RELATIONSHIP",
                        "severity": "ERROR",
                        "resource_ref": resource_ref,
                        "details": {"missing_relationships": [predicate], "interface_id": interface_id},
                        "suggested_fix": "Add the missing relationship or update the interface contract.",
                        "message": (
                            f"Ontology '{ontology_id}' missing relationship '{predicate}' "
                            f"required by interface '{interface_id}'"
                        ),
                    }
                )
                continue
            required_target = extract_entry_value(
                entry, keys=("target", "object_type_ref", "objectTypeRef", "class")
            )
            if required_target:
                actual_target = extract_relationship_target(actual)
                if (
                    actual_target
                    and normalize_reference_value(actual_target)
                    != normalize_reference_value(required_target)
                ):
                    issues.append(
                        {
                            "code": "IFACE_REL_TARGET_MISMATCH",
                            "severity": "ERROR",
                            "resource_ref": resource_ref,
                            "details": {
                                "field": predicate,
                                "interface_id": interface_id,
                                "expected": {"target": required_target},
                                "actual": {"target": actual_target},
                            },
                            "suggested_fix": "Align the relationship target with the interface requirement.",
                            "message": (
                                f"Ontology '{ontology_id}' relationship '{predicate}' "
                                f"target '{actual_target}' does not match interface '{interface_id}' "
                                f"requirement '{required_target}'"
                            ),
                        }
                    )

    return issues


def extract_required_entries(
    items: Any,
    *,
    name_keys: Tuple[str, ...],
) -> List[Tuple[str, Dict[str, Any]]]:
    entries: List[Tuple[str, Dict[str, Any]]] = []
    if not isinstance(items, list):
        return entries
    for item in items:
        if isinstance(item, str):
            name = item.strip()
            if name:
                entries.append((name, {}))
            continue
        if not isinstance(item, dict):
            continue
        name = extract_entry_value(item, keys=name_keys)
        if name:
            entries.append((name, item))
    return entries


def extract_entry_value(entry: Any, *, keys: Tuple[str, ...]) -> Optional[str]:
    if not isinstance(entry, dict):
        return None
    for key in keys:
        value = entry.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def build_property_map(items: List[Any]) -> Dict[str, Any]:
    prop_map: Dict[str, Any] = {}
    for item in items:
        name = None
        if hasattr(item, "name"):
            name = getattr(item, "name")
        elif isinstance(item, dict):
            name = item.get("name")
        if isinstance(name, str) and name.strip():
            prop_map[name.strip()] = item
    return prop_map


def build_relationship_map(items: List[Any]) -> Dict[str, Any]:
    rel_map: Dict[str, Any] = {}
    for item in items:
        name = None
        if hasattr(item, "predicate"):
            name = getattr(item, "predicate")
        elif isinstance(item, dict):
            name = item.get("predicate") or item.get("name")
        if isinstance(name, str) and name.strip():
            rel_map[name.strip()] = item
    return rel_map


def extract_property_type(item: Any) -> Optional[str]:
    if hasattr(item, "type"):
        value = getattr(item, "type")
        return value if isinstance(value, str) else None
    if isinstance(item, dict):
        value = item.get("type")
        return value if isinstance(value, str) else None
    return None


def extract_relationship_target(item: Any) -> Optional[str]:
    if hasattr(item, "target"):
        value = getattr(item, "target")
        return value if isinstance(value, str) else None
    if isinstance(item, dict):
        value = item.get("target")
        return value if isinstance(value, str) else None
    return None


def normalize_reference_value(value: str) -> str:
    if not isinstance(value, str):
        return ""
    normalized = value.strip()
    for prefix in (
        "object:",
        "class:",
        "value_type:",
        "value:",
        "interface:",
        "shared_property:",
        "shared:",
    ):
        if normalized.startswith(prefix):
            return normalized[len(prefix) :].strip()
    return normalized
