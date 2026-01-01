"""
Ontology health issue catalog and normalization utilities.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

ISSUE_CATALOG_VERSION = "v1"

ALLOWED_CODE_PREFIXES = ("IFACE_", "RESOURCE_", "REL_", "LINT_")

CODE_PREFIX_BY_SOURCE = {
    "interface_contract": "IFACE_",
    "resource_validation": "RESOURCE_",
    "relationship_validation": "REL_",
    "lint": "LINT_",
}

ISSUE_CODE_REGISTRY: Dict[str, Dict[str, Any]] = {
    "IFACE_NOT_FOUND": {
        "severity": "ERROR",
        "details_schema": {"interface_ref": "", "interface_id": ""},
        "suggested_fix": "Create the interface resource or remove the reference.",
    },
    "IFACE_MISSING_PROPERTY": {
        "severity": "ERROR",
        "details_schema": {"missing_fields": [], "interface_id": ""},
        "suggested_fix": "Add the missing property or update the interface contract.",
    },
    "IFACE_PROPERTY_TYPE_MISMATCH": {
        "severity": "ERROR",
        "details_schema": {"field": "", "expected": {}, "actual": {}, "interface_id": ""},
        "suggested_fix": "Align the property type with the interface requirement.",
    },
    "IFACE_MISSING_RELATIONSHIP": {
        "severity": "ERROR",
        "details_schema": {"missing_relationships": [], "interface_id": ""},
        "suggested_fix": "Add the missing relationship or update the interface contract.",
    },
    "IFACE_REL_TARGET_MISMATCH": {
        "severity": "ERROR",
        "details_schema": {"field": "", "expected": {}, "actual": {}, "interface_id": ""},
        "suggested_fix": "Align the relationship target with the interface requirement.",
    },
    "RESOURCE_SPEC_INVALID": {
        "severity": "ERROR",
        "details_schema": {"missing_fields": [], "invalid_fields": []},
        "suggested_fix": "Fill required spec fields for this resource.",
    },
    "RESOURCE_MISSING_REFERENCE": {
        "severity": "ERROR",
        "details_schema": {"missing_refs": []},
        "suggested_fix": "Create referenced resources or update the spec.",
    },
    "RESOURCE_OBJECT_TYPE_CONTRACT_MISSING": {
        "severity": "ERROR",
        "details_schema": {"object_type_id": ""},
        "suggested_fix": "Create the object_type resource with pk_spec and backing_source.",
    },
    "RESOURCE_UNUSED": {
        "severity": "WARN",
        "details_schema": {},
        "suggested_fix": "Remove the resource or reference it from a schema.",
    },
    "REL_MISSING_PREDICATE": {"severity": "ERROR", "details_schema": {"field": "", "related_objects": []}},
    "REL_MISSING_TARGET": {"severity": "ERROR", "details_schema": {"field": "", "related_objects": []}},
    "REL_INVALID_PREDICATE_FORMAT": {"severity": "ERROR", "details_schema": {"field": "", "related_objects": []}},
    "REL_PREDICATE_NAMING_CONVENTION": {"severity": "WARN", "details_schema": {"field": "", "related_objects": []}},
    "REL_INVALID_CARDINALITY": {"severity": "ERROR", "details_schema": {"field": "", "related_objects": []}},
    "REL_CARDINALITY_RECOMMENDATION": {"severity": "WARN", "details_schema": {"field": "", "related_objects": []}},
    "REL_INVALID_TARGET_FORMAT": {"severity": "ERROR", "details_schema": {"field": "", "related_objects": []}},
    "REL_UNKNOWN_TARGET_CLASS": {"severity": "WARN", "details_schema": {"field": "", "related_objects": []}},
    "REL_SELF_REFERENCE_ONE_TO_ONE": {"severity": "WARN", "details_schema": {"field": "", "related_objects": []}},
    "REL_SELF_REFERENCE_DETECTED": {"severity": "INFO", "details_schema": {"field": "", "related_objects": []}},
    "REL_EMPTY_LABEL": {"severity": "WARN", "details_schema": {"field": "", "related_objects": []}},
    "REL_INCOMPATIBLE_CARDINALITIES": {"severity": "ERROR", "details_schema": {"field": "", "related_objects": []}},
    "REL_UNUSUAL_CARDINALITY_PAIR": {"severity": "WARN", "details_schema": {"field": "", "related_objects": []}},
    "REL_MISMATCHED_INVERSE_PREDICATE": {"severity": "ERROR", "details_schema": {"field": "", "related_objects": []}},
    "REL_TARGET_MISMATCH": {"severity": "ERROR", "details_schema": {"field": "", "related_objects": []}},
    "REL_DUPLICATE_RELATIONSHIP": {"severity": "ERROR", "details_schema": {"field": "", "related_objects": []}},
    "REL_DUPLICATE_PREDICATE": {"severity": "ERROR", "details_schema": {"field": "", "related_objects": []}},
    "REL_ISOLATED_CLASS": {"severity": "INFO", "details_schema": {"field": "", "related_objects": []}},
    "REL_EXTERNAL_CLASS_REFERENCE": {"severity": "WARN", "details_schema": {"field": "", "related_objects": []}},
    "REL_GLOBAL_PREDICATE_CONFLICT": {"severity": "WARN", "details_schema": {"field": "", "related_objects": []}},
}

REL_CLASS_LEVEL_CODES = {
    "REL_ISOLATED_CLASS",
    "REL_SELF_REFERENCE_ONE_TO_ONE",
    "REL_SELF_REFERENCE_DETECTED",
}

RESOURCE_REF_TEMPLATES = {
    "object_type": "/ontology/object-types/{id}",
    "link_type": "/ontology/link-types/{id}",
    "__ontology_resource": "/ontology/resources/{id}",
}


def build_object_type_ref(object_id: str) -> str:
    return _build_resource_ref("object_type", object_id)


def build_link_type_ref(link_id: str) -> str:
    return _build_resource_ref("link_type", link_id)


def build_ontology_resource_ref(resource_type: Optional[str], resource_id: Optional[str]) -> str:
    if resource_type and resource_id:
        return _build_resource_ref("__ontology_resource", f"{resource_type}:{resource_id}")
    if resource_id:
        return _build_resource_ref("__ontology_resource", str(resource_id))
    return "__ontology_resource:unknown"


def normalize_issue_code(code: Optional[str], source: Optional[str] = None) -> str:
    raw = (code or "UNKNOWN").strip()
    if raw and raw.startswith(ALLOWED_CODE_PREFIXES):
        return raw
    prefix = CODE_PREFIX_BY_SOURCE.get(source or "")
    if prefix:
        return f"{prefix}{raw}"
    return raw


def normalize_severity(value: Any) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "value"):
        value = getattr(value, "value")
    return str(value).upper()


def normalize_issue(
    *,
    code: Optional[str],
    severity: Optional[str],
    resource_ref: str,
    details: Optional[Dict[str, Any]] = None,
    suggested_fix: Optional[str] = None,
    message: Optional[str] = None,
    source: Optional[str] = None,
) -> Dict[str, Any]:
    normalized_code = normalize_issue_code(code, source)
    defaults = ISSUE_CODE_REGISTRY.get(normalized_code, {})
    normalized_severity = normalize_severity(severity) or defaults.get("severity") or "WARN"
    normalized_details = _enforce_details_schema(normalized_code, details)
    fix = suggested_fix or defaults.get("suggested_fix")

    issue = {
        "code": normalized_code,
        "severity": normalized_severity,
        "resource_ref": resource_ref,
        "details": normalized_details,
    }
    if fix:
        issue["suggested_fix"] = fix
    if message:
        issue["message"] = message
    if source:
        issue["source"] = source
    return issue


def _build_resource_ref(kind: str, resource_id: str) -> str:
    resource_id = str(resource_id or "unknown").strip()
    return f"{kind}:{resource_id}" if resource_id else f"{kind}:unknown"


def _enforce_details_schema(code: str, details: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    payload = dict(details or {})
    schema = ISSUE_CODE_REGISTRY.get(code, {}).get("details_schema")
    if not schema:
        return payload
    for key, default in schema.items():
        if key in payload:
            continue
        if isinstance(default, list):
            payload[key] = []
        elif isinstance(default, dict):
            payload[key] = {}
        else:
            payload[key] = default
    return payload
