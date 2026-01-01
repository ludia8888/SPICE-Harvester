"""
Ontology resource validation (required spec + reference checks).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from oms.services.async_terminus import AsyncTerminusService
from oms.services.ontology_resources import OntologyResourceService, normalize_resource_type

logger = logging.getLogger(__name__)


class ResourceSpecError(ValueError):
    """Raised when resource spec is invalid or missing required fields."""


class ResourceReferenceError(ValueError):
    """Raised when resource spec references missing entities."""


_SPEC_ALIASES = {
    "returnTypeRef": "return_type_ref",
    "return_type": "return_type_ref",
    "returnType": "return_type_ref",
    "baseType": "base_type",
    "pkSpec": "pk_spec",
    "backingSource": "backing_source",
    "inputSchema": "input_schema",
    "permissionPolicy": "permission_policy",
    "requiredProperties": "required_properties",
    "requiredRelationships": "required_relationships",
    "sideEffects": "side_effects",
    "deterministic": "deterministic",
    "language": "language",
}

_REFERENCE_KEYS = {
    "return_type_ref",
    "returnTypeRef",
    "type_ref",
    "typeRef",
    "value_type_ref",
    "valueTypeRef",
    "object_type_ref",
    "objectTypeRef",
    "interface_ref",
    "interfaceRef",
    "shared_property_ref",
    "sharedPropertyRef",
}

_REFERENCE_SKIP_PREFIXES = ("xsd:", "sys:", "rdf:", "owl:")

_PRIMITIVE_BASE_TYPES = {
    "string",
    "text",
    "integer",
    "int",
    "number",
    "decimal",
    "float",
    "double",
    "boolean",
    "date",
    "datetime",
    "time",
}

_REFERENCE_TYPE_PREFIX = {
    "value_type:": "value_type",
    "value:": "value_type",
    "interface:": "interface",
    "shared_property:": "shared_property",
    "shared:": "shared_property",
    "object_type:": "object_type",
    "object:": "object",
    "class:": "object",
}


def _normalize_spec(spec: Any) -> Dict[str, Any]:
    if spec is None:
        return {}
    if not isinstance(spec, dict):
        raise ResourceSpecError("spec must be an object")

    normalized: Dict[str, Any] = {}
    for key, value in spec.items():
        canonical = _SPEC_ALIASES.get(key, key)
        normalized[canonical] = value
    return normalized


def _merge_payload_spec(payload: Dict[str, Any]) -> Dict[str, Any]:
    spec = payload.get("spec") if isinstance(payload, dict) else None
    normalized = _normalize_spec(spec)
    for key, value in (payload or {}).items():
        if key in {"id", "label", "description", "spec", "metadata"}:
            continue
        canonical = _SPEC_ALIASES.get(key, key)
        normalized.setdefault(canonical, value)
    return normalized


def _extract_reference_values(value: Any, *, keys: Set[str], parent_is_ref: bool = False) -> List[str]:
    refs: List[str] = []
    if isinstance(value, dict):
        for k, v in value.items():
            refs.extend(_extract_reference_values(v, keys=keys, parent_is_ref=k in keys))
    elif isinstance(value, list):
        for item in value:
            refs.extend(_extract_reference_values(item, keys=keys, parent_is_ref=parent_is_ref))
    elif isinstance(value, str) and parent_is_ref:
        refs.append(value)
    return refs


def collect_reference_values(spec: Dict[str, Any]) -> List[str]:
    return _extract_reference_values(spec or {}, keys=_REFERENCE_KEYS)


def check_required_fields(resource_type: str, spec: Dict[str, Any]) -> List[Dict[str, Any]]:
    normalized_type = normalize_resource_type(resource_type)
    return _collect_required_field_issues(normalized_type, spec)


async def find_missing_references(
    *,
    db_name: str,
    resource_type: str,
    payload: Dict[str, Any],
    terminus: AsyncTerminusService,
    branch: str,
) -> List[str]:
    normalized_type = normalize_resource_type(resource_type)
    spec = _merge_payload_spec(payload)
    refs = collect_reference_values(spec)

    base_type = spec.get("base_type")
    if isinstance(base_type, str) and base_type.strip() and not _is_primitive_reference(base_type):
        refs.append(base_type.strip())

    resources = OntologyResourceService(terminus)
    missing: List[str] = []
    for raw_ref in refs:
        ref_type, ref = _canonicalize_ref(raw_ref)
        if not ref:
            continue
        exists = await _reference_exists(
            terminus=terminus,
            resources=resources,
            db_name=db_name,
            branch=branch,
            ref_type=ref_type,
            ref=ref,
        )
        if not exists:
            missing.append(raw_ref)

    return sorted(set(missing))


def _canonicalize_ref(raw: str) -> Tuple[Optional[str], Optional[str]]:
    ref = (raw or "").strip()
    if not ref:
        return None, None
    for prefix, ref_type in _REFERENCE_TYPE_PREFIX.items():
        if ref.startswith(prefix):
            return ref_type, ref[len(prefix) :].strip()
    return None, ref


def _is_primitive_reference(value: str) -> bool:
    lowered = value.lower()
    if lowered.startswith("primitive:"):
        base = lowered.split(":", 1)[1]
        return base in _PRIMITIVE_BASE_TYPES
    if lowered in _PRIMITIVE_BASE_TYPES:
        return True
    return any(lowered.startswith(prefix) for prefix in _REFERENCE_SKIP_PREFIXES)


def _validate_required_fields(resource_type: str, spec: Dict[str, Any]) -> None:
    issues = _collect_required_field_issues(resource_type, spec)
    if issues:
        raise ResourceSpecError(issues[0]["message"])


def _collect_required_field_issues(resource_type: str, spec: Dict[str, Any]) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    if resource_type == "value_type":
        base_type = spec.get("base_type")
        if not isinstance(base_type, str) or not base_type.strip():
            _append_spec_issue(
                issues,
                message="value_type requires non-empty base_type",
                missing_fields=["base_type"],
            )
    elif resource_type == "object_type":
        pk_spec = spec.get("pk_spec")
        backing_source = spec.get("backing_source")
        if not isinstance(pk_spec, dict) or not pk_spec:
            _append_spec_issue(
                issues,
                message="object_type requires non-empty pk_spec",
                missing_fields=["pk_spec"],
            )
        if not isinstance(backing_source, dict) or not backing_source:
            _append_spec_issue(
                issues,
                message="object_type requires non-empty backing_source",
                missing_fields=["backing_source"],
            )
        if isinstance(backing_source, dict):
            if not str(backing_source.get("kind") or "").strip():
                _append_spec_issue(
                    issues,
                    message="object_type backing_source requires kind",
                    missing_fields=["backing_source.kind"],
                )
            if not str(backing_source.get("ref") or "").strip():
                _append_spec_issue(
                    issues,
                    message="object_type backing_source requires ref",
                    missing_fields=["backing_source.ref"],
                )
            if not str(backing_source.get("schema_hash") or "").strip():
                _append_spec_issue(
                    issues,
                    message="object_type backing_source requires schema_hash",
                    missing_fields=["backing_source.schema_hash"],
                )
    elif resource_type == "function":
        expr = spec.get("expression") or spec.get("dsl")
        if not isinstance(expr, str) or not expr.strip():
            _append_spec_issue(
                issues,
                message="function requires expression (or dsl)",
                missing_fields=["expression"],
            )
        return_ref = spec.get("return_type_ref")
        if not isinstance(return_ref, str) or not return_ref.strip():
            _append_spec_issue(
                issues,
                message="function requires return_type_ref",
                missing_fields=["return_type_ref"],
            )
        deterministic = spec.get("deterministic")
        if not isinstance(deterministic, bool):
            _append_spec_issue(
                issues,
                message="function requires deterministic=true|false",
                invalid_fields=["deterministic"],
            )
    elif resource_type == "action_type":
        input_schema = spec.get("input_schema")
        if not isinstance(input_schema, dict) or not input_schema:
            _append_spec_issue(
                issues,
                message="action_type requires non-empty input_schema object",
                missing_fields=["input_schema"],
            )
        if not any(key in input_schema for key in ("fields", "properties", "schema")):
            _append_spec_issue(
                issues,
                message="action_type input_schema must include fields/properties/schema",
                invalid_fields=["input_schema"],
            )
        permission_policy = spec.get("permission_policy")
        if not isinstance(permission_policy, dict) or not permission_policy:
            _append_spec_issue(
                issues,
                message="action_type requires non-empty permission_policy object",
                missing_fields=["permission_policy"],
            )
        if not any(key in permission_policy for key in ("roles", "scopes", "policy", "rules")):
            _append_spec_issue(
                issues,
                message="action_type permission_policy must include roles/scopes/policy/rules",
                missing_fields=["permission_policy.roles|scopes|policy|rules"],
            )
        issues.extend(_collect_permission_policy_issues(permission_policy))
    elif resource_type == "interface":
        props = spec.get("required_properties")
        rels = spec.get("required_relationships")
        has_props = isinstance(props, list) and bool(props)
        has_rels = isinstance(rels, list) and bool(rels)
        if not has_props and not has_rels:
            _append_spec_issue(
                issues,
                message="interface requires required_properties or required_relationships",
                missing_fields=["required_properties", "required_relationships"],
            )
        if props is not None and not isinstance(props, list):
            _append_spec_issue(
                issues,
                message="interface required_properties must be a list",
                invalid_fields=["required_properties"],
            )
        if rels is not None and not isinstance(rels, list):
            _append_spec_issue(
                issues,
                message="interface required_relationships must be a list",
                invalid_fields=["required_relationships"],
            )
        if has_props:
            issues.extend(
                _collect_required_items_issues(
                    props, item_name="required_properties", name_keys=("name",)
                )
            )
        if has_rels:
            issues.extend(
                _collect_required_items_issues(
                    rels, item_name="required_relationships", name_keys=("predicate", "name")
                )
            )
    elif resource_type == "shared_property":
        props = spec.get("properties")
        if not isinstance(props, list) or not props:
            _append_spec_issue(
                issues,
                message="shared_property requires properties list",
                missing_fields=["properties"],
            )
        for prop in props:
            if not isinstance(prop, dict) or not prop.get("name"):
                _append_spec_issue(
                    issues,
                    message="shared_property properties must include name",
                    invalid_fields=["properties"],
                )
    elif resource_type == "group":
        return issues
    else:
        _append_spec_issue(
            issues,
            message=f"Unsupported resource_type: {resource_type}",
            invalid_fields=["resource_type"],
        )
    return issues


def _collect_required_items_issues(
    items: List[Any],
    *,
    item_name: str,
    name_keys: Tuple[str, ...],
) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    for item in items:
        if isinstance(item, str):
            if not item.strip():
                _append_spec_issue(
                    issues,
                    message=f"interface {item_name} must be non-empty strings",
                    invalid_fields=[item_name],
                )
            continue
        if isinstance(item, dict):
            name = None
            for key in name_keys:
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    name = value.strip()
                    break
            if not name:
                _append_spec_issue(
                    issues,
                    message=f"interface {item_name} requires {name_keys[0]}",
                    invalid_fields=[item_name],
                )
            continue
        _append_spec_issue(
            issues,
            message=f"interface {item_name} must be strings or objects",
            invalid_fields=[item_name],
        )
    return issues


def _collect_permission_policy_issues(policy: Dict[str, Any]) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    has_policy = False
    roles = policy.get("roles")
    scopes = policy.get("scopes")
    rules = policy.get("rules")
    policy_text = policy.get("policy")

    if roles is not None:
        if _validate_string_list(roles, field_name="permission_policy.roles"):
            has_policy = True
        else:
            _append_spec_issue(
                issues,
                message="permission_policy.roles must be a list of non-empty strings",
                invalid_fields=["permission_policy.roles"],
            )
    if scopes is not None:
        if _validate_string_list(scopes, field_name="permission_policy.scopes"):
            has_policy = True
        else:
            _append_spec_issue(
                issues,
                message="permission_policy.scopes must be a list of non-empty strings",
                invalid_fields=["permission_policy.scopes"],
            )
    if rules is not None:
        if isinstance(rules, list):
            if not rules:
                _append_spec_issue(
                    issues,
                    message="permission_policy.rules must be non-empty",
                    invalid_fields=["permission_policy.rules"],
                )
            else:
                has_policy = True
        elif isinstance(rules, dict):
            if not rules:
                _append_spec_issue(
                    issues,
                    message="permission_policy.rules must be non-empty",
                    invalid_fields=["permission_policy.rules"],
                )
            else:
                has_policy = True
        else:
            _append_spec_issue(
                issues,
                message="permission_policy.rules must be object or list",
                invalid_fields=["permission_policy.rules"],
            )
    if policy_text is not None:
        if not isinstance(policy_text, str) or not policy_text.strip():
            _append_spec_issue(
                issues,
                message="permission_policy.policy must be non-empty string",
                invalid_fields=["permission_policy.policy"],
            )
        else:
            has_policy = True
    if not has_policy:
        _append_spec_issue(
            issues,
            message="action_type permission_policy must define non-empty roles/scopes/policy/rules",
            missing_fields=["permission_policy.roles|scopes|policy|rules"],
        )
    return issues


def _validate_string_list(value: Any, *, field_name: str) -> bool:
    if isinstance(value, str):
        return False
    if not isinstance(value, list):
        return False
    if not all(isinstance(item, str) and item.strip() for item in value):
        return False
    return True


def _append_spec_issue(
    issues: List[Dict[str, Any]],
    *,
    message: str,
    missing_fields: Optional[List[str]] = None,
    invalid_fields: Optional[List[str]] = None,
) -> None:
    issues.append(
        {
            "code": "RESOURCE_SPEC_INVALID",
            "message": message,
            "details": {
                "missing_fields": missing_fields or [],
                "invalid_fields": invalid_fields or [],
            },
        }
    )


async def _reference_exists(
    *,
    terminus: AsyncTerminusService,
    resources: OntologyResourceService,
    db_name: str,
    branch: str,
    ref_type: Optional[str],
    ref: str,
) -> bool:
    if _is_primitive_reference(ref):
        return True

    if ref_type == "value_type":
        return bool(
            await resources.get_resource(db_name, branch=branch, resource_type="value_type", resource_id=ref)
        )
    if ref_type == "interface":
        return bool(
            await resources.get_resource(db_name, branch=branch, resource_type="interface", resource_id=ref)
        )
    if ref_type == "shared_property":
        return bool(
            await resources.get_resource(db_name, branch=branch, resource_type="shared_property", resource_id=ref)
        )
    if ref_type == "object_type":
        return bool(
            await resources.get_resource(db_name, branch=branch, resource_type="object_type", resource_id=ref)
        )
    if ref_type == "object":
        return bool(await terminus.get_ontology(db_name, ref, branch=branch))

    if await terminus.get_ontology(db_name, ref, branch=branch):
        return True
    if await resources.get_resource(db_name, branch=branch, resource_type="value_type", resource_id=ref):
        return True
    if await resources.get_resource(db_name, branch=branch, resource_type="interface", resource_id=ref):
        return True
    if await resources.get_resource(db_name, branch=branch, resource_type="shared_property", resource_id=ref):
        return True
    return False


async def validate_resource(
    *,
    db_name: str,
    resource_type: str,
    payload: Dict[str, Any],
    terminus: AsyncTerminusService,
    branch: str,
    expected_head_commit: Optional[str] = None,
    strict: bool = True,
) -> Dict[str, Any]:
    normalized_type = normalize_resource_type(resource_type)
    spec = _merge_payload_spec(payload)
    _validate_required_fields(normalized_type, spec)

    missing = await find_missing_references(
        db_name=db_name,
        resource_type=normalized_type,
        payload=payload,
        terminus=terminus,
        branch=branch,
    )

    if missing:
        message = f"Missing referenced types: {', '.join(missing)}"
        if strict:
            raise ResourceReferenceError(message)
        logger.warning("Ontology resource reference validation (lenient): %s", message)

    return spec
