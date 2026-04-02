from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from fastapi import status

from objectify_worker.validation_codes import ObjectifyValidationCode as VC
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.services.core.sheet_import_service import FieldMapping
from shared.utils.import_type_normalization import resolve_import_type
from shared.utils.key_spec import extract_payload_key_spec, normalize_object_type_key_spec
from shared.utils.ontology_fields import extract_ontology_fields as extract_ontology_fields_raw
from shared.utils.ontology_type_normalization import normalize_ontology_base_type
from shared.utils.payload_utils import unwrap_data_payload
from shared.utils.blank_utils import is_blank_value
from shared.validators import get_validator
from shared.validators.constraint_validator import ConstraintValidator


def normalize_ontology_payload(payload: Any) -> Dict[str, Any]:
    return unwrap_data_payload(payload)


def extract_ontology_fields(payload: Any) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    return extract_ontology_fields_raw(payload)


def is_blank(value: Any) -> bool:
    return is_blank_value(value)


def normalize_relationship_ref(value: Any, *, target_class: str) -> str:
    if isinstance(value, dict):
        candidate = value.get("@id") or value.get("id")
        if candidate is not None:
            value = candidate
    if isinstance(value, list):
        raise ValueError("Relationship value must be a scalar")
    if value is None:
        raise ValueError("Relationship value is empty")
    raw = str(value).strip()
    if not raw:
        raise ValueError("Relationship value is empty")
    if "/" in raw:
        target, instance_id = raw.split("/", 1)
        if target != target_class:
            raise ValueError(
                f"Relationship target mismatch: expected {target_class}, got {target}"
            )
        if not instance_id.strip():
            raise ValueError("Relationship instance id is empty")
        return f"{target}/{instance_id}"
    return f"{target_class}/{raw}"


def normalize_constraints(
    constraints: Any,
    *,
    raw_type: Optional[Any] = None,
) -> Dict[str, Any]:
    if not isinstance(constraints, dict):
        constraints = {}
    normalized = dict(constraints)

    if "min" in normalized and "minimum" not in normalized:
        normalized["minimum"] = normalized["min"]
    if "max" in normalized and "maximum" not in normalized:
        normalized["maximum"] = normalized["max"]
    if "min_length" in normalized and "minLength" not in normalized:
        normalized["minLength"] = normalized["min_length"]
    if "max_length" in normalized and "maxLength" not in normalized:
        normalized["maxLength"] = normalized["max_length"]
    if "min_items" in normalized and "minItems" not in normalized:
        normalized["minItems"] = normalized["min_items"]
    if "max_items" in normalized and "maxItems" not in normalized:
        normalized["maxItems"] = normalized["max_items"]
    if "unique_items" in normalized and "uniqueItems" not in normalized:
        normalized["uniqueItems"] = normalized["unique_items"]
    if "enum_values" in normalized and "enum" not in normalized:
        normalized["enum"] = normalized["enum_values"]
    if "enumValues" in normalized and "enum" not in normalized:
        normalized["enum"] = normalized["enumValues"]
    if "regex" in normalized and "pattern" not in normalized:
        normalized["pattern"] = normalized["regex"]

    type_hint = str(raw_type or "").strip().lower()
    if type_hint.startswith("xsd:"):
        type_hint = type_hint[4:]
    if "format" not in normalized:
        if type_hint == "email":
            normalized["format"] = "email"
        elif type_hint in {"url", "uri"}:
            normalized["format"] = "uri"
        elif type_hint == "uuid":
            normalized["format"] = "uuid"

    normalized.pop("required", None)
    normalized.pop("nullable", None)
    return normalized


def validate_value_constraints(
    value: Any,
    *,
    constraints: Any,
    raw_type: Optional[Any],
) -> Optional[str]:
    if value is None:
        return None
    if isinstance(constraints, list):
        for constraint_set in constraints:
            message = validate_value_constraints_single(
                value,
                constraints=constraint_set,
                raw_type=raw_type,
            )
            if message:
                return message
        return None
    return validate_value_constraints_single(value, constraints=constraints, raw_type=raw_type)


def validate_value_constraints_single(
    value: Any,
    *,
    constraints: Any,
    raw_type: Optional[Any],
) -> Optional[str]:
    if value is None:
        return None
    normalized = normalize_constraints(constraints, raw_type=raw_type)

    enum_values = normalized.get("enum")
    if enum_values is not None:
        if not isinstance(enum_values, list):
            enum_values = list(enum_values) if isinstance(enum_values, (set, tuple)) else [enum_values]
        if value not in enum_values:
            return f"Value must be one of: {enum_values}"

    type_hint = str(raw_type or "").strip().lower()
    if type_hint.startswith("xsd:"):
        type_hint = type_hint[4:]
    format_hint = str(normalized.get("format") or "").strip().lower()

    canonical_type = normalize_ontology_base_type(type_hint) if type_hint else None

    validator_key = None
    if canonical_type in {
        "array",
        "struct",
        "vector",
        "geopoint",
        "geoshape",
        "cipher",
        "marking",
        "media",
        "attachment",
        "time_series",
    }:
        validator_key = canonical_type
    elif type_hint in {"email", "url", "uri", "uuid", "ip", "phone"}:
        validator_key = "url" if type_hint in {"url", "uri"} else type_hint
    elif format_hint in {"email", "uuid", "uri", "url", "ipv4", "ipv6"}:
        validator_key = "url" if format_hint in {"uri", "url"} else format_hint
        if validator_key in {"ipv4", "ipv6"}:
            validator_key = "ip"
            normalized = dict(normalized)
            normalized.setdefault("version", "4" if format_hint == "ipv4" else "6")

    if validator_key:
        validator = get_validator(validator_key)
        if validator:
            result = validator.validate(value, normalized)
            if not result.is_valid:
                return result.message
            if "format" in normalized:
                normalized = dict(normalized)
                normalized.pop("format", None)

    if normalized:
        result = ConstraintValidator.validate_constraints(value, "unknown", normalized)
        if not result.is_valid:
            return result.message
    return None


def extract_ontology_pk_targets(payload: Any) -> List[str]:
    data = normalize_ontology_payload(payload)
    primary_key, _ = extract_payload_key_spec(data)
    return primary_key


def map_mappings_by_target(mappings: List[FieldMapping]) -> Dict[str, List[str]]:
    mapping: Dict[str, List[str]] = {}
    for item in mappings:
        target = str(item.target_field or "").strip()
        source = str(item.source_field or "").strip()
        if not target or not source:
            continue
        mapping.setdefault(target, []).append(source)
    return mapping


def build_property_type_context(
    *,
    prop_map: Dict[str, Dict[str, Any]],
    value_type_defs: Dict[str, Dict[str, Any]],
    target_class_id: str,
) -> Tuple[
    Dict[str, str],
    Dict[str, Any],
    Dict[str, Optional[Any]],
    set[str],
    set[str],
    List[str],
]:
    resolved_field_types: Dict[str, str] = {}
    field_constraints: Dict[str, Any] = {}
    field_raw_types: Dict[str, Optional[Any]] = {}
    required_targets: set[str] = set()
    explicit_pk_targets: set[str] = set()
    unsupported_targets: List[str] = []
    for name, meta in prop_map.items():
        raw_type = meta.get("type") or meta.get("data_type") or meta.get("datatype")
        raw_type_norm = normalize_ontology_base_type(raw_type) or raw_type
        value_type_ref = str(meta.get("value_type_ref") or meta.get("valueTypeRef") or "").strip() or None
        value_type_spec = value_type_defs.get(value_type_ref) if value_type_ref else None
        value_type_base = None
        value_type_constraints = None
        if value_type_spec:
            value_type_base = value_type_spec.get("base_type") or value_type_spec.get("baseType")
            value_type_constraints = value_type_spec.get("constraints") or value_type_spec.get("constraint") or {}

        if value_type_base and normalize_ontology_base_type(raw_type_norm) in {None, "string"}:
            raw_type_norm = value_type_base

        field_raw_types[name] = raw_type_norm

        is_relationship = bool(
            raw_type_norm == "link"
            or meta.get("isRelationship")
            or meta.get("target")
            or meta.get("linkTarget")
        )
        items = meta.get("items") if isinstance(meta, dict) else None
        if isinstance(items, dict):
            item_type = items.get("type")
            if item_type == "link" and (items.get("target") or items.get("linkTarget")):
                is_relationship = True

        if is_relationship:
            unsupported_targets.append(name)
            continue
        import_type = resolve_import_type(raw_type_norm)
        if not import_type:
            unsupported_targets.append(name)
            continue
        resolved_field_types[name] = import_type
        prop_constraints = normalize_constraints(meta.get("constraints"), raw_type=raw_type_norm)
        base_constraints: Dict[str, Any] = {}
        if normalize_ontology_base_type(raw_type_norm) == "array":
            base_constraints = {"noNullItems": True, "noNestedArrays": True}
        elif normalize_ontology_base_type(raw_type_norm) == "struct":
            base_constraints = {"noNestedStructs": True, "noArrayFields": True}

        constraint_sets: List[Dict[str, Any]] = []
        if base_constraints:
            constraint_sets.append(base_constraints)
        if value_type_constraints:
            constraint_sets.append(normalize_constraints(value_type_constraints, raw_type=value_type_base))
        if prop_constraints:
            constraint_sets.append(prop_constraints)
        field_constraints[name] = constraint_sets if constraint_sets else {}
        if bool(meta.get("required")):
            required_targets.add(name)
        if bool(meta.get("primary_key") or meta.get("primaryKey")):
            explicit_pk_targets.add(name)

    if not explicit_pk_targets:
        expected_pk = f"{target_class_id.lower()}_id"
        if expected_pk in prop_map:
            explicit_pk_targets.add(expected_pk)

    return (
        resolved_field_types,
        field_constraints,
        field_raw_types,
        required_targets,
        explicit_pk_targets,
        unsupported_targets,
    )


async def resolve_object_type_key_contract(
    *,
    job: Any,
    ontology_payload: Any,
    prop_map: Dict[str, Dict[str, Any]],
    fail_job: Any,
    warnings: List[Dict[str, Any]],
    fetch_object_type_contract: Callable[[Any], Awaitable[Dict[str, Any]]],
    ontology_pk_validation_mode: str,
) -> Tuple[List[str], List[str], List[List[str]], List[str], set[str]]:
    object_type_resource = await fetch_object_type_contract(job)
    object_type_data = object_type_resource.get("data") if isinstance(object_type_resource, dict) else None
    if not isinstance(object_type_data, dict):
        object_type_data = object_type_resource if isinstance(object_type_resource, dict) else {}
    object_type_spec = object_type_data.get("spec") if isinstance(object_type_data.get("spec"), dict) else {}
    if not object_type_spec:
        await fail_job(
            "validation_failed",
            report={
                "errors": [
                    {
                        "code": VC.OBJECT_TYPE_CONTRACT_MISSING.value,
                        "message": "Object type contract is required for objectify",
                    }
                ]
            },
        )
    status_value = str(object_type_spec.get("status") or "ACTIVE").strip().upper()
    if status_value != "ACTIVE":
        await fail_job(
            "validation_failed",
            report={
                "errors": [
                    {
                        "code": VC.OBJECT_TYPE_INACTIVE.value,
                        "status": status_value,
                        "message": "Object type contract is not active",
                    }
                ]
            },
        )
    object_type_key_spec = normalize_object_type_key_spec(
        object_type_spec,
        columns=list(prop_map.keys()),
    )
    object_type_pk_targets = [str(v).strip() for v in object_type_key_spec.get("primary_key") or [] if str(v).strip()]
    object_type_title_targets = [str(v).strip() for v in object_type_key_spec.get("title_key") or [] if str(v).strip()]
    object_type_unique_keys = [
        [str(v).strip() for v in key if str(v).strip()]
        for key in object_type_key_spec.get("unique_keys") or []
        if isinstance(key, list)
    ]
    object_type_required_fields = [str(v).strip() for v in object_type_key_spec.get("required_fields") or [] if str(v).strip()]
    object_type_nullable_fields = {str(v).strip() for v in object_type_key_spec.get("nullable_fields") or [] if str(v).strip()}

    if not object_type_pk_targets:
        await fail_job(
            "validation_failed",
            report={
                "errors": [
                    {
                        "code": VC.OBJECT_TYPE_PRIMARY_KEY_MISSING.value,
                        "message": "Object type pk_spec.primary_key is required",
                    }
                ]
            },
        )
    if not object_type_title_targets:
        await fail_job(
            "validation_failed",
            report={
                "errors": [
                    {
                        "code": VC.OBJECT_TYPE_TITLE_KEY_MISSING.value,
                        "message": "Object type pk_spec.title_key is required",
                    }
                ]
            },
        )

    missing_contract_fields = sorted(
        {
            *object_type_pk_targets,
            *object_type_title_targets,
            *object_type_required_fields,
        }
        - set(prop_map.keys())
    )
    if missing_contract_fields:
        await fail_job(
            "validation_failed",
            report={
                "errors": [
                    {
                        "code": VC.OBJECT_TYPE_KEY_FIELDS_MISSING.value,
                        "fields": missing_contract_fields,
                        "message": "Object type key spec fields missing from ontology schema",
                    }
                ]
            },
        )

    ontology_pk_targets = extract_ontology_pk_targets(ontology_payload)
    if ontology_pk_targets and ontology_pk_targets != object_type_pk_targets:
        issue = {
            "code": VC.ONTOLOGY_OBJECT_TYPE_PRIMARY_KEY_MISMATCH.value,
            "expected": object_type_pk_targets,
            "observed": ontology_pk_targets,
            "message": "Ontology primaryKey does not match object_type pk_spec (fields+order)",
        }
        if ontology_pk_validation_mode == "fail":
            await fail_job("validation_failed", report={"errors": [issue]})
        if ontology_pk_validation_mode == "warn":
            warnings.append(issue)

    return (
        object_type_pk_targets,
        object_type_title_targets,
        object_type_unique_keys,
        object_type_required_fields,
        object_type_nullable_fields,
    )
