from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from shared.security.input_sanitizer import (
    SecurityViolationError,
    input_sanitizer,
    validate_class_id,
    validate_instance_id,
)


class ActionInputSchemaError(ValueError):
    pass


class ActionInputValidationError(ValueError):
    pass


_RESERVED_INTERNAL_KEYS: set[str] = {
    "base_token",
    "observed_base",
    "applied_changes",
    "conflict",
    "patchset_commit_id",
    "action_applied_seq",
}


@dataclass(frozen=True)
class _FieldSpec:
    name: str
    type: str
    required: bool
    object_type: Optional[str]
    enum: Optional[List[Any]]
    min_length: Optional[int]
    max_length: Optional[int]
    min_value: Optional[float]
    max_value: Optional[float]
    items: Optional[Dict[str, Any]]
    properties: Optional[Dict[str, Any]]


def _json_size_bytes(value: Any) -> int:
    try:
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    except Exception:
        payload = json.dumps(str(value), ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return len(payload.encode("utf-8"))


def _walk_keys(value: Any) -> Iterable[str]:
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            for k, v in item.items():
                if isinstance(k, str):
                    yield k
                stack.append(v)
        elif isinstance(item, list):
            stack.extend(item)


def _require_public_key(name: str) -> None:
    if not isinstance(name, str) or not name.strip():
        raise ActionInputSchemaError("field name must be a non-empty string")
    # Reuse internal field name sanitizer (no Unicode/spaces).
    input_sanitizer.sanitize_field_name(name.strip())


def _normalize_field_spec(raw: Dict[str, Any]) -> _FieldSpec:
    if not isinstance(raw, dict):
        raise ActionInputSchemaError("input_schema.fields items must be objects")
    name = raw.get("name")
    _require_public_key(str(name or ""))
    name = str(name).strip()

    raw_type = str(raw.get("type") or "").strip()
    if not raw_type:
        raise ActionInputSchemaError(f"input_schema.fields[{name}].type is required")
    field_type = raw_type.lower()
    if field_type not in {"string", "integer", "number", "boolean", "object_ref", "list", "object"}:
        raise ActionInputSchemaError(f"unsupported input_schema field type: {raw_type}")

    required = bool(raw.get("required") is True)
    object_type = str(raw.get("object_type") or "").strip() or None
    if object_type:
        validate_class_id(object_type)

    enum = raw.get("enum")
    if enum is not None and not isinstance(enum, list):
        raise ActionInputSchemaError(f"input_schema.fields[{name}].enum must be a list")

    min_length = raw.get("min_length")
    max_length = raw.get("max_length")
    if min_length is not None:
        if not isinstance(min_length, int) or min_length < 0:
            raise ActionInputSchemaError(f"input_schema.fields[{name}].min_length must be a non-negative integer")
    if max_length is not None:
        if not isinstance(max_length, int) or max_length < 0:
            raise ActionInputSchemaError(f"input_schema.fields[{name}].max_length must be a non-negative integer")
    if min_length is not None and max_length is not None and min_length > max_length:
        raise ActionInputSchemaError(f"input_schema.fields[{name}].min_length cannot exceed max_length")

    min_value = raw.get("min")
    max_value = raw.get("max")
    if min_value is not None and not isinstance(min_value, (int, float)) or isinstance(min_value, bool):
        raise ActionInputSchemaError(f"input_schema.fields[{name}].min must be a number")
    if max_value is not None and not isinstance(max_value, (int, float)) or isinstance(max_value, bool):
        raise ActionInputSchemaError(f"input_schema.fields[{name}].max must be a number")
    if min_value is not None and max_value is not None and float(min_value) > float(max_value):
        raise ActionInputSchemaError(f"input_schema.fields[{name}].min cannot exceed max")

    items = raw.get("items")
    if field_type == "list":
        if not isinstance(items, dict) or not items:
            raise ActionInputSchemaError(f"input_schema.fields[{name}].items is required for list types")
    elif items is not None:
        raise ActionInputSchemaError(f"input_schema.fields[{name}].items is only allowed for list types")

    properties = raw.get("properties")
    if field_type == "object":
        if not isinstance(properties, dict) or not properties:
            raise ActionInputSchemaError(f"input_schema.fields[{name}].properties is required for object types")
    elif properties is not None:
        raise ActionInputSchemaError(f"input_schema.fields[{name}].properties is only allowed for object types")

    return _FieldSpec(
        name=name,
        type=field_type,
        required=required,
        object_type=object_type,
        enum=list(enum) if isinstance(enum, list) else None,
        min_length=int(min_length) if isinstance(min_length, int) else None,
        max_length=int(max_length) if isinstance(max_length, int) else None,
        min_value=float(min_value) if isinstance(min_value, (int, float)) and not isinstance(min_value, bool) else None,
        max_value=float(max_value) if isinstance(max_value, (int, float)) and not isinstance(max_value, bool) else None,
        items=dict(items) if isinstance(items, dict) else None,
        properties=dict(properties) if isinstance(properties, dict) else None,
    )


def normalize_input_schema(input_schema: Any) -> Tuple[List[_FieldSpec], bool]:
    """
    Normalize an ActionType input_schema definition.

    Returns:
      (fields, allow_extra_fields)
    """
    if not isinstance(input_schema, dict) or not input_schema:
        raise ActionInputSchemaError("input_schema must be a non-empty object")

    allow_extra_fields = bool(input_schema.get("allow_extra_fields") is True)

    fields = input_schema.get("fields")
    if fields is None:
        # Support a minimal "properties" alias (JSON-schema-like).
        props = input_schema.get("properties")
        if isinstance(props, dict) and props:
            fields = [{"name": k, **(v if isinstance(v, dict) else {"type": v})} for k, v in props.items()]
    if not isinstance(fields, list) or not fields:
        raise ActionInputSchemaError("input_schema.fields must be a non-empty list")

    normalized: List[_FieldSpec] = []
    names: set[str] = set()
    for item in fields:
        spec = _normalize_field_spec(item)
        if spec.name in names:
            raise ActionInputSchemaError(f"duplicate input_schema field: {spec.name}")
        names.add(spec.name)
        normalized.append(spec)

    return normalized, allow_extra_fields


def validate_action_input(
    *,
    input_schema: Any,
    payload: Any,
    max_total_bytes: int = 200_000,
) -> Dict[str, Any]:
    """
    Validate and normalize an Action submission payload against ActionType.input_schema.

    Notes:
    - This is a strict validator (safety-first): unknown fields are rejected by default.
    - It also rejects known internal patchset keys anywhere in the payload.
    """
    if _json_size_bytes(payload) > int(max_total_bytes):
        raise ActionInputValidationError("input payload too large")

    if not isinstance(payload, dict):
        raise ActionInputValidationError("input payload must be an object")

    reserved = sorted({k for k in _walk_keys(payload) if k in _RESERVED_INTERNAL_KEYS})
    if reserved:
        raise ActionInputValidationError(f"input payload contains reserved keys: {', '.join(reserved)}")

    fields, allow_extra = normalize_input_schema(input_schema)
    field_map = {f.name: f for f in fields}

    out: Dict[str, Any] = {}

    for field in fields:
        name = field.name
        if name not in payload:
            if field.required:
                raise ActionInputValidationError(f"missing required input field: {name}")
            continue
        value = payload.get(name)
        if value is None:
            if field.required:
                raise ActionInputValidationError(f"missing required input field: {name}")
            out[name] = None
            continue

        try:
            out[name] = _validate_value(field=field, value=value)
        except SecurityViolationError as exc:
            raise ActionInputValidationError(str(exc)) from exc

    if allow_extra:
        for k, v in payload.items():
            if k in out:
                continue
            out[k] = v
        return out

    unknown = sorted({k for k in payload.keys() if k not in field_map})
    if unknown:
        raise ActionInputValidationError(f"unknown input fields: {', '.join(unknown)}")

    return out


def _validate_value(*, field: _FieldSpec, value: Any) -> Any:
    t = field.type
    if t == "string":
        if not isinstance(value, str):
            raise ActionInputValidationError(f"field '{field.name}' must be a string")
        if field.min_length is not None and len(value) < field.min_length:
            raise ActionInputValidationError(f"field '{field.name}' is too short")
        if field.max_length is not None and len(value) > field.max_length:
            raise ActionInputValidationError(f"field '{field.name}' is too long")
        if field.enum is not None and value not in field.enum:
            raise ActionInputValidationError(f"field '{field.name}' must be one of {field.enum}")
        return value

    if t == "boolean":
        if not isinstance(value, bool):
            raise ActionInputValidationError(f"field '{field.name}' must be a boolean")
        return value

    if t == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            raise ActionInputValidationError(f"field '{field.name}' must be an integer")
        if field.min_value is not None and float(value) < field.min_value:
            raise ActionInputValidationError(f"field '{field.name}' must be >= {field.min_value}")
        if field.max_value is not None and float(value) > field.max_value:
            raise ActionInputValidationError(f"field '{field.name}' must be <= {field.max_value}")
        return value

    if t == "number":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ActionInputValidationError(f"field '{field.name}' must be a number")
        fv = float(value)
        if field.min_value is not None and fv < field.min_value:
            raise ActionInputValidationError(f"field '{field.name}' must be >= {field.min_value}")
        if field.max_value is not None and fv > field.max_value:
            raise ActionInputValidationError(f"field '{field.name}' must be <= {field.max_value}")
        return value

    if t == "object_ref":
        if not isinstance(value, dict):
            raise ActionInputValidationError(f"field '{field.name}' must be an object_ref")
        class_id = validate_class_id(str(value.get("class_id") or ""))
        instance_id = validate_instance_id(str(value.get("instance_id") or ""))
        if field.object_type and class_id != field.object_type:
            raise ActionInputValidationError(
                f"field '{field.name}' must reference object_type '{field.object_type}'"
            )
        return {"class_id": class_id, "instance_id": instance_id}

    if t == "list":
        if not isinstance(value, list):
            raise ActionInputValidationError(f"field '{field.name}' must be a list")
        item_schema = field.items or {}
        item_type = str(item_schema.get("type") or "").strip()
        if not item_type:
            raise ActionInputValidationError(f"field '{field.name}'.items.type is required")
        # Recurse using a single-field schema.
        item_field = _normalize_field_spec({"name": "item", **item_schema})
        out: List[Any] = []
        for item in value:
            out.append(_validate_value(field=item_field, value=item))
        return out

    if t == "object":
        if not isinstance(value, dict):
            raise ActionInputValidationError(f"field '{field.name}' must be an object")
        props = field.properties or {}
        if not isinstance(props, dict) or not props:
            raise ActionInputValidationError(f"field '{field.name}'.properties is required")
        # Minimal: validate only declared properties and reject unknown.
        validated: Dict[str, Any] = {}
        for key, schema in props.items():
            _require_public_key(str(key or ""))
            prop_schema = schema if isinstance(schema, dict) else {"type": schema}
            prop_field = _normalize_field_spec({"name": str(key), **prop_schema})
            if prop_field.name in value:
                validated[prop_field.name] = _validate_value(field=prop_field, value=value.get(prop_field.name))
            elif prop_field.required:
                raise ActionInputValidationError(f"missing required input field: {field.name}.{prop_field.name}")

        unknown = sorted({k for k in value.keys() if k not in props})
        if unknown:
            raise ActionInputValidationError(f"unknown input fields: {field.name}.{', '.join(unknown)}")
        return validated

    raise ActionInputValidationError(f"unsupported field type: {field.type}")

