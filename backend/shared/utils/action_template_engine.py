from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from shared.security.input_sanitizer import (
    SecurityViolationError,
    input_sanitizer,
    validate_class_id,
    validate_instance_id,
)
from shared.utils.time_utils import utcnow


class ActionImplementationError(ValueError):
    pass


_ALLOWED_IMPLEMENTATION_TYPES = {"template_v1", "template_v2", "function_v1"}
_ALLOWED_REF_ROOTS = {"input", "user", "target"}
_V2_EXECUTABLE_TYPES = {"template_v2", "function_v1"}

_ALLOWED_EXPR_OPERATORS = {"$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$and", "$or", "$not"}
_ALLOWED_VALUE_DIRECTIVES_V2 = {"$if", "$switch", "$call"}

# Mustache/Handlebars syntax detection — these are NOT supported by the $ref engine.
_MUSTACHE_PATTERN = re.compile(r"\{\{.*?\}\}")


def _detect_mustache_syntax(value: Any, *, label: str) -> None:
    """Reject mustache/handlebars syntax in template values.

    SPICE uses ``{"$ref": "input.field_name"}`` for dynamic value resolution.
    Mustache-style ``{{input.field_name}}`` is silently treated as a literal string
    by ``_resolve_value()``, leading to incorrect data being persisted.
    """
    if isinstance(value, str) and _MUSTACHE_PATTERN.search(value):
        raise ActionImplementationError(
            f"{label} contains mustache syntax '{value}'. "
            f'Use {{"$ref": "input.field_name"}} instead.'
        )
    if isinstance(value, list):
        for i, item in enumerate(value):
            _detect_mustache_syntax(item, label=f"{label}[{i}]")
    if isinstance(value, dict):
        # Skip $ref / $now objects — they are valid template directives.
        if len(value) == 1 and ("$ref" in value or "$now" in value):
            return
        for k, v in value.items():
            _detect_mustache_syntax(v, label=f"{label}.{k}")


def _is_non_empty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _require_public_identifier(value: str, *, label: str) -> str:
    text = str(value or "").strip()
    if not text or text.startswith("_"):
        raise ActionImplementationError(f"{label} must be a public identifier")
    try:
        input_sanitizer.sanitize_field_name(text)
    except SecurityViolationError as exc:
        raise ActionImplementationError(f"{label} is invalid: {exc}") from exc
    return text


def _split_dotted_path(path: Any, *, label: str) -> List[str]:
    raw = str(path or "").strip()
    if not raw:
        raise ActionImplementationError(f"{label} is required")
    parts = [p for p in raw.split(".") if p != ""]
    if len(parts) < 2:
        raise ActionImplementationError(f"{label} must be a dotted path like 'input.ticket'")
    root = parts[0]
    if root not in _ALLOWED_REF_ROOTS:
        raise ActionImplementationError(f"{label} must start with one of: {', '.join(sorted(_ALLOWED_REF_ROOTS))}")
    for idx, part in enumerate(parts[1:], start=1):
        _require_public_identifier(part, label=f"{label}[{idx}]")
    return parts


def _get_by_path(obj: Any, path: List[str], *, label: str) -> Any:
    cur = obj
    for idx, key in enumerate(path):
        if not isinstance(cur, dict):
            raise ActionImplementationError(f"{label} path segment '{key}' does not resolve on a non-object")
        if key not in cur:
            raise ActionImplementationError(f"{label} path segment '{key}' is missing")
        cur = cur.get(key)
    return cur


def _normalize_object_ref(value: Any, *, label: str) -> Dict[str, str]:
    if not isinstance(value, dict):
        raise ActionImplementationError(f"{label} must be an object_ref")
    try:
        class_id = validate_class_id(str(value.get("class_id") or ""))
        instance_id = validate_instance_id(str(value.get("instance_id") or ""))
    except SecurityViolationError as exc:
        raise ActionImplementationError(f"{label} contains invalid identifiers: {exc}") from exc
    return {"class_id": class_id, "instance_id": instance_id}


def _coerce_link_value(value: Any, *, label: str) -> str:
    if isinstance(value, dict) and {"class_id", "instance_id"} <= set(value.keys()):
        ref = _normalize_object_ref(value, label=label)
        return f"{ref['class_id']}:{ref['instance_id']}"
    if not _is_non_empty_str(value):
        raise ActionImplementationError(f"{label} must be a non-empty string (or object_ref)")
    return str(value).strip()


def _is_ref_object(value: Any) -> bool:
    return isinstance(value, dict) and len(value) == 1 and ("$ref" in value or "$now" in value)


def _is_expression_object(value: Any) -> bool:
    if not isinstance(value, dict) or len(value) != 1:
        return False
    key = next(iter(value.keys()))
    if not isinstance(key, str):
        return False
    return key in _ALLOWED_VALUE_DIRECTIVES_V2 or key in _ALLOWED_EXPR_OPERATORS


def _fn_concat(*args: Any) -> str:
    return "".join("" if a is None else str(a) for a in args)


def _fn_coalesce(*args: Any) -> Any:
    for value in args:
        if value is not None:
            return value
    return None


def _fn_upper(value: Any) -> str:
    return str(value or "").upper()


def _fn_lower(value: Any) -> str:
    return str(value or "").lower()


def _fn_trim(value: Any) -> str:
    return str(value or "").strip()


def _coerce_number(value: Any, *, label: str) -> float:
    if isinstance(value, bool):
        raise ActionImplementationError(f"{label} must be a number")
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except Exception as exc:
        raise ActionImplementationError(f"{label} must be a number") from exc


def _fn_add(*args: Any) -> float:
    return sum(_coerce_number(v, label="add") for v in args)


def _fn_sub(*args: Any) -> float:
    if not args:
        raise ActionImplementationError("$call sub requires at least one argument")
    head = _coerce_number(args[0], label="sub")
    if len(args) == 1:
        return -head
    for value in args[1:]:
        head -= _coerce_number(value, label="sub")
    return head


def _fn_mul(*args: Any) -> float:
    result = 1.0
    for value in args:
        result *= _coerce_number(value, label="mul")
    return result


def _fn_div(*args: Any) -> float:
    if len(args) < 2:
        raise ActionImplementationError("$call div requires at least two arguments")
    result = _coerce_number(args[0], label="div")
    for value in args[1:]:
        divisor = _coerce_number(value, label="div")
        if divisor == 0:
            raise ActionImplementationError("$call div cannot divide by zero")
        result /= divisor
    return result


_FUNCTION_REGISTRY: Dict[str, Callable[..., Any]] = {
    "concat": _fn_concat,
    "coalesce": _fn_coalesce,
    "upper": _fn_upper,
    "lower": _fn_lower,
    "trim": _fn_trim,
    "add": _fn_add,
    "sub": _fn_sub,
    "mul": _fn_mul,
    "div": _fn_div,
}


def _resolve_ref_object(
    value: Dict[str, Any],
    *,
    input_payload: Dict[str, Any],
    user: Dict[str, Any],
    target: Dict[str, Any],
    now: datetime,
) -> Any:
    if "$now" in value:
        if value.get("$now") is not True:
            raise ActionImplementationError("$now must be true")
        return now.isoformat()
    ref_path = value.get("$ref")
    parts = _split_dotted_path(ref_path, label="$ref")
    root = parts[0]
    rest = parts[1:]
    if root == "input":
        return _get_by_path(input_payload, rest, label="$ref(input)")
    if root == "user":
        return _get_by_path(user, rest, label="$ref(user)")
    if root == "target":
        return _get_by_path(target, rest, label="$ref(target)")
    raise ActionImplementationError(f"unsupported $ref root: {root}")


def _resolve_expr_operand(
    value: Any,
    *,
    input_payload: Dict[str, Any],
    user: Dict[str, Any],
    target: Dict[str, Any],
    now: datetime,
) -> Any:
    return _resolve_value(
        value,
        input_payload=input_payload,
        user=user,
        target=target,
        now=now,
        allow_v2=True,
    )


def _resolve_comparison_expression(
    *,
    op: str,
    payload: Any,
    input_payload: Dict[str, Any],
    user: Dict[str, Any],
    target: Dict[str, Any],
    now: datetime,
) -> bool:
    if op == "$not":
        value = _resolve_expr_operand(payload, input_payload=input_payload, user=user, target=target, now=now)
        return not bool(value)

    if not isinstance(payload, list):
        raise ActionImplementationError(f"{op} requires a list payload")

    if op in {"$and", "$or"}:
        if not payload:
            raise ActionImplementationError(f"{op} requires at least one operand")
        evaluated = [
            bool(_resolve_expr_operand(item, input_payload=input_payload, user=user, target=target, now=now))
            for item in payload
        ]
        return all(evaluated) if op == "$and" else any(evaluated)

    if len(payload) != 2:
        raise ActionImplementationError(f"{op} requires exactly two operands")

    lhs = _resolve_expr_operand(payload[0], input_payload=input_payload, user=user, target=target, now=now)
    rhs = _resolve_expr_operand(payload[1], input_payload=input_payload, user=user, target=target, now=now)
    if op == "$eq":
        return lhs == rhs
    if op == "$ne":
        return lhs != rhs
    try:
        if op == "$gt":
            return lhs > rhs
        if op == "$gte":
            return lhs >= rhs
        if op == "$lt":
            return lhs < rhs
        if op == "$lte":
            return lhs <= rhs
    except Exception as exc:
        raise ActionImplementationError(f"{op} operands are not comparable") from exc
    raise ActionImplementationError(f"Unsupported expression operator: {op}")


def _resolve_function_call(
    payload: Any,
    *,
    input_payload: Dict[str, Any],
    user: Dict[str, Any],
    target: Dict[str, Any],
    now: datetime,
) -> Any:
    if not isinstance(payload, dict):
        raise ActionImplementationError("$call payload must be an object")
    fn_name = str(payload.get("fn") or payload.get("name") or "").strip().lower()
    if not fn_name:
        raise ActionImplementationError("$call requires fn")
    fn = _FUNCTION_REGISTRY.get(fn_name)
    if fn is None:
        raise ActionImplementationError(f"$call function not supported: {fn_name}")
    args = payload.get("args")
    if args is None:
        args = []
    if not isinstance(args, list):
        raise ActionImplementationError("$call args must be a list")
    resolved_args = [
        _resolve_expr_operand(arg, input_payload=input_payload, user=user, target=target, now=now)
        for arg in args
    ]
    try:
        return fn(*resolved_args)
    except ActionImplementationError:
        raise
    except Exception as exc:
        raise ActionImplementationError(f"$call {fn_name} failed: {exc}") from exc


def _resolve_expression_object(
    value: Dict[str, Any],
    *,
    input_payload: Dict[str, Any],
    user: Dict[str, Any],
    target: Dict[str, Any],
    now: datetime,
) -> Any:
    op = next(iter(value.keys()))
    payload = value[op]
    if op in _ALLOWED_EXPR_OPERATORS:
        return _resolve_comparison_expression(
            op=op,
            payload=payload,
            input_payload=input_payload,
            user=user,
            target=target,
            now=now,
        )
    if op == "$call":
        return _resolve_function_call(
            payload,
            input_payload=input_payload,
            user=user,
            target=target,
            now=now,
        )
    if op == "$if":
        if not isinstance(payload, dict):
            raise ActionImplementationError("$if payload must be an object")
        condition = _resolve_expr_operand(
            payload.get("cond"),
            input_payload=input_payload,
            user=user,
            target=target,
            now=now,
        )
        branch = payload.get("then") if bool(condition) else payload.get("else")
        return _resolve_expr_operand(branch, input_payload=input_payload, user=user, target=target, now=now)
    if op == "$switch":
        if not isinstance(payload, dict):
            raise ActionImplementationError("$switch payload must be an object")
        cases = payload.get("cases")
        if not isinstance(cases, list):
            raise ActionImplementationError("$switch cases must be a list")
        for idx, case in enumerate(cases):
            if not isinstance(case, dict):
                raise ActionImplementationError(f"$switch.cases[{idx}] must be an object")
            when_value = _resolve_expr_operand(
                case.get("when"),
                input_payload=input_payload,
                user=user,
                target=target,
                now=now,
            )
            if bool(when_value):
                return _resolve_expr_operand(
                    case.get("then"),
                    input_payload=input_payload,
                    user=user,
                    target=target,
                    now=now,
                )
        return _resolve_expr_operand(
            payload.get("default"),
            input_payload=input_payload,
            user=user,
            target=target,
            now=now,
        )
    raise ActionImplementationError(f"Unsupported template_v2 directive: {op}")


def _resolve_value(
    value: Any,
    *,
    input_payload: Dict[str, Any],
    user: Dict[str, Any],
    target: Dict[str, Any],
    now: datetime,
    allow_v2: bool = False,
) -> Any:
    if _is_ref_object(value):
        return _resolve_ref_object(
            value,
            input_payload=input_payload,
            user=user,
            target=target,
            now=now,
        )
    if isinstance(value, list):
        return [
            _resolve_value(
                item,
                input_payload=input_payload,
                user=user,
                target=target,
                now=now,
                allow_v2=allow_v2,
            )
            for item in value
        ]
    if isinstance(value, dict):
        if allow_v2 and _is_expression_object(value):
            return _resolve_expression_object(
                value,
                input_payload=input_payload,
                user=user,
                target=target,
                now=now,
            )
        return {
            k: _resolve_value(
                v,
                input_payload=input_payload,
                user=user,
                target=target,
                now=now,
                allow_v2=allow_v2,
            )
            for k, v in value.items()
        }
    # Defense: reject mustache syntax that would otherwise pass through as a literal string.
    if isinstance(value, str) and _MUSTACHE_PATTERN.search(value):
        raise ActionImplementationError(
            f"Unresolved mustache syntax in value: '{value}'. Use $ref objects instead."
        )
    return value


def _extract_link_field_names(raw_ops: Any) -> set[str]:
    fields: set[str] = set()
    if not isinstance(raw_ops, list):
        return fields
    for item in raw_ops:
        field = None
        if isinstance(item, dict):
            field = item.get("field") or item.get("predicate") or item.get("name")
            if field is None and len(item) == 1:
                k, _v = next(iter(item.items()))
                field = k
        elif isinstance(item, str):
            raw = item.strip()
            if ":" in raw:
                field = raw.split(":", 1)[0]
        if _is_non_empty_str(field):
            fields.add(str(field).strip())
    return fields


def _compile_link_ops(
    raw_ops: Any,
    *,
    input_payload: Dict[str, Any],
    user: Dict[str, Any],
    target: Dict[str, Any],
    now: datetime,
    label: str,
    allow_v2: bool = False,
) -> List[Dict[str, str]]:
    if raw_ops is None:
        return []
    if not isinstance(raw_ops, list):
        raise ActionImplementationError(f"{label} must be a list")

    out: List[Dict[str, str]] = []
    for idx, item in enumerate(raw_ops):
        field = None
        value = None
        if isinstance(item, dict):
            field = item.get("field") or item.get("predicate") or item.get("name")
            value = item.get("value") or item.get("to") or item.get("target")
            if (field is None or value is None) and len(item) == 1:
                k, v = next(iter(item.items()))
                field = k
                value = v
        elif isinstance(item, str):
            raw = item.strip()
            if ":" in raw:
                field, value = raw.split(":", 1)
        field_str = _require_public_identifier(field, label=f"{label}[{idx}].field")
        value_resolved = _resolve_value(
            value,
            input_payload=input_payload,
            user=user,
            target=target,
            now=now,
            allow_v2=allow_v2,
        )
        value_str = _coerce_link_value(value_resolved, label=f"{label}[{idx}].value")
        out.append({"field": field_str, "value": value_str})
    return out


def _normalize_unset_list(value: Any, *, label: str) -> List[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ActionImplementationError(f"{label} must be a list")
    out: List[str] = []
    for idx, item in enumerate(value):
        field = _require_public_identifier(item, label=f"{label}[{idx}]")
        out.append(field)
    return out


def _validate_expression_object_shape(
    value: Dict[str, Any],
    *,
    label: str,
) -> None:
    op = next(iter(value.keys()))
    payload = value[op]
    if op in {"$eq", "$ne", "$gt", "$gte", "$lt", "$lte"}:
        if not isinstance(payload, list) or len(payload) != 2:
            raise ActionImplementationError(f"{label}.{op} must be a 2-item list")
        _validate_template_value(payload[0], label=f"{label}.{op}[0]", allow_v2=True)
        _validate_template_value(payload[1], label=f"{label}.{op}[1]", allow_v2=True)
        return
    if op in {"$and", "$or"}:
        if not isinstance(payload, list) or not payload:
            raise ActionImplementationError(f"{label}.{op} must be a non-empty list")
        for idx, item in enumerate(payload):
            _validate_template_value(item, label=f"{label}.{op}[{idx}]", allow_v2=True)
        return
    if op == "$not":
        _validate_template_value(payload, label=f"{label}.{op}", allow_v2=True)
        return
    if op == "$call":
        if not isinstance(payload, dict):
            raise ActionImplementationError(f"{label}.$call must be an object")
        fn_name = str(payload.get("fn") or payload.get("name") or "").strip().lower()
        if not fn_name:
            raise ActionImplementationError(f"{label}.$call requires fn")
        if fn_name not in _FUNCTION_REGISTRY:
            raise ActionImplementationError(f"{label}.$call function not supported: {fn_name}")
        args = payload.get("args")
        if args is None:
            return
        if not isinstance(args, list):
            raise ActionImplementationError(f"{label}.$call args must be a list")
        for idx, arg in enumerate(args):
            _validate_template_value(arg, label=f"{label}.$call.args[{idx}]", allow_v2=True)
        return
    if op == "$if":
        if not isinstance(payload, dict):
            raise ActionImplementationError(f"{label}.$if must be an object")
        if "cond" not in payload or "then" not in payload:
            raise ActionImplementationError(f"{label}.$if requires cond and then")
        _validate_template_value(payload.get("cond"), label=f"{label}.$if.cond", allow_v2=True)
        _validate_template_value(payload.get("then"), label=f"{label}.$if.then", allow_v2=True)
        if "else" in payload:
            _validate_template_value(payload.get("else"), label=f"{label}.$if.else", allow_v2=True)
        return
    if op == "$switch":
        if not isinstance(payload, dict):
            raise ActionImplementationError(f"{label}.$switch must be an object")
        cases = payload.get("cases")
        if not isinstance(cases, list) or not cases:
            raise ActionImplementationError(f"{label}.$switch.cases must be a non-empty list")
        for idx, case in enumerate(cases):
            if not isinstance(case, dict):
                raise ActionImplementationError(f"{label}.$switch.cases[{idx}] must be an object")
            if "when" not in case or "then" not in case:
                raise ActionImplementationError(f"{label}.$switch.cases[{idx}] requires when and then")
            _validate_template_value(case.get("when"), label=f"{label}.$switch.cases[{idx}].when", allow_v2=True)
            _validate_template_value(case.get("then"), label=f"{label}.$switch.cases[{idx}].then", allow_v2=True)
        if "default" in payload:
            _validate_template_value(payload.get("default"), label=f"{label}.$switch.default", allow_v2=True)
        return
    raise ActionImplementationError(f"{label} has unsupported directive: {op}")


def _validate_template_value(value: Any, *, label: str, allow_v2: bool) -> None:
    _detect_mustache_syntax(value, label=label)
    if _is_ref_object(value):
        if "$ref" in value:
            _split_dotted_path(value.get("$ref"), label=f"{label}.$ref")
        if "$now" in value and value.get("$now") is not True:
            raise ActionImplementationError(f"{label}.$now must be true")
        return
    if isinstance(value, list):
        for idx, item in enumerate(value):
            _validate_template_value(item, label=f"{label}[{idx}]", allow_v2=allow_v2)
        return
    if isinstance(value, dict):
        if len(value) == 1 and next(iter(value.keys())).startswith("$"):
            if not allow_v2:
                raise ActionImplementationError(
                    f"{label} uses {next(iter(value.keys()))} which requires implementation.type=template_v2|function_v1"
                )
            _validate_expression_object_shape(value, label=label)
            return
        for key, item in value.items():
            _validate_template_value(item, label=f"{label}.{key}", allow_v2=allow_v2)


def _normalize_set_ops(value: Any, *, label: str, allow_v2: bool = False) -> Dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ActionImplementationError(f"{label} must be an object")
    out: Dict[str, Any] = {}
    for k, v in value.items():
        key = _require_public_identifier(k, label=f"{label}.{k}")
        _validate_template_value(v, label=f"{label}.{k}", allow_v2=allow_v2)
        out[key] = v
    return out


def _is_noop_change_spec(changes: Dict[str, Any]) -> bool:
    if bool(changes.get("delete")):
        return False
    return not (
        (changes.get("set") or {})
        or (changes.get("unset") or [])
        or (changes.get("link_add") or [])
        or (changes.get("link_remove") or [])
    )


def _merge_change_specs(existing: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    if bool(existing.get("delete")) or bool(incoming.get("delete")):
        if bool(existing.get("delete")) and _is_noop_change_spec(incoming):
            return existing
        if bool(incoming.get("delete")) and _is_noop_change_spec(existing):
            return incoming
        raise ActionImplementationError("delete=true cannot be combined with other edits for the same target")

    merged_set = dict(existing.get("set") or {})
    merged_set.update(incoming.get("set") or {})

    merged_unset = set(existing.get("unset") or []) | set(incoming.get("unset") or [])
    for k in list(merged_unset):
        if k in merged_set:
            merged_unset.discard(k)

    return {
        "set": merged_set,
        "unset": sorted(merged_unset),
        "link_add": list(existing.get("link_add") or []) + list(incoming.get("link_add") or []),
        "link_remove": list(existing.get("link_remove") or []) + list(incoming.get("link_remove") or []),
        "delete": False,
    }


def _validate_implementation(
    implementation: Any,
    *,
    allowed_types: Optional[set[str]] = None,
) -> tuple[Dict[str, Any], str]:
    if not isinstance(implementation, dict) or not implementation:
        raise ActionImplementationError("implementation must be a non-empty object")
    impl_type = str(implementation.get("type") or "").strip()
    if impl_type not in _ALLOWED_IMPLEMENTATION_TYPES:
        raise ActionImplementationError(
            f"implementation.type must be one of: {', '.join(sorted(_ALLOWED_IMPLEMENTATION_TYPES))}"
        )
    if allowed_types is not None and impl_type not in allowed_types:
        if impl_type == "function_v1":
            raise ActionImplementationError("implementation.type=function_v1 is not executable in P0")
        raise ActionImplementationError(f"implementation.type={impl_type} is not executable in P0")
    targets = implementation.get("targets")
    if not isinstance(targets, list) or not targets:
        raise ActionImplementationError("implementation.targets must be a non-empty list")
    return implementation, impl_type


def _validate_template_v1(implementation: Any) -> Dict[str, Any]:
    impl, _impl_type = _validate_implementation(implementation, allowed_types={"template_v1"})
    return impl


@dataclass(frozen=True)
class CompiledTarget:
    class_id: str
    instance_id: str
    changes: Dict[str, Any]


def validate_template_v1_definition(implementation: Any) -> None:
    """
    Validate that an ActionType.implementation is executable (P0).

    This validates template structure only (not against a specific input payload).
    """
    impl = _validate_template_v1(implementation)
    _validate_targets_definition(impl=impl, allow_v2=False)


def validate_action_implementation_definition(implementation: Any) -> None:
    """
    Validate executable ActionType implementation definitions.

    Supported types:
    - template_v1
    - template_v2
    - function_v1
    """
    impl, impl_type = _validate_implementation(implementation)
    _validate_targets_definition(impl=impl, allow_v2=impl_type in _V2_EXECUTABLE_TYPES)


def _validate_targets_definition(
    *,
    impl: Dict[str, Any],
    allow_v2: bool,
) -> None:
    for idx, entry in enumerate(impl.get("targets") or []):
        if not isinstance(entry, dict):
            raise ActionImplementationError(f"implementation.targets[{idx}] must be an object")
        target_sel = entry.get("target")
        if not isinstance(target_sel, dict):
            raise ActionImplementationError(f"implementation.targets[{idx}].target must be an object")
        from_path = str(target_sel.get("from") or "").strip()
        parts = _split_dotted_path(from_path, label=f"implementation.targets[{idx}].target.from")
        if parts[0] != "input":
            raise ActionImplementationError("target.from must start with input.")

        raw_changes = entry.get("changes")
        if not isinstance(raw_changes, dict) or raw_changes is None:
            raise ActionImplementationError(f"implementation.targets[{idx}].changes must be an object")

        delete_flag = bool(raw_changes.get("delete") or False)
        set_ops = _normalize_set_ops(
            raw_changes.get("set"),
            label=f"implementation.targets[{idx}].changes.set",
            allow_v2=allow_v2,
        )
        unset_ops = _normalize_unset_list(raw_changes.get("unset"), label=f"implementation.targets[{idx}].changes.unset")

        # Ensure link ops are lists and validate field names (value types are validated at compile time).
        link_add = raw_changes.get("link_add")
        link_remove = raw_changes.get("link_remove")
        if link_add is not None and not isinstance(link_add, list):
            raise ActionImplementationError(f"implementation.targets[{idx}].changes.link_add must be a list")
        if link_remove is not None and not isinstance(link_remove, list):
            raise ActionImplementationError(f"implementation.targets[{idx}].changes.link_remove must be a list")

        raw_link_fields = _extract_link_field_names(link_add) | _extract_link_field_names(link_remove)
        for f in raw_link_fields:
            _require_public_identifier(f, label=f"implementation.targets[{idx}].changes.link_field")

        if delete_flag and (set_ops or unset_ops or raw_link_fields):
            raise ActionImplementationError(
                f"implementation.targets[{idx}].changes.delete=true cannot include other edits"
            )


def _compile_change_shape(
    *,
    impl: Dict[str, Any],
    input_payload: Dict[str, Any],
    allow_v2: bool,
) -> List[CompiledTarget]:
    compiled: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for idx, entry in enumerate(impl.get("targets") or []):
        if not isinstance(entry, dict):
            raise ActionImplementationError(f"implementation.targets[{idx}] must be an object")
        target_sel = entry.get("target")
        if not isinstance(target_sel, dict):
            raise ActionImplementationError(f"implementation.targets[{idx}].target must be an object")
        from_path = str(target_sel.get("from") or "").strip()
        parts = _split_dotted_path(from_path, label=f"implementation.targets[{idx}].target.from")
        if parts[0] != "input":
            raise ActionImplementationError("target.from must start with input.")
        value = _get_by_path(input_payload, parts[1:], label="target.from")

        refs: List[Dict[str, str]] = []
        if isinstance(value, list):
            refs = [_normalize_object_ref(v, label="target.from[]") for v in value]
        else:
            refs = [_normalize_object_ref(value, label="target.from")]
        if not refs:
            raise ActionImplementationError("target.from resolved to an empty target set")

        raw_changes = entry.get("changes")
        if not isinstance(raw_changes, dict) or raw_changes is None:
            raise ActionImplementationError(f"implementation.targets[{idx}].changes must be an object")

        delete_flag = bool(raw_changes.get("delete") or False)
        set_ops = _normalize_set_ops(
            raw_changes.get("set"),
            label=f"implementation.targets[{idx}].changes.set",
            allow_v2=allow_v2,
        )
        unset_ops = _normalize_unset_list(raw_changes.get("unset"), label=f"implementation.targets[{idx}].changes.unset")
        raw_link_fields = _extract_link_field_names(raw_changes.get("link_add")) | _extract_link_field_names(
            raw_changes.get("link_remove")
        )
        link_fields = {
            _require_public_identifier(f, label=f"implementation.targets[{idx}].changes.link_field")
            for f in raw_link_fields
        }

        if delete_flag and (set_ops or unset_ops or link_fields):
            raise ActionImplementationError(
                f"implementation.targets[{idx}].changes.delete=true cannot include other edits"
            )

        shape = {
            "set": {k: None for k in set_ops.keys()},
            "unset": unset_ops,
            "link_add": [{"field": f, "value": "__touch__"} for f in sorted(link_fields)],
            "link_remove": [],
            "delete": delete_flag,
        }

        for ref in refs:
            key = (ref["class_id"], ref["instance_id"])
            if key in compiled:
                compiled[key] = _merge_change_specs(compiled[key], shape)
            else:
                compiled[key] = shape
    return [CompiledTarget(class_id=k[0], instance_id=k[1], changes=v) for k, v in compiled.items()]


def compile_action_change_shape(
    implementation: Any,
    *,
    input_payload: Dict[str, Any],
) -> List[CompiledTarget]:
    """
    Compile any executable implementation type into per-target "change shape".

    This does NOT resolve $ref values; it only derives:
    - targets (from input.* paths)
    - touched field keys (set/unset)
    - touched link field names (link_add/link_remove field names)
    - delete flag
    """
    impl, impl_type = _validate_implementation(implementation)
    return _compile_change_shape(impl=impl, input_payload=input_payload, allow_v2=impl_type in _V2_EXECUTABLE_TYPES)


def compile_template_v1_change_shape(
    implementation: Any,
    *,
    input_payload: Dict[str, Any],
) -> List[CompiledTarget]:
    """
    Backward-compatible template_v1-only compiler.
    """
    impl = _validate_template_v1(implementation)
    return _compile_change_shape(impl=impl, input_payload=input_payload, allow_v2=False)

def _compile_concrete_changes(
    *,
    impl: Dict[str, Any],
    input_payload: Dict[str, Any],
    user: Dict[str, Any],
    target_docs: Mapping[Tuple[str, str], Dict[str, Any]],
    now: datetime,
    allow_v2: bool,
) -> List[CompiledTarget]:
    compiled: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for idx, entry in enumerate(impl.get("targets") or []):
        if not isinstance(entry, dict):
            raise ActionImplementationError(f"implementation.targets[{idx}] must be an object")
        target_sel = entry.get("target")
        if not isinstance(target_sel, dict):
            raise ActionImplementationError(f"implementation.targets[{idx}].target must be an object")
        from_path = str(target_sel.get("from") or "").strip()
        parts = _split_dotted_path(from_path, label=f"implementation.targets[{idx}].target.from")
        if parts[0] != "input":
            raise ActionImplementationError("target.from must start with input.")

        value = _get_by_path(input_payload, parts[1:], label="target.from")
        refs: List[Dict[str, str]] = []
        if isinstance(value, list):
            refs = [_normalize_object_ref(v, label="target.from[]") for v in value]
        else:
            refs = [_normalize_object_ref(value, label="target.from")]
        if not refs:
            raise ActionImplementationError("target.from resolved to an empty target set")

        raw_changes = entry.get("changes")
        if not isinstance(raw_changes, dict) or raw_changes is None:
            raise ActionImplementationError(f"implementation.targets[{idx}].changes must be an object")

        delete_flag = bool(raw_changes.get("delete") or False)
        raw_set_ops = _normalize_set_ops(
            raw_changes.get("set"),
            label=f"implementation.targets[{idx}].changes.set",
            allow_v2=allow_v2,
        )
        unset_ops = _normalize_unset_list(raw_changes.get("unset"), label=f"implementation.targets[{idx}].changes.unset")

        for ref in refs:
            key = (ref["class_id"], ref["instance_id"])
            target_doc = target_docs.get(key)
            if not isinstance(target_doc, dict):
                target_doc = {}

            set_ops: Dict[str, Any] = {}
            for field, raw_value in raw_set_ops.items():
                set_ops[field] = _resolve_value(
                    raw_value,
                    input_payload=input_payload,
                    user=user,
                    target=target_doc,
                    now=now,
                    allow_v2=allow_v2,
                )

            link_add = _compile_link_ops(
                raw_changes.get("link_add"),
                input_payload=input_payload,
                user=user,
                target=target_doc,
                now=now,
                label=f"implementation.targets[{idx}].changes.link_add",
                allow_v2=allow_v2,
            )
            link_remove = _compile_link_ops(
                raw_changes.get("link_remove"),
                input_payload=input_payload,
                user=user,
                target=target_doc,
                now=now,
                label=f"implementation.targets[{idx}].changes.link_remove",
                allow_v2=allow_v2,
            )

            if delete_flag and (set_ops or unset_ops or link_add or link_remove):
                raise ActionImplementationError(
                    f"implementation.targets[{idx}].changes.delete=true cannot include other edits"
                )

            incoming = {
                "set": set_ops,
                "unset": unset_ops,
                "link_add": link_add,
                "link_remove": link_remove,
                "delete": delete_flag,
            }

            if key in compiled:
                compiled[key] = _merge_change_specs(compiled[key], incoming)
            else:
                compiled[key] = incoming

    return [CompiledTarget(class_id=k[0], instance_id=k[1], changes=v) for k, v in compiled.items()]


def compile_action_implementation(
    implementation: Any,
    *,
    input_payload: Dict[str, Any],
    user: Dict[str, Any],
    target_docs: Mapping[Tuple[str, str], Dict[str, Any]],
    now: Optional[datetime] = None,
) -> List[CompiledTarget]:
    """
    Compile any executable action implementation into concrete per-target changes.

    This resolves:
    - target.from selectors (input.*)
    - value references ($ref/$now) and template_v2 directives ($if/$switch/$call)
    - per-target merges (including delete tombstones)
    """
    impl, impl_type = _validate_implementation(implementation)
    resolved_now = now or utcnow()
    if resolved_now.tzinfo is None:
        resolved_now = resolved_now.replace(tzinfo=timezone.utc)

    if not isinstance(user, dict):
        raise ActionImplementationError("user context must be an object")

    return _compile_concrete_changes(
        impl=impl,
        input_payload=input_payload,
        user=user,
        target_docs=target_docs,
        now=resolved_now,
        allow_v2=impl_type in _V2_EXECUTABLE_TYPES,
    )


def compile_template_v1(
    implementation: Any,
    *,
    input_payload: Dict[str, Any],
    user: Dict[str, Any],
    target_docs: Mapping[Tuple[str, str], Dict[str, Any]],
    now: Optional[datetime] = None,
) -> List[CompiledTarget]:
    """
    Backward-compatible template_v1-only compiler.
    """
    impl = _validate_template_v1(implementation)
    now = now or utcnow()
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    if not isinstance(user, dict):
        raise ActionImplementationError("user context must be an object")

    return _compile_concrete_changes(
        impl=impl,
        input_payload=input_payload,
        user=user,
        target_docs=target_docs,
        now=now,
        allow_v2=False,
    )
