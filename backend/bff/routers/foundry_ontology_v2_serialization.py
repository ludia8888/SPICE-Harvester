from __future__ import annotations

from typing import Any

from shared.utils.action_permission_profile import ActionPermissionProfileError, resolve_action_permission_profile
from shared.utils.language import first_localized_text
from shared.utils.payload_utils import extract_payload_object, extract_payload_rows


def _extract_ontology_resource_rows(payload: Any) -> list[dict[str, Any]]:
    return extract_payload_rows(payload, key="resources")


def _extract_ontology_resource(payload: Any) -> dict[str, Any] | None:
    row = extract_payload_object(payload)
    return row or None


def _localized_text(value: Any) -> str | None:
    return first_localized_text(value)


def _to_foundry_named_metadata(resource: dict[str, Any]) -> dict[str, Any] | None:
    api_name = str(resource.get("id") or "").strip()
    if not api_name:
        return None

    spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
    metadata = resource.get("metadata") if isinstance(resource.get("metadata"), dict) else {}

    out: dict[str, Any] = {"apiName": api_name}

    display_name = (
        _localized_text(resource.get("label"))
        or _localized_text(spec.get("display_name"))
        or _localized_text(spec.get("displayName"))
        or _localized_text(metadata.get("displayName"))
    )
    if display_name:
        out["displayName"] = display_name

    description = (
        _localized_text(resource.get("description"))
        or _localized_text(spec.get("description"))
        or _localized_text(metadata.get("description"))
    )
    if description:
        out["description"] = description

    status_value = str(spec.get("status") or resource.get("status") or metadata.get("status") or "ACTIVE").strip()
    if status_value:
        out["status"] = status_value.upper()

    rid = str(resource.get("rid") or "").strip()
    if rid:
        out["rid"] = rid

    return out


def _to_foundry_named_metadata_map(resources: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for resource in resources:
        api_name = str(resource.get("id") or "").strip()
        if not api_name:
            continue
        mapped = _to_foundry_named_metadata(resource)
        if isinstance(mapped, dict) and mapped:
            out[api_name] = mapped
    return out


def _dict_or_none(value: Any) -> dict[str, Any] | None:
    return dict(value) if isinstance(value, dict) else None


def _list_or_none(value: Any) -> list[Any] | None:
    return list(value) if isinstance(value, list) else None


def _first_non_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _coerce_optional_bool(value: Any, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y", "on"}:
            return True
        if normalized in {"false", "0", "no", "n", "off"}:
            return False
    return default


def _extract_project_policy_inheritance(
    *,
    permission_policy: dict[str, Any] | None,
    spec: dict[str, Any],
) -> dict[str, Any] | None:
    policy = permission_policy if isinstance(permission_policy, dict) else {}
    inherit = _coerce_optional_bool(
        _first_non_none(
            policy.get("inherit_project_policy"),
            policy.get("inheritProjectPolicy"),
            spec.get("inherit_project_policy"),
            spec.get("inheritProjectPolicy"),
        ),
        default=False,
    )
    if not inherit:
        return None

    scope = str(
        _first_non_none(
            policy.get("project_policy_scope"),
            policy.get("projectPolicyScope"),
            spec.get("project_policy_scope"),
            spec.get("projectPolicyScope"),
            "action_access",
        )
        or "action_access"
    ).strip() or "action_access"
    subject_type = str(
        _first_non_none(
            policy.get("project_policy_subject_type"),
            policy.get("projectPolicySubjectType"),
            spec.get("project_policy_subject_type"),
            spec.get("projectPolicySubjectType"),
            "project",
        )
        or "project"
    ).strip() or "project"
    subject_id = str(
        _first_non_none(
            policy.get("project_policy_subject_id"),
            policy.get("projectPolicySubjectId"),
            spec.get("project_policy_subject_id"),
            spec.get("projectPolicySubjectId"),
            "",
        )
        or ""
    ).strip()
    require_policy = _coerce_optional_bool(
        _first_non_none(
            policy.get("require_project_policy"),
            policy.get("requireProjectPolicy"),
            spec.get("require_project_policy"),
            spec.get("requireProjectPolicy"),
        ),
        default=True,
    )
    out: dict[str, Any] = {
        "enabled": True,
        "scope": scope,
        "subjectType": subject_type,
        "requirePolicy": require_policy,
    }
    if subject_id:
        out["subjectId"] = subject_id
    return out


def _to_foundry_action_type(resource: dict[str, Any]) -> dict[str, Any] | None:
    mapped = _to_foundry_named_metadata(resource)
    if mapped is None:
        return None
    spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
    metadata = resource.get("metadata") if isinstance(resource.get("metadata"), dict) else {}
    parameters = (
        _dict_or_none(resource.get("parameters"))
        or _dict_or_none(spec.get("parameters"))
        or _dict_or_none(metadata.get("parameters"))
    )
    if parameters:
        mapped["parameters"] = parameters
    operations = (
        _list_or_none(resource.get("operations"))
        or _list_or_none(spec.get("operations"))
        or _list_or_none(metadata.get("operations"))
    )
    if operations:
        mapped["operations"] = operations
    tool_description = (
        str(resource.get("toolDescription") or "").strip()
        or str(spec.get("toolDescription") or "").strip()
        or str(metadata.get("toolDescription") or "").strip()
    )
    if tool_description:
        mapped["toolDescription"] = tool_description

    permission_policy = (
        _dict_or_none(resource.get("permissionPolicy"))
        or _dict_or_none(resource.get("permission_policy"))
        or _dict_or_none(spec.get("permission_policy"))
        or _dict_or_none(spec.get("permissionPolicy"))
        or _dict_or_none(metadata.get("permission_policy"))
        or _dict_or_none(metadata.get("permissionPolicy"))
    )
    if permission_policy:
        mapped["permissionPolicy"] = permission_policy

    writeback_target = (
        _dict_or_none(resource.get("writebackTarget"))
        or _dict_or_none(resource.get("writeback_target"))
        or _dict_or_none(spec.get("writeback_target"))
        or _dict_or_none(spec.get("writebackTarget"))
        or _dict_or_none(metadata.get("writeback_target"))
        or _dict_or_none(metadata.get("writebackTarget"))
    )
    if writeback_target:
        mapped["writebackTarget"] = writeback_target

    conflict_policy = str(
        _first_non_none(
            resource.get("conflictPolicy"),
            resource.get("conflict_policy"),
            spec.get("conflict_policy"),
            spec.get("conflictPolicy"),
            metadata.get("conflict_policy"),
            metadata.get("conflictPolicy"),
            "",
        )
        or ""
    ).strip()
    if conflict_policy:
        mapped["conflictPolicy"] = conflict_policy

    target_object_type = str(
        _first_non_none(
            resource.get("targetObjectType"),
            resource.get("target_object_type"),
            spec.get("target_object_type"),
            spec.get("targetObjectType"),
            metadata.get("target_object_type"),
            metadata.get("targetObjectType"),
            "",
        )
        or ""
    ).strip()
    if target_object_type:
        mapped["targetObjectType"] = target_object_type

    validation_rules = (
        _list_or_none(resource.get("validationRules"))
        or _list_or_none(resource.get("validation_rules"))
        or _list_or_none(spec.get("validation_rules"))
        or _list_or_none(spec.get("validationRules"))
        or _list_or_none(metadata.get("validation_rules"))
        or _list_or_none(metadata.get("validationRules"))
    )
    if validation_rules:
        mapped["validationRules"] = validation_rules

    project_policy_inheritance = _extract_project_policy_inheritance(
        permission_policy=permission_policy,
        spec=spec,
    )
    if project_policy_inheritance:
        mapped["projectPolicyInheritance"] = project_policy_inheritance

    permission_model = "ontology_roles"
    edits_beyond_actions = False
    try:
        profile = resolve_action_permission_profile(spec)
        permission_model = profile.permission_model
        edits_beyond_actions = profile.edits_beyond_actions
    except ActionPermissionProfileError:
        raw_permission_model = str(
            _first_non_none(
                spec.get("permission_model"),
                spec.get("permissionModel"),
                metadata.get("permission_model"),
                metadata.get("permissionModel"),
                "",
            )
            or ""
        ).strip()
        if raw_permission_model:
            permission_model = raw_permission_model
        edits_beyond_actions = _coerce_optional_bool(
            _first_non_none(
                spec.get("edits_beyond_actions"),
                spec.get("editsBeyondActions"),
                metadata.get("edits_beyond_actions"),
                metadata.get("editsBeyondActions"),
            ),
            default=False,
        )

    mapped["permissionModel"] = permission_model
    mapped["editsBeyondActions"] = edits_beyond_actions
    dynamic_security: dict[str, Any] = {
        "permissionModel": permission_model,
        "editsBeyondActions": edits_beyond_actions,
    }
    if permission_policy:
        dynamic_security["permissionPolicy"] = permission_policy
    if project_policy_inheritance:
        dynamic_security["projectPolicyInheritance"] = project_policy_inheritance
    mapped["dynamicSecurity"] = dynamic_security
    return mapped


def _to_foundry_action_type_map(resources: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for resource in resources:
        api_name = str(resource.get("id") or "").strip()
        if not api_name:
            continue
        mapped = _to_foundry_action_type(resource)
        if isinstance(mapped, dict) and mapped:
            out[api_name] = mapped
    return out


def _to_foundry_query_type(resource: dict[str, Any]) -> dict[str, Any] | None:
    mapped = _to_foundry_named_metadata(resource)
    if mapped is None:
        return None
    spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
    metadata = resource.get("metadata") if isinstance(resource.get("metadata"), dict) else {}
    parameters = (
        _dict_or_none(resource.get("parameters"))
        or _dict_or_none(spec.get("parameters"))
        or _dict_or_none(metadata.get("parameters"))
    )
    if parameters:
        mapped["parameters"] = parameters
    output = resource.get("output")
    if output is None:
        output = spec.get("output")
    if output is None:
        output = metadata.get("output")
    if output is not None:
        mapped["output"] = output
    version = (
        str(resource.get("version") or "").strip()
        or str(spec.get("version") or "").strip()
        or str(metadata.get("version") or "").strip()
    )
    if version:
        mapped["version"] = version
    return mapped


def _to_foundry_query_type_map_key(resource: dict[str, Any]) -> str | None:
    mapped = _to_foundry_query_type(resource)
    if not mapped:
        return None
    api_name = str(mapped.get("apiName") or "").strip()
    if not api_name:
        return None
    version = str(mapped.get("version") or "").strip()
    if not version:
        return api_name
    return f"{api_name}:{version}"


def _to_foundry_query_type_metadata_map(resources: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for resource in resources:
        key = _to_foundry_query_type_map_key(resource)
        if not key:
            continue
        mapped = _to_foundry_query_type(resource)
        if isinstance(mapped, dict) and mapped:
            out[key] = mapped
    return out


def _resolve_query_placeholder_key(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None

    if text.startswith("${") and text.endswith("}") and len(text) > 3:
        key = text[2:-1].strip()
        return key or None

    if text.startswith("{{") and text.endswith("}}") and len(text) > 4:
        key = text[2:-2].strip()
        return key or None

    if text.startswith("$") and len(text) > 1:
        key = text[1:].strip()
        if key and all(ch.isalnum() or ch == "_" for ch in key):
            return key

    return None


def _materialize_query_execution_value(value: Any, *, parameters: dict[str, Any]) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _materialize_query_execution_value(inner, parameters=parameters)
            for key, inner in value.items()
        }
    if isinstance(value, list):
        return [_materialize_query_execution_value(item, parameters=parameters) for item in value]

    placeholder = _resolve_query_placeholder_key(value)
    if placeholder is None:
        return value
    if placeholder not in parameters:
        raise ValueError(f"Missing required query parameter: {placeholder}")
    return parameters[placeholder]


_QUERY_OBJECT_TYPE_CANONICAL_FIELDS: tuple[str, ...] = ("objectTypeApiName",)
_QUERY_OBJECT_TYPE_FALLBACK_FIELDS: tuple[str, ...] = (
    "objectType",
    "targetObjectType",
    "target_object_type",
)


def _resolve_query_execution_object_type(
    *,
    execution: dict[str, Any],
    search: dict[str, Any],
    spec: dict[str, Any],
    metadata: dict[str, Any],
) -> str | None:
    sources = (execution, search, spec, metadata)
    for field in _QUERY_OBJECT_TYPE_CANONICAL_FIELDS:
        for source in sources:
            normalized = str(source.get(field) or "").strip()
            if normalized:
                return normalized

    for field in _QUERY_OBJECT_TYPE_FALLBACK_FIELDS:
        for source in sources:
            normalized = str(source.get(field) or "").strip()
            if normalized:
                return normalized
    return None


def _extract_query_execution_plan(resource: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
    metadata = resource.get("metadata") if isinstance(resource.get("metadata"), dict) else {}

    execution: dict[str, Any] = {}
    for candidate in (
        resource.get("execution"),
        spec.get("execution"),
        metadata.get("execution"),
        resource.get("query"),
        spec.get("query"),
        metadata.get("query"),
    ):
        if isinstance(candidate, dict):
            execution = candidate
            break

    search = execution.get("search") if isinstance(execution.get("search"), dict) else {}

    object_type = _resolve_query_execution_object_type(
        execution=execution,
        search=search,
        spec=spec,
        metadata=metadata,
    )

    payload: dict[str, Any] = {}
    for source in (search, execution):
        if not isinstance(source, dict):
            continue
        where = source.get("where")
        if isinstance(where, dict):
            payload["where"] = where
        elif "where" in source and where is not None:
            payload["where"] = where

        filter_clause = source.get("filter")
        if "where" not in payload and isinstance(filter_clause, dict):
            payload["where"] = filter_clause

        for key in ("pageSize", "pageToken", "select", "selectV2", "orderBy", "excludeRid", "snapshot"):
            if key in source and source.get(key) is not None:
                payload[key] = source.get(key)

    return object_type, payload


def _apply_query_execute_options(
    *,
    base_payload: dict[str, Any],
    options: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = dict(base_payload)
    if not isinstance(options, dict):
        return payload

    for key in ("where", "filter", "pageSize", "pageToken", "select", "selectV2", "orderBy", "excludeRid", "snapshot"):
        if key not in options:
            continue
        value = options.get(key)
        if value is not None:
            payload[key] = value
    return payload


def _to_foundry_ontology(row: dict[str, Any]) -> dict[str, Any]:
    api_name = str(row.get("name") or row.get("db_name") or row.get("apiName") or "").strip()
    display_name = (
        str(row.get("displayName") or "").strip()
        or str(row.get("label") or "").strip()
        or api_name
    )
    out: dict[str, Any] = {
        "apiName": api_name,
        "displayName": display_name,
    }
    description = str(row.get("description") or "").strip()
    if description:
        out["description"] = description
    rid = str(row.get("rid") or row.get("ontologyRid") or "").strip()
    if rid:
        out["rid"] = rid
    return out
