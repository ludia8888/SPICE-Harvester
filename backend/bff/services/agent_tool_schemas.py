from __future__ import annotations

import functools
from typing import Any, Optional

from fastapi import FastAPI


@functools.lru_cache(maxsize=1)
def _openapi_spec() -> dict[str, Any]:
    """
    Build a minimal OpenAPI spec for agent tool schema resolution.

    Notes:
    - We intentionally include only routers that are exposed as agent tools.
    - This avoids importing the full BFF app (and prevents circular imports).
    """

    from bff.routers import actions, graph, objectify, pipeline, query  # local import to avoid cycles

    app = FastAPI(title="agent-tool-schema")
    for router in (actions.router, graph.router, objectify.router, pipeline.router, query.router):
        app.include_router(router, prefix="/api/v1")
    return app.openapi()


def _lookup_json_pointer(doc: dict[str, Any], ref: str) -> Optional[dict[str, Any]]:
    if not ref.startswith("#/"):
        return None
    node: Any = doc
    for part in ref[2:].split("/"):
        if not isinstance(node, dict):
            return None
        node = node.get(part)
    return node if isinstance(node, dict) else None


def resolve_schema_ref(schema: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not schema or not isinstance(schema, dict):
        return None
    ref = schema.get("$ref")
    if isinstance(ref, str) and ref:
        resolved = _lookup_json_pointer(_openapi_spec(), ref)
        return resolved if resolved else schema
    return schema


def get_operation_spec(*, method: str, path: str) -> Optional[dict[str, Any]]:
    spec = _openapi_spec()
    paths = spec.get("paths")
    if not isinstance(paths, dict):
        return None
    path_item = paths.get(path)
    if not isinstance(path_item, dict):
        return None
    op = path_item.get(str(method or "").strip().lower())
    return op if isinstance(op, dict) else None


def get_request_body_schema(*, method: str, path: str) -> Optional[dict[str, Any]]:
    op = get_operation_spec(method=method, path=path)
    if not op:
        return None
    request_body = op.get("requestBody")
    if not isinstance(request_body, dict):
        return None
    content = request_body.get("content")
    if not isinstance(content, dict):
        return None
    app_json = content.get("application/json")
    if not isinstance(app_json, dict):
        return None
    schema = app_json.get("schema")
    return schema if isinstance(schema, dict) else None


def get_request_body_required(*, method: str, path: str) -> bool:
    op = get_operation_spec(method=method, path=path)
    if not op:
        return False
    request_body = op.get("requestBody")
    if not isinstance(request_body, dict):
        return False
    return bool(request_body.get("required") or False)


def get_response_schema(*, method: str, path: str) -> Optional[dict[str, Any]]:
    op = get_operation_spec(method=method, path=path)
    if not op:
        return None
    responses = op.get("responses")
    if not isinstance(responses, dict):
        return None
    for key in ("200", "201", "202", "204"):
        response = responses.get(key)
        if not isinstance(response, dict):
            continue
        content = response.get("content")
        if not isinstance(content, dict):
            continue
        app_json = content.get("application/json")
        if not isinstance(app_json, dict):
            continue
        schema = app_json.get("schema")
        return schema if isinstance(schema, dict) else None
    return None


def get_query_params(*, method: str, path: str) -> list[dict[str, Any]]:
    op = get_operation_spec(method=method, path=path)
    if not op:
        return []
    params = op.get("parameters")
    if not isinstance(params, list):
        return []
    out: list[dict[str, Any]] = []
    for param in params:
        if not isinstance(param, dict):
            continue
        if str(param.get("in") or "").strip() != "query":
            continue
        name = str(param.get("name") or "").strip()
        if not name:
            continue
        schema = param.get("schema") if isinstance(param.get("schema"), dict) else {}
        schema_resolved = resolve_schema_ref(schema) or schema
        out.append(
            {
                "name": name,
                "required": bool(param.get("required") or False),
                "schema": schema_resolved,
                "description": param.get("description"),
            }
        )
    return out


def schema_hint(schema: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    resolved = resolve_schema_ref(schema)
    if not resolved:
        return None
    if not isinstance(resolved, dict):
        return None
    if resolved.get("type") != "object":
        return {"type": resolved.get("type")}
    props = resolved.get("properties")
    if not isinstance(props, dict):
        props = {}
    required = resolved.get("required")
    required_list = [str(item) for item in required] if isinstance(required, list) else []

    compact_props: dict[str, Any] = {}
    for key, value in props.items():
        if not isinstance(value, dict):
            continue
        ref = value.get("$ref")
        if isinstance(ref, str) and ref:
            resolved_prop = resolve_schema_ref(value) or value
            compact_props[str(key)] = {"$ref": ref, "type": resolved_prop.get("type")}
        else:
            compact_props[str(key)] = {"type": value.get("type"), "description": value.get("description")}

    return {"type": "object", "required": required_list, "properties": compact_props}


def validate_against_request_schema(*, method: str, path: str, body: Any, query: dict[str, Any]) -> list[str]:
    """
    Best-effort JSON Schema validation for agent plan steps.

    We validate:
    - required requestBody presence (when required=true)
    - JSON Schema validation against requestBody schema (when present)
    - required query parameters (when declared in OpenAPI)
    """

    errors: list[str] = []
    body_required = get_request_body_required(method=method, path=path)
    if body_required and body is None:
        errors.append("request body is required")
        return errors

    try:
        from jsonschema import Draft202012Validator, RefResolver

        schema = get_request_body_schema(method=method, path=path)
        if schema and body is not None:
            resolver = RefResolver.from_schema(_openapi_spec())
            validator = Draft202012Validator(schema, resolver=resolver)
            for err in validator.iter_errors(body):
                errors.append(str(err.message))
    except Exception:
        # Schema validation is best-effort; callers may still rely on server-side endpoint validation.
        pass

    for param in get_query_params(method=method, path=path):
        if not param.get("required"):
            continue
        name = str(param.get("name") or "").strip()
        if name and not query.get(name):
            errors.append(f"missing required query param: {name}")

    return errors

