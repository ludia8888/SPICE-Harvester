from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

import pytest
import yaml
from fastapi.routing import APIRoute

from bff.main import app as bff_app


_MANIFEST_PATH = Path(__file__).with_name("foundry_contract_manifest.yml")
_PATH_PARAM_CONVERTER_RE = re.compile(r"{([^}:]+):[^}]+}")


def _load_manifest() -> dict[str, Any]:
    payload = yaml.safe_load(_MANIFEST_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError("Manifest must be a mapping")
    return payload


def _openapi_operation_parameters(openapi: dict[str, Any], path: str, method: str) -> list[dict[str, Any]]:
    paths = openapi.get("paths", {})
    if not isinstance(paths, dict) or path not in paths:
        return []
    path_item = paths.get(path, {})
    if not isinstance(path_item, dict):
        return []
    op = path_item.get(str(method).lower(), {})
    if not isinstance(op, dict):
        return []
    params: list[dict[str, Any]] = []
    for scope_key in ("parameters",):
        raw = op.get(scope_key, [])
        if isinstance(raw, list):
            params.extend([p for p in raw if isinstance(p, dict)])
    # FastAPI can also place shared params at the path level.
    raw_path_params = path_item.get("parameters", [])
    if isinstance(raw_path_params, list):
        params.extend([p for p in raw_path_params if isinstance(p, dict)])
    return params


def _split_params(params: Iterable[dict[str, Any]]) -> tuple[dict[str, bool], dict[str, bool]]:
    path_params: dict[str, bool] = {}
    query_params: dict[str, bool] = {}
    for param in params:
        name = str(param.get("name") or "")
        location = str(param.get("in") or "")
        required = bool(param.get("required"))
        if location == "path" and name:
            path_params[name] = required
        elif location == "query" and name:
            query_params[name] = required
    return path_params, query_params


def _route_index(app_routes: Iterable[Any]) -> dict[tuple[str, str], APIRoute]:
    index: dict[tuple[str, str], APIRoute] = {}
    for route in app_routes:
        if not isinstance(route, APIRoute):
            continue
        normalized_path = _PATH_PARAM_CONVERTER_RE.sub(r"{\g<1>}", route.path)
        for method in sorted(route.methods or []):
            if method in {"HEAD", "OPTIONS"}:
                continue
            index[(normalized_path, method.lower())] = route
    return index


def _route_required_scopes(route: APIRoute) -> tuple[str, ...]:
    scopes: set[str] = set()
    dependant = getattr(route, "dependant", None)
    for dep in getattr(dependant, "dependencies", []) or []:
        required = getattr(getattr(dep, "call", None), "__foundry_required_scopes__", None)
        if required:
            scopes.update(str(scope) for scope in required if str(scope).strip())
    return tuple(sorted(scopes))


@pytest.mark.unit
def test_foundry_contract_manifest_matches_openapi_and_scopes() -> None:
    manifest = _load_manifest()
    endpoints = manifest.get("endpoints")
    assert isinstance(endpoints, list) and endpoints, "Manifest must define endpoints"

    openapi = bff_app.openapi()
    route_index = _route_index(bff_app.routes)

    failures: list[str] = []
    for entry in endpoints:
        if not isinstance(entry, dict):
            failures.append("Manifest endpoint entry must be a mapping")
            continue

        endpoint_id = str(entry.get("id") or "<missing-id>")
        path = str(entry.get("path") or "")
        method = str(entry.get("method") or "").lower()
        if not path or not method:
            failures.append(f"{endpoint_id}: missing path/method")
            continue

        # 1) OpenAPI path+method presence
        paths = openapi.get("paths", {})
        if not isinstance(paths, dict) or path not in paths:
            failures.append(f"{endpoint_id}: missing OpenAPI path {path}")
            continue
        path_item = paths.get(path, {})
        if not isinstance(path_item, dict) or method not in path_item:
            failures.append(f"{endpoint_id}: missing OpenAPI method {method.upper()} {path}")
            continue

        # 2) Param names + required flags
        params = _openapi_operation_parameters(openapi, path, method)
        actual_path_params, actual_query_params = _split_params(params)

        expected_path_params = [str(p) for p in entry.get("path_params") or []]
        expected_query_params = entry.get("query_params") or []
        if not isinstance(expected_query_params, list):
            failures.append(f"{endpoint_id}: query_params must be a list")
            continue

        expected_query_required: dict[str, bool] = {}
        expected_query_names: list[str] = []
        for qp in expected_query_params:
            if isinstance(qp, dict):
                name = str(qp.get("name") or "")
                required = bool(qp.get("required"))
            else:
                name = str(qp or "")
                required = False
            if not name:
                failures.append(f"{endpoint_id}: query_params contains empty name")
                continue
            expected_query_names.append(name)
            expected_query_required[name] = required

        if sorted(actual_path_params.keys()) != sorted(expected_path_params):
            failures.append(
                f"{endpoint_id}: path params mismatch "
                f"(expected={sorted(expected_path_params)}, actual={sorted(actual_path_params.keys())})"
            )
        else:
            for name in expected_path_params:
                if actual_path_params.get(name) is not True:
                    failures.append(f"{endpoint_id}: path param {name} must be required")

        if sorted(actual_query_params.keys()) != sorted(expected_query_names):
            failures.append(
                f"{endpoint_id}: query params mismatch "
                f"(expected={sorted(expected_query_names)}, actual={sorted(actual_query_params.keys())})"
            )
        else:
            for name, expected_required in expected_query_required.items():
                actual_required = bool(actual_query_params.get(name))
                if actual_required != expected_required:
                    failures.append(
                        f"{endpoint_id}: query param {name} required mismatch "
                        f"(expected={expected_required}, actual={actual_required})"
                    )

        # 3) Required scopes (via dependency metadata)
        expected_scopes = entry.get("required_scopes") or []
        if not isinstance(expected_scopes, list):
            failures.append(f"{endpoint_id}: required_scopes must be a list")
            continue
        expected_scopes_sorted = tuple(sorted(str(s) for s in expected_scopes if str(s).strip()))
        if expected_scopes_sorted:
            route = route_index.get((path, method))
            if not route:
                failures.append(f"{endpoint_id}: missing FastAPI route for {method.upper()} {path}")
            else:
                actual_scopes = _route_required_scopes(route)
                if actual_scopes != expected_scopes_sorted:
                    failures.append(
                        f"{endpoint_id}: scopes mismatch "
                        f"(expected={expected_scopes_sorted}, actual={actual_scopes})"
                    )

    assert not failures, "Foundry contract manifest mismatches:\n- " + "\n- ".join(failures)
