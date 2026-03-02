#!/usr/bin/env python3
"""Inventory collection for OpenAPI duplicate/legacy audit."""

from __future__ import annotations

import importlib
import inspect
import json
import logging
import os
import re
import sys
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Mapping

from fastapi.routing import APIRoute

METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"}

SERVICE_CONFIG: dict[str, dict[str, str]] = {
    "bff": {
        "module": "bff.main",
        "base_url_env": "BFF_BASE_URL",
        "default_base_url": "http://localhost:8002",
    },
    "oms": {
        "module": "oms.main",
        "base_url_env": "OMS_BASE_URL",
        "default_base_url": "http://localhost:8000",
    },
    "agent": {
        "module": "agent.main",
        "base_url_env": "AGENT_BASE_URL",
        "default_base_url": "http://localhost:8004",
    },
}


@dataclass
class EndpointRecord:
    endpoint_id: str
    service: str
    method: str
    path: str
    canonical_path: str
    include_in_schema: bool
    static_openapi_present: bool
    runtime_openapi_present: bool
    version: str
    domain: str
    tags: list[str]
    operation_id: str
    summary: str
    request_fingerprint: str
    response_fingerprint: str
    documented_statuses: list[str]
    has_request_body: bool
    source_file: str
    source_module: str
    source_qualname: str
    hidden_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ServiceDiff:
    service: str
    runtime_available: bool
    runtime_error: str | None
    static_operation_count: int
    runtime_operation_count: int
    static_only_count: int
    runtime_only_count: int
    static_only_samples: list[str]
    runtime_only_samples: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RouteStats:
    service: str
    total_routes: int
    schema_routes: int
    hidden_routes: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class InventoryCollection:
    endpoints: list[EndpointRecord]
    endpoint_callables: dict[str, Callable[..., Any]]
    service_diffs: dict[str, ServiceDiff]
    route_stats: dict[str, RouteStats]

    def to_dict(self) -> dict[str, Any]:
        return {
            "endpoints": [endpoint.to_dict() for endpoint in self.endpoints],
            "service_diffs": {name: diff.to_dict() for name, diff in self.service_diffs.items()},
            "route_stats": {name: stats.to_dict() for name, stats in self.route_stats.items()},
        }


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def backend_root() -> Path:
    return repo_root() / "backend"


def ensure_backend_import_path() -> None:
    backend_path = str(backend_root())
    if backend_path not in sys.path:
        sys.path.insert(0, backend_path)


def _tokenize_text(value: str) -> list[str]:
    return [token for token in re.split(r"[^a-zA-Z0-9]+", value.lower()) if token]


def _classify_path(path: str) -> tuple[str, str]:
    normalized = (path or "").strip()
    if normalized.startswith("/api/v1/") or normalized == "/api/v1":
        tail = normalized.removeprefix("/api/v1/").removeprefix("/api/v1")
        version = "v1"
    elif normalized.startswith("/api/v2/") or normalized == "/api/v2":
        tail = normalized.removeprefix("/api/v2/").removeprefix("/api/v2")
        version = "v2"
    elif normalized.startswith("/api/"):
        tail = normalized.removeprefix("/api/")
        version = "api-other"
    else:
        tail = normalized.lstrip("/")
        version = "other"

    domain = (tail.split("/", 1)[0] if tail else "root") or "root"
    return version, domain


def _canonicalize_path(path: str) -> str:
    canonical = re.sub(r"\{[^}]+\}", "{}", path.strip())
    canonical = re.sub(r"/+", "/", canonical)
    return canonical.lower() or "/"


def _json_fingerprint(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _read_schema_descriptor(schema: Mapping[str, Any] | None) -> str:
    if not isinstance(schema, Mapping):
        return "-"
    if "$ref" in schema:
        return str(schema["$ref"])
    schema_type = str(schema.get("type") or "-")
    schema_format = str(schema.get("format") or "-")
    return f"{schema_type}:{schema_format}"


def _extract_operation_meta(schema: Mapping[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    paths_obj = schema.get("paths")
    if not isinstance(paths_obj, dict):
        return result

    for path, methods_obj in paths_obj.items():
        if not isinstance(methods_obj, dict):
            continue
        for method, meta_obj in methods_obj.items():
            method_upper = str(method or "").upper()
            if method_upper not in METHODS:
                continue
            if not isinstance(meta_obj, dict):
                continue

            parameters_raw = meta_obj.get("parameters")
            parameter_shape: list[dict[str, Any]] = []
            if isinstance(parameters_raw, list):
                for param in parameters_raw:
                    if not isinstance(param, dict):
                        continue
                    parameter_shape.append(
                        {
                            "in": str(param.get("in") or "-"),
                            "name": str(param.get("name") or "-"),
                            "required": bool(param.get("required")),
                            "schema": _read_schema_descriptor(param.get("schema")),
                        }
                    )

            request_body = meta_obj.get("requestBody")
            has_request_body = isinstance(request_body, dict)
            request_shape: dict[str, Any] = {"required": False, "media": [], "schemas": []}
            if isinstance(request_body, dict):
                request_shape["required"] = bool(request_body.get("required"))
                content_obj = request_body.get("content")
                if isinstance(content_obj, dict):
                    media_types = sorted(str(media) for media in content_obj.keys())
                    request_shape["media"] = media_types
                    schema_descriptors: list[str] = []
                    for media in media_types:
                        media_obj = content_obj.get(media)
                        if isinstance(media_obj, dict):
                            schema_descriptors.append(_read_schema_descriptor(media_obj.get("schema")))
                    request_shape["schemas"] = sorted(schema_descriptors)

            responses_obj = meta_obj.get("responses")
            response_shape: dict[str, Any] = {}
            documented_statuses: list[str] = []
            if isinstance(responses_obj, dict):
                for status_code, response_meta in sorted(responses_obj.items(), key=lambda item: str(item[0])):
                    status_key = str(status_code)
                    documented_statuses.append(status_key)
                    media_types: list[str] = []
                    if isinstance(response_meta, dict):
                        content_obj = response_meta.get("content")
                        if isinstance(content_obj, dict):
                            media_types = sorted(str(media) for media in content_obj.keys())
                    response_shape[status_key] = media_types

            tags_raw = meta_obj.get("tags")
            tags = [str(tag) for tag in tags_raw if str(tag).strip()] if isinstance(tags_raw, list) else []

            result[(method_upper, str(path))] = {
                "tags": tags,
                "operation_id": str(meta_obj.get("operationId") or "").strip(),
                "summary": str(meta_obj.get("summary") or meta_obj.get("description") or "").strip(),
                "request_fingerprint": _json_fingerprint({"parameters": parameter_shape, "request": request_shape}),
                "response_fingerprint": _json_fingerprint(response_shape),
                "documented_statuses": documented_statuses,
                "has_request_body": has_request_body,
            }
    return result


def _load_service_module(service: str) -> Any:
    if service not in SERVICE_CONFIG:
        raise ValueError(f"Unsupported service: {service}")
    module_name = SERVICE_CONFIG[service]["module"]
    ensure_backend_import_path()
    logging.disable(logging.CRITICAL)
    try:
        return importlib.import_module(module_name)
    finally:
        logging.disable(logging.NOTSET)


def _route_key(method: str, path: str) -> tuple[str, str]:
    return (method.upper(), str(path))


def _collect_route_entries(service: str, app: Any) -> tuple[dict[tuple[str, str], dict[str, Any]], RouteStats]:
    entries: dict[tuple[str, str], dict[str, Any]] = {}
    total = 0
    schema_routes = 0
    hidden_routes = 0

    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        total += 1
        include_in_schema = bool(route.include_in_schema)
        if include_in_schema:
            schema_routes += 1
        else:
            hidden_routes += 1

        endpoint = inspect.unwrap(route.endpoint)
        endpoint_methods = sorted({method.upper() for method in (route.methods or set()) if method.upper() in METHODS})
        for method in endpoint_methods:
            entries[_route_key(method, route.path)] = {
                "include_in_schema": include_in_schema,
                "endpoint": endpoint,
                "source_module": str(getattr(endpoint, "__module__", "") or ""),
                "source_qualname": str(getattr(endpoint, "__qualname__", getattr(endpoint, "__name__", "")) or ""),
                "source_file": str(inspect.getsourcefile(endpoint) or ""),
                "hidden_reason": None if include_in_schema else "include_in_schema=false",
            }

    return entries, RouteStats(
        service=service,
        total_routes=total,
        schema_routes=schema_routes,
        hidden_routes=hidden_routes,
    )


def _runtime_openapi_url(service: str) -> str:
    cfg = SERVICE_CONFIG[service]
    base_url = (os.getenv(cfg["base_url_env"]) or cfg["default_base_url"]).strip().rstrip("/")
    return f"{base_url}/openapi.json"


def _admin_headers() -> dict[str, str]:
    admin_token = (os.getenv("ADMIN_TOKEN") or "change_me").strip() or "change_me"
    return {"X-Admin-Token": admin_token, "Accept": "application/json"}


def _fetch_runtime_openapi(service: str, timeout_seconds: float = 3.0) -> tuple[dict[str, Any] | None, str | None]:
    url = _runtime_openapi_url(service)
    request = urllib.request.Request(url, headers=_admin_headers(), method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
            data = response.read()
        payload = json.loads(data.decode("utf-8"))
        if isinstance(payload, dict):
            return payload, None
        return None, f"non-dict runtime schema payload from {url}"
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _service_operation_keys(schema: Mapping[str, Any]) -> set[tuple[str, str]]:
    return set(_extract_operation_meta(schema).keys())


def _sample_keys(keys: Iterable[tuple[str, str]], limit: int = 10) -> list[str]:
    return [f"{method} {path}" for method, path in sorted(keys)[: max(0, int(limit))]]


def _collect_service_inventory(service: str, *, include_hidden: bool) -> tuple[list[EndpointRecord], dict[str, Callable[..., Any]], ServiceDiff, RouteStats]:
    module = _load_service_module(service)
    app = getattr(module, "app", None)
    if app is None:
        raise RuntimeError(f"{service} module has no FastAPI app object")

    static_schema = app.openapi()
    if not isinstance(static_schema, dict):
        raise RuntimeError(f"{service} app.openapi() did not return a dict")

    route_entries, route_stats = _collect_route_entries(service, app)
    static_meta = _extract_operation_meta(static_schema)
    static_keys = set(static_meta.keys())

    runtime_schema, runtime_error = _fetch_runtime_openapi(service)
    runtime_meta = _extract_operation_meta(runtime_schema or {})
    runtime_keys = set(runtime_meta.keys())

    if runtime_schema is None:
        runtime_keys = static_keys.copy()

    service_diff = ServiceDiff(
        service=service,
        runtime_available=runtime_schema is not None,
        runtime_error=runtime_error,
        static_operation_count=len(static_keys),
        runtime_operation_count=len(runtime_keys),
        static_only_count=len(static_keys - runtime_keys),
        runtime_only_count=len(runtime_keys - static_keys),
        static_only_samples=_sample_keys(static_keys - runtime_keys),
        runtime_only_samples=_sample_keys(runtime_keys - static_keys),
    )

    all_keys = sorted(static_keys | set(route_entries.keys()), key=lambda item: (item[1], item[0]))
    endpoints: list[EndpointRecord] = []
    endpoint_callables: dict[str, Callable[..., Any]] = {}

    for method, path in all_keys:
        route_entry = route_entries.get((method, path), {})
        include_in_schema = bool(route_entry.get("include_in_schema", (method, path) in static_keys))
        if not include_hidden and not include_in_schema:
            continue

        meta = static_meta.get((method, path), {})
        version, domain = _classify_path(path)
        operation_id = str(meta.get("operation_id") or "").strip() or f"{service}_{method.lower()}_{domain}"
        endpoint_id = f"{service}:{method}:{path}"

        endpoint = EndpointRecord(
            endpoint_id=endpoint_id,
            service=service,
            method=method,
            path=path,
            canonical_path=_canonicalize_path(path),
            include_in_schema=include_in_schema,
            static_openapi_present=(method, path) in static_keys,
            runtime_openapi_present=(method, path) in runtime_keys,
            version=version,
            domain=domain,
            tags=list(meta.get("tags") or []),
            operation_id=operation_id,
            summary=str(meta.get("summary") or "").strip(),
            request_fingerprint=str(meta.get("request_fingerprint") or "{}"),
            response_fingerprint=str(meta.get("response_fingerprint") or "{}"),
            documented_statuses=[str(code) for code in (meta.get("documented_statuses") or [])],
            has_request_body=bool(meta.get("has_request_body")),
            source_file=str(route_entry.get("source_file") or ""),
            source_module=str(route_entry.get("source_module") or ""),
            source_qualname=str(route_entry.get("source_qualname") or ""),
            hidden_reason=route_entry.get("hidden_reason"),
        )
        endpoints.append(endpoint)

        endpoint_callable = route_entry.get("endpoint")
        if callable(endpoint_callable):
            endpoint_callables[endpoint_id] = endpoint_callable

    return endpoints, endpoint_callables, service_diff, route_stats


def collect_inventory(*, services: list[str], include_hidden: bool) -> InventoryCollection:
    endpoints: list[EndpointRecord] = []
    endpoint_callables: dict[str, Callable[..., Any]] = {}
    service_diffs: dict[str, ServiceDiff] = {}
    route_stats: dict[str, RouteStats] = {}

    for service in services:
        service_endpoints, service_callables, service_diff, service_route_stats = _collect_service_inventory(
            service,
            include_hidden=include_hidden,
        )
        endpoints.extend(service_endpoints)
        endpoint_callables.update(service_callables)
        service_diffs[service] = service_diff
        route_stats[service] = service_route_stats

    endpoints.sort(key=lambda endpoint: (endpoint.path, endpoint.method, endpoint.service))
    return InventoryCollection(
        endpoints=endpoints,
        endpoint_callables=endpoint_callables,
        service_diffs=service_diffs,
        route_stats=route_stats,
    )


def parse_services(raw: str) -> list[str]:
    services = [value.strip().lower() for value in (raw or "").split(",") if value.strip()]
    unknown = [service for service in services if service not in SERVICE_CONFIG]
    if unknown:
        raise ValueError(f"Unsupported services requested: {', '.join(sorted(unknown))}")
    if not services:
        return ["bff", "oms", "agent"]
    return services
