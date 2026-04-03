from __future__ import annotations

import asyncio
import re
import uuid
from typing import Any, Dict, Optional
from urllib.parse import unquote

import grpc
import httpx
from shared.config.settings import get_settings
from shared.services.grpc.oms_gateway_client import OMSGrpcHttpCompatClient

from mcp_servers.bff_auth import bff_admin_token as _bff_admin_token
from mcp_servers.bff_auth import bff_api_base_url, bff_api_v2_base_url
import logging


_OMS_GRPC_COMPAT_CLIENT: Optional[OMSGrpcHttpCompatClient] = None
_OMS_GRPC_COMPAT_CLIENT_LOCK = asyncio.Lock()


async def http_json(
    method: str,
    url: str,
    *,
    headers: Dict[str, str],
    json_body: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout_seconds: float = 30.0,
    error_prefix: str = "HTTP",
    error_path: str = "",
) -> Dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            resp = await client.request(method, url, headers=headers, json=json_body, params=params)
    except httpx.TimeoutException as exc:
        path = error_path or url
        return {
            "error": f"{error_prefix} {method} {path} timed out",
            "status_code": 504,
            "response": {"detail": str(exc)},
        }
    except httpx.RequestError as exc:
        path = error_path or url
        return {
            "error": f"{error_prefix} {method} {path} unavailable",
            "status_code": 503,
            "response": {"detail": str(exc)},
        }
    try:
        payload = resp.json()
    except Exception:
        logging.getLogger(__name__).warning("Exception fallback at mcp_servers/pipeline_mcp_http.py:28", exc_info=True)
        payload = {"raw": (resp.text or "").strip()}
    if resp.status_code >= 400:
        detail = payload.get("detail") if isinstance(payload, dict) else None
        message = payload.get("message") if isinstance(payload, dict) else None
        path = error_path or url
        return {
            "error": message or detail or f"{error_prefix} {method} {path} failed ({resp.status_code})",
            "status_code": resp.status_code,
            "response": payload,
        }
    return payload if isinstance(payload, dict) else {"response": payload}


def bff_headers(
    *,
    db_name: str,
    principal_id: Optional[str],
    principal_type: Optional[str],
) -> Dict[str, str]:
    token = _bff_admin_token()
    if not token:
        raise RuntimeError("BFF admin token unavailable (set BFF_ADMIN_TOKEN or ADMIN_TOKEN)")

    headers: Dict[str, str] = {
        "X-Admin-Token": token,
        "Content-Type": "application/json",
    }
    db_name = str(db_name or "").strip()
    if db_name:
        headers["X-DB-Name"] = db_name
        headers["X-Project"] = db_name
        headers["X-Project-Scope"] = db_name

    pid = (principal_id or "").strip() or None
    ptype = (principal_type or "").strip().lower() or None
    if pid:
        headers["X-Principal-Id"] = pid
        headers["X-User-ID"] = pid
        headers["X-Actor"] = pid
    if ptype in {"user", "service"}:
        headers["X-Principal-Type"] = ptype
        headers["X-Actor-Type"] = ptype
    return headers


def resolve_db_name_for_bff_call(
    *,
    db_name: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
) -> str:
    """Resolve db_name for generic BFF calls from explicit args, payload, or path."""
    explicit = str(db_name or "").strip()
    if explicit:
        return explicit

    query = params if isinstance(params, dict) else {}
    body = json_body if isinstance(json_body, dict) else {}
    keys = ("db_name", "dbName", "database_name", "database", "project", "project_name")

    for source in (query, body):
        for key in keys:
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    raw_path = str(path or "").strip()
    path_only = raw_path.split("?", 1)[0]
    decoded = unquote(path_only)
    patterns = (
        r"^/api/v\d+/databases/([^/]+)(?:/|$)",
        r"^/databases/([^/]+)(?:/|$)",
        r"^/api/v\d+/database/([^/]+)(?:/|$)",
        r"^/database/([^/]+)(?:/|$)",
        r"^/api/v1/graph-query/([^/]+)(?:/|$)",
        r"^/graph-query/([^/]+)(?:/|$)",
    )
    for pattern in patterns:
        match = re.match(pattern, decoded)
        if match:
            return match.group(1)
    return ""


async def bff_json(
    method: str,
    path: str,
    *,
    db_name: str,
    principal_id: Optional[str],
    principal_type: Optional[str],
    json_body: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout_seconds: float = 30.0,
) -> Dict[str, Any]:
    base = bff_api_base_url()
    url = f"{base}{path}"
    headers = bff_headers(db_name=db_name, principal_id=principal_id, principal_type=principal_type)
    # BFF requires Idempotency-Key for mutating requests (POST/PUT/PATCH).
    if method.upper() in {"POST", "PUT", "PATCH"} and "Idempotency-Key" not in headers:
        headers["Idempotency-Key"] = f"mcp-{uuid.uuid4()}"
    return await http_json(
        method,
        url,
        headers=headers,
        json_body=json_body,
        params=params,
        timeout_seconds=timeout_seconds,
        error_prefix="BFF",
        error_path=path,
    )


async def bff_v2_json(
    method: str,
    path: str,
    *,
    db_name: str,
    principal_id: Optional[str],
    principal_type: Optional[str],
    json_body: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout_seconds: float = 30.0,
) -> Dict[str, Any]:
    """Make a request to BFF v2 API. *path* should start with /v2/... (e.g. /v2/orchestration/builds/create)."""
    base = bff_api_v2_base_url()
    url = f"{base}{path}"
    headers = bff_headers(db_name=db_name, principal_id=principal_id, principal_type=principal_type)
    if method.upper() in {"POST", "PUT", "PATCH"} and "Idempotency-Key" not in headers:
        headers["Idempotency-Key"] = f"mcp-{uuid.uuid4()}"
    return await http_json(
        method,
        url,
        headers=headers,
        json_body=json_body,
        params=params,
        timeout_seconds=timeout_seconds,
        error_prefix="BFF-v2",
        error_path=path,
    )


def _oms_admin_token() -> str:
    """Resolve OMS admin token from centralized settings."""
    return str(get_settings().clients.oms_client_token or "").strip()


async def oms_json(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout_seconds: float = 30.0,
) -> Dict[str, Any]:
    """Make an OMS request through gRPC bridge and return JSON response."""
    headers: Dict[str, str] = {"Content-Type": "application/json", "Accept": "application/json"}
    token = _oms_admin_token()
    if token:
        headers["X-Admin-Token"] = token

    compat = await _get_oms_grpc_compat_client()
    try:
        method_upper = method.upper()
        class_fetch_match = re.fullmatch(r"/api/v1/database/([^/]+)/ontology/([^/]+)", str(path))
        if method_upper == "GET" and class_fetch_match:
            db_name = class_fetch_match.group(1)
            class_id = class_fetch_match.group(2)
            branch_value = str((params or {}).get("branch") or "main")
            resp = await asyncio.wait_for(
                compat.get_ontology_typed(
                    db_name=db_name,
                    class_id=class_id,
                    branch=branch_value,
                    headers=headers,
                ),
                timeout=timeout_seconds,
            )
        else:
            resp = await asyncio.wait_for(
                compat.request(
                    method_upper,
                    path,
                    params=params,
                    headers=headers,
                    json_body=json_body,
                ),
                timeout=timeout_seconds,
            )
    except asyncio.TimeoutError:
        return {
            "error": f"OMS {method.upper()} {path} timed out after {timeout_seconds:.1f}s",
            "status_code": 504,
            "response": {},
        }
    except httpx.TimeoutException as exc:
        return {
            "error": f"OMS {method.upper()} {path} timed out: {exc}",
            "status_code": 504,
            "response": {},
        }
    except httpx.RequestError as exc:
        return {
            "error": f"OMS {method.upper()} {path} unavailable: {exc}",
            "status_code": 503,
            "response": {},
        }
    except grpc.RpcError as exc:
        code_func = getattr(exc, "code", None)
        status_code = code_func() if callable(code_func) else None
        if status_code == grpc.StatusCode.DEADLINE_EXCEEDED:
            return {
                "error": f"OMS {method.upper()} {path} timed out",
                "status_code": 504,
                "response": {},
            }
        if status_code in {
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
        }:
            return {
                "error": f"OMS {method.upper()} {path} unavailable",
                "status_code": 503,
                "response": {},
            }
        logging.getLogger(__name__).error("OMS bridge internal error for %s %s", method.upper(), path, exc_info=True)
        return {
            "error": f"OMS {method.upper()} {path} internal bridge failure: {exc}",
            "status_code": 500,
            "response": {"detail": str(exc)},
        }
    except Exception as exc:
        logging.getLogger(__name__).error("OMS bridge internal error for %s %s", method.upper(), path, exc_info=True)
        return {
            "error": f"OMS {method.upper()} {path} internal bridge failure: {exc}",
            "status_code": 500,
            "response": {"detail": str(exc)},
        }

    try:
        payload = resp.json()
    except Exception:
        logging.getLogger(__name__).warning("Exception fallback at mcp_servers/pipeline_mcp_http.py:oms_json", exc_info=True)
        payload = {"raw": (resp.text or "").strip()}

    if resp.status_code >= 400:
        detail = payload.get("detail") if isinstance(payload, dict) else None
        message = payload.get("message") if isinstance(payload, dict) else None
        return {
            "error": message or detail or f"OMS {method.upper()} {path} failed ({resp.status_code})",
            "status_code": resp.status_code,
            "response": payload,
        }
    return payload if isinstance(payload, dict) else {"response": payload}


async def _get_oms_grpc_compat_client() -> OMSGrpcHttpCompatClient:
    global _OMS_GRPC_COMPAT_CLIENT
    if _OMS_GRPC_COMPAT_CLIENT is not None:
        return _OMS_GRPC_COMPAT_CLIENT
    async with _OMS_GRPC_COMPAT_CLIENT_LOCK:
        if _OMS_GRPC_COMPAT_CLIENT is None:
            _OMS_GRPC_COMPAT_CLIENT = OMSGrpcHttpCompatClient()
    return _OMS_GRPC_COMPAT_CLIENT


async def close_oms_grpc_compat_client() -> None:
    global _OMS_GRPC_COMPAT_CLIENT
    async with _OMS_GRPC_COMPAT_CLIENT_LOCK:
        client = _OMS_GRPC_COMPAT_CLIENT
        _OMS_GRPC_COMPAT_CLIENT = None
    if client is not None:
        await client.aclose()
