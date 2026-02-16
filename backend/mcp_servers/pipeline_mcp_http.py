from __future__ import annotations

import os
import uuid
from typing import Any, Dict, Optional

import httpx

from mcp_servers.bff_auth import bff_admin_token as _bff_admin_token
from mcp_servers.bff_auth import bff_api_base_url
import logging


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
    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        resp = await client.request(method, url, headers=headers, json=json_body, params=params)
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


def oms_api_base_url() -> str:
    """Get OMS API base URL from environment."""
    return os.getenv("OMS_BASE_URL", "http://oms:8000").rstrip("/")


def _oms_admin_token() -> str:
    """Resolve OMS admin token from environment (same fallback as OMSClient)."""
    for key in ("OMS_CLIENT_TOKEN", "OMS_ADMIN_TOKEN", "ADMIN_API_KEY", "ADMIN_TOKEN"):
        val = (os.getenv(key) or "").strip()
        if val:
            return val
    return ""


async def oms_json(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout_seconds: float = 30.0,
) -> Dict[str, Any]:
    """Make an HTTP request to OMS API and return JSON response."""
    base = oms_api_base_url()
    url = f"{base}{path}"
    headers: Dict[str, str] = {"Content-Type": "application/json", "Accept": "application/json"}
    token = _oms_admin_token()
    if token:
        headers["X-Admin-Token"] = token
    return await http_json(
        method,
        url,
        headers=headers,
        json_body=json_body,
        params=params,
        timeout_seconds=timeout_seconds,
        error_prefix="OMS",
        error_path=path,
    )
