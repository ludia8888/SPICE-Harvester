"""
Agent proxy router (BFF).

Ensures the Agent service is only reachable through the BFF.
"""

from __future__ import annotations

import logging
from typing import Dict

import httpx
from fastapi import APIRouter, HTTPException, Request, Response, status

from shared.config.settings import build_client_ssl_config, get_settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agent", tags=["Agent"])

_FORWARD_HEADER_ALLOWLIST = {
    "accept",
    "accept-language",
    "authorization",
    "content-type",
    "user-agent",
    "x-admin-token",
    "x-delegated-authorization",
    "x-actor",
    "x-actor-type",
    "x-org-id",
    "x-principal-id",
    "x-principal-type",
    "x-request-id",
    "x-tenant-id",
    "x-user",
    "x-user-id",
    "x-user-roles",
    "x-user-type",
}
_CALLER_HEADER = "x-spice-caller"
_BLOCKED_CALLER = "agent"

_HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
}


def _forward_headers(request: Request) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for key, value in request.headers.items():
        if not value:
            continue
        if key.lower() in _FORWARD_HEADER_ALLOWLIST:
            headers[key] = value
    return headers


def _filter_response_headers(headers: httpx.Headers) -> Dict[str, str]:
    output: Dict[str, str] = {}
    for key, value in headers.items():
        if key.lower() in _HOP_BY_HOP_HEADERS:
            continue
        output[key] = value
    return output


async def _proxy_agent_request(request: Request, path: str) -> Response:
    caller = (request.headers.get("X-Spice-Caller") or "").strip().lower()
    if caller == _BLOCKED_CALLER:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Agent proxy loop blocked",
        )
    settings = get_settings()
    agent_url = settings.services.agent_base_url
    suffix = path.lstrip("/")
    target_path = "/api/v1/agent"
    if suffix:
        target_path = f"{target_path}/{suffix}"
    url = f"{agent_url}{target_path}"

    timeout_seconds = float(settings.clients.agent_proxy_timeout_seconds)
    ssl_config = build_client_ssl_config(settings)
    headers = _forward_headers(request)
    body = await request.body()

    try:
        async with httpx.AsyncClient(
            timeout=timeout_seconds,
            verify=ssl_config.get("verify", True),
        ) as client:
            response = await client.request(
                request.method,
                url,
                params=request.query_params,
                content=body,
                headers=headers,
            )
    except httpx.RequestError as exc:
        logger.error("Agent proxy request failed: %s %s (%s)", request.method, url, exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Agent service request failed",
        ) from exc

    return Response(
        content=response.content,
        status_code=response.status_code,
        headers=_filter_response_headers(response.headers),
    )


@router.post("/runs", status_code=status.HTTP_202_ACCEPTED)
async def create_agent_run(request: Request) -> Response:
    return await _proxy_agent_request(request, "runs")


@router.post("/pipeline-runs")
async def create_pipeline_run(request: Request) -> Response:
    return await _proxy_agent_request(request, "pipeline-runs")


@router.get("/runs/{run_id}")
async def get_agent_run(request: Request, run_id: str) -> Response:
    return await _proxy_agent_request(request, f"runs/{run_id}")


@router.get("/runs/{run_id}/events")
async def list_agent_run_events(request: Request, run_id: str) -> Response:
    return await _proxy_agent_request(request, f"runs/{run_id}/events")
