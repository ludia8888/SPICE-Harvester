from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional
from uuid import uuid4
from datetime import datetime, timezone

from fastapi import FastAPI, Request, WebSocket, status
from fastapi.responses import JSONResponse, Response

from shared.config.settings import get_settings
from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.security.auth_utils import extract_presented_token, is_exempt_path
from shared.security.user_context import UserPrincipal, UserTokenError, extract_bearer_token, verify_user_token
from shared.services.registries.agent_tool_registry import AgentToolPolicyRecord
from shared.observability.request_context import get_correlation_id, get_request_id
from shared.utils.token_count import approx_token_count_json
from shared.utils.llm_safety import digest_for_audit
from shared.utils.uuid_utils import safe_uuid as _safe_uuid

_EXEMPT_PATHS_DEFAULT = (
    "/api/v1/health",
    "/api/v1/",
    "/api/v1/auth/login",
    "/api/v1/auth/refresh",
)
logger = logging.getLogger(__name__)
_DELEGATED_AUTH_HEADER = "X-Delegated-Authorization"
_USER_CONTEXT_REQUIRED_PREFIXES = (
    "/api/v1/agent",
)
_AGENT_TOOL_ID_HEADER = "X-Agent-Tool-ID"
_AGENT_TOOL_RUN_ID_HEADER = "X-Agent-Tool-Run-ID"
_AGENT_SESSION_ID_HEADER = "X-Agent-Session-ID"
_AGENT_JOB_ID_HEADER = "X-Agent-Job-ID"
_AGENT_PLAN_ID_HEADER = "X-Agent-Plan-ID"
_IDEMPOTENCY_KEY_HEADER = "Idempotency-Key"
_AGENT_TOOL_AUTHZ_EXEMPT_PREFIXES = (
    "/api/v1/health",
    "/api/v1/commands/",
    "/api/v1/ws/commands/",
    "/api/v1/metrics",
)
_AGENT_TOOL_PATH_RE_CACHE: dict[str, re.Pattern[str]] = {}
_AGENT_TOOL_PATH_PARAMS_RE_CACHE: dict[str, re.Pattern[str]] = {}
_TENANT_POLICY_CACHE: dict[str, tuple[float, object]] = {}
_TENANT_POLICY_CACHE_TTL_S = 30.0
_PLATFORM_API_SCOPES = (
    "api:datasets-read",
    "api:datasets-write",
    "api:ontologies-read",
    "api:ontologies-write",
    "api:orchestration-read",
    "api:orchestration-write",
    "api:connectivity-read",
    "api:connectivity-write",
)


def _attach_pytest_scoped_principal(request: Request) -> None:
    if getattr(request.state, "user", None) is not None:
        return
    user_id = (
        str(request.headers.get("X-User-ID") or "").strip()
        or str(request.headers.get("X-Actor") or "").strip()
        or "pytest-user"
    )
    user_type = (
        str(request.headers.get("X-User-Type") or "").strip()
        or str(request.headers.get("X-Actor-Type") or "").strip()
        or "user"
    )
    principal = UserPrincipal(
        id=user_id,
        type=user_type,
        roles=("admin", "platform_admin"),
        tenant_id="default",
        org_id="default",
        verified=True,
        claims={
            "pytest": True,
            "scope": " ".join(_PLATFORM_API_SCOPES),
        },
    )
    _attach_verified_principal(request, principal)


def _dev_master_auth_enabled() -> bool:
    settings = get_settings()
    auth = settings.auth
    if not auth.dev_master_auth_enabled:
        return False
    return bool(settings.is_development)


def _attach_dev_master_principal(request: Request) -> None:
    settings = get_settings()
    auth = settings.auth
    roles = auth.dev_master_role_set or ("admin", "platform_admin")
    principal = UserPrincipal(
        id=str(auth.dev_master_user_id or "dev-admin"),
        type=str(auth.dev_master_user_type or "user"),
        roles=tuple(roles),
        tenant_id="default",
        org_id="default",
        verified=True,
        claims={"dev_master": True, "scope": " ".join(_PLATFORM_API_SCOPES)},
    )
    _attach_verified_principal(request, principal)
    request.state.dev_master_auth = True


def _attach_admin_token_principal(request: Request) -> None:
    if getattr(request.state, "user", None) is not None:
        return
    principal = UserPrincipal(
        # Treat admin-token traffic as a system actor. This keeps action validation/apply
        # usable in unconfigured dev stacks (edit policy enforcement is skipped for `system`).
        id="system",
        type="user",
        roles=("admin", "platform_admin"),
        tenant_id="default",
        org_id="default",
        verified=True,
        claims={"admin_token": True, "scope": " ".join(_PLATFORM_API_SCOPES)},
    )
    _attach_verified_principal(request, principal)


def _approx_token_count(payload: Any) -> int:
    return approx_token_count_json(payload, empty_collections_as_zero=True)


def _resolve_agent_tool_run_id(request: Request) -> str:
    existing = _safe_uuid(request.headers.get(_AGENT_TOOL_RUN_ID_HEADER))
    if existing:
        return existing
    tool_run_id = str(uuid4())
    _set_scope_header(request, _AGENT_TOOL_RUN_ID_HEADER, tool_run_id)
    return tool_run_id


def _error_response(
    *,
    request: Request,
    status_code: int,
    message: str,
    code: Optional[ErrorCode] = None,
    category: Optional[ErrorCategory] = None,
    detail: Optional[str] = None,
    context: Optional[dict[str, Any]] = None,
    headers: Optional[dict[str, str]] = None,
) -> JSONResponse:
    request_id = request.headers.get("X-Request-Id") or request.headers.get("X-Request-ID")
    correlation_id = request.headers.get("X-Correlation-Id") or request.headers.get("X-Correlation-ID")
    payload = build_error_envelope(
        service_name="bff",
        message=message,
        detail=detail,
        code=code,
        category=category,
        status_code=int(status_code),
        context=context,
        origin={
            "service": "bff",
            "method": request.method,
            "path": request.url.path,
        },
        request_id=request_id,
        correlation_id=correlation_id,
    )
    return JSONResponse(status_code=int(status_code), content=payload, headers=headers)


def _internal_error_payload(request: Request, *, message: str, detail: Optional[str] = None) -> dict[str, Any]:
    request_id = request.headers.get("X-Request-Id") or request.headers.get("X-Request-ID") or get_request_id()
    correlation_id = (
        request.headers.get("X-Correlation-Id") or request.headers.get("X-Correlation-ID") or get_correlation_id()
    )
    return build_error_envelope(
        service_name="bff",
        message=message,
        detail=detail or message,
        code=ErrorCode.INTERNAL_ERROR,
        category=ErrorCategory.INTERNAL,
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        origin={
            "service": "bff",
            "method": request.method,
            "path": request.url.path,
        },
        request_id=request_id,
        correlation_id=correlation_id,
    )


def _set_scope_header(request: Request, name: str, value: str) -> None:
    """
    Attach a trusted header into the ASGI scope so downstream code that still
    reads `request.headers[...]` observes the verified principal.

    This lets us migrate away from spoofable headers without rewriting every
    router at once.
    """
    key = name.lower().encode("utf-8")
    raw = list(request.scope.get("headers") or [])
    raw = [(k, v) for (k, v) in raw if k.lower() != key]
    raw.append((key, value.encode("utf-8")))
    request.scope["headers"] = raw


def _attach_verified_principal(request: Request, principal: UserPrincipal) -> None:
    request.state.user = principal
    request.state.principal = f"{principal.type}:{principal.id}"
    tenant_id = str(principal.tenant_id or principal.org_id or "default").strip() or "default"
    request.state.tenant_id = tenant_id
    _set_scope_header(request, "X-User-ID", principal.id)
    _set_scope_header(request, "X-User-Type", principal.type or "user")
    _set_scope_header(request, "X-Principal-Id", principal.id)
    _set_scope_header(request, "X-Principal-Type", principal.type or "user")
    _set_scope_header(request, "X-Actor", principal.id)
    _set_scope_header(request, "X-Actor-Type", principal.type or "user")
    _set_scope_header(request, "X-Tenant-ID", tenant_id)
    if principal.org_id:
        _set_scope_header(request, "X-Org-ID", str(principal.org_id))
    if principal.roles:
        _set_scope_header(request, "X-User-Roles", ",".join([str(r) for r in principal.roles if str(r).strip()]))


def _compile_agent_tool_path(pattern: str) -> re.Pattern[str]:
    normalized = (pattern or "").strip().rstrip("/") or "/"
    cached = _AGENT_TOOL_PATH_RE_CACHE.get(normalized)
    if cached:
        return cached
    escaped = re.escape(normalized)
    replaced = re.sub(r"\\{[^}]+\\}", r"[^/]+", escaped)
    # Support `{param:path}` patterns (FastAPI path converters) by widening the match.
    replaced = replaced.replace(r"[^/]+:path", r".+")
    compiled = re.compile(rf"^{replaced}/?$")
    _AGENT_TOOL_PATH_RE_CACHE[normalized] = compiled
    return compiled


def _path_matches_tool_policy(policy: AgentToolPolicyRecord, request_path: str) -> bool:
    template = (policy.path or "").strip()
    if not template:
        return True
    return bool(_compile_agent_tool_path(template).match(request_path or ""))


def _compile_agent_tool_path_params(pattern: str) -> re.Pattern[str]:
    normalized = (pattern or "").strip().rstrip("/") or "/"
    cached = _AGENT_TOOL_PATH_PARAMS_RE_CACHE.get(normalized)
    if cached:
        return cached

    parts: list[str] = []
    cursor = 0
    for match in re.finditer(r"{([^{}]+)}", normalized):
        parts.append(re.escape(normalized[cursor:match.start()]))
        token = str(match.group(1) or "").strip()
        if ":" in token:
            name, conv = token.split(":", 1)
        else:
            name, conv = token, ""
        name = name.strip()
        conv = conv.strip().lower()
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
            group = rf"(?P<{name}>.+)" if conv == "path" else rf"(?P<{name}>[^/]+)"
        else:
            group = r".+" if conv == "path" else r"[^/]+"
        parts.append(group)
        cursor = match.end()
    parts.append(re.escape(normalized[cursor:]))
    compiled = re.compile(rf"^{''.join(parts)}/?$")
    _AGENT_TOOL_PATH_PARAMS_RE_CACHE[normalized] = compiled
    return compiled


def _extract_agent_tool_path_params(policy: AgentToolPolicyRecord, request_path: str) -> dict[str, str]:
    template = (policy.path or "").strip()
    if not template:
        return {}
    match = _compile_agent_tool_path_params(template).match(request_path or "")
    if not match:
        return {}
    return {k: str(v) for k, v in match.groupdict().items() if v not in (None, "")}


def _resolve_agent_tool_id(request: Request) -> Optional[str]:
    value = (request.headers.get(_AGENT_TOOL_ID_HEADER) or "").strip()
    return value or None


async def _capture_agent_tool_request(request: Request) -> dict[str, Any]:
    body_obj: Any = None
    raw_body = b""
    try:
        raw_body = await request.body()
    except (RuntimeError, TypeError, ValueError) as exc:
        logger.warning("Failed to read request body for agent tool capture: %s", exc, exc_info=True)
        raw_body = b""
    if raw_body:
        try:
            body_obj = await request.json()
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            logger.warning("Failed to parse request JSON for agent tool capture: %s", exc, exc_info=True)
            body_obj = raw_body.decode("utf-8", errors="replace")

    query_items = []
    try:
        query_items = sorted([(k, v) for (k, v) in request.query_params.multi_items()])
    except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
        logger.warning("Failed to capture query params for agent tool request: %s", exc, exc_info=True)
        query_items = []

    payload = {
        "method": str(request.method or "").strip().upper(),
        "path": request.url.path,
        "query": query_items,
        "body": body_obj,
    }
    request.state.agent_tool_request = payload
    return payload


async def _maybe_start_session_tool_call(request: Request) -> None:
    if not bool(getattr(request.state, "is_internal_agent", False)):
        return
    tool_id = _resolve_agent_tool_id(request)
    if not tool_id:
        return
    session_id = _safe_uuid(request.headers.get(_AGENT_SESSION_ID_HEADER))
    if not session_id:
        return
    session_registry = _resolve_agent_session_registry(request)
    if session_registry is None:
        return

    tenant_id = str(getattr(request.state, "tenant_id", None) or "default").strip() or "default"
    job_id = _safe_uuid(request.headers.get(_AGENT_JOB_ID_HEADER))
    plan_id = _safe_uuid(request.headers.get(_AGENT_PLAN_ID_HEADER))
    idempotency_key = (request.headers.get(_IDEMPOTENCY_KEY_HEADER) or "").strip() or None

    tool_run_id = _resolve_agent_tool_run_id(request)
    request_payload = getattr(request.state, "agent_tool_request", None)
    if not isinstance(request_payload, dict):
        request_payload = await _capture_agent_tool_request(request)

    request_digest = digest_for_audit(
        {
            "tool_id": tool_id,
            "method": request_payload.get("method"),
            "path": request_payload.get("path"),
            "query": request_payload.get("query"),
            "body": request_payload.get("body"),
        }
    )

    request.state.agent_tool_started_monotonic = time.monotonic()
    request_token_count = _approx_token_count({"query": request_payload.get("query"), "body": request_payload.get("body")})
    request.state.agent_tool_session_ids = {
        "tenant_id": tenant_id,
        "session_id": session_id,
        "job_id": job_id,
        "plan_id": plan_id,
        "tool_run_id": tool_run_id,
        "tool_id": tool_id,
        "request_digest": request_digest,
        "idempotency_key": idempotency_key,
        "request_token_count": request_token_count,
    }

    try:
        await session_registry.start_tool_call(
            tool_run_id=tool_run_id,
            session_id=session_id,
            tenant_id=tenant_id,
            job_id=job_id,
            plan_id=plan_id,
            tool_id=tool_id,
            method=str(request_payload.get("method") or request.method or "GET"),
            path=str(request_payload.get("path") or request.url.path),
            query={"items": request_payload.get("query") or []},
            request_body=request_payload.get("body"),
            request_digest=request_digest,
            request_token_count=request_token_count,
            idempotency_key=idempotency_key,
            started_at=datetime.now(timezone.utc),
        )
        await session_registry.append_event(
            event_id=str(uuid4()),
            session_id=session_id,
            tenant_id=tenant_id,
            event_type="TOOL_CALL_STARTED",
            occurred_at=datetime.now(timezone.utc),
            data={
                "tool_run_id": tool_run_id,
                "tool_id": tool_id,
                "method": str(request_payload.get("method") or request.method or "GET"),
                "path": str(request_payload.get("path") or request.url.path),
                "request_digest": request_digest,
                "request_token_count": request_token_count,
                "job_id": job_id,
                "plan_id": plan_id,
                "idempotency_key": idempotency_key,
            },
        )
    except Exception as exc:
        logger.warning("Failed to persist TOOL_CALL_STARTED session event: %s", exc, exc_info=True)
        return


async def _finalize_session_tool_call(request: Request, response: Response, *, terminal_status: str) -> None:
    session_registry = _resolve_agent_session_registry(request)
    state = getattr(request.state, "agent_tool_session_ids", None)
    if session_registry is None or not isinstance(state, dict):
        return
    tenant_id = str(state.get("tenant_id") or "default")
    session_id = str(state.get("session_id") or "")
    tool_run_id = str(state.get("tool_run_id") or "")
    tool_id = str(state.get("tool_id") or "")
    if not (session_id and tool_run_id and tool_id):
        return

    latency_ms = None
    started_mono = getattr(request.state, "agent_tool_started_monotonic", None)
    if isinstance(started_mono, (int, float)):
        latency_ms = int(max(0.0, (time.monotonic() - float(started_mono))) * 1000.0)

    body_obj = getattr(request.state, "agent_tool_response_body", None)
    if body_obj is None:
        raw = getattr(response, "body", b"")
        if isinstance(raw, (bytes, bytearray)) and raw:
            try:
                body_obj = json.loads(bytes(raw).decode("utf-8", errors="replace"))
            except json.JSONDecodeError as exc:
                logger.warning("Failed to decode tool response body for session finalize: %s", exc, exc_info=True)
                body_obj = {"raw": bytes(raw).decode("utf-8", errors="replace")}
        else:
            body_obj = {}

    response_digest = digest_for_audit(body_obj)
    response_token_count = _approx_token_count(body_obj)
    error_code = None
    error_message = None
    side_effect_summary: dict[str, Any] = {}

    if isinstance(body_obj, dict):
        error_code = body_obj.get("code") if isinstance(body_obj.get("code"), str) else None
        error_message = body_obj.get("message") if isinstance(body_obj.get("message"), str) else None
        side_effect = body_obj.get("side_effect_summary")
        if isinstance(side_effect, dict):
            side_effect_summary = side_effect

    try:
        await session_registry.finish_tool_call(
            tool_run_id=tool_run_id,
            tenant_id=tenant_id,
            status=terminal_status,
            response_status=int(getattr(response, "status_code", 200)),
            response_body=body_obj,
            response_digest=response_digest,
            response_token_count=response_token_count,
            error_code=error_code,
            error_message=error_message,
            side_effect_summary=side_effect_summary,
            latency_ms=latency_ms,
            finished_at=datetime.now(timezone.utc),
        )
        await session_registry.append_event(
            event_id=str(uuid4()),
            session_id=session_id,
            tenant_id=tenant_id,
            event_type="TOOL_CALL_FINISHED",
            occurred_at=datetime.now(timezone.utc),
            data={
                "tool_run_id": tool_run_id,
                "tool_id": tool_id,
                "status": terminal_status,
                "http_status": int(getattr(response, "status_code", 200)),
                "response_digest": response_digest,
                "response_token_count": response_token_count,
                "latency_ms": latency_ms,
                "error_code": error_code,
                "error_message": error_message,
                "side_effect_summary": side_effect_summary,
            },
        )
    except Exception as exc:
        logger.warning("Failed to persist TOOL_CALL_FINISHED session event: %s", exc, exc_info=True)
        return


def _resolve_agent_tool_registry(request: Request):
    container = getattr(request.app.state, "bff_container", None)
    if container is None:
        return None
    try:
        return container.get_agent_tool_registry()
    except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
        logger.warning("Failed to resolve agent tool registry: %s", exc, exc_info=True)
        return None


def _resolve_agent_session_registry(request: Request):
    container = getattr(request.app.state, "bff_container", None)
    if container is None:
        return None
    try:
        return container.get_agent_session_registry()
    except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
        logger.warning("Failed to resolve agent session registry: %s", exc, exc_info=True)
        return None


def _resolve_agent_policy_registry(request: Request):
    container = getattr(request.app.state, "bff_container", None)
    if container is None:
        return None
    try:
        return container.get_agent_policy_registry()
    except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
        logger.warning("Failed to resolve agent policy registry: %s", exc, exc_info=True)
        return None


def _resolve_agent_registry(request: Request):
    container = getattr(request.app.state, "bff_container", None)
    if container is None:
        return None
    try:
        return container.get_agent_registry()
    except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
        logger.warning("Failed to resolve agent registry: %s", exc, exc_info=True)
        return None


async def _get_cached_tenant_policy(request: Request, tenant_id: str):  # noqa: ANN001
    registry = _resolve_agent_policy_registry(request)
    if registry is None:
        return None
    tenant_key = str(tenant_id or "").strip() or "default"
    now = time.monotonic()
    cached = _TENANT_POLICY_CACHE.get(tenant_key)
    if cached and cached[0] > now:
        return cached[1]
    policy = await registry.get_tenant_policy(tenant_id=tenant_key)
    _TENANT_POLICY_CACHE[tenant_key] = (now + float(_TENANT_POLICY_CACHE_TTL_S), policy)
    return policy


async def _enforce_internal_agent_tool_policy(request: Request) -> Optional[JSONResponse]:
    path = request.url.path or ""
    if any(path.startswith(prefix) for prefix in _AGENT_TOOL_AUTHZ_EXEMPT_PREFIXES):
        return None

    tool_id = _resolve_agent_tool_id(request)
    if not tool_id:
        return _error_response(
            request=request,
            status_code=status.HTTP_400_BAD_REQUEST,
            message=f"{_AGENT_TOOL_ID_HEADER} required for agent tool calls",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
            category=ErrorCategory.INPUT,
        )

    registry = _resolve_agent_tool_registry(request)
    if registry is None:
        # Degraded startup/unit-test mode: do not block the request, but keep the contract strict
        # when the tool registry is available.
        return None

    policy = await registry.get_tool_policy(tool_id=tool_id)
    if not policy:
        return _error_response(
            request=request,
            status_code=status.HTTP_403_FORBIDDEN,
            message=f"tool_id={tool_id} not in allowlist",
            code=ErrorCode.PERMISSION_DENIED,
            category=ErrorCategory.PERMISSION,
        )
    if str(policy.status or "").strip().upper() != "ACTIVE":
        return _error_response(
            request=request,
            status_code=status.HTTP_403_FORBIDDEN,
            message=f"tool_id={tool_id} is not ACTIVE",
            code=ErrorCode.PERMISSION_DENIED,
            category=ErrorCategory.PERMISSION,
        )
    if str(policy.method or "").strip().upper() != str(request.method or "").strip().upper():
        return _error_response(
            request=request,
            status_code=status.HTTP_403_FORBIDDEN,
            message=f"tool_id={tool_id} method mismatch",
            code=ErrorCode.PERMISSION_DENIED,
            category=ErrorCategory.PERMISSION,
        )
    if not _path_matches_tool_policy(policy, path):
        return _error_response(
            request=request,
            status_code=status.HTTP_403_FORBIDDEN,
            message=f"tool_id={tool_id} path mismatch",
            code=ErrorCode.PERMISSION_DENIED,
            category=ErrorCategory.PERMISSION,
        )

    if bool(policy.requires_idempotency_key):
        idem = (request.headers.get(_IDEMPOTENCY_KEY_HEADER) or "").strip()
        if not idem:
            return _error_response(
                request=request,
                status_code=status.HTTP_400_BAD_REQUEST,
                message=f"{_IDEMPOTENCY_KEY_HEADER} required for tool_id={tool_id}",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                category=ErrorCategory.INPUT,
            )

    required_roles = {str(role).strip() for role in (policy.roles or []) if str(role).strip()}
    if required_roles:
        principal = getattr(request.state, "user", None)
        user_roles = set(getattr(principal, "roles", ()) or ())
        if not user_roles or not (user_roles & required_roles):
            return _error_response(
                request=request,
                status_code=status.HTTP_403_FORBIDDEN,
                message="Permission denied",
                code=ErrorCode.PERMISSION_DENIED,
                category=ErrorCategory.PERMISSION,
            )

    principal = getattr(request.state, "user", None)
    tenant_id = str(getattr(principal, "tenant_id", None) or getattr(principal, "org_id", None) or "default").strip() or "default"

    session_registry = _resolve_agent_session_registry(request)
    if session_registry is not None:
        session_header = _safe_uuid(request.headers.get(_AGENT_SESSION_ID_HEADER))
        if not session_header:
            return _error_response(
                request=request,
                status_code=status.HTTP_400_BAD_REQUEST,
                message=f"{_AGENT_SESSION_ID_HEADER} required for agent tool calls",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                category=ErrorCategory.INPUT,
            )
        try:
            session = await session_registry.get_session(session_id=session_header, tenant_id=tenant_id)
        except Exception as exc:
            logger.warning("Failed to fetch agent session during policy check: %s", exc, exc_info=True)
            session = None
        user_id = str(getattr(principal, "id", "") or "").strip()
        if not session or (user_id and session.created_by != user_id):
            return _error_response(
                request=request,
                status_code=status.HTTP_403_FORBIDDEN,
                message="Session tool execution denied",
                code=ErrorCode.PERMISSION_DENIED,
                category=ErrorCategory.PERMISSION,
            )
        if str(session.status or "").strip().upper() == "TERMINATED":
            return _error_response(
                request=request,
                status_code=status.HTTP_409_CONFLICT,
                message="Session terminated",
                code=ErrorCode.CONFLICT,
                category=ErrorCategory.CONFLICT,
            )
        if tool_id not in set(session.enabled_tools or []):
            return _error_response(
                request=request,
                status_code=status.HTTP_403_FORBIDDEN,
                message=f"tool_id={tool_id} not enabled for session",
                code=ErrorCode.PERMISSION_DENIED,
                category=ErrorCategory.PERMISSION,
            )

    policy_rec = await _get_cached_tenant_policy(request, tenant_id)
    allowed_tools = {str(t).strip() for t in (getattr(policy_rec, "allowed_tools", None) or []) if str(t).strip()}
    if allowed_tools and tool_id not in allowed_tools:
        return _error_response(
            request=request,
            status_code=status.HTTP_403_FORBIDDEN,
            message=f"tool_id={tool_id} not allowed for tenant",
            code=ErrorCode.PERMISSION_DENIED,
            category=ErrorCategory.PERMISSION,
        )

    data_policies = getattr(policy_rec, "data_policies", None) or {}
    if isinstance(data_policies, dict) and data_policies:
        path_params = _extract_agent_tool_path_params(policy, path)
        body_obj: Any = None
        if any(
            data_policies.get(key)
            for key in (
                "allowed_dataset_ids",
                "allowed_pipeline_ids",
                "allowed_document_bundle_ids",
                "allowed_action_type_ids",
                "allowed_ontology_ids",
            )
        ):
            req_payload = getattr(request.state, "agent_tool_request", None)
            if not isinstance(req_payload, dict):
                req_payload = await _capture_agent_tool_request(request)
            if isinstance(req_payload, dict):
                body_obj = req_payload.get("body")

        def _body_lookup(keys: tuple[str, ...]) -> str:
            if not isinstance(body_obj, dict):
                return ""
            for key in keys:
                value = body_obj.get(key)
                if value not in (None, ""):
                    return str(value).strip()
            return ""
        allowed_db_names = {str(v).strip() for v in (data_policies.get("allowed_db_names") or []) if str(v).strip()}
        if allowed_db_names:
            db_name = (path_params.get("db_name") or request.query_params.get("db_name") or "").strip()
            if db_name and db_name not in allowed_db_names:
                return _error_response(
                    request=request,
                    status_code=status.HTTP_403_FORBIDDEN,
                    message="Permission denied",
                    code=ErrorCode.PERMISSION_DENIED,
                    category=ErrorCategory.PERMISSION,
                )

        allowed_branches = {str(v).strip() for v in (data_policies.get("allowed_branches") or []) if str(v).strip()}
        if allowed_branches:
            branch = (
                path_params.get("branch")
                or path_params.get("branch_name")
                or request.query_params.get("branch")
                or request.query_params.get("base_branch")
                or ""
            ).strip()
            if branch and branch not in allowed_branches:
                return _error_response(
                    request=request,
                    status_code=status.HTTP_403_FORBIDDEN,
                    message="Permission denied",
                    code=ErrorCode.PERMISSION_DENIED,
                    category=ErrorCategory.PERMISSION,
                )

        allowed_dataset_ids = {str(v).strip() for v in (data_policies.get("allowed_dataset_ids") or []) if str(v).strip()}
        if allowed_dataset_ids:
            dataset_id = (
                path_params.get("dataset_id")
                or request.query_params.get("dataset_id")
                or request.query_params.get("datasetId")
                or _body_lookup(("dataset_id", "datasetId"))
                or ""
            ).strip()
            if dataset_id and dataset_id not in allowed_dataset_ids:
                return _error_response(
                    request=request,
                    status_code=status.HTTP_403_FORBIDDEN,
                    message="Permission denied",
                    code=ErrorCode.PERMISSION_DENIED,
                    category=ErrorCategory.PERMISSION,
                )

        allowed_pipeline_ids = {str(v).strip() for v in (data_policies.get("allowed_pipeline_ids") or []) if str(v).strip()}
        if allowed_pipeline_ids:
            pipeline_id = (
                path_params.get("pipeline_id")
                or request.query_params.get("pipeline_id")
                or request.query_params.get("pipelineId")
                or _body_lookup(("pipeline_id", "pipelineId"))
                or ""
            ).strip()
            if pipeline_id and pipeline_id not in allowed_pipeline_ids:
                return _error_response(
                    request=request,
                    status_code=status.HTTP_403_FORBIDDEN,
                    message="Permission denied",
                    code=ErrorCode.PERMISSION_DENIED,
                    category=ErrorCategory.PERMISSION,
                )

        allowed_document_bundle_ids = {str(v).strip() for v in (data_policies.get("allowed_document_bundle_ids") or []) if str(v).strip()}
        if allowed_document_bundle_ids:
            bundle_id = (
                path_params.get("document_bundle_id")
                or path_params.get("bundle_id")
                or request.query_params.get("document_bundle_id")
                or request.query_params.get("bundle_id")
                or _body_lookup(("document_bundle_id", "bundle_id", "documentBundleId"))
                or ""
            ).strip()
            if bundle_id and bundle_id not in allowed_document_bundle_ids:
                return _error_response(
                    request=request,
                    status_code=status.HTTP_403_FORBIDDEN,
                    message="Permission denied",
                    code=ErrorCode.PERMISSION_DENIED,
                    category=ErrorCategory.PERMISSION,
                )

        allowed_action_type_ids = {str(v).strip() for v in (data_policies.get("allowed_action_type_ids") or []) if str(v).strip()}
        if allowed_action_type_ids:
            action_type_id = (
                path_params.get("action_type_id")
                or path_params.get("actionTypeId")
                or request.query_params.get("action_type_id")
                or request.query_params.get("actionTypeId")
                or _body_lookup(("action_type_id", "actionTypeId", "action_type", "actionType"))
                or ""
            ).strip()
            if action_type_id and action_type_id not in allowed_action_type_ids:
                return _error_response(
                    request=request,
                    status_code=status.HTTP_403_FORBIDDEN,
                    message="Permission denied",
                    code=ErrorCode.PERMISSION_DENIED,
                    category=ErrorCategory.PERMISSION,
                )

        allowed_ontology_ids = {str(v).strip() for v in (data_policies.get("allowed_ontology_ids") or []) if str(v).strip()}
        if allowed_ontology_ids:
            ontology_id = (
                path_params.get("ontology_id")
                or path_params.get("ontologyId")
                or path_params.get("class_id")
                or path_params.get("classId")
                or request.query_params.get("ontology_id")
                or request.query_params.get("ontologyId")
                or request.query_params.get("class_id")
                or request.query_params.get("classId")
                or _body_lookup(("ontology_id", "ontologyId", "class_id", "classId", "target_class_id", "targetClassId"))
                or ""
            ).strip()
            if ontology_id and ontology_id not in allowed_ontology_ids:
                return _error_response(
                    request=request,
                    status_code=status.HTTP_403_FORBIDDEN,
                    message="Permission denied",
                    code=ErrorCode.PERMISSION_DENIED,
                    category=ErrorCategory.PERMISSION,
                )

    request.state.agent_tool_policy = policy
    request.state.agent_tool_id = tool_id
    return None


async def _compute_agent_tool_idempotency_digest(request: Request, *, tool_id: str) -> str:
    body_obj: Any = None
    raw_body = b""
    try:
        raw_body = await request.body()
    except (RuntimeError, TypeError, ValueError) as exc:
        logger.warning("Failed to read request body for idempotency digest: %s", exc, exc_info=True)
        raw_body = b""
    if raw_body:
        try:
            body_obj = await request.json()
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            logger.warning("Failed to parse request JSON for idempotency digest: %s", exc, exc_info=True)
            body_obj = raw_body.decode("utf-8", errors="replace")

    query_items = []
    try:
        query_items = sorted([(k, v) for (k, v) in request.query_params.multi_items()])
    except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
        logger.warning("Failed to capture query params for idempotency digest: %s", exc, exc_info=True)
        query_items = []

    signature = {
        "tool_id": str(tool_id or "").strip(),
        "method": str(request.method or "").strip().upper(),
        "path": request.url.path,
        "query": query_items,
        "body": body_obj,
    }
    return digest_for_audit(signature)


async def _wait_for_tool_idempotency(  # noqa: ANN001
    registry,
    *,
    tenant_id: str,
    idempotency_key: str,
    timeout_s: float = 2.0,
    interval_s: float = 0.1,
):
    deadline = time.monotonic() + float(timeout_s)
    last = None
    while time.monotonic() < deadline:
        last = await registry.get_tool_idempotency(tenant_id=tenant_id, idempotency_key=idempotency_key)
        if last is None:
            return None
        status_value = str(getattr(last, "status", "") or "").strip().upper()
        if status_value and status_value != "IN_PROGRESS":
            return last
        await asyncio.sleep(float(interval_s))
    return last


async def _maybe_replay_or_start_tool_idempotency(request: Request) -> Optional[JSONResponse]:
    idempotency_key = (request.headers.get(_IDEMPOTENCY_KEY_HEADER) or "").strip()
    if not idempotency_key:
        return None
    tool_id = _resolve_agent_tool_id(request)
    if not tool_id:
        return None

    policy = getattr(request.state, "agent_tool_policy", None)
    registry = _resolve_agent_registry(request)
    if registry is None:
        if policy is not None and bool(getattr(policy, "requires_idempotency_key", False)):
            return _error_response(
                request=request,
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                message="Idempotency store unavailable",
                code=ErrorCode.UPSTREAM_UNAVAILABLE,
                category=ErrorCategory.UPSTREAM,
                context={"error": "idempotency/store-unavailable"},
            )
        return None

    tenant_id = str(getattr(request.state, "tenant_id", None) or "default").strip() or "default"
    request_digest = await _compute_agent_tool_idempotency_digest(request, tool_id=tool_id)

    try:
        record, created = await registry.begin_tool_idempotency(
            tenant_id=tenant_id,
            idempotency_key=idempotency_key,
            tool_id=tool_id,
            request_digest=request_digest,
        )
    except Exception as exc:
        logger.warning("Failed to begin tool idempotency transaction: %s", exc, exc_info=True)
        record, created = None, False

    if record is None:
        if policy is not None and bool(getattr(policy, "requires_idempotency_key", False)):
            return _error_response(
                request=request,
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                message="Idempotency store unavailable",
                code=ErrorCode.UPSTREAM_UNAVAILABLE,
                category=ErrorCategory.UPSTREAM,
                context={"error": "idempotency/store-unavailable"},
            )
        return None

    if not created:
        if str(getattr(record, "tool_id", "") or "").strip() != tool_id or str(
            getattr(record, "request_digest", "") or ""
        ).strip() != request_digest:
            return _error_response(
                request=request,
                status_code=status.HTTP_409_CONFLICT,
                message="Idempotency-Key conflict",
                code=ErrorCode.CONFLICT,
                category=ErrorCategory.CONFLICT,
                context={"error": "idempotency/conflict"},
            )

        status_value = str(getattr(record, "status", "") or "").strip().upper()
        if status_value == "COMPLETED":
            stored_status = int(getattr(record, "response_status", None) or 200)
            stored_body = getattr(record, "response_body", None)
            if stored_body is None:
                stored_body = {}
            response = JSONResponse(status_code=stored_status, content=stored_body)
            request.state.agent_tool_response_body = stored_body
            await _finalize_session_tool_call(request, response, terminal_status="REPLAYED")
            return response

        if status_value == "IN_PROGRESS":
            waited = await _wait_for_tool_idempotency(
                registry,
                tenant_id=tenant_id,
                idempotency_key=idempotency_key,
                timeout_s=2.0,
                interval_s=0.15,
            )
            if waited is not None and str(getattr(waited, "status", "") or "").strip().upper() == "COMPLETED":
                if str(getattr(waited, "tool_id", "") or "").strip() == tool_id and str(
                    getattr(waited, "request_digest", "") or ""
                ).strip() == request_digest:
                    stored_status = int(getattr(waited, "response_status", None) or 200)
                    stored_body = getattr(waited, "response_body", None)
                    if stored_body is None:
                        stored_body = {}
                    response = JSONResponse(status_code=stored_status, content=stored_body)
                    request.state.agent_tool_response_body = stored_body
                    await _finalize_session_tool_call(request, response, terminal_status="REPLAYED")
                    return response

            return _error_response(
                request=request,
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                message="Idempotency key in progress",
                code=ErrorCode.UPSTREAM_UNAVAILABLE,
                category=ErrorCategory.UPSTREAM,
                context={"error": "idempotency/in-progress"},
                headers={"Retry-After": "1"},
            )

    request.state.agent_tool_idempotency = {
        "tenant_id": tenant_id,
        "idempotency_key": idempotency_key,
        "tool_id": tool_id,
        "request_digest": request_digest,
    }
    return None


async def _finalize_tool_idempotency(request: Request, response: Response) -> Response:
    state = getattr(request.state, "agent_tool_idempotency", None)
    if not isinstance(state, dict):
        return response
    registry = _resolve_agent_registry(request)
    if registry is None:
        return response

    body_bytes = b""
    iterator = getattr(response, "body_iterator", None)
    if iterator is not None:
        try:
            chunks: list[bytes] = []
            async for chunk in iterator:
                if isinstance(chunk, str):
                    chunk = chunk.encode("utf-8")
                chunks.append(chunk)
            body_bytes = b"".join(chunks)
        except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
            logger.warning("Failed to buffer streaming tool response body: %s", exc, exc_info=True)
            body_bytes = b""
        headers = dict(response.headers)
        headers.pop("content-length", None)
        response = Response(
            content=body_bytes,
            status_code=int(getattr(response, "status_code", 200)),
            headers=headers,
            media_type=getattr(response, "media_type", None),
            background=getattr(response, "background", None),
        )
    else:
        raw = getattr(response, "body", None)
        if isinstance(raw, (bytes, bytearray)):
            body_bytes = bytes(raw)

    body_obj: Any = None
    try:
        if body_bytes:
            decoded = body_bytes.decode("utf-8", errors="replace")
            body_obj = json.loads(decoded)
        else:
            body_obj = {}
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        logger.warning("Failed to parse tool response body JSON for idempotency finalize: %s", exc, exc_info=True)
        body_obj = {}
    request.state.agent_tool_response_body = body_obj

    error_value = None
    if response.status_code >= 400:
        if isinstance(body_obj, dict):
            error_value = str(body_obj.get("message") or body_obj.get("detail") or "").strip() or None

    try:
        await registry.finalize_tool_idempotency(
            tenant_id=state["tenant_id"],
            idempotency_key=state["idempotency_key"],
            tool_id=state["tool_id"],
            request_digest=state["request_digest"],
            response_status=int(getattr(response, "status_code", 200)),
            response_body=body_obj,
            error=error_value,
        )
    except Exception as exc:
        logger.warning("Failed to finalize tool idempotency success path: %s", exc, exc_info=True)
        return response

    return response


async def _finalize_tool_idempotency_error(request: Request, exc: Exception) -> None:
    state = getattr(request.state, "agent_tool_idempotency", None)
    if not isinstance(state, dict):
        return
    registry = _resolve_agent_registry(request)
    if registry is None:
        return
    error_value = str(exc).strip()
    if error_value and len(error_value) > 500:
        error_value = error_value[:500] + "…"
    try:
        await registry.finalize_tool_idempotency(
            tenant_id=state["tenant_id"],
            idempotency_key=state["idempotency_key"],
            tool_id=state["tool_id"],
            request_digest=state["request_digest"],
            response_status=500,
            response_body=_internal_error_payload(request, message="Internal error"),
            error=error_value or "Internal error",
        )
    except Exception as finalize_exc:
        logger.warning("Failed to finalize tool idempotency error path: %s", finalize_exc, exc_info=True)
        return


def ensure_bff_auth_configured() -> None:
    auth = get_settings().auth
    if auth.bff_require_auth is False and not auth.bff_auth_disable_allowed:
        raise RuntimeError(
            "BFF auth explicitly disabled without approval. "
            "Set ALLOW_INSECURE_BFF_AUTH_DISABLE=true (or ALLOW_INSECURE_AUTH_DISABLE=true)."
        )
    if not auth.is_bff_auth_required(allow_pytest=True, default_required=True):
        return
    if auth.user_jwt_enabled and not (
        auth.user_jwt_hs256_secret or auth.user_jwt_public_key or auth.user_jwt_jwks_url
    ):
        raise RuntimeError(
            "USER_JWT_ENABLED=true but no verification key configured. "
            "Set USER_JWT_HS256_SECRET, USER_JWT_PUBLIC_KEY, or USER_JWT_JWKS_URL."
        )
    if not (auth.bff_expected_tokens or auth.bff_agent_tokens) and not auth.user_jwt_enabled:
        raise RuntimeError(
            "BFF auth is required but no token is configured. "
            "Set BFF_ADMIN_TOKEN (or BFF_WRITE_TOKEN/ADMIN_API_KEY/ADMIN_TOKEN)."
        )


_CallNext = Callable[[Request], Awaitable[Response]]


@dataclass(frozen=True)
class _BffAuthContext:
    settings: Any
    auth: Any
    dev_master: bool
    presented: Optional[str]
    agent_tokens: tuple[str, ...]
    expected_tokens: tuple[str, ...]


async def _bff_auth_handle_missing_token(request: Request, call_next: _CallNext, ctx: _BffAuthContext) -> Optional[Response]:
    if ctx.presented:
        return None
    if ctx.dev_master:
        _attach_dev_master_principal(request)
        return await call_next(request)
    return _error_response(
        request=request,
        status_code=status.HTTP_401_UNAUTHORIZED,
        message="Authentication required",
        code=ErrorCode.AUTH_REQUIRED,
        category=ErrorCategory.AUTH,
        headers={"WWW-Authenticate": "Bearer"},
    )


async def _bff_auth_handle_agent_token(request: Request, call_next: _CallNext, ctx: _BffAuthContext) -> Optional[Response]:
    presented = (ctx.presented or "").strip()
    if not presented:
        return None
    if not (ctx.agent_tokens and any(hmac.compare_digest(presented, token) for token in ctx.agent_tokens)):
        return None

    request.state.is_internal_agent = True
    delegated_raw = extract_bearer_token(request.headers.get(_DELEGATED_AUTH_HEADER)) or extract_bearer_token(
        request.headers.get("Authorization")
    )
    if not delegated_raw and ctx.dev_master:
        _attach_dev_master_principal(request)
        delegated_raw = None

    if not delegated_raw and not ctx.dev_master:
        return _error_response(
            request=request,
            status_code=status.HTTP_401_UNAUTHORIZED,
            message=f"{_DELEGATED_AUTH_HEADER} required for agent calls",
            code=ErrorCode.AUTH_REQUIRED,
            category=ErrorCategory.AUTH,
            headers={"WWW-Authenticate": "Bearer"},
        )

    if ctx.auth.user_jwt_enabled:
        if delegated_raw:
            try:
                principal = await verify_user_token(
                    delegated_raw,
                    jwt_enabled=bool(ctx.auth.user_jwt_enabled),
                    jwt_issuer=ctx.auth.user_jwt_issuer,
                    jwt_audience=ctx.auth.user_jwt_audience,
                    jwt_jwks_url=ctx.auth.user_jwt_jwks_url,
                    jwt_public_key=ctx.auth.user_jwt_public_key,
                    jwt_hs256_secret=ctx.auth.user_jwt_hs256_secret,
                    jwt_algorithms=ctx.auth.user_jwt_algorithms,
                )
                _attach_verified_principal(request, principal)
            except UserTokenError as exc:
                if ctx.dev_master:
                    _attach_dev_master_principal(request)
                else:
                    return _error_response(
                        request=request,
                        status_code=status.HTTP_403_FORBIDDEN,
                        message="Delegated user token invalid",
                        detail=str(exc),
                        code=ErrorCode.AUTH_INVALID,
                        category=ErrorCategory.AUTH,
                    )
        elif ctx.dev_master:
            _attach_dev_master_principal(request)
        else:
            return _error_response(
                request=request,
                status_code=status.HTTP_401_UNAUTHORIZED,
                message=f"{_DELEGATED_AUTH_HEADER} required for agent calls",
                code=ErrorCode.AUTH_REQUIRED,
                category=ErrorCategory.AUTH,
                headers={"WWW-Authenticate": "Bearer"},
            )
    else:
        if ctx.dev_master:
            _attach_dev_master_principal(request)
        else:
            return _error_response(
                request=request,
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                message="USER_JWT_ENABLED required for agent calls",
                code=ErrorCode.INTERNAL_ERROR,
                category=ErrorCategory.INTERNAL,
                context={"error": "user-jwt-required-for-agent"},
            )

    await _maybe_start_session_tool_call(request)
    denied = await _enforce_internal_agent_tool_policy(request)
    if denied is not None:
        denied_obj: Any = {}
        try:
            raw = denied.body if hasattr(denied, "body") else b""
            if isinstance(raw, (bytes, bytearray)) and raw:
                denied_obj = json.loads(bytes(raw).decode("utf-8", errors="replace"))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            logger.warning("Failed to parse denied response body for telemetry: %s", exc, exc_info=True)
        if denied_obj in (None, ""):
            denied_obj = {}
        request.state.agent_tool_response_body = denied_obj
        await _finalize_session_tool_call(request, denied, terminal_status="DENIED")
        return denied
    replay = await _maybe_replay_or_start_tool_idempotency(request)
    if replay is not None:
        return replay
    try:
        response = await call_next(request)
    except Exception as exc:
        await _finalize_tool_idempotency_error(request, exc)
        # Best-effort tool call telemetry for crashes.
        try:
            response = _error_response(
                request=request,
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                message="Internal error",
                code=ErrorCode.INTERNAL_ERROR,
                category=ErrorCategory.INTERNAL,
            )
            request.state.agent_tool_response_body = _internal_error_payload(request, message="Internal error")
            await _finalize_session_tool_call(request, response, terminal_status="FAILED")
        except Exception as telemetry_exc:
            logger.warning("Failed to finalize agent tool telemetry after crash: %s", telemetry_exc, exc_info=True)
        raise
    response = await _finalize_tool_idempotency(request, response)
    await _finalize_session_tool_call(
        request,
        response,
        terminal_status="COMPLETED" if int(getattr(response, "status_code", 200)) < 400 else "FAILED",
    )
    return response


async def _bff_auth_handle_expected_token(request: Request, call_next: _CallNext, ctx: _BffAuthContext) -> Optional[Response]:
    presented = (ctx.presented or "").strip()
    if not presented:
        return None
    if not (ctx.expected_tokens and any(hmac.compare_digest(presented, token) for token in ctx.expected_tokens)):
        return None

    if ctx.auth.user_jwt_enabled and request.url.path.startswith(_USER_CONTEXT_REQUIRED_PREFIXES):
        delegated_raw = extract_bearer_token(request.headers.get(_DELEGATED_AUTH_HEADER)) or extract_bearer_token(
            request.headers.get("Authorization")
        )
        if not delegated_raw:
            if ctx.dev_master:
                _attach_dev_master_principal(request)
                return await call_next(request)
            return _error_response(
                request=request,
                status_code=status.HTTP_401_UNAUTHORIZED,
                message="User JWT required for agent endpoints",
                code=ErrorCode.AUTH_REQUIRED,
                category=ErrorCategory.AUTH,
                headers={"WWW-Authenticate": "Bearer"},
            )
        try:
            principal = await verify_user_token(
                delegated_raw,
                jwt_enabled=bool(ctx.auth.user_jwt_enabled),
                jwt_issuer=ctx.auth.user_jwt_issuer,
                jwt_audience=ctx.auth.user_jwt_audience,
                jwt_jwks_url=ctx.auth.user_jwt_jwks_url,
                jwt_public_key=ctx.auth.user_jwt_public_key,
                jwt_hs256_secret=ctx.auth.user_jwt_hs256_secret,
                jwt_algorithms=ctx.auth.user_jwt_algorithms,
            )
            _attach_verified_principal(request, principal)
        except UserTokenError as exc:
            if ctx.dev_master:
                _attach_dev_master_principal(request)
                return await call_next(request)
            return _error_response(
                request=request,
                status_code=status.HTTP_403_FORBIDDEN,
                message="User JWT invalid",
                detail=str(exc),
                code=ErrorCode.AUTH_INVALID,
                category=ErrorCategory.AUTH,
            )

    # For all other expected-token traffic, attach a verified principal so Foundry-style
    # scope enforcement (`require_scopes`) has a concrete user context.
    if getattr(request.state, "user", None) is None:
        bearer = extract_bearer_token(request.headers.get("Authorization"))
        if bearer and ctx.auth.user_jwt_enabled:
            try:
                principal = await verify_user_token(
                    bearer,
                    jwt_enabled=bool(ctx.auth.user_jwt_enabled),
                    jwt_issuer=ctx.auth.user_jwt_issuer,
                    jwt_audience=ctx.auth.user_jwt_audience,
                    jwt_jwks_url=ctx.auth.user_jwt_jwks_url,
                    jwt_public_key=ctx.auth.user_jwt_public_key,
                    jwt_hs256_secret=ctx.auth.user_jwt_hs256_secret,
                    jwt_algorithms=ctx.auth.user_jwt_algorithms,
                )
                _attach_verified_principal(request, principal)
            except UserTokenError:
                # Keep the admin-token as the effective credential; fall back to an internal principal.
                pass

    if getattr(request.state, "user", None) is None:
        _attach_admin_token_principal(request)

    return await call_next(request)


async def _bff_auth_handle_user_jwt(request: Request, call_next: _CallNext, ctx: _BffAuthContext) -> Optional[Response]:
    if not ctx.auth.user_jwt_enabled:
        return None
    presented = (ctx.presented or "").strip()
    if not presented:
        return None
    try:
        principal = await verify_user_token(
            presented,
            jwt_enabled=bool(ctx.auth.user_jwt_enabled),
            jwt_issuer=ctx.auth.user_jwt_issuer,
            jwt_audience=ctx.auth.user_jwt_audience,
            jwt_jwks_url=ctx.auth.user_jwt_jwks_url,
            jwt_public_key=ctx.auth.user_jwt_public_key,
            jwt_hs256_secret=ctx.auth.user_jwt_hs256_secret,
            jwt_algorithms=ctx.auth.user_jwt_algorithms,
        )
        _attach_verified_principal(request, principal)
        return await call_next(request)
    except UserTokenError:
        return None


async def _bff_auth_handle_no_token_configured(
    request: Request, call_next: _CallNext, ctx: _BffAuthContext
) -> Optional[Response]:
    if ctx.expected_tokens or ctx.auth.user_jwt_enabled:
        return None
    return _error_response(
        request=request,
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        message="BFF auth required but no token configured",
        code=ErrorCode.INTERNAL_ERROR,
        category=ErrorCategory.INTERNAL,
    )


async def _bff_auth_handle_invalid_credentials(
    request: Request, call_next: _CallNext, ctx: _BffAuthContext
) -> Optional[Response]:
    return _error_response(
        request=request,
        status_code=status.HTTP_403_FORBIDDEN,
        message="Invalid authentication credentials",
        code=ErrorCode.AUTH_INVALID,
        category=ErrorCategory.AUTH,
    )


def install_bff_auth_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def _bff_auth_middleware(request: Request, call_next):
        settings = get_settings()
        auth = settings.auth
        if not auth.is_bff_auth_required(allow_pytest=True, default_required=True):
            if settings.is_pytest or bool(os.environ.get("PYTEST_CURRENT_TEST")):
                _attach_pytest_scoped_principal(request)
            return await call_next(request)

        exempt_paths = auth.resolve_bff_exempt_paths(defaults=_EXEMPT_PATHS_DEFAULT)
        if is_exempt_path(request.url.path, exempt_paths=exempt_paths):
            return await call_next(request)

        ctx = _BffAuthContext(
            settings=settings,
            auth=auth,
            dev_master=_dev_master_auth_enabled(),
            presented=extract_presented_token(request.headers),
            agent_tokens=auth.bff_agent_tokens,
            expected_tokens=auth.bff_expected_tokens,
        )

        handlers: tuple[Callable[[Request, _CallNext, _BffAuthContext], Awaitable[Optional[Response]]], ...] = (
            _bff_auth_handle_missing_token,
            _bff_auth_handle_agent_token,
            _bff_auth_handle_expected_token,
            _bff_auth_handle_user_jwt,
            _bff_auth_handle_no_token_configured,
            _bff_auth_handle_invalid_credentials,
        )
        for handler in handlers:
            response = await handler(request, call_next, ctx)
            if response is not None:
                return response

        return await call_next(request)


async def enforce_bff_websocket_auth(websocket: WebSocket, token: Optional[str]) -> bool:
    settings = get_settings()
    auth = settings.auth
    if not auth.is_bff_auth_required(allow_pytest=True, default_required=True):
        return True

    query_token = None
    try:
        query_token = websocket.query_params.get("token")
    except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
        logger.warning("Failed to read websocket query token: %s", exc, exc_info=True)
        query_token = None

    presented = token or query_token or extract_presented_token(websocket.headers)
    if not presented:
        has_query = bool(token or query_token)
        has_header = (
            "x-admin-token" in websocket.headers
            or "authorization" in websocket.headers
        )
        logger.warning(
            "WebSocket auth missing token (path=%s, has_query=%s, has_header=%s)",
            websocket.url.path,
            has_query,
            has_header,
        )
        await websocket.close(code=4401, reason="Authentication required")
        return False

    agent_tokens = auth.bff_agent_tokens
    if agent_tokens and any(hmac.compare_digest(presented, token) for token in agent_tokens):
        delegated_raw = (
            extract_bearer_token(websocket.headers.get(_DELEGATED_AUTH_HEADER))
            or extract_bearer_token(websocket.headers.get("Authorization"))
        )
        if not delegated_raw:
            await websocket.close(code=4401, reason=f"{_DELEGATED_AUTH_HEADER} required for agent calls")
            return False

        if auth.user_jwt_enabled:
            try:
                await verify_user_token(
                    delegated_raw,
                    jwt_enabled=bool(auth.user_jwt_enabled),
                    jwt_issuer=auth.user_jwt_issuer,
                    jwt_audience=auth.user_jwt_audience,
                    jwt_jwks_url=auth.user_jwt_jwks_url,
                    jwt_public_key=auth.user_jwt_public_key,
                    jwt_hs256_secret=auth.user_jwt_hs256_secret,
                    jwt_algorithms=auth.user_jwt_algorithms,
                )
            except UserTokenError:
                await websocket.close(code=4403, reason="Delegated user token invalid")
                return False
        else:
            await websocket.close(code=1011, reason="USER_JWT_ENABLED required for agent calls")
            return False
        return True

    expected_tokens = auth.bff_expected_tokens
    if expected_tokens and any(hmac.compare_digest(presented, token) for token in expected_tokens):
        return True

    if auth.user_jwt_enabled:
        try:
            await verify_user_token(
                presented,
                jwt_enabled=bool(auth.user_jwt_enabled),
                jwt_issuer=auth.user_jwt_issuer,
                jwt_audience=auth.user_jwt_audience,
                jwt_jwks_url=auth.user_jwt_jwks_url,
                jwt_public_key=auth.user_jwt_public_key,
                jwt_hs256_secret=auth.user_jwt_hs256_secret,
                jwt_algorithms=auth.user_jwt_algorithms,
            )
            return True
        except UserTokenError:
            pass

    if not expected_tokens and not auth.user_jwt_enabled:
        await websocket.close(code=1011, reason="BFF auth required but no token configured")
        return False

    logger.warning(
        "WebSocket auth token mismatch (path=%s)",
        websocket.url.path,
    )
    await websocket.close(code=4403, reason="Invalid authentication credentials")
    return False
