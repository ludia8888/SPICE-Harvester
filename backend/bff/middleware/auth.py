from __future__ import annotations

import hmac
import logging
import re
from typing import Optional

from fastapi import FastAPI, Request, WebSocket, status
from fastapi.responses import JSONResponse

from shared.config.settings import get_settings
from shared.security.auth_utils import extract_presented_token, is_exempt_path
from shared.security.user_context import UserPrincipal, UserTokenError, extract_bearer_token, verify_user_token
from shared.services.agent_tool_registry import AgentToolPolicyRecord

_EXEMPT_PATHS_DEFAULT = (
    "/api/v1/health",
    "/api/v1/",
    "/api/v1/data-connectors/google-sheets/oauth/callback",
)
logger = logging.getLogger(__name__)
_DELEGATED_AUTH_HEADER = "X-Delegated-Authorization"
_USER_CONTEXT_REQUIRED_PREFIXES = (
    "/api/v1/agent",
    "/api/v1/agent-plans",
    "/api/v1/agent-sessions",
)
_AGENT_TOOL_ID_HEADER = "X-Agent-Tool-ID"
_AGENT_TOOL_RUN_ID_HEADER = "X-Agent-Tool-Run-ID"
_AGENT_TOOL_AUTHZ_EXEMPT_PREFIXES = (
    "/api/v1/health",
    "/api/v1/commands/",
    "/api/v1/ws/commands/",
    "/api/v1/metrics",
)
_AGENT_TOOL_PATH_RE_CACHE: dict[str, re.Pattern[str]] = {}


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
    _set_scope_header(request, "X-User-ID", principal.id)
    _set_scope_header(request, "X-User-Type", principal.type or "user")
    _set_scope_header(request, "X-Principal-Id", principal.id)
    _set_scope_header(request, "X-Principal-Type", principal.type or "user")
    _set_scope_header(request, "X-Actor", principal.id)
    _set_scope_header(request, "X-Actor-Type", principal.type or "user")


def _compile_agent_tool_path(pattern: str) -> re.Pattern[str]:
    normalized = (pattern or "").strip().rstrip("/") or "/"
    cached = _AGENT_TOOL_PATH_RE_CACHE.get(normalized)
    if cached:
        return cached
    escaped = re.escape(normalized)
    replaced = re.sub(r"\\{[^}]+\\}", r"[^/]+", escaped)
    compiled = re.compile(rf"^{replaced}/?$")
    _AGENT_TOOL_PATH_RE_CACHE[normalized] = compiled
    return compiled


def _path_matches_tool_policy(policy: AgentToolPolicyRecord, request_path: str) -> bool:
    template = (policy.path or "").strip()
    if not template:
        return True
    return bool(_compile_agent_tool_path(template).match(request_path or ""))


def _resolve_agent_tool_id(request: Request) -> Optional[str]:
    value = (request.headers.get(_AGENT_TOOL_ID_HEADER) or "").strip()
    return value or None


def _resolve_agent_tool_registry(request: Request):
    container = getattr(request.app.state, "bff_container", None)
    if container is None:
        return None
    try:
        return container.get_agent_tool_registry()
    except Exception:
        return None


async def _enforce_internal_agent_tool_policy(request: Request) -> Optional[JSONResponse]:
    path = request.url.path or ""
    if any(path.startswith(prefix) for prefix in _AGENT_TOOL_AUTHZ_EXEMPT_PREFIXES):
        return None

    tool_id = _resolve_agent_tool_id(request)
    if not tool_id:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"detail": f"{_AGENT_TOOL_ID_HEADER} required for agent tool calls"},
        )

    registry = _resolve_agent_tool_registry(request)
    if registry is None:
        # Degraded startup/unit-test mode: do not block the request, but keep the contract strict
        # when the tool registry is available.
        return None

    policy = await registry.get_tool_policy(tool_id=tool_id)
    if not policy:
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={"detail": f"tool_id={tool_id} not in allowlist"},
        )
    if str(policy.status or "").strip().upper() != "ACTIVE":
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={"detail": f"tool_id={tool_id} is not ACTIVE"},
        )
    if str(policy.method or "").strip().upper() != str(request.method or "").strip().upper():
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={"detail": f"tool_id={tool_id} method mismatch"},
        )
    if not _path_matches_tool_policy(policy, path):
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={"detail": f"tool_id={tool_id} path mismatch"},
        )

    required_roles = {str(role).strip() for role in (policy.roles or []) if str(role).strip()}
    if required_roles:
        principal = getattr(request.state, "user", None)
        user_roles = set(getattr(principal, "roles", ()) or ())
        if not user_roles or not (user_roles & required_roles):
            return JSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={"detail": "Permission denied"},
            )

    return None


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
    if not auth.bff_expected_token and not auth.user_jwt_enabled:
        raise RuntimeError(
            "BFF auth is required but no token is configured. "
            "Set BFF_ADMIN_TOKEN (or BFF_WRITE_TOKEN/ADMIN_API_KEY/ADMIN_TOKEN)."
        )


def install_bff_auth_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def _bff_auth_middleware(request: Request, call_next):
        auth = get_settings().auth
        if not auth.is_bff_auth_required(allow_pytest=True, default_required=True):
            return await call_next(request)

        exempt_paths = auth.resolve_bff_exempt_paths(defaults=_EXEMPT_PATHS_DEFAULT)
        if is_exempt_path(request.url.path, exempt_paths=exempt_paths):
            return await call_next(request)

        presented = extract_presented_token(request.headers)
        if not presented:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                headers={"WWW-Authenticate": "Bearer"},
                content={"detail": "Authentication required"},
            )

        agent_token = (auth.bff_agent_token or "").strip()
        if agent_token and hmac.compare_digest(presented, agent_token):
            request.state.is_internal_agent = True
            delegated_raw = extract_bearer_token(request.headers.get(_DELEGATED_AUTH_HEADER))
            if auth.user_jwt_enabled:
                if not delegated_raw:
                    return JSONResponse(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        headers={"WWW-Authenticate": "Bearer"},
                        content={"detail": f"{_DELEGATED_AUTH_HEADER} required for agent calls"},
                    )
                try:
                    principal = await verify_user_token(
                        delegated_raw,
                        jwt_enabled=bool(auth.user_jwt_enabled),
                        jwt_issuer=auth.user_jwt_issuer,
                        jwt_audience=auth.user_jwt_audience,
                        jwt_jwks_url=auth.user_jwt_jwks_url,
                        jwt_public_key=auth.user_jwt_public_key,
                        jwt_hs256_secret=auth.user_jwt_hs256_secret,
                        jwt_algorithms=auth.user_jwt_algorithms,
                    )
                    _attach_verified_principal(request, principal)
                except UserTokenError as exc:
                    return JSONResponse(
                        status_code=status.HTTP_403_FORBIDDEN,
                        content={"detail": f"Delegated user token invalid: {exc}"},
                    )
                denied = await _enforce_internal_agent_tool_policy(request)
                if denied is not None:
                    return denied
            return await call_next(request)

        expected = auth.bff_expected_token
        if expected and hmac.compare_digest(presented, expected):
            if auth.user_jwt_enabled and request.url.path.startswith(_USER_CONTEXT_REQUIRED_PREFIXES):
                delegated_raw = extract_bearer_token(request.headers.get(_DELEGATED_AUTH_HEADER)) or extract_bearer_token(
                    request.headers.get("Authorization")
                )
                if not delegated_raw:
                    return JSONResponse(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        headers={"WWW-Authenticate": "Bearer"},
                        content={"detail": "User JWT required for agent endpoints"},
                    )
                try:
                    principal = await verify_user_token(
                        delegated_raw,
                        jwt_enabled=bool(auth.user_jwt_enabled),
                        jwt_issuer=auth.user_jwt_issuer,
                        jwt_audience=auth.user_jwt_audience,
                        jwt_jwks_url=auth.user_jwt_jwks_url,
                        jwt_public_key=auth.user_jwt_public_key,
                        jwt_hs256_secret=auth.user_jwt_hs256_secret,
                        jwt_algorithms=auth.user_jwt_algorithms,
                    )
                    _attach_verified_principal(request, principal)
                except UserTokenError as exc:
                    return JSONResponse(
                        status_code=status.HTTP_403_FORBIDDEN,
                        content={"detail": f"User JWT invalid: {exc}"},
                    )
            return await call_next(request)

        if auth.user_jwt_enabled:
            try:
                principal = await verify_user_token(
                    presented,
                    jwt_enabled=bool(auth.user_jwt_enabled),
                    jwt_issuer=auth.user_jwt_issuer,
                    jwt_audience=auth.user_jwt_audience,
                    jwt_jwks_url=auth.user_jwt_jwks_url,
                    jwt_public_key=auth.user_jwt_public_key,
                    jwt_hs256_secret=auth.user_jwt_hs256_secret,
                    jwt_algorithms=auth.user_jwt_algorithms,
                )
                _attach_verified_principal(request, principal)
                return await call_next(request)
            except UserTokenError:
                pass

        if not expected and not auth.user_jwt_enabled:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"detail": "BFF auth required but no token configured"},
            )

        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={"detail": "Invalid authentication credentials"},
        )


async def enforce_bff_websocket_auth(websocket: WebSocket, token: Optional[str]) -> bool:
    auth = get_settings().auth
    if not auth.is_bff_auth_required(allow_pytest=True, default_required=True):
        return True

    query_token = None
    try:
        query_token = websocket.query_params.get("token")
    except Exception:
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

    agent_token = (auth.bff_agent_token or "").strip()
    if agent_token and hmac.compare_digest(presented, agent_token):
        delegated_raw = extract_bearer_token(websocket.headers.get(_DELEGATED_AUTH_HEADER))
        if auth.user_jwt_enabled:
            if not delegated_raw:
                await websocket.close(code=4401, reason=f"{_DELEGATED_AUTH_HEADER} required for agent calls")
                return False
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
        return True

    expected = auth.bff_expected_token
    if expected and hmac.compare_digest(presented, expected):
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

    if not expected and not auth.user_jwt_enabled:
        await websocket.close(code=1011, reason="BFF auth required but no token configured")
        return False

    logger.warning(
        "WebSocket auth token mismatch (path=%s)",
        websocket.url.path,
    )
    await websocket.close(code=4403, reason="Invalid authentication credentials")
    return False
