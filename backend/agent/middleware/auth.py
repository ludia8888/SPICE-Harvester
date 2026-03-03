from __future__ import annotations

import hmac
import os

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from shared.config.settings import get_settings
from shared.security.auth_utils import extract_presented_token, is_exempt_path

_EXEMPT_PATHS_DEFAULT = ("/health", "/", "/metrics")


def _parse_boolish(value: str | None) -> bool | None:
    normalized = str(value or "").strip().lower()
    if normalized in {"true", "1", "yes", "on"}:
        return True
    if normalized in {"false", "0", "no", "off"}:
        return False
    return None


def _agent_auth_required() -> bool:
    settings = get_settings()
    override = _parse_boolish(os.getenv("AGENT_REQUIRE_AUTH"))
    if override is not None:
        return override
    if settings.auth.allow_insecure_auth_disable:
        return False
    return True


def _resolve_agent_expected_tokens() -> tuple[str, ...]:
    settings = get_settings()
    auth = settings.auth
    candidates = list(auth.bff_agent_tokens)
    if settings.agent.bff_token:
        candidates.extend(part.strip() for part in str(settings.agent.bff_token).split(",") if part.strip())
    candidates.extend(auth.bff_expected_tokens)
    deduped: list[str] = []
    seen: set[str] = set()
    for token in candidates:
        cleaned = str(token or "").strip()
        if not cleaned or cleaned in seen:
            continue
        deduped.append(cleaned)
        seen.add(cleaned)
    return tuple(deduped)


def _resolve_agent_exempt_paths() -> set[str]:
    raw = (os.getenv("AGENT_AUTH_EXEMPT_PATHS") or "").strip()
    if not raw:
        return set(_EXEMPT_PATHS_DEFAULT)
    paths = {part.strip() for part in raw.split(",") if part.strip()}
    return paths or set(_EXEMPT_PATHS_DEFAULT)


def ensure_agent_auth_configured() -> None:
    settings = get_settings()
    require_auth = _agent_auth_required()
    if not require_auth and not settings.auth.allow_insecure_auth_disable:
        raise RuntimeError(
            "Agent auth explicitly disabled without approval. "
            "Set ALLOW_INSECURE_AUTH_DISABLE=true to disable agent auth."
        )
    if not require_auth:
        return
    if not _resolve_agent_expected_tokens():
        raise RuntimeError(
            "Agent auth is required but no token is configured. "
            "Set BFF_AGENT_TOKEN (or AGENT_BFF_TOKEN/ADMIN_API_KEY/ADMIN_TOKEN)."
        )


def install_agent_auth_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def _agent_auth_middleware(request: Request, call_next):
        if not _agent_auth_required():
            return await call_next(request)

        exempt_paths = _resolve_agent_exempt_paths()
        if is_exempt_path(request.url.path, exempt_paths=exempt_paths):
            return await call_next(request)

        expected_tokens = _resolve_agent_expected_tokens()
        if not expected_tokens:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"detail": "Agent auth required but no token configured"},
            )

        presented = extract_presented_token(request.headers)
        if not presented:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                headers={"WWW-Authenticate": "Bearer"},
                content={"detail": "Authentication required"},
            )

        if not any(hmac.compare_digest(presented, expected) for expected in expected_tokens):
            return JSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={"detail": "Invalid authentication credentials"},
            )

        return await call_next(request)
