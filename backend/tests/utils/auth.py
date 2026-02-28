from __future__ import annotations

import os
from typing import Any, Dict, Iterable, Optional

from jose import jwt

from shared.utils.repo_dotenv import load_repo_dotenv


_DEFAULT_PLATFORM_API_SCOPES = (
    "api:datasets-read",
    "api:datasets-write",
    "api:ontologies-read",
    "api:ontologies-write",
    "api:orchestration-read",
    "api:orchestration-write",
    "api:connectivity-read",
    "api:connectivity-write",
)


def _load_repo_dotenv() -> Dict[str, str]:
    return load_repo_dotenv()


def require_token(env_keys: Iterable[str]) -> str:
    for key in env_keys:
        value = (os.getenv(key) or "").strip()
        if value:
            return value
    dotenv = _load_repo_dotenv()
    for key in env_keys:
        value = (dotenv.get(key) or "").strip()
        if value:
            return value
    raise AssertionError(f"Missing auth token env. Tried: {', '.join(env_keys)}")


def bff_auth_headers() -> Dict[str, str]:
    token = require_token(
        (
            "BFF_ADMIN_TOKEN",
            "BFF_WRITE_TOKEN",
            "SMOKE_ADMIN_TOKEN",
            "ADMIN_TOKEN",
            "ADMIN_API_KEY",
        )
    )
    headers = {"X-Admin-Token": token}
    db_scope = (os.getenv("BFF_DB_SCOPE") or os.getenv("BFF_DB_NAME") or "").strip()
    if db_scope:
        headers["X-DB-Name"] = db_scope
    return headers


def oms_auth_headers() -> Dict[str, str]:
    token = require_token(
        (
            "OMS_ADMIN_TOKEN",
            "OMS_CLIENT_TOKEN",
            "SMOKE_ADMIN_TOKEN",
            "ADMIN_TOKEN",
            "ADMIN_API_KEY",
        )
    )
    return {"X-Admin-Token": token}


def build_smoke_user_jwt(
    *,
    subject: str = "smoke_user",
    roles: Iterable[str] = ("admin",),
    scopes: Iterable[str] = _DEFAULT_PLATFORM_API_SCOPES,
    tenant_id: str = "smoke_tenant",
    org_id: str = "smoke_org",
    email: str = "smoke_user@local.test",
) -> str:
    """
    Build a deterministic HS256 user JWT for integration tests.

    docker-compose default: USER_JWT_HS256_SECRET=spice-dev-user-jwt-secret
    """
    secret = (os.getenv("USER_JWT_HS256_SECRET") or os.getenv("SMOKE_USER_JWT_SECRET") or "spice-dev-user-jwt-secret").strip()
    if not secret:
        secret = "spice-dev-user-jwt-secret"

    issuer: Optional[str] = (os.getenv("USER_JWT_ISSUER") or "").strip() or None
    audience: Optional[str] = (os.getenv("USER_JWT_AUDIENCE") or "").strip() or None

    claims: Dict[str, Any] = {
        "sub": subject,
        "email": email,
        "roles": list(roles),
        "tenant_id": tenant_id,
        "org_id": org_id,
    }
    scopes_list = [str(scope).strip() for scope in (scopes or ()) if str(scope).strip()]
    if scopes_list:
        # OAuth2 standard: space-delimited scope string.
        claims["scope"] = " ".join(dict.fromkeys(scopes_list))
    if issuer:
        claims["iss"] = issuer
    if audience:
        claims["aud"] = audience

    return jwt.encode(claims, secret, algorithm="HS256")


def with_delegated_user(headers: Dict[str, str]) -> Dict[str, str]:
    delegated = build_smoke_user_jwt()
    return {**headers, "X-Delegated-Authorization": f"Bearer {delegated}"}
