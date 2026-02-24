"""
Authentication router — login, token refresh, user info.

Endpoints:
    POST /auth/login   — username/password → access + refresh JWT
    POST /auth/refresh — refresh token → new access JWT
    GET  /auth/me      — current user info (requires valid access token)
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from jose import jwt
from pydantic import BaseModel, Field

from shared.config.settings import get_settings
from shared.security.user_store import UserInfo, get_user_store

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["auth"])


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    admin_token: Optional[str] = None


class RefreshRequest(BaseModel):
    refresh_token: str


class UserMeResponse(BaseModel):
    username: str
    email: str
    roles: List[str]
    tenant_id: str
    org_id: str
    display_name: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _jwt_secret() -> str:
    auth = get_settings().auth
    secret = (auth.user_jwt_hs256_secret or "").strip()
    if not secret:
        secret = "spice-dev-user-jwt-secret"
    return secret


def _build_access_token(user: UserInfo, ttl: int) -> str:
    now = int(time.time())
    auth = get_settings().auth
    claims: Dict[str, Any] = {
        "sub": user.username,
        "email": user.email,
        "roles": list(user.roles),
        "tenant_id": user.tenant_id,
        "org_id": user.org_id,
        "type": "access",
        "iat": now,
        "exp": now + ttl,
    }
    if auth.user_jwt_issuer:
        claims["iss"] = auth.user_jwt_issuer
    if auth.user_jwt_audience:
        claims["aud"] = auth.user_jwt_audience
    return jwt.encode(claims, _jwt_secret(), algorithm="HS256")


def _build_refresh_token(user: UserInfo, ttl: int) -> str:
    now = int(time.time())
    claims: Dict[str, Any] = {
        "sub": user.username,
        "type": "refresh",
        "iat": now,
        "exp": now + ttl,
    }
    return jwt.encode(claims, _jwt_secret(), algorithm="HS256")


def _decode_token(token: str, *, expected_type: str = "access") -> Dict[str, Any]:
    """Decode and validate a JWT. Raises HTTPException on failure."""
    try:
        claims = jwt.decode(
            token,
            _jwt_secret(),
            algorithms=["HS256"],
            options={"verify_aud": False},
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired",
        )
    except jwt.JWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token: {exc}",
        )

    if claims.get("type") != expected_type:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Expected {expected_type} token",
        )
    return claims


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/login", response_model=TokenResponse)
async def login(body: LoginRequest) -> TokenResponse:
    """Authenticate with username/password and receive JWT tokens."""
    auth = get_settings().auth
    if not auth.auth_login_enabled:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Login endpoint is disabled",
        )

    store = get_user_store()
    user = store.authenticate(body.username, body.password)
    if user is None:
        logger.warning("Login failed for user=%s", body.username)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
        )

    access_ttl = auth.auth_access_token_ttl
    refresh_ttl = auth.auth_refresh_token_ttl

    access_token = _build_access_token(user, access_ttl)
    refresh_token = _build_refresh_token(user, refresh_ttl)

    # Return admin token for users with admin role so the frontend can
    # auto-configure X-Admin-Token without a manual Settings step.
    admin_token_value: Optional[str] = None
    if "admin" in (user.roles or []):
        tokens = auth.bff_expected_tokens
        if tokens:
            admin_token_value = tokens[0]

    logger.info("Login successful for user=%s", body.username)
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        token_type="bearer",
        expires_in=access_ttl,
        admin_token=admin_token_value,
    )


@router.post("/refresh", response_model=TokenResponse)
async def refresh(body: RefreshRequest) -> TokenResponse:
    """Exchange a refresh token for a new access token."""
    claims = _decode_token(body.refresh_token, expected_type="refresh")
    username = claims.get("sub", "")

    store = get_user_store()
    user = store.get_user(username)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
        )

    auth = get_settings().auth
    access_ttl = auth.auth_access_token_ttl
    refresh_ttl = auth.auth_refresh_token_ttl

    access_token = _build_access_token(user, access_ttl)
    new_refresh = _build_refresh_token(user, refresh_ttl)

    return TokenResponse(
        access_token=access_token,
        refresh_token=new_refresh,
        token_type="bearer",
        expires_in=access_ttl,
    )


@router.get("/me", response_model=UserMeResponse)
async def me(request: Request) -> UserMeResponse:
    """Return current authenticated user info from the request principal."""
    # The auth middleware populates request.state.user when a valid Bearer
    # token is present.  Fallback: decode the Authorization header ourselves.
    principal = getattr(request.state, "user", None)
    if principal is not None:
        return UserMeResponse(
            username=principal.id,
            email=principal.email or "",
            roles=list(principal.roles),
            tenant_id=principal.tenant_id or "default",
            org_id=principal.org_id or "default",
            display_name=principal.email or principal.id,
        )

    # Manual fallback: extract Bearer token from header
    auth_header = request.headers.get("authorization", "")
    if not auth_header.lower().startswith("bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )

    token = auth_header[7:].strip()
    claims = _decode_token(token, expected_type="access")

    return UserMeResponse(
        username=claims.get("sub", ""),
        email=claims.get("email", ""),
        roles=claims.get("roles", []),
        tenant_id=claims.get("tenant_id", "default"),
        org_id=claims.get("org_id", "default"),
        display_name=claims.get("email", claims.get("sub", "")),
    )
