from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Sequence, Tuple

import httpx
from jose import jwt
from jose.exceptions import ExpiredSignatureError, JWTError


class UserTokenError(RuntimeError):
    """Raised when a user/delegated JWT cannot be verified."""


@dataclass(frozen=True)
class UserPrincipal:
    """
    Verified (or explicitly unverified) principal extracted from an auth token.

    This is attached to `request.state.user` where available, so middleware and
    business logic can avoid spoofable headers like X-User-ID.
    """

    id: str
    type: str = "user"
    email: Optional[str] = None
    roles: Tuple[str, ...] = ()
    tenant_id: Optional[str] = None
    org_id: Optional[str] = None
    verified: bool = True
    claims: Dict[str, Any] = field(default_factory=dict)

    def scopes(self) -> Tuple[str, ...]:
        return _claim_scopes(self.claims, ("scope", "scp"))


_DEFAULT_JWT_ALGORITHMS = ("RS256", "HS256")
_JWKS_CACHE_TTL_S = 300
_jwks_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}


def extract_bearer_token(value: Optional[str]) -> Optional[str]:
    raw = (value or "").strip()
    if not raw:
        return None
    if raw.lower().startswith("bearer "):
        raw = raw[7:].strip()
    return raw or None


def _parse_algorithms(raw: Optional[str]) -> Tuple[str, ...]:
    value = (raw or "").strip()
    if not value:
        return _DEFAULT_JWT_ALGORITHMS
    algos = [item.strip() for item in value.split(",") if item.strip()]
    return tuple(algos) or _DEFAULT_JWT_ALGORITHMS


def _claim_str(claims: Dict[str, Any], keys: Sequence[str]) -> Optional[str]:
    for key in keys:
        raw = claims.get(key)
        if raw is None:
            continue
        value = str(raw).strip()
        if value:
            return value
    return None


def _claim_list_str(claims: Dict[str, Any], keys: Sequence[str]) -> Tuple[str, ...]:
    for key in keys:
        raw = claims.get(key)
        if raw is None:
            continue
        if isinstance(raw, str):
            value = raw.strip()
            if not value:
                continue
            return tuple(item.strip() for item in value.split(",") if item.strip())
        if isinstance(raw, (list, tuple, set)):
            values = [str(item).strip() for item in raw if str(item).strip()]
            if values:
                return tuple(values)
    return ()


def _claim_scopes(claims: Dict[str, Any], keys: Sequence[str]) -> Tuple[str, ...]:
    """Parse OAuth scopes from OIDC claims.

    Supports:
    - `scope`: space-delimited string (OAuth2 standard)
    - `scp`: string or array (commonly used by Azure AD)
    """

    def _split(raw: str) -> list[str]:
        value = str(raw or "").strip()
        if not value:
            return []
        # Prefer whitespace (OAuth2), but accept commas as well.
        tokens: list[str] = []
        for part in value.replace(",", " ").split():
            token = part.strip()
            if token:
                tokens.append(token)
        return tokens

    seen: set[str] = set()
    ordered: list[str] = []

    for key in keys:
        raw = claims.get(key)
        if raw is None:
            continue
        candidates: list[str] = []
        if isinstance(raw, str):
            candidates = _split(raw)
        elif isinstance(raw, (list, tuple, set)):
            for item in raw:
                if item is None:
                    continue
                if isinstance(item, str):
                    candidates.extend(_split(item))
                else:
                    token = str(item).strip()
                    if token:
                        candidates.append(token)
        else:
            token = str(raw).strip()
            if token:
                candidates = _split(token) or [token]

        for token in candidates:
            if token in seen:
                continue
            seen.add(token)
            ordered.append(token)

        if ordered:
            break

    return tuple(ordered)


async def _fetch_jwks(url: str) -> Dict[str, Any]:
    now = time.monotonic()
    cached = _jwks_cache.get(url)
    if cached and cached[0] > now:
        return cached[1]

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(url)
    resp.raise_for_status()
    jwks = resp.json()
    if not isinstance(jwks, dict) or not isinstance(jwks.get("keys"), list):
        raise UserTokenError("JWKS response invalid")

    _jwks_cache[url] = (now + _JWKS_CACHE_TTL_S, jwks)
    return jwks


def _select_jwk(jwks: Dict[str, Any], kid: Optional[str]) -> Dict[str, Any]:
    keys = jwks.get("keys")
    if not isinstance(keys, list):
        raise UserTokenError("JWKS keys missing")

    if kid:
        for key in keys:
            if isinstance(key, dict) and key.get("kid") == kid:
                return key
        raise UserTokenError("JWT kid not found in JWKS")

    if len(keys) == 1 and isinstance(keys[0], dict):
        return keys[0]

    raise UserTokenError("JWT kid missing and JWKS has multiple keys")


def _decode_user_claims(
    token: str,
    *,
    key: Any,
    algorithms: Tuple[str, ...],
    issuer: Optional[str],
    audience: Optional[str],
) -> Dict[str, Any]:
    options = {
        "verify_aud": bool(audience),
        "verify_iss": bool(issuer),
    }
    return jwt.decode(
        token,
        key,
        algorithms=list(algorithms),
        issuer=issuer,
        audience=audience,
        options=options,
    )


async def verify_user_token(
    token: str,
    *,
    jwt_enabled: bool,
    jwt_issuer: Optional[str],
    jwt_audience: Optional[str],
    jwt_jwks_url: Optional[str],
    jwt_public_key: Optional[str],
    jwt_hs256_secret: Optional[str],
    jwt_algorithms: Optional[str],
) -> UserPrincipal:
    """
    Verify a user JWT and return a structured principal.

    The verification source is selected in order:
    1) HS256 secret (USER_JWT_HS256_SECRET)
    2) PEM public key (USER_JWT_PUBLIC_KEY)
    3) JWKS URL (USER_JWT_JWKS_URL)
    """
    raw = (token or "").strip()
    if not raw:
        raise UserTokenError("User token missing")

    if not jwt_enabled:
        raise UserTokenError("User JWT auth is disabled")

    algorithms = _parse_algorithms(jwt_algorithms)
    issuer = (jwt_issuer or "").strip() or None
    audience = (jwt_audience or "").strip() or None

    try:
        header = jwt.get_unverified_header(raw)
    except JWTError as exc:
        raise UserTokenError("JWT header invalid") from exc

    key: Any = None
    hs256_secrets: Tuple[str, ...] = ()
    if jwt_hs256_secret:
        parts = [part.strip() for part in str(jwt_hs256_secret).split(",") if part.strip()]
        if parts:
            hs256_secrets = tuple(parts)
    elif jwt_public_key:
        key = jwt_public_key
    elif jwt_jwks_url:
        jwks = await _fetch_jwks(jwt_jwks_url.strip())
        jwk_key = _select_jwk(jwks, kid=str(header.get("kid") or "").strip() or None)
        key = jwk_key
    else:
        raise UserTokenError("No JWT verification key configured")

    try:
        if hs256_secrets:
            last_exc: Optional[Exception] = None
            claims = None
            for secret in hs256_secrets:
                try:
                    claims = _decode_user_claims(
                        raw,
                        key=secret,
                        algorithms=algorithms,
                        issuer=issuer,
                        audience=audience,
                    )
                    last_exc = None
                    break
                except ExpiredSignatureError as exc:
                    raise UserTokenError("JWT expired") from exc
                except JWTError as exc:
                    last_exc = exc
                    continue
            if claims is None:
                raise UserTokenError("JWT verification failed") from last_exc
        else:
            claims = _decode_user_claims(
                raw,
                key=key,
                algorithms=algorithms,
                issuer=issuer,
                audience=audience,
            )
    except ExpiredSignatureError as exc:
        raise UserTokenError("JWT expired") from exc
    except JWTError as exc:
        raise UserTokenError("JWT verification failed") from exc

    if not isinstance(claims, dict):
        raise UserTokenError("JWT claims invalid")

    user_id = _claim_str(claims, ("sub", "user_id", "uid", "id", "principal_id", "principal"))
    if not user_id:
        raise UserTokenError("JWT missing user id claim")

    email = _claim_str(claims, ("email", "upn", "preferred_username"))
    tenant_id = _claim_str(claims, ("tenant_id", "tenant", "tid"))
    org_id = _claim_str(claims, ("org_id", "org", "organization_id", "organization"))
    roles = _claim_list_str(claims, ("roles", "role", "groups"))

    return UserPrincipal(
        id=user_id,
        type=str(_claim_str(claims, ("typ", "user_type", "principal_type")) or "user"),
        email=email,
        roles=roles,
        tenant_id=tenant_id,
        org_id=org_id,
        verified=True,
        claims=claims,
    )
