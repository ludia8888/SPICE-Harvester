"""
Simple user store for local/dev authentication.

Phase 1: environment-variable–based user list (``AUTH_USERS`` JSON).
When ``AUTH_USERS`` is not set a single dev user ``admin / admin`` is
available so that local development works out-of-the-box.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

from shared.config.settings import get_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# User info
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class UserInfo:
    """Authenticated user record."""
    username: str
    email: str = ""
    roles: tuple[str, ...] = ("user",)
    tenant_id: str = "default"
    org_id: str = "default"
    display_name: str = ""


# ---------------------------------------------------------------------------
# Default dev users
# ---------------------------------------------------------------------------

_DEFAULT_DEV_USERS: Dict[str, Dict[str, Any]] = {
    "admin": {
        "password": "admin",
        "email": "admin@local.dev",
        "roles": ["admin"],
        "tenant_id": "dev",
        "org_id": "dev",
        "display_name": "Admin (Dev)",
    },
}


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------

class UserStore:
    """
    In-memory user store backed by ``AUTH_USERS`` env-var (JSON string).

    Expected JSON format::

        {
          "alice": {
            "password": "hashed-or-plain",
            "email": "alice@example.com",
            "roles": ["admin"],
            "tenant_id": "t1",
            "org_id": "o1"
          }
        }

    When ``password`` starts with ``sha256:`` it is compared as
    ``sha256(attempt)`` hex-digest; otherwise plain-text comparison is used
    (suitable for dev environments only).
    """

    def __init__(self, *, auth_users_json: Optional[str] = None) -> None:
        self._users: Dict[str, Dict[str, Any]] = {}
        self._auth_users_json = str(auth_users_json or "").strip()
        self._load()

    # ------------------------------------------------------------------
    def _load(self) -> None:
        raw = self._auth_users_json
        if not raw:
            raw = str(get_settings().auth.auth_users or "").strip()
        if raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    self._users = parsed
                    logger.info("UserStore: loaded %d user(s) from AUTH_USERS", len(self._users))
                    return
            except json.JSONDecodeError:
                logger.warning("UserStore: AUTH_USERS is not valid JSON – falling back to defaults")

        # fallback: dev defaults
        self._users = dict(_DEFAULT_DEV_USERS)
        logger.info("UserStore: using default dev users (%d)", len(self._users))

    # ------------------------------------------------------------------
    def authenticate(self, username: str, password: str) -> Optional[UserInfo]:
        """Return *UserInfo* if credentials match, else ``None``."""
        entry = self._users.get(username)
        if entry is None:
            return None

        stored_pw = str(entry.get("password", ""))
        if not _verify_password(password, stored_pw):
            return None

        return UserInfo(
            username=username,
            email=str(entry.get("email", f"{username}@local.dev")),
            roles=tuple(entry.get("roles", ["user"])),
            tenant_id=str(entry.get("tenant_id", "default")),
            org_id=str(entry.get("org_id", "default")),
            display_name=str(entry.get("display_name", username)),
        )

    # ------------------------------------------------------------------
    def get_user(self, username: str) -> Optional[UserInfo]:
        """Look up a user without password verification."""
        entry = self._users.get(username)
        if entry is None:
            return None
        return UserInfo(
            username=username,
            email=str(entry.get("email", f"{username}@local.dev")),
            roles=tuple(entry.get("roles", ["user"])),
            tenant_id=str(entry.get("tenant_id", "default")),
            org_id=str(entry.get("org_id", "default")),
            display_name=str(entry.get("display_name", username)),
        )


# ---------------------------------------------------------------------------
# Password helpers
# ---------------------------------------------------------------------------

def _verify_password(attempt: str, stored: str) -> bool:
    """
    Compare *attempt* against *stored*.

    * ``sha256:<hex>`` → SHA-256 comparison (constant-time)
    * anything else    → plain-text comparison (dev only)
    """
    if stored.startswith("sha256:"):
        expected_hex = stored[7:]
        attempt_hex = hashlib.sha256(attempt.encode()).hexdigest()
        return hmac.compare_digest(attempt_hex, expected_hex)

    # plain-text (dev mode)
    return hmac.compare_digest(attempt, stored)


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_store: Optional[UserStore] = None


def get_user_store() -> UserStore:
    """Return the global singleton ``UserStore``."""
    global _store
    raw_auth_users = str(get_settings().auth.auth_users or "").strip()
    if _store is None or getattr(_store, "_auth_users_json", "") != raw_auth_users:
        _store = UserStore(auth_users_json=raw_auth_users)
    return _store
