from __future__ import annotations

import os
from typing import Iterable, Dict


def require_token(env_keys: Iterable[str]) -> str:
    for key in env_keys:
        value = (os.getenv(key) or "").strip()
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
    return {"X-Admin-Token": token}


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

