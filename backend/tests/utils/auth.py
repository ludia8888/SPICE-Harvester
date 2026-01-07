from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Dict


def _load_repo_dotenv() -> Dict[str, str]:
    try:
        repo_root = Path(__file__).resolve().parents[3]
    except Exception:
        return {}
    env_path = repo_root / ".env"
    if not env_path.exists():
        return {}
    values: Dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            values[key] = value
    return values


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
