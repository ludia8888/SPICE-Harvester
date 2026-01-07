from __future__ import annotations

import os
from typing import Iterable, Mapping, Optional, Tuple

import asyncpg

from shared.config.service_config import ServiceConfig
from shared.utils.env_utils import parse_bool

DATABASE_ACCESS_ROLES = (
    "Owner",
    "Editor",
    "Viewer",
    "DomainModeler",
    "DataEngineer",
    "Security",
)

DOMAIN_MODEL_ROLES = {"Owner", "Editor", "DomainModeler"}
DATA_ENGINEER_ROLES = {"Owner", "Editor", "DataEngineer"}
SECURITY_ROLES = {"Owner", "Security"}
READ_ROLES = set(DATABASE_ACCESS_ROLES)

_ROLE_ALIASES = {
    "owner": "Owner",
    "editor": "Editor",
    "viewer": "Viewer",
    "domainmodeler": "DomainModeler",
    "domain_modeler": "DomainModeler",
    "domain": "DomainModeler",
    "dataengineer": "DataEngineer",
    "data_engineer": "DataEngineer",
    "data": "DataEngineer",
    "security": "Security",
    "governance": "Security",
}


def resolve_database_actor(headers: Mapping[str, str]) -> Tuple[str, str]:
    principal_type = (headers.get("X-User-Type") or "user").strip().lower() or "user"
    principal_id = (
        headers.get("X-User-ID")
        or headers.get("X-User")
        or headers.get("X-User-Id")
        or ""
    ).strip() or "system"
    return principal_type, principal_id


def normalize_database_role(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    normalized = _ROLE_ALIASES.get(raw.lower())
    return normalized or raw


async def ensure_database_access_table(conn: asyncpg.Connection) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS database_access (
            db_name TEXT NOT NULL,
            principal_type TEXT NOT NULL,
            principal_id TEXT NOT NULL,
            principal_name TEXT,
            role TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (db_name, principal_type, principal_id)
        )
        """
    )
    await conn.execute(
        "ALTER TABLE database_access DROP CONSTRAINT IF EXISTS database_access_role_check"
    )
    await conn.execute(
        """
        ALTER TABLE database_access
            ADD CONSTRAINT database_access_role_check
            CHECK (role IN ('Owner', 'Editor', 'Viewer', 'DomainModeler', 'DataEngineer', 'Security'))
        """
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_database_access_db_name ON database_access(db_name)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_database_access_principal ON database_access(principal_type, principal_id)"
    )


async def get_database_access_role(
    *,
    db_name: str,
    principal_type: str,
    principal_id: str,
) -> Optional[str]:
    conn = await asyncpg.connect(ServiceConfig.get_postgres_url())
    try:
        await ensure_database_access_table(conn)
        row = await conn.fetchrow(
            """
            SELECT role
            FROM database_access
            WHERE db_name = $1 AND principal_type = $2 AND principal_id = $3
            """,
            db_name,
            principal_type,
            principal_id,
        )
    finally:
        await conn.close()

    if not row:
        return None
    return normalize_database_role(row["role"])


async def has_database_access_config(*, db_name: str) -> bool:
    conn = await asyncpg.connect(ServiceConfig.get_postgres_url())
    try:
        await ensure_database_access_table(conn)
        row = await conn.fetchrow(
            "SELECT 1 FROM database_access WHERE db_name = $1 LIMIT 1",
            db_name,
        )
    finally:
        await conn.close()
    return bool(row)


async def enforce_database_role(
    *,
    headers: Mapping[str, str],
    db_name: str,
    required_roles: Iterable[str],
    allow_if_unconfigured: bool = True,
    require_env_key: str = "BFF_REQUIRE_DB_ACCESS",
) -> None:
    required_set = {normalize_database_role(role) for role in required_roles if role}
    required_set = {role for role in required_set if role}
    if not required_set:
        return

    enforce_flag = parse_bool(os.getenv(require_env_key, ""))
    principal_type, principal_id = resolve_database_actor(headers)
    if not principal_id:
        raise ValueError("Permission denied")

    role = await get_database_access_role(
        db_name=db_name,
        principal_type=principal_type,
        principal_id=principal_id,
    )
    if role is None:
        if enforce_flag is False:
            return
        if allow_if_unconfigured and enforce_flag is not True:
            if not await has_database_access_config(db_name=db_name):
                return
        raise ValueError("Permission denied")

    if role not in required_set:
        raise ValueError("Permission denied")
