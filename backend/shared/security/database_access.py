from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
import logging
from typing import Any, DefaultDict, Dict, Iterable, List, Mapping, Optional, Tuple

import asyncpg

from shared.config.settings import get_settings
from shared.errors.infra_errors import RegistryUnavailableError

logger = logging.getLogger(__name__)

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

_DATABASE_ACCESS_UNAVAILABLE_ERRORS = (
    OSError,
    asyncpg.PostgresConnectionError,
    asyncpg.TooManyConnectionsError,
)


class DatabaseAccessState(str, Enum):
    CONFIGURED = "configured"
    UNCONFIGURED = "unconfigured"
    UNAVAILABLE = "unavailable"


class DatabaseAccessRegistryUnavailableError(RegistryUnavailableError):
    """Raised when the database-access registry cannot be reached."""


@dataclass(frozen=True)
class DatabaseAccessInspection:
    state: DatabaseAccessState
    role: Optional[str] = None

    @property
    def is_configured(self) -> bool:
        return self.state == DatabaseAccessState.CONFIGURED

    @property
    def is_unconfigured(self) -> bool:
        return self.state == DatabaseAccessState.UNCONFIGURED

    @property
    def is_unavailable(self) -> bool:
        return self.state == DatabaseAccessState.UNAVAILABLE


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


async def fetch_database_access_entries(*, db_names: Iterable[str]) -> Dict[str, List[Dict[str, Any]]]:
    names = [str(name).strip() for name in db_names or [] if str(name or "").strip()]
    if not names:
        return {}

    conn = await asyncpg.connect(get_settings().database.postgres_url)
    try:
        try:
            rows = await conn.fetch(
                """
                SELECT db_name, principal_type, principal_id, principal_name, role, created_at
                FROM database_access
                WHERE db_name = ANY($1)
                """,
                names,
            )
        except asyncpg.UndefinedTableError:
            # Treat missing table as "unconfigured" (migrations not applied yet).
            return {}
    finally:
        await conn.close()

    grouped: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows or []:
        raw_role = row["role"]
        normalized_role = normalize_database_role(raw_role) or str(raw_role)
        grouped[str(row["db_name"])].append(
            {
                "principal_type": str(row["principal_type"]),
                "principal_id": str(row["principal_id"]),
                "principal_name": str(row["principal_name"]) if row["principal_name"] else None,
                "role": normalized_role,
                "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
            }
        )
    return dict(grouped)


async def list_database_names() -> List[str]:
    conn = await asyncpg.connect(get_settings().database.postgres_url)
    try:
        try:
            rows = await conn.fetch(
                """
                SELECT DISTINCT db_name
                FROM database_access
                ORDER BY db_name ASC
                """
            )
        except asyncpg.UndefinedTableError:
            return []
    finally:
        await conn.close()

    names: List[str] = []
    for row in rows or []:
        raw = str(row["db_name"]).strip()
        if raw:
            names.append(raw)
    return names


async def upsert_database_access_entry(
    *,
    db_name: str,
    principal_type: str,
    principal_id: str,
    principal_name: str,
    role: str,
) -> None:
    if not principal_id:
        return
    normalized_role = normalize_database_role(role) or role

    conn = await asyncpg.connect(get_settings().database.postgres_url)
    try:
        try:
            await conn.execute(
                """
                INSERT INTO database_access (
                    db_name, principal_type, principal_id, principal_name, role, created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
                ON CONFLICT (db_name, principal_type, principal_id)
                DO UPDATE SET
                    principal_name = EXCLUDED.principal_name,
                    role = EXCLUDED.role,
                    updated_at = NOW()
                """,
                db_name,
                principal_type,
                principal_id,
                principal_name,
                normalized_role,
            )
        except asyncpg.UndefinedTableError:
            # Local/dev stacks may start without migrations. Self-heal on first write.
            await ensure_database_access_table(conn)
            await conn.execute(
                """
                INSERT INTO database_access (
                    db_name, principal_type, principal_id, principal_name, role, created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
                ON CONFLICT (db_name, principal_type, principal_id)
                DO UPDATE SET
                    principal_name = EXCLUDED.principal_name,
                    role = EXCLUDED.role,
                    updated_at = NOW()
                """,
                db_name,
                principal_type,
                principal_id,
                principal_name,
                normalized_role,
            )
    finally:
        await conn.close()


async def upsert_database_owner(
    *,
    db_name: str,
    principal_type: str,
    principal_id: str,
    principal_name: str,
) -> None:
    await upsert_database_access_entry(
        db_name=db_name,
        principal_type=principal_type,
        principal_id=principal_id,
        principal_name=principal_name,
        role="Owner",
    )


def resolve_database_actor_with_name(headers: Mapping[str, str]) -> Tuple[str, str, str]:
    principal_type, principal_id = resolve_database_actor(headers)
    principal_name = (headers.get("X-User-Name") or "").strip() or principal_id
    return principal_type, principal_id, principal_name


async def ensure_database_access_table(conn: asyncpg.Connection) -> None:
    # Prefer migrations in production (`backend/database/migrations/008_database_access.sql`).
    # This helper remains for local/dev/test bootstrapping.
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
        """
        DO $$
        BEGIN
            ALTER TABLE database_access
                ADD CONSTRAINT database_access_role_check
                CHECK (role IN ('Owner', 'Editor', 'Viewer', 'DomainModeler', 'DataEngineer', 'Security'));
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END $$;
        """
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_database_access_db_name ON database_access(db_name)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_database_access_principal ON database_access(principal_type, principal_id)"
    )


async def _connect_database_access_registry(*, operation: str) -> asyncpg.Connection | None:
    try:
        return await asyncpg.connect(get_settings().database.postgres_url)
    except _DATABASE_ACCESS_UNAVAILABLE_ERRORS as exc:
        logger.warning("Database access registry unavailable during %s: %s", operation, exc)
        return None


async def inspect_database_access(
    *,
    db_name: str,
    principal_type: Optional[str] = None,
    principal_id: Optional[str] = None,
) -> DatabaseAccessInspection:
    if (principal_type is None) != (principal_id is None):
        raise ValueError("principal_type and principal_id must be provided together")

    conn = await _connect_database_access_registry(operation="inspection")
    if conn is None:
        return DatabaseAccessInspection(state=DatabaseAccessState.UNAVAILABLE)

    try:
        try:
            if principal_type is not None and principal_id is not None:
                row = await conn.fetchrow(
                    """
                    SELECT
                        EXISTS(
                            SELECT 1
                            FROM database_access
                            WHERE db_name = $1
                        ) AS configured,
                        (
                            SELECT role
                            FROM database_access
                            WHERE db_name = $1 AND principal_type = $2 AND principal_id = $3
                            LIMIT 1
                        ) AS role
                    """,
                    db_name,
                    principal_type,
                    principal_id,
                )
                configured = bool(row["configured"]) if row else False
                role = normalize_database_role(row["role"]) if row and row["role"] else None
                return DatabaseAccessInspection(
                    state=DatabaseAccessState.CONFIGURED if configured else DatabaseAccessState.UNCONFIGURED,
                    role=role,
                )

            row = await conn.fetchrow(
                """
                SELECT EXISTS(
                    SELECT 1
                    FROM database_access
                    WHERE db_name = $1
                ) AS configured
                """,
                db_name,
            )
            configured = bool(row["configured"]) if row else False
            return DatabaseAccessInspection(
                state=DatabaseAccessState.CONFIGURED if configured else DatabaseAccessState.UNCONFIGURED
            )
        except asyncpg.UndefinedTableError:
            return DatabaseAccessInspection(state=DatabaseAccessState.UNCONFIGURED)
    finally:
        await conn.close()


async def get_database_access_role(
    *,
    db_name: str,
    principal_type: str,
    principal_id: str,
) -> Optional[str]:
    inspection = await inspect_database_access(
        db_name=db_name,
        principal_type=principal_type,
        principal_id=principal_id,
    )
    return inspection.role


async def has_database_access_config(*, db_name: str) -> bool:
    inspection = await inspect_database_access(db_name=db_name)
    return inspection.is_configured


async def delete_database_access_entries(*, db_name: str) -> None:
    conn = await asyncpg.connect(get_settings().database.postgres_url)
    try:
        try:
            await conn.execute(
                "DELETE FROM database_access WHERE db_name = $1",
                db_name,
            )
        except asyncpg.UndefinedTableError:
            return
    finally:
        await conn.close()


async def enforce_database_role(
    *,
    headers: Mapping[str, str],
    db_name: str,
    required_roles: Iterable[str],
    allow_if_unconfigured: bool = True,
    allow_if_registry_unavailable: bool = False,
    require_env_key: str = "BFF_REQUIRE_DB_ACCESS",
) -> None:
    required_set = {normalize_database_role(role) for role in required_roles if role}
    required_set = {role for role in required_set if role}
    if not required_set:
        return

    enforce_flag: Optional[bool] = None
    if require_env_key == "BFF_REQUIRE_DB_ACCESS":
        enforce_flag = get_settings().auth.bff_require_db_access
    principal_type, principal_id = resolve_database_actor(headers)
    if not principal_id:
        raise ValueError("Permission denied")

    inspection = await inspect_database_access(
        db_name=db_name,
        principal_type=principal_type,
        principal_id=principal_id,
    )
    role = inspection.role
    if role is None:
        if enforce_flag is False:
            return
        if inspection.is_unconfigured and allow_if_unconfigured and enforce_flag is not True:
            return
        if inspection.is_unavailable:
            if allow_if_registry_unavailable and enforce_flag is not True:
                return
            raise DatabaseAccessRegistryUnavailableError("Database access registry unavailable")
        raise ValueError("Permission denied")

    if role not in required_set:
        raise ValueError("Permission denied")
