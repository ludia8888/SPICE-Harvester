from __future__ import annotations

from typing import Iterable, Sequence

import asyncpg

from shared.config.settings import get_settings


class RuntimeDDLDisabledError(RuntimeError):
    """Raised when runtime DDL bootstrap is disabled for a missing schema object."""


def allow_runtime_ddl_bootstrap() -> bool:
    return bool(get_settings().allow_runtime_ddl_bootstrap)


async def schema_exists(conn: asyncpg.Connection, schema: str) -> bool:
    return bool(
        await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.schemata
                WHERE schema_name = $1
            )
            """,
            schema,
        )
    )


async def relation_exists(
    conn: asyncpg.Connection,
    relation_name: str,
    *,
    schema: str | None = None,
) -> bool:
    identifier = f"{schema}.{relation_name}" if schema else relation_name
    return bool(await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", identifier))


async def find_missing_schema_objects(
    conn: asyncpg.Connection,
    *,
    schema: str | None = None,
    required_relations: Sequence[str] = (),
) -> list[str]:
    missing: list[str] = []
    if schema and not await schema_exists(conn, schema):
        missing.append(f"schema:{schema}")
    for relation_name in required_relations:
        if not await relation_exists(conn, relation_name, schema=schema):
            qualified = f"{schema}.{relation_name}" if schema else relation_name
            missing.append(f"relation:{qualified}")
    return missing


def format_missing_schema_objects(
    owner: str,
    *,
    missing: Iterable[str],
    bootstrap_allowed: bool,
) -> str:
    details = ", ".join(str(item) for item in missing) or "unknown schema objects"
    suffix = (
        "Enable ALLOW_RUNTIME_DDL_BOOTSTRAP=true for dev/test bootstrap."
        if not bootstrap_allowed
        else "Runtime bootstrap fallback will be used."
    )
    return f"{owner} is missing required schema objects: {details}. {suffix}"
