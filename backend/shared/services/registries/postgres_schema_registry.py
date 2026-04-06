"""Postgres registry base (Template Method).

Several registries share the same lifecycle:
- create an asyncpg pool
- verify required schema objects exist
- optionally bootstrap schema/tables in dev/test only

This base class centralizes that boilerplate while delegating table creation
to concrete registries via the Template Method hook `_ensure_tables(...)`.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional

import asyncpg

from shared.config.settings import get_settings
from shared.services.registries.runtime_ddl import (
    RuntimeDDLDisabledError,
    find_missing_schema_objects,
    format_missing_schema_objects,
)


logger = logging.getLogger(__name__)


class MissingSchemaObjectsError(RuntimeDDLDisabledError):
    """Raised when required registry schema objects are missing."""


class PostgresSchemaRegistry(ABC):
    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_agent",
        pool_min: Optional[int] = None,
        pool_max: Optional[int] = None,
        command_timeout: Optional[int] = None,
        allow_runtime_ddl_bootstrap: Optional[bool] = None,
    ) -> None:
        settings = get_settings()
        self._dsn = dsn or settings.database.postgres_url
        self._schema = schema
        self._pool: Optional[asyncpg.Pool] = None
        self._pool_min = int(pool_min or 1)
        self._pool_max = int(pool_max or 5)
        self._command_timeout = int(command_timeout) if command_timeout is not None else 30
        self._allow_runtime_ddl_bootstrap = (
            settings.allow_runtime_ddl_bootstrap
            if allow_runtime_ddl_bootstrap is None
            else bool(allow_runtime_ddl_bootstrap)
        )
        # Guard against startup races when multiple services ensure the same schema.
        # We use non-blocking advisory lock polling to avoid command_timeout failures.
        self._schema_lock_wait_seconds = max(60.0, float(self._command_timeout) * 4.0)
        self._schema_lock_poll_interval_seconds = 0.2

    async def initialize(self) -> None:
        await self.connect()

    async def connect(self) -> None:
        if self._pool:
            return
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=self._pool_min,
            max_size=self._pool_max,
            command_timeout=self._command_timeout,
        )
        await self.ensure_schema()

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def shutdown(self) -> None:
        await self.close()

    async def health_check(self) -> bool:
        try:
            if not self._pool:
                await self.connect()
            async with self._pool.acquire() as conn:  # type: ignore[union-attr]
                await conn.execute("SELECT 1")
            return True
        except Exception:
            logger.warning("Exception fallback at shared/services/registries/postgres_schema_registry.py:68", exc_info=True)
            return False

    async def ensure_schema(self) -> None:
        if not self._pool:
            raise RuntimeError(f"{self.__class__.__name__} not connected")

        async with self._pool.acquire() as conn:
            lock_key = f"{self.__class__.__name__}:{self._schema}"
            lock_acquired = False
            try:
                lock_acquired = await self._acquire_schema_lock(conn, lock_key)
                missing_objects = await self._missing_schema_objects(conn)
                if missing_objects is None:
                    if not self._allow_runtime_ddl_bootstrap:
                        raise MissingSchemaObjectsError(
                            f"{self.__class__.__name__} does not define required schema objects and "
                            "ALLOW_RUNTIME_DDL_BOOTSTRAP is disabled"
                        )
                    logger.warning(
                        "Runtime DDL bootstrap fallback for %s because schema metadata is not declared",
                        self.__class__.__name__,
                    )
                    await self._bootstrap_schema(conn)
                elif missing_objects:
                    if not self._allow_runtime_ddl_bootstrap:
                        raise MissingSchemaObjectsError(
                            format_missing_schema_objects(
                                self.__class__.__name__,
                                missing=missing_objects,
                                bootstrap_allowed=False,
                            )
                        )
                    logger.warning(
                        format_missing_schema_objects(
                            self.__class__.__name__,
                            missing=missing_objects,
                            bootstrap_allowed=True,
                        )
                    )
                    await self._bootstrap_schema(conn)
            finally:
                if lock_acquired:
                    await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", lock_key)

    async def _acquire_schema_lock(self, conn: asyncpg.Connection, lock_key: str) -> bool:
        deadline = asyncio.get_running_loop().time() + self._schema_lock_wait_seconds
        while True:
            acquired = await conn.fetchval("SELECT pg_try_advisory_lock(hashtext($1))", lock_key)
            if acquired:
                return True
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(
                    f"Timed out waiting for schema advisory lock: {lock_key} "
                    f"(waited {self._schema_lock_wait_seconds:.1f}s)"
                )
            await asyncio.sleep(self._schema_lock_poll_interval_seconds)

    async def _missing_schema_objects(self, conn: asyncpg.Connection) -> list[str] | None:
        required_tables = tuple(self._required_tables())
        if not required_tables:
            return None
        return await find_missing_schema_objects(
            conn,
            schema=self._schema,
            required_relations=required_tables,
        )

    async def _bootstrap_schema(self, conn: asyncpg.Connection) -> None:
        await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
        await self._ensure_tables(conn)

    def _required_tables(self) -> tuple[str, ...]:
        value = getattr(self, "_REQUIRED_TABLES", ())
        if isinstance(value, tuple):
            return value
        if isinstance(value, list):
            return tuple(str(item) for item in value)
        return ()

    @abstractmethod
    async def _ensure_tables(self, conn: asyncpg.Connection) -> None:
        raise NotImplementedError
