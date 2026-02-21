"""Postgres registry base (Template Method).

Several registries share the same lifecycle:
- create an asyncpg pool
- ensure a schema exists
- ensure tables/indexes exist

This base class centralizes that boilerplate while delegating table creation
to concrete registries via the Template Method hook `_ensure_tables(...)`.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Optional

import asyncpg

from shared.config.settings import get_settings
import logging


class PostgresSchemaRegistry(ABC):
    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_agent",
        pool_min: Optional[int] = None,
        pool_max: Optional[int] = None,
        command_timeout: Optional[int] = None,
    ) -> None:
        self._dsn = dsn or get_settings().database.postgres_url
        self._schema = schema
        self._pool: Optional[asyncpg.Pool] = None
        self._pool_min = int(pool_min or 1)
        self._pool_max = int(pool_max or 5)
        self._command_timeout = int(command_timeout) if command_timeout is not None else 30
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
            logging.getLogger(__name__).warning("Exception fallback at shared/services/registries/postgres_schema_registry.py:68", exc_info=True)
            return False

    async def ensure_schema(self) -> None:
        if not self._pool:
            raise RuntimeError(f"{self.__class__.__name__} not connected")

        async with self._pool.acquire() as conn:
            lock_key = f"{self.__class__.__name__}:{self._schema}"
            lock_acquired = False
            try:
                lock_acquired = await self._acquire_schema_lock(conn, lock_key)
                await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
                await self._ensure_tables(conn)
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

    @abstractmethod
    async def _ensure_tables(self, conn: asyncpg.Connection) -> None:
        raise NotImplementedError
