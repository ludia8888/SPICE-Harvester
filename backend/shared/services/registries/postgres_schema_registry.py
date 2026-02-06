"""Postgres registry base (Template Method).

Several registries share the same lifecycle:
- create an asyncpg pool
- ensure a schema exists
- ensure tables/indexes exist

This base class centralizes that boilerplate while delegating table creation
to concrete registries via the Template Method hook `_ensure_tables(...)`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

import asyncpg

from shared.config.settings import get_settings


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

    async def ensure_schema(self) -> None:
        if not self._pool:
            raise RuntimeError(f"{self.__class__.__name__} not connected")

        async with self._pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
            await self._ensure_tables(conn)

    @abstractmethod
    async def _ensure_tables(self, conn: asyncpg.Connection) -> None:
        raise NotImplementedError
