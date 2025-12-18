"""
Postgres-backed per-aggregate sequence allocator (write-side).

Why:
- S3/MinIO is our immutable log (SSoT), but it cannot provide atomic "next seq" allocation.
- A durable, atomic allocator prevents duplicate sequence numbers under concurrent writers.

Contract:
- For a given (handler, aggregate_id), reserved sequences are strictly increasing.
- Seeding is supported to avoid restarting seq at 1 for pre-existing S3 streams.
"""

from __future__ import annotations

import os
from typing import Optional

import asyncpg

from shared.config.service_config import ServiceConfig


class OptimisticConcurrencyError(RuntimeError):
    """Raised when the aggregate's current sequence doesn't match the caller's expectation."""

    def __init__(
        self,
        *,
        handler: str,
        aggregate_id: str,
        expected_last_sequence: int,
        actual_last_sequence: int,
    ):
        self.handler = handler
        self.aggregate_id = aggregate_id
        self.expected_last_sequence = int(expected_last_sequence)
        self.actual_last_sequence = int(actual_last_sequence)
        super().__init__(
            f"optimistic concurrency conflict (handler={handler}, aggregate_id={aggregate_id}, "
            f"expected_last_sequence={expected_last_sequence}, actual_last_sequence={actual_last_sequence})"
        )


class AggregateSequenceAllocator:
    """
    Atomic per-aggregate sequence allocator.

    Storage:
    - Uses `{schema}.aggregate_versions` (last_sequence BIGINT) keyed by (handler, aggregate_id).

    Notes:
    - Consumers also use `aggregate_versions` for ordering guard, but with different `handler` values.
      Write-side allocators MUST use a dedicated handler namespace (e.g. "write_side:{aggregate_type}")
      to avoid interference.
    """

    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_event_registry",
        handler_prefix: str = "write_side",
    ):
        self._dsn = dsn or ServiceConfig.get_postgres_url()
        self._schema = schema
        self._handler_prefix = handler_prefix
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        if self._pool:
            return

        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=int(os.getenv("AGG_SEQ_PG_POOL_MIN", "1")),
            max_size=int(os.getenv("AGG_SEQ_PG_POOL_MAX", "5")),
            command_timeout=int(os.getenv("AGG_SEQ_PG_COMMAND_TIMEOUT", "30")),
        )
        await self.ensure_schema()

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def ensure_schema(self) -> None:
        if not self._pool:
            raise RuntimeError("AggregateSequenceAllocator not connected")

        async with self._pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.aggregate_versions (
                    handler TEXT NOT NULL,
                    aggregate_id TEXT NOT NULL,
                    last_sequence BIGINT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (handler, aggregate_id)
                )
                """
            )

    def handler_for(self, aggregate_type: str) -> str:
        if not aggregate_type:
            raise ValueError("aggregate_type is required")
        return f"{self._handler_prefix}:{aggregate_type}"

    async def try_reserve_next_sequence(self, *, handler: str, aggregate_id: str) -> Optional[int]:
        """
        Fast path: reserve the next seq if the allocator row already exists.

        Returns:
        - int: allocated sequence (row exists)
        - None: allocator row not found (caller should seed + retry)
        """
        if not self._pool:
            raise RuntimeError("AggregateSequenceAllocator not connected")
        if not handler:
            raise ValueError("handler is required")
        if not aggregate_id:
            raise ValueError("aggregate_id is required")

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.aggregate_versions
                SET last_sequence = last_sequence + 1,
                    updated_at = NOW()
                WHERE handler = $1 AND aggregate_id = $2
                RETURNING last_sequence
                """,
                handler,
                aggregate_id,
            )
            if not row:
                return None
            return int(row["last_sequence"])

    async def reserve_next_sequence(self, *, handler: str, aggregate_id: str, seed_last_sequence: int) -> int:
        """
        Reserve the next seq, initializing/catching-up using `seed_last_sequence`.

        This single SQL statement is atomic and safe under concurrency:
        - If the row doesn't exist, it creates it with `seed+1`.
        - If it exists, it advances to `max(current, seed)+1`.
        """
        if not self._pool:
            raise RuntimeError("AggregateSequenceAllocator not connected")
        if not handler:
            raise ValueError("handler is required")
        if not aggregate_id:
            raise ValueError("aggregate_id is required")
        if seed_last_sequence is None:
            raise ValueError("seed_last_sequence is required")
        seed = int(seed_last_sequence)
        if seed < 0:
            raise ValueError("seed_last_sequence must be >= 0")

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.aggregate_versions (handler, aggregate_id, last_sequence, updated_at)
                VALUES ($1, $2, $3 + 1, NOW())
                ON CONFLICT (handler, aggregate_id) DO UPDATE
                SET last_sequence = GREATEST({self._schema}.aggregate_versions.last_sequence, $3) + 1,
                    updated_at = NOW()
                RETURNING last_sequence
                """,
                handler,
                aggregate_id,
                seed,
            )
            if not row:  # pragma: no cover (defensive)
                raise RuntimeError("Failed to reserve next sequence")
            return int(row["last_sequence"])

    async def try_reserve_next_sequence_if_expected(
        self,
        *,
        handler: str,
        aggregate_id: str,
        expected_last_sequence: int,
    ) -> Optional[int]:
        """
        OCC fast path: reserve the next seq only if current last_sequence matches `expected_last_sequence`.

        Returns:
        - int: allocated next sequence
        - None: allocator row not found (caller should seed + retry)

        Raises:
        - OptimisticConcurrencyError: row exists but expected seq mismatched
        """
        if not self._pool:
            raise RuntimeError("AggregateSequenceAllocator not connected")
        if not handler:
            raise ValueError("handler is required")
        if not aggregate_id:
            raise ValueError("aggregate_id is required")
        expected = int(expected_last_sequence)
        if expected < 0:
            raise ValueError("expected_last_sequence must be >= 0")

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    f"""
                    UPDATE {self._schema}.aggregate_versions
                    SET last_sequence = last_sequence + 1,
                        updated_at = NOW()
                    WHERE handler = $1
                      AND aggregate_id = $2
                      AND last_sequence = $3
                    RETURNING last_sequence
                    """,
                    handler,
                    aggregate_id,
                    expected,
                )
                if row:
                    return int(row["last_sequence"])

                current = await conn.fetchval(
                    f"""
                    SELECT last_sequence
                    FROM {self._schema}.aggregate_versions
                    WHERE handler = $1 AND aggregate_id = $2
                    """,
                    handler,
                    aggregate_id,
                )
                if current is None:
                    return None
                raise OptimisticConcurrencyError(
                    handler=handler,
                    aggregate_id=aggregate_id,
                    expected_last_sequence=expected,
                    actual_last_sequence=int(current),
                )

    async def reserve_next_sequence_if_expected(
        self,
        *,
        handler: str,
        aggregate_id: str,
        seed_last_sequence: int,
        expected_last_sequence: int,
    ) -> int:
        """
        OCC reserve with seeding: ensure allocator is at least `seed_last_sequence`, then reserve next seq
        only if the current last_sequence equals `expected_last_sequence`.
        """
        if not self._pool:
            raise RuntimeError("AggregateSequenceAllocator not connected")
        if not handler:
            raise ValueError("handler is required")
        if not aggregate_id:
            raise ValueError("aggregate_id is required")
        seed = int(seed_last_sequence)
        if seed < 0:
            raise ValueError("seed_last_sequence must be >= 0")
        expected = int(expected_last_sequence)
        if expected < 0:
            raise ValueError("expected_last_sequence must be >= 0")

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    f"""
                    INSERT INTO {self._schema}.aggregate_versions (handler, aggregate_id, last_sequence, updated_at)
                    VALUES ($1, $2, $3, NOW())
                    ON CONFLICT (handler, aggregate_id) DO UPDATE
                    SET last_sequence = GREATEST({self._schema}.aggregate_versions.last_sequence, $3),
                        updated_at = NOW()
                    """,
                    handler,
                    aggregate_id,
                    seed,
                )

                row = await conn.fetchrow(
                    f"""
                    UPDATE {self._schema}.aggregate_versions
                    SET last_sequence = last_sequence + 1,
                        updated_at = NOW()
                    WHERE handler = $1
                      AND aggregate_id = $2
                      AND last_sequence = $3
                    RETURNING last_sequence
                    """,
                    handler,
                    aggregate_id,
                    expected,
                )
                if row:
                    return int(row["last_sequence"])

                current = await conn.fetchval(
                    f"""
                    SELECT last_sequence
                    FROM {self._schema}.aggregate_versions
                    WHERE handler = $1 AND aggregate_id = $2
                    """,
                    handler,
                    aggregate_id,
                )
                raise OptimisticConcurrencyError(
                    handler=handler,
                    aggregate_id=aggregate_id,
                    expected_last_sequence=expected,
                    actual_last_sequence=int(current or 0),
                )
