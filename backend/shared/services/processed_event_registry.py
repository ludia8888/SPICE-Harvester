"""
Durable processed-events registry (Postgres).

Contract:
- Idempotency key is `event_id` (scoped by `handler`).
- Consumers are at-least-once; this registry enforces idempotent side-effects.
- Per-aggregate ordering uses `sequence_number`; stale events are ignored.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional
from uuid import uuid4

import asyncpg

from shared.config.service_config import ServiceConfig


class ClaimDecision(str, Enum):
    CLAIMED = "claimed"
    DUPLICATE_DONE = "duplicate_done"
    IN_PROGRESS = "in_progress"
    STALE = "stale"


@dataclass(frozen=True)
class ClaimResult:
    decision: ClaimDecision
    attempt_count: int = 0
    existing_status: Optional[str] = None


class ProcessedEventRegistry:
    """
    Postgres-backed idempotency + ordering guard.

    The registry is keyed by (handler,event_id). Each handler maintains its own
    view of "processed" because write-side and projection-side have different
    side-effects.
    """

    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_event_registry",
        lease_timeout_seconds: Optional[int] = None,
    ):
        self._dsn = dsn or ServiceConfig.get_postgres_url()
        self._schema = schema
        self._pool: Optional[asyncpg.Pool] = None
        self._lease_timeout = timedelta(
            seconds=int(os.getenv("PROCESSED_EVENT_LEASE_TIMEOUT_SECONDS", str(lease_timeout_seconds or 900)))
        )
        self._owner = os.getenv("PROCESSED_EVENT_OWNER") or (
            f"{os.getenv('SERVICE_NAME') or os.getenv('HOSTNAME') or 'worker'}:"
            f"{os.getpid()}:{uuid4().hex[:8]}"
        )

    async def connect(self) -> None:
        if self._pool:
            return

        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=int(os.getenv("PROCESSED_EVENT_PG_POOL_MIN", "1")),
            max_size=int(os.getenv("PROCESSED_EVENT_PG_POOL_MAX", "5")),
            command_timeout=int(os.getenv("PROCESSED_EVENT_PG_COMMAND_TIMEOUT", "30")),
        )
        await self.ensure_schema()

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def ensure_schema(self) -> None:
        if not self._pool:
            raise RuntimeError("ProcessedEventRegistry not connected")

        async with self._pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.processed_events (
                    handler TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    aggregate_id TEXT,
                    sequence_number BIGINT,
                    status TEXT NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    owner TEXT,
                    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    processed_at TIMESTAMPTZ,
                    attempt_count INTEGER NOT NULL DEFAULT 1,
                    last_error TEXT,
                    PRIMARY KEY (handler, event_id)
                )
                """
            )

            # Forward-compatible schema evolution (safe in-place upgrades)
            await conn.execute(
                f"ALTER TABLE {self._schema}.processed_events ADD COLUMN IF NOT EXISTS owner TEXT"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.processed_events ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ"
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.processed_events ALTER COLUMN heartbeat_at SET DEFAULT NOW()"
            )
            await conn.execute(
                f"""
                UPDATE {self._schema}.processed_events
                SET heartbeat_at = COALESCE(heartbeat_at, started_at)
                WHERE heartbeat_at IS NULL
                """
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.processed_events ALTER COLUMN heartbeat_at SET NOT NULL"
            )

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

            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_processed_events_aggregate ON {self._schema}.processed_events(handler, aggregate_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_processed_events_status ON {self._schema}.processed_events(status)"
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_processed_events_lease
                ON {self._schema}.processed_events(handler, status, heartbeat_at)
                WHERE status = 'processing'
                """
            )

    async def claim(
        self,
        *,
        handler: str,
        event_id: str,
        aggregate_id: Optional[str] = None,
        sequence_number: Optional[int] = None,
    ) -> ClaimResult:
        """
        Try to claim an event for processing.

        Returns:
        - CLAIMED: caller should perform side-effects then mark_done/mark_failed.
        - DUPLICATE_DONE: already done/ignored; caller should ACK/skip.
        - STALE: older than current aggregate sequence; caller should ACK/skip.
        - IN_PROGRESS: another worker holds lease; caller should retry later.
        """
        if not self._pool:
            raise RuntimeError("ProcessedEventRegistry not connected")
        if not handler:
            raise ValueError("handler is required")
        if not event_id:
            raise ValueError("event_id is required")

        now = datetime.now(timezone.utc)
        lease_cutoff = now - self._lease_timeout

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                inserted = await conn.fetchrow(
                    f"""
                    INSERT INTO {self._schema}.processed_events (
                        handler, event_id, aggregate_id, sequence_number, status, started_at, owner, heartbeat_at
                    )
                    VALUES ($1, $2, $3, $4, 'processing', NOW(), $5, NOW())
                    ON CONFLICT (handler, event_id) DO NOTHING
                    RETURNING attempt_count
                    """,
                    handler,
                    event_id,
                    aggregate_id,
                    sequence_number,
                    self._owner,
                )

                if not inserted:
                    existing = await conn.fetchrow(
                        f"""
                        SELECT status, started_at, heartbeat_at, owner, attempt_count
                        FROM {self._schema}.processed_events
                        WHERE handler = $1 AND event_id = $2
                        """,
                        handler,
                        event_id,
                    )
                    if not existing:
                        # Extremely unlikely (row deleted between conflict and select); retry.
                        return ClaimResult(decision=ClaimDecision.IN_PROGRESS)

                    status = str(existing["status"])
                    attempt_count = int(existing["attempt_count"] or 0)
                    started_at = existing["started_at"]
                    heartbeat_at = existing["heartbeat_at"]
                    last_touch = heartbeat_at or started_at

                    if status in {"done", "skipped_stale"}:
                        return ClaimResult(
                            decision=ClaimDecision.DUPLICATE_DONE,
                            attempt_count=attempt_count,
                            existing_status=status,
                        )

                    if status == "processing":
                        if last_touch and last_touch < lease_cutoff:
                            reclaimed = await conn.fetchrow(
                                f"""
                                UPDATE {self._schema}.processed_events
                                SET started_at = NOW(),
                                    heartbeat_at = NOW(),
                                    owner = $4,
                                    attempt_count = attempt_count + 1,
                                    last_error = NULL
                                WHERE handler = $1
                                  AND event_id = $2
                                  AND status = 'processing'
                                  AND COALESCE(heartbeat_at, started_at) < $3
                                RETURNING attempt_count
                                """,
                                handler,
                                event_id,
                                lease_cutoff,
                                self._owner,
                            )
                            if reclaimed:
                                inserted = reclaimed
                            else:
                                return ClaimResult(
                                    decision=ClaimDecision.IN_PROGRESS,
                                    attempt_count=attempt_count,
                                    existing_status=status,
                                )
                        else:
                            return ClaimResult(
                                decision=ClaimDecision.IN_PROGRESS,
                                attempt_count=attempt_count,
                                existing_status=status,
                            )
                    elif status == "failed":
                        retried = await conn.fetchrow(
                            f"""
                            UPDATE {self._schema}.processed_events
                            SET status = 'processing',
                                started_at = NOW(),
                                heartbeat_at = NOW(),
                                owner = $3,
                                attempt_count = attempt_count + 1,
                                last_error = NULL
                            WHERE handler = $1 AND event_id = $2 AND status = 'failed'
                            RETURNING attempt_count
                            """,
                            handler,
                            event_id,
                            self._owner,
                        )
                        if retried:
                            inserted = retried
                        else:
                            return ClaimResult(
                                decision=ClaimDecision.IN_PROGRESS,
                                attempt_count=attempt_count,
                                existing_status=status,
                            )
                    else:
                        # Unknown status: treat as duplicate to prevent double side-effects.
                        return ClaimResult(
                            decision=ClaimDecision.DUPLICATE_DONE,
                            attempt_count=attempt_count,
                            existing_status=status,
                        )

                attempt_count = int(inserted["attempt_count"] or 1)

                # Ordering guard (best-effort). Only applies if we have both aggregate_id and sequence_number.
                if aggregate_id and sequence_number is not None:
                    current = await conn.fetchval(
                        f"""
                        SELECT last_sequence
                        FROM {self._schema}.aggregate_versions
                        WHERE handler = $1 AND aggregate_id = $2
                        """,
                        handler,
                        aggregate_id,
                    )
                    if current is not None and int(sequence_number) <= int(current):
                        await conn.execute(
                            f"""
                            UPDATE {self._schema}.processed_events
                            SET status = 'skipped_stale',
                                processed_at = NOW(),
                                heartbeat_at = NOW(),
                                owner = $3
                            WHERE handler = $1 AND event_id = $2
                            """,
                            handler,
                            event_id,
                            self._owner,
                        )
                        return ClaimResult(
                            decision=ClaimDecision.STALE,
                            attempt_count=attempt_count,
                            existing_status="skipped_stale",
                        )

                return ClaimResult(decision=ClaimDecision.CLAIMED, attempt_count=attempt_count)

    async def heartbeat(self, *, handler: str, event_id: str) -> bool:
        """Extend processing lease for a claimed event (owner-scoped)."""
        if not self._pool:
            raise RuntimeError("ProcessedEventRegistry not connected")

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.processed_events
                SET heartbeat_at = NOW()
                WHERE handler = $1
                  AND event_id = $2
                  AND status = 'processing'
                  AND owner = $3
                RETURNING 1
                """,
                handler,
                event_id,
                self._owner,
            )
            return row is not None

    async def mark_done(
        self,
        *,
        handler: str,
        event_id: str,
        aggregate_id: Optional[str] = None,
        sequence_number: Optional[int] = None,
    ) -> None:
        if not self._pool:
            raise RuntimeError("ProcessedEventRegistry not connected")

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                updated = await conn.fetchrow(
                    f"""
                    UPDATE {self._schema}.processed_events
                    SET status = 'done',
                        processed_at = NOW(),
                        heartbeat_at = NOW(),
                        aggregate_id = COALESCE(aggregate_id, $3),
                        sequence_number = COALESCE(sequence_number, $4),
                        last_error = NULL
                    WHERE handler = $1
                      AND event_id = $2
                      AND status = 'processing'
                      AND owner = $5
                    RETURNING 1
                    """,
                    handler,
                    event_id,
                    aggregate_id,
                    sequence_number,
                    self._owner,
                )

                if not updated:
                    existing = await conn.fetchrow(
                        f"""
                        SELECT status, owner
                        FROM {self._schema}.processed_events
                        WHERE handler = $1 AND event_id = $2
                        """,
                        handler,
                        event_id,
                    )
                    if existing and str(existing["status"]) in {"done", "skipped_stale"}:
                        return
                    raise RuntimeError(
                        f"mark_done rejected (handler={handler}, event_id={event_id}, owner={existing.get('owner') if existing else None})"
                    )

                if aggregate_id and sequence_number is not None:
                    # Atomic monotonic advance: returns a row iff this event becomes the latest applied seq.
                    await conn.fetchrow(
                        f"""
                        INSERT INTO {self._schema}.aggregate_versions (handler, aggregate_id, last_sequence, updated_at)
                        VALUES ($1, $2, $3, NOW())
                        ON CONFLICT (handler, aggregate_id) DO UPDATE
                        SET last_sequence = EXCLUDED.last_sequence,
                            updated_at = NOW()
                        WHERE {self._schema}.aggregate_versions.last_sequence < EXCLUDED.last_sequence
                        RETURNING last_sequence
                        """,
                        handler,
                        aggregate_id,
                        int(sequence_number),
                    )

    async def mark_failed(
        self,
        *,
        handler: str,
        event_id: str,
        error: str,
    ) -> None:
        if not self._pool:
            raise RuntimeError("ProcessedEventRegistry not connected")

        async with self._pool.acquire() as conn:
            updated = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.processed_events
                SET status = 'failed',
                    processed_at = NOW(),
                    heartbeat_at = NOW(),
                    last_error = $3
                WHERE handler = $1
                  AND event_id = $2
                  AND status = 'processing'
                  AND owner = $4
                RETURNING 1
                """,
                handler,
                event_id,
                error[:4000] if error else None,
                self._owner,
            )
            if updated:
                return

            # If we lost the lease/owner or the event is already finalized, never overwrite.
            existing = await conn.fetchrow(
                f"""
                SELECT status
                FROM {self._schema}.processed_events
                WHERE handler = $1 AND event_id = $2
                """,
                handler,
                event_id,
            )
            if existing and str(existing["status"]) in {"done", "skipped_stale"}:
                return
