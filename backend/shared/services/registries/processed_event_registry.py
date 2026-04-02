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

from shared.config.settings import get_settings
from shared.services.registries.runtime_ddl import (
    RuntimeDDLDisabledError,
    allow_runtime_ddl_bootstrap,
    find_missing_schema_objects,
    format_missing_schema_objects,
)


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


class MissingProcessedEventRegistrySchemaError(RuntimeDDLDisabledError):
    """Raised when the processed event registry schema is missing."""


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
        settings = get_settings()
        self._settings = settings
        self._dsn = dsn or settings.database.postgres_url
        self._schema = schema
        self._pool: Optional[asyncpg.Pool] = None
        lease_timeout = (
            int(lease_timeout_seconds)
            if lease_timeout_seconds is not None
            else int(settings.event_sourcing.processed_event_lease_timeout_seconds)
        )
        self._lease_timeout = timedelta(seconds=lease_timeout)

        owner_override = (settings.event_sourcing.processed_event_owner or "").strip()
        obs = settings.observability
        owner_token = str(obs.service_name or obs.hostname or "worker").strip() or "worker"
        self._owner = owner_override or f"{owner_token}:{os.getpid()}:{uuid4().hex[:8]}"

    async def connect(self) -> None:
        if self._pool:
            return

        settings = self._settings
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=int(settings.event_sourcing.processed_event_pg_pool_min),
            max_size=int(settings.event_sourcing.processed_event_pg_pool_max),
            command_timeout=int(settings.event_sourcing.processed_event_pg_command_timeout),
        )
        await self.ensure_schema()

    async def initialize(self) -> None:
        await self.connect()

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def shutdown(self) -> None:
        await self.close()

    async def ensure_schema(self) -> None:
        if not self._pool:
            raise RuntimeError("ProcessedEventRegistry not connected")

        async with self._pool.acquire() as conn:
            missing_objects = await find_missing_schema_objects(
                conn,
                schema=self._schema,
                required_relations=("processed_events", "aggregate_versions"),
            )
            if not missing_objects:
                return
            bootstrap_allowed = allow_runtime_ddl_bootstrap()
            if not bootstrap_allowed:
                raise MissingProcessedEventRegistrySchemaError(
                    format_missing_schema_objects(
                        "ProcessedEventRegistry",
                        missing=missing_objects,
                        bootstrap_allowed=False,
                    )
                )
            await self._bootstrap_schema(conn, missing_objects=missing_objects)

    async def _bootstrap_schema(self, conn: asyncpg.Connection, *, missing_objects: list[str]) -> None:
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

    async def _lock_aggregate(self, conn: asyncpg.Connection, *, handler: str, aggregate_id: str) -> None:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))",
            handler,
            aggregate_id,
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
                if aggregate_id and sequence_number is not None:
                    await self._lock_aggregate(conn, handler=handler, aggregate_id=aggregate_id)
                    lower_inflight = await conn.fetchval(
                        f"""
                        SELECT 1
                        FROM {self._schema}.processed_events
                        WHERE handler = $1
                          AND aggregate_id = $2
                          AND status = 'processing'
                          AND sequence_number IS NOT NULL
                          AND sequence_number < $3
                          AND event_id <> $4
                          AND COALESCE(heartbeat_at, started_at) >= $5
                        LIMIT 1
                        """,
                        handler,
                        aggregate_id,
                        sequence_number,
                        event_id,
                        lease_cutoff,
                    )
                    if lower_inflight:
                        return ClaimResult(
                            decision=ClaimDecision.IN_PROGRESS,
                            existing_status="processing",
                        )

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
                    elif status in {"failed", "retrying"}:
                        retried = await conn.fetchrow(
                            f"""
                            UPDATE {self._schema}.processed_events
                            SET status = 'processing',
                                started_at = NOW(),
                                heartbeat_at = NOW(),
                                owner = $3,
                                attempt_count = attempt_count + 1,
                                last_error = NULL,
                                processed_at = NULL
                            WHERE handler = $1
                              AND event_id = $2
                              AND status = ANY($4::text[])
                            RETURNING attempt_count
                            """,
                            handler,
                            event_id,
                            self._owner,
                            ["failed", "retrying"],
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

    async def get_event_record(self, *, event_id: str) -> Optional[dict]:
        if not self._pool:
            raise RuntimeError("ProcessedEventRegistry not connected")

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT handler,
                       status,
                       last_error,
                       attempt_count,
                       started_at,
                       processed_at,
                       heartbeat_at,
                       aggregate_id,
                       sequence_number
                FROM {self._schema}.processed_events
                WHERE event_id = $1
                """,
                event_id,
            )

        if not rows:
            return None

        normalized_rows = [dict(row) for row in rows]
        statuses = {str(row.get("status") or "") for row in normalized_rows}

        if "processing" in statuses:
            aggregate_status = "processing"
        elif "retrying" in statuses:
            aggregate_status = "retrying"
        elif "failed" in statuses:
            aggregate_status = "failed"
        elif "done" in statuses:
            aggregate_status = "done"
        elif "skipped_stale" in statuses:
            aggregate_status = "skipped_stale"
        else:
            aggregate_status = str(normalized_rows[0].get("status") or "")

        def _timestamp(row: dict) -> datetime:
            value = (
                row.get("processed_at")
                or row.get("heartbeat_at")
                or row.get("started_at")
                or datetime.fromtimestamp(0, tz=timezone.utc)
            )
            return value if isinstance(value, datetime) else datetime.fromtimestamp(0, tz=timezone.utc)

        def _row_priority(row: dict) -> tuple[int, datetime]:
            status_value = str(row.get("status") or "")
            order = {"processing": 5, "retrying": 4, "failed": 3, "done": 2, "skipped_stale": 1}.get(status_value, 0)
            return order, _timestamp(row)

        representative = max(normalized_rows, key=_row_priority)
        aggregate: dict = dict(representative)
        aggregate["status"] = aggregate_status
        aggregate["handler"] = None if len(normalized_rows) > 1 else representative.get("handler")
        aggregate["handler_states"] = normalized_rows
        aggregate["attempt_count"] = max(int(row.get("attempt_count") or 0) for row in normalized_rows)

        error_row = next(
            (row for row in sorted(normalized_rows, key=_row_priority, reverse=True) if row.get("last_error")),
            None,
        )
        if error_row is not None:
            aggregate["last_error"] = error_row.get("last_error")
        return aggregate

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
                "mark_failed rejected "
                f"(handler={handler}, event_id={event_id}, "
                f"status={existing.get('status') if existing else None}, "
                f"owner={existing.get('owner') if existing else None})"
            )

    async def mark_retrying(
        self,
        *,
        handler: str,
        event_id: str,
        error: str,
    ) -> None:
        """
        Mark a claimed event as retrying without finalizing it as failed.

        This is used for transient errors where the worker intends to retry soon.
        """
        if not self._pool:
            raise RuntimeError("ProcessedEventRegistry not connected")

        async with self._pool.acquire() as conn:
            updated = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.processed_events
                SET status = 'retrying',
                    processed_at = NULL,
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
                "mark_retrying rejected "
                f"(handler={handler}, event_id={event_id}, "
                f"status={existing.get('status') if existing else None}, "
                f"owner={existing.get('owner') if existing else None})"
            )


def validate_lease_settings(
    *,
    lease_timeout_seconds: Optional[int] = None,
    heartbeat_interval_seconds: Optional[int] = None,
) -> None:
    settings = get_settings()
    heartbeat = int(
        heartbeat_interval_seconds
        if heartbeat_interval_seconds is not None
        else settings.event_sourcing.processed_event_heartbeat_interval_seconds
    )
    lease = int(
        lease_timeout_seconds
        if lease_timeout_seconds is not None
        else settings.event_sourcing.processed_event_lease_timeout_seconds
    )

    if heartbeat <= 0 or lease <= 0:
        raise RuntimeError(
            "ProcessedEventRegistry lease settings must be positive. "
            f"Got heartbeat={heartbeat}, lease={lease}."
        )

    if heartbeat >= lease:
        raise RuntimeError(
            "ProcessedEventRegistry heartbeat interval must be smaller than lease timeout. "
            f"Got heartbeat={heartbeat}, lease={lease}."
        )


def validate_registry_enabled() -> None:
    settings = get_settings()
    enabled = bool(settings.event_sourcing.enable_processed_event_registry)
    if not enabled:
        raise RuntimeError(
            "ENABLE_PROCESSED_EVENT_REGISTRY=false is not supported. "
            "ProcessedEventRegistry is required for idempotency and ordering safety."
        )
