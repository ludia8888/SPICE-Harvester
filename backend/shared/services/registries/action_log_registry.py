"""
Durable Action log registry (Postgres).

Action logs are the audit SSoT for writeback decisions and provide a state
machine/outbox to tolerate partial failures:

PENDING -> COMMIT_WRITTEN -> EVENT_EMITTED -> SUCCEEDED
FAILED is terminal when commit creation fails.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

import asyncpg

from shared.config.settings import get_settings
from shared.services.registries.postgres_schema_registry import PostgresSchemaRegistry


class ActionLogStatus(str, Enum):
    PENDING = "PENDING"
    COMMIT_WRITTEN = "COMMIT_WRITTEN"
    EVENT_EMITTED = "EVENT_EMITTED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class ActionDependencyRecord:
    child_action_log_id: str
    parent_action_log_id: str
    trigger_on: str
    parent_status: Optional[str]


@dataclass(frozen=True)
class ActionLogRecord:
    action_log_id: str
    db_name: str
    action_type_id: str
    action_type_rid: Optional[str]
    resource_rid: Optional[str]
    ontology_commit_id: Optional[str]
    input: Dict[str, Any]
    status: str
    result: Optional[Dict[str, Any]]
    correlation_id: Optional[str]
    submitted_by: Optional[str]
    submitted_at: datetime
    finished_at: Optional[datetime]
    writeback_target: Optional[Dict[str, Any]]
    writeback_commit_id: Optional[str]
    action_applied_event_id: Optional[str]
    action_applied_seq: Optional[int]
    metadata: Dict[str, Any]
    updated_at: datetime


def _coerce_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return None


def _jsonb_to_dict(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _jsonb_to_optional_dict(value: Any) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return dict(parsed) if isinstance(parsed, dict) else None
    return None


def _row_to_record(row: Optional[asyncpg.Record]) -> Optional[ActionLogRecord]:
    if not row:
        return None
    return ActionLogRecord(
        action_log_id=str(row["action_log_id"]),
        db_name=str(row["db_name"]),
        action_type_id=str(row["action_type_id"]),
        action_type_rid=row.get("action_type_rid"),
        resource_rid=row.get("resource_rid"),
        ontology_commit_id=row.get("ontology_commit_id"),
        input=_jsonb_to_dict(row.get("input")),
        status=str(row["status"]),
        result=_jsonb_to_optional_dict(row.get("result")),
        correlation_id=row.get("correlation_id"),
        submitted_by=row.get("submitted_by"),
        submitted_at=_coerce_dt(row.get("submitted_at")) or datetime.now(timezone.utc),
        finished_at=_coerce_dt(row.get("finished_at")),
        writeback_target=_jsonb_to_optional_dict(row.get("writeback_target")),
        writeback_commit_id=row.get("writeback_commit_id"),
        action_applied_event_id=row.get("action_applied_event_id"),
        action_applied_seq=int(row["action_applied_seq"]) if row.get("action_applied_seq") is not None else None,
        metadata=_jsonb_to_dict(row.get("metadata")),
        updated_at=_coerce_dt(row.get("updated_at")) or datetime.now(timezone.utc),
    )


class ActionLogRegistry(PostgresSchemaRegistry):
    """
    Postgres-backed Action log registry.

    Notes:
    - Uses its own schema/pool so workers can run independently from OMS.
    - Updates are intentionally simple (no multi-step saga engine).
    """

    _DEPENDENCY_TRIGGER_VALUES = {"SUCCEEDED", "FAILED", "COMPLETED"}
    _REQUIRED_TABLES = ("ontology_action_logs", "ontology_action_dependencies")

    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_action_logs",
        pool_min: Optional[int] = None,
        pool_max: Optional[int] = None,
        allow_runtime_ddl_bootstrap: Optional[bool] = None,
    ) -> None:
        perf = get_settings().performance
        pool_min_value = int(pool_min) if pool_min is not None else int(perf.action_log_pg_pool_min)
        pool_max_value = int(pool_max) if pool_max is not None else int(perf.action_log_pg_pool_max)
        super().__init__(
            dsn=dsn,
            schema=schema,
            pool_min=pool_min_value,
            pool_max=pool_max_value,
            command_timeout=int(perf.action_log_pg_command_timeout_seconds),
            allow_runtime_ddl_bootstrap=allow_runtime_ddl_bootstrap,
        )

    @staticmethod
    def _jsonb_param(value: Any) -> Optional[str]:
        """
        asyncpg expects JSON/JSONB bind params as strings by default.

        We keep the SQL cast (::jsonb) and serialize Python objects explicitly.
        """

        if value is None:
            return None
        if isinstance(value, str):
            return value
        try:
            return json.dumps(value, ensure_ascii=False)
        except TypeError:
            return json.dumps(str(value), ensure_ascii=False)

    def _required_tables(self) -> tuple[str, ...]:
        return self._REQUIRED_TABLES

    async def _ensure_tables(self, conn: asyncpg.Connection) -> None:  # type: ignore[override]
        statuses = ",".join(f"'{s.value}'" for s in ActionLogStatus)
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.ontology_action_logs (
                action_log_id UUID PRIMARY KEY,
                db_name TEXT NOT NULL,
                action_type_id TEXT NOT NULL,
                action_type_rid TEXT,
                resource_rid TEXT,
                ontology_commit_id TEXT,
                input JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                status TEXT NOT NULL CHECK (status IN ({statuses})),
                result JSONB,
                correlation_id TEXT,
                submitted_by TEXT,
                submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                finished_at TIMESTAMPTZ,
                writeback_target JSONB,
                writeback_commit_id TEXT,
                action_applied_event_id TEXT,
                action_applied_seq BIGINT,
                metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_action_logs_db_status ON {self._schema}.ontology_action_logs(db_name, status, submitted_at DESC)"
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_action_logs_status_updated ON {self._schema}.ontology_action_logs(status, updated_at DESC)"
        )
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.ontology_action_dependencies (
                child_action_log_id UUID NOT NULL,
                parent_action_log_id UUID NOT NULL,
                trigger_on TEXT NOT NULL CHECK (trigger_on IN ('SUCCEEDED', 'FAILED', 'COMPLETED')),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (child_action_log_id, parent_action_log_id),
                CONSTRAINT fk_action_dependency_child
                    FOREIGN KEY (child_action_log_id)
                    REFERENCES {self._schema}.ontology_action_logs(action_log_id)
                    ON DELETE CASCADE,
                CONSTRAINT fk_action_dependency_parent
                    FOREIGN KEY (parent_action_log_id)
                    REFERENCES {self._schema}.ontology_action_logs(action_log_id)
                    ON DELETE CASCADE
            )
            """
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_action_dependencies_parent ON {self._schema}.ontology_action_dependencies(parent_action_log_id, created_at DESC)"
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_action_dependencies_child ON {self._schema}.ontology_action_dependencies(child_action_log_id, created_at DESC)"
        )

        # Forward-compatible schema evolution
        await conn.execute(
            f"ALTER TABLE {self._schema}.ontology_action_logs ADD COLUMN IF NOT EXISTS action_type_id TEXT"
        )
        await conn.execute(
            f"ALTER TABLE {self._schema}.ontology_action_logs ADD COLUMN IF NOT EXISTS db_name TEXT"
        )
        await conn.execute(
            f"ALTER TABLE {self._schema}.ontology_action_logs ADD COLUMN IF NOT EXISTS action_applied_event_id TEXT"
        )
        await conn.execute(
            f"ALTER TABLE {self._schema}.ontology_action_logs ADD COLUMN IF NOT EXISTS action_applied_seq BIGINT"
        )
        await conn.execute(
            f"ALTER TABLE {self._schema}.ontology_action_logs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ"
        )
        await conn.execute(
            f"ALTER TABLE {self._schema}.ontology_action_logs ALTER COLUMN updated_at SET DEFAULT NOW()"
        )
        await conn.execute(
            f"""
            UPDATE {self._schema}.ontology_action_logs
            SET updated_at = COALESCE(updated_at, submitted_at, NOW())
            WHERE updated_at IS NULL
            """
        )
        await conn.execute(
            f"ALTER TABLE {self._schema}.ontology_action_logs ALTER COLUMN updated_at SET NOT NULL"
        )

    @classmethod
    def _normalize_dependency_trigger(cls, value: str) -> str:
        trigger = str(value or "").strip().upper() or "SUCCEEDED"
        if trigger not in cls._DEPENDENCY_TRIGGER_VALUES:
            raise ValueError(
                f"trigger_on must be one of {sorted(cls._DEPENDENCY_TRIGGER_VALUES)}"
            )
        return trigger

    async def create_log(
        self,
        *,
        action_log_id: UUID,
        db_name: str,
        action_type_id: str,
        action_type_rid: Optional[str] = None,
        resource_rid: Optional[str] = None,
        ontology_commit_id: Optional[str] = None,
        input_payload: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None,
        submitted_by: Optional[str] = None,
        writeback_target: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ActionLogRecord:
        if not self._pool:
            await self.connect()
        if not db_name:
            raise ValueError("db_name is required")
        if not action_type_id:
            raise ValueError("action_type_id is required")

        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self._schema}.ontology_action_logs (
                    action_log_id, db_name, action_type_id, action_type_rid, resource_rid, ontology_commit_id,
                    input, status, correlation_id, submitted_by, writeback_target, metadata, updated_at
                ) VALUES (
                    $1::uuid, $2, $3, $4, $5, $6,
                    $7::jsonb, $8, $9, $10, $11::jsonb, $12::jsonb, NOW()
                )
                ON CONFLICT (action_log_id) DO NOTHING
                """,
                str(action_log_id),
                db_name,
                action_type_id,
                action_type_rid,
                resource_rid,
                ontology_commit_id,
                self._jsonb_param(input_payload or {}),
                ActionLogStatus.PENDING.value,
                correlation_id,
                submitted_by,
                self._jsonb_param(writeback_target),
                self._jsonb_param(metadata or {}),
            )
            row = await conn.fetchrow(
                f"SELECT * FROM {self._schema}.ontology_action_logs WHERE action_log_id = $1::uuid",
                str(action_log_id),
            )
        record = _row_to_record(row)
        if not record:
            raise RuntimeError("Failed to create action log")
        return record

    async def add_dependency(
        self,
        *,
        child_action_log_id: str,
        parent_action_log_id: str,
        trigger_on: str = "SUCCEEDED",
    ) -> None:
        if not self._pool:
            await self.connect()
        child_id = str(child_action_log_id or "").strip()
        parent_id = str(parent_action_log_id or "").strip()
        if not child_id:
            raise ValueError("child_action_log_id is required")
        if not parent_id:
            raise ValueError("parent_action_log_id is required")
        if child_id == parent_id:
            raise ValueError("child_action_log_id cannot depend on itself")
        normalized_trigger = self._normalize_dependency_trigger(trigger_on)

        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self._schema}.ontology_action_dependencies (
                    child_action_log_id,
                    parent_action_log_id,
                    trigger_on
                )
                VALUES ($1::uuid, $2::uuid, $3)
                ON CONFLICT (child_action_log_id, parent_action_log_id) DO UPDATE
                SET trigger_on = EXCLUDED.trigger_on
                """,
                child_id,
                parent_id,
                normalized_trigger,
            )

    async def list_dependent_children(self, *, parent_action_log_id: str) -> List[str]:
        if not self._pool:
            await self.connect()
        parent_id = str(parent_action_log_id or "").strip()
        if not parent_id:
            return []
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT DISTINCT child_action_log_id
                FROM {self._schema}.ontology_action_dependencies
                WHERE parent_action_log_id = $1::uuid
                ORDER BY child_action_log_id
                """,
                parent_id,
            )
        return [str(row["child_action_log_id"]) for row in rows if row.get("child_action_log_id")]

    async def list_dependency_status_for_child(
        self,
        *,
        child_action_log_id: str,
    ) -> List[ActionDependencyRecord]:
        if not self._pool:
            await self.connect()
        child_id = str(child_action_log_id or "").strip()
        if not child_id:
            return []
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT
                    d.child_action_log_id,
                    d.parent_action_log_id,
                    d.trigger_on,
                    p.status AS parent_status
                FROM {self._schema}.ontology_action_dependencies d
                LEFT JOIN {self._schema}.ontology_action_logs p
                    ON p.action_log_id = d.parent_action_log_id
                WHERE d.child_action_log_id = $1::uuid
                ORDER BY d.created_at ASC
                """,
                child_id,
            )

        records: List[ActionDependencyRecord] = []
        for row in rows:
            records.append(
                ActionDependencyRecord(
                    child_action_log_id=str(row["child_action_log_id"]),
                    parent_action_log_id=str(row["parent_action_log_id"]),
                    trigger_on=str(row["trigger_on"]),
                    parent_status=str(row["parent_status"]) if row.get("parent_status") is not None else None,
                )
            )
        return records

    async def get_log(self, *, action_log_id: str) -> Optional[ActionLogRecord]:
        if not self._pool:
            await self.connect()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT * FROM {self._schema}.ontology_action_logs WHERE action_log_id = $1::uuid",
                str(action_log_id),
            )
        return _row_to_record(row)

    async def list_logs(
        self,
        *,
        db_name: str,
        statuses: Optional[List[str]] = None,
        action_type_id: Optional[str] = None,
        submitted_by: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[ActionLogRecord]:
        if not self._pool:
            await self.connect()
        db_name = str(db_name or "").strip()
        if not db_name:
            raise ValueError("db_name is required")

        status_values = None
        if statuses:
            status_values = [str(s or "").strip() for s in statuses if str(s or "").strip()]
            if not status_values:
                status_values = None

        action_type_id = str(action_type_id or "").strip() or None
        submitted_by = str(submitted_by or "").strip() or None

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT *
                FROM {self._schema}.ontology_action_logs
                WHERE db_name = $1
                  AND ($2::text[] IS NULL OR status = ANY($2::text[]))
                  AND ($3::text IS NULL OR action_type_id = $3)
                  AND ($4::text IS NULL OR submitted_by = $4)
                ORDER BY submitted_at DESC
                LIMIT $5
                OFFSET $6
                """,
                db_name,
                status_values,
                action_type_id,
                submitted_by,
                max(1, int(limit)),
                max(0, int(offset)),
            )

        return [rec for rec in (_row_to_record(row) for row in rows) if rec is not None]

    async def count_logs(
        self,
        *,
        db_name: str,
        statuses: Optional[List[str]] = None,
        action_type_id: Optional[str] = None,
        submitted_by: Optional[str] = None,
    ) -> int:
        if not self._pool:
            await self.connect()
        db_name = str(db_name or "").strip()
        if not db_name:
            raise ValueError("db_name is required")

        status_values = None
        if statuses:
            status_values = [str(s or "").strip() for s in statuses if str(s or "").strip()]
            if not status_values:
                status_values = None

        action_type_id = str(action_type_id or "").strip() or None
        submitted_by = str(submitted_by or "").strip() or None

        async with self._pool.acquire() as conn:
            value = await conn.fetchval(
                f"""
                SELECT COUNT(*)
                FROM {self._schema}.ontology_action_logs
                WHERE db_name = $1
                  AND ($2::text[] IS NULL OR status = ANY($2::text[]))
                  AND ($3::text IS NULL OR action_type_id = $3)
                  AND ($4::text IS NULL OR submitted_by = $4)
                """,
                db_name,
                status_values,
                action_type_id,
                submitted_by,
            )
        return int(value or 0)

    async def list_outbox_candidates(
        self,
        *,
        limit: int = 100,
        statuses: Optional[List[ActionLogStatus]] = None,
        skip_locked: bool = True,
    ) -> List[ActionLogRecord]:
        """
        List action log records that need outbox processing.

        When ``skip_locked=True`` (default), each candidate row is guarded by a
        Postgres session-level advisory lock (``pg_try_advisory_lock``) derived
        from its ``action_log_id`` UUID.  Rows already claimed by another outbox
        worker instance are silently skipped, preventing duplicate processing
        across concurrent workers.  The advisory lock is released automatically
        when the connection is returned to the pool.
        """
        if not self._pool:
            await self.connect()
        statuses = statuses or [ActionLogStatus.COMMIT_WRITTEN, ActionLogStatus.EVENT_EMITTED]
        status_values = [s.value for s in statuses]
        if skip_locked:
            # Use advisory lock: hash UUID to two int4 values for pg_try_advisory_lock
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    f"""
                    SELECT *
                    FROM {self._schema}.ontology_action_logs
                    WHERE status = ANY($1::text[])
                      AND pg_try_advisory_lock(
                          hashtext('outbox:' || action_log_id::text)
                      )
                    ORDER BY updated_at ASC
                    LIMIT $2
                    """,
                    status_values,
                    max(1, int(limit)),
                )
        else:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    f"""
                    SELECT *
                    FROM {self._schema}.ontology_action_logs
                    WHERE status = ANY($1::text[])
                    ORDER BY updated_at ASC
                    LIMIT $2
                    """,
                    status_values,
                    max(1, int(limit)),
                )
        return [rec for rec in (_row_to_record(row) for row in rows) if rec is not None]

    async def release_outbox_lock(self, *, action_log_id: str) -> None:
        """Release the advisory lock for a given action_log_id after processing."""
        if not self._pool:
            return
        async with self._pool.acquire() as conn:
            await conn.execute(
                "SELECT pg_advisory_unlock(hashtext('outbox:' || $1::text))",
                str(action_log_id),
            )

    async def mark_commit_written(
        self,
        *,
        action_log_id: str,
        writeback_commit_id: str,
        result: Optional[Dict[str, Any]] = None,
    ) -> Optional[ActionLogRecord]:
        if not self._pool:
            await self.connect()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.ontology_action_logs
                SET status = $2,
                    writeback_commit_id = $3,
                    result = COALESCE(result, '{{}}'::jsonb) || COALESCE($4::jsonb, '{{}}'::jsonb),
                    updated_at = NOW()
                WHERE action_log_id = $1::uuid
                RETURNING *
                """,
                str(action_log_id),
                ActionLogStatus.COMMIT_WRITTEN.value,
                str(writeback_commit_id),
                self._jsonb_param(result),
            )
        return _row_to_record(row)

    async def mark_event_emitted(
        self,
        *,
        action_log_id: str,
        action_applied_event_id: str,
        action_applied_seq: Optional[int],
    ) -> Optional[ActionLogRecord]:
        if not self._pool:
            await self.connect()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.ontology_action_logs
                SET status = $2,
                    action_applied_event_id = $3,
                    action_applied_seq = $4,
                    updated_at = NOW()
                WHERE action_log_id = $1::uuid
                RETURNING *
                """,
                str(action_log_id),
                ActionLogStatus.EVENT_EMITTED.value,
                str(action_applied_event_id),
                int(action_applied_seq) if action_applied_seq is not None else None,
            )
        return _row_to_record(row)

    async def mark_succeeded(
        self,
        *,
        action_log_id: str,
        result: Optional[Dict[str, Any]] = None,
        finished_at: Optional[datetime] = None,
    ) -> Optional[ActionLogRecord]:
        if not self._pool:
            await self.connect()
        finished_at = finished_at or datetime.now(timezone.utc)
        if finished_at.tzinfo is None:
            finished_at = finished_at.replace(tzinfo=timezone.utc)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.ontology_action_logs
                SET status = $2,
                    result = COALESCE(result, '{{}}'::jsonb) || COALESCE($3::jsonb, '{{}}'::jsonb),
                    finished_at = $4,
                    updated_at = NOW()
                WHERE action_log_id = $1::uuid
                RETURNING *
                """,
                str(action_log_id),
                ActionLogStatus.SUCCEEDED.value,
                self._jsonb_param(result),
                finished_at,
            )
        return _row_to_record(row)

    async def mark_failed(
        self,
        *,
        action_log_id: str,
        result: Optional[Dict[str, Any]] = None,
        finished_at: Optional[datetime] = None,
    ) -> Optional[ActionLogRecord]:
        if not self._pool:
            await self.connect()
        finished_at = finished_at or datetime.now(timezone.utc)
        if finished_at.tzinfo is None:
            finished_at = finished_at.replace(tzinfo=timezone.utc)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.ontology_action_logs
                SET status = $2,
                    result = COALESCE(result, '{{}}'::jsonb) || COALESCE($3::jsonb, '{{}}'::jsonb),
                    finished_at = $4,
                    updated_at = NOW()
                WHERE action_log_id = $1::uuid
                RETURNING *
                """,
                str(action_log_id),
                ActionLogStatus.FAILED.value,
                self._jsonb_param(result),
                finished_at,
            )
        return _row_to_record(row)
