"""
Durable Action log registry (Postgres).

Action logs are the audit SSoT for writeback decisions and provide a state
machine/outbox to tolerate partial failures:

PENDING -> COMMIT_WRITTEN -> EVENT_EMITTED -> SUCCEEDED
FAILED is terminal when commit creation fails.
"""

from __future__ import annotations

import os
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

import asyncpg

from shared.config.service_config import ServiceConfig


class ActionLogStatus(str, Enum):
    PENDING = "PENDING"
    COMMIT_WRITTEN = "COMMIT_WRITTEN"
    EVENT_EMITTED = "EVENT_EMITTED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


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


class ActionLogRegistry:
    """
    Postgres-backed Action log registry.

    Notes:
    - Uses its own schema/pool so workers can run independently from OMS.
    - Updates are intentionally simple (no multi-step saga engine).
    """

    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_action_logs",
        pool_min: Optional[int] = None,
        pool_max: Optional[int] = None,
    ) -> None:
        self._dsn = dsn or ServiceConfig.get_postgres_url()
        self._schema = schema
        self._pool: Optional[asyncpg.Pool] = None
        self._pool_min = int(os.getenv("ACTION_LOG_PG_POOL_MIN", str(pool_min or 1)))
        self._pool_max = int(os.getenv("ACTION_LOG_PG_POOL_MAX", str(pool_max or 5)))

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

    async def connect(self) -> None:
        if self._pool:
            return
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=self._pool_min,
            max_size=self._pool_max,
            command_timeout=int(os.getenv("ACTION_LOG_PG_COMMAND_TIMEOUT", "30")),
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
            raise RuntimeError("ActionLogRegistry not connected")

        statuses = ",".join(f"'{s.value}'" for s in ActionLogStatus)

        async with self._pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
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

    async def list_outbox_candidates(
        self,
        *,
        limit: int = 100,
        statuses: Optional[List[ActionLogStatus]] = None,
    ) -> List[ActionLogRecord]:
        if not self._pool:
            await self.connect()
        statuses = statuses or [ActionLogStatus.COMMIT_WRITTEN, ActionLogStatus.EVENT_EMITTED]
        status_values = [s.value for s in statuses]
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
