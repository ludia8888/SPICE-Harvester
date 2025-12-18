"""
First-class audit log store (Postgres).

Audit logs are stored as structured records (not just log lines) with a
tamper-evident hash chain per partition_key.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

import asyncpg

from shared.config.service_config import ServiceConfig
from shared.models.audit_log import AuditLogEntry, AuditStatus


class AuditLogStore:
    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_audit",
        pool_min: Optional[int] = None,
        pool_max: Optional[int] = None,
    ):
        self._dsn = dsn or ServiceConfig.get_postgres_url()
        self._schema = schema
        self._pool: Optional[asyncpg.Pool] = None
        self._pool_min = int(os.getenv("AUDIT_PG_POOL_MIN", str(pool_min or 1)))
        self._pool_max = int(os.getenv("AUDIT_PG_POOL_MAX", str(pool_max or 5)))

    async def initialize(self) -> None:
        await self.connect()

    async def connect(self) -> None:
        if self._pool:
            return
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=self._pool_min,
            max_size=self._pool_max,
            command_timeout=int(os.getenv("AUDIT_PG_COMMAND_TIMEOUT", "30")),
        )
        await self.ensure_schema()

    async def shutdown(self) -> None:
        await self.close()

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def health_check(self) -> bool:
        try:
            if not self._pool:
                await self.connect()
            async with self._pool.acquire() as conn:
                await conn.execute("SELECT 1")
            return True
        except Exception:
            return False

    async def ensure_schema(self) -> None:
        if not self._pool:
            raise RuntimeError("AuditLogStore not connected")

        async with self._pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.audit_chain_heads (
                    partition_key TEXT PRIMARY KEY,
                    head_audit_id UUID,
                    head_hash TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.audit_logs (
                    audit_id UUID PRIMARY KEY,
                    partition_key TEXT NOT NULL,
                    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    actor TEXT,
                    action TEXT NOT NULL,
                    status TEXT NOT NULL,
                    resource_type TEXT,
                    resource_id TEXT,
                    event_id TEXT,
                    command_id TEXT,
                    trace_id TEXT,
                    correlation_id TEXT,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    error TEXT,
                    prev_hash TEXT,
                    entry_hash TEXT NOT NULL
                )
                """
            )

            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_audit_logs_partition ON {self._schema}.audit_logs(partition_key)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_audit_logs_time ON {self._schema}.audit_logs(occurred_at)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_audit_logs_action ON {self._schema}.audit_logs(action)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_audit_logs_event ON {self._schema}.audit_logs(event_id)"
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_audit_logs_resource
                ON {self._schema}.audit_logs(resource_type, resource_id)
                """
            )

    @staticmethod
    def _canonical_json(value: Dict[str, Any]) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @classmethod
    def _compute_hash(cls, *, prev_hash: Optional[str], payload: Dict[str, Any]) -> str:
        base = (prev_hash or "").encode("utf-8")
        body = cls._canonical_json(payload).encode("utf-8")
        return hashlib.sha256(base + b"\n" + body).hexdigest()

    async def append(
        self,
        *,
        entry: AuditLogEntry,
        partition_key: str,
    ) -> UUID:
        if not self._pool:
            await self.connect()
        if not partition_key:
            raise ValueError("partition_key is required")

        audit_id = UUID(entry.audit_id) if entry.audit_id else uuid4()
        occurred_at = entry.occurred_at or datetime.now(timezone.utc)
        if occurred_at.tzinfo is None:
            occurred_at = occurred_at.replace(tzinfo=timezone.utc)

        entry_payload: Dict[str, Any] = {
            "audit_id": str(audit_id),
            "partition_key": partition_key,
            "occurred_at": occurred_at.isoformat(),
            "actor": entry.actor,
            "action": entry.action,
            "status": entry.status,
            "resource_type": entry.resource_type,
            "resource_id": entry.resource_id,
            "event_id": entry.event_id,
            "command_id": entry.command_id,
            "trace_id": entry.trace_id,
            "correlation_id": entry.correlation_id,
            "metadata": entry.metadata or {},
            "error": entry.error,
        }

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    f"""
                    INSERT INTO {self._schema}.audit_chain_heads (partition_key, head_audit_id, head_hash)
                    VALUES ($1, NULL, NULL)
                    ON CONFLICT (partition_key) DO NOTHING
                    """,
                    partition_key,
                )

                head = await conn.fetchrow(
                    f"""
                    SELECT head_hash
                    FROM {self._schema}.audit_chain_heads
                    WHERE partition_key = $1
                    FOR UPDATE
                    """,
                    partition_key,
                )
                prev_hash = head["head_hash"] if head else None
                entry_hash = self._compute_hash(prev_hash=prev_hash, payload=entry_payload)

                await conn.execute(
                    f"""
                    INSERT INTO {self._schema}.audit_logs (
                        audit_id, partition_key, occurred_at, actor, action, status,
                        resource_type, resource_id, event_id, command_id, trace_id, correlation_id,
                        metadata, error, prev_hash, entry_hash
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6,
                        $7, $8, $9, $10, $11, $12,
                        $13::jsonb, $14, $15, $16
                    )
                    ON CONFLICT (audit_id) DO NOTHING
                    """,
                    audit_id,
                    partition_key,
                    occurred_at,
                    entry.actor,
                    entry.action,
                    entry.status,
                    entry.resource_type,
                    entry.resource_id,
                    entry.event_id,
                    entry.command_id,
                    entry.trace_id,
                    entry.correlation_id,
                    json.dumps(entry.metadata or {}, ensure_ascii=False),
                    entry.error,
                    prev_hash,
                    entry_hash,
                )

                await conn.execute(
                    f"""
                    UPDATE {self._schema}.audit_chain_heads
                    SET head_audit_id = $2,
                        head_hash = $3,
                        updated_at = NOW()
                    WHERE partition_key = $1
                    """,
                    partition_key,
                    audit_id,
                    entry_hash,
                )

        return audit_id

    async def log(
        self,
        *,
        partition_key: str,
        actor: Optional[str],
        action: str,
        status: AuditStatus,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        event_id: Optional[str] = None,
        command_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        occurred_at: Optional[datetime] = None,
    ) -> UUID:
        entry = AuditLogEntry(
            occurred_at=occurred_at,
            actor=actor,
            action=action,
            status=status,
            resource_type=resource_type,
            resource_id=resource_id,
            event_id=event_id,
            command_id=command_id,
            trace_id=trace_id,
            correlation_id=correlation_id,
            metadata=metadata or {},
            error=error,
        )
        return await self.append(entry=entry, partition_key=partition_key)

    async def list_logs(
        self,
        *,
        partition_key: Optional[str] = None,
        action: Optional[str] = None,
        status: Optional[str] = None,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        event_id: Optional[str] = None,
        command_id: Optional[str] = None,
        actor: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[AuditLogEntry]:
        if not self._pool:
            await self.connect()

        clauses: List[str] = []
        params: List[Any] = []

        def _add(condition: str, value: Any) -> None:
            params.append(value)
            clauses.append(condition.replace("$X", f"${len(params)}"))

        if partition_key:
            _add("partition_key = $X", partition_key)
        if action:
            _add("action = $X", action)
        if status:
            _add("status = $X", status)
        if resource_type:
            _add("resource_type = $X", resource_type)
        if resource_id:
            _add("resource_id = $X", resource_id)
        if event_id:
            _add("event_id = $X", event_id)
        if command_id:
            _add("command_id = $X", command_id)
        if actor:
            _add("actor = $X", actor)
        if since:
            _add("occurred_at >= $X", since)
        if until:
            _add("occurred_at <= $X", until)

        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        limit = max(1, min(int(limit), 1000))
        offset = max(0, int(offset))

        query = (
            f"""
            SELECT audit_id, occurred_at, actor, action, status, resource_type, resource_id,
                   event_id, command_id, trace_id, correlation_id, metadata, error, prev_hash, entry_hash
            FROM {self._schema}.audit_logs
            {where}
            ORDER BY occurred_at DESC
            LIMIT {limit} OFFSET {offset}
            """
        )

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        items: List[AuditLogEntry] = []
        for row in rows:
            items.append(
                AuditLogEntry(
                    audit_id=str(row["audit_id"]),
                    occurred_at=row["occurred_at"],
                    actor=row["actor"],
                    action=row["action"],
                    status=row["status"],
                    resource_type=row["resource_type"],
                    resource_id=row["resource_id"],
                    event_id=row["event_id"],
                    command_id=row["command_id"],
                    trace_id=row["trace_id"],
                    correlation_id=row["correlation_id"],
                    metadata=dict(row["metadata"] or {}),
                    error=row["error"],
                    prev_hash=row["prev_hash"],
                    entry_hash=row["entry_hash"],
                )
            )
        return items

    async def count_logs(
        self,
        *,
        partition_key: Optional[str] = None,
        action: Optional[str] = None,
        status: Optional[str] = None,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        event_id: Optional[str] = None,
        command_id: Optional[str] = None,
        actor: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> int:
        if not self._pool:
            await self.connect()

        clauses: List[str] = []
        params: List[Any] = []

        def _add(condition: str, value: Any) -> None:
            params.append(value)
            clauses.append(condition.replace("$X", f"${len(params)}"))

        if partition_key:
            _add("partition_key = $X", partition_key)
        if action:
            _add("action = $X", action)
        if status:
            _add("status = $X", status)
        if resource_type:
            _add("resource_type = $X", resource_type)
        if resource_id:
            _add("resource_id = $X", resource_id)
        if event_id:
            _add("event_id = $X", event_id)
        if command_id:
            _add("command_id = $X", command_id)
        if actor:
            _add("actor = $X", actor)
        if since:
            _add("occurred_at >= $X", since)
        if until:
            _add("occurred_at <= $X", until)

        where = " WHERE " + " AND ".join(clauses) if clauses else ""

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT COUNT(*)::bigint AS c FROM {self._schema}.audit_logs{where}",
                *params,
            )
        return int(row["c"] or 0) if row else 0

    async def get_chain_head(self, *, partition_key: str) -> Dict[str, Any]:
        if not self._pool:
            await self.connect()
        if not partition_key:
            raise ValueError("partition_key is required")

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT partition_key, head_audit_id, head_hash, updated_at
                FROM {self._schema}.audit_chain_heads
                WHERE partition_key = $1
                """,
                partition_key,
            )
        if not row:
            return {"partition_key": partition_key, "head_audit_id": None, "head_hash": None, "updated_at": None}
        return {
            "partition_key": row["partition_key"],
            "head_audit_id": str(row["head_audit_id"]) if row["head_audit_id"] else None,
            "head_hash": row["head_hash"],
            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
        }


def create_audit_log_store(settings: Any) -> AuditLogStore:
    # settings is unused; ServiceConfig reads envs and handles docker/local.
    return AuditLogStore()
