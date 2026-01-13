"""
Agent session registry (Postgres).

Persists long-lived, user-scoped agent conversations so the system can:
- maintain a clean session boundary (no implicit context leakage),
- track messages, tool calls, approvals, and jobs,
- provide an outline/event stream for UI clients,
- enforce tenant/user isolation at the storage layer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import asyncpg

from shared.config.service_config import ServiceConfig
from shared.utils.json_utils import coerce_json_dataset, normalize_json_payload


@dataclass(frozen=True)
class AgentSessionRecord:
    session_id: str
    tenant_id: str
    created_by: str
    status: str
    selected_model: Optional[str]
    enabled_tools: List[str]
    summary: Optional[str]
    metadata: Dict[str, Any]
    started_at: datetime
    terminated_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class AgentSessionMessageRecord:
    message_id: str
    session_id: str
    role: str
    content: str
    content_digest: Optional[str]
    token_count: Optional[int]
    cost_estimate: Optional[float]
    latency_ms: Optional[int]
    metadata: Dict[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class AgentSessionJobRecord:
    job_id: str
    session_id: str
    plan_id: Optional[str]
    run_id: Optional[str]
    status: str
    error: Optional[str]
    metadata: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
    finished_at: Optional[datetime]


class AgentSessionRegistry:
    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_agent",
        pool_min: Optional[int] = None,
        pool_max: Optional[int] = None,
    ) -> None:
        self._dsn = dsn or ServiceConfig.get_postgres_url()
        self._schema = schema
        self._pool: Optional[asyncpg.Pool] = None
        self._pool_min = int(pool_min or 1)
        self._pool_max = int(pool_max or 5)

    async def initialize(self) -> None:
        await self.connect()

    async def connect(self) -> None:
        if self._pool:
            return
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=self._pool_min,
            max_size=self._pool_max,
            command_timeout=30,
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
            raise RuntimeError("AgentSessionRegistry not connected")

        async with self._pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_sessions (
                    session_id UUID PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    selected_model TEXT,
                    enabled_tools JSONB NOT NULL DEFAULT '[]'::jsonb,
                    summary TEXT,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    terminated_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_sessions_tenant ON {self._schema}.agent_sessions(tenant_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_sessions_creator ON {self._schema}.agent_sessions(created_by)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_sessions_status ON {self._schema}.agent_sessions(status)"
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_session_messages (
                    message_id UUID PRIMARY KEY,
                    session_id UUID NOT NULL
                        REFERENCES {self._schema}.agent_sessions(session_id)
                        ON DELETE CASCADE,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    content_digest TEXT,
                    token_count INTEGER,
                    cost_estimate DOUBLE PRECISION,
                    latency_ms INTEGER,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_messages_session ON {self._schema}.agent_session_messages(session_id)"
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_session_jobs (
                    job_id UUID PRIMARY KEY,
                    session_id UUID NOT NULL
                        REFERENCES {self._schema}.agent_sessions(session_id)
                        ON DELETE CASCADE,
                    plan_id UUID,
                    run_id UUID,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    error TEXT,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ
                )
                """
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_jobs_session ON {self._schema}.agent_session_jobs(session_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_session_jobs_status ON {self._schema}.agent_session_jobs(status)"
            )

    def _row_to_session(self, row: asyncpg.Record) -> AgentSessionRecord:
        return AgentSessionRecord(
            session_id=str(row["session_id"]),
            tenant_id=str(row["tenant_id"]),
            created_by=str(row["created_by"]),
            status=str(row["status"]),
            selected_model=row["selected_model"],
            enabled_tools=[str(t) for t in (coerce_json_dataset(row["enabled_tools"]) or []) if t],
            summary=row["summary"],
            metadata=coerce_json_dataset(row["metadata"]),
            started_at=row["started_at"],
            terminated_at=row["terminated_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_message(self, row: asyncpg.Record) -> AgentSessionMessageRecord:
        return AgentSessionMessageRecord(
            message_id=str(row["message_id"]),
            session_id=str(row["session_id"]),
            role=str(row["role"]),
            content=str(row["content"]),
            content_digest=row["content_digest"],
            token_count=row["token_count"],
            cost_estimate=row["cost_estimate"],
            latency_ms=row["latency_ms"],
            metadata=coerce_json_dataset(row["metadata"]),
            created_at=row["created_at"],
        )

    def _row_to_job(self, row: asyncpg.Record) -> AgentSessionJobRecord:
        return AgentSessionJobRecord(
            job_id=str(row["job_id"]),
            session_id=str(row["session_id"]),
            plan_id=str(row["plan_id"]) if row["plan_id"] else None,
            run_id=str(row["run_id"]) if row["run_id"] else None,
            status=str(row["status"]),
            error=row["error"],
            metadata=coerce_json_dataset(row["metadata"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            finished_at=row["finished_at"],
        )

    async def create_session(
        self,
        *,
        session_id: str,
        tenant_id: str,
        created_by: str,
        status: str = "ACTIVE",
        selected_model: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        started_at: Optional[datetime] = None,
    ) -> AgentSessionRecord:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")

        enabled_tools_payload = normalize_json_payload([t for t in (enabled_tools or []) if str(t).strip()])
        metadata_payload = normalize_json_payload(metadata or {})

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_sessions (
                    session_id, tenant_id, created_by, status, selected_model,
                    enabled_tools, metadata, started_at
                )
                VALUES ($1::uuid, $2, $3, $4, $5, $6::jsonb, $7::jsonb, COALESCE($8, NOW()))
                RETURNING session_id, tenant_id, created_by, status, selected_model,
                          enabled_tools, summary, metadata, started_at, terminated_at, created_at, updated_at
                """,
                session_id,
                tenant_id,
                created_by,
                status,
                selected_model,
                enabled_tools_payload,
                metadata_payload,
                started_at,
            )
        return self._row_to_session(row)

    async def get_session(self, *, session_id: str, tenant_id: str) -> Optional[AgentSessionRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT session_id, tenant_id, created_by, status, selected_model,
                       enabled_tools, summary, metadata, started_at, terminated_at, created_at, updated_at
                FROM {self._schema}.agent_sessions
                WHERE session_id = $1::uuid AND tenant_id = $2
                """,
                session_id,
                tenant_id,
            )
        return self._row_to_session(row) if row else None

    async def list_sessions(
        self,
        *,
        tenant_id: str,
        created_by: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[AgentSessionRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        clauses: list[str] = ["tenant_id = $1"]
        values: list[Any] = [tenant_id]
        if created_by:
            values.append(created_by)
            clauses.append(f"created_by = ${len(values)}")
        if status:
            values.append(status)
            clauses.append(f"status = ${len(values)}")
        where = " AND ".join(clauses)
        values.extend([int(limit), int(offset)])
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT session_id, tenant_id, created_by, status, selected_model,
                       enabled_tools, summary, metadata, started_at, terminated_at, created_at, updated_at
                FROM {self._schema}.agent_sessions
                WHERE {where}
                ORDER BY created_at DESC
                LIMIT ${len(values) - 1}
                OFFSET ${len(values)}
                """,
                *values,
            )
        return [self._row_to_session(row) for row in rows]

    async def update_session(
        self,
        *,
        session_id: str,
        tenant_id: str,
        status: Optional[str] = None,
        selected_model: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None,
        summary: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        terminated_at: Optional[datetime] = None,
    ) -> Optional[AgentSessionRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")

        enabled_tools_payload = (
            normalize_json_payload([t for t in (enabled_tools or []) if str(t).strip()])
            if enabled_tools is not None
            else None
        )
        metadata_payload = normalize_json_payload(metadata) if metadata is not None else None

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.agent_sessions
                SET status = COALESCE($3, status),
                    selected_model = COALESCE($4, selected_model),
                    enabled_tools = COALESCE($5::jsonb, enabled_tools),
                    summary = COALESCE($6, summary),
                    metadata = COALESCE($7::jsonb, metadata),
                    terminated_at = COALESCE($8, terminated_at),
                    updated_at = NOW()
                WHERE session_id = $1::uuid AND tenant_id = $2
                RETURNING session_id, tenant_id, created_by, status, selected_model,
                          enabled_tools, summary, metadata, started_at, terminated_at, created_at, updated_at
                """,
                session_id,
                tenant_id,
                status,
                selected_model,
                enabled_tools_payload,
                summary,
                metadata_payload,
                terminated_at,
            )
        return self._row_to_session(row) if row else None

    async def add_message(
        self,
        *,
        message_id: str,
        session_id: str,
        tenant_id: str,
        role: str,
        content: str,
        content_digest: Optional[str] = None,
        token_count: Optional[int] = None,
        cost_estimate: Optional[float] = None,
        latency_ms: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
        created_at: Optional[datetime] = None,
    ) -> AgentSessionMessageRecord:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")

        # Tenant isolation guard: fail closed if the session isn't in the tenant.
        existing = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not existing:
            raise ValueError("session not found")

        payload = normalize_json_payload(metadata or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_session_messages (
                    message_id, session_id, role, content, content_digest,
                    token_count, cost_estimate, latency_ms, metadata, created_at
                )
                VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9::jsonb, COALESCE($10, NOW()))
                RETURNING message_id, session_id, role, content, content_digest,
                          token_count, cost_estimate, latency_ms, metadata, created_at
                """,
                message_id,
                session_id,
                role,
                content,
                content_digest,
                token_count,
                cost_estimate,
                latency_ms,
                payload,
                created_at,
            )
        return self._row_to_message(row)

    async def list_messages(
        self,
        *,
        session_id: str,
        tenant_id: str,
        limit: int = 200,
        offset: int = 0,
    ) -> List[AgentSessionMessageRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        existing = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not existing:
            raise ValueError("session not found")

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT message_id, session_id, role, content, content_digest,
                       token_count, cost_estimate, latency_ms, metadata, created_at
                FROM {self._schema}.agent_session_messages
                WHERE session_id = $1::uuid
                ORDER BY created_at ASC
                LIMIT $2
                OFFSET $3
                """,
                session_id,
                int(limit),
                int(offset),
            )
        return [self._row_to_message(row) for row in rows]

    async def create_job(
        self,
        *,
        job_id: str,
        session_id: str,
        tenant_id: str,
        plan_id: Optional[str] = None,
        run_id: Optional[str] = None,
        status: str = "PENDING",
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        created_at: Optional[datetime] = None,
    ) -> AgentSessionJobRecord:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        existing = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not existing:
            raise ValueError("session not found")

        payload = normalize_json_payload(metadata or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_session_jobs (
                    job_id, session_id, plan_id, run_id, status, error,
                    metadata, created_at
                )
                VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, $5, $6, $7::jsonb, COALESCE($8, NOW()))
                RETURNING job_id, session_id, plan_id, run_id, status, error, metadata,
                          created_at, updated_at, finished_at
                """,
                job_id,
                session_id,
                plan_id,
                run_id,
                status,
                error,
                payload,
                created_at,
            )
        return self._row_to_job(row)

    async def update_job(
        self,
        *,
        job_id: str,
        tenant_id: str,
        status: Optional[str] = None,
        run_id: Optional[str] = None,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        finished_at: Optional[datetime] = None,
    ) -> Optional[AgentSessionJobRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")

        metadata_payload = normalize_json_payload(metadata) if metadata is not None else None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.agent_session_jobs j
                SET status = COALESCE($2, j.status),
                    run_id = COALESCE($3::uuid, j.run_id),
                    error = COALESCE($4, j.error),
                    metadata = COALESCE($5::jsonb, j.metadata),
                    finished_at = COALESCE($6, j.finished_at),
                    updated_at = NOW()
                FROM {self._schema}.agent_sessions s
                WHERE j.job_id = $1::uuid
                  AND j.session_id = s.session_id
                  AND s.tenant_id = $7
                RETURNING j.job_id, j.session_id, j.plan_id, j.run_id, j.status, j.error,
                          j.metadata, j.created_at, j.updated_at, j.finished_at
                """,
                job_id,
                status,
                run_id,
                error,
                metadata_payload,
                finished_at,
                tenant_id,
            )
        return self._row_to_job(row) if row else None

    async def get_job(self, *, job_id: str, tenant_id: str) -> Optional[AgentSessionJobRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT j.job_id, j.session_id, j.plan_id, j.run_id, j.status, j.error,
                       j.metadata, j.created_at, j.updated_at, j.finished_at
                FROM {self._schema}.agent_session_jobs j
                JOIN {self._schema}.agent_sessions s
                  ON j.session_id = s.session_id
                WHERE j.job_id = $1::uuid
                  AND s.tenant_id = $2
                """,
                job_id,
                tenant_id,
            )
        return self._row_to_job(row) if row else None

    async def list_jobs(
        self,
        *,
        session_id: str,
        tenant_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[AgentSessionJobRecord]:
        if not self._pool:
            raise RuntimeError("AgentSessionRegistry not connected")
        existing = await self.get_session(session_id=session_id, tenant_id=tenant_id)
        if not existing:
            raise ValueError("session not found")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT job_id, session_id, plan_id, run_id, status, error,
                       metadata, created_at, updated_at, finished_at
                FROM {self._schema}.agent_session_jobs
                WHERE session_id = $1::uuid
                ORDER BY created_at DESC
                LIMIT $2
                OFFSET $3
                """,
                session_id,
                int(limit),
                int(offset),
            )
        return [self._row_to_job(row) for row in rows]

