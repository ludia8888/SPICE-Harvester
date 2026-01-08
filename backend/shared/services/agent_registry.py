"""
Agent registry for runs/steps/approvals (Postgres).

This is a lightweight control-plane store to persist agent execution state,
plan approvals, and step-level audit references.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import asyncpg

from shared.config.service_config import ServiceConfig
from shared.utils.json_utils import coerce_json_dataset, normalize_json_payload


@dataclass(frozen=True)
class AgentRunRecord:
    run_id: str
    plan_id: Optional[str]
    status: str
    risk_level: str
    requester: Optional[str]
    delegated_actor: Optional[str]
    context: Dict[str, Any]
    plan_snapshot: Dict[str, Any]
    started_at: datetime
    finished_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class AgentStepRecord:
    run_id: str
    step_id: str
    tool_id: str
    status: str
    command_id: Optional[str]
    task_id: Optional[str]
    input_digest: Optional[str]
    output_digest: Optional[str]
    error: Optional[str]
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    metadata: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class AgentApprovalRecord:
    approval_id: str
    plan_id: str
    step_id: Optional[str]
    decision: str
    approved_by: str
    approved_at: datetime
    comment: Optional[str]
    metadata: Dict[str, Any]
    created_at: datetime


class AgentRegistry:
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
            raise RuntimeError("AgentRegistry not connected")

        async with self._pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_runs (
                    run_id UUID PRIMARY KEY,
                    plan_id UUID,
                    status TEXT NOT NULL,
                    risk_level TEXT NOT NULL DEFAULT 'read',
                    requester TEXT,
                    delegated_actor TEXT,
                    context JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    plan_snapshot JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_runs_plan_id ON {self._schema}.agent_runs(plan_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_runs_status ON {self._schema}.agent_runs(status)"
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_steps (
                    run_id UUID NOT NULL
                        REFERENCES {self._schema}.agent_runs(run_id)
                        ON DELETE CASCADE,
                    step_id TEXT NOT NULL,
                    tool_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    command_id TEXT,
                    task_id TEXT,
                    input_digest TEXT,
                    output_digest TEXT,
                    error TEXT,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (run_id, step_id)
                )
                """
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_steps_run_id ON {self._schema}.agent_steps(run_id)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_steps_tool_id ON {self._schema}.agent_steps(tool_id)"
            )

            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_approvals (
                    approval_id UUID PRIMARY KEY,
                    plan_id UUID NOT NULL,
                    step_id TEXT,
                    decision TEXT NOT NULL,
                    approved_by TEXT NOT NULL,
                    approved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    comment TEXT,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_approvals_plan_id ON {self._schema}.agent_approvals(plan_id)"
            )

    def _row_to_run(self, row: asyncpg.Record) -> AgentRunRecord:
        return AgentRunRecord(
            run_id=str(row["run_id"]),
            plan_id=str(row["plan_id"]) if row["plan_id"] else None,
            status=str(row["status"]),
            risk_level=str(row["risk_level"]),
            requester=row["requester"],
            delegated_actor=row["delegated_actor"],
            context=coerce_json_dataset(row["context"]),
            plan_snapshot=coerce_json_dataset(row["plan_snapshot"]),
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_step(self, row: asyncpg.Record) -> AgentStepRecord:
        return AgentStepRecord(
            run_id=str(row["run_id"]),
            step_id=str(row["step_id"]),
            tool_id=str(row["tool_id"]),
            status=str(row["status"]),
            command_id=row["command_id"],
            task_id=row["task_id"],
            input_digest=row["input_digest"],
            output_digest=row["output_digest"],
            error=row["error"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            metadata=coerce_json_dataset(row["metadata"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_approval(self, row: asyncpg.Record) -> AgentApprovalRecord:
        return AgentApprovalRecord(
            approval_id=str(row["approval_id"]),
            plan_id=str(row["plan_id"]),
            step_id=row["step_id"],
            decision=str(row["decision"]),
            approved_by=str(row["approved_by"]),
            approved_at=row["approved_at"],
            comment=row["comment"],
            metadata=coerce_json_dataset(row["metadata"]),
            created_at=row["created_at"],
        )

    async def create_run(
        self,
        *,
        run_id: str,
        plan_id: Optional[str],
        status: str,
        risk_level: str,
        requester: Optional[str],
        delegated_actor: Optional[str],
        context: Optional[Dict[str, Any]] = None,
        plan_snapshot: Optional[Dict[str, Any]] = None,
        started_at: Optional[datetime] = None,
    ) -> AgentRunRecord:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        payload_context = normalize_json_payload(context or {})
        payload_snapshot = normalize_json_payload(plan_snapshot or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_runs (
                    run_id, plan_id, status, risk_level, requester, delegated_actor,
                    context, plan_snapshot, started_at
                )
                VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7::jsonb, $8::jsonb, COALESCE($9, NOW()))
                RETURNING run_id, plan_id, status, risk_level, requester, delegated_actor,
                          context, plan_snapshot, started_at, finished_at, created_at, updated_at
                """,
                run_id,
                plan_id,
                status,
                risk_level,
                requester,
                delegated_actor,
                payload_context,
                payload_snapshot,
                started_at,
            )
        return self._row_to_run(row)

    async def update_run_status(
        self,
        *,
        run_id: str,
        status: str,
        finished_at: Optional[datetime] = None,
    ) -> Optional[AgentRunRecord]:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.agent_runs
                SET status = $2,
                    finished_at = COALESCE($3, finished_at),
                    updated_at = NOW()
                WHERE run_id = $1::uuid
                RETURNING run_id, plan_id, status, risk_level, requester, delegated_actor,
                          context, plan_snapshot, started_at, finished_at, created_at, updated_at
                """,
                run_id,
                status,
                finished_at,
            )
        return self._row_to_run(row) if row else None

    async def get_run(self, *, run_id: str) -> Optional[AgentRunRecord]:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT run_id, plan_id, status, risk_level, requester, delegated_actor,
                       context, plan_snapshot, started_at, finished_at, created_at, updated_at
                FROM {self._schema}.agent_runs
                WHERE run_id = $1::uuid
                """,
                run_id,
            )
        return self._row_to_run(row) if row else None

    async def list_runs(
        self,
        *,
        plan_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[AgentRunRecord]:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        clauses = []
        values: List[Any] = []
        if plan_id:
            values.append(plan_id)
            clauses.append(f"plan_id = ${len(values)}::uuid")
        if status:
            values.append(status)
            clauses.append(f"status = ${len(values)}")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        values.append(limit)
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT run_id, plan_id, status, risk_level, requester, delegated_actor,
                       context, plan_snapshot, started_at, finished_at, created_at, updated_at
                FROM {self._schema}.agent_runs
                {where}
                ORDER BY created_at DESC
                LIMIT ${len(values)}
                """,
                *values,
            )
        return [self._row_to_run(row) for row in rows]

    async def create_step(
        self,
        *,
        run_id: str,
        step_id: str,
        tool_id: str,
        status: str,
        command_id: Optional[str] = None,
        task_id: Optional[str] = None,
        input_digest: Optional[str] = None,
        output_digest: Optional[str] = None,
        error: Optional[str] = None,
        started_at: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AgentStepRecord:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        payload = normalize_json_payload(metadata or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_steps (
                    run_id, step_id, tool_id, status, command_id, task_id,
                    input_digest, output_digest, error, started_at, metadata
                )
                VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, COALESCE($10, NOW()), $11::jsonb)
                RETURNING run_id, step_id, tool_id, status, command_id, task_id, input_digest,
                          output_digest, error, started_at, finished_at, metadata, created_at, updated_at
                """,
                run_id,
                step_id,
                tool_id,
                status,
                command_id,
                task_id,
                input_digest,
                output_digest,
                error,
                started_at,
                payload,
            )
        return self._row_to_step(row)

    async def update_step_status(
        self,
        *,
        run_id: str,
        step_id: str,
        status: str,
        output_digest: Optional[str] = None,
        error: Optional[str] = None,
        finished_at: Optional[datetime] = None,
    ) -> Optional[AgentStepRecord]:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self._schema}.agent_steps
                SET status = $3,
                    output_digest = COALESCE($4, output_digest),
                    error = COALESCE($5, error),
                    finished_at = COALESCE($6, finished_at),
                    updated_at = NOW()
                WHERE run_id = $1::uuid AND step_id = $2
                RETURNING run_id, step_id, tool_id, status, command_id, task_id, input_digest,
                          output_digest, error, started_at, finished_at, metadata, created_at, updated_at
                """,
                run_id,
                step_id,
                status,
                output_digest,
                error,
                finished_at,
            )
        return self._row_to_step(row) if row else None

    async def list_steps(self, *, run_id: str) -> List[AgentStepRecord]:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT run_id, step_id, tool_id, status, command_id, task_id, input_digest,
                       output_digest, error, started_at, finished_at, metadata, created_at, updated_at
                FROM {self._schema}.agent_steps
                WHERE run_id = $1::uuid
                ORDER BY created_at ASC
                """,
                run_id,
            )
        return [self._row_to_step(row) for row in rows]

    async def create_approval(
        self,
        *,
        approval_id: str,
        plan_id: str,
        step_id: Optional[str],
        decision: str,
        approved_by: str,
        approved_at: Optional[datetime] = None,
        comment: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AgentApprovalRecord:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        payload = normalize_json_payload(metadata or {})
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_approvals (
                    approval_id, plan_id, step_id, decision, approved_by, approved_at, comment, metadata
                )
                VALUES ($1::uuid, $2::uuid, $3, $4, $5, COALESCE($6, NOW()), $7, $8::jsonb)
                RETURNING approval_id, plan_id, step_id, decision, approved_by, approved_at,
                          comment, metadata, created_at
                """,
                approval_id,
                plan_id,
                step_id,
                decision,
                approved_by,
                approved_at,
                comment,
                payload,
            )
        return self._row_to_approval(row)

    async def list_approvals(self, *, plan_id: str) -> List[AgentApprovalRecord]:
        if not self._pool:
            raise RuntimeError("AgentRegistry not connected")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT approval_id, plan_id, step_id, decision, approved_by, approved_at,
                       comment, metadata, created_at
                FROM {self._schema}.agent_approvals
                WHERE plan_id = $1::uuid
                ORDER BY approved_at ASC
                """,
                plan_id,
            )
        return [self._row_to_approval(row) for row in rows]
