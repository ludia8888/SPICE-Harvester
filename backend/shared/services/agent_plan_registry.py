"""
Agent plan registry (Postgres).

Stores compiled AgentPlan artifacts so:
- approvals can reference an immutable plan_id,
- executions can load the exact plan snapshot that was reviewed/approved,
- audits can reproduce "what was intended" vs "what happened".

This is a control-plane store (not a data-plane log).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import asyncpg

from shared.config.service_config import ServiceConfig
from shared.utils.canonical_json import sha256_canonical_json_prefixed
from shared.utils.json_utils import coerce_json_dataset, normalize_json_payload


@dataclass(frozen=True)
class AgentPlanRecord:
    plan_id: str
    tenant_id: str
    status: str
    goal: str
    risk_level: str
    requires_approval: bool
    plan: Dict[str, Any]
    plan_digest: str
    created_by: Optional[str]
    created_at: datetime
    updated_at: datetime


class AgentPlanRegistry:
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
            raise RuntimeError("AgentPlanRegistry not connected")

        async with self._pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._schema}.agent_plans (
                    plan_id UUID PRIMARY KEY,
                    tenant_id TEXT NOT NULL DEFAULT 'default',
                    status TEXT NOT NULL DEFAULT 'COMPILED',
                    goal TEXT NOT NULL DEFAULT '',
                    risk_level TEXT NOT NULL DEFAULT 'read',
                    requires_approval BOOLEAN NOT NULL DEFAULT false,
                    plan JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    plan_digest TEXT NOT NULL DEFAULT '',
                    created_by TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                f"ALTER TABLE {self._schema}.agent_plans ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default'"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_plans_status ON {self._schema}.agent_plans(status)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_plans_created_at ON {self._schema}.agent_plans(created_at)"
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_agent_plans_tenant ON {self._schema}.agent_plans(tenant_id)"
            )

    def _row_to_plan(self, row: asyncpg.Record) -> AgentPlanRecord:
        return AgentPlanRecord(
            plan_id=str(row["plan_id"]),
            tenant_id=str(row.get("tenant_id") or "default"),
            status=str(row["status"]),
            goal=str(row["goal"] or ""),
            risk_level=str(row["risk_level"] or "read"),
            requires_approval=bool(row["requires_approval"]),
            plan=coerce_json_dataset(row["plan"]),
            plan_digest=str(row["plan_digest"] or ""),
            created_by=row["created_by"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def upsert_plan(
        self,
        *,
        plan_id: str,
        tenant_id: str,
        status: str,
        goal: str,
        risk_level: str,
        requires_approval: bool,
        plan: Dict[str, Any],
        created_by: Optional[str] = None,
    ) -> AgentPlanRecord:
        if not self._pool:
            raise RuntimeError("AgentPlanRegistry not connected")

        plan_payload = normalize_json_payload(plan or {})
        digest = sha256_canonical_json_prefixed(plan_payload)
        tenant_value = str(tenant_id or "").strip() or "default"
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.agent_plans (
                    plan_id, tenant_id, status, goal, risk_level, requires_approval,
                    plan, plan_digest, created_by
                )
                VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)
                ON CONFLICT (plan_id) DO UPDATE SET
                    tenant_id = EXCLUDED.tenant_id,
                    status = EXCLUDED.status,
                    goal = EXCLUDED.goal,
                    risk_level = EXCLUDED.risk_level,
                    requires_approval = EXCLUDED.requires_approval,
                    plan = EXCLUDED.plan,
                    plan_digest = EXCLUDED.plan_digest,
                    updated_at = NOW()
                WHERE {self._schema}.agent_plans.tenant_id = EXCLUDED.tenant_id
                RETURNING plan_id, status, goal, risk_level, requires_approval,
                          plan, plan_digest, created_by, created_at, updated_at, tenant_id
                """,
                plan_id,
                tenant_value,
                status,
                goal,
                risk_level,
                requires_approval,
                plan_payload,
                digest,
                created_by,
            )
        if not row:
            raise ValueError("plan upsert failed (tenant mismatch)")
        return self._row_to_plan(row)

    async def get_plan(self, *, plan_id: str, tenant_id: str) -> Optional[AgentPlanRecord]:
        if not self._pool:
            raise RuntimeError("AgentPlanRegistry not connected")
        tenant_value = str(tenant_id or "").strip() or "default"
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT plan_id, status, goal, risk_level, requires_approval,
                       plan, plan_digest, created_by, created_at, updated_at, tenant_id
                FROM {self._schema}.agent_plans
                WHERE plan_id = $1::uuid AND tenant_id = $2
                """,
                plan_id,
                tenant_value,
            )
        return self._row_to_plan(row) if row else None

    async def list_plans(
        self,
        *,
        tenant_id: str,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[AgentPlanRecord]:
        if not self._pool:
            raise RuntimeError("AgentPlanRegistry not connected")
        clauses: list[str] = ["tenant_id = $1"]
        values: list[Any] = [str(tenant_id or "").strip() or "default"]
        if status:
            values.append(str(status).strip().upper())
            clauses.append(f"status = ${len(values)}")
        where = f"WHERE {' AND '.join(clauses)}"
        values.extend([int(limit), int(offset)])
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT plan_id, status, goal, risk_level, requires_approval,
                       plan, plan_digest, created_by, created_at, updated_at, tenant_id
                FROM {self._schema}.agent_plans
                {where}
                ORDER BY created_at DESC
                LIMIT ${len(values) - 1}
                OFFSET ${len(values)}
                """,
                *values,
            )
        return [self._row_to_plan(row) for row in rows]
