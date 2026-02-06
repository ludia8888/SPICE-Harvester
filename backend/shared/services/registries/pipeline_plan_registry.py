"""
Pipeline plan registry (Postgres).

Stores compiled PipelinePlan artifacts for review/preview/repair loops.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import asyncpg

from shared.config.settings import get_settings
from shared.services.registries.postgres_schema_registry import PostgresSchemaRegistry
from shared.utils.canonical_json import sha256_canonical_json_prefixed
from shared.utils.json_utils import coerce_json_dataset, normalize_json_payload


@dataclass(frozen=True)
class PipelinePlanRecord:
    plan_id: str
    tenant_id: str
    status: str
    goal: str
    db_name: Optional[str]
    branch: Optional[str]
    plan: Dict[str, Any]
    plan_digest: str
    created_by: Optional[str]
    created_at: datetime
    updated_at: datetime


class PipelinePlanRegistry(PostgresSchemaRegistry):
    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_pipelines",
        pool_min: Optional[int] = None,
        pool_max: Optional[int] = None,
    ) -> None:
        perf = get_settings().performance
        resolved_pool_min = int(pool_min) if pool_min is not None else int(perf.pipeline_registry_pg_pool_min)
        resolved_pool_max = int(pool_max) if pool_max is not None else int(perf.pipeline_registry_pg_pool_max)
        super().__init__(
            dsn=dsn,
            schema=schema,
            pool_min=resolved_pool_min,
            pool_max=resolved_pool_max,
            command_timeout=int(perf.pipeline_registry_pg_command_timeout_seconds),
        )

    async def _ensure_tables(self, conn: asyncpg.Connection) -> None:  # type: ignore[override]
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.pipeline_plans (
                plan_id UUID PRIMARY KEY,
                tenant_id TEXT NOT NULL DEFAULT 'default',
                status TEXT NOT NULL DEFAULT 'COMPILED',
                goal TEXT NOT NULL DEFAULT '',
                db_name TEXT,
                branch TEXT,
                plan JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                plan_digest TEXT NOT NULL DEFAULT '',
                created_by TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_pipeline_plans_status ON {self._schema}.pipeline_plans(status)"
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_pipeline_plans_tenant ON {self._schema}.pipeline_plans(tenant_id)"
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_pipeline_plans_db ON {self._schema}.pipeline_plans(db_name)"
        )

    def _row_to_plan(self, row: asyncpg.Record) -> PipelinePlanRecord:
        return PipelinePlanRecord(
            plan_id=str(row["plan_id"]),
            tenant_id=str(row.get("tenant_id") or "default"),
            status=str(row["status"]),
            goal=str(row["goal"] or ""),
            db_name=str(row["db_name"]) if row["db_name"] else None,
            branch=str(row["branch"]) if row["branch"] else None,
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
        db_name: Optional[str],
        branch: Optional[str],
        plan: Dict[str, Any],
        created_by: Optional[str] = None,
    ) -> PipelinePlanRecord:
        if not self._pool:
            raise RuntimeError("PipelinePlanRegistry not connected")

        plan_payload = normalize_json_payload(plan or {})
        digest = sha256_canonical_json_prefixed(plan or {})
        tenant_value = str(tenant_id or "").strip() or "default"

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._schema}.pipeline_plans (
                    plan_id, tenant_id, status, goal, db_name, branch,
                    plan, plan_digest, created_by
                ) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)
                ON CONFLICT (plan_id) DO UPDATE SET
                    tenant_id = EXCLUDED.tenant_id,
                    status = EXCLUDED.status,
                    goal = EXCLUDED.goal,
                    db_name = EXCLUDED.db_name,
                    branch = EXCLUDED.branch,
                    plan = EXCLUDED.plan,
                    plan_digest = EXCLUDED.plan_digest,
                    updated_at = NOW()
                WHERE {self._schema}.pipeline_plans.tenant_id = EXCLUDED.tenant_id
                RETURNING plan_id, tenant_id, status, goal, db_name, branch,
                          plan, plan_digest, created_by, created_at, updated_at
                """,
                plan_id,
                tenant_value,
                status,
                goal,
                db_name,
                branch,
                plan_payload,
                digest,
                created_by,
            )
        if not row:
            raise ValueError("plan upsert failed (tenant mismatch)")
        return self._row_to_plan(row)

    async def get_plan(self, *, plan_id: str, tenant_id: str) -> Optional[PipelinePlanRecord]:
        if not self._pool:
            raise RuntimeError("PipelinePlanRegistry not connected")
        tenant_value = str(tenant_id or "").strip() or "default"
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT plan_id, tenant_id, status, goal, db_name, branch,
                       plan, plan_digest, created_by, created_at, updated_at
                FROM {self._schema}.pipeline_plans
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
    ) -> List[PipelinePlanRecord]:
        if not self._pool:
            raise RuntimeError("PipelinePlanRegistry not connected")
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
                SELECT plan_id, tenant_id, status, goal, db_name, branch,
                       plan, plan_digest, created_by, created_at, updated_at
                FROM {self._schema}.pipeline_plans
                {where}
                ORDER BY created_at DESC
                LIMIT ${len(values) - 1} OFFSET ${len(values)}
                """,
                *values,
            )
        return [self._row_to_plan(row) for row in rows or []]
