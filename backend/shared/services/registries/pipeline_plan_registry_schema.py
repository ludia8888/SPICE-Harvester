from __future__ import annotations

import asyncpg


async def ensure_pipeline_plan_registry_schema(conn: asyncpg.Connection, *, schema: str) -> None:
    statements = (
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.pipeline_plans (
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
        """,
        f"CREATE INDEX IF NOT EXISTS idx_pipeline_plans_status ON {schema}.pipeline_plans(status)",
        f"CREATE INDEX IF NOT EXISTS idx_pipeline_plans_tenant ON {schema}.pipeline_plans(tenant_id)",
        f"CREATE INDEX IF NOT EXISTS idx_pipeline_plans_db ON {schema}.pipeline_plans(db_name)",
    )
    for sql in statements:
        await conn.execute(sql)
