from __future__ import annotations

import asyncpg


async def ensure_agent_registry_schema(conn: asyncpg.Connection, *, schema: str) -> None:
    statements = (
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.agent_runs (
            run_id UUID PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
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
        """,
        f"ALTER TABLE {schema}.agent_runs ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default'",
        f"CREATE INDEX IF NOT EXISTS idx_agent_runs_plan_id ON {schema}.agent_runs(plan_id)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_runs_status ON {schema}.agent_runs(status)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_runs_tenant ON {schema}.agent_runs(tenant_id)",
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.agent_steps (
            run_id UUID NOT NULL
                REFERENCES {schema}.agent_runs(run_id)
                ON DELETE CASCADE,
            step_id TEXT NOT NULL,
            tenant_id TEXT NOT NULL DEFAULT 'default',
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
        """,
        f"ALTER TABLE {schema}.agent_steps ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default'",
        f"CREATE INDEX IF NOT EXISTS idx_agent_steps_run_id ON {schema}.agent_steps(run_id)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_steps_tool_id ON {schema}.agent_steps(tool_id)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_steps_tenant ON {schema}.agent_steps(tenant_id)",
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.agent_approvals (
            approval_id UUID PRIMARY KEY,
            plan_id UUID NOT NULL,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            step_id TEXT,
            decision TEXT NOT NULL,
            approved_by TEXT NOT NULL,
            approved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            comment TEXT,
            metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        f"ALTER TABLE {schema}.agent_approvals ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default'",
        f"CREATE INDEX IF NOT EXISTS idx_agent_approvals_plan_id ON {schema}.agent_approvals(plan_id)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_approvals_tenant ON {schema}.agent_approvals(tenant_id)",
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.agent_approval_requests (
            approval_request_id UUID PRIMARY KEY,
            plan_id UUID NOT NULL,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            session_id UUID,
            job_id UUID,
            status TEXT NOT NULL DEFAULT 'PENDING',
            risk_level TEXT NOT NULL DEFAULT 'write',
            requested_by TEXT NOT NULL,
            requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            decision TEXT,
            decided_by TEXT,
            decided_at TIMESTAMPTZ,
            comment TEXT,
            request_payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        f"ALTER TABLE {schema}.agent_approval_requests ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default'",
        f"CREATE INDEX IF NOT EXISTS idx_agent_approval_requests_plan_id ON {schema}.agent_approval_requests(plan_id)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_approval_requests_session_id ON {schema}.agent_approval_requests(session_id)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_approval_requests_job_id ON {schema}.agent_approval_requests(job_id)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_approval_requests_status ON {schema}.agent_approval_requests(status)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_approval_requests_tenant ON {schema}.agent_approval_requests(tenant_id)",
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.agent_tool_idempotency (
            tenant_id TEXT NOT NULL DEFAULT 'default',
            idempotency_key TEXT NOT NULL,
            tool_id TEXT NOT NULL,
            request_digest TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'IN_PROGRESS',
            response_status INTEGER,
            response_body JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            error TEXT,
            started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            finished_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (tenant_id, idempotency_key)
        )
        """,
        f"CREATE INDEX IF NOT EXISTS idx_agent_tool_idempotency_tool_id ON {schema}.agent_tool_idempotency(tool_id)",
    )
    for sql in statements:
        await conn.execute(sql)
