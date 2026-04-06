from __future__ import annotations

import asyncpg


async def ensure_agent_session_registry_schema(conn: asyncpg.Connection, *, schema: str) -> None:
    statements = (
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.agent_sessions (
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
        """,
        f"CREATE INDEX IF NOT EXISTS idx_agent_sessions_tenant ON {schema}.agent_sessions(tenant_id)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_sessions_creator ON {schema}.agent_sessions(created_by)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_sessions_status ON {schema}.agent_sessions(status)",
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.agent_session_messages (
            message_id UUID PRIMARY KEY,
            session_id UUID NOT NULL
                REFERENCES {schema}.agent_sessions(session_id)
                ON DELETE CASCADE,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            content_digest TEXT,
            is_removed BOOLEAN NOT NULL DEFAULT false,
            removed_at TIMESTAMPTZ,
            removed_by TEXT,
            removed_reason TEXT,
            token_count INTEGER,
            cost_estimate DOUBLE PRECISION,
            latency_ms INTEGER,
            metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        f"ALTER TABLE {schema}.agent_session_messages ADD COLUMN IF NOT EXISTS is_removed BOOLEAN NOT NULL DEFAULT false",
        f"ALTER TABLE {schema}.agent_session_messages ADD COLUMN IF NOT EXISTS removed_at TIMESTAMPTZ",
        f"ALTER TABLE {schema}.agent_session_messages ADD COLUMN IF NOT EXISTS removed_by TEXT",
        f"ALTER TABLE {schema}.agent_session_messages ADD COLUMN IF NOT EXISTS removed_reason TEXT",
        f"CREATE INDEX IF NOT EXISTS idx_agent_session_messages_session ON {schema}.agent_session_messages(session_id)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_session_messages_removed ON {schema}.agent_session_messages(session_id, is_removed)",
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.agent_session_jobs (
            job_id UUID PRIMARY KEY,
            session_id UUID NOT NULL
                REFERENCES {schema}.agent_sessions(session_id)
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
        """,
        f"CREATE INDEX IF NOT EXISTS idx_agent_session_jobs_session ON {schema}.agent_session_jobs(session_id)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_session_jobs_status ON {schema}.agent_session_jobs(status)",
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.agent_session_context_items (
            item_id UUID PRIMARY KEY,
            session_id UUID NOT NULL
                REFERENCES {schema}.agent_sessions(session_id)
                ON DELETE CASCADE,
            item_type TEXT NOT NULL,
            include_mode TEXT NOT NULL DEFAULT 'summary',
            ref JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            token_count INTEGER,
            metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        f"CREATE INDEX IF NOT EXISTS idx_agent_session_context_items_session ON {schema}.agent_session_context_items(session_id)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_session_context_items_type ON {schema}.agent_session_context_items(item_type)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_session_context_items_updated ON {schema}.agent_session_context_items(updated_at)",
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.agent_session_events (
            event_id UUID PRIMARY KEY,
            session_id UUID NOT NULL
                REFERENCES {schema}.agent_sessions(session_id)
                ON DELETE CASCADE,
            tenant_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            trace_id TEXT,
            correlation_id TEXT,
            data JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        f"CREATE INDEX IF NOT EXISTS idx_agent_session_events_session_time ON {schema}.agent_session_events(session_id, occurred_at)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_session_events_tenant_time ON {schema}.agent_session_events(tenant_id, occurred_at)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_session_events_type ON {schema}.agent_session_events(event_type)",
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.agent_session_tool_calls (
            tool_run_id UUID PRIMARY KEY,
            session_id UUID NOT NULL
                REFERENCES {schema}.agent_sessions(session_id)
                ON DELETE CASCADE,
            tenant_id TEXT NOT NULL,
            job_id UUID,
            plan_id UUID,
            run_id UUID,
            step_id TEXT,
            tool_id TEXT NOT NULL,
            method TEXT NOT NULL,
            path TEXT NOT NULL,
            query JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            request_body JSONB,
            request_digest TEXT,
            request_token_count INTEGER,
            idempotency_key TEXT,
            status TEXT NOT NULL DEFAULT 'STARTED',
            response_status INTEGER,
            response_body JSONB,
            response_digest TEXT,
            response_token_count INTEGER,
            error_code TEXT,
            error_message TEXT,
            side_effect_summary JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            latency_ms INTEGER,
            started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            finished_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        f"ALTER TABLE {schema}.agent_session_tool_calls ADD COLUMN IF NOT EXISTS request_token_count INTEGER",
        f"ALTER TABLE {schema}.agent_session_tool_calls ADD COLUMN IF NOT EXISTS response_token_count INTEGER",
        f"CREATE INDEX IF NOT EXISTS idx_agent_session_tool_calls_session_time ON {schema}.agent_session_tool_calls(session_id, started_at)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_session_tool_calls_tenant_time ON {schema}.agent_session_tool_calls(tenant_id, started_at)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_session_tool_calls_tool_id ON {schema}.agent_session_tool_calls(tool_id)",
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.agent_session_llm_calls (
            llm_call_id UUID PRIMARY KEY,
            session_id UUID NOT NULL
                REFERENCES {schema}.agent_sessions(session_id)
                ON DELETE CASCADE,
            tenant_id TEXT NOT NULL,
            job_id UUID,
            plan_id UUID,
            call_type TEXT NOT NULL,
            provider TEXT NOT NULL,
            model_id TEXT NOT NULL,
            cache_hit BOOLEAN NOT NULL DEFAULT false,
            latency_ms INTEGER NOT NULL,
            prompt_tokens INTEGER NOT NULL DEFAULT 0,
            completion_tokens INTEGER NOT NULL DEFAULT 0,
            total_tokens INTEGER NOT NULL DEFAULT 0,
            cost_estimate DOUBLE PRECISION,
            input_digest TEXT,
            output_digest TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        f"CREATE INDEX IF NOT EXISTS idx_agent_session_llm_calls_session_time ON {schema}.agent_session_llm_calls(session_id, created_at)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_session_llm_calls_tenant_time ON {schema}.agent_session_llm_calls(tenant_id, created_at)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_session_llm_calls_model ON {schema}.agent_session_llm_calls(model_id)",
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.agent_session_ci_results (
            ci_result_id UUID PRIMARY KEY,
            session_id UUID NOT NULL
                REFERENCES {schema}.agent_sessions(session_id)
                ON DELETE CASCADE,
            tenant_id TEXT NOT NULL,
            job_id UUID,
            plan_id UUID,
            run_id UUID,
            provider TEXT,
            status TEXT NOT NULL,
            details_url TEXT,
            summary TEXT,
            checks JSONB NOT NULL DEFAULT '[]'::jsonb,
            raw JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        f"CREATE INDEX IF NOT EXISTS idx_agent_session_ci_results_session_time ON {schema}.agent_session_ci_results(session_id, created_at)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_session_ci_results_tenant_time ON {schema}.agent_session_ci_results(tenant_id, created_at)",
    )
    for sql in statements:
        await conn.execute(sql)
