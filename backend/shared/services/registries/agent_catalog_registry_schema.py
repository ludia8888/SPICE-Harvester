from __future__ import annotations

import asyncpg


async def ensure_agent_function_registry_schema(conn: asyncpg.Connection, *, schema: str) -> None:
    statements = (
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.agent_functions (
            function_id TEXT NOT NULL,
            version TEXT NOT NULL DEFAULT 'v1',
            status TEXT NOT NULL DEFAULT 'ACTIVE',
            handler TEXT NOT NULL,
            tags JSONB NOT NULL DEFAULT '[]'::jsonb,
            roles JSONB NOT NULL DEFAULT '[]'::jsonb,
            input_schema JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            output_schema JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (function_id, version)
        )
        """,
        f"CREATE INDEX IF NOT EXISTS idx_agent_functions_status ON {schema}.agent_functions(status)",
        f"CREATE INDEX IF NOT EXISTS idx_agent_functions_function_id ON {schema}.agent_functions(function_id)",
    )
    for sql in statements:
        await conn.execute(sql)


async def ensure_agent_model_registry_schema(conn: asyncpg.Connection, *, schema: str) -> None:
    statements = (
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.agent_models (
            model_id TEXT PRIMARY KEY,
            provider TEXT NOT NULL,
            display_name TEXT,
            status TEXT NOT NULL DEFAULT 'ACTIVE',
            supports_json_mode BOOLEAN NOT NULL DEFAULT true,
            supports_native_tool_calling BOOLEAN NOT NULL DEFAULT false,
            max_context_tokens INTEGER,
            max_output_tokens INTEGER,
            prompt_per_1k DOUBLE PRECISION,
            completion_per_1k DOUBLE PRECISION,
            metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_agent_models_status
        ON {schema}.agent_models(status)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_agent_models_provider
        ON {schema}.agent_models(provider)
        """,
    )
    for sql in statements:
        await conn.execute(sql)


async def ensure_agent_policy_registry_schema(conn: asyncpg.Connection, *, schema: str) -> None:
    statements = (
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.agent_tenant_policies (
            tenant_id TEXT PRIMARY KEY,
            allowed_models JSONB NOT NULL DEFAULT '[]'::jsonb,
            allowed_tools JSONB NOT NULL DEFAULT '[]'::jsonb,
            default_model TEXT,
            auto_approve_rules JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            data_policies JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        f"CREATE INDEX IF NOT EXISTS idx_agent_tenant_policies_updated_at ON {schema}.agent_tenant_policies(updated_at)",
    )
    for sql in statements:
        await conn.execute(sql)


async def ensure_agent_tool_registry_schema(conn: asyncpg.Connection, *, schema: str) -> None:
    statements = (
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.agent_tool_policies (
            tool_id TEXT PRIMARY KEY,
            method TEXT NOT NULL,
            path TEXT NOT NULL,
            risk_level TEXT NOT NULL DEFAULT 'read',
            requires_approval BOOLEAN NOT NULL DEFAULT false,
            requires_idempotency_key BOOLEAN NOT NULL DEFAULT false,
            status TEXT NOT NULL DEFAULT 'ACTIVE',
            roles JSONB NOT NULL DEFAULT '[]'::jsonb,
            max_payload_bytes INTEGER,
            version TEXT NOT NULL DEFAULT 'v1',
            tool_type TEXT NOT NULL DEFAULT 'unknown',
            input_schema JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            output_schema JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            timeout_seconds DOUBLE PRECISION,
            retry_policy JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            resource_scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
            metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        f"ALTER TABLE {schema}.agent_tool_policies ADD COLUMN IF NOT EXISTS version TEXT NOT NULL DEFAULT 'v1'",
        f"ALTER TABLE {schema}.agent_tool_policies ADD COLUMN IF NOT EXISTS tool_type TEXT NOT NULL DEFAULT 'unknown'",
        f"ALTER TABLE {schema}.agent_tool_policies ADD COLUMN IF NOT EXISTS input_schema JSONB NOT NULL DEFAULT '{{}}'::jsonb",
        f"ALTER TABLE {schema}.agent_tool_policies ADD COLUMN IF NOT EXISTS output_schema JSONB NOT NULL DEFAULT '{{}}'::jsonb",
        f"ALTER TABLE {schema}.agent_tool_policies ADD COLUMN IF NOT EXISTS timeout_seconds DOUBLE PRECISION",
        f"ALTER TABLE {schema}.agent_tool_policies ADD COLUMN IF NOT EXISTS retry_policy JSONB NOT NULL DEFAULT '{{}}'::jsonb",
        f"ALTER TABLE {schema}.agent_tool_policies ADD COLUMN IF NOT EXISTS resource_scopes JSONB NOT NULL DEFAULT '[]'::jsonb",
        f"ALTER TABLE {schema}.agent_tool_policies ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb",
        f"""
        CREATE INDEX IF NOT EXISTS idx_agent_tool_policies_status
        ON {schema}.agent_tool_policies(status)
        """,
        f"CREATE INDEX IF NOT EXISTS idx_agent_tool_policies_type ON {schema}.agent_tool_policies(tool_type)",
    )
    for sql in statements:
        await conn.execute(sql)
