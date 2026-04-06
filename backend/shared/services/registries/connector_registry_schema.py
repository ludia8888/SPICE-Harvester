from __future__ import annotations

import asyncpg


async def ensure_connector_registry_schema(conn: asyncpg.Connection, *, schema: str) -> None:
    statements = (
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.connector_sources (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            config_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (source_type, source_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.connector_mappings (
            mapping_id UUID PRIMARY KEY,
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'draft',
            enabled BOOLEAN NOT NULL DEFAULT FALSE,
            target_db_name TEXT,
            target_branch TEXT,
            target_class_label TEXT,
            field_mappings JSONB NOT NULL DEFAULT '[]'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (source_type, source_id),
            FOREIGN KEY (source_type, source_id)
                REFERENCES {schema}.connector_sources(source_type, source_id)
                ON DELETE CASCADE
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.connector_sync_state (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            last_seen_cursor TEXT,
            last_emitted_seq BIGINT NOT NULL DEFAULT 0,
            last_polled_at TIMESTAMPTZ,
            last_success_at TIMESTAMPTZ,
            last_failure_at TIMESTAMPTZ,
            last_error TEXT,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            rate_limit_until TIMESTAMPTZ,
            next_retry_at TIMESTAMPTZ,
            last_command_id TEXT,
            sync_state_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (source_type, source_id),
            FOREIGN KEY (source_type, source_id)
                REFERENCES {schema}.connector_sources(source_type, source_id)
                ON DELETE CASCADE
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.connector_update_outbox (
            outbox_id UUID PRIMARY KEY,
            event_id TEXT NOT NULL UNIQUE,
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            sequence_number BIGINT,
            payload JSONB NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            publish_attempts INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            published_at TIMESTAMPTZ,
            last_error TEXT,
            FOREIGN KEY (source_type, source_id)
                REFERENCES {schema}.connector_sources(source_type, source_id)
                ON DELETE CASCADE
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.connector_connection_secrets (
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            secrets_json_enc JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (source_type, source_id),
            FOREIGN KEY (source_type, source_id)
                REFERENCES {schema}.connector_sources(source_type, source_id)
                ON DELETE CASCADE
        )
        """,
        f"""
        ALTER TABLE {schema}.connector_sync_state
        ADD COLUMN IF NOT EXISTS sync_state_json JSONB NOT NULL DEFAULT '{{}}'::jsonb
        """,
        f"CREATE INDEX IF NOT EXISTS idx_connector_outbox_status ON {schema}.connector_update_outbox(status, created_at)",
        f"CREATE INDEX IF NOT EXISTS idx_connector_sources_enabled ON {schema}.connector_sources(enabled)",
        f"CREATE INDEX IF NOT EXISTS idx_connector_sync_next_retry ON {schema}.connector_sync_state(next_retry_at)",
    )
    for sql in statements:
        await conn.execute(sql)
