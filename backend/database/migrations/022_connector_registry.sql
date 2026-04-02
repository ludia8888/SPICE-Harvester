-- Connector registry schema bootstrap.
-- Runtime DDL should only be needed in dev/test after this migration lands.

CREATE SCHEMA IF NOT EXISTS spice_connectors;

CREATE TABLE IF NOT EXISTS spice_connectors.connector_sources (
    source_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_type, source_id)
);

CREATE TABLE IF NOT EXISTS spice_connectors.connector_mappings (
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
        REFERENCES spice_connectors.connector_sources(source_type, source_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS spice_connectors.connector_sync_state (
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
    sync_state_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_type, source_id),
    FOREIGN KEY (source_type, source_id)
        REFERENCES spice_connectors.connector_sources(source_type, source_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS spice_connectors.connector_update_outbox (
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
        REFERENCES spice_connectors.connector_sources(source_type, source_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS spice_connectors.connector_connection_secrets (
    source_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    secrets_json_enc JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_type, source_id),
    FOREIGN KEY (source_type, source_id)
        REFERENCES spice_connectors.connector_sources(source_type, source_id)
        ON DELETE CASCADE
);

ALTER TABLE spice_connectors.connector_sync_state
    ADD COLUMN IF NOT EXISTS sync_state_json JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_connector_outbox_status
    ON spice_connectors.connector_update_outbox(status, created_at);

CREATE INDEX IF NOT EXISTS idx_connector_sources_enabled
    ON spice_connectors.connector_sources(enabled);

CREATE INDEX IF NOT EXISTS idx_connector_sync_next_retry
    ON spice_connectors.connector_sync_state(next_retry_at);
