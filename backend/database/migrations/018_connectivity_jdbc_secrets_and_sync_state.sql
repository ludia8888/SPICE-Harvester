-- Foundry connectivity JDBC P0
-- - Add separated encrypted secrets storage
-- - Add extensible sync_state_json for watermark/cdc token state

CREATE SCHEMA IF NOT EXISTS spice_connectors;

CREATE TABLE IF NOT EXISTS spice_connectors.connector_connection_secrets (
    source_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    secrets_json_enc JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_type, source_id)
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'spice_connectors'
          AND table_name = 'connector_sources'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_schema = 'spice_connectors'
          AND table_name = 'connector_connection_secrets'
          AND constraint_name = 'connector_connection_secrets_source_fk'
    ) THEN
        ALTER TABLE spice_connectors.connector_connection_secrets
            ADD CONSTRAINT connector_connection_secrets_source_fk
            FOREIGN KEY (source_type, source_id)
            REFERENCES spice_connectors.connector_sources(source_type, source_id)
            ON DELETE CASCADE;
    END IF;
END $$;

ALTER TABLE spice_connectors.connector_sync_state
    ADD COLUMN IF NOT EXISTS sync_state_json JSONB NOT NULL DEFAULT '{}'::jsonb;
