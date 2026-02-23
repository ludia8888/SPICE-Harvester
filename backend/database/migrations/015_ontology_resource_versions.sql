-- 015_ontology_resource_versions.sql
-- Add explicit versioned records for ontology resources.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='ontology_resources') THEN
        ALTER TABLE ontology_resources
            ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS ontology_resource_versions (
    db_name TEXT NOT NULL,
    branch TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    version BIGINT NOT NULL,
    operation TEXT NOT NULL,
    snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (db_name, branch, resource_type, resource_id, version)
);

CREATE INDEX IF NOT EXISTS idx_ontology_resource_versions_lookup
    ON ontology_resource_versions (
        db_name,
        branch,
        resource_type,
        resource_id,
        version DESC
    );
