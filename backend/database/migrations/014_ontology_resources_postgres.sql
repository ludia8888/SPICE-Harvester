-- 014_ontology_resources_postgres.sql
-- Foundry-style ontology resource registry in PostgreSQL.
-- Keeps ontology extension resources in PostgreSQL as the canonical store.

CREATE TABLE IF NOT EXISTS ontology_resources (
    db_name TEXT NOT NULL,
    branch TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    label TEXT NOT NULL,
    description TEXT,
    label_i18n JSONB,
    description_i18n JSONB,
    spec JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (db_name, branch, resource_type, resource_id)
);

CREATE INDEX IF NOT EXISTS idx_ontology_resources_lookup
    ON ontology_resources (db_name, branch, resource_type, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_ontology_resources_db_branch
    ON ontology_resources (db_name, branch);
