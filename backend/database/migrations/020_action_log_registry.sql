-- Action log registry schema bootstrap.
-- Runtime DDL should only be needed in dev/test after this migration lands.

CREATE SCHEMA IF NOT EXISTS spice_action_logs;

CREATE TABLE IF NOT EXISTS spice_action_logs.ontology_action_logs (
    action_log_id UUID PRIMARY KEY,
    db_name TEXT NOT NULL,
    action_type_id TEXT NOT NULL,
    action_type_rid TEXT,
    resource_rid TEXT,
    ontology_commit_id TEXT,
    input JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'COMMIT_WRITTEN', 'EVENT_EMITTED', 'SUCCEEDED', 'FAILED')),
    result JSONB,
    correlation_id TEXT,
    submitted_by TEXT,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    writeback_target JSONB,
    writeback_commit_id TEXT,
    action_applied_event_id TEXT,
    action_applied_seq BIGINT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_action_logs_db_status
    ON spice_action_logs.ontology_action_logs(db_name, status, submitted_at DESC);

CREATE INDEX IF NOT EXISTS idx_action_logs_status_updated
    ON spice_action_logs.ontology_action_logs(status, updated_at DESC);

CREATE TABLE IF NOT EXISTS spice_action_logs.ontology_action_dependencies (
    child_action_log_id UUID NOT NULL,
    parent_action_log_id UUID NOT NULL,
    trigger_on TEXT NOT NULL CHECK (trigger_on IN ('SUCCEEDED', 'FAILED', 'COMPLETED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (child_action_log_id, parent_action_log_id),
    CONSTRAINT fk_action_dependency_child
        FOREIGN KEY (child_action_log_id)
        REFERENCES spice_action_logs.ontology_action_logs(action_log_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_action_dependency_parent
        FOREIGN KEY (parent_action_log_id)
        REFERENCES spice_action_logs.ontology_action_logs(action_log_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_action_dependencies_parent
    ON spice_action_logs.ontology_action_dependencies(parent_action_log_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_action_dependencies_child
    ON spice_action_logs.ontology_action_dependencies(child_action_log_id, created_at DESC);

ALTER TABLE spice_action_logs.ontology_action_logs
    ADD COLUMN IF NOT EXISTS action_type_id TEXT,
    ADD COLUMN IF NOT EXISTS db_name TEXT,
    ADD COLUMN IF NOT EXISTS action_applied_event_id TEXT,
    ADD COLUMN IF NOT EXISTS action_applied_seq BIGINT,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

ALTER TABLE spice_action_logs.ontology_action_logs
    ALTER COLUMN updated_at SET DEFAULT NOW();

UPDATE spice_action_logs.ontology_action_logs
SET updated_at = COALESCE(updated_at, submitted_at, NOW())
WHERE updated_at IS NULL;

ALTER TABLE spice_action_logs.ontology_action_logs
    ALTER COLUMN updated_at SET NOT NULL;
