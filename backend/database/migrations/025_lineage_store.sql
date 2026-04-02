-- Lineage store schema bootstrap.
-- Runtime DDL should only be needed in dev/test after this migration lands.

CREATE SCHEMA IF NOT EXISTS spice_lineage;

CREATE TABLE IF NOT EXISTS spice_lineage.lineage_nodes (
    node_id TEXT PRIMARY KEY,
    node_type TEXT NOT NULL,
    label TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_id TEXT,
    aggregate_type TEXT,
    aggregate_id TEXT,
    artifact_kind TEXT,
    artifact_key TEXT,
    db_name TEXT,
    branch TEXT,
    run_id TEXT,
    code_sha TEXT,
    schema_version TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS spice_lineage.lineage_edges (
    edge_id UUID PRIMARY KEY,
    from_node_id TEXT NOT NULL,
    to_node_id TEXT NOT NULL,
    edge_type TEXT NOT NULL,
    projection_name TEXT NOT NULL DEFAULT '',
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    db_name TEXT,
    branch TEXT,
    run_id TEXT,
    code_sha TEXT,
    schema_version TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS spice_lineage.lineage_backfill_queue (
    backfill_id UUID PRIMARY KEY,
    event_id TEXT NOT NULL,
    s3_bucket TEXT,
    s3_key TEXT,
    db_name TEXT,
    branch TEXT,
    occurred_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE spice_lineage.lineage_backfill_queue
    ADD COLUMN IF NOT EXISTS branch TEXT;

ALTER TABLE spice_lineage.lineage_nodes
    ADD COLUMN IF NOT EXISTS recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS event_id TEXT,
    ADD COLUMN IF NOT EXISTS aggregate_type TEXT,
    ADD COLUMN IF NOT EXISTS aggregate_id TEXT,
    ADD COLUMN IF NOT EXISTS artifact_kind TEXT,
    ADD COLUMN IF NOT EXISTS artifact_key TEXT,
    ADD COLUMN IF NOT EXISTS db_name TEXT,
    ADD COLUMN IF NOT EXISTS branch TEXT,
    ADD COLUMN IF NOT EXISTS run_id TEXT,
    ADD COLUMN IF NOT EXISTS code_sha TEXT,
    ADD COLUMN IF NOT EXISTS schema_version TEXT;

ALTER TABLE spice_lineage.lineage_edges
    ADD COLUMN IF NOT EXISTS projection_name TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS db_name TEXT,
    ADD COLUMN IF NOT EXISTS branch TEXT,
    ADD COLUMN IF NOT EXISTS run_id TEXT,
    ADD COLUMN IF NOT EXISTS code_sha TEXT,
    ADD COLUMN IF NOT EXISTS schema_version TEXT;

UPDATE spice_lineage.lineage_edges
SET db_name = COALESCE(db_name, metadata->>'db_name')
WHERE db_name IS NULL AND (metadata ? 'db_name');

UPDATE spice_lineage.lineage_nodes
SET db_name = COALESCE(db_name, metadata->>'db_name')
WHERE db_name IS NULL AND (metadata ? 'db_name');

UPDATE spice_lineage.lineage_edges
SET branch = COALESCE(branch, metadata->>'branch')
WHERE branch IS NULL AND (metadata ? 'branch');

UPDATE spice_lineage.lineage_nodes
SET branch = COALESCE(branch, metadata->>'branch')
WHERE branch IS NULL AND (metadata ? 'branch');

WITH ranked AS (
    SELECT edge_id,
           ROW_NUMBER() OVER (
               PARTITION BY from_node_id, to_node_id, edge_type, projection_name
               ORDER BY recorded_at ASC, edge_id ASC
           ) AS rn
    FROM spice_lineage.lineage_edges
)
DELETE FROM spice_lineage.lineage_edges
WHERE edge_id IN (SELECT edge_id FROM ranked WHERE rn > 1);

CREATE UNIQUE INDEX IF NOT EXISTS idx_lineage_backfill_event_unique
    ON spice_lineage.lineage_backfill_queue(event_id);

CREATE INDEX IF NOT EXISTS idx_lineage_backfill_status_time
    ON spice_lineage.lineage_backfill_queue(status, created_at);

CREATE INDEX IF NOT EXISTS idx_lineage_backfill_db_branch_status
    ON spice_lineage.lineage_backfill_queue(db_name, branch, status, created_at);

CREATE INDEX IF NOT EXISTS idx_lineage_edges_from
    ON spice_lineage.lineage_edges(from_node_id);

CREATE INDEX IF NOT EXISTS idx_lineage_edges_to
    ON spice_lineage.lineage_edges(to_node_id);

CREATE INDEX IF NOT EXISTS idx_lineage_edges_to_time
    ON spice_lineage.lineage_edges(to_node_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_lineage_edges_type
    ON spice_lineage.lineage_edges(edge_type);

CREATE UNIQUE INDEX IF NOT EXISTS idx_lineage_edges_unique
    ON spice_lineage.lineage_edges(from_node_id, to_node_id, edge_type, projection_name);

CREATE INDEX IF NOT EXISTS idx_lineage_edges_from_type
    ON spice_lineage.lineage_edges(from_node_id, edge_type);

CREATE INDEX IF NOT EXISTS idx_lineage_edges_to_type
    ON spice_lineage.lineage_edges(to_node_id, edge_type);

CREATE INDEX IF NOT EXISTS idx_lineage_edges_db_time
    ON spice_lineage.lineage_edges(db_name, occurred_at);

CREATE INDEX IF NOT EXISTS idx_lineage_edges_db_branch_time
    ON spice_lineage.lineage_edges(db_name, branch, occurred_at);

CREATE INDEX IF NOT EXISTS idx_lineage_edges_projection_time
    ON spice_lineage.lineage_edges(projection_name, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_lineage_edges_run_id_time
    ON spice_lineage.lineage_edges(run_id, occurred_at);

CREATE INDEX IF NOT EXISTS idx_lineage_edges_meta_producer_run_time
    ON spice_lineage.lineage_edges((metadata->>'producer_run_id'), occurred_at);

CREATE INDEX IF NOT EXISTS idx_lineage_edges_meta_run_time
    ON spice_lineage.lineage_edges((metadata->>'run_id'), occurred_at);

CREATE INDEX IF NOT EXISTS idx_lineage_nodes_event_id
    ON spice_lineage.lineage_nodes(event_id);

CREATE INDEX IF NOT EXISTS idx_lineage_nodes_aggregate
    ON spice_lineage.lineage_nodes(aggregate_type, aggregate_id);

CREATE INDEX IF NOT EXISTS idx_lineage_nodes_artifact
    ON spice_lineage.lineage_nodes(artifact_kind, artifact_key);

CREATE INDEX IF NOT EXISTS idx_lineage_nodes_db
    ON spice_lineage.lineage_nodes(db_name);

CREATE INDEX IF NOT EXISTS idx_lineage_nodes_db_branch
    ON spice_lineage.lineage_nodes(db_name, branch);
