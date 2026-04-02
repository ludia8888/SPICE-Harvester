-- Processed event registry schema bootstrap.
-- Runtime DDL should only be needed in dev/test after this migration lands.

CREATE SCHEMA IF NOT EXISTS spice_event_registry;

CREATE TABLE IF NOT EXISTS spice_event_registry.processed_events (
    handler TEXT NOT NULL,
    event_id TEXT NOT NULL,
    aggregate_id TEXT,
    sequence_number BIGINT,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    owner TEXT,
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 1,
    last_error TEXT,
    PRIMARY KEY (handler, event_id)
);

ALTER TABLE spice_event_registry.processed_events
    ADD COLUMN IF NOT EXISTS owner TEXT,
    ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ;

ALTER TABLE spice_event_registry.processed_events
    ALTER COLUMN heartbeat_at SET DEFAULT NOW();

UPDATE spice_event_registry.processed_events
SET heartbeat_at = COALESCE(heartbeat_at, started_at)
WHERE heartbeat_at IS NULL;

ALTER TABLE spice_event_registry.processed_events
    ALTER COLUMN heartbeat_at SET NOT NULL;

CREATE TABLE IF NOT EXISTS spice_event_registry.aggregate_versions (
    handler TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    last_sequence BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (handler, aggregate_id)
);

CREATE INDEX IF NOT EXISTS idx_processed_events_aggregate
    ON spice_event_registry.processed_events(handler, aggregate_id);

CREATE INDEX IF NOT EXISTS idx_processed_events_status
    ON spice_event_registry.processed_events(status);

CREATE INDEX IF NOT EXISTS idx_processed_events_lease
    ON spice_event_registry.processed_events(handler, status, heartbeat_at)
    WHERE status = 'processing';
