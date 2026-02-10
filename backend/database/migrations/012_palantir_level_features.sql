-- 012_palantir_level_features.sql
-- Palantir Foundry-level features: changelog storage, projection positions

-- ============================================================================
-- Objectify Changelog: stores delta summaries for each objectify job execution
-- ============================================================================
CREATE TABLE IF NOT EXISTS objectify_changelog (
    changelog_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          TEXT NOT NULL,
    mapping_spec_id UUID,
    db_name         TEXT NOT NULL,
    branch          TEXT NOT NULL DEFAULT 'main',
    target_class_id TEXT NOT NULL,
    execution_mode  TEXT NOT NULL DEFAULT 'full',
    added_count     INT NOT NULL DEFAULT 0,
    modified_count  INT NOT NULL DEFAULT 0,
    deleted_count   INT NOT NULL DEFAULT 0,
    total_instances INT NOT NULL DEFAULT 0,
    delta_summary   JSONB,
    delta_s3_key    TEXT,
    lakefs_base_commit  TEXT,
    lakefs_target_commit TEXT,
    watermark_before TEXT,
    watermark_after  TEXT,
    duration_ms     BIGINT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_changelog_db_name ON objectify_changelog (db_name, branch, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_changelog_job_id ON objectify_changelog (job_id);
CREATE INDEX IF NOT EXISTS idx_changelog_class ON objectify_changelog (db_name, target_class_id);

-- ============================================================================
-- Projection Positions: tracks last processed event for each projection
-- ============================================================================
CREATE TABLE IF NOT EXISTS projection_positions (
    projection_name TEXT NOT NULL,
    db_name         TEXT NOT NULL,
    branch          TEXT NOT NULL DEFAULT 'main',
    last_sequence   BIGINT NOT NULL DEFAULT 0,
    last_event_id   UUID,
    last_job_id     TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (projection_name, db_name, branch)
);
