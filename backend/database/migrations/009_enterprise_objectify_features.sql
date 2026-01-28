-- Enterprise Objectify Features Migration
-- Adds tables for: FK detection, objectify watermarks, schema drift history, schema subscriptions

-- ============================================================
-- 1. FK Detection Results
-- ============================================================
CREATE TABLE IF NOT EXISTS fk_detection_results (
    detection_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID NOT NULL,
    dataset_version_id UUID,
    db_name TEXT NOT NULL,
    patterns JSONB NOT NULL DEFAULT '[]'::jsonb,
    detection_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reviewed_at TIMESTAMPTZ,
    reviewed_by TEXT
);

DO $$
BEGIN
    ALTER TABLE fk_detection_results
        ADD CONSTRAINT fk_detection_status_check
        CHECK (status IN ('PENDING', 'REVIEWED', 'APPLIED', 'DISMISSED'));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_fk_detection_dataset
    ON fk_detection_results(dataset_id, status);

CREATE INDEX IF NOT EXISTS idx_fk_detection_db_name
    ON fk_detection_results(db_name, created_at DESC);

-- ============================================================
-- 2. Objectify Watermarks (for incremental processing)
-- ============================================================
CREATE TABLE IF NOT EXISTS objectify_watermarks (
    watermark_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    mapping_spec_id UUID NOT NULL,
    dataset_branch TEXT NOT NULL DEFAULT 'main',
    watermark_column TEXT NOT NULL,
    watermark_value TEXT NOT NULL,
    dataset_version_id UUID,
    lakefs_commit_id TEXT,
    rows_processed BIGINT DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (mapping_spec_id, dataset_branch)
);

CREATE INDEX IF NOT EXISTS idx_objectify_watermarks_mapping_spec
    ON objectify_watermarks(mapping_spec_id);

-- ============================================================
-- 3. Schema Drift History
-- ============================================================
CREATE TABLE IF NOT EXISTS schema_drift_history (
    drift_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subject_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    db_name TEXT NOT NULL,
    previous_hash TEXT,
    current_hash TEXT NOT NULL,
    drift_type TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'info',
    changes JSONB NOT NULL DEFAULT '[]'::jsonb,
    impacted_mappings JSONB NOT NULL DEFAULT '[]'::jsonb,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acknowledged_at TIMESTAMPTZ,
    acknowledged_by TEXT
);

DO $$
BEGIN
    ALTER TABLE schema_drift_history
        ADD CONSTRAINT schema_drift_subject_type_check
        CHECK (subject_type IN ('dataset', 'mapping_spec', 'object_type'));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE schema_drift_history
        ADD CONSTRAINT schema_drift_severity_check
        CHECK (severity IN ('info', 'warning', 'breaking'));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_schema_drift_subject
    ON schema_drift_history(subject_type, subject_id, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_schema_drift_severity
    ON schema_drift_history(severity, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_schema_drift_db
    ON schema_drift_history(db_name, detected_at DESC);

-- ============================================================
-- 4. Schema Subscriptions (for notifications)
-- ============================================================
CREATE TABLE IF NOT EXISTS schema_subscriptions (
    subscription_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id TEXT NOT NULL,
    subject_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    db_name TEXT NOT NULL,
    severity_filter TEXT[] DEFAULT ARRAY['warning', 'breaking'],
    notification_channels TEXT[] DEFAULT ARRAY['websocket'],
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, subject_type, subject_id)
);

DO $$
BEGIN
    ALTER TABLE schema_subscriptions
        ADD CONSTRAINT schema_subscriptions_status_check
        CHECK (status IN ('ACTIVE', 'PAUSED', 'DELETED'));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_schema_subscriptions_user
    ON schema_subscriptions(user_id, status);

CREATE INDEX IF NOT EXISTS idx_schema_subscriptions_subject
    ON schema_subscriptions(subject_type, subject_id);

-- ============================================================
-- 5. Add columns to ontology_mapping_specs (if table exists)
-- ============================================================
DO $$
BEGIN
    -- Add execution_mode column
    ALTER TABLE ontology_mapping_specs
        ADD COLUMN IF NOT EXISTS execution_mode TEXT DEFAULT 'full';
EXCEPTION
    WHEN undefined_table THEN NULL;
END $$;

DO $$
BEGIN
    -- Add watermark_column column
    ALTER TABLE ontology_mapping_specs
        ADD COLUMN IF NOT EXISTS watermark_column TEXT;
EXCEPTION
    WHEN undefined_table THEN NULL;
END $$;

DO $$
BEGIN
    -- Add last_watermark_value column
    ALTER TABLE ontology_mapping_specs
        ADD COLUMN IF NOT EXISTS last_watermark_value TEXT;
EXCEPTION
    WHEN undefined_table THEN NULL;
END $$;

DO $$
BEGIN
    -- Add last_successful_commit column
    ALTER TABLE ontology_mapping_specs
        ADD COLUMN IF NOT EXISTS last_successful_commit TEXT;
EXCEPTION
    WHEN undefined_table THEN NULL;
END $$;

DO $$
BEGIN
    -- Add expected_schema_hash column
    ALTER TABLE ontology_mapping_specs
        ADD COLUMN IF NOT EXISTS expected_schema_hash TEXT;
EXCEPTION
    WHEN undefined_table THEN NULL;
END $$;

-- Add constraint for execution_mode
DO $$
BEGIN
    ALTER TABLE ontology_mapping_specs
        ADD CONSTRAINT ontology_mapping_specs_execution_mode_check
        CHECK (execution_mode IN ('full', 'incremental', 'delta'));
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN undefined_table THEN NULL;
END $$;
