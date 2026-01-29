-- Migration: 010_occ_version_columns.sql
-- Description: Add version columns for Optimistic Concurrency Control (OCC)
-- Date: 2024-01-28

-- Ensure shared/core schema exists (some environments rely on init scripts; keep migration self-contained).
CREATE SCHEMA IF NOT EXISTS spice_core;

-- ============================================================
-- 1. Add version column to objectify_jobs
-- ============================================================
ALTER TABLE spice_objectify.objectify_jobs
ADD COLUMN IF NOT EXISTS version INT DEFAULT 1 NOT NULL;

COMMENT ON COLUMN spice_objectify.objectify_jobs.version IS
    'Optimistic concurrency control version - incremented on each update';

-- Create index for version-based queries
CREATE INDEX IF NOT EXISTS idx_objectify_jobs_version
ON spice_objectify.objectify_jobs(job_id, version);

-- ============================================================
-- 2. Add version column to ontology_mapping_specs
-- ============================================================
ALTER TABLE spice_objectify.ontology_mapping_specs
ADD COLUMN IF NOT EXISTS occ_version INT DEFAULT 1 NOT NULL;

COMMENT ON COLUMN spice_objectify.ontology_mapping_specs.occ_version IS
    'Optimistic concurrency control version - incremented on each update';

-- Create index for version-based queries
CREATE INDEX IF NOT EXISTS idx_mapping_specs_occ_version
ON spice_objectify.ontology_mapping_specs(mapping_spec_id, occ_version);

-- ============================================================
-- 3. Add version column to datasets table
-- ============================================================
ALTER TABLE spice_datasets.datasets
ADD COLUMN IF NOT EXISTS version INT DEFAULT 1 NOT NULL;

COMMENT ON COLUMN spice_datasets.datasets.version IS
    'Optimistic concurrency control version - incremented on each update';

CREATE INDEX IF NOT EXISTS idx_datasets_version
ON spice_datasets.datasets(dataset_id, version);

-- ============================================================
-- 4. Add version column to dataset_versions table
-- ============================================================
ALTER TABLE spice_datasets.dataset_versions
ADD COLUMN IF NOT EXISTS version INT DEFAULT 1 NOT NULL;

COMMENT ON COLUMN spice_datasets.dataset_versions.version IS
    'Optimistic concurrency control version - incremented on each update';

CREATE INDEX IF NOT EXISTS idx_dataset_versions_occ
ON spice_datasets.dataset_versions(version_id, version);

-- ============================================================
-- 5. Add version column to pipelines table
-- ============================================================
ALTER TABLE spice_pipelines.pipelines
ADD COLUMN IF NOT EXISTS version INT DEFAULT 1 NOT NULL;

COMMENT ON COLUMN spice_pipelines.pipelines.version IS
    'Optimistic concurrency control version - incremented on each update';

CREATE INDEX IF NOT EXISTS idx_pipelines_version
ON spice_pipelines.pipelines(pipeline_id, version);

-- ============================================================
-- 6. Add version column to connector_sources table
-- ============================================================
ALTER TABLE spice_connectors.connector_sources
ADD COLUMN IF NOT EXISTS version INT DEFAULT 1 NOT NULL;

COMMENT ON COLUMN spice_connectors.connector_sources.version IS
    'Optimistic concurrency control version - incremented on each update';

CREATE INDEX IF NOT EXISTS idx_connector_sources_version
ON spice_connectors.connector_sources(source_type, source_id, version);

-- ============================================================
-- 7. Create helper function for OCC validation
-- ============================================================
CREATE OR REPLACE FUNCTION spice_core.check_occ_version(
    p_current_version INT,
    p_expected_version INT
) RETURNS BOOLEAN AS $$
BEGIN
    IF p_expected_version IS NULL THEN
        -- No OCC check requested
        RETURN TRUE;
    END IF;
    RETURN p_current_version = p_expected_version;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION spice_core.check_occ_version IS
    'Helper function for optimistic concurrency control validation';

-- ============================================================
-- 8. Create custom exception type for OCC conflicts
-- ============================================================
DO $$
BEGIN
    -- Create domain for OCC error if not exists
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'occ_conflict_info') THEN
        CREATE TYPE spice_core.occ_conflict_info AS (
            table_name TEXT,
            record_id TEXT,
            expected_version INT,
            actual_version INT
        );
    END IF;
END $$;
