-- Migration: 010_occ_version_columns.sql
-- Description: Add version columns for Optimistic Concurrency Control (OCC)
-- Date: 2024-01-28

-- Ensure all referenced schemas exist (keep migration self-contained).
CREATE SCHEMA IF NOT EXISTS spice_core;
CREATE SCHEMA IF NOT EXISTS spice_objectify;
CREATE SCHEMA IF NOT EXISTS spice_datasets;
CREATE SCHEMA IF NOT EXISTS spice_pipelines;
CREATE SCHEMA IF NOT EXISTS spice_connectors;

-- ============================================================
-- 1-6. Add version columns to tables (only if tables exist)
-- ============================================================
DO $$
BEGIN
    -- 1. objectify_jobs
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='spice_objectify' AND table_name='objectify_jobs') THEN
        ALTER TABLE spice_objectify.objectify_jobs ADD COLUMN IF NOT EXISTS version INT DEFAULT 1 NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_objectify_jobs_version ON spice_objectify.objectify_jobs(job_id, version);
    END IF;

    -- 2. ontology_mapping_specs
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='spice_objectify' AND table_name='ontology_mapping_specs') THEN
        ALTER TABLE spice_objectify.ontology_mapping_specs ADD COLUMN IF NOT EXISTS occ_version INT DEFAULT 1 NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_mapping_specs_occ_version ON spice_objectify.ontology_mapping_specs(mapping_spec_id, occ_version);
    END IF;

    -- 3. datasets
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='spice_datasets' AND table_name='datasets') THEN
        ALTER TABLE spice_datasets.datasets ADD COLUMN IF NOT EXISTS version INT DEFAULT 1 NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_datasets_version ON spice_datasets.datasets(dataset_id, version);
    END IF;

    -- 4. dataset_versions
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='spice_datasets' AND table_name='dataset_versions') THEN
        ALTER TABLE spice_datasets.dataset_versions ADD COLUMN IF NOT EXISTS version INT DEFAULT 1 NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_dataset_versions_occ ON spice_datasets.dataset_versions(version_id, version);
    END IF;

    -- 5. pipelines
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='spice_pipelines' AND table_name='pipelines') THEN
        ALTER TABLE spice_pipelines.pipelines ADD COLUMN IF NOT EXISTS version INT DEFAULT 1 NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_pipelines_version ON spice_pipelines.pipelines(pipeline_id, version);
    END IF;

    -- 6. connector_sources
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='spice_connectors' AND table_name='connector_sources') THEN
        ALTER TABLE spice_connectors.connector_sources ADD COLUMN IF NOT EXISTS version INT DEFAULT 1 NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_connector_sources_version ON spice_connectors.connector_sources(source_type, source_id, version);
    END IF;
END $$;

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
