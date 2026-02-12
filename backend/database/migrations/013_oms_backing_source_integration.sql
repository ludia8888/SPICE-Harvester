-- 013_oms_backing_source_integration.sql
-- Foundry-style backing datasource integration: OMS becomes the primary source
-- of truth for property_mappings. PostgreSQL MappingSpec is soft-deprecated.

-- ============================================================================
-- 1. Make objectify_jobs.mapping_spec_id nullable (OMS-backed jobs have no PG spec)
-- ============================================================================
ALTER TABLE IF EXISTS objectify_jobs
    ALTER COLUMN mapping_spec_id DROP NOT NULL;

ALTER TABLE IF EXISTS objectify_jobs
    ALTER COLUMN mapping_spec_version DROP NOT NULL;

-- Drop FK constraint so OMS-backed jobs (mapping_spec_id=NULL) don't violate it
DO $$
BEGIN
    ALTER TABLE objectify_jobs
        DROP CONSTRAINT IF EXISTS objectify_jobs_mapping_spec_id_fkey;
EXCEPTION WHEN undefined_table THEN
    NULL;
END $$;

-- ============================================================================
-- 2. Add deprecation marker to ontology_mapping_specs
-- ============================================================================
ALTER TABLE IF EXISTS ontology_mapping_specs
    ADD COLUMN IF NOT EXISTS deprecated_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS oms_synced BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN ontology_mapping_specs.deprecated_at IS
    'Timestamp when this PG-based mapping spec was superseded by OMS backing_source';
COMMENT ON COLUMN ontology_mapping_specs.oms_synced IS
    'Whether this mapping spec has been synced to OMS backing_source.property_mappings';

-- ============================================================================
-- 3. Objectify changelog: make mapping_spec_id nullable
-- ============================================================================
ALTER TABLE IF EXISTS objectify_changelog
    ALTER COLUMN mapping_spec_id DROP NOT NULL;

-- ============================================================================
-- 4. Objectify watermarks: add target_class_id as alternative key
-- ============================================================================
ALTER TABLE IF EXISTS objectify_watermarks
    ADD COLUMN IF NOT EXISTS target_class_id TEXT;

CREATE INDEX IF NOT EXISTS idx_objectify_watermarks_class_id
    ON objectify_watermarks (target_class_id, dataset_branch)
    WHERE target_class_id IS NOT NULL;

COMMENT ON COLUMN objectify_watermarks.target_class_id IS
    'Alternative key for OMS-backed watermarks (used when mapping_spec_id is NULL)';
