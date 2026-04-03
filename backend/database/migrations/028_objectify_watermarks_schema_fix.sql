-- 028_objectify_watermarks_schema_fix.sql
-- Normalize incremental objectify watermark storage into spice_objectify.
--
-- Historical migrations created/altered objectify_watermarks without schema
-- qualification. Runtime now expects spice_objectify.objectify_watermarks, so we
-- create the canonical table there, migrate legacy rows from public, and retire
-- the misplaced table.

CREATE SCHEMA IF NOT EXISTS spice_objectify;

CREATE TABLE IF NOT EXISTS spice_objectify.objectify_watermarks (
    watermark_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    mapping_spec_id UUID,
    target_class_id TEXT,
    dataset_branch TEXT NOT NULL DEFAULT 'main',
    watermark_column TEXT NOT NULL,
    watermark_value TEXT NOT NULL,
    dataset_version_id UUID,
    lakefs_commit_id TEXT,
    rows_processed BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (mapping_spec_id, dataset_branch)
);

ALTER TABLE spice_objectify.objectify_watermarks
    ADD COLUMN IF NOT EXISTS mapping_spec_id UUID,
    ADD COLUMN IF NOT EXISTS target_class_id TEXT,
    ADD COLUMN IF NOT EXISTS dataset_branch TEXT NOT NULL DEFAULT 'main',
    ADD COLUMN IF NOT EXISTS watermark_column TEXT,
    ADD COLUMN IF NOT EXISTS watermark_value TEXT,
    ADD COLUMN IF NOT EXISTS dataset_version_id UUID,
    ADD COLUMN IF NOT EXISTS lakefs_commit_id TEXT,
    ADD COLUMN IF NOT EXISTS rows_processed BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

DO $$
DECLARE
    legacy_has_target_class BOOLEAN := FALSE;
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'objectify_watermarks'
    ) THEN
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'objectify_watermarks'
              AND column_name = 'target_class_id'
        )
        INTO legacy_has_target_class;

        IF legacy_has_target_class THEN
            EXECUTE $insert$
                INSERT INTO spice_objectify.objectify_watermarks (
                    watermark_id,
                    mapping_spec_id,
                    target_class_id,
                    dataset_branch,
                    watermark_column,
                    watermark_value,
                    dataset_version_id,
                    lakefs_commit_id,
                    rows_processed,
                    created_at,
                    updated_at
                )
                SELECT
                    watermark_id,
                    mapping_spec_id,
                    target_class_id,
                    dataset_branch,
                    watermark_column,
                    watermark_value,
                    dataset_version_id,
                    lakefs_commit_id,
                    rows_processed,
                    created_at,
                    updated_at
                FROM public.objectify_watermarks
                ON CONFLICT (mapping_spec_id, dataset_branch)
                DO UPDATE SET
                    target_class_id = EXCLUDED.target_class_id,
                    watermark_column = EXCLUDED.watermark_column,
                    watermark_value = EXCLUDED.watermark_value,
                    dataset_version_id = EXCLUDED.dataset_version_id,
                    lakefs_commit_id = EXCLUDED.lakefs_commit_id,
                    rows_processed = GREATEST(
                        spice_objectify.objectify_watermarks.rows_processed,
                        EXCLUDED.rows_processed
                    ),
                    created_at = LEAST(
                        spice_objectify.objectify_watermarks.created_at,
                        EXCLUDED.created_at
                    ),
                    updated_at = GREATEST(
                        spice_objectify.objectify_watermarks.updated_at,
                        EXCLUDED.updated_at
                    )
            $insert$;
        ELSE
            EXECUTE $insert$
                INSERT INTO spice_objectify.objectify_watermarks (
                    watermark_id,
                    mapping_spec_id,
                    dataset_branch,
                    watermark_column,
                    watermark_value,
                    dataset_version_id,
                    lakefs_commit_id,
                    rows_processed,
                    created_at,
                    updated_at
                )
                SELECT
                    watermark_id,
                    mapping_spec_id,
                    dataset_branch,
                    watermark_column,
                    watermark_value,
                    dataset_version_id,
                    lakefs_commit_id,
                    rows_processed,
                    created_at,
                    updated_at
                FROM public.objectify_watermarks
                ON CONFLICT (mapping_spec_id, dataset_branch)
                DO UPDATE SET
                    watermark_column = EXCLUDED.watermark_column,
                    watermark_value = EXCLUDED.watermark_value,
                    dataset_version_id = EXCLUDED.dataset_version_id,
                    lakefs_commit_id = EXCLUDED.lakefs_commit_id,
                    rows_processed = GREATEST(
                        spice_objectify.objectify_watermarks.rows_processed,
                        EXCLUDED.rows_processed
                    ),
                    created_at = LEAST(
                        spice_objectify.objectify_watermarks.created_at,
                        EXCLUDED.created_at
                    ),
                    updated_at = GREATEST(
                        spice_objectify.objectify_watermarks.updated_at,
                        EXCLUDED.updated_at
                    )
            $insert$;
        END IF;

        DROP TABLE public.objectify_watermarks;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_objectify_watermarks_mapping_spec
    ON spice_objectify.objectify_watermarks(mapping_spec_id);

CREATE INDEX IF NOT EXISTS idx_objectify_watermarks_class_id
    ON spice_objectify.objectify_watermarks(target_class_id, dataset_branch)
    WHERE target_class_id IS NOT NULL;

COMMENT ON COLUMN spice_objectify.objectify_watermarks.target_class_id IS
    'Alternative key for OMS-backed watermarks (used when mapping_spec_id is NULL)';
