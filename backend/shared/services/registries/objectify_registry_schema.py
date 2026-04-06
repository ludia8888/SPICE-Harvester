from __future__ import annotations

import asyncpg


async def ensure_objectify_registry_schema(conn: asyncpg.Connection, *, schema: str) -> None:
    statements = (
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.ontology_mapping_specs (
            mapping_spec_id UUID PRIMARY KEY,
            dataset_id UUID NOT NULL,
            dataset_branch TEXT NOT NULL DEFAULT 'main',
            artifact_output_name TEXT,
            schema_hash TEXT,
            backing_datasource_id UUID,
            backing_datasource_version_id UUID,
            target_class_id TEXT NOT NULL,
            mappings JSONB NOT NULL DEFAULT '[]'::jsonb,
            target_field_types JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            status TEXT NOT NULL DEFAULT 'ACTIVE',
            version INTEGER NOT NULL DEFAULT 1,
            auto_sync BOOLEAN NOT NULL DEFAULT true,
            options JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_mapping_specs_dataset
        ON {schema}.ontology_mapping_specs(dataset_id, dataset_branch, target_class_id)
        """,
        f"ALTER TABLE {schema}.ontology_mapping_specs ADD COLUMN IF NOT EXISTS artifact_output_name TEXT",
        f"ALTER TABLE {schema}.ontology_mapping_specs ADD COLUMN IF NOT EXISTS schema_hash TEXT",
        f"""
        CREATE INDEX IF NOT EXISTS idx_mapping_specs_output
        ON {schema}.ontology_mapping_specs(dataset_id, dataset_branch, artifact_output_name, schema_hash, status)
        """,
        f"ALTER TABLE {schema}.ontology_mapping_specs ADD COLUMN IF NOT EXISTS backing_datasource_id UUID",
        f"ALTER TABLE {schema}.ontology_mapping_specs ADD COLUMN IF NOT EXISTS backing_datasource_version_id UUID",
        f"ALTER TABLE {schema}.ontology_mapping_specs ADD COLUMN IF NOT EXISTS occ_version INT DEFAULT 1 NOT NULL",
        f"""
        CREATE INDEX IF NOT EXISTS idx_mapping_specs_occ_version
        ON {schema}.ontology_mapping_specs(mapping_spec_id, occ_version)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.objectify_jobs (
            job_id UUID PRIMARY KEY,
            mapping_spec_id UUID,
            mapping_spec_version INTEGER,
            dedupe_key TEXT,
            dataset_id UUID NOT NULL,
            dataset_version_id UUID,
            artifact_id UUID,
            artifact_output_name TEXT,
            dataset_branch TEXT NOT NULL DEFAULT 'main',
            target_class_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'QUEUED',
            command_id TEXT,
            error TEXT,
            report JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            version INT NOT NULL DEFAULT 1,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            completed_at TIMESTAMPTZ
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.objectify_job_outbox (
            outbox_id UUID PRIMARY KEY,
            job_id UUID NOT NULL,
            payload JSONB NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            publish_attempts INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            claimed_by TEXT,
            claimed_at TIMESTAMPTZ,
            next_attempt_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            FOREIGN KEY (job_id)
                REFERENCES {schema}.objectify_jobs(job_id)
                ON DELETE CASCADE
        )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_objectify_outbox_status
        ON {schema}.objectify_job_outbox(status, next_attempt_at, created_at)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_objectify_outbox_claimed
        ON {schema}.objectify_job_outbox(status, claimed_at)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_objectify_outbox_job
        ON {schema}.objectify_job_outbox(job_id, status, created_at)
        """,
        f"ALTER TABLE {schema}.objectify_job_outbox ADD COLUMN IF NOT EXISTS claimed_by TEXT",
        f"ALTER TABLE {schema}.objectify_job_outbox ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ",
        f"ALTER TABLE {schema}.objectify_jobs ADD COLUMN IF NOT EXISTS artifact_id UUID",
        f"ALTER TABLE {schema}.objectify_jobs ADD COLUMN IF NOT EXISTS artifact_output_name TEXT",
        f"ALTER TABLE {schema}.objectify_jobs ADD COLUMN IF NOT EXISTS dedupe_key TEXT",
        f"ALTER TABLE {schema}.objectify_jobs ADD COLUMN IF NOT EXISTS version INT DEFAULT 1 NOT NULL",
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_objectify_jobs_dedupe_key
        ON {schema}.objectify_jobs(dedupe_key)
        WHERE dedupe_key IS NOT NULL
        """,
        f"""
        UPDATE {schema}.objectify_jobs
        SET dedupe_key = COALESCE(dedupe_key, job_id::text)
        WHERE dedupe_key IS NULL
        """,
        f"ALTER TABLE {schema}.objectify_jobs ALTER COLUMN dataset_version_id DROP NOT NULL",
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'objectify_jobs_input_xor'
                  AND conrelid = '{schema}.objectify_jobs'::regclass
            ) THEN
                ALTER TABLE {schema}.objectify_jobs
                ADD CONSTRAINT objectify_jobs_input_xor
                CHECK (
                    (dataset_version_id IS NOT NULL AND artifact_id IS NULL)
                    OR (dataset_version_id IS NULL AND artifact_id IS NOT NULL)
                );
            END IF;
        END $$;
        """,
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'objectify_jobs_artifact_output_required'
                  AND conrelid = '{schema}.objectify_jobs'::regclass
            ) THEN
                ALTER TABLE {schema}.objectify_jobs
                ADD CONSTRAINT objectify_jobs_artifact_output_required
                CHECK (
                    artifact_id IS NULL
                    OR (artifact_output_name IS NOT NULL AND length(trim(artifact_output_name)) > 0)
                );
            END IF;
        END $$;
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_objectify_jobs_status
        ON {schema}.objectify_jobs(status, created_at)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_objectify_jobs_version
        ON {schema}.objectify_jobs(dataset_version_id, mapping_spec_id, mapping_spec_version, status)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_objectify_jobs_artifact
        ON {schema}.objectify_jobs(artifact_id, artifact_output_name, mapping_spec_id, mapping_spec_version, status)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.objectify_watermarks (
            watermark_id UUID PRIMARY KEY,
            mapping_spec_id UUID,
            target_class_id TEXT,
            dataset_branch TEXT NOT NULL DEFAULT 'main',
            watermark_column TEXT NOT NULL,
            watermark_value TEXT NOT NULL,
            dataset_version_id UUID,
            lakefs_commit_id TEXT,
            rows_processed BIGINT NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        f"ALTER TABLE {schema}.objectify_watermarks ADD COLUMN IF NOT EXISTS target_class_id TEXT",
        f"ALTER TABLE {schema}.objectify_watermarks ADD COLUMN IF NOT EXISTS dataset_version_id UUID",
        f"ALTER TABLE {schema}.objectify_watermarks ADD COLUMN IF NOT EXISTS lakefs_commit_id TEXT",
        f"ALTER TABLE {schema}.objectify_watermarks ADD COLUMN IF NOT EXISTS rows_processed BIGINT NOT NULL DEFAULT 0",
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_objectify_watermarks_mapping
        ON {schema}.objectify_watermarks(mapping_spec_id, dataset_branch)
        WHERE mapping_spec_id IS NOT NULL
        """,
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_objectify_watermarks_target
        ON {schema}.objectify_watermarks(target_class_id, dataset_branch)
        WHERE target_class_id IS NOT NULL
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_objectify_watermarks_updated
        ON {schema}.objectify_watermarks(updated_at DESC)
        """,
    )
    for sql in statements:
        await conn.execute(sql)
