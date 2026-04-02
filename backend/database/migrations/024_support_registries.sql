-- Support registry schema bootstrap for dataset profiles, pipeline plans, and objectify.
-- Runtime DDL should only be needed in dev/test after this migration lands.

CREATE SCHEMA IF NOT EXISTS spice_datasets;
CREATE SCHEMA IF NOT EXISTS spice_pipelines;
CREATE SCHEMA IF NOT EXISTS spice_objectify;

CREATE TABLE IF NOT EXISTS spice_datasets.dataset_profiles (
    profile_id UUID PRIMARY KEY,
    dataset_id UUID NOT NULL,
    dataset_version_id UUID,
    db_name TEXT NOT NULL,
    branch TEXT,
    schema_hash TEXT,
    profile JSONB NOT NULL DEFAULT '{}'::jsonb,
    profile_digest TEXT NOT NULL DEFAULT '',
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dataset_profiles_dataset
    ON spice_datasets.dataset_profiles(dataset_id, computed_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dataset_profiles_version
    ON spice_datasets.dataset_profiles(dataset_version_id);

CREATE INDEX IF NOT EXISTS idx_dataset_profiles_db
    ON spice_datasets.dataset_profiles(db_name, computed_at DESC);

CREATE TABLE IF NOT EXISTS spice_pipelines.pipeline_plans (
    plan_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    status TEXT NOT NULL DEFAULT 'COMPILED',
    goal TEXT NOT NULL DEFAULT '',
    db_name TEXT,
    branch TEXT,
    plan JSONB NOT NULL DEFAULT '{}'::jsonb,
    plan_digest TEXT NOT NULL DEFAULT '',
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_plans_status
    ON spice_pipelines.pipeline_plans(status);

CREATE INDEX IF NOT EXISTS idx_pipeline_plans_tenant
    ON spice_pipelines.pipeline_plans(tenant_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_plans_db
    ON spice_pipelines.pipeline_plans(db_name);

CREATE TABLE IF NOT EXISTS spice_objectify.ontology_mapping_specs (
    mapping_spec_id UUID PRIMARY KEY,
    dataset_id UUID NOT NULL,
    dataset_branch TEXT NOT NULL DEFAULT 'main',
    artifact_output_name TEXT,
    schema_hash TEXT,
    backing_datasource_id UUID,
    backing_datasource_version_id UUID,
    target_class_id TEXT NOT NULL,
    mappings JSONB NOT NULL DEFAULT '[]'::jsonb,
    target_field_types JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    version INTEGER NOT NULL DEFAULT 1,
    occ_version INTEGER NOT NULL DEFAULT 1,
    auto_sync BOOLEAN NOT NULL DEFAULT true,
    options JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE spice_objectify.ontology_mapping_specs
    ADD COLUMN IF NOT EXISTS artifact_output_name TEXT,
    ADD COLUMN IF NOT EXISTS schema_hash TEXT,
    ADD COLUMN IF NOT EXISTS backing_datasource_id UUID,
    ADD COLUMN IF NOT EXISTS backing_datasource_version_id UUID,
    ADD COLUMN IF NOT EXISTS occ_version INT DEFAULT 1 NOT NULL;

CREATE INDEX IF NOT EXISTS idx_mapping_specs_dataset
    ON spice_objectify.ontology_mapping_specs(dataset_id, dataset_branch, target_class_id);

CREATE INDEX IF NOT EXISTS idx_mapping_specs_output
    ON spice_objectify.ontology_mapping_specs(dataset_id, dataset_branch, artifact_output_name, schema_hash, status);

CREATE INDEX IF NOT EXISTS idx_mapping_specs_occ_version
    ON spice_objectify.ontology_mapping_specs(mapping_spec_id, occ_version);

CREATE TABLE IF NOT EXISTS spice_objectify.objectify_jobs (
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
    report JSONB NOT NULL DEFAULT '{}'::jsonb,
    version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

ALTER TABLE spice_objectify.objectify_jobs
    ADD COLUMN IF NOT EXISTS artifact_id UUID,
    ADD COLUMN IF NOT EXISTS artifact_output_name TEXT,
    ADD COLUMN IF NOT EXISTS dedupe_key TEXT,
    ADD COLUMN IF NOT EXISTS version INT DEFAULT 1 NOT NULL;

ALTER TABLE spice_objectify.objectify_jobs
    ALTER COLUMN dataset_version_id DROP NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'objectify_jobs_input_xor'
          AND conrelid = 'spice_objectify.objectify_jobs'::regclass
    ) THEN
        ALTER TABLE spice_objectify.objectify_jobs
        ADD CONSTRAINT objectify_jobs_input_xor
        CHECK (
            (dataset_version_id IS NOT NULL AND artifact_id IS NULL)
            OR (dataset_version_id IS NULL AND artifact_id IS NOT NULL)
        );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'objectify_jobs_artifact_output_required'
          AND conrelid = 'spice_objectify.objectify_jobs'::regclass
    ) THEN
        ALTER TABLE spice_objectify.objectify_jobs
        ADD CONSTRAINT objectify_jobs_artifact_output_required
        CHECK (
            artifact_id IS NULL
            OR (artifact_output_name IS NOT NULL AND length(trim(artifact_output_name)) > 0)
        );
    END IF;
END $$;

UPDATE spice_objectify.objectify_jobs
SET dedupe_key = COALESCE(dedupe_key, job_id::text)
WHERE dedupe_key IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_objectify_jobs_dedupe_key
    ON spice_objectify.objectify_jobs(dedupe_key)
    WHERE dedupe_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_objectify_jobs_status
    ON spice_objectify.objectify_jobs(status, created_at);

CREATE INDEX IF NOT EXISTS idx_objectify_jobs_version
    ON spice_objectify.objectify_jobs(dataset_version_id, mapping_spec_id, mapping_spec_version, status);

CREATE INDEX IF NOT EXISTS idx_objectify_jobs_artifact
    ON spice_objectify.objectify_jobs(artifact_id, artifact_output_name, mapping_spec_id, mapping_spec_version, status);

CREATE TABLE IF NOT EXISTS spice_objectify.objectify_job_outbox (
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
        REFERENCES spice_objectify.objectify_jobs(job_id)
        ON DELETE CASCADE
);

ALTER TABLE spice_objectify.objectify_job_outbox
    ADD COLUMN IF NOT EXISTS claimed_by TEXT,
    ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_objectify_outbox_status
    ON spice_objectify.objectify_job_outbox(status, next_attempt_at, created_at);

CREATE INDEX IF NOT EXISTS idx_objectify_outbox_claimed
    ON spice_objectify.objectify_job_outbox(status, claimed_at);

CREATE INDEX IF NOT EXISTS idx_objectify_outbox_job
    ON spice_objectify.objectify_job_outbox(job_id, status, created_at);
