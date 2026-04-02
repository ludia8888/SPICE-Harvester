-- Pipeline registry schema bootstrap.
-- Runtime DDL should only be needed in dev/test after this migration lands.

CREATE SCHEMA IF NOT EXISTS spice_pipelines;

CREATE TABLE IF NOT EXISTS spice_pipelines.lakefs_credentials (
    principal_type TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    access_key_id TEXT NOT NULL,
    secret_access_key_enc TEXT NOT NULL,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (principal_type, principal_id)
);

DO $$
BEGIN
    ALTER TABLE spice_pipelines.lakefs_credentials
        ADD CONSTRAINT lakefs_credentials_principal_type_check
        CHECK (principal_type IN ('user', 'service'));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS spice_pipelines.pipelines (
    pipeline_id UUID PRIMARY KEY,
    db_name TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    pipeline_type TEXT NOT NULL,
    location TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    branch TEXT NOT NULL DEFAULT 'main',
    lakefs_repository TEXT,
    proposal_status TEXT,
    proposal_title TEXT,
    proposal_description TEXT,
    proposal_submitted_at TIMESTAMPTZ,
    proposal_reviewed_at TIMESTAMPTZ,
    proposal_review_comment TEXT,
    proposal_bundle JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_preview_status TEXT,
    last_preview_at TIMESTAMPTZ,
    last_preview_rows INTEGER,
    last_preview_job_id TEXT,
    last_preview_node_id TEXT,
    last_preview_sample JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_preview_nodes JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_build_status TEXT,
    last_build_at TIMESTAMPTZ,
    last_build_output JSONB NOT NULL DEFAULT '{}'::jsonb,
    deployed_at TIMESTAMPTZ,
    deployed_commit_id TEXT,
    schedule_interval_seconds INTEGER,
    schedule_cron TEXT,
    last_scheduled_at TIMESTAMPTZ,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (db_name, name, branch)
);

CREATE TABLE IF NOT EXISTS spice_pipelines.pipeline_branches (
    db_name TEXT NOT NULL,
    branch TEXT NOT NULL,
    archived BOOLEAN NOT NULL DEFAULT FALSE,
    archived_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (db_name, branch)
);

ALTER TABLE spice_pipelines.pipelines
    ADD COLUMN IF NOT EXISTS branch TEXT NOT NULL DEFAULT 'main',
    ADD COLUMN IF NOT EXISTS lakefs_repository TEXT,
    ADD COLUMN IF NOT EXISTS proposal_status TEXT,
    ADD COLUMN IF NOT EXISTS proposal_title TEXT,
    ADD COLUMN IF NOT EXISTS proposal_description TEXT,
    ADD COLUMN IF NOT EXISTS proposal_submitted_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS proposal_reviewed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS proposal_review_comment TEXT,
    ADD COLUMN IF NOT EXISTS proposal_bundle JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS last_preview_job_id TEXT,
    ADD COLUMN IF NOT EXISTS last_preview_node_id TEXT,
    ADD COLUMN IF NOT EXISTS last_preview_nodes JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS deployed_commit_id TEXT,
    ADD COLUMN IF NOT EXISTS schedule_interval_seconds INTEGER,
    ADD COLUMN IF NOT EXISTS schedule_cron TEXT,
    ADD COLUMN IF NOT EXISTS last_scheduled_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1;

INSERT INTO spice_pipelines.pipeline_branches (db_name, branch, archived)
SELECT DISTINCT db_name, branch, FALSE
FROM spice_pipelines.pipelines
ON CONFLICT (db_name, branch) DO NOTHING;

ALTER TABLE spice_pipelines.pipelines
    DROP COLUMN IF EXISTS branch_base_branch,
    DROP COLUMN IF EXISTS branch_base_version,
    DROP COLUMN IF EXISTS deployed_version;

ALTER TABLE spice_pipelines.pipelines
    DROP CONSTRAINT IF EXISTS pipelines_db_name_name_key;

CREATE UNIQUE INDEX IF NOT EXISTS pipelines_db_name_name_branch_key
    ON spice_pipelines.pipelines(db_name, name, branch);

CREATE TABLE IF NOT EXISTS spice_pipelines.pipeline_versions (
    version_id UUID PRIMARY KEY,
    pipeline_id UUID NOT NULL,
    branch TEXT NOT NULL DEFAULT 'main',
    lakefs_commit_id TEXT NOT NULL,
    definition_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (pipeline_id, branch, lakefs_commit_id),
    FOREIGN KEY (pipeline_id)
        REFERENCES spice_pipelines.pipelines(pipeline_id)
        ON DELETE CASCADE
);

ALTER TABLE spice_pipelines.pipeline_versions
    ADD COLUMN IF NOT EXISTS branch TEXT NOT NULL DEFAULT 'main',
    ADD COLUMN IF NOT EXISTS lakefs_commit_id TEXT;

ALTER TABLE spice_pipelines.pipeline_versions
    DROP COLUMN IF EXISTS version;

ALTER TABLE spice_pipelines.pipeline_versions
    DROP CONSTRAINT IF EXISTS pipeline_versions_pipeline_id_branch_version_key;

CREATE UNIQUE INDEX IF NOT EXISTS pipeline_versions_pipeline_id_branch_commit_key
    ON spice_pipelines.pipeline_versions(pipeline_id, branch, lakefs_commit_id);

CREATE TABLE IF NOT EXISTS spice_pipelines.pipeline_runs (
    run_id UUID PRIMARY KEY,
    pipeline_id UUID NOT NULL,
    job_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    node_id TEXT,
    row_count INTEGER,
    sample_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    pipeline_spec_commit_id TEXT,
    pipeline_spec_hash TEXT,
    input_lakefs_commits JSONB,
    output_lakefs_commit_id TEXT,
    spark_conf JSONB,
    code_version TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    UNIQUE (pipeline_id, job_id),
    FOREIGN KEY (pipeline_id)
        REFERENCES spice_pipelines.pipelines(pipeline_id)
        ON DELETE CASCADE
);

ALTER TABLE spice_pipelines.pipeline_runs
    ADD COLUMN IF NOT EXISTS pipeline_spec_commit_id TEXT,
    ADD COLUMN IF NOT EXISTS pipeline_spec_hash TEXT,
    ADD COLUMN IF NOT EXISTS input_lakefs_commits JSONB,
    ADD COLUMN IF NOT EXISTS output_lakefs_commit_id TEXT,
    ADD COLUMN IF NOT EXISTS spark_conf JSONB,
    ADD COLUMN IF NOT EXISTS code_version TEXT;

CREATE TABLE IF NOT EXISTS spice_pipelines.pipeline_artifacts (
    artifact_id UUID PRIMARY KEY,
    pipeline_id UUID NOT NULL,
    job_id TEXT NOT NULL,
    run_id UUID,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    definition_hash TEXT,
    definition_commit_id TEXT,
    pipeline_spec_hash TEXT,
    pipeline_spec_commit_id TEXT,
    inputs JSONB NOT NULL DEFAULT '{}'::jsonb,
    lakefs_repository TEXT,
    lakefs_branch TEXT,
    lakefs_commit_id TEXT,
    outputs JSONB NOT NULL DEFAULT '[]'::jsonb,
    declared_outputs JSONB NOT NULL DEFAULT '[]'::jsonb,
    sampling_strategy JSONB NOT NULL DEFAULT '{}'::jsonb,
    error JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (pipeline_id, job_id, mode),
    FOREIGN KEY (pipeline_id)
        REFERENCES spice_pipelines.pipelines(pipeline_id)
        ON DELETE CASCADE
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'pipeline_artifacts_outputs_array'
          AND conrelid = 'spice_pipelines.pipeline_artifacts'::regclass
    ) THEN
        ALTER TABLE spice_pipelines.pipeline_artifacts
        ADD CONSTRAINT pipeline_artifacts_outputs_array
        CHECK (jsonb_typeof(outputs) = 'array') NOT VALID;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'pipeline_artifacts_declared_outputs_array'
          AND conrelid = 'spice_pipelines.pipeline_artifacts'::regclass
    ) THEN
        ALTER TABLE spice_pipelines.pipeline_artifacts
        ADD CONSTRAINT pipeline_artifacts_declared_outputs_array
        CHECK (jsonb_typeof(declared_outputs) = 'array') NOT VALID;
    END IF;
END $$;

UPDATE spice_pipelines.pipeline_artifacts
SET outputs = jsonb_build_array(outputs)
WHERE jsonb_typeof(outputs) = 'object';

UPDATE spice_pipelines.pipeline_artifacts
SET outputs = '[]'::jsonb
WHERE outputs IS NULL OR jsonb_typeof(outputs) <> 'array';

UPDATE spice_pipelines.pipeline_artifacts
SET declared_outputs = jsonb_build_array(declared_outputs)
WHERE jsonb_typeof(declared_outputs) = 'object';

UPDATE spice_pipelines.pipeline_artifacts
SET declared_outputs = '[]'::jsonb
WHERE declared_outputs IS NULL OR jsonb_typeof(declared_outputs) <> 'array';

CREATE TABLE IF NOT EXISTS spice_pipelines.promotion_manifests (
    manifest_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id UUID NOT NULL,
    db_name TEXT NOT NULL,
    build_job_id TEXT NOT NULL,
    artifact_id UUID,
    definition_hash TEXT,
    lakefs_repository TEXT NOT NULL,
    lakefs_commit_id TEXT NOT NULL,
    ontology_commit_id TEXT NOT NULL,
    mapping_spec_id TEXT,
    mapping_spec_version INTEGER,
    mapping_spec_target_class_id TEXT,
    promoted_dataset_version_id UUID NOT NULL,
    promoted_dataset_name TEXT,
    target_branch TEXT NOT NULL,
    promoted_by TEXT,
    promoted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_promotion_manifests_pipeline_id
    ON spice_pipelines.promotion_manifests(pipeline_id);

CREATE INDEX IF NOT EXISTS idx_promotion_manifests_build_job
    ON spice_pipelines.promotion_manifests(build_job_id);

CREATE INDEX IF NOT EXISTS idx_promotion_manifests_artifact_id
    ON spice_pipelines.promotion_manifests(artifact_id);

CREATE INDEX IF NOT EXISTS idx_promotion_manifests_dataset_version
    ON spice_pipelines.promotion_manifests(promoted_dataset_version_id);

CREATE INDEX IF NOT EXISTS idx_promotion_manifests_lakefs_commit
    ON spice_pipelines.promotion_manifests(lakefs_commit_id);

CREATE INDEX IF NOT EXISTS idx_promotion_manifests_ontology_commit
    ON spice_pipelines.promotion_manifests(ontology_commit_id);

CREATE TABLE IF NOT EXISTS spice_pipelines.pipeline_watermarks (
    pipeline_id UUID NOT NULL,
    branch TEXT NOT NULL,
    watermarks_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_id, branch),
    FOREIGN KEY (pipeline_id)
        REFERENCES spice_pipelines.pipelines(pipeline_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS spice_pipelines.pipeline_dependencies (
    pipeline_id UUID NOT NULL,
    depends_on_pipeline_id UUID NOT NULL,
    required_status TEXT NOT NULL DEFAULT 'DEPLOYED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_id, depends_on_pipeline_id),
    FOREIGN KEY (pipeline_id)
        REFERENCES spice_pipelines.pipelines(pipeline_id)
        ON DELETE CASCADE,
    FOREIGN KEY (depends_on_pipeline_id)
        REFERENCES spice_pipelines.pipelines(pipeline_id)
        ON DELETE RESTRICT
);

DO $$
BEGIN
    ALTER TABLE spice_pipelines.pipeline_dependencies
        ADD CONSTRAINT pipeline_dependencies_required_status_check
        CHECK (required_status IN ('DEPLOYED', 'SUCCESS'));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS spice_pipelines.pipeline_permissions (
    pipeline_id UUID NOT NULL,
    principal_type TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    role TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_id, principal_type, principal_id),
    FOREIGN KEY (pipeline_id)
        REFERENCES spice_pipelines.pipelines(pipeline_id)
        ON DELETE CASCADE
);

DO $$
BEGIN
    ALTER TABLE spice_pipelines.pipeline_permissions
        ADD CONSTRAINT pipeline_permissions_role_check
        CHECK (role IN ('read', 'edit', 'approve', 'admin'));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS spice_pipelines.pipeline_udfs (
    udf_id UUID PRIMARY KEY,
    db_name TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    latest_version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (db_name, name)
);

CREATE TABLE IF NOT EXISTS spice_pipelines.pipeline_udf_versions (
    version_id UUID PRIMARY KEY,
    udf_id UUID NOT NULL,
    version INTEGER NOT NULL,
    code TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (udf_id, version),
    FOREIGN KEY (udf_id)
        REFERENCES spice_pipelines.pipeline_udfs(udf_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pipelines_db_name
    ON spice_pipelines.pipelines(db_name);

CREATE INDEX IF NOT EXISTS idx_pipelines_version
    ON spice_pipelines.pipelines(pipeline_id, version);

CREATE INDEX IF NOT EXISTS idx_pipelines_schedule
    ON spice_pipelines.pipelines(schedule_interval_seconds);

CREATE INDEX IF NOT EXISTS idx_pipelines_schedule_cron
    ON spice_pipelines.pipelines(schedule_cron);

CREATE INDEX IF NOT EXISTS idx_pipeline_versions_pipeline_id
    ON spice_pipelines.pipeline_versions(pipeline_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_versions_branch
    ON spice_pipelines.pipeline_versions(branch);

CREATE INDEX IF NOT EXISTS idx_pipeline_artifacts_pipeline_id
    ON spice_pipelines.pipeline_artifacts(pipeline_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_artifacts_job_id
    ON spice_pipelines.pipeline_artifacts(job_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_dependencies_pipeline_id
    ON spice_pipelines.pipeline_dependencies(pipeline_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_dependencies_depends_on
    ON spice_pipelines.pipeline_dependencies(depends_on_pipeline_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_permissions_pipeline_id
    ON spice_pipelines.pipeline_permissions(pipeline_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_permissions_principal
    ON spice_pipelines.pipeline_permissions(principal_type, principal_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_watermarks_pipeline_id
    ON spice_pipelines.pipeline_watermarks(pipeline_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_udfs_db_name
    ON spice_pipelines.pipeline_udfs(db_name);

CREATE INDEX IF NOT EXISTS idx_pipeline_udf_versions_udf_id
    ON spice_pipelines.pipeline_udf_versions(udf_id);

ALTER TABLE spice_pipelines.pipeline_versions
    ADD COLUMN IF NOT EXISTS branch TEXT NOT NULL DEFAULT 'main';

ALTER TABLE spice_pipelines.pipeline_versions
    DROP CONSTRAINT IF EXISTS pipeline_versions_pipeline_id_version_key;

DROP INDEX IF EXISTS spice_pipelines.pipeline_versions_pipeline_id_branch_version_key;

UPDATE spice_pipelines.pipeline_versions v
SET branch = p.branch
FROM spice_pipelines.pipelines p
WHERE v.pipeline_id = p.pipeline_id AND (v.branch IS NULL OR v.branch = '');
