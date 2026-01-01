-- Promotion manifests (reproducibility SSoT for pipeline deploys)

CREATE SCHEMA IF NOT EXISTS spice_pipelines;

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
