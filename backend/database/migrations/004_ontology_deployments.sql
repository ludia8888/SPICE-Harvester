-- Ontology deployment registry (SSoT)

CREATE TABLE IF NOT EXISTS ontology_deployments (
    deployment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    db_name VARCHAR(255) NOT NULL,
    proposal_id UUID NOT NULL,
    source_branch VARCHAR(255) NOT NULL,
    target_branch VARCHAR(255) NOT NULL,
    approved_ontology_commit_id VARCHAR(255) NOT NULL,
    merge_commit_id VARCHAR(255) NOT NULL,
    definition_hash VARCHAR(255),
    status VARCHAR(50) NOT NULL DEFAULT 'succeeded'
        CHECK (status IN ('succeeded', 'failed')),
    deployed_by VARCHAR(255) DEFAULT 'system',
    deployed_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_ontology_deployments_db ON ontology_deployments(db_name);
CREATE INDEX IF NOT EXISTS idx_ontology_deployments_target_branch ON ontology_deployments(target_branch);
CREATE INDEX IF NOT EXISTS idx_ontology_deployments_proposal ON ontology_deployments(proposal_id);
CREATE INDEX IF NOT EXISTS idx_ontology_deployments_created_at ON ontology_deployments(deployed_at DESC);

CREATE TABLE IF NOT EXISTS ontology_deploy_outbox (
    outbox_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deployment_id UUID NOT NULL REFERENCES ontology_deployments(deployment_id) ON DELETE CASCADE,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    claimed_by TEXT,
    claimed_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ontology_deploy_outbox_status
    ON ontology_deploy_outbox(status, next_attempt_at, created_at);
CREATE INDEX IF NOT EXISTS idx_ontology_deploy_outbox_claimed
    ON ontology_deploy_outbox(status, claimed_at);
CREATE INDEX IF NOT EXISTS idx_ontology_deploy_outbox_deployment
    ON ontology_deploy_outbox(deployment_id, status, created_at);
