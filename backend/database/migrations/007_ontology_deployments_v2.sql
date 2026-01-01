-- Ontology deployment registry v2 (SSoT)

CREATE TABLE IF NOT EXISTS ontology_deployments_v2 (
    deployment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    db_name VARCHAR(255) NOT NULL,
    target_branch VARCHAR(255) NOT NULL,
    ontology_commit_id VARCHAR(255) NOT NULL,
    snapshot_rid VARCHAR(255),
    proposal_id UUID,
    status VARCHAR(50) NOT NULL DEFAULT 'succeeded'
        CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
    gate_policy JSONB,
    health_summary JSONB,
    deployed_by VARCHAR(255) DEFAULT 'system',
    deployed_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    error TEXT,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_ontology_deployments_v2_db ON ontology_deployments_v2(db_name);
CREATE INDEX IF NOT EXISTS idx_ontology_deployments_v2_target_branch ON ontology_deployments_v2(target_branch);
CREATE INDEX IF NOT EXISTS idx_ontology_deployments_v2_proposal ON ontology_deployments_v2(proposal_id);
CREATE INDEX IF NOT EXISTS idx_ontology_deployments_v2_created_at ON ontology_deployments_v2(deployed_at DESC);

CREATE TABLE IF NOT EXISTS ontology_deploy_outbox_v2 (
    outbox_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deployment_id UUID NOT NULL REFERENCES ontology_deployments_v2(deployment_id) ON DELETE CASCADE,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    claimed_by TEXT,
    claimed_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ontology_deploy_outbox_v2_status
    ON ontology_deploy_outbox_v2(status, next_attempt_at, created_at);
CREATE INDEX IF NOT EXISTS idx_ontology_deploy_outbox_v2_claimed
    ON ontology_deploy_outbox_v2(status, claimed_at);
CREATE INDEX IF NOT EXISTS idx_ontology_deploy_outbox_v2_deployment
    ON ontology_deploy_outbox_v2(deployment_id, status, created_at);
