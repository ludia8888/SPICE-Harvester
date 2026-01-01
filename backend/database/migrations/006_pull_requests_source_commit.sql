-- Capture source branch head commit at proposal creation

ALTER TABLE pull_requests
    ADD COLUMN IF NOT EXISTS source_commit_id VARCHAR(255);

CREATE INDEX IF NOT EXISTS idx_pr_source_commit ON pull_requests(source_commit_id);

COMMENT ON COLUMN pull_requests.source_commit_id IS 'Source branch head commit captured at proposal creation';
