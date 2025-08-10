-- Pull Request System Tables
-- Implements GitHub-like PR workflow with MVCC support
-- Following SOLID principles - Single Responsibility for PR management

CREATE TABLE IF NOT EXISTS pull_requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Database and branch information
    db_name VARCHAR(255) NOT NULL,
    source_branch VARCHAR(255) NOT NULL,
    target_branch VARCHAR(255) NOT NULL,
    
    -- PR metadata
    title TEXT NOT NULL,
    description TEXT,
    author VARCHAR(255) DEFAULT 'system',
    
    -- Status management
    status VARCHAR(50) DEFAULT 'open' CHECK (status IN ('open', 'merged', 'closed', 'rejected')),
    merge_commit_id VARCHAR(255),
    
    -- MVCC optimistic locking
    version INT DEFAULT 1 NOT NULL,
    
    -- Cache for TerminusDB diff results
    diff_cache JSONB,
    conflicts JSONB,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    merged_at TIMESTAMPTZ,
    
    -- Ensure only one open PR per branch combination
    CONSTRAINT unique_open_pr_per_branches 
        UNIQUE(db_name, source_branch, target_branch, status)
        DEFERRABLE INITIALLY DEFERRED
);

-- Indexes for performance
CREATE INDEX idx_pr_status ON pull_requests(status) WHERE status = 'open';
CREATE INDEX idx_pr_db_name ON pull_requests(db_name);
CREATE INDEX idx_pr_created_at ON pull_requests(created_at DESC);
CREATE INDEX idx_pr_author ON pull_requests(author);

-- Trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_pr_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_pr_timestamp
    BEFORE UPDATE ON pull_requests
    FOR EACH ROW
    EXECUTE FUNCTION update_pr_updated_at();

-- Comments table for PR discussions (future enhancement)
CREATE TABLE IF NOT EXISTS pull_request_comments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pr_id UUID NOT NULL REFERENCES pull_requests(id) ON DELETE CASCADE,
    author VARCHAR(255) NOT NULL,
    comment TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE INDEX idx_pr_comments_pr_id ON pull_request_comments(pr_id);

-- PR review approvals (future enhancement)
CREATE TABLE IF NOT EXISTS pull_request_reviews (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pr_id UUID NOT NULL REFERENCES pull_requests(id) ON DELETE CASCADE,
    reviewer VARCHAR(255) NOT NULL,
    status VARCHAR(50) CHECK (status IN ('approved', 'changes_requested', 'commented')),
    comment TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    UNIQUE(pr_id, reviewer)
);

CREATE INDEX idx_pr_reviews_pr_id ON pull_request_reviews(pr_id);

-- Add comment to explain MVCC integration
COMMENT ON COLUMN pull_requests.version IS 'Optimistic locking version for MVCC - prevents concurrent updates';
COMMENT ON TABLE pull_requests IS 'Pull Request management with branch-based workflow, similar to GitHub PRs';