-- Agent registry family schema bootstrap.
-- Runtime DDL should only be needed in dev/test after this migration lands.

CREATE SCHEMA IF NOT EXISTS spice_agent;

CREATE TABLE IF NOT EXISTS spice_agent.agent_models (
    model_id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    display_name TEXT,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    supports_json_mode BOOLEAN NOT NULL DEFAULT true,
    supports_native_tool_calling BOOLEAN NOT NULL DEFAULT false,
    max_context_tokens INTEGER,
    max_output_tokens INTEGER,
    prompt_per_1k DOUBLE PRECISION,
    completion_per_1k DOUBLE PRECISION,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_models_status
    ON spice_agent.agent_models(status);

CREATE INDEX IF NOT EXISTS idx_agent_models_provider
    ON spice_agent.agent_models(provider);

CREATE TABLE IF NOT EXISTS spice_agent.agent_tenant_policies (
    tenant_id TEXT PRIMARY KEY,
    allowed_models JSONB NOT NULL DEFAULT '[]'::jsonb,
    allowed_tools JSONB NOT NULL DEFAULT '[]'::jsonb,
    default_model TEXT,
    auto_approve_rules JSONB NOT NULL DEFAULT '{}'::jsonb,
    data_policies JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_tenant_policies_updated_at
    ON spice_agent.agent_tenant_policies(updated_at);

CREATE TABLE IF NOT EXISTS spice_agent.agent_tool_policies (
    tool_id TEXT PRIMARY KEY,
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    risk_level TEXT NOT NULL DEFAULT 'read',
    requires_approval BOOLEAN NOT NULL DEFAULT false,
    requires_idempotency_key BOOLEAN NOT NULL DEFAULT false,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    roles JSONB NOT NULL DEFAULT '[]'::jsonb,
    max_payload_bytes INTEGER,
    version TEXT NOT NULL DEFAULT 'v1',
    tool_type TEXT NOT NULL DEFAULT 'unknown',
    input_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
    timeout_seconds DOUBLE PRECISION,
    retry_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    resource_scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE spice_agent.agent_tool_policies
    ADD COLUMN IF NOT EXISTS version TEXT NOT NULL DEFAULT 'v1',
    ADD COLUMN IF NOT EXISTS tool_type TEXT NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS input_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS output_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS timeout_seconds DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS retry_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS resource_scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_agent_tool_policies_status
    ON spice_agent.agent_tool_policies(status);

CREATE INDEX IF NOT EXISTS idx_agent_tool_policies_type
    ON spice_agent.agent_tool_policies(tool_type);

CREATE TABLE IF NOT EXISTS spice_agent.agent_functions (
    function_id TEXT NOT NULL,
    version TEXT NOT NULL DEFAULT 'v1',
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    handler TEXT NOT NULL,
    tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    roles JSONB NOT NULL DEFAULT '[]'::jsonb,
    input_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (function_id, version)
);

CREATE INDEX IF NOT EXISTS idx_agent_functions_status
    ON spice_agent.agent_functions(status);

CREATE INDEX IF NOT EXISTS idx_agent_functions_function_id
    ON spice_agent.agent_functions(function_id);

CREATE TABLE IF NOT EXISTS spice_agent.agent_runs (
    run_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    plan_id UUID,
    status TEXT NOT NULL,
    risk_level TEXT NOT NULL DEFAULT 'read',
    requester TEXT,
    delegated_actor TEXT,
    context JSONB NOT NULL DEFAULT '{}'::jsonb,
    plan_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE spice_agent.agent_runs
    ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default';

CREATE INDEX IF NOT EXISTS idx_agent_runs_plan_id
    ON spice_agent.agent_runs(plan_id);

CREATE INDEX IF NOT EXISTS idx_agent_runs_status
    ON spice_agent.agent_runs(status);

CREATE INDEX IF NOT EXISTS idx_agent_runs_tenant
    ON spice_agent.agent_runs(tenant_id);

CREATE TABLE IF NOT EXISTS spice_agent.agent_steps (
    run_id UUID NOT NULL
        REFERENCES spice_agent.agent_runs(run_id)
        ON DELETE CASCADE,
    step_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    tool_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    command_id TEXT,
    task_id TEXT,
    input_digest TEXT,
    output_digest TEXT,
    error TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, step_id)
);

ALTER TABLE spice_agent.agent_steps
    ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default';

CREATE INDEX IF NOT EXISTS idx_agent_steps_run_id
    ON spice_agent.agent_steps(run_id);

CREATE INDEX IF NOT EXISTS idx_agent_steps_tool_id
    ON spice_agent.agent_steps(tool_id);

CREATE INDEX IF NOT EXISTS idx_agent_steps_tenant
    ON spice_agent.agent_steps(tenant_id);

CREATE TABLE IF NOT EXISTS spice_agent.agent_approvals (
    approval_id UUID PRIMARY KEY,
    plan_id UUID NOT NULL,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    step_id TEXT,
    decision TEXT NOT NULL,
    approved_by TEXT NOT NULL,
    approved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    comment TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE spice_agent.agent_approvals
    ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default';

CREATE INDEX IF NOT EXISTS idx_agent_approvals_plan_id
    ON spice_agent.agent_approvals(plan_id);

CREATE INDEX IF NOT EXISTS idx_agent_approvals_tenant
    ON spice_agent.agent_approvals(tenant_id);

CREATE TABLE IF NOT EXISTS spice_agent.agent_approval_requests (
    approval_request_id UUID PRIMARY KEY,
    plan_id UUID NOT NULL,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    session_id UUID,
    job_id UUID,
    status TEXT NOT NULL DEFAULT 'PENDING',
    risk_level TEXT NOT NULL DEFAULT 'write',
    requested_by TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    decision TEXT,
    decided_by TEXT,
    decided_at TIMESTAMPTZ,
    comment TEXT,
    request_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE spice_agent.agent_approval_requests
    ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default';

CREATE INDEX IF NOT EXISTS idx_agent_approval_requests_plan_id
    ON spice_agent.agent_approval_requests(plan_id);

CREATE INDEX IF NOT EXISTS idx_agent_approval_requests_session_id
    ON spice_agent.agent_approval_requests(session_id);

CREATE INDEX IF NOT EXISTS idx_agent_approval_requests_job_id
    ON spice_agent.agent_approval_requests(job_id);

CREATE INDEX IF NOT EXISTS idx_agent_approval_requests_status
    ON spice_agent.agent_approval_requests(status);

CREATE INDEX IF NOT EXISTS idx_agent_approval_requests_tenant
    ON spice_agent.agent_approval_requests(tenant_id);

CREATE TABLE IF NOT EXISTS spice_agent.agent_tool_idempotency (
    tenant_id TEXT NOT NULL DEFAULT 'default',
    idempotency_key TEXT NOT NULL,
    tool_id TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'IN_PROGRESS',
    response_status INTEGER,
    response_body JSONB NOT NULL DEFAULT '{}'::jsonb,
    error TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_agent_tool_idempotency_tool_id
    ON spice_agent.agent_tool_idempotency(tool_id);

CREATE TABLE IF NOT EXISTS spice_agent.agent_sessions (
    session_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    created_by TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    selected_model TEXT,
    enabled_tools JSONB NOT NULL DEFAULT '[]'::jsonb,
    summary TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    terminated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_sessions_tenant
    ON spice_agent.agent_sessions(tenant_id);

CREATE INDEX IF NOT EXISTS idx_agent_sessions_creator
    ON spice_agent.agent_sessions(created_by);

CREATE INDEX IF NOT EXISTS idx_agent_sessions_status
    ON spice_agent.agent_sessions(status);

CREATE TABLE IF NOT EXISTS spice_agent.agent_session_messages (
    message_id UUID PRIMARY KEY,
    session_id UUID NOT NULL
        REFERENCES spice_agent.agent_sessions(session_id)
        ON DELETE CASCADE,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    content_digest TEXT,
    is_removed BOOLEAN NOT NULL DEFAULT false,
    removed_at TIMESTAMPTZ,
    removed_by TEXT,
    removed_reason TEXT,
    token_count INTEGER,
    cost_estimate DOUBLE PRECISION,
    latency_ms INTEGER,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE spice_agent.agent_session_messages
    ADD COLUMN IF NOT EXISTS is_removed BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS removed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS removed_by TEXT,
    ADD COLUMN IF NOT EXISTS removed_reason TEXT;

CREATE INDEX IF NOT EXISTS idx_agent_session_messages_session
    ON spice_agent.agent_session_messages(session_id);

CREATE INDEX IF NOT EXISTS idx_agent_session_messages_removed
    ON spice_agent.agent_session_messages(session_id, is_removed);

CREATE TABLE IF NOT EXISTS spice_agent.agent_session_jobs (
    job_id UUID PRIMARY KEY,
    session_id UUID NOT NULL
        REFERENCES spice_agent.agent_sessions(session_id)
        ON DELETE CASCADE,
    plan_id UUID,
    run_id UUID,
    status TEXT NOT NULL DEFAULT 'PENDING',
    error TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_agent_session_jobs_session
    ON spice_agent.agent_session_jobs(session_id);

CREATE INDEX IF NOT EXISTS idx_agent_session_jobs_status
    ON spice_agent.agent_session_jobs(status);

CREATE TABLE IF NOT EXISTS spice_agent.agent_session_context_items (
    item_id UUID PRIMARY KEY,
    session_id UUID NOT NULL
        REFERENCES spice_agent.agent_sessions(session_id)
        ON DELETE CASCADE,
    item_type TEXT NOT NULL,
    include_mode TEXT NOT NULL DEFAULT 'summary',
    ref JSONB NOT NULL DEFAULT '{}'::jsonb,
    token_count INTEGER,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_session_context_items_session
    ON spice_agent.agent_session_context_items(session_id);

CREATE INDEX IF NOT EXISTS idx_agent_session_context_items_type
    ON spice_agent.agent_session_context_items(item_type);

CREATE INDEX IF NOT EXISTS idx_agent_session_context_items_updated
    ON spice_agent.agent_session_context_items(updated_at);

CREATE TABLE IF NOT EXISTS spice_agent.agent_session_events (
    event_id UUID PRIMARY KEY,
    session_id UUID NOT NULL
        REFERENCES spice_agent.agent_sessions(session_id)
        ON DELETE CASCADE,
    tenant_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    trace_id TEXT,
    correlation_id TEXT,
    data JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_session_events_session_time
    ON spice_agent.agent_session_events(session_id, occurred_at);

CREATE INDEX IF NOT EXISTS idx_agent_session_events_tenant_time
    ON spice_agent.agent_session_events(tenant_id, occurred_at);

CREATE INDEX IF NOT EXISTS idx_agent_session_events_type
    ON spice_agent.agent_session_events(event_type);

CREATE TABLE IF NOT EXISTS spice_agent.agent_session_tool_calls (
    tool_run_id UUID PRIMARY KEY,
    session_id UUID NOT NULL
        REFERENCES spice_agent.agent_sessions(session_id)
        ON DELETE CASCADE,
    tenant_id TEXT NOT NULL,
    job_id UUID,
    plan_id UUID,
    run_id UUID,
    step_id TEXT,
    tool_id TEXT NOT NULL,
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    query JSONB NOT NULL DEFAULT '{}'::jsonb,
    request_body JSONB,
    request_digest TEXT,
    request_token_count INTEGER,
    idempotency_key TEXT,
    status TEXT NOT NULL DEFAULT 'STARTED',
    response_status INTEGER,
    response_body JSONB,
    response_digest TEXT,
    response_token_count INTEGER,
    error_code TEXT,
    error_message TEXT,
    side_effect_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    latency_ms INTEGER,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE spice_agent.agent_session_tool_calls
    ADD COLUMN IF NOT EXISTS request_token_count INTEGER,
    ADD COLUMN IF NOT EXISTS response_token_count INTEGER;

CREATE INDEX IF NOT EXISTS idx_agent_session_tool_calls_session_time
    ON spice_agent.agent_session_tool_calls(session_id, started_at);

CREATE INDEX IF NOT EXISTS idx_agent_session_tool_calls_tenant_time
    ON spice_agent.agent_session_tool_calls(tenant_id, started_at);

CREATE INDEX IF NOT EXISTS idx_agent_session_tool_calls_tool_id
    ON spice_agent.agent_session_tool_calls(tool_id);

CREATE TABLE IF NOT EXISTS spice_agent.agent_session_llm_calls (
    llm_call_id UUID PRIMARY KEY,
    session_id UUID NOT NULL
        REFERENCES spice_agent.agent_sessions(session_id)
        ON DELETE CASCADE,
    tenant_id TEXT NOT NULL,
    job_id UUID,
    plan_id UUID,
    call_type TEXT NOT NULL,
    provider TEXT NOT NULL,
    model_id TEXT NOT NULL,
    cache_hit BOOLEAN NOT NULL DEFAULT false,
    latency_ms INTEGER NOT NULL,
    prompt_tokens INTEGER NOT NULL DEFAULT 0,
    completion_tokens INTEGER NOT NULL DEFAULT 0,
    total_tokens INTEGER NOT NULL DEFAULT 0,
    cost_estimate DOUBLE PRECISION,
    input_digest TEXT,
    output_digest TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_session_llm_calls_session_time
    ON spice_agent.agent_session_llm_calls(session_id, created_at);

CREATE INDEX IF NOT EXISTS idx_agent_session_llm_calls_tenant_time
    ON spice_agent.agent_session_llm_calls(tenant_id, created_at);

CREATE INDEX IF NOT EXISTS idx_agent_session_llm_calls_model
    ON spice_agent.agent_session_llm_calls(model_id);

CREATE TABLE IF NOT EXISTS spice_agent.agent_session_ci_results (
    ci_result_id UUID PRIMARY KEY,
    session_id UUID NOT NULL
        REFERENCES spice_agent.agent_sessions(session_id)
        ON DELETE CASCADE,
    tenant_id TEXT NOT NULL,
    job_id UUID,
    plan_id UUID,
    run_id UUID,
    provider TEXT,
    status TEXT NOT NULL,
    details_url TEXT,
    summary TEXT,
    checks JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_session_ci_results_session_time
    ON spice_agent.agent_session_ci_results(session_id, created_at);

CREATE INDEX IF NOT EXISTS idx_agent_session_ci_results_tenant_time
    ON spice_agent.agent_session_ci_results(tenant_id, created_at);
