# SPICE HARVESTER — BFF API Reference (v1, code-backed)

> Updated: 2026-01-11  \
> Base URL (local): `http://localhost:8002/api/v1`  \
> OpenAPI (local): `http://localhost:8002/openapi.json`  \
> Swagger UI (local): `http://localhost:8002/docs`

## Scope

- **BFF is the frontend contract**. Frontend clients should only call `/api/v1/...` BFF routes.
- OMS/Funnel/Workers are internal dependencies. Their APIs are not the frontend contract.
- Payload schemas are best read from OpenAPI. This document enumerates **current routes** and behavior.
- Agent service (:8004) is reachable via the BFF `/api/v1/agent/*` proxy only; direct access is internal. Details in `docs/ARCHITECTURE.md` / `docs/LLM_INTEGRATION.md`.

## Conventions

### Authentication

- **Required by default**. BFF requires a shared token unless explicitly disabled.
- Token sources: `X-Admin-Token` or `Authorization: Bearer <token>`.
- Config:
  - `BFF_REQUIRE_AUTH=true` (default)
  - `BFF_ADMIN_TOKEN` or `BFF_WRITE_TOKEN` (or `ADMIN_API_KEY`/`ADMIN_TOKEN`)
  - To disable auth **only in non-production**: set `BFF_REQUIRE_AUTH=false` AND `ALLOW_INSECURE_BFF_AUTH_DISABLE=true`.
- Exempt paths (default): `/api/v1`, `/api/v1/health`, Google Sheets OAuth callback.

### Async writes (202 Accepted)

- Many write endpoints return **202 + `command_id`**; poll `/api/v1/commands/{command_id}/status`.
- Some writes are synchronous (200/201) when no async command is created.
- Action writeback submissions return **202 + `action_log_id`** (not `command_id`).

### Rate limiting

- Rate limiting is active when Redis is available.
- Standard headers are returned:
  - `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`
  - `X-RateLimit-Mode`, `X-RateLimit-Degraded`, `X-RateLimit-Disabled` (best-effort)

### Common response shapes

- `ApiResponse`: `{ status, message, data, errors }`
- `CommandResult`: `{ command_id, status, result, error, completed_at, retry_count }`

### Branching

- Many endpoints accept `?branch=` (default `main`). For read APIs, `branch` is a deprecated alias for `base_branch`.
- Branch names in path parameters use `{branch_name:path}` to allow `/`.

### Action writeback (overlay reads)

Some read APIs support **Action-only writeback overlay** (`docs/ACTION_WRITEBACK_DESIGN.md`).

Branch parameters (read APIs):
- `base_branch`: authoritative base branch (Terminus + base ES index). Default: `main`.
- `overlay_branch`: ES overlay branch. Default is resolved server-side for writeback-enabled types (usually `writeback-{db_name}`).
- `branch` (deprecated): alias for `base_branch` only.

Writeback read flags:
- `WRITEBACK_READ_OVERLAY=true` enables automatic overlay reads for object types listed in `WRITEBACK_ENABLED_OBJECT_TYPES`.
- `overlay_branch` can be explicitly provided to force overlay reads regardless of `WRITEBACK_READ_OVERLAY`.

Overlay response metadata (best-effort, varies per endpoint):
- `overlay_status`: `ACTIVE | DISABLED | DEGRADED`
- `writeback_enabled`: whether the type is writeback-enabled (by server config)
- `writeback_edits_present`: whether writeback edits exist (may be `null` when not computable)

Notes:
- ES overlay documents are only written when an Action produces a **non-noop** `applied_changes` (or a tombstone).
  If a conflict policy results in a no-op (e.g. `BASE_WINS` skip), reads may return the base document even when
  `overlay_branch` is provided, and overlay-only fields like `patchset_commit_id` may be absent.
- Conflict/decision details should be read from the ActionLog (`/api/v1/databases/{db_name}/actions/logs/...`).

Degraded behavior:
- For writeback-enabled types, **no silent Terminus-only fallback** is allowed when overlay is required.
- If ES overlay is unavailable, endpoints either:
  - return `503` with `overlay_status=DEGRADED`, or
  - return a server-merged view with `overlay_status=DEGRADED` (currently only implemented for single-instance reads).

Action submission note:
- `POST /api/v1/databases/{db_name}/actions/{action_type_id}/submit` returns an `action_log_id` (ActionLog).
- Use `GET /api/v1/databases/{db_name}/actions/logs/{action_log_id}` (or `GET /api/v1/databases/{db_name}/actions/logs`) to read status/result.
- Actor identity is derived from request headers (`X-User-ID`, optional `X-User-Type` default `user`) and forwarded to OMS as `metadata.user_id` / `metadata.user_type`.
- ActionLog is also exposed as a virtual ontology object type `ActionLog`:
  `GET /api/v1/databases/{db_name}/class/ActionLog/instance/{action_log_id}` and `GET /api/v1/databases/{db_name}/class/ActionLog/instances`.

### Direct CRUD guard (writeback-enabled types)

When `WRITEBACK_ENFORCE=true`, direct instance CRUD is **ingestion-only** for writeback-enabled object types:
- `POST/PUT/DELETE /api/v1/databases/{db_name}/instances/...` requires `metadata.kind=ingest` (or a system principal).
- Operational edits should use `POST /api/v1/databases/{db_name}/actions/{action_type_id}/submit`.

## Endpoint Index

This section is auto-generated from BFF OpenAPI. To update, run:

```bash
python scripts/generate_api_reference.py
```

<!-- BEGIN AUTO-GENERATED ENDPOINTS -->
> Generated from OpenAPI by `scripts/generate_api_reference.py`. Do not edit manually.

### Actions
- `GET /api/v1/databases/{db_name}/actions/logs`
- `GET /api/v1/databases/{db_name}/actions/logs/{action_log_id}`
- `GET /api/v1/databases/{db_name}/actions/simulations`
- `GET /api/v1/databases/{db_name}/actions/simulations/{simulation_id}`
- `GET /api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions`
- `GET /api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions/{version}`
- `POST /api/v1/databases/{db_name}/actions/{action_type_id}/simulate`
- `POST /api/v1/databases/{db_name}/actions/{action_type_id}/submit`

### Admin
- `POST /api/v1/admin/ci/ci-results`

### Admin Operations
- `POST /api/v1/admin/cleanup-old-replays`
- `GET /api/v1/admin/lakefs/credentials`
- `POST /api/v1/admin/lakefs/credentials`
- `POST /api/v1/admin/recompute-projection`
- `GET /api/v1/admin/recompute-projection/{task_id}/result`
- `POST /api/v1/admin/replay-instance-state`
- `GET /api/v1/admin/replay-instance-state/{task_id}/result`
- `GET /api/v1/admin/replay-instance-state/{task_id}/trace`
- `GET /api/v1/admin/system-health`

### Agent
- `POST /api/v1/agent/pipeline-runs`
- `POST /api/v1/agent/runs`
- `GET /api/v1/agent/runs/{run_id}`
- `GET /api/v1/agent/runs/{run_id}/events`

### Agent Functions
- `GET /api/v1/agent-functions`
- `POST /api/v1/agent-functions`
- `POST /api/v1/agent-functions/{function_id}/execute`

### Agent Model Admin
- `GET /api/v1/admin/agent-models`
- `POST /api/v1/admin/agent-models`
- `GET /api/v1/admin/agent-models/{model_id}`

### Agent Plans
- `POST /api/v1/agent-plans/compile`
- `POST /api/v1/agent-plans/context-pack`
- `POST /api/v1/agent-plans/validate`
- `GET /api/v1/agent-plans/{plan_id}`
- `POST /api/v1/agent-plans/{plan_id}/apply-patch`
- `POST /api/v1/agent-plans/{plan_id}/approvals`
- `POST /api/v1/agent-plans/{plan_id}/execute`
- `POST /api/v1/agent-plans/{plan_id}/preview`

### Agent Policy Admin
- `GET /api/v1/admin/agent-policies`
- `POST /api/v1/admin/agent-policies`
- `GET /api/v1/admin/agent-policies/{tenant_id}`

### Agent Sessions
- `GET /api/v1/agent-sessions`
- `POST /api/v1/agent-sessions`
- `GET /api/v1/agent-sessions/metrics/llm-usage`
- `GET /api/v1/agent-sessions/{session_id}`
- `DELETE /api/v1/agent-sessions/{session_id}`
- `GET /api/v1/agent-sessions/{session_id}/approvals`
- `POST /api/v1/agent-sessions/{session_id}/approvals/{approval_request_id}`
- `GET /api/v1/agent-sessions/{session_id}/ci-results`
- `POST /api/v1/agent-sessions/{session_id}/ci-results`
- `POST /api/v1/agent-sessions/{session_id}/clarifications`
- `POST /api/v1/agent-sessions/{session_id}/context/file-upload`
- `GET /api/v1/agent-sessions/{session_id}/context/items`
- `POST /api/v1/agent-sessions/{session_id}/context/items`
- `DELETE /api/v1/agent-sessions/{session_id}/context/items/{item_id}`
- `GET /api/v1/agent-sessions/{session_id}/events`
- `GET /api/v1/agent-sessions/{session_id}/jobs`
- `POST /api/v1/agent-sessions/{session_id}/jobs`
- `GET /api/v1/agent-sessions/{session_id}/jobs/{job_id}`
- `GET /api/v1/agent-sessions/{session_id}/llm-calls`
- `POST /api/v1/agent-sessions/{session_id}/messages`
- `POST /api/v1/agent-sessions/{session_id}/messages/remove`
- `PUT /api/v1/agent-sessions/{session_id}/model`
- `GET /api/v1/agent-sessions/{session_id}/models`
- `POST /api/v1/agent-sessions/{session_id}/summarize`
- `GET /api/v1/agent-sessions/{session_id}/token-budget`
- `GET /api/v1/agent-sessions/{session_id}/tool-calls`
- `GET /api/v1/agent-sessions/{session_id}/tools`
- `PUT /api/v1/agent-sessions/{session_id}/tools`
- `PUT /api/v1/agent-sessions/{session_id}/variables`

### Agent Tool Admin
- `GET /api/v1/admin/agent-tools`
- `POST /api/v1/admin/agent-tools`
- `GET /api/v1/admin/agent-tools/{tool_id}`

### AI
- `POST /api/v1/ai/intent`
- `POST /api/v1/ai/query/{db_name}`
- `POST /api/v1/ai/translate/query-plan/{db_name}`

### Async Instance Management
- `POST /api/v1/databases/{db_name}/instances/{class_label}/bulk-create`
- `POST /api/v1/databases/{db_name}/instances/{class_label}/create`
- `DELETE /api/v1/databases/{db_name}/instances/{class_label}/{instance_id}/delete`
- `PUT /api/v1/databases/{db_name}/instances/{class_label}/{instance_id}/update`

### Audit
- `GET /api/v1/audit/chain-head`
- `GET /api/v1/audit/logs`

### Background Tasks
- `GET /api/v1/tasks/`
- `GET /api/v1/tasks/metrics/summary`
- `GET /api/v1/tasks/{task_id}`
- `DELETE /api/v1/tasks/{task_id}`
- `GET /api/v1/tasks/{task_id}/result`

### Command Status
- `GET /api/v1/commands/{command_id}/status`

### Config Monitoring
- `GET /api/v1/config/config/changes`
- `POST /api/v1/config/config/check-changes`
- `GET /api/v1/config/config/current`
- `GET /api/v1/config/config/drift-analysis`
- `GET /api/v1/config/config/health-impact`
- `GET /api/v1/config/config/monitoring-status`
- `GET /api/v1/config/config/report`
- `GET /api/v1/config/config/security-audit`
- `GET /api/v1/config/config/validation`

### Context Tools
- `POST /api/v1/context-tools/datasets/describe`
- `POST /api/v1/context-tools/ontology/snapshot`

### context7
- `POST /api/v1/context7/analyze/ontology`
- `GET /api/v1/context7/context/{entity_id}`
- `GET /api/v1/context7/health`
- `POST /api/v1/context7/knowledge`
- `POST /api/v1/context7/link`
- `POST /api/v1/context7/search`
- `GET /api/v1/context7/suggestions/{db_name}/{class_id}`

### Data Connectors
- `GET /api/v1/data-connectors/google-sheets/connections`
- `DELETE /api/v1/data-connectors/google-sheets/connections/{connection_id}`
- `GET /api/v1/data-connectors/google-sheets/drive/spreadsheets`
- `POST /api/v1/data-connectors/google-sheets/grid`
- `GET /api/v1/data-connectors/google-sheets/oauth/callback`
- `POST /api/v1/data-connectors/google-sheets/oauth/start`
- `POST /api/v1/data-connectors/google-sheets/preview`
- `POST /api/v1/data-connectors/google-sheets/register`
- `GET /api/v1/data-connectors/google-sheets/registered`
- `GET /api/v1/data-connectors/google-sheets/spreadsheets/{sheet_id}/worksheets`
- `DELETE /api/v1/data-connectors/google-sheets/{sheet_id}`
- `GET /api/v1/data-connectors/google-sheets/{sheet_id}/preview`
- `POST /api/v1/data-connectors/google-sheets/{sheet_id}/start-pipelining`

### Database Management
- `GET /api/v1/databases`
- `POST /api/v1/databases`
- `GET /api/v1/databases/{db_name}`
- `DELETE /api/v1/databases/{db_name}`
- `GET /api/v1/databases/{db_name}/branches`
- `POST /api/v1/databases/{db_name}/branches`
- `GET /api/v1/databases/{db_name}/branches/{branch_name}`
- `DELETE /api/v1/databases/{db_name}/branches/{branch_name}`
- `GET /api/v1/databases/{db_name}/classes`
- `POST /api/v1/databases/{db_name}/classes`
- `GET /api/v1/databases/{db_name}/classes/{class_id}`
- `GET /api/v1/databases/{db_name}/expected-seq`
- `GET /api/v1/databases/{db_name}/versions`

### Document Bundles
- `POST /api/v1/document-bundles/{bundle_id}/search`

### Governance
- `GET /api/v1/access-policies`
- `POST /api/v1/access-policies`
- `GET /api/v1/backing-datasource-versions/{version_id}`
- `GET /api/v1/backing-datasources`
- `POST /api/v1/backing-datasources`
- `GET /api/v1/backing-datasources/{backing_id}`
- `GET /api/v1/backing-datasources/{backing_id}/versions`
- `POST /api/v1/backing-datasources/{backing_id}/versions`
- `GET /api/v1/gate-policies`
- `POST /api/v1/gate-policies`
- `GET /api/v1/gate-results`
- `GET /api/v1/key-specs`
- `POST /api/v1/key-specs`
- `GET /api/v1/key-specs/{key_spec_id}`
- `GET /api/v1/schema-migration-plans`

### Graph
- `GET /api/v1/graph-query/health`
- `POST /api/v1/graph-query/{db_name}`
- `POST /api/v1/graph-query/{db_name}/multi-hop`
- `GET /api/v1/graph-query/{db_name}/paths`
- `POST /api/v1/graph-query/{db_name}/simple`

### Health
- `GET /api/v1/`
- `GET /api/v1/health`

### Instance Management
- `GET /api/v1/databases/{db_name}/class/{class_id}/instance/{instance_id}`
- `GET /api/v1/databases/{db_name}/class/{class_id}/instances`
- `GET /api/v1/databases/{db_name}/class/{class_id}/sample-values`

### Label Mappings
- `GET /api/v1/databases/{db_name}/mappings/`
- `DELETE /api/v1/databases/{db_name}/mappings/`
- `POST /api/v1/databases/{db_name}/mappings/export`
- `POST /api/v1/databases/{db_name}/mappings/import`
- `POST /api/v1/databases/{db_name}/mappings/validate`

### Lineage
- `GET /api/v1/lineage/graph`
- `GET /api/v1/lineage/impact`
- `GET /api/v1/lineage/metrics`

### Merge Conflict Resolution
- `POST /api/v1/databases/{db_name}/merge/resolve`
- `POST /api/v1/databases/{db_name}/merge/simulate`

### Monitoring
- `GET /api/v1/monitoring/background-tasks/active`
- `GET /api/v1/monitoring/background-tasks/health`
- `GET /api/v1/monitoring/background-tasks/metrics`
- `GET /api/v1/monitoring/config`
- `GET /api/v1/monitoring/health`
- `GET /api/v1/monitoring/health/detailed`
- `GET /api/v1/monitoring/health/liveness`
- `GET /api/v1/monitoring/health/readiness`
- `GET /api/v1/monitoring/metrics`
- `GET /api/v1/monitoring/status`

### Objectify
- `POST /api/v1/objectify/datasets/{dataset_id}/run`
- `GET /api/v1/objectify/mapping-specs`
- `POST /api/v1/objectify/mapping-specs`

### Ontology Extensions
- `GET /api/v1/databases/{db_name}/ontology/action-types`
- `POST /api/v1/databases/{db_name}/ontology/action-types`
- `GET /api/v1/databases/{db_name}/ontology/action-types/{resource_id}`
- `PUT /api/v1/databases/{db_name}/ontology/action-types/{resource_id}`
- `DELETE /api/v1/databases/{db_name}/ontology/action-types/{resource_id}`
- `GET /api/v1/databases/{db_name}/ontology/branches`
- `POST /api/v1/databases/{db_name}/ontology/branches`
- `POST /api/v1/databases/{db_name}/ontology/deploy`
- `GET /api/v1/databases/{db_name}/ontology/functions`
- `POST /api/v1/databases/{db_name}/ontology/functions`
- `GET /api/v1/databases/{db_name}/ontology/functions/{resource_id}`
- `PUT /api/v1/databases/{db_name}/ontology/functions/{resource_id}`
- `DELETE /api/v1/databases/{db_name}/ontology/functions/{resource_id}`
- `GET /api/v1/databases/{db_name}/ontology/groups`
- `POST /api/v1/databases/{db_name}/ontology/groups`
- `GET /api/v1/databases/{db_name}/ontology/groups/{resource_id}`
- `PUT /api/v1/databases/{db_name}/ontology/groups/{resource_id}`
- `DELETE /api/v1/databases/{db_name}/ontology/groups/{resource_id}`
- `GET /api/v1/databases/{db_name}/ontology/health`
- `GET /api/v1/databases/{db_name}/ontology/interfaces`
- `POST /api/v1/databases/{db_name}/ontology/interfaces`
- `GET /api/v1/databases/{db_name}/ontology/interfaces/{resource_id}`
- `PUT /api/v1/databases/{db_name}/ontology/interfaces/{resource_id}`
- `DELETE /api/v1/databases/{db_name}/ontology/interfaces/{resource_id}`
- `GET /api/v1/databases/{db_name}/ontology/proposals`
- `POST /api/v1/databases/{db_name}/ontology/proposals`
- `POST /api/v1/databases/{db_name}/ontology/proposals/{proposal_id}/approve`
- `GET /api/v1/databases/{db_name}/ontology/shared-properties`
- `POST /api/v1/databases/{db_name}/ontology/shared-properties`
- `GET /api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}`
- `PUT /api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}`
- `DELETE /api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}`
- `GET /api/v1/databases/{db_name}/ontology/value-types`
- `POST /api/v1/databases/{db_name}/ontology/value-types`
- `GET /api/v1/databases/{db_name}/ontology/value-types/{resource_id}`
- `PUT /api/v1/databases/{db_name}/ontology/value-types/{resource_id}`
- `DELETE /api/v1/databases/{db_name}/ontology/value-types/{resource_id}`

### Ontology Link Types
- `GET /api/v1/databases/{db_name}/ontology/link-types`
- `POST /api/v1/databases/{db_name}/ontology/link-types`
- `GET /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}`
- `PUT /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}`
- `GET /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/edits`
- `POST /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/edits`
- `POST /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/reindex`

### Ontology Management
- `POST /api/v1/databases/{db_name}/check-circular-references`
- `POST /api/v1/databases/{db_name}/import-from-excel/commit`
- `POST /api/v1/databases/{db_name}/import-from-excel/dry-run`
- `POST /api/v1/databases/{db_name}/import-from-google-sheets/commit`
- `POST /api/v1/databases/{db_name}/import-from-google-sheets/dry-run`
- `POST /api/v1/databases/{db_name}/ontology`
- `POST /api/v1/databases/{db_name}/ontology-advanced`
- `GET /api/v1/databases/{db_name}/ontology/list`
- `POST /api/v1/databases/{db_name}/ontology/validate`
- `POST /api/v1/databases/{db_name}/ontology/{class_id}/mapping-metadata`
- `GET /api/v1/databases/{db_name}/ontology/{class_id}/schema`
- `GET /api/v1/databases/{db_name}/ontology/{class_label}`
- `PUT /api/v1/databases/{db_name}/ontology/{class_label}`
- `DELETE /api/v1/databases/{db_name}/ontology/{class_label}`
- `POST /api/v1/databases/{db_name}/ontology/{class_label}/validate`
- `GET /api/v1/databases/{db_name}/relationship-network/analyze`
- `GET /api/v1/databases/{db_name}/relationship-paths`
- `POST /api/v1/databases/{db_name}/suggest-mappings`
- `POST /api/v1/databases/{db_name}/suggest-mappings-from-excel`
- `POST /api/v1/databases/{db_name}/suggest-mappings-from-google-sheets`
- `POST /api/v1/databases/{db_name}/suggest-schema-from-data`
- `POST /api/v1/databases/{db_name}/suggest-schema-from-excel`
- `POST /api/v1/databases/{db_name}/suggest-schema-from-google-sheets`
- `POST /api/v1/databases/{db_name}/validate-relationships`

### Ontology Object Types
- `POST /api/v1/databases/{db_name}/ontology/object-types`
- `GET /api/v1/databases/{db_name}/ontology/object-types/{class_id}`
- `PUT /api/v1/databases/{db_name}/ontology/object-types/{class_id}`

### Ops
- `GET /api/v1/ops/status`

### Pipeline Builder
- `GET /api/v1/pipelines`
- `POST /api/v1/pipelines`
- `GET /api/v1/pipelines/branches`
- `POST /api/v1/pipelines/branches/{branch}/archive`
- `POST /api/v1/pipelines/branches/{branch}/restore`
- `GET /api/v1/pipelines/datasets`
- `POST /api/v1/pipelines/datasets`
- `POST /api/v1/pipelines/datasets/csv-upload`
- `POST /api/v1/pipelines/datasets/excel-upload`
- `GET /api/v1/pipelines/datasets/ingest-requests/{ingest_request_id}`
- `POST /api/v1/pipelines/datasets/ingest-requests/{ingest_request_id}/schema/approve`
- `POST /api/v1/pipelines/datasets/media-upload`
- `GET /api/v1/pipelines/datasets/{dataset_id}/raw-file`
- `POST /api/v1/pipelines/datasets/{dataset_id}/versions`
- `POST /api/v1/pipelines/datasets/{dataset_id}/versions/{version_id}/funnel-analysis`
- `GET /api/v1/pipelines/proposals`
- `POST /api/v1/pipelines/simulate-definition`
- `GET /api/v1/pipelines/{pipeline_id}`
- `PUT /api/v1/pipelines/{pipeline_id}`
- `GET /api/v1/pipelines/{pipeline_id}/artifacts`
- `GET /api/v1/pipelines/{pipeline_id}/artifacts/{artifact_id}`
- `POST /api/v1/pipelines/{pipeline_id}/branches`
- `POST /api/v1/pipelines/{pipeline_id}/build`
- `POST /api/v1/pipelines/{pipeline_id}/deploy`
- `POST /api/v1/pipelines/{pipeline_id}/preview`
- `POST /api/v1/pipelines/{pipeline_id}/proposals`
- `POST /api/v1/pipelines/{pipeline_id}/proposals/approve`
- `POST /api/v1/pipelines/{pipeline_id}/proposals/reject`
- `GET /api/v1/pipelines/{pipeline_id}/readiness`
- `GET /api/v1/pipelines/{pipeline_id}/runs`

### Pipeline Plans
- `POST /api/v1/pipeline-plans/compile`
- `POST /api/v1/pipeline-plans/context-pack`
- `GET /api/v1/pipeline-plans/{plan_id}`
- `POST /api/v1/pipeline-plans/{plan_id}/evaluate-joins`
- `POST /api/v1/pipeline-plans/{plan_id}/inspect-preview`
- `POST /api/v1/pipeline-plans/{plan_id}/preview`

### Query
- `POST /api/v1/databases/{db_name}/query`
- `GET /api/v1/databases/{db_name}/query/builder`
- `POST /api/v1/databases/{db_name}/query/raw`

### Summary
- `GET /api/v1/summary`

<!-- END AUTO-GENERATED ENDPOINTS -->

### Service-level (non `/api/v1`, not in OpenAPI)

- `GET /metrics`
- `GET /debug/cors` (debug only)

### WebSocket (not in OpenAPI)

- `WEBSOCKET /api/v1/ws/commands`
- `WEBSOCKET /api/v1/ws/commands/{command_id}`
