# SPICE HARVESTER — BFF API Reference (v1, code-backed)

> Updated: 2026-01-08  \
> Base URL (local): `http://localhost:8002/api/v1`  \
> OpenAPI (local): `http://localhost:8002/openapi.json`  \
> Swagger UI (local): `http://localhost:8002/docs`

## Scope

- **BFF is the frontend contract**. Frontend clients should only call `/api/v1/...` BFF routes.
- OMS/Funnel/Workers are internal dependencies. Their APIs are not the frontend contract.
- Payload schemas are best read from OpenAPI. This document enumerates **current routes** and behavior.

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

### Rate limiting

- Rate limiting is active when Redis is available.
- Standard headers are returned:
  - `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`
  - `X-RateLimit-Mode`, `X-RateLimit-Degraded`, `X-RateLimit-Disabled` (best-effort)

### Common response shapes

- `ApiResponse`: `{ status, message, data, errors }`
- `CommandResult`: `{ command_id, status, result, error, completed_at, retry_count }`

### Branching

- Many endpoints accept `?branch=` (default `main`).
- Branch names in path parameters use `{branch_name:path}` to allow `/`.

## Endpoint Index

### Health / Monitoring / Config

- `GET /api/v1`
- `GET /api/v1/health`
- `GET /api/v1/monitoring/health`
- `GET /api/v1/monitoring/health/detailed`
- `GET /api/v1/monitoring/health/readiness`
- `GET /api/v1/monitoring/health/liveness`
- `GET /api/v1/monitoring/metrics`
- `GET /api/v1/monitoring/status`
- `GET /api/v1/monitoring/config`
- `GET /api/v1/monitoring/dependencies`
- `GET /api/v1/monitoring/background-tasks/metrics`
- `GET /api/v1/monitoring/background-tasks/active`
- `GET /api/v1/monitoring/background-tasks/health`
- `POST /api/v1/monitoring/services/{service_name}/restart`
- `GET /api/v1/config/config/current`
- `GET /api/v1/config/changes`
- `GET /api/v1/config/validation`
- `GET /api/v1/config/security-audit`
- `GET /api/v1/config/report`
- `POST /api/v1/config/check-changes`
- `GET /api/v1/config/drift-analysis`
- `GET /api/v1/config/health-impact`
- `GET /api/v1/config/monitoring-status`

Service-level (non `/api/v1`):
- `GET /metrics`
- `GET /debug/cors` (debug only)

### Admin

- `POST /api/v1/admin/cleanup-old-replays`
- `POST /api/v1/admin/recompute-projection`
- `GET /api/v1/admin/recompute-projection/{task_id}/result`
- `POST /api/v1/admin/replay-instance-state`
- `GET /api/v1/admin/replay-instance-state/{task_id}/result`
- `GET /api/v1/admin/replay-instance-state/{task_id}/trace`
- `GET /api/v1/admin/system-health`
- `GET /api/v1/admin/lakefs/credentials`
- `POST /api/v1/admin/lakefs/credentials`

### Audit

- `GET /api/v1/audit/logs`
- `GET /api/v1/audit/chain-head`

### Governance (Backing Data / KeySpec / Gates)

- `POST /api/v1/backing-datasources`
- `GET /api/v1/backing-datasources`
- `GET /api/v1/backing-datasources/{backing_id}`
- `POST /api/v1/backing-datasources/{backing_id}/versions`
- `GET /api/v1/backing-datasources/{backing_id}/versions`
- `GET /api/v1/backing-datasource-versions/{version_id}`
- `POST /api/v1/key-specs`
- `GET /api/v1/key-specs`
- `GET /api/v1/key-specs/{key_spec_id}`
- `GET /api/v1/schema-migration-plans`
- `POST /api/v1/gate-policies`
- `GET /api/v1/gate-policies`
- `GET /api/v1/gate-results`
- `POST /api/v1/access-policies`
- `GET /api/v1/access-policies`

### Command Status

- `GET /api/v1/commands/{command_id}/status`

### Data Connectors (Google Sheets)

- `POST /api/v1/data-connectors/google-sheets/oauth/start`
- `GET /api/v1/data-connectors/google-sheets/oauth/callback`
- `GET /api/v1/data-connectors/google-sheets/connections`
- `DELETE /api/v1/data-connectors/google-sheets/connections/{connection_id}`
- `GET /api/v1/data-connectors/google-sheets/drive/spreadsheets`
- `GET /api/v1/data-connectors/google-sheets/spreadsheets/{sheet_id}/worksheets`
- `POST /api/v1/data-connectors/google-sheets/grid`
- `POST /api/v1/data-connectors/google-sheets/preview`
- `POST /api/v1/data-connectors/google-sheets/register`
- `GET /api/v1/data-connectors/google-sheets/{sheet_id}/preview`
- `GET /api/v1/data-connectors/google-sheets/registered`
- `POST /api/v1/data-connectors/google-sheets/{sheet_id}/start-pipelining`
- `DELETE /api/v1/data-connectors/google-sheets/{sheet_id}`

### Databases / Branches / Classes

- `GET /api/v1/databases`
- `POST /api/v1/databases`
- `DELETE /api/v1/databases/{db_name}`
- `GET /api/v1/databases/{db_name}`
- `GET /api/v1/databases/{db_name}/branches`
- `POST /api/v1/databases/{db_name}/branches`
- `GET /api/v1/databases/{db_name}/branches/{branch_name:path}`
- `DELETE /api/v1/databases/{db_name}/branches/{branch_name:path}`
- `GET /api/v1/databases/{db_name}/classes`
- `POST /api/v1/databases/{db_name}/classes`
- `GET /api/v1/databases/{db_name}/classes/{class_id}`
- `GET /api/v1/databases/{db_name}/expected-seq`
- `GET /api/v1/databases/{db_name}/versions`

### Instances (Async writes)

- `POST /api/v1/databases/{db_name}/instances/{class_label}/create`
- `PUT /api/v1/databases/{db_name}/instances/{class_label}/{instance_id}/update`
- `DELETE /api/v1/databases/{db_name}/instances/{class_label}/{instance_id}/delete`
- `POST /api/v1/databases/{db_name}/instances/{class_label}/bulk-create`

### Instances (Read)

- `GET /api/v1/databases/{db_name}/class/{class_id}/instances`
- `GET /api/v1/databases/{db_name}/class/{class_id}/sample-values`
- `GET /api/v1/databases/{db_name}/class/{class_id}/instance/{instance_id}`

### Mappings

- `GET /api/v1/databases/{db_name}/mappings`
- `DELETE /api/v1/databases/{db_name}/mappings`
- `POST /api/v1/databases/{db_name}/mappings/export`
- `POST /api/v1/databases/{db_name}/mappings/import`
- `POST /api/v1/databases/{db_name}/mappings/validate`

### Merge

- `POST /api/v1/databases/{db_name}/merge/simulate`
- `POST /api/v1/databases/{db_name}/merge/resolve`

### Ontology (classes + validation)

- `POST /api/v1/databases/{db_name}/ontology`
- `GET /api/v1/databases/{db_name}/ontology/list`
- `GET /api/v1/databases/{db_name}/ontology/{class_label}`
- `PUT /api/v1/databases/{db_name}/ontology/{class_label}`
- `DELETE /api/v1/databases/{db_name}/ontology/{class_label}`
- `POST /api/v1/databases/{db_name}/ontology/validate`
- `POST /api/v1/databases/{db_name}/ontology/{class_id}/validate`
- `GET /api/v1/databases/{db_name}/ontology/{class_id}/schema`
- `POST /api/v1/databases/{db_name}/ontology-advanced`
- `POST /api/v1/databases/{db_name}/validate-relationships`
- `POST /api/v1/databases/{db_name}/check-circular-references`
- `GET /api/v1/databases/{db_name}/relationship-network/analyze`
- `GET /api/v1/databases/{db_name}/relationship-paths`
- `POST /api/v1/databases/{db_name}/suggest-schema-from-data`
- `POST /api/v1/databases/{db_name}/suggest-mappings`
- `POST /api/v1/databases/{db_name}/suggest-mappings-from-google-sheets`
- `POST /api/v1/databases/{db_name}/suggest-mappings-from-excel`
- `POST /api/v1/databases/{db_name}/import-from-google-sheets/dry-run`
- `POST /api/v1/databases/{db_name}/import-from-google-sheets/commit`
- `POST /api/v1/databases/{db_name}/import-from-excel/dry-run`
- `POST /api/v1/databases/{db_name}/import-from-excel/commit`
- `POST /api/v1/databases/{db_name}/suggest-schema-from-google-sheets`
- `POST /api/v1/databases/{db_name}/suggest-schema-from-excel`
- `POST /api/v1/databases/{db_name}/ontology/{class_id}/mapping-metadata`

### Ontology Extensions (branches/proposals)

- `GET /api/v1/databases/{db_name}/ontology/branches`
- `POST /api/v1/databases/{db_name}/ontology/branches`
- `GET /api/v1/databases/{db_name}/ontology/proposals`
- `POST /api/v1/databases/{db_name}/ontology/proposals`
- `POST /api/v1/databases/{db_name}/ontology/proposals/{proposal_id}/approve`
- `POST /api/v1/databases/{db_name}/ontology/deploy`
- `GET /api/v1/databases/{db_name}/ontology/health`

### Ontology Resources (patterned)

Resource types: `shared-properties`, `value-types`, `interfaces`, `groups`, `functions`, `action-types`.

- `GET /api/v1/databases/{db_name}/ontology/{resource_type}`
- `POST /api/v1/databases/{db_name}/ontology/{resource_type}`
- `GET /api/v1/databases/{db_name}/ontology/{resource_type}/{resource_id}`
- `PUT /api/v1/databases/{db_name}/ontology/{resource_type}/{resource_id}`
- `DELETE /api/v1/databases/{db_name}/ontology/{resource_type}/{resource_id}`

### Object Types

- `POST /api/v1/databases/{db_name}/ontology/object-types`
- `GET /api/v1/databases/{db_name}/ontology/object-types/{class_id}`
- `PUT /api/v1/databases/{db_name}/ontology/object-types/{class_id}`

### Link Types

- `POST /api/v1/databases/{db_name}/ontology/link-types`
- `GET /api/v1/databases/{db_name}/ontology/link-types`
- `GET /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}`
- `PUT /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}`
- `DELETE /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}`
- `GET /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/edits`
- `POST /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/edits`
- `POST /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/reindex`

### Query

- `POST /api/v1/databases/{db_name}/query`
- `POST /api/v1/databases/{db_name}/query/raw`
- `GET /api/v1/databases/{db_name}/query/builder`

### Pipelines

- `GET /api/v1/pipelines`
- `POST /api/v1/pipelines`
- `GET /api/v1/pipelines/proposals`
- `POST /api/v1/pipelines/{pipeline_id}/proposals`
- `POST /api/v1/pipelines/{pipeline_id}/proposals/approve`
- `POST /api/v1/pipelines/{pipeline_id}/proposals/reject`
- `GET /api/v1/pipelines/{pipeline_id}`
- `PUT /api/v1/pipelines/{pipeline_id}`
- `GET /api/v1/pipelines/{pipeline_id}/readiness`
- `GET /api/v1/pipelines/{pipeline_id}/runs`
- `GET /api/v1/pipelines/{pipeline_id}/artifacts`
- `GET /api/v1/pipelines/{pipeline_id}/artifacts/{artifact_id}`
- `POST /api/v1/pipelines/{pipeline_id}/preview`
- `POST /api/v1/pipelines/{pipeline_id}/build`
- `POST /api/v1/pipelines/{pipeline_id}/deploy`
- `POST /api/v1/pipelines/{pipeline_id}/branches`
- `GET /api/v1/pipelines/branches`
- `POST /api/v1/pipelines/branches/{branch}/archive`
- `POST /api/v1/pipelines/branches/{branch}/restore`
- `GET /api/v1/pipelines/datasets`
- `POST /api/v1/pipelines/datasets`
- `POST /api/v1/pipelines/datasets/{dataset_id}/versions`
- `POST /api/v1/pipelines/datasets/{dataset_id}/versions/{version_id}/funnel-analysis`
- `POST /api/v1/pipelines/datasets/excel-upload`
- `POST /api/v1/pipelines/datasets/csv-upload`
- `POST /api/v1/pipelines/datasets/media-upload`
- `GET /api/v1/pipelines/datasets/ingest-requests/{ingest_request_id}`
- `POST /api/v1/pipelines/datasets/ingest-requests/{ingest_request_id}/schema/approve`

### Objectify

- `POST /api/v1/objectify/mapping-specs`
- `GET /api/v1/objectify/mapping-specs`
- `POST /api/v1/objectify/datasets/{dataset_id}/run`

### Lineage

- `GET /api/v1/lineage/graph`
- `GET /api/v1/lineage/impact`
- `GET /api/v1/lineage/metrics`

### Graph Query / Projections

- `POST /api/v1/graph-query/{db_name}`
- `POST /api/v1/graph-query/{db_name}/simple`
- `POST /api/v1/graph-query/{db_name}/multi-hop`
- `GET /api/v1/graph-query/{db_name}/paths`
- `GET /api/v1/graph-query/health`
- `POST /api/v1/projections/{db_name}/register`
- `POST /api/v1/projections/{db_name}/query`
- `GET /api/v1/projections/{db_name}/list`

### Ops / Tasks / Summary

- `GET /api/v1/ops/status`
- `GET /api/v1/tasks`
- `GET /api/v1/tasks/{task_id}`
- `DELETE /api/v1/tasks/{task_id}`
- `GET /api/v1/tasks/metrics/summary`
- `POST /api/v1/tasks/{task_id}/retry`
- `GET /api/v1/tasks/{task_id}/result`
- `GET /api/v1/summary`

### WebSocket

- `WEBSOCKET /api/v1/ws/commands`
- `WEBSOCKET /api/v1/ws/commands/{command_id}`

### AI

- `POST /api/v1/ai/query/{db_name}`
- `POST /api/v1/ai/translate/query-plan/{db_name}`

### Context7

- `POST /api/v1/api/context7/search`
- `GET /api/v1/api/context7/context/{entity_id}`
- `POST /api/v1/api/context7/knowledge`
- `POST /api/v1/api/context7/link`
- `POST /api/v1/api/context7/analyze/ontology`
- `GET /api/v1/api/context7/suggestions/{db_name}/{class_id}`
- `GET /api/v1/api/context7/health`
