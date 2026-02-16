# API Reference (Auto-Generated)

> Generated from BFF OpenAPI by `scripts/generate_api_reference.py`.
> Do not edit manually.

## OpenAPI Metadata

- Title: `BFF (Backend for Frontend) Service`
- Version: `2.0.0`
- OpenAPI: `3.1.0`

## Endpoint Index (`/api/v1`, `/api/v2`)

### Actions
- `GET /api/v1/databases/{db_name}/actions/logs`
- `GET /api/v1/databases/{db_name}/actions/logs/{action_log_id}`
- `POST /api/v1/databases/{db_name}/actions/logs/{action_log_id}/undo`
- `GET /api/v1/databases/{db_name}/actions/simulations`
- `GET /api/v1/databases/{db_name}/actions/simulations/{simulation_id}`
- `GET /api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions`
- `GET /api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions/{version}`
- `POST /api/v1/databases/{db_name}/actions/{action_type_id}/simulate`
- `POST /api/v1/databases/{db_name}/actions/{action_type_id}/submit`
- `POST /api/v1/databases/{db_name}/actions/{action_type_id}/submit-batch`

### Admin
- `POST /api/v1/admin/ci/ci-results`

### Admin Operations
- `POST /api/v1/admin/cleanup-old-replays`
- `POST /api/v1/admin/databases/{db_name}/rebuild-index`
- `GET /api/v1/admin/databases/{db_name}/rebuild-index/{task_id}/status`
- `GET /api/v1/admin/lakefs/credentials`
- `POST /api/v1/admin/lakefs/credentials`
- `POST /api/v1/admin/recompute-projection`
- `GET /api/v1/admin/recompute-projection/{task_id}/result`
- `POST /api/v1/admin/reindex-instances`
- `POST /api/v1/admin/replay-instance-state`
- `GET /api/v1/admin/replay-instance-state/{task_id}/result`
- `GET /api/v1/admin/replay-instance-state/{task_id}/trace`
- `GET /api/v1/admin/system-health`

### Agent
- `POST /api/v1/agent/pipeline-runs`
- `POST /api/v1/agent/pipeline-runs/stream`

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
- `GET /api/v1/databases/{db_name}/classes`
- `POST /api/v1/databases/{db_name}/classes`
- `GET /api/v1/databases/{db_name}/classes/{class_id}`
- `GET /api/v1/databases/{db_name}/expected-seq`

### Document Bundles
- `POST /api/v1/document-bundles/{bundle_id}/search`

### Foundry Ontologies v2
- `GET /api/v2/ontologies`
- `GET /api/v2/ontologies/{ontology}`
- `GET /api/v2/ontologies/{ontology}/actionTypes`
- `GET /api/v2/ontologies/{ontology}/actionTypes/byRid/{actionTypeRid}`
- `GET /api/v2/ontologies/{ontology}/actionTypes/{actionType}`
- `GET /api/v2/ontologies/{ontology}/fullMetadata`
- `GET /api/v2/ontologies/{ontology}/interfaceTypes`
- `GET /api/v2/ontologies/{ontology}/interfaceTypes/{interfaceType}`
- `GET /api/v2/ontologies/{ontology}/objectTypes`
- `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}`
- `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}/fullMetadata`
- `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes`
- `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes/{linkType}`
- `GET /api/v2/ontologies/{ontology}/objects/{objectType}`
- `POST /api/v2/ontologies/{ontology}/objects/{objectType}/search`
- `GET /api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}`
- `GET /api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/links/{linkType}`
- `GET /api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/links/{linkType}/{linkedObjectPrimaryKey}`
- `GET /api/v2/ontologies/{ontology}/queryTypes`
- `GET /api/v2/ontologies/{ontology}/queryTypes/{queryApiName}`
- `GET /api/v2/ontologies/{ontology}/sharedPropertyTypes`
- `GET /api/v2/ontologies/{ontology}/sharedPropertyTypes/{sharedPropertyType}`
- `GET /api/v2/ontologies/{ontology}/valueTypes`
- `GET /api/v2/ontologies/{ontology}/valueTypes/{valueType}`

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
- `GET /api/v1/lineage/column-lineage`
- `GET /api/v1/lineage/diff`
- `GET /api/v1/lineage/graph`
- `GET /api/v1/lineage/impact`
- `GET /api/v1/lineage/metrics`
- `GET /api/v1/lineage/out-of-date`
- `GET /api/v1/lineage/path`
- `GET /api/v1/lineage/run-impact`
- `GET /api/v1/lineage/runs`
- `GET /api/v1/lineage/timeline`

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
- `POST /api/v1/objectify/databases/{db_name}/datasets/{dataset_id}/detect-relationships`
- `POST /api/v1/objectify/databases/{db_name}/run-dag`
- `POST /api/v1/objectify/datasets/{dataset_id}/run`
- `GET /api/v1/objectify/mapping-specs`
- `POST /api/v1/objectify/mapping-specs`
- `POST /api/v1/objectify/mapping-specs/{mapping_spec_id}/trigger-incremental`
- `GET /api/v1/objectify/mapping-specs/{mapping_spec_id}/watermark`

### Ontology Agent
- `POST /api/v1/ontology-agent/runs`

### Ontology Extensions
- `GET /api/v1/databases/{db_name}/ontology/action-types`
- `POST /api/v1/databases/{db_name}/ontology/action-types`
- `GET /api/v1/databases/{db_name}/ontology/action-types/{resource_id}`
- `PUT /api/v1/databases/{db_name}/ontology/action-types/{resource_id}`
- `DELETE /api/v1/databases/{db_name}/ontology/action-types/{resource_id}`
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
- `GET /api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types`
- `GET /api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types/{link_type_api_name}`

### Ontology Management
- `POST /api/v1/databases/{db_name}/import-from-excel/commit`
- `POST /api/v1/databases/{db_name}/import-from-excel/dry-run`
- `POST /api/v1/databases/{db_name}/import-from-google-sheets/commit`
- `POST /api/v1/databases/{db_name}/import-from-google-sheets/dry-run`
- `POST /api/v1/databases/{db_name}/ontology`
- `POST /api/v1/databases/{db_name}/ontology/validate`
- `POST /api/v1/databases/{db_name}/ontology/{class_id}/mapping-metadata`
- `GET /api/v1/databases/{db_name}/ontology/{class_id}/schema`
- `POST /api/v1/databases/{db_name}/suggest-mappings`
- `POST /api/v1/databases/{db_name}/suggest-mappings-from-excel`
- `POST /api/v1/databases/{db_name}/suggest-mappings-from-google-sheets`
- `POST /api/v1/databases/{db_name}/suggest-schema-from-data`
- `POST /api/v1/databases/{db_name}/suggest-schema-from-excel`
- `POST /api/v1/databases/{db_name}/suggest-schema-from-google-sheets`

### Ontology Object Types
- `GET /api/v1/databases/{db_name}/ontology/object-types`
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
- `GET /api/v1/pipelines/udfs`
- `POST /api/v1/pipelines/udfs`
- `GET /api/v1/pipelines/udfs/{udf_id}`
- `POST /api/v1/pipelines/udfs/{udf_id}/versions`
- `GET /api/v1/pipelines/udfs/{udf_id}/versions/{version}`
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
- `GET /api/v1/pipeline-plans/{plan_id}`
- `POST /api/v1/pipeline-plans/{plan_id}/evaluate-joins`
- `POST /api/v1/pipeline-plans/{plan_id}/inspect-preview`
- `POST /api/v1/pipeline-plans/{plan_id}/preview`

### Query
- `POST /api/v1/databases/{db_name}/query`
- `GET /api/v1/databases/{db_name}/query/builder`

### Schema Changes
- `PUT /api/v1/schema-changes/drifts/{drift_id}/acknowledge`
- `GET /api/v1/schema-changes/history`
- `GET /api/v1/schema-changes/mappings/{mapping_spec_id}/compatibility`
- `GET /api/v1/schema-changes/stats`
- `GET /api/v1/schema-changes/subscriptions`
- `POST /api/v1/schema-changes/subscriptions`
- `DELETE /api/v1/schema-changes/subscriptions/{subscription_id}`

### Summary
- `GET /api/v1/summary`
