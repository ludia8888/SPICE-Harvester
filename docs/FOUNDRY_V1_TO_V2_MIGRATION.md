# Foundry v1 -> v2 Migration Guide

## Scope
This guide covers read/query routes and action execution routes that now have Foundry-style v2 successors.
It also documents the strict-compat baseline used to harden v2 wire/behavior parity.

## Cross-Check Baseline (2026-02-17)
- Official docs baseline follows `docs/FOUNDRY_ALIGNMENT_CHECKLIST.md` reference URLs.
- API v2 overview/index: `https://www.palantir.com/docs/foundry/api/v2`
- `Get Ontology Full Metadata` canonical URL: `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontologies/get-ontology-full-metadata`
- `Search Json Query` canonical reference: `https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-objects/search-objects` (request body contract)
- `Aggregate Objects` canonical reference: `https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-objects/aggregate-objects`
- `Outgoing Link Types` canonical references:
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/object-types/list-outgoing-link-types`
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/object-types/get-outgoing-link-type`
- `Object Sets` canonical references:
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-object-sets/load-object-set-objects`
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-object-sets/load-object-set-multiple-object-types`
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-object-sets/load-object-set-objects-or-interfaces`
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-object-sets/aggregate-object-set`
  - `https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-object-sets/create-temporary-object-set/`
- `Connectivity Table Imports` canonical reference: `https://www.palantir.com/docs/foundry/api/v2/connectivity-v2-resources/table-imports/create-table-import`
- `Dataset Schema` canonical references:
  - `https://www.palantir.com/docs/foundry/api/v2/datasets-v2-resources/datasets/get-dataset-schema`
  - `https://www.palantir.com/docs/foundry/api/datasets-v2-resources/datasets/put-dataset-schema`
- `Action Types` canonical references:
  - `https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/action-types/list-action-types`
  - `https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/action-types/get-action-type`
  - `https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/action-types/get-action-type-by-rid`
- `Actions` canonical references:
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/actions/apply-action`
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/actions/apply-action-batch`
- `Query Types` canonical references:
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/query-types/list-query-types`
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/query-types/get-query-type`
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/queries/execute-query`
- `Pipeline Builder / Orchestration` canonical references:
  - `https://www.palantir.com/docs/foundry/pipeline-builder/overview`
  - `https://www.palantir.com/docs/foundry/api/v2/orchestration-v2-resources/builds/create-build`
- Supplementary (non-authoritative) validation sources:
  - Foundry platform Python SDK: `https://github.com/palantir/foundry-platform-python`
  - Foundry SDK object-set preview contract (`load_links`): `https://github.com/palantir/foundry-platform-python/blob/develop/foundry_sdk/v2/ontologies/ontology_object_set.py`
  - Palantir Developer Community: `https://community.palantir.com/`

## P0 Parity Hardening Update (2026-02-17)
- Object search/read payload alignment: object rows now include Foundry-required locator fields (`__apiName`, `__primaryKey`) and synthesize `__rid` when absent; null-valued fields are omitted from response payloads.
- Object aggregate parity: `POST /api/v2/ontologies/{ontology}/objects/{objectType}/aggregate` is now exposed and aligned with Foundry aggregate clause shapes.
- ObjectSet aggregate correctness: `POST /api/v2/ontologies/{ontology}/objectSets/aggregate` now paginates through all result pages instead of aggregating only the first page.
- Action error envelope alignment: non-Foundry upstream action validation errors (`code/category/message`) are normalized into Foundry error envelope (`errorCode/errorName/errorInstanceId/parameters`).
- Pagination token compatibility: Foundry v2 page token acceptance window increased from 60 seconds to 24 hours on OMS/BFF v2 read surfaces.

## Deprecation Policy
- v2 successor가 있는 legacy read/query compat 엔드포인트는 코드에서 완전 제거되었습니다.
- 제거된 operation은 OpenAPI에서 노출되지 않으며, 런타임에서도 더 이상 제공되지 않습니다.
- CI static guard(`scripts/architecture_guard.py`)가 production code의 legacy path literal 재유입(`actions/logs*`, `actions/simulations*`, `/api/v1/actions/*`, `/api/v1/funnel/*`, `/api/v1/version/*`, `/api/v1/branch/*`)을 차단합니다.

### Removed v1 compatibility routes (code deleted)
These routes are fully deleted from runtime handlers and OpenAPI:
- `GET /api/v1/databases/{db_name}/ontology/object-types`
- `GET /api/v1/databases/{db_name}/ontology/object-types/{class_id}`
- `GET /api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types`
- `GET /api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types/{link_type_api_name}`
- `POST /api/v1/databases/{db_name}/ontology/object-types`
- `PUT /api/v1/databases/{db_name}/ontology/object-types/{class_id}`
- `POST /api/v1/databases/{db_name}/ontology/validate`
- `GET /api/v1/databases/{db_name}/ontology/{class_id}/schema`
- `POST /api/v1/databases/{db_name}/ontology/{class_id}/mapping-metadata`
- `GET /api/v1/databases/{db_name}/ontology/proposals`
- `POST /api/v1/databases/{db_name}/ontology/proposals`
- `POST /api/v1/databases/{db_name}/ontology/proposals/{proposal_id}/approve`
- `POST /api/v1/databases/{db_name}/ontology/deploy`
- `GET /api/v1/databases/{db_name}/ontology/health`
- `POST /api/v1/databases/{db_name}/query`
- `GET /api/v1/databases/{db_name}/query/builder`
- `POST /api/v1/databases/{db_name}/actions/{action_type_id}/simulate`
- `POST /api/v1/databases/{db_name}/actions/{action_type_id}/submit`
- `POST /api/v1/databases/{db_name}/actions/{action_type_id}/submit-batch`
- `POST /api/v1/databases/{db_name}/actions/logs/{action_log_id}/undo`
- `GET /api/v1/databases/{db_name}/actions/logs`
- `GET /api/v1/databases/{db_name}/actions/logs/{action_log_id}`
- `GET /api/v1/databases/{db_name}/actions/simulations`
- `GET /api/v1/databases/{db_name}/actions/simulations/{simulation_id}`
- `GET /api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions`
- `GET /api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions/{version}`
- `POST /api/v1/actions/{db_name}/async/{action_type_id}/submit`
- `POST /api/v1/actions/{db_name}/async/{action_type_id}/submit-batch`
- `POST /api/v1/actions/{db_name}/async/{action_type_id}/simulate`
- `POST /api/v1/actions/{db_name}/async/logs/{action_log_id}/undo`
- `GET /api/v1/databases/{db_name}/ontology/link-types`
- `GET /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}`
- `POST /api/v1/databases/{db_name}/ontology/link-types`
- `PUT /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}`
- `GET /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/edits`
- `POST /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/edits`
- `POST /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/reindex`
- `GET /api/v1/databases/{db_name}/ontology/action-types`
- `GET /api/v1/databases/{db_name}/ontology/action-types/{resource_id}`
- `POST /api/v1/databases/{db_name}/ontology/action-types`
- `PUT /api/v1/databases/{db_name}/ontology/action-types/{resource_id}`
- `DELETE /api/v1/databases/{db_name}/ontology/action-types/{resource_id}`
- `GET /api/v1/databases/{db_name}/ontology/functions`
- `GET /api/v1/databases/{db_name}/ontology/functions/{resource_id}`
- `POST /api/v1/databases/{db_name}/ontology/functions`
- `PUT /api/v1/databases/{db_name}/ontology/functions/{resource_id}`
- `DELETE /api/v1/databases/{db_name}/ontology/functions/{resource_id}`
- `GET /api/v1/databases/{db_name}/ontology/groups`
- `POST /api/v1/databases/{db_name}/ontology/groups`
- `GET /api/v1/databases/{db_name}/ontology/groups/{resource_id}`
- `PUT /api/v1/databases/{db_name}/ontology/groups/{resource_id}`
- `DELETE /api/v1/databases/{db_name}/ontology/groups/{resource_id}`
- `GET /api/v1/databases/{db_name}/ontology/interfaces`
- `GET /api/v1/databases/{db_name}/ontology/interfaces/{resource_id}`
- `POST /api/v1/databases/{db_name}/ontology/interfaces`
- `PUT /api/v1/databases/{db_name}/ontology/interfaces/{resource_id}`
- `DELETE /api/v1/databases/{db_name}/ontology/interfaces/{resource_id}`
- `GET /api/v1/databases/{db_name}/ontology/shared-properties`
- `GET /api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}`
- `POST /api/v1/databases/{db_name}/ontology/shared-properties`
- `PUT /api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}`
- `DELETE /api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}`
- `GET /api/v1/databases/{db_name}/ontology/value-types`
- `GET /api/v1/databases/{db_name}/ontology/value-types/{resource_id}`
- `POST /api/v1/databases/{db_name}/ontology/value-types`
- `PUT /api/v1/databases/{db_name}/ontology/value-types/{resource_id}`
- `DELETE /api/v1/databases/{db_name}/ontology/value-types/{resource_id}`
- `GET /api/v1/databases/{db_name}/classes`
- `GET /api/v1/databases/{db_name}/classes/{class_id}`
- `GET /api/v1/databases/{db_name}/class/{class_id}/instances`
- `GET /api/v1/databases/{db_name}/class/{class_id}/instance/{instance_id}`
- `POST /api/v1/databases/{db_name}/classes`
- `GET /api/v1/databases/{db_name}/class/{class_id}/sample-values`
- `POST /api/v1/objects/{db_name}/{object_type}/search`
- `POST /api/v1/pipeline-plans/compile`
- `GET /api/v1/pipeline-plans/{plan_id}`
- `POST /api/v1/pipeline-plans/{plan_id}/preview`
- `POST /api/v1/pipeline-plans/{plan_id}/inspect-preview`
- `POST /api/v1/pipeline-plans/{plan_id}/evaluate-joins`
- `POST /api/v1/pipelines/simulate-definition`
- `GET /api/v1/pipelines/branches`
- `POST /api/v1/pipelines/branches/{branch}/archive`
- `POST /api/v1/pipelines/branches/{branch}/restore`
- `POST /api/v1/pipelines/{pipeline_id}/branches`
- `POST /api/v1/pipelines/datasets/{dataset_id}/versions/{version_id}/funnel-analysis`
- `POST /api/v1/databases/{db_name}/suggest-schema-from-data`
- `POST /api/v1/databases/{db_name}/suggest-mappings`
- `POST /api/v1/databases/{db_name}/suggest-mappings-from-google-sheets`
- `POST /api/v1/databases/{db_name}/suggest-mappings-from-excel`
- `POST /api/v1/databases/{db_name}/suggest-schema-from-google-sheets`
- `POST /api/v1/databases/{db_name}/suggest-schema-from-excel`
- `POST /api/v1/databases/{db_name}/import-from-google-sheets/dry-run`
- `POST /api/v1/databases/{db_name}/import-from-google-sheets/commit`
- `POST /api/v1/databases/{db_name}/import-from-excel/dry-run`
- `POST /api/v1/databases/{db_name}/import-from-excel/commit`
- `POST /api/v1/data-connectors/google-sheets/grid`
- `POST /api/v1/data-connectors/google-sheets/preview`

### Removed OMS legacy governance routes (code deleted)
These OMS-internal v1 governance endpoints are also removed from runtime handlers and OpenAPI:
- `GET /api/v1/database/{db_name}/ontology/proposals`
- `POST /api/v1/database/{db_name}/ontology/proposals`
- `POST /api/v1/database/{db_name}/ontology/proposals/{proposal_id}/approve`
- `POST /api/v1/database/{db_name}/ontology/deploy`
- `GET /api/v1/database/{db_name}/ontology/health`
- `GET /api/v1/database/{db_name}/pull-requests`
- `POST /api/v1/database/{db_name}/pull-requests`
- `GET /api/v1/database/{db_name}/pull-requests/{pr_id}`
- `POST /api/v1/database/{db_name}/pull-requests/{pr_id}/merge`
- `POST /api/v1/database/{db_name}/pull-requests/{pr_id}/close`
- `GET /api/v1/database/{db_name}/pull-requests/{pr_id}/diff`

Legacy pull-request persistence schema is also retired with forward migration:
- `backend/database/migrations/016_remove_legacy_pull_requests.sql`

## Endpoint Mapping
| v1 | v2 successor |
|---|---|
| `GET /api/v1/databases/{db_name}/ontology/object-types` | `GET /api/v2/ontologies/{ontology}/objectTypes` |
| `GET /api/v1/databases/{db_name}/ontology/object-types/{class_id}` | `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}` |
| `GET /api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types` | `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes` |
| `GET /api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types/{link_type_api_name}` | `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes/{linkType}` |
| `POST /api/v1/databases/{db_name}/ontology/object-types` | No direct Foundry public write endpoint |
| `PUT /api/v1/databases/{db_name}/ontology/object-types/{class_id}` | No direct Foundry public write endpoint |
| `POST /api/v1/databases/{db_name}/ontology/validate` | No direct Foundry public endpoint (ontology create-lint endpoint is product-internal behavior) |
| `GET /api/v1/databases/{db_name}/ontology/{class_id}/schema` | `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}/fullMetadata` |
| `POST /api/v1/databases/{db_name}/ontology/{class_id}/mapping-metadata` | No direct Foundry public endpoint (mapping-metadata annotation is product-internal behavior) |
| `GET /api/v1/databases/{db_name}/ontology/proposals` | No direct Foundry public endpoint (proposal listing is product-internal governance behavior) |
| `POST /api/v1/databases/{db_name}/ontology/proposals` | No direct Foundry public endpoint (proposal creation is product-internal governance behavior) |
| `POST /api/v1/databases/{db_name}/ontology/proposals/{proposal_id}/approve` | No direct Foundry public endpoint (proposal approval is product-internal governance behavior) |
| `POST /api/v1/databases/{db_name}/ontology/deploy` | No direct Foundry public endpoint (ontology deployment registration is product-internal governance behavior) |
| `GET /api/v1/databases/{db_name}/ontology/health` | No direct Foundry public endpoint (ontology health diagnostics are product-internal behavior) |
| `POST /api/v1/databases/{db_name}/query` | `POST /api/v2/ontologies/{ontology}/objects/{objectType}/search` |
| `POST /api/v1/objects/{db_name}/{object_type}/search` | `POST /api/v2/ontologies/{ontology}/objects/{objectType}/search` |
| `GET /api/v1/databases/{db_name}/query/builder` | No direct Foundry public action/query-resource endpoint |
| `POST /api/v1/pipeline-plans/compile` | No direct Foundry public endpoint (pipeline plan compile is product-internal behavior) |
| `GET /api/v1/pipeline-plans/{plan_id}` | No direct Foundry public endpoint (pipeline plan read is product-internal behavior) |
| `POST /api/v1/pipeline-plans/{plan_id}/preview` | No direct Foundry public endpoint (pipeline plan preview is product-internal behavior) |
| `POST /api/v1/pipeline-plans/{plan_id}/inspect-preview` | No direct Foundry public endpoint (pipeline plan inspection is product-internal behavior) |
| `POST /api/v1/pipeline-plans/{plan_id}/evaluate-joins` | No direct Foundry public endpoint (pipeline plan join evaluation is product-internal behavior) |
| `POST /api/v1/pipelines/simulate-definition` | No direct Foundry public endpoint (definition simulation endpoint is not exposed) |
| `GET /api/v1/pipelines/branches` | No direct Foundry public endpoint (pipeline branch management is product-internal behavior) |
| `POST /api/v1/pipelines/branches/{branch}/archive` | No direct Foundry public endpoint (pipeline branch archive is product-internal behavior) |
| `POST /api/v1/pipelines/branches/{branch}/restore` | No direct Foundry public endpoint (pipeline branch restore is product-internal behavior) |
| `POST /api/v1/pipelines/{pipeline_id}/branches` | No direct Foundry public endpoint (pipeline branch creation is product-internal behavior) |
| `POST /api/v1/pipelines/datasets/{dataset_id}/versions/{version_id}/funnel-analysis` | No direct Foundry public endpoint (tabular reanalysis is product-internal behavior) |
| `POST /api/v1/databases/{db_name}/suggest-schema-from-data` | No direct Foundry public endpoint (schema suggestion is product-internal behavior) |
| `POST /api/v1/databases/{db_name}/suggest-mappings` | No direct Foundry public endpoint (mapping suggestion is product-internal behavior) |
| `POST /api/v1/databases/{db_name}/suggest-mappings-from-google-sheets` | No direct Foundry public endpoint (mapping suggestion from connector sample is product-internal behavior) |
| `POST /api/v1/databases/{db_name}/suggest-mappings-from-excel` | No direct Foundry public endpoint (mapping suggestion from file sample is product-internal behavior) |
| `POST /api/v1/databases/{db_name}/suggest-schema-from-google-sheets` | No direct Foundry public endpoint (schema suggestion from connector sample is product-internal behavior) |
| `POST /api/v1/databases/{db_name}/suggest-schema-from-excel` | No direct Foundry public endpoint (schema suggestion from file sample is product-internal behavior) |
| `POST /api/v1/databases/{db_name}/import-from-google-sheets/dry-run` | No direct Foundry public endpoint (instance import preview is product-internal behavior) |
| `POST /api/v1/databases/{db_name}/import-from-google-sheets/commit` | No direct Foundry public endpoint (instance import commit is product-internal behavior) |
| `POST /api/v1/databases/{db_name}/import-from-excel/dry-run` | No direct Foundry public endpoint (instance import preview is product-internal behavior) |
| `POST /api/v1/databases/{db_name}/import-from-excel/commit` | No direct Foundry public endpoint (instance import commit is product-internal behavior) |
| `POST /api/v1/data-connectors/google-sheets/grid` | No direct Foundry public endpoint (tabular connector grid extraction is product-internal behavior) |
| `POST /api/v1/data-connectors/google-sheets/preview` | No direct Foundry public endpoint (tabular connector preview is product-internal behavior) |
| `POST /api/v1/databases/{db_name}/actions/{action_type_id}/simulate` | `POST /api/v2/ontologies/{ontology}/actions/{action}/apply` (`options.mode=VALIDATE_ONLY`) |
| `POST /api/v1/databases/{db_name}/actions/{action_type_id}/submit` | `POST /api/v2/ontologies/{ontology}/actions/{action}/apply` |
| `POST /api/v1/actions/{db_name}/async/{action_type_id}/submit` | `POST /api/v2/ontologies/{ontology}/actions/{action}/apply` |
| `POST /api/v1/actions/{db_name}/async/{action_type_id}/submit-batch` | `POST /api/v2/ontologies/{ontology}/actions/{action}/applyBatch` |
| `POST /api/v1/actions/{db_name}/async/{action_type_id}/simulate` | `POST /api/v2/ontologies/{ontology}/actions/{action}/apply` (`options.mode=VALIDATE_ONLY`) |
| `POST /api/v1/databases/{db_name}/actions/{action_type_id}/submit-batch` | `POST /api/v2/ontologies/{ontology}/actions/{action}/applyBatch` |
| `POST /api/v1/databases/{db_name}/actions/logs/{action_log_id}/undo` | No direct Foundry public action-resource endpoint |
| `GET /api/v1/databases/{db_name}/actions/logs` | No direct Foundry public action-resource endpoint |
| `GET /api/v1/databases/{db_name}/actions/logs/{action_log_id}` | No direct Foundry public action-resource endpoint |
| `GET /api/v1/databases/{db_name}/actions/simulations` | No direct Foundry public action-resource endpoint |
| `GET /api/v1/databases/{db_name}/actions/simulations/{simulation_id}` | No direct Foundry public action-resource endpoint |
| `GET /api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions` | No direct Foundry public action-resource endpoint |
| `GET /api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions/{version}` | No direct Foundry public action-resource endpoint |
| `GET /api/v1/databases/{db_name}/ontology/link-types` | `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes` |
| `GET /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}` | `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes/{linkType}` |
| `POST /api/v1/databases/{db_name}/ontology/link-types` | No direct Foundry public write endpoint |
| `PUT /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}` | No direct Foundry public write endpoint |
| `GET /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/edits` | No direct Foundry public write endpoint |
| `POST /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/edits` | No direct Foundry public write endpoint |
| `POST /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/reindex` | No direct Foundry public write endpoint |
| `GET /api/v1/databases/{db_name}/ontology/action-types` | `GET /api/v2/ontologies/{ontology}/actionTypes` |
| `GET /api/v1/databases/{db_name}/ontology/action-types/{resource_id}` | `GET /api/v2/ontologies/{ontology}/actionTypes/{actionType}` |
| `POST /api/v1/databases/{db_name}/ontology/action-types` | No direct Foundry public write endpoint |
| `PUT /api/v1/databases/{db_name}/ontology/action-types/{resource_id}` | No direct Foundry public write endpoint |
| `DELETE /api/v1/databases/{db_name}/ontology/action-types/{resource_id}` | No direct Foundry public write endpoint |
| `GET /api/v1/databases/{db_name}/ontology/functions` | `GET /api/v2/ontologies/{ontology}/queryTypes` |
| `GET /api/v1/databases/{db_name}/ontology/functions/{resource_id}` | `GET /api/v2/ontologies/{ontology}/queryTypes/{queryApiName}` |
| `POST /api/v1/databases/{db_name}/ontology/functions` | No direct Foundry public write endpoint |
| `PUT /api/v1/databases/{db_name}/ontology/functions/{resource_id}` | No direct Foundry public write endpoint |
| `DELETE /api/v1/databases/{db_name}/ontology/functions/{resource_id}` | No direct Foundry public write endpoint |
| `GET /api/v1/databases/{db_name}/ontology/groups` | No direct Foundry public counterpart |
| `POST /api/v1/databases/{db_name}/ontology/groups` | No direct Foundry public counterpart |
| `GET /api/v1/databases/{db_name}/ontology/groups/{resource_id}` | No direct Foundry public counterpart |
| `PUT /api/v1/databases/{db_name}/ontology/groups/{resource_id}` | No direct Foundry public counterpart |
| `DELETE /api/v1/databases/{db_name}/ontology/groups/{resource_id}` | No direct Foundry public counterpart |
| `GET /api/v1/databases/{db_name}/ontology/interfaces` | `GET /api/v2/ontologies/{ontology}/interfaceTypes` |
| `GET /api/v1/databases/{db_name}/ontology/interfaces/{resource_id}` | `GET /api/v2/ontologies/{ontology}/interfaceTypes/{interfaceType}` |
| `POST /api/v1/databases/{db_name}/ontology/interfaces` | No direct Foundry public write endpoint |
| `PUT /api/v1/databases/{db_name}/ontology/interfaces/{resource_id}` | No direct Foundry public write endpoint |
| `DELETE /api/v1/databases/{db_name}/ontology/interfaces/{resource_id}` | No direct Foundry public write endpoint |
| `GET /api/v1/databases/{db_name}/ontology/shared-properties` | `GET /api/v2/ontologies/{ontology}/sharedPropertyTypes` |
| `GET /api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}` | `GET /api/v2/ontologies/{ontology}/sharedPropertyTypes/{sharedPropertyType}` |
| `POST /api/v1/databases/{db_name}/ontology/shared-properties` | No direct Foundry public write endpoint |
| `PUT /api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}` | No direct Foundry public write endpoint |
| `DELETE /api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}` | No direct Foundry public write endpoint |
| `GET /api/v1/databases/{db_name}/ontology/value-types` | `GET /api/v2/ontologies/{ontology}/valueTypes` |
| `GET /api/v1/databases/{db_name}/ontology/value-types/{resource_id}` | `GET /api/v2/ontologies/{ontology}/valueTypes/{valueType}` |
| `POST /api/v1/databases/{db_name}/ontology/value-types` | No direct Foundry public write endpoint |
| `PUT /api/v1/databases/{db_name}/ontology/value-types/{resource_id}` | No direct Foundry public write endpoint |
| `DELETE /api/v1/databases/{db_name}/ontology/value-types/{resource_id}` | No direct Foundry public write endpoint |
| `GET /api/v1/databases/{db_name}/classes` | `GET /api/v2/ontologies/{ontology}/objectTypes` |
| `GET /api/v1/databases/{db_name}/classes/{class_id}` | `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}` |
| `GET /api/v1/databases/{db_name}/class/{class_id}/instances` | `GET /api/v2/ontologies/{ontology}/objects/{objectType}` |
| `GET /api/v1/databases/{db_name}/class/{class_id}/instance/{instance_id}` | `GET /api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}` |
| `POST /api/v1/databases/{db_name}/classes` | No direct Foundry public write endpoint |
| `GET /api/v1/databases/{db_name}/class/{class_id}/sample-values` | No direct Foundry public endpoint (sample-values profile is product-internal behavior) |
| (new in v2) | `POST /api/v2/ontologies/{ontology}/actions/{action}/apply` |
| (new in v2) | `POST /api/v2/ontologies/{ontology}/actions/{action}/applyBatch` |
| (new in v2) | `GET /api/v2/ontologies/{ontology}/fullMetadata` |
| (new in v2) | `GET /api/v2/ontologies/{ontology}/actionTypes` |
| (new in v2) | `GET /api/v2/ontologies/{ontology}/actionTypes/{actionType}` |
| (new in v2) | `GET /api/v2/ontologies/{ontology}/actionTypes/byRid/{actionTypeRid}` |
| (new in v2) | `GET /api/v2/ontologies/{ontology}/queryTypes` |
| (new in v2) | `GET /api/v2/ontologies/{ontology}/queryTypes/{queryApiName}` |
| (new in v2) | `POST /api/v2/ontologies/{ontology}/queries/{queryApiName}/execute` |
| (new in v2, preview param supported) | `GET /api/v2/ontologies/{ontology}/interfaceTypes` |
| (new in v2, preview param supported) | `GET /api/v2/ontologies/{ontology}/interfaceTypes/{interfaceType}` |
| (new in v2, preview param supported) | `GET /api/v2/ontologies/{ontology}/valueTypes` |
| (new in v2, preview param supported) | `GET /api/v2/ontologies/{ontology}/valueTypes/{valueType}` |
| (new in v2) | `POST /api/v2/ontologies/{ontology}/objectSets/loadObjects` |
| (new in v2, preview param supported) | `POST /api/v2/ontologies/{ontology}/objectSets/loadLinks` |
| (new in v2, preview param supported) | `POST /api/v2/ontologies/{ontology}/objectSets/loadObjectsMultipleObjectTypes` |
| (new in v2, preview param supported) | `POST /api/v2/ontologies/{ontology}/objectSets/loadObjectsOrInterfaces` |
| (new in v2) | `POST /api/v2/ontologies/{ontology}/objectSets/aggregate` |
| (new in v2) | `POST /api/v2/ontologies/{ontology}/objectSets/createTemporary` |
| (new in v2) | `GET /api/v2/ontologies/{ontology}/objectSets/{objectSetRid}` |

`{ontology}` is the ontology API name (usually same value as `db_name` in current deployments).

## Contract Changes
### Foundry Surface Boundary
- Foundry 공식 문서 기준으로 tabular type inference / sheet structure analysis는 public REST contract가 아닙니다.
- tabular type inference runtime은 내부(in-process) 컴포넌트로 고정되며, external Funnel HTTP transport mode는 제거되었습니다.
- 기본 gateway/nginx 구성에서는 Funnel 전용 프록시 경로(`/api/funnel/*`, `/health/funnel`)를 노출하지 않습니다.
- compose/deploy/test 기본 경로에서도 별도 Funnel 서비스 부팅 단계가 존재하지 않습니다.
- standalone Funnel 서비스 아티팩트(`backend/start_funnel.sh`, `backend/funnel/Dockerfile`, `backend/funnel/requirements.txt`)는 제거되었고, Funnel 로직은 BFF 내부 in-process runtime으로만 사용됩니다.
- `backend/funnel/main.py`의 standalone 실행 경로도 제거되어(실행 시 종료), external Funnel 서비스 프로세스는 기본 런타임에서 지원되지 않습니다.
- 내부 Funnel runtime 라우트도 레거시 버전 경로(`/api/v1/funnel/*`)를 사용하지 않으며, internal namespace(`/internal/funnel/*`)로 고정됩니다.
- Pipeline dataset ingest/read 응답의 legacy 키 `funnel_analysis`는 제거되고 `tabular_analysis`로 통일됩니다(Foundry public surface 비정합 내부 용어 축소 목적).
- 내부 Funnel의 Google Sheets 구조분석 경로도 BFF HTTP 왕복(`/api/v1/data-connectors/google-sheets/grid`)을 사용하지 않고, connector library(`GoogleSheetsService`) 직접 호출로 고정됩니다.
- `instance-worker`의 relationship object 동기화 경로는 더 이상 legacy BFF `POST /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/reindex`를 호출하지 않으며, ObjectifyRegistry/DatasetRegistry 기반 내부 enqueue로 동작합니다.
- BFF 내부의 legacy link-type 라우터 composition shim (`bff/routers/link_types.py`)과 helper shim (`bff/routers/link_types_ops.py`)은 코드 삭제되었고, 링크 타입 매핑 로직은 service module(`bff/services/link_types_mapping_service.py`)로 고정됩니다.
- Action: 외부 통합 계약은 `/api/v2/ontologies/*` 중심으로 유지하고, Funnel 계열 경로를 신규 공개 계약으로 추가하지 않습니다.

### Pagination
- v1: 일부 경로에서 base64 offset 토큰 사용
- v2: opaque `pageToken` 사용 (scope-bound + TTL)
- v2 token 재사용 시 `pageSize`/요청 파라미터가 동일해야 함
- Action: page token을 직접 생성하지 말고, 이전 응답의 `nextPageToken`만 전달

### Errors
- v1: 서비스별 에러 포맷 혼재
- v2: Foundry envelope 고정
  - `{errorCode, errorName, errorInstanceId, parameters}`
- 권한 실패는 `403 + PERMISSION_DENIED`, 입력 오류는 `400 + INVALID_ARGUMENT`
- Action: `errorName` 기반 분기 추가 (`OntologyNotFound`, `ObjectTypeNotFound`, `LinkTypeNotFound`, `ObjectNotFound`, `LinkedObjectNotFound`)

### Query DSL
- v2는 `SearchJsonQueryV2` 중심
- 텍스트 연산자는 `containsAnyTerm`, `containsAllTerms`, `containsAllTermsInOrder`, `containsAllTermsInOrderPrefixLastTerm` 사용
- `startsWith`는 Foundry 문서 기준 deprecated alias로 여전히 허용되지만 신규 코드는 비권장
- Action: 신규/마이그레이션 코드는 `containsAllTermsInOrderPrefixLastTerm`를 우선 사용

### Parameters
- v2 object read/search routes는 `branch` 외 `sdkPackageRid`, `sdkVersion`를 허용
- Action: SDK 기반 호출은 해당 파라미터를 전달 가능하도록 클라이언트 스키마 업데이트
- Foundry 문서의 preview 엔드포인트(`fullMetadata`, `interfaceTypes*`, `sharedPropertyTypes*`, `valueTypes*`, `objectTypes/*/fullMetadata`)는 warning 기준 `preview=true` 호출을 전제로 함
- 현재 구현은 strict compat 고정 동작으로 preview 엔드포인트에서 `preview=true`를 항상 강제
- Foundry/Postgres 런타임에서는 legacy branch-management API(`/api/v1/databases/{db_name}/branches*`, `/api/v1/databases/{db_name}/ontology/branches`)에 의존하지 않음
- Action: 제안/배포/OCC 호출은 branch API 조회 대신 `branch:<name>` 토큰 기준으로 처리
- `GET /api/v2/ontologies/{ontology}/actionTypes`는 `pageSize`, `pageToken`, `branch`를 사용하고 `preview`/`sdk*` 파라미터는 사용하지 않음
- `GET /api/v2/ontologies/{ontology}/actionTypes/{actionType}` 및 `/actionTypes/byRid/{actionTypeRid}`는 `branch`만 사용 (pagination/preview/sdks 미사용)
- `GET /api/v2/ontologies/{ontology}/queryTypes`는 `pageSize/pageToken`만 사용 (branch 미사용)
- `GET /api/v2/ontologies/{ontology}/queryTypes/{queryApiName}`는 `version`, `sdkPackageRid`, `sdkVersion`를 허용
- `POST /api/v2/ontologies/{ontology}/queries/{queryApiName}/execute`는 `version`, `sdkPackageRid`, `sdkVersion`, `transactionId`를 허용 (branch 미사용)
- `GET /api/v2/ontologies/{ontology}/valueTypes`는 pagination 파라미터를 받지 않음
- `POST /api/v2/ontologies/{ontology}/actions/{action}/apply`는 query로 `branch`, `sdkPackageRid`, `sdkVersion`, `transactionId`를 사용하고, 실행 모드는 body `options.mode`로 제어
- `POST /api/v2/ontologies/{ontology}/actions/{action}/applyBatch`는 query로 `branch`, `sdkPackageRid`, `sdkVersion`를 사용
- `POST /api/v2/ontologies/{ontology}/objectSets/loadObjects`는 query로 `branch`, `transactionId`, `sdkPackageRid`, `sdkVersion`를 사용하고 body로 `select/selectV2`, `pageSize/pageToken`, `orderBy`, `excludeRid`, `snapshot`, `includeComputeUsage`를 사용
- `POST /api/v2/ontologies/{ontology}/objectSets/loadLinks`는 query로 `branch`, `preview`, `sdkPackageRid`, `sdkVersion`를 사용하고 body로 `objectSet`, `links`, `pageToken`, `includeComputeUsage`를 사용
- `POST /api/v2/ontologies/{ontology}/objectSets/loadObjectsMultipleObjectTypes`는 query로 `branch`, `preview`, `transactionId`, `sdkPackageRid`, `sdkVersion`를 사용
- `POST /api/v2/ontologies/{ontology}/objectSets/loadObjectsOrInterfaces`는 query로 `branch`, `preview`, `sdkPackageRid`, `sdkVersion`를 사용
- `POST /api/v2/ontologies/{ontology}/objectSets/aggregate`는 query로 `branch`, `transactionId`, `sdkPackageRid`, `sdkVersion`를 사용하고 body로 `aggregation`, `groupBy`, `accuracy`, `includeComputeUsage`를 사용
- Action: preview 엔드포인트 호출 클라이언트는 `preview=true`를 기본값으로 고정

### Full Metadata Shape
- `GET /api/v2/ontologies/{ontology}/fullMetadata` 응답은 top-level `ontology` 객체를 포함
- `branch` 필드는 `{"rid": "<branch>"}`를 사용
- `queryTypes` map key는 `VersionedQueryTypeApiName` (`{apiName}:{version}`) 형식 사용

### Strict Compat Baseline (P0 hardening)
- Foundry v2 strict wire/행동 계약은 런타임 기본이 아니라 고정 동작입니다(옵트아웃/완화 게이트 제거).
- 고정된 핵심 계약:
  - v2 object/link 응답 필수 필드 자동 보정
  - unresolved outgoing link type 처리 엄격화 (list에서 제외, get은 `404 LinkTypeNotFound`)
  - preview 엔드포인트(`fullMetadata`, `interfaceTypes*`, `sharedPropertyTypes*`, `valueTypes*`, `objectTypes/*/fullMetadata`)는 `preview=true` 미지정 시 `400 INVALID_ARGUMENT` + `errorName=ApiFeaturePreviewUsageOnly`
  - ontology extension resource OCC 엄격화 (`expected_head_commit` 공백 `400 INVALID_ARGUMENT`, 불일치 `409 CONFLICT`)

### Action Execution (Foundry-style)
- Batch apply: `POST /v2/ontologies/{ontology}/actions/{action}/applyBatch`는 v2 공식 경로입니다.
- Dependency trigger: batch item은 `dependencies`(`on`, `trigger_on`)로 선행 액션 완료 조건을 정의
- Undo/revert는 Foundry 공개 action-resource 표면에 별도 리소스로 노출되지 않으며, 공개 v2 정합성 범위는 `apply`/`applyBatch`에 한정됩니다.
- ActionLog/Simulation dedicated read routes는 Foundry 공식 공개 action-resource surface에 포함되지 않습니다.
- Runtime note: OMS public OpenAPI에서도 `/api/v2/ontologies/{ontology}/actions/logs/{actionLogId}/undo`는 제거되며, action-worker의 `direct_undo` 실행 경로도 삭제되어 undo/revert는 런타임 표준 경로에서 제외됩니다.
- Runtime note: BFF -> OMS 내부 프록시도 `/api/v2/ontologies/{ontology}/actions/*` 경로를 사용하며, legacy `/api/v1/actions/{db_name}/async/*` 경로 의존은 제거되었습니다.
- Runtime note: `apply` + `options.mode=VALIDATE_ONLY` 경로는 내부 simulation 레지스트리 상태를 더 이상 생성/갱신하지 않으며, Foundry 공개 계약에 맞춰 validation 결과 payload만 반환합니다.
- Runtime note: `apply`의 기본 실행 경로(`VALIDATE_AND_EXECUTE`)도 응답으로 validation payload(`validation` + top-level `parameters`)를 반환하도록 고정되어, 성공 시 빈 객체가 아닌 Foundry-style validation envelope을 제공합니다.
- Runtime note: OMS action runtime에서 `ActionSimulationRegistry` 의존을 제거해 apply 실행 경로와 내부 simulation 버전 저장 모델 간 결합을 해소했습니다.
- Runtime note: legacy simulation registry module은 코드에서 제거되었고, legacy persistence schema/tables는 `backend/database/migrations/017_remove_legacy_action_simulations.sql`로 정리됩니다.
- Runtime note: live action writeback E2E suites도 OMS v1 async submit-batch 경로를 더 이상 호출하지 않으며, v2 `apply` 호출 + action log 조회 방식으로 동작합니다.
- 2026-02-17 공식문서 재검증: Foundry Actions 공개 엔드포인트는 `POST /v2/ontologies/{ontology}/actions/{action}/apply`, `POST /v2/ontologies/{ontology}/actions/{action}/applyBatch`이며, logs/simulations 계열 전용 read/write 엔드포인트는 공식 v2 Actions 표면에 포함되지 않습니다 (`https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/actions/action-basics/`, `https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/actions/apply-action`, `https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/actions/apply-action-batch`).
- Runtime note: BFF Foundry v2 read/search 내부 프록시는 OMS v1 object-search 경로(`/api/v1/objects/{db}/{objectType}/search`)를 더 이상 사용하지 않고, OMS v2 경로(`/api/v2/ontologies/{ontology}/objects/{objectType}/search`)를 사용합니다.
- Runtime note: OMS public OpenAPI에서도 legacy object-search 경로(`/api/v1/objects/{db_name}/{object_type}/search`)는 제거되고, Foundry v2 object-search 경로(`/api/v2/ontologies/{ontology}/objects/{objectType}/search`)만 노출됩니다.
- Runtime note: BFF public OpenAPI에서도 legacy pipeline planner/simulation/branch 경로(`/api/v1/pipeline-plans/compile`, `/api/v1/pipeline-plans/{plan_id}`, `/api/v1/pipeline-plans/{plan_id}/preview`, `/api/v1/pipeline-plans/{plan_id}/inspect-preview`, `/api/v1/pipeline-plans/{plan_id}/evaluate-joins`, `/api/v1/pipelines/simulate-definition`, `/api/v1/pipelines/branches`, `/api/v1/pipelines/branches/{branch}/archive`, `/api/v1/pipelines/branches/{branch}/restore`, `/api/v1/pipelines/{pipeline_id}/branches`)는 제거되며, Foundry 공개 API 정합성 범위에서 제외됩니다.
- 제약:
  - `trigger_on`은 `SUCCEEDED|FAILED|COMPLETED`만 허용

## Recommended Cutover Steps
1. v2 라우트로 읽기/검색 요청을 먼저 전환
2. v1 page token 생성 로직 삭제
3. v2 에러 스키마(`errorName`) 기반 처리로 교체
4. 모니터링에서 v1 호출량을 0으로 수렴
5. legacy v1 read/query 의존성 제거 완료
