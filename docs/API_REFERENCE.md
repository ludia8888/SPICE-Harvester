# API Reference (Auto-Generated)

> Generated from BFF OpenAPI by `scripts/generate_api_reference.py`.
> Do not edit manually.

## OpenAPI Metadata

- Title: `BFF (Backend for Frontend) Service`
- Version: `2.0.0`
- OpenAPI: `3.1.0`

## Developer Quick Start

> [!TIP]
> New API consumer quick-start (recommended order)

1. Confirm service liveness and auth behavior with a small read endpoint.
2. Enumerate ontology scope: `/api/v2/ontologies`
3. Inspect full metadata contract: `/api/v2/ontologies/{ontology}/fullMetadata`
4. Execute object search queries: `/api/v2/ontologies/{ontology}/objects/{objectType}/search`
5. Validate lineage visibility for impact analysis: `-`

## Endpoint Coverage Summary

- Total documented endpoints: **270**
- Deprecated endpoints: **0**
- Security-enabled endpoints: **0**

| API Version | Endpoint Count |
| --- | --- |
| `v1` | 246 |
| `v2` | 24 |

| Top Domains (first path segment) | Endpoint Count |
| --- | --- |
| `databases` | 89 |
| `pipelines` | 35 |
| `ontologies` | 24 |
| `admin` | 13 |
| `data-connectors` | 13 |
| `lineage` | 10 |
| `monitoring` | 10 |
| `config` | 9 |
| `context7` | 7 |
| `objectify` | 7 |
| `schema-changes` | 7 |
| `backing-datasources` | 5 |
| `graph-query` | 5 |
| `pipeline-plans` | 5 |
| `tasks` | 5 |

## Endpoint Catalog (`/api/v1`, `/api/v2`)

### Actions

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/databases/{db_name}/actions/logs` | List Action Logs | `v1` | no | no | `list_action_logs_api_v1_databases__db_name__actions_logs_get` |
| `GET` | `/api/v1/databases/{db_name}/actions/logs/{action_log_id}` | Get Action Log | `v1` | no | no | `get_action_log_api_v1_databases__db_name__actions_logs__action_log_id__get` |
| `POST` | `/api/v1/databases/{db_name}/actions/logs/{action_log_id}/undo` | Undo Action | `v1` | no | no | `undo_action_api_v1_databases__db_name__actions_logs__action_log_id__undo_post` |
| `GET` | `/api/v1/databases/{db_name}/actions/simulations` | List Action Simulations | `v1` | no | no | `list_action_simulations_api_v1_databases__db_name__actions_simulations_get` |
| `GET` | `/api/v1/databases/{db_name}/actions/simulations/{simulation_id}` | Get Action Simulation | `v1` | no | no | `get_action_simulation_api_v1_databases__db_name__actions_simulations__simulation_id__get` |
| `GET` | `/api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions` | List Action Simulation Versions | `v1` | no | no | `list_action_simulation_versions_api_v1_databases__db_name__actions_simulations__simulation_id__versions_get` |
| `GET` | `/api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions/{version}` | Get Action Simulation Version | `v1` | no | no | `get_action_simulation_version_api_v1_databases__db_name__actions_simulations__simulation_id__versions__version__get` |
| `POST` | `/api/v1/databases/{db_name}/actions/{action_type_id}/simulate` | Simulate Action | `v1` | no | no | `simulate_action_api_v1_databases__db_name__actions__action_type_id__simulate_post` |
| `POST` | `/api/v1/databases/{db_name}/actions/{action_type_id}/submit` | Submit Action | `v1` | no | no | `submit_action_api_v1_databases__db_name__actions__action_type_id__submit_post` |
| `POST` | `/api/v1/databases/{db_name}/actions/{action_type_id}/submit-batch` | Submit Action Batch | `v1` | no | no | `submit_action_batch_api_v1_databases__db_name__actions__action_type_id__submit_batch_post` |

### Admin

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `POST` | `/api/v1/admin/ci/ci-results` | Ingest Ci Result | `v1` | no | no | `ingest_ci_result_api_v1_admin_ci_ci_results_post` |

### Admin Operations

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `POST` | `/api/v1/admin/cleanup-old-replays` | Cleanup Old Replay Results | `v1` | no | no | `cleanup_old_replay_results_api_v1_admin_cleanup_old_replays_post` |
| `POST` | `/api/v1/admin/databases/{db_name}/rebuild-index` | Rebuild Instance Index Endpoint | `v1` | no | no | `rebuild_instance_index_endpoint_api_v1_admin_databases__db_name__rebuild_index_post` |
| `GET` | `/api/v1/admin/databases/{db_name}/rebuild-index/{task_id}/status` | Get Rebuild Status | `v1` | no | no | `get_rebuild_status_api_v1_admin_databases__db_name__rebuild_index__task_id__status_get` |
| `GET` | `/api/v1/admin/lakefs/credentials` | List Lakefs Credentials | `v1` | no | no | `list_lakefs_credentials_api_v1_admin_lakefs_credentials_get` |
| `POST` | `/api/v1/admin/lakefs/credentials` | Upsert Lakefs Credentials | `v1` | no | no | `upsert_lakefs_credentials_api_v1_admin_lakefs_credentials_post` |
| `POST` | `/api/v1/admin/recompute-projection` | Recompute Projection | `v1` | no | no | `recompute_projection_api_v1_admin_recompute_projection_post` |
| `GET` | `/api/v1/admin/recompute-projection/{task_id}/result` | Get Recompute Projection Result | `v1` | no | no | `get_recompute_projection_result_api_v1_admin_recompute_projection__task_id__result_get` |
| `POST` | `/api/v1/admin/reindex-instances` | Reindex all instances for a database (dataset-primary rebuild) | `v1` | no | no | `reindex_instances_endpoint_api_v1_admin_reindex_instances_post` |
| `POST` | `/api/v1/admin/replay-instance-state` | Replay Instance State | `v1` | no | no | `replay_instance_state_api_v1_admin_replay_instance_state_post` |
| `GET` | `/api/v1/admin/replay-instance-state/{task_id}/result` | Get Replay Result | `v1` | no | no | `get_replay_result_api_v1_admin_replay_instance_state__task_id__result_get` |
| `GET` | `/api/v1/admin/replay-instance-state/{task_id}/trace` | Get Replay Trace | `v1` | no | no | `get_replay_trace_api_v1_admin_replay_instance_state__task_id__trace_get` |
| `GET` | `/api/v1/admin/system-health` | Get System Health | `v1` | no | no | `get_system_health_api_v1_admin_system_health_get` |

### Agent

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `POST` | `/api/v1/agent/pipeline-runs` | Create Pipeline Run | `v1` | no | no | `create_pipeline_run_api_v1_agent_pipeline_runs_post` |
| `POST` | `/api/v1/agent/pipeline-runs/stream` | Stream Pipeline Run | `v1` | no | no | `stream_pipeline_run_api_v1_agent_pipeline_runs_stream_post` |

### AI

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `POST` | `/api/v1/ai/intent` | Ai Intent | `v1` | no | no | `ai_intent_api_v1_ai_intent_post` |
| `POST` | `/api/v1/ai/query/{db_name}` | Ai Query | `v1` | no | no | `ai_query_api_v1_ai_query__db_name__post` |
| `POST` | `/api/v1/ai/translate/query-plan/{db_name}` | Translate Query Plan | `v1` | no | no | `translate_query_plan_api_v1_ai_translate_query_plan__db_name__post` |

### Async Instance Management

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `POST` | `/api/v1/databases/{db_name}/instances/{class_label}/bulk-create` | Bulk Create Instances Async | `v1` | no | no | `bulk_create_instances_async_api_v1_databases__db_name__instances__class_label__bulk_create_post` |
| `POST` | `/api/v1/databases/{db_name}/instances/{class_label}/create` | Create Instance Async | `v1` | no | no | `create_instance_async_api_v1_databases__db_name__instances__class_label__create_post` |
| `DELETE` | `/api/v1/databases/{db_name}/instances/{class_label}/{instance_id}/delete` | Delete Instance Async | `v1` | no | no | `delete_instance_async_api_v1_databases__db_name__instances__class_label___instance_id__delete_delete` |
| `PUT` | `/api/v1/databases/{db_name}/instances/{class_label}/{instance_id}/update` | Update Instance Async | `v1` | no | no | `update_instance_async_api_v1_databases__db_name__instances__class_label___instance_id__update_put` |

### Audit

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/audit/chain-head` | Get Chain Head | `v1` | no | no | `get_chain_head_api_v1_audit_chain_head_get` |
| `GET` | `/api/v1/audit/logs` | List Audit Logs | `v1` | no | no | `list_audit_logs_api_v1_audit_logs_get` |

### Background Tasks

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/tasks/` | List Tasks | `v1` | no | no | `list_tasks_api_v1_tasks__get` |
| `GET` | `/api/v1/tasks/metrics/summary` | Get Task Metrics | `v1` | no | no | `get_task_metrics_api_v1_tasks_metrics_summary_get` |
| `GET` | `/api/v1/tasks/{task_id}` | Get Task Status | `v1` | no | no | `get_task_status_api_v1_tasks__task_id__get` |
| `DELETE` | `/api/v1/tasks/{task_id}` | Cancel Task | `v1` | no | no | `cancel_task_api_v1_tasks__task_id__delete` |
| `GET` | `/api/v1/tasks/{task_id}/result` | Get Task Result | `v1` | no | no | `get_task_result_api_v1_tasks__task_id__result_get` |

### Command Status

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/commands/{command_id}/status` | Get Command Status | `v1` | no | no | `get_command_status_api_v1_commands__command_id__status_get` |

### Config Monitoring

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/config/config/changes` | Configuration Changes | `v1` | no | no | `get_configuration_changes_api_v1_config_config_changes_get` |
| `POST` | `/api/v1/config/config/check-changes` | Check for Changes | `v1` | no | no | `check_configuration_changes_api_v1_config_config_check_changes_post` |
| `GET` | `/api/v1/config/config/current` | Current Configuration | `v1` | no | no | `get_current_configuration_api_v1_config_config_current_get` |
| `GET` | `/api/v1/config/config/drift-analysis` | Environment Drift Analysis | `v1` | no | no | `analyze_environment_drift_api_v1_config_config_drift_analysis_get` |
| `GET` | `/api/v1/config/config/health-impact` | Configuration Health Impact | `v1` | no | no | `analyze_configuration_health_impact_api_v1_config_config_health_impact_get` |
| `GET` | `/api/v1/config/config/monitoring-status` | Configuration Monitoring Status | `v1` | no | no | `get_monitoring_status_api_v1_config_config_monitoring_status_get` |
| `GET` | `/api/v1/config/config/report` | Configuration Report | `v1` | no | no | `get_configuration_report_api_v1_config_config_report_get` |
| `GET` | `/api/v1/config/config/security-audit` | Security Audit | `v1` | no | no | `perform_security_audit_api_v1_config_config_security_audit_get` |
| `GET` | `/api/v1/config/config/validation` | Configuration Validation | `v1` | no | no | `validate_configuration_api_v1_config_config_validation_get` |

### Context Tools

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `POST` | `/api/v1/context-tools/datasets/describe` | Describe Datasets | `v1` | no | no | `describe_datasets_api_v1_context_tools_datasets_describe_post` |
| `POST` | `/api/v1/context-tools/ontology/snapshot` | Snapshot Ontology | `v1` | no | no | `snapshot_ontology_api_v1_context_tools_ontology_snapshot_post` |

### context7

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `POST` | `/api/v1/context7/analyze/ontology` | Analyze Ontology | `v1` | no | no | `analyze_ontology_api_v1_context7_analyze_ontology_post` |
| `GET` | `/api/v1/context7/context/{entity_id}` | Get Entity Context | `v1` | no | no | `get_entity_context_api_v1_context7_context__entity_id__get` |
| `GET` | `/api/v1/context7/health` | Check Context7 Health | `v1` | no | no | `check_context7_health_api_v1_context7_health_get` |
| `POST` | `/api/v1/context7/knowledge` | Add Knowledge | `v1` | no | no | `add_knowledge_api_v1_context7_knowledge_post` |
| `POST` | `/api/v1/context7/link` | Create Entity Link | `v1` | no | no | `create_entity_link_api_v1_context7_link_post` |
| `POST` | `/api/v1/context7/search` | Search Context7 | `v1` | no | no | `search_context7_api_v1_context7_search_post` |
| `GET` | `/api/v1/context7/suggestions/{db_name}/{class_id}` | Get Ontology Suggestions | `v1` | no | no | `get_ontology_suggestions_api_v1_context7_suggestions__db_name___class_id__get` |

### Data Connectors

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/data-connectors/google-sheets/connections` | List Google Sheets connections | `v1` | no | no | `list_google_sheets_connections_api_v1_data_connectors_google_sheets_connections_get` |
| `DELETE` | `/api/v1/data-connectors/google-sheets/connections/{connection_id}` | Remove Google Sheets connection | `v1` | no | no | `delete_google_sheets_connection_api_v1_data_connectors_google_sheets_connections__connection_id__delete` |
| `GET` | `/api/v1/data-connectors/google-sheets/drive/spreadsheets` | List Google Sheets spreadsheets | `v1` | no | no | `list_google_sheets_spreadsheets_api_v1_data_connectors_google_sheets_drive_spreadsheets_get` |
| `POST` | `/api/v1/data-connectors/google-sheets/grid` | Extract Google Sheets grid + merges | `v1` | no | no | `extract_google_sheet_grid_api_v1_data_connectors_google_sheets_grid_post` |
| `GET` | `/api/v1/data-connectors/google-sheets/oauth/callback` | Google Sheets OAuth callback | `v1` | no | no | `google_sheets_oauth_callback_api_v1_data_connectors_google_sheets_oauth_callback_get` |
| `POST` | `/api/v1/data-connectors/google-sheets/oauth/start` | Start Google Sheets OAuth flow | `v1` | no | no | `start_google_sheets_oauth_api_v1_data_connectors_google_sheets_oauth_start_post` |
| `POST` | `/api/v1/data-connectors/google-sheets/preview` | Preview Google Sheet data (for Funnel) | `v1` | no | no | `preview_google_sheet_for_funnel_api_v1_data_connectors_google_sheets_preview_post` |
| `POST` | `/api/v1/data-connectors/google-sheets/register` | Register Google Sheet for data monitoring | `v1` | no | no | `register_google_sheet_api_v1_data_connectors_google_sheets_register_post` |
| `GET` | `/api/v1/data-connectors/google-sheets/registered` | List registered Google Sheets | `v1` | no | no | `list_registered_sheets_api_v1_data_connectors_google_sheets_registered_get` |
| `GET` | `/api/v1/data-connectors/google-sheets/spreadsheets/{sheet_id}/worksheets` | List worksheets for a spreadsheet | `v1` | no | no | `list_google_sheets_worksheets_api_v1_data_connectors_google_sheets_spreadsheets__sheet_id__worksheets_get` |
| `DELETE` | `/api/v1/data-connectors/google-sheets/{sheet_id}` | Unregister Google Sheet | `v1` | no | no | `unregister_google_sheet_api_v1_data_connectors_google_sheets__sheet_id__delete` |
| `GET` | `/api/v1/data-connectors/google-sheets/{sheet_id}/preview` | Preview registered Google Sheet data | `v1` | no | no | `preview_google_sheet_api_v1_data_connectors_google_sheets__sheet_id__preview_get` |
| `POST` | `/api/v1/data-connectors/google-sheets/{sheet_id}/start-pipelining` | Start pipelining from a registered Google Sheet | `v1` | no | no | `start_pipelining_google_sheet_api_v1_data_connectors_google_sheets__sheet_id__start_pipelining_post` |

### Database Management

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/databases` | List Databases | `v1` | no | no | `list_databases_api_v1_databases_get` |
| `POST` | `/api/v1/databases` | Create Database | `v1` | no | no | `create_database_api_v1_databases_post` |
| `GET` | `/api/v1/databases/{db_name}` | Get Database | `v1` | no | no | `get_database_api_v1_databases__db_name__get` |
| `DELETE` | `/api/v1/databases/{db_name}` | Delete Database | `v1` | no | no | `delete_database_api_v1_databases__db_name__delete` |
| `GET` | `/api/v1/databases/{db_name}/classes` | List Classes | `v1` | no | no | `list_classes_api_v1_databases__db_name__classes_get` |
| `POST` | `/api/v1/databases/{db_name}/classes` | Create Class | `v1` | no | no | `create_class_api_v1_databases__db_name__classes_post` |
| `GET` | `/api/v1/databases/{db_name}/classes/{class_id}` | Get Class | `v1` | no | no | `get_class_api_v1_databases__db_name__classes__class_id__get` |
| `GET` | `/api/v1/databases/{db_name}/expected-seq` | Get Database Expected Seq | `v1` | no | no | `get_database_expected_seq_api_v1_databases__db_name__expected_seq_get` |

### Document Bundles

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `POST` | `/api/v1/document-bundles/{bundle_id}/search` | Search Document Bundle | `v1` | no | no | `search_document_bundle_api_v1_document_bundles__bundle_id__search_post` |

### Foundry Ontologies v2

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v2/ontologies` | List Ontologies V2 | `v2` | no | no | `list_ontologies_v2_api_v2_ontologies_get` |
| `GET` | `/api/v2/ontologies/{ontology}` | Get Ontology V2 | `v2` | no | no | `get_ontology_v2_api_v2_ontologies__ontology__get` |
| `GET` | `/api/v2/ontologies/{ontology}/actionTypes` | List Action Types V2 | `v2` | no | no | `list_action_types_v2_api_v2_ontologies__ontology__actionTypes_get` |
| `GET` | `/api/v2/ontologies/{ontology}/actionTypes/byRid/{actionTypeRid}` | Get Action Type By Rid V2 | `v2` | no | no | `get_action_type_by_rid_v2_api_v2_ontologies__ontology__actionTypes_byRid__actionTypeRid__get` |
| `GET` | `/api/v2/ontologies/{ontology}/actionTypes/{actionType}` | Get Action Type V2 | `v2` | no | no | `get_action_type_v2_api_v2_ontologies__ontology__actionTypes__actionType__get` |
| `GET` | `/api/v2/ontologies/{ontology}/fullMetadata` | Get Full Metadata V2 | `v2` | no | no | `get_full_metadata_v2_api_v2_ontologies__ontology__fullMetadata_get` |
| `GET` | `/api/v2/ontologies/{ontology}/interfaceTypes` | List Interface Types V2 | `v2` | no | no | `list_interface_types_v2_api_v2_ontologies__ontology__interfaceTypes_get` |
| `GET` | `/api/v2/ontologies/{ontology}/interfaceTypes/{interfaceType}` | Get Interface Type V2 | `v2` | no | no | `get_interface_type_v2_api_v2_ontologies__ontology__interfaceTypes__interfaceType__get` |
| `GET` | `/api/v2/ontologies/{ontology}/objectTypes` | List Object Types V2 | `v2` | no | no | `list_object_types_v2_api_v2_ontologies__ontology__objectTypes_get` |
| `GET` | `/api/v2/ontologies/{ontology}/objectTypes/{objectType}` | Get Object Type V2 | `v2` | no | no | `get_object_type_v2_api_v2_ontologies__ontology__objectTypes__objectType__get` |
| `GET` | `/api/v2/ontologies/{ontology}/objectTypes/{objectType}/fullMetadata` | Get Object Type Full Metadata V2 | `v2` | no | no | `get_object_type_full_metadata_v2_api_v2_ontologies__ontology__objectTypes__objectType__fullMetadata_get` |
| `GET` | `/api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes` | List Outgoing Link Types V2 | `v2` | no | no | `list_outgoing_link_types_v2_api_v2_ontologies__ontology__objectTypes__objectType__outgoingLinkTypes_get` |
| `GET` | `/api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes/{linkType}` | Get Outgoing Link Type V2 | `v2` | no | no | `get_outgoing_link_type_v2_api_v2_ontologies__ontology__objectTypes__objectType__outgoingLinkTypes__linkType__get` |
| `GET` | `/api/v2/ontologies/{ontology}/objects/{objectType}` | List Objects V2 | `v2` | no | no | `list_objects_v2_api_v2_ontologies__ontology__objects__objectType__get` |
| `POST` | `/api/v2/ontologies/{ontology}/objects/{objectType}/search` | Search Objects V2 | `v2` | no | no | `search_objects_v2_api_v2_ontologies__ontology__objects__objectType__search_post` |
| `GET` | `/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}` | Get Object V2 | `v2` | no | no | `get_object_v2_api_v2_ontologies__ontology__objects__objectType___primaryKey__get` |
| `GET` | `/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/links/{linkType}` | List Linked Objects V2 | `v2` | no | no | `list_linked_objects_v2_api_v2_ontologies__ontology__objects__objectType___primaryKey__links__linkType__get` |
| `GET` | `/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/links/{linkType}/{linkedObjectPrimaryKey}` | Get Linked Object V2 | `v2` | no | no | `get_linked_object_v2_api_v2_ontologies__ontology__objects__objectType___primaryKey__links__linkType___linkedObjectPrimaryKey__get` |
| `GET` | `/api/v2/ontologies/{ontology}/queryTypes` | List Query Types V2 | `v2` | no | no | `list_query_types_v2_api_v2_ontologies__ontology__queryTypes_get` |
| `GET` | `/api/v2/ontologies/{ontology}/queryTypes/{queryApiName}` | Get Query Type V2 | `v2` | no | no | `get_query_type_v2_api_v2_ontologies__ontology__queryTypes__queryApiName__get` |
| `GET` | `/api/v2/ontologies/{ontology}/sharedPropertyTypes` | List Shared Property Types V2 | `v2` | no | no | `list_shared_property_types_v2_api_v2_ontologies__ontology__sharedPropertyTypes_get` |
| `GET` | `/api/v2/ontologies/{ontology}/sharedPropertyTypes/{sharedPropertyType}` | Get Shared Property Type V2 | `v2` | no | no | `get_shared_property_type_v2_api_v2_ontologies__ontology__sharedPropertyTypes__sharedPropertyType__get` |
| `GET` | `/api/v2/ontologies/{ontology}/valueTypes` | List Value Types V2 | `v2` | no | no | `list_value_types_v2_api_v2_ontologies__ontology__valueTypes_get` |
| `GET` | `/api/v2/ontologies/{ontology}/valueTypes/{valueType}` | Get Value Type V2 | `v2` | no | no | `get_value_type_v2_api_v2_ontologies__ontology__valueTypes__valueType__get` |

### Governance

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/access-policies` | List Access Policies | `v1` | no | no | `list_access_policies_api_v1_access_policies_get` |
| `POST` | `/api/v1/access-policies` | Upsert Access Policy | `v1` | no | no | `upsert_access_policy_api_v1_access_policies_post` |
| `GET` | `/api/v1/backing-datasource-versions/{version_id}` | Get Backing Datasource Version | `v1` | no | no | `get_backing_datasource_version_api_v1_backing_datasource_versions__version_id__get` |
| `GET` | `/api/v1/backing-datasources` | List Backing Datasources | `v1` | no | no | `list_backing_datasources_api_v1_backing_datasources_get` |
| `POST` | `/api/v1/backing-datasources` | Create Backing Datasource | `v1` | no | no | `create_backing_datasource_api_v1_backing_datasources_post` |
| `GET` | `/api/v1/backing-datasources/{backing_id}` | Get Backing Datasource | `v1` | no | no | `get_backing_datasource_api_v1_backing_datasources__backing_id__get` |
| `GET` | `/api/v1/backing-datasources/{backing_id}/versions` | List Backing Datasource Versions | `v1` | no | no | `list_backing_datasource_versions_api_v1_backing_datasources__backing_id__versions_get` |
| `POST` | `/api/v1/backing-datasources/{backing_id}/versions` | Create Backing Datasource Version | `v1` | no | no | `create_backing_datasource_version_api_v1_backing_datasources__backing_id__versions_post` |
| `GET` | `/api/v1/gate-policies` | List Gate Policies | `v1` | no | no | `list_gate_policies_api_v1_gate_policies_get` |
| `POST` | `/api/v1/gate-policies` | Upsert Gate Policy | `v1` | no | no | `upsert_gate_policy_api_v1_gate_policies_post` |
| `GET` | `/api/v1/gate-results` | List Gate Results | `v1` | no | no | `list_gate_results_api_v1_gate_results_get` |
| `GET` | `/api/v1/key-specs` | List Key Specs | `v1` | no | no | `list_key_specs_api_v1_key_specs_get` |
| `POST` | `/api/v1/key-specs` | Create Key Spec | `v1` | no | no | `create_key_spec_api_v1_key_specs_post` |
| `GET` | `/api/v1/key-specs/{key_spec_id}` | Get Key Spec | `v1` | no | no | `get_key_spec_api_v1_key_specs__key_spec_id__get` |
| `GET` | `/api/v1/schema-migration-plans` | List Schema Migration Plans | `v1` | no | no | `list_schema_migration_plans_api_v1_schema_migration_plans_get` |

### Graph

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/graph-query/health` | Graph Service Health | `v1` | no | no | `graph_service_health_api_v1_graph_query_health_get` |
| `POST` | `/api/v1/graph-query/{db_name}` | Execute Graph Query | `v1` | no | no | `execute_graph_query_api_v1_graph_query__db_name__post` |
| `POST` | `/api/v1/graph-query/{db_name}/multi-hop` | Execute Multi Hop Query | `v1` | no | no | `execute_multi_hop_query_api_v1_graph_query__db_name__multi_hop_post` |
| `GET` | `/api/v1/graph-query/{db_name}/paths` | Find Relationship Paths | `v1` | no | no | `find_relationship_paths_api_v1_graph_query__db_name__paths_get` |
| `POST` | `/api/v1/graph-query/{db_name}/simple` | Execute Simple Graph Query | `v1` | no | no | `execute_simple_graph_query_api_v1_graph_query__db_name__simple_post` |

### Health

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/` | Root | `v1` | no | no | `root_api_v1__get` |
| `GET` | `/api/v1/health` | Health Check | `v1` | no | no | `health_check_api_v1_health_get` |

### Instance Management

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/databases/{db_name}/class/{class_id}/instance/{instance_id}` | Get Instance | `v1` | no | no | `get_instance_api_v1_databases__db_name__class__class_id__instance__instance_id__get` |
| `GET` | `/api/v1/databases/{db_name}/class/{class_id}/instances` | Get Class Instances | `v1` | no | no | `get_class_instances_api_v1_databases__db_name__class__class_id__instances_get` |
| `GET` | `/api/v1/databases/{db_name}/class/{class_id}/sample-values` | Get Class Sample Values | `v1` | no | no | `get_class_sample_values_api_v1_databases__db_name__class__class_id__sample_values_get` |

### Label Mappings

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/databases/{db_name}/mappings/` | Get Mappings Summary | `v1` | no | no | `get_mappings_summary_api_v1_databases__db_name__mappings__get` |
| `DELETE` | `/api/v1/databases/{db_name}/mappings/` | Clear Mappings | `v1` | no | no | `clear_mappings_api_v1_databases__db_name__mappings__delete` |
| `POST` | `/api/v1/databases/{db_name}/mappings/export` | Export Mappings | `v1` | no | no | `export_mappings_api_v1_databases__db_name__mappings_export_post` |
| `POST` | `/api/v1/databases/{db_name}/mappings/import` | Import Mappings | `v1` | no | no | `import_mappings_api_v1_databases__db_name__mappings_import_post` |
| `POST` | `/api/v1/databases/{db_name}/mappings/validate` | Validate Mappings | `v1` | no | no | `validate_mappings_api_v1_databases__db_name__mappings_validate_post` |

### Lineage

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/lineage/column-lineage` | Get Lineage Column Lineage | `v1` | no | no | `get_lineage_column_lineage_api_v1_lineage_column_lineage_get` |
| `GET` | `/api/v1/lineage/diff` | Get Lineage Diff | `v1` | no | no | `get_lineage_diff_api_v1_lineage_diff_get` |
| `GET` | `/api/v1/lineage/graph` | Get Lineage Graph | `v1` | no | no | `get_lineage_graph_api_v1_lineage_graph_get` |
| `GET` | `/api/v1/lineage/impact` | Get Lineage Impact | `v1` | no | no | `get_lineage_impact_api_v1_lineage_impact_get` |
| `GET` | `/api/v1/lineage/metrics` | Get Lineage Metrics | `v1` | no | no | `get_lineage_metrics_api_v1_lineage_metrics_get` |
| `GET` | `/api/v1/lineage/out-of-date` | Get Lineage Out Of Date | `v1` | no | no | `get_lineage_out_of_date_api_v1_lineage_out_of_date_get` |
| `GET` | `/api/v1/lineage/path` | Get Lineage Path | `v1` | no | no | `get_lineage_path_api_v1_lineage_path_get` |
| `GET` | `/api/v1/lineage/run-impact` | Get Lineage Run Impact | `v1` | no | no | `get_lineage_run_impact_api_v1_lineage_run_impact_get` |
| `GET` | `/api/v1/lineage/runs` | Get Lineage Runs | `v1` | no | no | `get_lineage_runs_api_v1_lineage_runs_get` |
| `GET` | `/api/v1/lineage/timeline` | Get Lineage Timeline | `v1` | no | no | `get_lineage_timeline_api_v1_lineage_timeline_get` |

### Monitoring

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/monitoring/background-tasks/active` | Active Background Tasks | `v1` | no | no | `get_active_background_tasks_api_v1_monitoring_background_tasks_active_get` |
| `GET` | `/api/v1/monitoring/background-tasks/health` | Background Task System Health | `v1` | no | no | `get_background_task_health_api_v1_monitoring_background_tasks_health_get` |
| `GET` | `/api/v1/monitoring/background-tasks/metrics` | Background Task Metrics | `v1` | no | no | `get_background_task_metrics_api_v1_monitoring_background_tasks_metrics_get` |
| `GET` | `/api/v1/monitoring/config` | Configuration Overview | `v1` | no | no | `get_configuration_overview_api_v1_monitoring_config_get` |
| `GET` | `/api/v1/monitoring/health` | Basic Health Check | `v1` | no | no | `basic_health_check_api_v1_monitoring_health_get` |
| `GET` | `/api/v1/monitoring/health/detailed` | Detailed Health Check | `v1` | no | no | `detailed_health_check_api_v1_monitoring_health_detailed_get` |
| `GET` | `/api/v1/monitoring/health/liveness` | Kubernetes Liveness Probe | `v1` | no | no | `liveness_probe_api_v1_monitoring_health_liveness_get` |
| `GET` | `/api/v1/monitoring/health/readiness` | Kubernetes Readiness Probe | `v1` | no | no | `readiness_probe_api_v1_monitoring_health_readiness_get` |
| `GET` | `/api/v1/monitoring/metrics` | Service Metrics | `v1` | no | no | `get_service_metrics_api_v1_monitoring_metrics_get` |
| `GET` | `/api/v1/monitoring/status` | Service Status Overview | `v1` | no | no | `get_service_status_api_v1_monitoring_status_get` |

### Objectify

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `POST` | `/api/v1/objectify/databases/{db_name}/datasets/{dataset_id}/detect-relationships` | Detect FK relationships in a dataset | `v1` | no | no | `detect_relationships_api_v1_objectify_databases__db_name__datasets__dataset_id__detect_relationships_post` |
| `POST` | `/api/v1/objectify/databases/{db_name}/run-dag` | Run Objectify Dag | `v1` | no | no | `run_objectify_dag_api_v1_objectify_databases__db_name__run_dag_post` |
| `POST` | `/api/v1/objectify/datasets/{dataset_id}/run` | Run Objectify | `v1` | no | no | `run_objectify_api_v1_objectify_datasets__dataset_id__run_post` |
| `GET` | `/api/v1/objectify/mapping-specs` | List Mapping Specs | `v1` | no | no | `list_mapping_specs_api_v1_objectify_mapping_specs_get` |
| `POST` | `/api/v1/objectify/mapping-specs` | Create Mapping Spec | `v1` | no | no | `create_mapping_spec_api_v1_objectify_mapping_specs_post` |
| `POST` | `/api/v1/objectify/mapping-specs/{mapping_spec_id}/trigger-incremental` | Trigger incremental objectify | `v1` | no | no | `trigger_incremental_objectify_api_v1_objectify_mapping_specs__mapping_spec_id__trigger_incremental_post` |
| `GET` | `/api/v1/objectify/mapping-specs/{mapping_spec_id}/watermark` | Get objectify watermark | `v1` | no | no | `get_mapping_spec_watermark_api_v1_objectify_mapping_specs__mapping_spec_id__watermark_get` |

### Ontology Agent

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `POST` | `/api/v1/ontology-agent/runs` | Run ontology agent | `v1` | no | no | `run_ontology_agent_api_v1_ontology_agent_runs_post` |

### Ontology Extensions

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/databases/{db_name}/ontology/action-types` | List Route | `v1` | no | no | `list_route_api_v1_databases__db_name__ontology_action_types_get` |
| `POST` | `/api/v1/databases/{db_name}/ontology/action-types` | Create Route | `v1` | no | no | `create_route_api_v1_databases__db_name__ontology_action_types_post` |
| `GET` | `/api/v1/databases/{db_name}/ontology/action-types/{resource_id}` | Get Route | `v1` | no | no | `get_route_api_v1_databases__db_name__ontology_action_types__resource_id__get` |
| `PUT` | `/api/v1/databases/{db_name}/ontology/action-types/{resource_id}` | Update Route | `v1` | no | no | `update_route_api_v1_databases__db_name__ontology_action_types__resource_id__put` |
| `DELETE` | `/api/v1/databases/{db_name}/ontology/action-types/{resource_id}` | Delete Route | `v1` | no | no | `delete_route_api_v1_databases__db_name__ontology_action_types__resource_id__delete` |
| `POST` | `/api/v1/databases/{db_name}/ontology/deploy` | Deploy Ontology | `v1` | no | no | `deploy_ontology_api_v1_databases__db_name__ontology_deploy_post` |
| `GET` | `/api/v1/databases/{db_name}/ontology/functions` | List Route | `v1` | no | no | `list_route_api_v1_databases__db_name__ontology_functions_get` |
| `POST` | `/api/v1/databases/{db_name}/ontology/functions` | Create Route | `v1` | no | no | `create_route_api_v1_databases__db_name__ontology_functions_post` |
| `GET` | `/api/v1/databases/{db_name}/ontology/functions/{resource_id}` | Get Route | `v1` | no | no | `get_route_api_v1_databases__db_name__ontology_functions__resource_id__get` |
| `PUT` | `/api/v1/databases/{db_name}/ontology/functions/{resource_id}` | Update Route | `v1` | no | no | `update_route_api_v1_databases__db_name__ontology_functions__resource_id__put` |
| `DELETE` | `/api/v1/databases/{db_name}/ontology/functions/{resource_id}` | Delete Route | `v1` | no | no | `delete_route_api_v1_databases__db_name__ontology_functions__resource_id__delete` |
| `GET` | `/api/v1/databases/{db_name}/ontology/groups` | List Route | `v1` | no | no | `list_route_api_v1_databases__db_name__ontology_groups_get` |
| `POST` | `/api/v1/databases/{db_name}/ontology/groups` | Create Route | `v1` | no | no | `create_route_api_v1_databases__db_name__ontology_groups_post` |
| `GET` | `/api/v1/databases/{db_name}/ontology/groups/{resource_id}` | Get Route | `v1` | no | no | `get_route_api_v1_databases__db_name__ontology_groups__resource_id__get` |
| `PUT` | `/api/v1/databases/{db_name}/ontology/groups/{resource_id}` | Update Route | `v1` | no | no | `update_route_api_v1_databases__db_name__ontology_groups__resource_id__put` |
| `DELETE` | `/api/v1/databases/{db_name}/ontology/groups/{resource_id}` | Delete Route | `v1` | no | no | `delete_route_api_v1_databases__db_name__ontology_groups__resource_id__delete` |
| `GET` | `/api/v1/databases/{db_name}/ontology/health` | Ontology Health | `v1` | no | no | `ontology_health_api_v1_databases__db_name__ontology_health_get` |
| `GET` | `/api/v1/databases/{db_name}/ontology/interfaces` | List Route | `v1` | no | no | `list_route_api_v1_databases__db_name__ontology_interfaces_get` |
| `POST` | `/api/v1/databases/{db_name}/ontology/interfaces` | Create Route | `v1` | no | no | `create_route_api_v1_databases__db_name__ontology_interfaces_post` |
| `GET` | `/api/v1/databases/{db_name}/ontology/interfaces/{resource_id}` | Get Route | `v1` | no | no | `get_route_api_v1_databases__db_name__ontology_interfaces__resource_id__get` |
| `PUT` | `/api/v1/databases/{db_name}/ontology/interfaces/{resource_id}` | Update Route | `v1` | no | no | `update_route_api_v1_databases__db_name__ontology_interfaces__resource_id__put` |
| `DELETE` | `/api/v1/databases/{db_name}/ontology/interfaces/{resource_id}` | Delete Route | `v1` | no | no | `delete_route_api_v1_databases__db_name__ontology_interfaces__resource_id__delete` |
| `GET` | `/api/v1/databases/{db_name}/ontology/proposals` | List Ontology Proposals | `v1` | no | no | `list_ontology_proposals_api_v1_databases__db_name__ontology_proposals_get` |
| `POST` | `/api/v1/databases/{db_name}/ontology/proposals` | Create Ontology Proposal | `v1` | no | no | `create_ontology_proposal_api_v1_databases__db_name__ontology_proposals_post` |
| `POST` | `/api/v1/databases/{db_name}/ontology/proposals/{proposal_id}/approve` | Approve Ontology Proposal | `v1` | no | no | `approve_ontology_proposal_api_v1_databases__db_name__ontology_proposals__proposal_id__approve_post` |
| `GET` | `/api/v1/databases/{db_name}/ontology/shared-properties` | List Route | `v1` | no | no | `list_route_api_v1_databases__db_name__ontology_shared_properties_get` |
| `POST` | `/api/v1/databases/{db_name}/ontology/shared-properties` | Create Route | `v1` | no | no | `create_route_api_v1_databases__db_name__ontology_shared_properties_post` |
| `GET` | `/api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}` | Get Route | `v1` | no | no | `get_route_api_v1_databases__db_name__ontology_shared_properties__resource_id__get` |
| `PUT` | `/api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}` | Update Route | `v1` | no | no | `update_route_api_v1_databases__db_name__ontology_shared_properties__resource_id__put` |
| `DELETE` | `/api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}` | Delete Route | `v1` | no | no | `delete_route_api_v1_databases__db_name__ontology_shared_properties__resource_id__delete` |
| `GET` | `/api/v1/databases/{db_name}/ontology/value-types` | List Route | `v1` | no | no | `list_route_api_v1_databases__db_name__ontology_value_types_get` |
| `POST` | `/api/v1/databases/{db_name}/ontology/value-types` | Create Route | `v1` | no | no | `create_route_api_v1_databases__db_name__ontology_value_types_post` |
| `GET` | `/api/v1/databases/{db_name}/ontology/value-types/{resource_id}` | Get Route | `v1` | no | no | `get_route_api_v1_databases__db_name__ontology_value_types__resource_id__get` |
| `PUT` | `/api/v1/databases/{db_name}/ontology/value-types/{resource_id}` | Update Route | `v1` | no | no | `update_route_api_v1_databases__db_name__ontology_value_types__resource_id__put` |
| `DELETE` | `/api/v1/databases/{db_name}/ontology/value-types/{resource_id}` | Delete Route | `v1` | no | no | `delete_route_api_v1_databases__db_name__ontology_value_types__resource_id__delete` |

### Ontology Link Types

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/databases/{db_name}/ontology/link-types` | List Link Types | `v1` | no | no | `list_link_types_api_v1_databases__db_name__ontology_link_types_get` |
| `POST` | `/api/v1/databases/{db_name}/ontology/link-types` | Create Link Type | `v1` | no | no | `create_link_type_api_v1_databases__db_name__ontology_link_types_post` |
| `GET` | `/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}` | Get Link Type | `v1` | no | no | `get_link_type_api_v1_databases__db_name__ontology_link_types__link_type_id__get` |
| `PUT` | `/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}` | Update Link Type | `v1` | no | no | `update_link_type_api_v1_databases__db_name__ontology_link_types__link_type_id__put` |
| `GET` | `/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/edits` | List Link Edits | `v1` | no | no | `list_link_edits_api_v1_databases__db_name__ontology_link_types__link_type_id__edits_get` |
| `POST` | `/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/edits` | Create Link Edit | `v1` | no | no | `create_link_edit_api_v1_databases__db_name__ontology_link_types__link_type_id__edits_post` |
| `POST` | `/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/reindex` | Reindex Link Type | `v1` | no | no | `reindex_link_type_api_v1_databases__db_name__ontology_link_types__link_type_id__reindex_post` |

### Ontology Management

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `POST` | `/api/v1/databases/{db_name}/import-from-excel/commit` | Commit Import From Excel | `v1` | no | no | `commit_import_from_excel_api_v1_databases__db_name__import_from_excel_commit_post` |
| `POST` | `/api/v1/databases/{db_name}/import-from-excel/dry-run` | Dry Run Import From Excel | `v1` | no | no | `dry_run_import_from_excel_api_v1_databases__db_name__import_from_excel_dry_run_post` |
| `POST` | `/api/v1/databases/{db_name}/import-from-google-sheets/commit` | Commit Import From Google Sheets | `v1` | no | no | `commit_import_from_google_sheets_api_v1_databases__db_name__import_from_google_sheets_commit_post` |
| `POST` | `/api/v1/databases/{db_name}/import-from-google-sheets/dry-run` | Dry Run Import From Google Sheets | `v1` | no | no | `dry_run_import_from_google_sheets_api_v1_databases__db_name__import_from_google_sheets_dry_run_post` |
| `POST` | `/api/v1/databases/{db_name}/ontology` | Create Ontology | `v1` | no | no | `create_ontology_api_v1_databases__db_name__ontology_post` |
| `POST` | `/api/v1/databases/{db_name}/ontology/validate` | Validate Ontology Create Bff | `v1` | no | no | `validate_ontology_create_bff_api_v1_databases__db_name__ontology_validate_post` |
| `POST` | `/api/v1/databases/{db_name}/ontology/{class_id}/mapping-metadata` | Save Mapping Metadata | `v1` | no | no | `save_mapping_metadata_api_v1_databases__db_name__ontology__class_id__mapping_metadata_post` |
| `GET` | `/api/v1/databases/{db_name}/ontology/{class_id}/schema` | Get Ontology Schema | `v1` | no | no | `get_ontology_schema_api_v1_databases__db_name__ontology__class_id__schema_get` |
| `POST` | `/api/v1/databases/{db_name}/suggest-mappings` | Suggest Mappings | `v1` | no | no | `suggest_mappings_api_v1_databases__db_name__suggest_mappings_post` |
| `POST` | `/api/v1/databases/{db_name}/suggest-mappings-from-excel` | Suggest Mappings From Excel | `v1` | no | no | `suggest_mappings_from_excel_api_v1_databases__db_name__suggest_mappings_from_excel_post` |
| `POST` | `/api/v1/databases/{db_name}/suggest-mappings-from-google-sheets` | Suggest Mappings From Google Sheets | `v1` | no | no | `suggest_mappings_from_google_sheets_api_v1_databases__db_name__suggest_mappings_from_google_sheets_post` |
| `POST` | `/api/v1/databases/{db_name}/suggest-schema-from-data` | Suggest Schema From Data | `v1` | no | no | `suggest_schema_from_data_api_v1_databases__db_name__suggest_schema_from_data_post` |
| `POST` | `/api/v1/databases/{db_name}/suggest-schema-from-excel` | Suggest Schema From Excel | `v1` | no | no | `suggest_schema_from_excel_api_v1_databases__db_name__suggest_schema_from_excel_post` |
| `POST` | `/api/v1/databases/{db_name}/suggest-schema-from-google-sheets` | Suggest Schema From Google Sheets | `v1` | no | no | `suggest_schema_from_google_sheets_api_v1_databases__db_name__suggest_schema_from_google_sheets_post` |

### Ontology Object Types

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `POST` | `/api/v1/databases/{db_name}/ontology/object-types` | Create Object Type Contract | `v1` | no | no | `create_object_type_contract_api_v1_databases__db_name__ontology_object_types_post` |
| `PUT` | `/api/v1/databases/{db_name}/ontology/object-types/{class_id}` | Update Object Type Contract | `v1` | no | no | `update_object_type_contract_api_v1_databases__db_name__ontology_object_types__class_id__put` |

### Ops

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/ops/status` | Ops Status | `v1` | no | no | `ops_status_api_v1_ops_status_get` |

### Pipeline Builder

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/pipelines` | List Pipelines | `v1` | no | no | `list_pipelines_api_v1_pipelines_get` |
| `POST` | `/api/v1/pipelines` | Create Pipeline | `v1` | no | no | `create_pipeline_api_v1_pipelines_post` |
| `GET` | `/api/v1/pipelines/branches` | List Pipeline Branches | `v1` | no | no | `list_pipeline_branches_api_v1_pipelines_branches_get` |
| `POST` | `/api/v1/pipelines/branches/{branch}/archive` | Archive Pipeline Branch | `v1` | no | no | `archive_pipeline_branch_api_v1_pipelines_branches__branch__archive_post` |
| `POST` | `/api/v1/pipelines/branches/{branch}/restore` | Restore Pipeline Branch | `v1` | no | no | `restore_pipeline_branch_api_v1_pipelines_branches__branch__restore_post` |
| `GET` | `/api/v1/pipelines/datasets` | List Datasets | `v1` | no | no | `list_datasets_api_v1_pipelines_datasets_get` |
| `POST` | `/api/v1/pipelines/datasets` | Create Dataset | `v1` | no | no | `create_dataset_api_v1_pipelines_datasets_post` |
| `POST` | `/api/v1/pipelines/datasets/csv-upload` | Upload Csv Dataset | `v1` | no | no | `upload_csv_dataset_api_v1_pipelines_datasets_csv_upload_post` |
| `POST` | `/api/v1/pipelines/datasets/excel-upload` | Upload Excel Dataset | `v1` | no | no | `upload_excel_dataset_api_v1_pipelines_datasets_excel_upload_post` |
| `GET` | `/api/v1/pipelines/datasets/ingest-requests/{ingest_request_id}` | Get Dataset Ingest Request | `v1` | no | no | `get_dataset_ingest_request_api_v1_pipelines_datasets_ingest_requests__ingest_request_id__get` |
| `POST` | `/api/v1/pipelines/datasets/ingest-requests/{ingest_request_id}/schema/approve` | Approve Dataset Schema | `v1` | no | no | `approve_dataset_schema_api_v1_pipelines_datasets_ingest_requests__ingest_request_id__schema_approve_post` |
| `POST` | `/api/v1/pipelines/datasets/media-upload` | Upload Media Dataset | `v1` | no | no | `upload_media_dataset_api_v1_pipelines_datasets_media_upload_post` |
| `GET` | `/api/v1/pipelines/datasets/{dataset_id}/raw-file` | Get Dataset Raw File | `v1` | no | no | `get_dataset_raw_file_api_v1_pipelines_datasets__dataset_id__raw_file_get` |
| `POST` | `/api/v1/pipelines/datasets/{dataset_id}/versions` | Create Dataset Version | `v1` | no | no | `create_dataset_version_api_v1_pipelines_datasets__dataset_id__versions_post` |
| `POST` | `/api/v1/pipelines/datasets/{dataset_id}/versions/{version_id}/funnel-analysis` | Reanalyze Dataset Version | `v1` | no | no | `reanalyze_dataset_version_api_v1_pipelines_datasets__dataset_id__versions__version_id__funnel_analysis_post` |
| `GET` | `/api/v1/pipelines/proposals` | List Pipeline Proposals | `v1` | no | no | `list_pipeline_proposals_api_v1_pipelines_proposals_get` |
| `POST` | `/api/v1/pipelines/simulate-definition` | Simulate Pipeline Definition | `v1` | no | no | `simulate_pipeline_definition_api_v1_pipelines_simulate_definition_post` |
| `GET` | `/api/v1/pipelines/udfs` | List Udfs | `v1` | no | no | `list_udfs_api_v1_pipelines_udfs_get` |
| `POST` | `/api/v1/pipelines/udfs` | Create Udf | `v1` | no | no | `create_udf_api_v1_pipelines_udfs_post` |
| `GET` | `/api/v1/pipelines/udfs/{udf_id}` | Get Udf | `v1` | no | no | `get_udf_api_v1_pipelines_udfs__udf_id__get` |
| `POST` | `/api/v1/pipelines/udfs/{udf_id}/versions` | Create Udf Version | `v1` | no | no | `create_udf_version_api_v1_pipelines_udfs__udf_id__versions_post` |
| `GET` | `/api/v1/pipelines/udfs/{udf_id}/versions/{version}` | Get Udf Version | `v1` | no | no | `get_udf_version_api_v1_pipelines_udfs__udf_id__versions__version__get` |
| `GET` | `/api/v1/pipelines/{pipeline_id}` | Get Pipeline | `v1` | no | no | `get_pipeline_api_v1_pipelines__pipeline_id__get` |
| `PUT` | `/api/v1/pipelines/{pipeline_id}` | Update Pipeline | `v1` | no | no | `update_pipeline_api_v1_pipelines__pipeline_id__put` |
| `GET` | `/api/v1/pipelines/{pipeline_id}/artifacts` | List Pipeline Artifacts | `v1` | no | no | `list_pipeline_artifacts_api_v1_pipelines__pipeline_id__artifacts_get` |
| `GET` | `/api/v1/pipelines/{pipeline_id}/artifacts/{artifact_id}` | Get Pipeline Artifact | `v1` | no | no | `get_pipeline_artifact_api_v1_pipelines__pipeline_id__artifacts__artifact_id__get` |
| `POST` | `/api/v1/pipelines/{pipeline_id}/branches` | Create Pipeline Branch | `v1` | no | no | `create_pipeline_branch_api_v1_pipelines__pipeline_id__branches_post` |
| `POST` | `/api/v1/pipelines/{pipeline_id}/build` | Build Pipeline | `v1` | no | no | `build_pipeline_api_v1_pipelines__pipeline_id__build_post` |
| `POST` | `/api/v1/pipelines/{pipeline_id}/deploy` | Deploy Pipeline | `v1` | no | no | `deploy_pipeline_api_v1_pipelines__pipeline_id__deploy_post` |
| `POST` | `/api/v1/pipelines/{pipeline_id}/preview` | Preview Pipeline | `v1` | no | no | `preview_pipeline_api_v1_pipelines__pipeline_id__preview_post` |
| `POST` | `/api/v1/pipelines/{pipeline_id}/proposals` | Submit Pipeline Proposal | `v1` | no | no | `submit_pipeline_proposal_api_v1_pipelines__pipeline_id__proposals_post` |
| `POST` | `/api/v1/pipelines/{pipeline_id}/proposals/approve` | Approve Pipeline Proposal | `v1` | no | no | `approve_pipeline_proposal_api_v1_pipelines__pipeline_id__proposals_approve_post` |
| `POST` | `/api/v1/pipelines/{pipeline_id}/proposals/reject` | Reject Pipeline Proposal | `v1` | no | no | `reject_pipeline_proposal_api_v1_pipelines__pipeline_id__proposals_reject_post` |
| `GET` | `/api/v1/pipelines/{pipeline_id}/readiness` | Get Pipeline Readiness | `v1` | no | no | `get_pipeline_readiness_api_v1_pipelines__pipeline_id__readiness_get` |
| `GET` | `/api/v1/pipelines/{pipeline_id}/runs` | List Pipeline Runs | `v1` | no | no | `list_pipeline_runs_api_v1_pipelines__pipeline_id__runs_get` |

### Pipeline Plans

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `POST` | `/api/v1/pipeline-plans/compile` | Compile Plan | `v1` | no | no | `compile_plan_api_v1_pipeline_plans_compile_post` |
| `GET` | `/api/v1/pipeline-plans/{plan_id}` | Get Plan | `v1` | no | no | `get_plan_api_v1_pipeline_plans__plan_id__get` |
| `POST` | `/api/v1/pipeline-plans/{plan_id}/evaluate-joins` | Evaluate Joins | `v1` | no | no | `evaluate_joins_api_v1_pipeline_plans__plan_id__evaluate_joins_post` |
| `POST` | `/api/v1/pipeline-plans/{plan_id}/inspect-preview` | Inspect Plan Preview | `v1` | no | no | `inspect_plan_preview_api_v1_pipeline_plans__plan_id__inspect_preview_post` |
| `POST` | `/api/v1/pipeline-plans/{plan_id}/preview` | Preview Plan | `v1` | no | no | `preview_plan_api_v1_pipeline_plans__plan_id__preview_post` |

### Query

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/databases/{db_name}/query/builder` | Query Builder Info | `v1` | no | no | `query_builder_info_api_v1_databases__db_name__query_builder_get` |

### Schema Changes

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `PUT` | `/api/v1/schema-changes/drifts/{drift_id}/acknowledge` | Acknowledge Drift | `v1` | no | no | `acknowledge_drift_api_v1_schema_changes_drifts__drift_id__acknowledge_put` |
| `GET` | `/api/v1/schema-changes/history` | List Schema Changes | `v1` | no | no | `list_schema_changes_api_v1_schema_changes_history_get` |
| `GET` | `/api/v1/schema-changes/mappings/{mapping_spec_id}/compatibility` | Check Mapping Compatibility | `v1` | no | no | `check_mapping_compatibility_api_v1_schema_changes_mappings__mapping_spec_id__compatibility_get` |
| `GET` | `/api/v1/schema-changes/stats` | Get Schema Change Stats | `v1` | no | no | `get_schema_change_stats_api_v1_schema_changes_stats_get` |
| `GET` | `/api/v1/schema-changes/subscriptions` | List Subscriptions | `v1` | no | no | `list_subscriptions_api_v1_schema_changes_subscriptions_get` |
| `POST` | `/api/v1/schema-changes/subscriptions` | Create Subscription | `v1` | no | no | `create_subscription_api_v1_schema_changes_subscriptions_post` |
| `DELETE` | `/api/v1/schema-changes/subscriptions/{subscription_id}` | Delete Subscription | `v1` | no | no | `delete_subscription_api_v1_schema_changes_subscriptions__subscription_id__delete` |

### Summary

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/summary` | Get Summary | `v1` | no | no | `get_summary_api_v1_summary_get` |
