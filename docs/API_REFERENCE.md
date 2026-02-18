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

- Total documented endpoints: **246**
- Deprecated endpoints: **0**
- Security-enabled endpoints: **0**

| API Version | Endpoint Count |
| --- | --- |
| `v1` | 148 |
| `v2` | 98 |

| Top Domains (first path segment) | Endpoint Count |
| --- | --- |
| `ontologies` | 46 |
| `pipelines` | 29 |
| `connectivity` | 25 |
| `datasets` | 16 |
| `databases` | 15 |
| `admin` | 13 |
| `orchestration` | 11 |
| `lineage` | 10 |
| `monitoring` | 10 |
| `config` | 9 |
| `context7` | 7 |
| `objectify` | 7 |
| `schema-changes` | 7 |
| `backing-datasources` | 5 |
| `graph-query` | 5 |

## Endpoint Catalog (`/api/v1`, `/api/v2`)

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

### Database Management

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/databases` | List Databases | `v1` | no | no | `list_databases_api_v1_databases_get` |
| `POST` | `/api/v1/databases` | Create Database | `v1` | no | no | `create_database_api_v1_databases_post` |
| `GET` | `/api/v1/databases/{db_name}` | Get Database | `v1` | no | no | `get_database_api_v1_databases__db_name__get` |
| `DELETE` | `/api/v1/databases/{db_name}` | Delete Database | `v1` | no | no | `delete_database_api_v1_databases__db_name__delete` |
| `GET` | `/api/v1/databases/{db_name}/expected-seq` | Get Database Expected Seq | `v1` | no | no | `get_database_expected_seq_api_v1_databases__db_name__expected_seq_get` |

### Document Bundles

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `POST` | `/api/v1/document-bundles/{bundle_id}/search` | Search Document Bundle | `v1` | no | no | `search_document_bundle_api_v1_document_bundles__bundle_id__search_post` |

### Foundry Connectivity v2

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v2/connectivity/connections` | List Connections V2 | `v2` | no | no | `list_connections_v2_api_v2_connectivity_connections_get` |
| `POST` | `/api/v2/connectivity/connections` | Create Connection V2 | `v2` | no | no | `create_connection_v2_api_v2_connectivity_connections_post` |
| `POST` | `/api/v2/connectivity/connections/getConfigurationBatch` | Get Connection Configuration Batch V2 | `v2` | no | no | `get_connection_configuration_batch_v2_api_v2_connectivity_connections_getConfigurationBatch_post` |
| `GET` | `/api/v2/connectivity/connections/{connectionRid}` | Get Connection V2 | `v2` | no | no | `get_connection_v2_api_v2_connectivity_connections__connectionRid__get` |
| `DELETE` | `/api/v2/connectivity/connections/{connectionRid}` | Delete Connection V2 | `v2` | no | no | `delete_connection_v2_api_v2_connectivity_connections__connectionRid__delete` |
| `GET` | `/api/v2/connectivity/connections/{connectionRid}/fileImports` | List File Imports V2 | `v2` | no | no | `list_file_imports_v2_api_v2_connectivity_connections__connectionRid__fileImports_get` |
| `POST` | `/api/v2/connectivity/connections/{connectionRid}/fileImports` | Create File Import V2 | `v2` | no | no | `create_file_import_v2_api_v2_connectivity_connections__connectionRid__fileImports_post` |
| `GET` | `/api/v2/connectivity/connections/{connectionRid}/fileImports/{fileImportRid}` | Get File Import V2 | `v2` | no | no | `get_file_import_v2_api_v2_connectivity_connections__connectionRid__fileImports__fileImportRid__get` |
| `PUT` | `/api/v2/connectivity/connections/{connectionRid}/fileImports/{fileImportRid}` | Replace File Import V2 | `v2` | no | no | `replace_file_import_v2_api_v2_connectivity_connections__connectionRid__fileImports__fileImportRid__put` |
| `DELETE` | `/api/v2/connectivity/connections/{connectionRid}/fileImports/{fileImportRid}` | Delete File Import V2 | `v2` | no | no | `delete_file_import_v2_api_v2_connectivity_connections__connectionRid__fileImports__fileImportRid__delete` |
| `POST` | `/api/v2/connectivity/connections/{connectionRid}/fileImports/{fileImportRid}/execute` | Execute File Import V2 | `v2` | no | no | `execute_file_import_v2_api_v2_connectivity_connections__connectionRid__fileImports__fileImportRid__execute_post` |
| `GET` | `/api/v2/connectivity/connections/{connectionRid}/getConfiguration` | Get Connection Configuration V2 | `v2` | no | no | `get_connection_configuration_v2_api_v2_connectivity_connections__connectionRid__getConfiguration_get` |
| `GET` | `/api/v2/connectivity/connections/{connectionRid}/tableImports` | List Table Imports V2 | `v2` | no | no | `list_table_imports_v2_api_v2_connectivity_connections__connectionRid__tableImports_get` |
| `POST` | `/api/v2/connectivity/connections/{connectionRid}/tableImports` | Create Table Import V2 | `v2` | no | no | `create_table_import_v2_api_v2_connectivity_connections__connectionRid__tableImports_post` |
| `GET` | `/api/v2/connectivity/connections/{connectionRid}/tableImports/{tableImportRid}` | Get Table Import V2 | `v2` | no | no | `get_table_import_v2_api_v2_connectivity_connections__connectionRid__tableImports__tableImportRid__get` |
| `PUT` | `/api/v2/connectivity/connections/{connectionRid}/tableImports/{tableImportRid}` | Replace Table Import V2 | `v2` | no | no | `replace_table_import_v2_api_v2_connectivity_connections__connectionRid__tableImports__tableImportRid__put` |
| `DELETE` | `/api/v2/connectivity/connections/{connectionRid}/tableImports/{tableImportRid}` | Delete Table Import V2 | `v2` | no | no | `delete_table_import_v2_api_v2_connectivity_connections__connectionRid__tableImports__tableImportRid__delete` |
| `POST` | `/api/v2/connectivity/connections/{connectionRid}/tableImports/{tableImportRid}/execute` | Execute Table Import V2 | `v2` | no | no | `execute_table_import_v2_api_v2_connectivity_connections__connectionRid__tableImports__tableImportRid__execute_post` |
| `POST` | `/api/v2/connectivity/connections/{connectionRid}/test` | Test Connection V2 | `v2` | no | no | `test_connection_v2_api_v2_connectivity_connections__connectionRid__test_post` |
| `POST` | `/api/v2/connectivity/connections/{connectionRid}/updateSecrets` | Update Connection Secrets V2 | `v2` | no | no | `update_connection_secrets_v2_api_v2_connectivity_connections__connectionRid__updateSecrets_post` |
| `GET` | `/api/v2/connectivity/connections/{connectionRid}/virtualTables` | List Virtual Tables V2 | `v2` | no | no | `list_virtual_tables_v2_api_v2_connectivity_connections__connectionRid__virtualTables_get` |
| `POST` | `/api/v2/connectivity/connections/{connectionRid}/virtualTables` | Create Virtual Table V2 | `v2` | no | no | `create_virtual_table_v2_api_v2_connectivity_connections__connectionRid__virtualTables_post` |
| `GET` | `/api/v2/connectivity/connections/{connectionRid}/virtualTables/{virtualTableRid}` | Get Virtual Table V2 | `v2` | no | no | `get_virtual_table_v2_api_v2_connectivity_connections__connectionRid__virtualTables__virtualTableRid__get` |
| `PUT` | `/api/v2/connectivity/connections/{connectionRid}/virtualTables/{virtualTableRid}` | Replace Virtual Table V2 | `v2` | no | no | `replace_virtual_table_v2_api_v2_connectivity_connections__connectionRid__virtualTables__virtualTableRid__put` |
| `DELETE` | `/api/v2/connectivity/connections/{connectionRid}/virtualTables/{virtualTableRid}` | Delete Virtual Table V2 | `v2` | no | no | `delete_virtual_table_v2_api_v2_connectivity_connections__connectionRid__virtualTables__virtualTableRid__delete` |

### Foundry Datasets v2

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v2/datasets` | List Datasets V2 | `v2` | no | no | `list_datasets_v2_api_v2_datasets_get` |
| `POST` | `/api/v2/datasets` | Create Dataset V2 | `v2` | no | no | `create_dataset_v2_api_v2_datasets_post` |
| `GET` | `/api/v2/datasets/{datasetRid}` | Get Dataset V2 | `v2` | no | no | `get_dataset_v2_api_v2_datasets__datasetRid__get` |
| `GET` | `/api/v2/datasets/{datasetRid}/branches` | List Branches V2 | `v2` | no | no | `list_branches_v2_api_v2_datasets__datasetRid__branches_get` |
| `POST` | `/api/v2/datasets/{datasetRid}/branches` | Create Branch V2 | `v2` | no | no | `create_branch_v2_api_v2_datasets__datasetRid__branches_post` |
| `GET` | `/api/v2/datasets/{datasetRid}/branches/{branchName}` | Get Branch V2 | `v2` | no | no | `get_branch_v2_api_v2_datasets__datasetRid__branches__branchName__get` |
| `DELETE` | `/api/v2/datasets/{datasetRid}/branches/{branchName}` | Delete Branch V2 | `v2` | no | no | `delete_branch_v2_api_v2_datasets__datasetRid__branches__branchName__delete` |
| `GET` | `/api/v2/datasets/{datasetRid}/files` | List Files V2 | `v2` | no | no | `list_files_v2_api_v2_datasets__datasetRid__files_get` |
| `GET` | `/api/v2/datasets/{datasetRid}/files/{filePath}/content` | Get File Content V2 | `v2` | no | no | `get_file_content_v2_api_v2_datasets__datasetRid__files__filePath__content_get` |
| `POST` | `/api/v2/datasets/{datasetRid}/files:upload` | Upload File V2 | `v2` | no | no | `upload_file_v2_api_v2_datasets__datasetRid__files_upload_post` |
| `POST` | `/api/v2/datasets/{datasetRid}/readTable` | Read Table V2 | `v2` | no | no | `read_table_v2_api_v2_datasets__datasetRid__readTable_post` |
| `GET` | `/api/v2/datasets/{datasetRid}/schema` | Get Schema V2 | `v2` | no | no | `get_schema_v2_api_v2_datasets__datasetRid__schema_get` |
| `PUT` | `/api/v2/datasets/{datasetRid}/schema` | Update Schema V2 | `v2` | no | no | `update_schema_v2_api_v2_datasets__datasetRid__schema_put` |
| `POST` | `/api/v2/datasets/{datasetRid}/transactions` | Create Transaction V2 | `v2` | no | no | `create_transaction_v2_api_v2_datasets__datasetRid__transactions_post` |
| `POST` | `/api/v2/datasets/{datasetRid}/transactions/{transactionRid}/abort` | Abort Transaction V2 | `v2` | no | no | `abort_transaction_v2_api_v2_datasets__datasetRid__transactions__transactionRid__abort_post` |
| `POST` | `/api/v2/datasets/{datasetRid}/transactions/{transactionRid}/commit` | Commit Transaction V2 | `v2` | no | no | `commit_transaction_v2_api_v2_datasets__datasetRid__transactions__transactionRid__commit_post` |

### Foundry Ontologies v2

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v2/ontologies` | List Ontologies V2 | `v2` | no | no | `list_ontologies_v2_api_v2_ontologies_get` |
| `POST` | `/api/v2/ontologies/attachments/upload` | Upload Attachment V2 | `v2` | no | no | `upload_attachment_v2_api_v2_ontologies_attachments_upload_post` |
| `GET` | `/api/v2/ontologies/{ontology}` | Get Ontology V2 | `v2` | no | no | `get_ontology_v2_api_v2_ontologies__ontology__get` |
| `GET` | `/api/v2/ontologies/{ontology}/actionTypes` | List Action Types V2 | `v2` | no | no | `list_action_types_v2_api_v2_ontologies__ontology__actionTypes_get` |
| `GET` | `/api/v2/ontologies/{ontology}/actionTypes/byRid/{actionTypeRid}` | Get Action Type By Rid V2 | `v2` | no | no | `get_action_type_by_rid_v2_api_v2_ontologies__ontology__actionTypes_byRid__actionTypeRid__get` |
| `GET` | `/api/v2/ontologies/{ontology}/actionTypes/{actionType}` | Get Action Type V2 | `v2` | no | no | `get_action_type_v2_api_v2_ontologies__ontology__actionTypes__actionType__get` |
| `POST` | `/api/v2/ontologies/{ontology}/actions/{action}/apply` | Apply Action V2 | `v2` | no | no | `apply_action_v2_api_v2_ontologies__ontology__actions__action__apply_post` |
| `POST` | `/api/v2/ontologies/{ontology}/actions/{action}/applyBatch` | Apply Action Batch V2 | `v2` | no | no | `apply_action_batch_v2_api_v2_ontologies__ontology__actions__action__applyBatch_post` |
| `GET` | `/api/v2/ontologies/{ontology}/fullMetadata` | Get Full Metadata V2 | `v2` | no | no | `get_full_metadata_v2_api_v2_ontologies__ontology__fullMetadata_get` |
| `GET` | `/api/v2/ontologies/{ontology}/interfaceTypes` | List Interface Types V2 | `v2` | no | no | `list_interface_types_v2_api_v2_ontologies__ontology__interfaceTypes_get` |
| `GET` | `/api/v2/ontologies/{ontology}/interfaceTypes/{interfaceType}` | Get Interface Type V2 | `v2` | no | no | `get_interface_type_v2_api_v2_ontologies__ontology__interfaceTypes__interfaceType__get` |
| `POST` | `/api/v2/ontologies/{ontology}/objectSets/aggregate` | Aggregate Object Set V2 | `v2` | no | no | `aggregate_object_set_v2_api_v2_ontologies__ontology__objectSets_aggregate_post` |
| `POST` | `/api/v2/ontologies/{ontology}/objectSets/createTemporary` | Create Temporary Object Set V2 | `v2` | no | no | `create_temporary_object_set_v2_api_v2_ontologies__ontology__objectSets_createTemporary_post` |
| `POST` | `/api/v2/ontologies/{ontology}/objectSets/loadLinks` | Load Object Set Links V2 | `v2` | no | no | `load_object_set_links_v2_api_v2_ontologies__ontology__objectSets_loadLinks_post` |
| `POST` | `/api/v2/ontologies/{ontology}/objectSets/loadObjects` | Load Object Set Objects V2 | `v2` | no | no | `load_object_set_objects_v2_api_v2_ontologies__ontology__objectSets_loadObjects_post` |
| `POST` | `/api/v2/ontologies/{ontology}/objectSets/loadObjectsMultipleObjectTypes` | Load Object Set Multiple Object Types V2 | `v2` | no | no | `load_object_set_multiple_object_types_v2_api_v2_ontologies__ontology__objectSets_loadObjectsMultipleObjectTypes_post` |
| `POST` | `/api/v2/ontologies/{ontology}/objectSets/loadObjectsOrInterfaces` | Load Object Set Objects Or Interfaces V2 | `v2` | no | no | `load_object_set_objects_or_interfaces_v2_api_v2_ontologies__ontology__objectSets_loadObjectsOrInterfaces_post` |
| `GET` | `/api/v2/ontologies/{ontology}/objectSets/{objectSetRid}` | Get Object Set V2 | `v2` | no | no | `get_object_set_v2_api_v2_ontologies__ontology__objectSets__objectSetRid__get` |
| `GET` | `/api/v2/ontologies/{ontology}/objectTypes` | List Object Types V2 | `v2` | no | no | `list_object_types_v2_api_v2_ontologies__ontology__objectTypes_get` |
| `GET` | `/api/v2/ontologies/{ontology}/objectTypes/{objectType}` | Get Object Type V2 | `v2` | no | no | `get_object_type_v2_api_v2_ontologies__ontology__objectTypes__objectType__get` |
| `GET` | `/api/v2/ontologies/{ontology}/objectTypes/{objectType}/fullMetadata` | Get Object Type Full Metadata V2 | `v2` | no | no | `get_object_type_full_metadata_v2_api_v2_ontologies__ontology__objectTypes__objectType__fullMetadata_get` |
| `GET` | `/api/v2/ontologies/{ontology}/objectTypes/{objectType}/incomingLinkTypes` | List Incoming Link Types V2 | `v2` | no | no | `list_incoming_link_types_v2_api_v2_ontologies__ontology__objectTypes__objectType__incomingLinkTypes_get` |
| `GET` | `/api/v2/ontologies/{ontology}/objectTypes/{objectType}/incomingLinkTypes/{linkType}` | Get Incoming Link Type V2 | `v2` | no | no | `get_incoming_link_type_v2_api_v2_ontologies__ontology__objectTypes__objectType__incomingLinkTypes__linkType__get` |
| `GET` | `/api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes` | List Outgoing Link Types V2 | `v2` | no | no | `list_outgoing_link_types_v2_api_v2_ontologies__ontology__objectTypes__objectType__outgoingLinkTypes_get` |
| `GET` | `/api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes/{linkType}` | Get Outgoing Link Type V2 | `v2` | no | no | `get_outgoing_link_type_v2_api_v2_ontologies__ontology__objectTypes__objectType__outgoingLinkTypes__linkType__get` |
| `GET` | `/api/v2/ontologies/{ontology}/objects/{objectType}` | List Objects V2 | `v2` | no | no | `list_objects_v2_api_v2_ontologies__ontology__objects__objectType__get` |
| `POST` | `/api/v2/ontologies/{ontology}/objects/{objectType}/aggregate` | Aggregate Objects V2 | `v2` | no | no | `aggregate_objects_v2_api_v2_ontologies__ontology__objects__objectType__aggregate_post` |
| `POST` | `/api/v2/ontologies/{ontology}/objects/{objectType}/count` | Count Objects V2 | `v2` | no | no | `count_objects_v2_api_v2_ontologies__ontology__objects__objectType__count_post` |
| `POST` | `/api/v2/ontologies/{ontology}/objects/{objectType}/search` | Search Objects V2 | `v2` | no | no | `search_objects_v2_api_v2_ontologies__ontology__objects__objectType__search_post` |
| `GET` | `/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}` | Get Object V2 | `v2` | no | no | `get_object_v2_api_v2_ontologies__ontology__objects__objectType___primaryKey__get` |
| `GET` | `/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/attachments/{property}` | List Attachment Property V2 | `v2` | no | no | `list_attachment_property_v2_api_v2_ontologies__ontology__objects__objectType___primaryKey__attachments__property__get` |
| `GET` | `/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/attachments/{property}/content` | Get Attachment Content V2 | `v2` | no | no | `get_attachment_content_v2_api_v2_ontologies__ontology__objects__objectType___primaryKey__attachments__property__content_get` |
| `GET` | `/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/attachments/{property}/{attachmentRid}` | Get Attachment By Rid V2 | `v2` | no | no | `get_attachment_by_rid_v2_api_v2_ontologies__ontology__objects__objectType___primaryKey__attachments__property___attachmentRid__get` |
| `GET` | `/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/attachments/{property}/{attachmentRid}/content` | Get Attachment Content By Rid V2 | `v2` | no | no | `get_attachment_content_by_rid_v2_api_v2_ontologies__ontology__objects__objectType___primaryKey__attachments__property___attachmentRid__content_get` |
| `GET` | `/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/links/{linkType}` | List Linked Objects V2 | `v2` | no | no | `list_linked_objects_v2_api_v2_ontologies__ontology__objects__objectType___primaryKey__links__linkType__get` |
| `GET` | `/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/links/{linkType}/{linkedObjectPrimaryKey}` | Get Linked Object V2 | `v2` | no | no | `get_linked_object_v2_api_v2_ontologies__ontology__objects__objectType___primaryKey__links__linkType___linkedObjectPrimaryKey__get` |
| `GET` | `/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/timeseries/{property}/firstPoint` | Get Timeseries First Point V2 | `v2` | no | no | `get_timeseries_first_point_v2_api_v2_ontologies__ontology__objects__objectType___primaryKey__timeseries__property__firstPoint_get` |
| `GET` | `/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/timeseries/{property}/lastPoint` | Get Timeseries Last Point V2 | `v2` | no | no | `get_timeseries_last_point_v2_api_v2_ontologies__ontology__objects__objectType___primaryKey__timeseries__property__lastPoint_get` |
| `POST` | `/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/timeseries/{property}/streamPoints` | Stream Timeseries Points V2 | `v2` | no | no | `stream_timeseries_points_v2_api_v2_ontologies__ontology__objects__objectType___primaryKey__timeseries__property__streamPoints_post` |
| `POST` | `/api/v2/ontologies/{ontology}/queries/{queryApiName}/execute` | Execute Query V2 | `v2` | no | no | `execute_query_v2_api_v2_ontologies__ontology__queries__queryApiName__execute_post` |
| `GET` | `/api/v2/ontologies/{ontology}/queryTypes` | List Query Types V2 | `v2` | no | no | `list_query_types_v2_api_v2_ontologies__ontology__queryTypes_get` |
| `GET` | `/api/v2/ontologies/{ontology}/queryTypes/{queryApiName}` | Get Query Type V2 | `v2` | no | no | `get_query_type_v2_api_v2_ontologies__ontology__queryTypes__queryApiName__get` |
| `GET` | `/api/v2/ontologies/{ontology}/sharedPropertyTypes` | List Shared Property Types V2 | `v2` | no | no | `list_shared_property_types_v2_api_v2_ontologies__ontology__sharedPropertyTypes_get` |
| `GET` | `/api/v2/ontologies/{ontology}/sharedPropertyTypes/{sharedPropertyType}` | Get Shared Property Type V2 | `v2` | no | no | `get_shared_property_type_v2_api_v2_ontologies__ontology__sharedPropertyTypes__sharedPropertyType__get` |
| `GET` | `/api/v2/ontologies/{ontology}/valueTypes` | List Value Types V2 | `v2` | no | no | `list_value_types_v2_api_v2_ontologies__ontology__valueTypes_get` |
| `GET` | `/api/v2/ontologies/{ontology}/valueTypes/{valueType}` | Get Value Type V2 | `v2` | no | no | `get_value_type_v2_api_v2_ontologies__ontology__valueTypes__valueType__get` |

### Foundry Orchestration v2

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `POST` | `/api/v2/orchestration/builds/create` | Create Build V2 | `v2` | no | no | `create_build_v2_api_v2_orchestration_builds_create_post` |
| `POST` | `/api/v2/orchestration/builds/getBatch` | Get Builds Batch V2 | `v2` | no | no | `get_builds_batch_v2_api_v2_orchestration_builds_getBatch_post` |
| `GET` | `/api/v2/orchestration/builds/{buildRid}` | Get Build V2 | `v2` | no | no | `get_build_v2_api_v2_orchestration_builds__buildRid__get` |
| `POST` | `/api/v2/orchestration/builds/{buildRid}/cancel` | Cancel Build V2 | `v2` | no | no | `cancel_build_v2_api_v2_orchestration_builds__buildRid__cancel_post` |
| `GET` | `/api/v2/orchestration/builds/{buildRid}/jobs` | List Build Jobs V2 | `v2` | no | no | `list_build_jobs_v2_api_v2_orchestration_builds__buildRid__jobs_get` |
| `POST` | `/api/v2/orchestration/schedules` | Create Schedule V2 | `v2` | no | no | `create_schedule_v2_api_v2_orchestration_schedules_post` |
| `GET` | `/api/v2/orchestration/schedules/{scheduleRid}` | Get Schedule V2 | `v2` | no | no | `get_schedule_v2_api_v2_orchestration_schedules__scheduleRid__get` |
| `DELETE` | `/api/v2/orchestration/schedules/{scheduleRid}` | Delete Schedule V2 | `v2` | no | no | `delete_schedule_v2_api_v2_orchestration_schedules__scheduleRid__delete` |
| `POST` | `/api/v2/orchestration/schedules/{scheduleRid}/pause` | Pause Schedule V2 | `v2` | no | no | `pause_schedule_v2_api_v2_orchestration_schedules__scheduleRid__pause_post` |
| `GET` | `/api/v2/orchestration/schedules/{scheduleRid}/runs` | List Schedule Runs V2 | `v2` | no | no | `list_schedule_runs_v2_api_v2_orchestration_schedules__scheduleRid__runs_get` |
| `POST` | `/api/v2/orchestration/schedules/{scheduleRid}/unpause` | Unpause Schedule V2 | `v2` | no | no | `unpause_schedule_v2_api_v2_orchestration_schedules__scheduleRid__unpause_post` |

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

### Ontology Management

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `POST` | `/api/v1/databases/{db_name}/ontology` | Create Ontology | `v1` | no | no | `create_ontology_api_v1_databases__db_name__ontology_post` |

### Ops

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/ops/status` | Ops Status | `v1` | no | no | `ops_status_api_v1_ops_status_get` |

### Pipeline Builder

| Method | Path | Summary | Version | Auth | Deprecated | Operation ID |
| --- | --- | --- | --- | --- | --- | --- |
| `GET` | `/api/v1/pipelines` | List Pipelines | `v1` | no | no | `list_pipelines_api_v1_pipelines_get` |
| `POST` | `/api/v1/pipelines` | Create Pipeline | `v1` | no | no | `create_pipeline_api_v1_pipelines_post` |
| `GET` | `/api/v1/pipelines/datasets` | List Datasets | `v1` | no | no | `list_datasets_api_v1_pipelines_datasets_get` |
| `POST` | `/api/v1/pipelines/datasets` | Create Dataset | `v1` | no | no | `create_dataset_api_v1_pipelines_datasets_post` |
| `POST` | `/api/v1/pipelines/datasets/csv-upload` | Upload Csv Dataset | `v1` | no | no | `upload_csv_dataset_api_v1_pipelines_datasets_csv_upload_post` |
| `POST` | `/api/v1/pipelines/datasets/excel-upload` | Upload Excel Dataset | `v1` | no | no | `upload_excel_dataset_api_v1_pipelines_datasets_excel_upload_post` |
| `GET` | `/api/v1/pipelines/datasets/ingest-requests/{ingest_request_id}` | Get Dataset Ingest Request | `v1` | no | no | `get_dataset_ingest_request_api_v1_pipelines_datasets_ingest_requests__ingest_request_id__get` |
| `POST` | `/api/v1/pipelines/datasets/ingest-requests/{ingest_request_id}/schema/approve` | Approve Dataset Schema | `v1` | no | no | `approve_dataset_schema_api_v1_pipelines_datasets_ingest_requests__ingest_request_id__schema_approve_post` |
| `POST` | `/api/v1/pipelines/datasets/media-upload` | Upload Media Dataset | `v1` | no | no | `upload_media_dataset_api_v1_pipelines_datasets_media_upload_post` |
| `GET` | `/api/v1/pipelines/datasets/{dataset_id}/raw-file` | Get Dataset Raw File | `v1` | no | no | `get_dataset_raw_file_api_v1_pipelines_datasets__dataset_id__raw_file_get` |
| `POST` | `/api/v1/pipelines/datasets/{dataset_id}/versions` | Create Dataset Version | `v1` | no | no | `create_dataset_version_api_v1_pipelines_datasets__dataset_id__versions_post` |
| `GET` | `/api/v1/pipelines/proposals` | List Pipeline Proposals | `v1` | no | no | `list_pipeline_proposals_api_v1_pipelines_proposals_get` |
| `GET` | `/api/v1/pipelines/udfs` | List Udfs | `v1` | no | no | `list_udfs_api_v1_pipelines_udfs_get` |
| `POST` | `/api/v1/pipelines/udfs` | Create Udf | `v1` | no | no | `create_udf_api_v1_pipelines_udfs_post` |
| `GET` | `/api/v1/pipelines/udfs/{udf_id}` | Get Udf | `v1` | no | no | `get_udf_api_v1_pipelines_udfs__udf_id__get` |
| `POST` | `/api/v1/pipelines/udfs/{udf_id}/versions` | Create Udf Version | `v1` | no | no | `create_udf_version_api_v1_pipelines_udfs__udf_id__versions_post` |
| `GET` | `/api/v1/pipelines/udfs/{udf_id}/versions/{version}` | Get Udf Version | `v1` | no | no | `get_udf_version_api_v1_pipelines_udfs__udf_id__versions__version__get` |
| `GET` | `/api/v1/pipelines/{pipeline_id}` | Get Pipeline | `v1` | no | no | `get_pipeline_api_v1_pipelines__pipeline_id__get` |
| `PUT` | `/api/v1/pipelines/{pipeline_id}` | Update Pipeline | `v1` | no | no | `update_pipeline_api_v1_pipelines__pipeline_id__put` |
| `GET` | `/api/v1/pipelines/{pipeline_id}/artifacts` | List Pipeline Artifacts | `v1` | no | no | `list_pipeline_artifacts_api_v1_pipelines__pipeline_id__artifacts_get` |
| `GET` | `/api/v1/pipelines/{pipeline_id}/artifacts/{artifact_id}` | Get Pipeline Artifact | `v1` | no | no | `get_pipeline_artifact_api_v1_pipelines__pipeline_id__artifacts__artifact_id__get` |
| `POST` | `/api/v1/pipelines/{pipeline_id}/build` | Build Pipeline | `v1` | no | no | `build_pipeline_api_v1_pipelines__pipeline_id__build_post` |
| `POST` | `/api/v1/pipelines/{pipeline_id}/deploy` | Deploy Pipeline | `v1` | no | no | `deploy_pipeline_api_v1_pipelines__pipeline_id__deploy_post` |
| `POST` | `/api/v1/pipelines/{pipeline_id}/preview` | Preview Pipeline | `v1` | no | no | `preview_pipeline_api_v1_pipelines__pipeline_id__preview_post` |
| `POST` | `/api/v1/pipelines/{pipeline_id}/proposals` | Submit Pipeline Proposal | `v1` | no | no | `submit_pipeline_proposal_api_v1_pipelines__pipeline_id__proposals_post` |
| `POST` | `/api/v1/pipelines/{pipeline_id}/proposals/approve` | Approve Pipeline Proposal | `v1` | no | no | `approve_pipeline_proposal_api_v1_pipelines__pipeline_id__proposals_approve_post` |
| `POST` | `/api/v1/pipelines/{pipeline_id}/proposals/reject` | Reject Pipeline Proposal | `v1` | no | no | `reject_pipeline_proposal_api_v1_pipelines__pipeline_id__proposals_reject_post` |
| `GET` | `/api/v1/pipelines/{pipeline_id}/readiness` | Get Pipeline Readiness | `v1` | no | no | `get_pipeline_readiness_api_v1_pipelines__pipeline_id__readiness_get` |
| `GET` | `/api/v1/pipelines/{pipeline_id}/runs` | List Pipeline Runs | `v1` | no | no | `list_pipeline_runs_api_v1_pipelines__pipeline_id__runs_get` |

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
