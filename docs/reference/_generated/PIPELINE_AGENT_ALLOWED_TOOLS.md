# Pipeline Agent Tool Allowlist

<!-- BEGIN AUTO-GENERATED: pipeline_tooling_reference -->
> Updated: 2026-02-14T10:12:35+09:00
> Revision: `07bcfddd6a9a0c66a3c66601640ec76fb3c234aa`
> Source of truth: `backend/bff/services/pipeline_agent_autonomous_loop.py` (`_PIPELINE_AGENT_ALLOWED_TOOLS`).
> Regenerate: `python scripts/generate_pipeline_tooling_reference.py`

## Allowed tools (runtime-enforced)

- `create_link_type_from_fk`
- `data_query`
- `dataset_get_by_name`
- `dataset_get_latest_version`
- `dataset_list`
- `dataset_profile`
- `dataset_sample`
- `dataset_validate_columns`
- `debug_dry_run`
- `debug_explain_failure`
- `debug_get_errors`
- `debug_get_execution_log`
- `debug_inspect_node`
- `detect_foreign_keys`
- `get_objectify_watermark`
- `objectify_create_mapping_spec`
- `objectify_get_status`
- `objectify_list_mapping_specs`
- `objectify_run`
- `objectify_suggest_mapping`
- `objectify_wait`
- `ontology_add_property`
- `ontology_add_relationship`
- `ontology_check_circular_refs`
- `ontology_check_relationships`
- `ontology_create`
- `ontology_get_class`
- `ontology_infer_schema_from_data`
- `ontology_list_classes`
- `ontology_load`
- `ontology_new`
- `ontology_preview`
- `ontology_query_instances`
- `ontology_register_object_type`
- `ontology_remove_property`
- `ontology_remove_relationship`
- `ontology_reset`
- `ontology_search_classes`
- `ontology_set_abstract`
- `ontology_set_class_meta`
- `ontology_set_primary_key`
- `ontology_suggest_mappings`
- `ontology_update`
- `ontology_update_property`
- `ontology_update_relationship`
- `ontology_validate`
- `pipeline_build_wait`
- `pipeline_create_from_plan`
- `pipeline_deploy_promote_build`
- `pipeline_preview_wait`
- `pipeline_update_from_plan`
- `plan_add_cast`
- `plan_add_compute`
- `plan_add_compute_assignments`
- `plan_add_compute_column`
- `plan_add_dedupe`
- `plan_add_drop`
- `plan_add_edge`
- `plan_add_explode`
- `plan_add_external_input`
- `plan_add_filter`
- `plan_add_geospatial`
- `plan_add_group_by`
- `plan_add_group_by_expr`
- `plan_add_input`
- `plan_add_join`
- `plan_add_normalize`
- `plan_add_output`
- `plan_add_pattern_mining`
- `plan_add_pivot`
- `plan_add_regex_replace`
- `plan_add_rename`
- `plan_add_select`
- `plan_add_select_expr`
- `plan_add_sort`
- `plan_add_split`
- `plan_add_stream_join`
- `plan_add_transform`
- `plan_add_union`
- `plan_add_window`
- `plan_add_window_expr`
- `plan_configure_input_read`
- `plan_delete_edge`
- `plan_delete_node`
- `plan_evaluate_joins`
- `plan_new`
- `plan_preview`
- `plan_refute_claims`
- `plan_reset`
- `plan_set_node_inputs`
- `plan_update_node_metadata`
- `plan_update_output`
- `plan_update_settings`
- `plan_validate`
- `plan_validate_structure`
- `preview_inspect`
- `reconcile_relationships`
- `trigger_incremental_objectify`

## Consistency checks

### Allowed by agent, but missing from MCP tool specs (should be empty)

- `ontology_add_property`
- `ontology_add_relationship`
- `ontology_check_circular_refs`
- `ontology_check_relationships`
- `ontology_create`
- `ontology_get_class`
- `ontology_infer_schema_from_data`
- `ontology_list_classes`
- `ontology_load`
- `ontology_new`
- `ontology_preview`
- `ontology_remove_property`
- `ontology_remove_relationship`
- `ontology_reset`
- `ontology_search_classes`
- `ontology_set_abstract`
- `ontology_set_class_meta`
- `ontology_set_primary_key`
- `ontology_suggest_mappings`
- `ontology_update`
- `ontology_update_property`
- `ontology_update_relationship`
- `ontology_validate`

- `mcp_specs_minus_allowed`:
  - `check_schema_drift`
  - `list_schema_changes`
  - `plan_add_udf`

<!-- END AUTO-GENERATED: pipeline_tooling_reference -->
