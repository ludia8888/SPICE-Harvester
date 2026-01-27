# Pipeline Agent Tool Allowlist

<!-- BEGIN AUTO-GENERATED: pipeline_tooling_reference -->
> Updated: 2026-01-27T09:22:12+09:00
> Revision: `d4b71b63bfd4c68fb9c677077d4200b7bb236bec`
> Source of truth: `backend/bff/services/pipeline_agent_autonomous_loop.py` (`_PIPELINE_AGENT_ALLOWED_TOOLS`).
> Regenerate: `python scripts/generate_pipeline_tooling_reference.py`

## Allowed tools (runtime-enforced)

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
- `plan_add_group_by`
- `plan_add_group_by_expr`
- `plan_add_input`
- `plan_add_join`
- `plan_add_normalize`
- `plan_add_output`
- `plan_add_pivot`
- `plan_add_regex_replace`
- `plan_add_rename`
- `plan_add_select`
- `plan_add_select_expr`
- `plan_add_sort`
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

## Consistency checks

- `allowed_minus_mcp_specs`: empty (OK)
- `mcp_specs_minus_allowed`: empty (OK)

<!-- END AUTO-GENERATED: pipeline_tooling_reference -->
