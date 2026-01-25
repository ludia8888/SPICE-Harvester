# Pipeline MCP Tool Catalog

<!-- BEGIN AUTO-GENERATED: pipeline_tooling_reference -->
> Updated: 2026-01-25T11:45:03Z
> Source of truth: `backend/mcp/pipeline_mcp_server.py` (parsed from the `tool_specs` literal).
> Regenerate: `python scripts/generate_pipeline_tooling_reference.py`

## Context Pack (analysis hints)

| Tool | Required args | Description |
| --- | --- | --- |
| `context_pack_build` | `db_name` | Build a safe pipeline context pack (schemas/samples + join/pk/fk/cast/cleansing hints). |
| `context_pack_infer_join_plan` | `context_pack` | Infer a best-effort join plan (spanning tree) from context pack candidates (deterministic, sample-based). |
| `context_pack_infer_keys` | `context_pack` | Infer PK/FK candidates from a context pack (deterministic, sample-based). |
| `context_pack_infer_types` | `context_pack` | Infer column types + join-key cast suggestions from a context pack (deterministic, sample-based). |
| `context_pack_null_report` | `context_pack` | Generate a null/missing report from a context pack (no plan changes). |

## Other

| Tool | Required args | Description |
| --- | --- | --- |
| `preview_inspect` | `preview` | Inspect a preview sample and propose cleansing suggestions (deterministic). |

## Pipeline Control Plane (Spark worker execution)

| Tool | Required args | Description |
| --- | --- | --- |
| `pipeline_build_wait` | `pipeline_id` | Queue a Spark build for a Pipeline and wait (poll) until completion; returns masked build output summary. |
| `pipeline_create_from_plan` | `plan, name, location` | Create a Pipeline (control plane) from a PipelinePlan.definition_json. Requires admin token; respects principal headers for permissions. |
| `pipeline_deploy_promote_build` | `pipeline_id, build_job_id, node_id, db_name, dataset_name` | Promote a successful build to a deployed dataset (requires approve permission). |
| `pipeline_preview_wait` | `pipeline_id` | Queue a Spark preview for a Pipeline and wait (poll) until completion; returns masked preview sample. |
| `pipeline_update_from_plan` | `pipeline_id, plan` | Update an existing Pipeline definition from a PipelinePlan.definition_json. |

## Plan Builder / Validation

| Tool | Required args | Description |
| --- | --- | --- |
| `plan_add_cast` | `plan, input_node_id, casts` | Add a cast transform node. casts=[{column,type}]. |
| `plan_add_compute` | `plan, input_node_id, expression` | Add a compute transform node (expression). |
| `plan_add_compute_assignments` | `plan, input_node_id, assignments` | Add a compute transform node that writes multiple columns. assignments=[{column,expression}]. |
| `plan_add_compute_column` | `plan, input_node_id, target_column, formula` | Add a compute transform node that writes target_column = formula (avoids '=' ambiguity). |
| `plan_add_dedupe` | `plan, input_node_id, columns` | Add a dedupe transform node. |
| `plan_add_drop` | `plan, input_node_id, columns` | Add a drop transform node. |
| `plan_add_edge` | `plan, from_node_id, to_node_id` | Add an edge from->to (idempotent). Incoming edge order can matter for joins. |
| `plan_add_external_input` | `plan, read` | Add an input node backed by a Spark-native source (jdbc/kafka/file URI) via metadata.read. |
| `plan_add_filter` | `plan, input_node_id, expression` | Add a filter transform node. |
| `plan_add_group_by` | `plan, input_node_id, aggregates` | Add a groupBy/aggregate transform node (group_by + aggregates). aggregates items: {column,op,alias?}. |
| `plan_add_group_by_expr` | `plan, input_node_id, aggregate_expressions` | Add a groupBy/aggregate node using Spark SQL aggregate expressions (supports approx_percentile, etc). |
| `plan_add_input` | `plan` | Add an input node for a dataset. |
| `plan_add_join` | `plan, left_node_id, right_node_id, left_keys, right_keys` | Add a join transform node (LEFT then RIGHT edge order). Cross joins are rejected. |
| `plan_add_normalize` | `plan, input_node_id, columns` | Add a normalize transform node. |
| `plan_add_output` | `plan, input_node_id, output_name` | Add an output node + outputs[] entry. |
| `plan_add_regex_replace` | `plan, input_node_id, rules` | Add a regexReplace transform node. rules=[{column,pattern,replacement,flags?}]. |
| `plan_add_rename` | `plan, input_node_id, rename` | Add a rename transform node. rename={src:dst}. |
| `plan_add_select` | `plan, input_node_id, columns` | Add a select transform node. |
| `plan_add_select_expr` | `plan, input_node_id, expressions` | Add a select transform node using Spark selectExpr expressions. |
| `plan_add_transform` | `plan, operation, input_node_ids` | Add a generic transform node (operation + metadata) with edges from input_node_ids. |
| `plan_add_window` | `plan, input_node_id` | Add a window transform node. order_by supports ['-col'] for DESC or [{'column','direction'}]. |
| `plan_add_window_expr` | `plan, input_node_id, expressions` | Add a window transform node computing one or more Spark SQL window expressions. |
| `plan_configure_input_read` | `plan, node_id` | Patch an input node's Spark read config (format/options/schema, permissive parsing, corrupt record capture). |
| `plan_delete_edge` | `plan, from_node_id, to_node_id` | Delete edges from->to (no-op if not found). |
| `plan_delete_node` | `plan, node_id` | Delete a node and any incident edges; for output nodes also removes outputs[] entry. |
| `plan_evaluate_joins` | `plan` | Evaluate join nodes in a plan (coverage/explosion) using sample-safe execution. |
| `plan_new` | `goal, db_name` | Create a new PipelinePlan JSON (empty nodes/edges). |
| `plan_preview` | `plan` | Preview a plan via the deterministic PipelineExecutor (sample-safe). |
| `plan_refute_claims` | `plan` | Refute plan-embedded claims with concrete counterexamples (witness-based hard gate). |
| `plan_set_node_inputs` | `plan, node_id, input_node_ids` | Replace all incoming edges to node_id with input_node_ids (in order). |
| `plan_update_node_metadata` | `plan, node_id` | Patch node.metadata (merge by default, replace if requested). |
| `plan_update_output` | `plan, output_name` | Patch outputs[] entry (by output_name); keeps output node metadata.outputName in sync if renamed. |
| `plan_update_settings` | `plan` | Patch plan.definition_json.settings (merge by default, replace if requested). |
| `plan_validate` | `plan` | Validate a PipelinePlan using dataset-aware preflight rules. |
| `plan_validate_structure` | `plan` | Validate plan.definition_json structure without resolving dataset schemas. |

<!-- END AUTO-GENERATED: pipeline_tooling_reference -->
