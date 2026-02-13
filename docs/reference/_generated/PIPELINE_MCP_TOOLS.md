# Pipeline MCP Tool Catalog

<!-- BEGIN AUTO-GENERATED: pipeline_tooling_reference -->
> Updated: 2026-02-14T02:58:03+09:00
> Revision: `2025fdb195b800ea75b094938c77e98cecef217f`
> Source of truth: `backend/mcp_servers/pipeline_mcp_server.py` (parsed from the `tool_specs` literal).
> Regenerate: `python scripts/generate_pipeline_tooling_reference.py`

## Other

| Tool | Required args | Description |
| --- | --- | --- |
| `check_schema_drift` | `db_name, mapping_spec_id` | Check if dataset schema has drifted from mapping spec expectations. Detects column additions, removals, type changes. |
| `create_link_type_from_fk` | `db_name, fk_pattern, source_class_id, target_class_id` | Create a link_type from a detected FK pattern. Generates the relationship spec and link_type definition. |
| `data_query` | `dataset_id, sql` | Execute ad-hoc SQL query on dataset sample rows using DuckDB. Supports GROUP BY, JOIN, WINDOW, CTE, and all standard SQL. Table name is 'data'. Max 500 result rows. Query runs on sample rows, not full dataset. |
| `dataset_get_by_name` | `db_name, dataset_name` | Look up a dataset by name and return its dataset_id. Use this when you have a dataset name but need the dataset_id for objectify or other tools. |
| `dataset_get_latest_version` | `dataset_id` | Get the latest version of a dataset. Returns version_id, artifact_key, and schema info. |
| `dataset_list` | `db_name` | List all datasets in a database/project with their column names and metadata. Use this to understand what data is available before building pipelines. |
| `dataset_profile` | `dataset_id` | Get column-level statistics for a dataset: null ratio, distinct count, top values, numeric histogram. Use to understand data quality and distributions. |
| `dataset_sample` | `dataset_id` | Get a small sample of actual data rows from a dataset (PII-masked). Use this to understand data patterns, values, and quality before designing transformations. |
| `dataset_validate_columns` | `dataset_id, columns` | Validate that specified columns exist in a dataset's schema. Returns valid/invalid columns and available columns for suggestions. Use this before adding operations that reference specific columns. |
| `debug_dry_run` | `plan` | Validate a pipeline plan without actually executing it. Checks for structural issues, missing references, and join key compatibility. |
| `debug_explain_failure` | `` | Analyze accumulated errors and provide diagnostic suggestions with potential fixes. |
| `debug_get_errors` | `` | Get accumulated errors and warnings from current pipeline run. Use this to see what went wrong. |
| `debug_get_execution_log` | `` | Get step-by-step execution log of tool calls made during this run. Useful for understanding what happened. |
| `debug_inspect_node` | `plan, node_id` | Inspect a specific node's configuration, inputs, and outputs in the pipeline plan. |
| `detect_foreign_keys` | `db_name, dataset_id` | Detect potential FK relationships in a dataset based on naming conventions and value overlap analysis. Returns detected FK patterns with confidence scores. |
| `get_objectify_watermark` | `mapping_spec_id` | Get the current watermark state for a mapping spec. Shows last processed watermark value and timestamp. |
| `list_schema_changes` | `db_name, subject_type, subject_id` | List recent schema changes for a dataset or mapping spec. Shows drift history with severity and change details. |
| `objectify_create_mapping_spec` | `dataset_id, target_class_id, mappings, db_name` | Create a mapping specification that defines how dataset columns map to ontology class properties. Required before running objectify. |
| `objectify_get_status` | `job_id` | Get the status of an objectify job. |
| `objectify_list_mapping_specs` | `dataset_id` | List existing mapping specifications for a dataset. |
| `objectify_run` | `dataset_id, db_name` | Execute objectify transformation to convert dataset rows into ontology instances. Can use OMS backing_source (via target_class_id) or PostgreSQL mapping spec. |
| `objectify_suggest_mapping` | `dataset_id, target_class_id, db_name` | Suggest field mappings from dataset columns to ontology class properties. Uses schema matching and naming heuristics. Call this before creating a mapping spec. |
| `objectify_wait` | `job_id` | Wait for an objectify job to complete. Polls the job status until completion, failure, or timeout. Returns final job status with results. |
| `ontology_query_instances` | `db_name, class_id` | Query ontology instances by class type. Use this to verify objectify results by counting instances or retrieving sample data. Returns instance count and sample instances. |
| `ontology_register_object_type` | `db_name, class_id, dataset_id, primary_key, title_key` | Register an ontology class as an object_type resource for objectify. REQUIRED before running objectify. Creates the object_type contract with pk_spec and backing_source configuration. |
| `preview_inspect` | `preview` | Inspect a preview sample and propose cleansing suggestions (deterministic). |
| `reconcile_relationships` | `db_name` | Auto-populate relationships on ES instances by detecting FK references between ontology classes. Call this AFTER objectify completes for all classes to link instances via foreign keys (e.g. Order.customer_id → Customer). Returns per-relationship stats. |
| `trigger_incremental_objectify` | `db_name, mapping_spec_id` | Trigger objectify in incremental mode (watermark or delta). Processes only changed rows since last run. |

## Pipeline Control Plane (Spark worker execution)

| Tool | Required args | Description |
| --- | --- | --- |
| `pipeline_build_wait` | `pipeline_id` | Queue a Spark build for a Pipeline and optionally wait (poll) until completion. If job_id is provided, this tool will poll that existing job_id without enqueuing a new build (prevents runaway QUEUED runs). Note: limit is capped at 500 rows. |
| `pipeline_create_from_plan` | `plan, name, location` | Create a Pipeline (control plane) from a PipelinePlan.definition_json. If the pipeline already exists (same db/name/branch), this tool will update it and return the existing pipeline_id (idempotent upsert). Requires admin token; respects principal headers for permissions. |
| `pipeline_deploy_promote_build` | `pipeline_id, build_job_id, node_id, db_name, dataset_name` | Promote a successful build to a deployed dataset (requires approve permission). To avoid build↔deploy definition hash mismatches, you may pass definition_json, or pass pipeline_spec_commit_id and this tool will fetch the exact build snapshot from pipeline_versions. |
| `pipeline_preview_wait` | `pipeline_id` | Queue a Spark preview for a Pipeline and optionally wait (poll) until completion. If job_id is provided, this tool will poll that existing job_id without enqueuing a new preview (prevents runaway QUEUED runs). Note: limit is capped at 500 rows. |
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
| `plan_add_explode` | `plan, input_node_id, column` | Add an explode transform node for an array/map-like column (replaces column with exploded elements). |
| `plan_add_external_input` | `plan, read` | Add an input node backed by a Spark-native source (jdbc/kafka/file URI) via metadata.read. |
| `plan_add_filter` | `plan, input_node_id, expression` | Add a filter transform node. |
| `plan_add_geospatial` | `plan, input_node_id, mode` | Add geospatial transform (mode=point|geohash|distance). |
| `plan_add_group_by` | `plan, input_node_id, aggregates` | Add a groupBy/aggregate transform node (group_by + aggregates). aggregates items: {column,op,alias?}. |
| `plan_add_group_by_expr` | `plan, input_node_id, aggregate_expressions` | Add a groupBy/aggregate node using Spark SQL aggregate expressions (supports approx_percentile, etc). |
| `plan_add_input` | `plan` | Add an input node for a dataset. |
| `plan_add_join` | `plan, left_node_id, right_node_id, left_keys, right_keys, join_type` | Add a join transform node (LEFT then RIGHT edge order). Cross joins are rejected. IMPORTANT: join_type is required - specify 'inner', 'left', 'right', 'full', or 'cross'. |
| `plan_add_normalize` | `plan, input_node_id, columns` | Add a normalize transform node. WARNING: Default behavior modifies data (trim=true, empty_to_null=true, whitespace_to_null=true). Set these to false explicitly if you want to preserve original values. |
| `plan_add_output` | `plan, input_node_id, output_name` | Add an output node + outputs[] entry. |
| `plan_add_pattern_mining` | `plan, input_node_id, source_column, pattern, output_column` | Add patternMining transform (contains/extract regex semantics). |
| `plan_add_pivot` | `plan, input_node_id, index, columns, values` | Add a pivot transform node (groupBy(index...).pivot(columns).agg(values)). |
| `plan_add_regex_replace` | `plan, input_node_id, rules` | Add a regexReplace transform node. rules=[{column,pattern,replacement,flags?}]. |
| `plan_add_rename` | `plan, input_node_id` | Add a rename transform node. Prefer rename={src:dst}, but renames/mappings as [{from,to}] are also accepted. |
| `plan_add_select` | `plan, input_node_id, columns` | Add a select transform node. |
| `plan_add_select_expr` | `plan, input_node_id, expressions` | Add a select transform node using Spark selectExpr expressions. |
| `plan_add_sort` | `plan, input_node_id, columns` | Add a sort transform node. columns supports ['col','-col2'] or [{'column','direction'}]. |
| `plan_add_split` | `plan, input_node_id, expression` | Add split semantics as macro-expanded true/false filter branches. |
| `plan_add_stream_join` | `plan, left_node_id, right_node_id, left_keys, right_keys` | Add streamJoin transform with strategy metadata (dynamic|left_lookup|static). |
| `plan_add_transform` | `plan, operation, input_node_ids` | Add a generic transform node (operation + metadata) with edges from input_node_ids. |
| `plan_add_udf` | `plan, input_node_id` | Add a reference-only UDF transform node (udfId + pinned udfVersion). |
| `plan_add_union` | `plan, left_node_id, right_node_id` | Add a union transform node for two inputs (unionByName). union_mode: strict|common_only|pad_missing_nulls|pad. |
| `plan_add_window` | `plan, input_node_id` | Add a window transform node. order_by supports ['-col'] for DESC or [{'column','direction'}]. |
| `plan_add_window_expr` | `plan, input_node_id, expressions` | Add a window transform node computing one or more Spark SQL window expressions. |
| `plan_configure_input_read` | `plan, node_id` | Patch an input node's Spark read config (format/options/schema, permissive parsing, corrupt record capture). |
| `plan_delete_edge` | `plan, from_node_id, to_node_id` | Delete edges from->to (no-op if not found). |
| `plan_delete_node` | `plan, node_id` | Delete a node and any incident edges; for output nodes also removes outputs[] entry. |
| `plan_evaluate_joins` | `plan` | Evaluate join nodes in a plan (coverage/explosion) using sample-safe execution. |
| `plan_new` | `goal, db_name` | Create a new PipelinePlan JSON (empty nodes/edges). |
| `plan_preview` | `plan` | Preview a plan via the deterministic PipelineExecutor (sample-safe). Note: limit is capped at 200 rows. For larger previews, use pipeline_preview_wait. |
| `plan_refute_claims` | `plan` | Refute plan-embedded claims with concrete counterexamples (witness-based hard gate). Note: Stops after max_hard_failures (default: 5) or max_soft_warnings (default: 20). |
| `plan_reset` | `plan` | Reset an existing plan to empty (preserves goal + data_scope). |
| `plan_set_node_inputs` | `plan, node_id, input_node_ids` | Replace all incoming edges to node_id with input_node_ids (in order). |
| `plan_update_node_metadata` | `plan, node_id` | Patch node.metadata (merge by default, replace if requested). Use `set` (aliases: `metadata`, `meta`). |
| `plan_update_output` | `plan` | Patch outputs[] entry (by output_name or output node_id); keeps output node metadata.outputName in sync if renamed. |
| `plan_update_settings` | `plan` | Patch plan.definition_json.settings (merge by default, replace if requested). Use `set` (alias: `settings`). |
| `plan_validate` | `plan` | Validate a PipelinePlan using dataset-aware preflight rules. |
| `plan_validate_structure` | `plan` | Validate plan.definition_json structure without resolving dataset schemas. |

<!-- END AUTO-GENERATED: pipeline_tooling_reference -->
