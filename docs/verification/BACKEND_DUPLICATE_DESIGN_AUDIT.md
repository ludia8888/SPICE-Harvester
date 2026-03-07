# Backend Duplicate Design Audit

Date: 2026-03-07

## Summary
- Scope: `backend` runtime plus backend-facing compose/test contracts.
- Goal: confirm remaining duplicate design patterns, fix high-confidence runtime bugs, and leave a prioritized backlog for the rest.
- Result: 22 confirmed duplicate-design/runtime issues were fixed across the audit + rescan passes. After the latest backend/runtime rescan, no additional high-confidence runtime duplicate-design bugs remain open.
- Verification:
  - targeted payload/localized-text helper convergence set: `70 passed`
  - targeted Foundry ontology resource-unwrap convergence set: `148 passed`
  - targeted MCP/objectify regression set: `20 passed`
  - targeted Foundry connectivity execute contract set: `30 passed`
  - targeted Foundry connectivity create/replace contract set: `13 passed`
  - targeted Foundry ontology typed-ref normalization set: `4 passed`
  - targeted ontology property identity normalization set: `8 passed`
  - targeted ontology key-spec registry normalization set: `1 passed`
  - targeted shared key-spec property identity set: `11 passed`
  - targeted object-type contract service key inference set: `4 passed`
  - targeted object-type Foundry projection key inference set: `2 passed`
  - targeted AI schema-context ontology normalization set: `1 passed`
  - targeted MCP ontology search ontology normalization set: `1 passed`
  - targeted OMS nested property identity set: `59 passed`
  - full backend unit regression: `1701 passed, 4 skipped`
  - full backend regression: `2040 passed, 13 skipped`

## Confirmed And Fixed
### P1: MCP objectify mapping-spec creation bypassed the canonical BFF contract path
- Classification: duplicate design + implementation bug
- Impact: `pipeline_mcp_server` could create PostgreSQL mapping specs directly and then dual-write `property_mappings` into OMS `backing_source`, which meant the same logical mapping lived in two stores with different selection/validation rules.
- Affected paths:
  - `backend/mcp_servers/pipeline_tools/objectify_tools.py`
  - `backend/bff/routers/objectify_mapping_specs.py`
  - `backend/bff/services/objectify_mapping_spec_service.py`
- Fix: route `objectify_create_mapping_spec` through `POST /api/v1/objectify/mapping-specs` and remove OMS dual-write from MCP.
- Regression:
  - `backend/tests/unit/mcp/test_pipeline_mcp_canonical_objectify_paths.py`

### P1: MCP objectify-run path duplicated mapping resolution and could drift from BFF
- Classification: duplicate design + implementation bug
- Impact: `pipeline_mcp_server` had its own objectify enqueue path that resolved OMS backing mappings and PostgreSQL mapping specs locally, so MCP behavior could diverge from the canonical BFF runtime selection order.
- Affected paths:
  - `backend/mcp_servers/pipeline_tools/objectify_tools.py`
  - `backend/bff/services/objectify_run_service.py`
- Fix:
  - route `objectify_run` through `POST /api/v1/objectify/datasets/{dataset_id}/run`
  - make BFF runtime prefer a matching PostgreSQL mapping spec for `target_class_id` and only fall back to legacy OMS `backing_source` mappings when no canonical spec exists
- Regression:
  - `backend/tests/unit/mcp/test_pipeline_mcp_canonical_objectify_paths.py`
  - `backend/tests/unit/services/test_objectify_run_service_version_pin.py`

### P1: MCP object-type registration bypassed canonical BFF v2 contract service
- Classification: duplicate design + implementation bug
- Impact: `ontology_register_object_type` used direct OMS resource writes and stored column mappings in OMS `backing_source`, bypassing BFF object-type contract validation, backing resolution, mapping-spec linkage, and migration/update policy.
- Affected paths:
  - `backend/mcp_servers/pipeline_tools/ontology_tools.py`
  - `backend/bff/routers/foundry_ontology_v2.py`
  - `backend/bff/services/object_type_contract_service.py`
- Fix: route object-type registration through `POST/PATCH /api/v2/ontologies/{ontology}/objectTypes`, create mapping specs through canonical BFF objectify APIs, and patch the object-type contract with `mappingSpecId/mappingSpecVersion` instead of storing mappings directly in OMS.
- Regression:
  - `backend/tests/unit/mcp/test_pipeline_mcp_canonical_objectify_paths.py`

### P0: Objectify job dedupe conflict could still enqueue duplicate outbox work
- Classification: duplicate design + implementation bug
- Impact: objectify job requests that race on the same `dedupe_key` could reuse the existing job row but still insert another outbox row, which risks duplicate publish/retry for the same job.
- Affected path: `backend/shared/services/registries/objectify_registry.py`
- Fix: only create `objectify_job_outbox` rows when the job row was newly inserted, not when the unique conflict path returns an existing job.
- Regression: `backend/tests/unit/services/test_objectify_registry_job_dedupe.py`

### P1: Objectify DAG mapping lookup had dead fallback-branch logic
- Classification: duplicate design + implementation bug
- Impact: object types backed by a non-`main` dataset branch could fail to find an active mapping spec even when one existed on fallback branches.
- Affected path: `backend/bff/services/objectify_dag_service.py`
- Fix: replace the duplicated inline branch fallback logic with the shared `build_branch_candidates()` helper so DAG orchestration uses the same branch candidate policy as other dataset resolution paths.
- Regression: `backend/tests/unit/services/test_objectify_dag_service.py`

### P1: key spec fallback logic had drift between shared helper and workers
- Classification: duplicate design + implementation bug
- Impact: object/ontology payloads that use `primaryKey` / `titleKey` flags on properties could be read correctly in worker-specific code but missed by shared normalization paths when `pk_spec` was absent.
- Affected paths:
  - `backend/shared/utils/key_spec.py`
  - `backend/ontology_worker/main.py`
  - `backend/objectify_worker/main.py`
  - `backend/bff/routers/object_types.py`
- Fix: expand the shared key-spec helper to support camelCase property flags and reuse it from the ontology/objectify worker paths and Foundry object-type projection.
- Regression: `backend/tests/unit/utils/test_key_spec.py`

### P2: Ontology key-spec registry kept its own key-column normalizer
- Classification: duplicate design
- Impact: `ontology_key_spec_registry` still normalized persisted `primary_key/title_key` values with a local comma-split helper, so wrapper aliases such as `primaryKeys` or future canonical key-column rules could drift from the shared key-spec contract used elsewhere.
- Affected paths:
  - `backend/shared/services/registries/ontology_key_spec_registry.py`
  - `backend/shared/utils/key_spec.py`
- Fix: remove the registry-local normalizer and route persisted key-spec writes through `normalize_key_columns()`.
- Regression:
  - `backend/tests/unit/services/test_ontology_key_spec_registry.py`

### P2: Object-type contract service duplicated ontology property flag extraction
- Classification: duplicate design
- Impact: `_infer_pk_spec_from_ontology()` reimplemented `primaryKey/titleKey` flag extraction locally instead of using the shared key-spec helper, so future changes to property-flag interpretation could drift between object-type contract bootstrap and the rest of the key-spec pipeline.
- Affected paths:
  - `backend/bff/services/object_type_contract_service.py`
  - `backend/shared/utils/key_spec.py`
- Fix: route ontology property flag extraction through `derive_key_spec_from_properties()` while keeping the service-specific fallback policy (`<class>_id`, `id`, `name`) intact.
- Regression:
  - `backend/tests/unit/services/test_object_type_contract_service.py`

### P1: Foundry ontology source primary-key normalization drifted from linked-object normalization
- Classification: duplicate design + implementation bug
- Impact: linked object refs in Foundry ontology object-set responses stripped typed refs like `Order/order-1 -> order-1`, but source object primary keys and row-identity dedupe still used raw values. The same object could therefore appear under different identities depending on which router path normalized it first.
- Affected paths:
  - `backend/bff/routers/foundry_ontology_v2.py`
- Fix:
  - centralize source-row primary-key lookup on `_PRIMARY_KEY_VALUE_KEYS`
  - normalize source PKs and row-dedupe identity through the same typed-ref stripping helper already used for linked targets
- Regression:
  - `backend/tests/unit/openapi/test_foundry_ontology_v2_contract.py`
  - `backend/tests/unit/routers/test_foundry_ontology_v2_helpers.py`

### P1: Shared ontology property extraction dropped `id`-only properties after helper centralization
- Classification: duplicate design + implementation bug
- Impact:
  - the shared `ontology_fields` helper only indexed properties by `name`, while some ontology payloads still identify fields by `id`
  - after router/service convergence onto the shared helper, Foundry object-type projection could silently lose `id`-based primary-key fields and fall back to the wrong primary key
- Affected paths:
  - `backend/shared/utils/ontology_fields.py`
  - `backend/bff/routers/object_types.py`
  - `backend/bff/routers/foundry_ontology_v2.py`
- Fix:
  - make shared ontology property extraction honor `name` first and `id` as the canonical fallback
  - route Foundry ontology property-list reads through the shared helper
  - keep Foundry object-type projection on the shared key-spec derivation path so `primaryKey/titleKey` flag handling and `id`-based field identity stay aligned
- Regression:
  - `backend/tests/unit/utils/test_ontology_fields.py`
  - `backend/bff/tests/test_object_types_foundry_projection.py`

### P1: Object-type contract service parsed ontology property identity differently across validation and auto-mapping
- Classification: duplicate design + implementation bug
- Impact:
  - `_infer_pk_spec_from_ontology()` already honored `properties[].id` as a fallback field identity, but `_extract_ontology_property_names()` and auto-generated mapping target-schema assembly still read only `name`
  - the same ontology payload could therefore infer `order_id` as the primary key and then reject that key as “missing from ontology properties”, or omit `id`-only fields from auto-generated mapping suggestions
- Affected paths:
  - `backend/bff/services/object_type_contract_service.py`
  - `backend/shared/utils/ontology_fields.py`
- Fix:
  - route property-name extraction, PK inference, and auto-mapping target-schema assembly through `list_ontology_properties()`
  - keep `name` as the preferred field identity and `id` as the canonical fallback everywhere in the service
- Regression:
  - `backend/tests/unit/services/test_object_type_contract_service.py`

### P1: Shared key-spec helper still ignored `properties[].id` in raw ontology/object-type payloads
- Classification: duplicate design + implementation bug
- Impact:
  - `derive_key_spec_from_properties()` only read `name`, while multiple worker/service paths passed raw payloads that may identify fields by `id`
  - ontology/objectify workers and other shared consumers could therefore miss `primaryKey/titleKey` flags, fall back to metadata key specs, or reject otherwise valid contracts
- Affected paths:
  - `backend/shared/utils/key_spec.py`
  - `backend/ontology_worker/main.py`
  - `backend/objectify_worker/main.py`
- Fix:
  - make the shared helper treat `name` as preferred and `id` as the canonical fallback field identity
  - keep raw consumers on the shared helper instead of adding more local normalization layers
- Regression:
  - `backend/tests/unit/utils/test_key_spec.py`
  - `backend/tests/unit/workers/test_objectify_worker_key_contract.py`

### P2: AI schema-context builder duplicated ontology property/relationship parsing rules
- Classification: duplicate design + implementation bug
- Impact:
  - `_load_schema_context()` rebuilt ontology property and relationship parsing locally, so prompt/planner context could drop `id`-only properties or `name`-only relationships even though the rest of the backend already accepted those forms
  - natural-language planning could therefore reason over an incomplete schema snapshot
- Affected paths:
  - `backend/bff/services/ai_service.py`
  - `backend/shared/utils/ontology_fields.py`
- Fix:
  - route schema-context property extraction through `list_ontology_properties()`
  - route relationship extraction through `extract_ontology_relationships()`
  - keep compact AI schema summaries aligned with the canonical ontology field parser
- Regression:
  - `backend/tests/unit/services/test_ai_service_schema_context.py`

### P2: MCP ontology class search duplicated ontology property parsing and missed `id`-only fields
- Classification: duplicate design + implementation bug
- Impact:
  - `ontology_search_classes` matched property names with a router-local `prop["name"]` parser, so ontology search results could miss classes whose properties were identified only by `id`
  - this made MCP ontology exploration less reliable than the canonical backend ontology parsers
- Affected paths:
  - `backend/mcp_servers/ontology_mcp_server.py`
  - `backend/shared/utils/ontology_fields.py`
- Fix:
  - route ontology search property matching through `list_ontology_properties()`
  - keep MCP ontology discovery aligned with the shared ontology field identity rules
- Regression:
  - `backend/tests/unit/mcp/test_ontology_mcp_server.py`

### P1: OMS read routers duplicated nested instance-property parsing and missed `id`-only fields
- Classification: duplicate design + implementation bug
- Impact:
  - object search flattening, timeseries property lookup, and attachment property lookup each re-parsed nested instance `properties[]` rows with `prop["name"]` only
  - instance documents carrying legacy or normalized `id`-only property entries could therefore search correctly in some paths but disappear in object payloads, timeseries lookups, or attachment reads
- Affected paths:
  - `backend/oms/routers/query.py`
  - `backend/oms/routers/timeseries.py`
  - `backend/oms/routers/attachments.py`
  - `backend/shared/utils/instance_properties.py`
- Fix:
  - add shared nested-property helpers for `name`-preferred / `id`-fallback identity
  - route object-search flattening, timeseries property lookup, and attachment property lookup through the same helper
- Regression:
  - `backend/tests/unit/utils/test_instance_properties.py`
  - `backend/tests/unit/oms/test_object_search_router.py`
  - `backend/tests/unit/oms/test_timeseries_router.py`
  - `backend/tests/unit/oms/test_attachments_router.py`

### P2: Foundry read routers kept their own payload unwrappers and localized-text selectors
- Classification: duplicate design
- Impact:
  - `link_types_read`, `object_types`, and `foundry_ontology_v2` each carried near-identical `payload.data` unwrapping, `resources[]` filtering, and `en/ko` display-text selection helpers
  - the duplication had already started to drift structurally, which made future read-surface fixes likely to land in only one router family
- Affected paths:
  - `backend/shared/utils/payload_utils.py`
  - `backend/shared/utils/language.py`
  - `backend/bff/routers/link_types_read.py`
  - `backend/bff/routers/object_types.py`
  - `backend/bff/routers/foundry_ontology_v2.py`
- Fix:
  - add shared `extract_payload_rows()` / `extract_payload_object()` helpers for normalized `payload.data` object/list extraction
  - add shared `first_localized_text()` for stable `en -> ko -> any` read-surface label selection
  - keep router-local helper names as thin wrappers so tests and callers preserve the existing surface while the behavior now comes from one shared contract
  - route remaining direct ontology resource fetches inside `foundry_ontology_v2` through the same shared/object-router unwrap helpers instead of repeating local `payload.get("data")` branches
- Regression:
  - `backend/tests/unit/utils/test_payload_utils.py`
  - `backend/tests/unit/utils/test_language.py`
  - `backend/tests/unit/routers/test_foundry_ontology_v2_helpers.py`
  - `backend/bff/tests/test_link_types_retrieval.py`
  - `backend/bff/tests/test_object_types_foundry_projection.py`
  - `backend/tests/unit/openapi/test_foundry_ontology_v2_contract.py`
  - `backend/bff/tests/test_foundry_ontology_v2_router.py`

### P1: Objectify enqueue pre-checks were duplicated across BFF, workers, MCP, and connector helpers
- Classification: duplicate design
- Impact: call sites were re-implementing “existing job vs newly enqueued job” semantics around the same registry contract, which kept response behavior and dedupe handling fragile.
- Affected paths:
  - `backend/bff/services/objectify_run_service.py`
  - `backend/bff/services/objectify_dag_service.py`
  - `backend/bff/routers/objectify_job_ops.py`
  - `backend/bff/routers/pipeline_datasets_ops_objectify.py`
  - `backend/shared/services/core/connector_ingest_service.py`
  - `backend/pipeline_worker/main.py`
  - `backend/mcp_servers/pipeline_tools/objectify_tools.py`
- Fix: add `ObjectifyJobEnqueueResult(created, record)` plus `get_or_enqueue_objectify_job()` at the registry/queue layer and switch runtime call sites to that shared contract.
- Regression:
  - `backend/tests/unit/services/test_objectify_registry_job_dedupe.py`
  - `backend/tests/unit/services/test_objectify_run_service_version_pin.py`
  - `backend/tests/unit/workers/test_pipeline_worker_objectify_auto_enqueue.py`
  - `backend/tests/unit/workers/test_pipeline_worker_objectify_auto_enqueue_nospark.py`
  - `backend/bff/tests/test_pipeline_router_uploads.py`

### P1: Virtual output dataset-setting validation was split across plugin, preflight, and worker runtime
- Classification: duplicate design
- Impact: the same “virtual outputs must not accept dataset-style write settings” rule was maintained separately, so one path could reject metadata that another path silently accepted.
- Affected paths:
  - `backend/shared/services/pipeline/output_plugins.py`
  - `backend/shared/services/pipeline/pipeline_preflight_utils.py`
  - `backend/pipeline_worker/output_domain.py`
- Fix: centralize unsupported-setting detection in `find_virtual_dataset_style_settings()` and route the worker runtime through the same `validate_output_payload()` contract.
- Regression:
  - `backend/tests/unit/services/test_output_plugins.py`
  - `backend/tests/unit/services/test_pipeline_preflight_dataset_output.py`
  - `backend/tests/unit/workers/test_spark_advanced_transforms.py`

### P2: Connector import-config primary-key alias parsing was duplicated
- Classification: duplicate design
- Impact: connector import/update paths were each re-parsing `primary_key_column` and `primaryKeyColumn`, so future alias changes could drift between BFF orchestration and worker sync behavior.
- Affected paths:
  - `backend/bff/services/data_connector_pipelining_service.py`
  - `backend/connector_sync_worker/main.py`
- Fix: add shared helper `resolve_primary_key_column()` and route both paths through it.
- Regression:
  - `backend/tests/unit/utils/test_connector_import_config.py`

### P1: Objectify worker still bypassed the shared object-type key-spec helper
- Classification: duplicate design + implementation bug
- Impact: objectify jobs and link-index jobs could reject a valid object-type contract when it expressed key flags only through `properties[].primaryKey/titleKey` and omitted `pk_spec`.
- Affected paths:
  - `backend/objectify_worker/main.py`
- Fix: replace the worker-local `pk_spec` fallback logic with `normalize_object_type_key_spec()` in both the object path and link-index path.
- Regression:
  - `backend/tests/unit/workers/test_objectify_worker_key_contract.py`
  - `backend/tests/unit/workers/test_objectify_worker_link_index_dangling.py`

### P2: Foundry connectivity import surface was triplicated across table/file/virtual-table routes
- Classification: duplicate design
- Impact:
  - `execute_table_import_v2`, `execute_file_import_v2`, and `execute_virtual_table_v2` each maintained the same pipeline-ensure, build-run recording, success/failure envelope, and actor propagation logic
  - `load/get/list` paths for the same three resource kinds also repeated owner-resolution, response-building, and pagination parsing logic
  - future fixes were likely to land in only one of the three resource kinds
- Affected paths:
  - `backend/bff/routers/foundry_connectivity_v2.py`
- Fix:
  - centralize import pipeline creation in `_ensure_connector_import_pipeline()`
  - centralize build-run orchestration in `_execute_connector_import_build()`
  - centralize execute preflight loading/validation in `_load_validated_executable_connector_import()`
  - centralize import-source loading in `_load_connector_import_source()`
  - centralize resource response shaping in `_build_connector_import_response()`
  - centralize page-size/page-token parsing and owned-source collection for import resource lists
  - centralize create/replace source-config assembly in `_build_connector_import_source_config()`
  - centralize create/replace mapping status/upsert rules in `_upsert_created_connector_import_mapping()` and `_upsert_replaced_connector_import_mapping()`
  - add regressions covering shared execution, not-ready handling, and replace-contract behavior so table/file/virtual-table no longer maintain separate state-transition logic
- Regression:
  - `backend/tests/unit/openapi/test_foundry_platform_v2_contract.py`

## Downgraded Or Intentional
### Objectify link-index job builder dedupe lookup
- Status: intentional duplication
- Rationale: `backend/shared/services/core/link_index_job_builder.py` still reads `get_objectify_job_by_dedupe_key()`, but it is a builder that returns either `existing_job_id` or a job payload without enqueuing. After queue/registry centralization, this is no longer a duplicated enqueue contract.

### MCP SDK wrappers vs workflow MCP tools
- Status: intentional duplication
- Rationale:
  - `backend/mcp_servers/bff_sdk_mcp_server.py` is a thin SDK-style wrapper over BFF REST endpoints and still intentionally overlaps with specific MCP tools and with `bff_api_call`
  - `backend/mcp_servers/pipeline_mcp_server.py` exposes higher-level workflow tools (`objectify_run`, `ontology_register_object_type`, planning, waits, fan-out helpers)
  - after the latest fixes, these surfaces now converge on the same canonical BFF APIs instead of maintaining separate business rules

### `bff_start_objectify` vs dataset-level objectify MCP tools
- Status: intentional duplication
- Rationale: `bff_start_objectify` is a pipeline-level fan-out macro over `POST /api/v1/objectify/datasets/{dataset_id}/run`, while `objectify_run` remains the dataset-level execution tool. Capability overlaps, but runtime decisions are now delegated to the same BFF path.

### `bff_api_call` overlap with specialized SDK tools
- Status: intentional duplication
- Rationale: the generic SDK tool intentionally overlaps every specialized SDK wrapper. It exists as an escape hatch for uncovered endpoints, not as a second source of business logic.

### Dataset-output PK alias normalization in planner/adapters
- Status: intentional duplication
- Rationale: `pipeline_definition_utils.resolve_pk_columns()`, `pipeline_ops_schema`, and MCP planner helpers still normalize several alias forms (`primary_key_columns`, `primaryKeyColumns`, `pkColumns`) before runtime execution. This is adapter/planner normalization, while `dataset_output_semantics` remains the canonical runtime validator. No runtime drift was reproduced in the rescan.

### Object-type projection / inference helpers
- Status: intentional duplication
- Rationale: `object_type_contract_service` and Foundry projection helpers still contain some fallback selection logic because they solve projection/bootstrap problems, not runtime contract enforcement. Property-flag extraction and ontology field identity now converge on shared helpers, and the latest rescan did not reproduce remaining runtime drift from these paths.

### Event identity / sequence handling
- Status: intentional duplication
- Rationale: `spice_event_id`, aggregate sequence allocation, processed-event ordering, and pipeline control-plane event ids are used for different streams with different dedupe scopes. No same-stream contract bug was confirmed in this audit pass.

### Agent session existence guards
- Status: intentional duplication for now
- Rationale: `agent_session_registry` repeats `get_session()` checks around distinct mutations, but this is currently a service-shape issue rather than a confirmed race or contract failure.

### Settings / error / infra drift rescan
- Status: no new confirmed issue
- Rationale: the follow-up scan did not find new runtime regressions in `settings` SSoT usage, error-envelope usage, or compose/test readiness contracts beyond the already-fixed items covered by existing guard tests.

### Pipeline executor vs preflight schema helpers
- Status: intentional duplication for now
- Rationale: `pipeline_executor` and `pipeline_preflight_utils` still have similarly named schema/sample extraction helpers, but the executor path is runtime-oriented (ordered preview table columns + cast-target defaults), while preflight normalizes BOM/deduped schema metadata for validation. The latest rescan did not reproduce a same-contract runtime drift severe enough to justify merging them in this pass.
