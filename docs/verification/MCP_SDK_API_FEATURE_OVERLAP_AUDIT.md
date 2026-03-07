# MCP / SDK / API Feature Overlap Audit

Date: 2026-03-07

## Scope
- `backend/mcp_servers/bff_sdk_mcp_server.py`
- `backend/mcp_servers/pipeline_mcp_server.py`
- `backend/mcp_servers/pipeline_tools/*.py`
- canonical BFF API routes those tools call

## Conclusion
- Functional overlap still exists.
- After the latest MCP canonicalization work, no new high-confidence harmful duplicate business logic was found in this rescan.
- Internal duplicate request-construction logic for pipeline execution payloads and objectify run bodies has been reduced into shared MCP helpers.
- Internal duplicate upstream-response parsing and objectify job-status serialization in MCP tools has also been reduced into shared helpers.
- Build RID to job-id extraction is now shared across MCP, agent runtime, and Foundry orchestration surfaces, including both `ri.foundry...build...` and `build://...` forms.
- Remaining overlap is mostly one of two categories:
  - raw SDK-style wrapper vs workflow macro
  - generic escape hatch vs specialized tool

## Confirmed Functional Overlap Groups

### 1. Pipeline execution family
- Overlap type: raw wrapper vs workflow macro
- Canonical routes:
  - `POST /api/v2/orchestration/builds/create`
  - `GET /api/v1/pipelines/{pipeline_id}/runs`
- Surfaces:
  - `bff_execute_pipeline`
  - `pipeline_preview_wait`
  - `pipeline_build_wait`
  - `pipeline_deploy_promote_build`
- Why this overlaps:
  - all of these tools operate on the same pipeline run lifecycle
  - SDK wrapper exposes the raw enqueue call
  - pipeline MCP exposes wait/reuse/poll/deploy orchestration around the same execution family
- Current status:
  - intentional overlap
  - no divergent run-creation business logic was found beyond macro behavior

### 2. Objectify execution family
- Overlap type: raw wrapper vs workflow macro
- Canonical routes:
  - `POST /api/v1/objectify/datasets/{dataset_id}/run`
  - `GET /api/v1/pipelines/{pipeline_id}/datasets`
- Surfaces:
  - `bff_start_objectify`
  - `objectify_run`
  - `objectify_wait`
  - `objectify_get_status`
- Why this overlaps:
  - `bff_start_objectify` fans out pipeline datasets into repeated dataset-level objectify runs
  - `objectify_run` is the dataset-level canonical execution tool
- Current status:
  - intentional overlap
  - the harmful duplicate logic that previously existed in MCP objectify selection/creation was already removed; both surfaces now converge on canonical BFF objectify routes

### 3. Objectify mapping-spec / object-type contract family
- Overlap type: generic API overlap plus domain-specific macro
- Canonical routes:
  - `POST /api/v1/objectify/mapping-specs`
  - `POST/PATCH /api/v2/ontologies/{ontology}/objectTypes`
- Surfaces:
  - `objectify_create_mapping_spec`
  - `ontology_register_object_type`
  - `bff_api_call`
- Why this overlaps:
  - specialized MCP tools now wrap the canonical BFF contract/mapping APIs
  - generic SDK API call can hit the same routes directly
- Current status:
  - intentional overlap
  - no separate MCP-side business rules remain in this path

### 4. Graph query family
- Overlap type: raw wrapper vs pre-shaped domain wrapper
- Canonical route:
  - `POST /api/v1/graph-query/{db_name}/simple`
- Surfaces:
  - `bff_simple_graph_query`
  - `ontology_query_instances`
- Why this overlaps:
  - both hit the same simple graph query route
  - `ontology_query_instances` constrains the request to `class_type + limit + filters`, masks PII, and returns a friendlier objectify-verification shape
  - `bff_simple_graph_query` exposes the raw request body
- Current status:
  - intentional overlap
  - no conflicting query semantics were found

### 5. Pipeline CRUD family
- Overlap type: raw API vs plan-aware macro
- Canonical routes:
  - `POST /api/v1/pipelines`
  - `PUT /api/v1/pipelines/{pipeline_id}`
- Surfaces:
  - `pipeline_create_from_plan`
  - `pipeline_update_from_plan`
  - `bff_api_call`
- Why this overlaps:
  - pipeline MCP tools turn `PipelinePlan` into BFF payloads and add idempotent upsert behavior
  - generic API tool can call the same routes directly
- Current status:
  - intentional overlap
  - extra behavior is macro/orchestration behavior, not a second source of pipeline runtime rules

### 6. Pipeline run inspection family
- Overlap type: route-family overlap
- Canonical route:
  - `GET /api/v1/pipelines/{pipeline_id}/runs`
- Surfaces:
  - `bff_list_objectify_runs`
  - `pipeline_preview_wait`
  - `pipeline_build_wait`
  - `pipeline_deploy_promote_build`
- Why this overlaps:
  - all of these inspect the same pipeline run ledger
  - each tool filters or interprets the run list differently
- Current status:
  - intentional overlap
  - no duplicate mutation logic found

### 7. Generic SDK escape hatch
- Overlap type: structural overlap by design
- Canonical routes:
  - any BFF v1/v2 endpoint
- Surfaces:
  - `bff_api_call`
  - every specialized `bff_*` tool
  - many pipeline MCP tools that wrap BFF endpoints
- Why this overlaps:
  - `bff_api_call` is intentionally a catch-all for uncovered endpoints
- Current status:
  - intentional overlap
  - cannot be eliminated without removing the escape hatch itself

## Surfaces That Look Similar But Are Not The Same Capability

### `bff_list_datasets` vs `dataset_list`
- `bff_list_datasets`: datasets attached to one pipeline
- `dataset_list`: datasets in a database/project
- Result: not a true duplicate capability

### `bff_list_ontology_classes` / `bff_get_ontology_class` vs `ontology_register_object_type`
- ontology class CRUD and object-type contract registration are different domain objects
- Result: not a true duplicate capability

### `plan_preview` vs `pipeline_preview_wait`
- `plan_preview` uses deterministic plan execution / sample-safe local preview
- `pipeline_preview_wait` queues Spark preview jobs against pipeline execution infrastructure
- Result: adjacent, not duplicate

### `data_query` vs graph query tools
- `data_query` runs SQL over sampled tabular rows
- graph query tools operate on ontology/object graph reads
- Result: different capability

## Previously Harmful Overlap That Is Now Closed
- MCP objectify mapping-spec creation used to dual-write PostgreSQL + OMS
- MCP objectify run used to reimplement mapping selection logic
- MCP object-type registration used to bypass canonical BFF v2 contract APIs
- Those paths now converge on:
  - `POST /api/v1/objectify/mapping-specs`
  - `POST /api/v1/objectify/datasets/{dataset_id}/run`
  - `POST/PATCH /api/v2/ontologies/{ontology}/objectTypes`

## Final Assessment
- If the question is “is there any functional overlap at all?” the answer is yes.
- If the question is “is there still high-confidence harmful duplicate business logic across MCP / SDK / API?” this rescan did not find a new open case.
- To make overlap literally zero, the product would need a surface-reduction decision:
  - remove or hide `bff_api_call`
  - choose either raw SDK wrappers or workflow macros as the primary user surface
  - deprecate overlapping helpers such as `bff_execute_pipeline` vs `pipeline_*_wait`
