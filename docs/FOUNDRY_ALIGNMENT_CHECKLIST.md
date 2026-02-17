# Foundry Alignment Checklist

Source policy:
- Use Palantir Foundry official docs as the decision baseline.
- If behavior is unclear, stop and verify against official docs before implementation.

Official references:
- API v2 overview/index: https://www.palantir.com/docs/foundry/api/v2
- Object Backend Overview: https://www.palantir.com/docs/foundry/object-backend/overview
- Search Objects (v2): https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-objects/search-objects
- List Objects (v2): https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-objects/list-objects
- Get Object (v2): https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-objects/get-object
- List Linked Objects (v2): https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/linked-objects/list-linked-objects
- Get Linked Object (v2): https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/linked-objects/get-linked-object
- Search JSON Query body (v2 Search Objects): https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-objects/search-objects
- Load Object Set Objects (v2): https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-object-sets/load-object-set-objects
- Load Object Set Multiple Object Types (v2): https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-object-sets/load-object-set-multiple-object-types
- Load Object Set Objects Or Interfaces (v2): https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-object-sets/load-object-set-objects-or-interfaces
- Aggregate Object Set (v2): https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-object-sets/aggregate-object-set
- Create Temporary Object Set (v2): https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-object-sets/create-temporary-object-set/
- Get Ontology Full Metadata (v2): https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontologies/get-ontology-full-metadata
- List Interface Types (v2): https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-interfaces/list-interface-types
- Get Interface Type (v2): https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-interfaces/get-interface-type
- List Outgoing Link Types (v2): https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/object-types/list-outgoing-link-types
- Get Outgoing Link Type (v2): https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/object-types/get-outgoing-link-type
- List Action Types (v2): https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/action-types/list-action-types
- Get Action Type (v2): https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/action-types/get-action-type
- Get Action Type by RID (v2): https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/action-types/get-action-type-by-rid
- Apply Action (v2): https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/actions/apply-action
- Apply Action Batch (v2): https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/actions/apply-action-batch
- List Query Types (v2): https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/query-types/list-query-types
- Get Query Type (v2): https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/query-types/get-query-type
- List Value Types (v2): https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-value-types/list-ontology-value-types
- Get Value Type (v2): https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-value-types/get-ontology-value-type
- List Ontologies (v2): https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontologies/list-ontologies
- Connectivity Table Imports (v2): https://www.palantir.com/docs/foundry/api/v2/connectivity-v2-resources/table-imports/create-table-import
- Datasets Schema (v2): https://www.palantir.com/docs/foundry/api/v2/datasets-v2-resources/datasets/get-dataset-schema
- API Errors (v2): https://www.palantir.com/docs/foundry/api/general/overview/errors
- Object Edits Overview: https://www.palantir.com/docs/foundry/object-edits/overview
- How Edits Are Applied: https://www.palantir.com/docs/foundry/object-edits/how-edits-applied/

Supplementary references (non-authoritative, validation aid only):
- Foundry platform Python SDK (`develop`): https://github.com/palantir/foundry-platform-python
- Foundry SDK `OntologyObjectSet.load_links` contract (preview): https://github.com/palantir/foundry-platform-python/blob/develop/foundry_sdk/v2/ontologies/ontology_object_set.py
- Palantir Developer Community: https://community.palantir.com/

Current status:
- [x] OMS Search Objects read surface is Foundry-style (`/objects/{db_name}/{object_type}/search`).
- [x] SearchJsonQueryV2 operator mapping is implemented in OMS router.
- [x] SearchJsonQueryV2 contract aligned with Foundry docs: undocumented `in` operator removed; `isNull` supports boolean null/not-null semantics.
- [x] Search Objects branch parameter now accepts Foundry-style branch RID values (`ri.ontology.main.branch...`) in addition to branch names.
- [x] BFF Foundry v2 read/search OpenAPI now exposes `branch` query params on branch-aware routes (`objectTypes`, `outgoingLinkTypes`, `objects/{objectType}/search`).
- [x] BFF Foundry v2 object read surface includes `GET /v2/ontologies/{ontology}/objects/{objectType}` and `GET /v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}` with Foundry-style query params/envelope.
- [x] BFF Foundry v2 object-set load surface includes `POST /v2/ontologies/{ontology}/objectSets/loadObjects` with Foundry-style query params (`branch`, `transactionId`, `sdkPackageRid`, `sdkVersion`) and body paging/select/order fields (`select/selectV2`, `pageSize/pageToken`, `orderBy`, `excludeRid`, `snapshot`).
- [x] BFF Foundry v2 object-set extended surfaces include `POST /v2/ontologies/{ontology}/objectSets/loadLinks` (preview), `POST /v2/ontologies/{ontology}/objectSets/loadObjectsMultipleObjectTypes`, `POST /v2/ontologies/{ontology}/objectSets/loadObjectsOrInterfaces`, `POST /v2/ontologies/{ontology}/objectSets/aggregate`, `POST /v2/ontologies/{ontology}/objectSets/createTemporary`, and `GET /v2/ontologies/{ontology}/objectSets/{objectSetRid}`.
- [x] BFF Foundry v2 linked-object read surface includes `GET /v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/links/{linkType}` and `GET /v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/links/{linkType}/{linkedObjectPrimaryKey}`.
- [x] Foundry-style API error envelope is implemented for object search and v2 read routers.
- [x] v1->v2 migration guide is documented (`docs/FOUNDRY_V1_TO_V2_MIGRATION.md`) and removed read/query compat operations are synchronized to `code deleted` status.
- [x] Migration guide and alignment checklist are cross-synced for P0 strict compat semantics (preview required, fullMetadata branch `rid` shape, strict fixed-on policy).
- [x] v2-successor가 존재하는 주요 v1 read/query compat endpoints are code-deleted (operation removed from runtime handlers and OpenAPI).
- [x] v1 query compatibility endpoint (`POST /api/v1/databases/{db}/query`) is removed from runtime handlers/OpenAPI; callers must use v2 object search.
- [x] v1 ontology read compatibility endpoints (`GET /api/v1/databases/{db}/ontology/link-types*`, `/action-types*`, `/interfaces*`, `/shared-properties*`, `/value-types*`) are removed from BFF runtime/OpenAPI; callers must use v2 `objectTypes/*/outgoingLinkTypes*`, `actionTypes*`, `interfaceTypes*`, `sharedPropertyTypes*`, `valueTypes*`.
- [x] v1 classes read compatibility endpoints (`GET /api/v1/databases/{db}/classes`, `/classes/{class_id}`) are removed from BFF runtime/OpenAPI; callers must use v2 `objectTypes*` read surfaces.
- [x] v1 instances read compatibility endpoints (`GET /api/v1/databases/{db}/class/{class_id}/instances`, `/class/{class_id}/instance/{instance_id}`) are removed from BFF runtime/OpenAPI; callers must use v2 `objects/{objectType}` and `objects/{objectType}/{primaryKey}`.
- [x] v1 single-action submit compatibility endpoints (`POST /api/v1/databases/{db}/actions/{actionType}/submit`, `POST /api/v1/actions/{db}/async/{actionType}/submit`) are removed from BFF/OMS runtime OpenAPI surfaces.
- [x] v1 batch submit compatibility endpoint (`POST /api/v1/databases/{db}/actions/{actionType}/submit-batch`) is removed from BFF runtime/OpenAPI; callers must use v2 `applyBatch`.
- [x] v1 undo compatibility endpoint (`POST /api/v1/databases/{db}/actions/logs/{action_log_id}/undo`) is removed from BFF runtime/OpenAPI; callers must use v2 `POST /api/v2/ontologies/{ontology}/actions/logs/{actionLogId}/undo`.
- [x] OMS legacy async action endpoints (`POST /api/v1/actions/{db}/async/{actionType}/submit-batch`, `POST /api/v1/actions/{db}/async/logs/{actionLogId}/undo`) are removed from runtime OpenAPI; callers must use v2 action apply/undo surfaces.
- [x] OMS legacy async simulate endpoint (`POST /api/v1/actions/{db}/async/{actionType}/simulate`) is removed from runtime OpenAPI; validation-only execution uses v2 `apply` with `options.mode=VALIDATE_ONLY`.
- [x] v1 simulate compatibility endpoint (`POST /api/v1/databases/{db}/actions/{actionType}/simulate`) is removed from BFF runtime/OpenAPI; callers must use v2 `apply` with `options.mode=VALIDATE_ONLY`.
- [x] Foundry v2 action apply surfaces are exposed with official path/parameter shape: `POST /api/v2/ontologies/{ontology}/actions/{action}/apply` (`branch`, `sdkPackageRid`, `sdkVersion`, `transactionId` + body `options.mode`), `POST /api/v2/ontologies/{ontology}/actions/{action}/applyBatch` (`branch`, `sdkPackageRid`, `sdkVersion`).
- [x] BFF action v2 runtime no longer proxies to legacy OMS async v1 routes (`/api/v1/actions/{db}/async/*`); internal calls use OMS v2 action paths (`/api/v2/ontologies/{ontology}/actions/*`).
- [x] Action writeback E2E suites no longer call legacy OMS async submit-batch routes; action submission uses v2 `apply` (`/api/v2/ontologies/{ontology}/actions/{action}/apply`) and log lookup for completion tracking.
- [x] OpenAPI smoke recipe guard covers Foundry v2 preview/action/query endpoints and prevents bool `preview` query regressions in request plans.
- [x] Foundry v2 OpenAPI parameter-surface guard explicitly verifies official actionTypes/queryTypes/fullMetadata contracts (branch/pagination/preview/sdk parameter rules).
- [x] ES-native graph traversal path exists without TerminusDB dependency.
- [x] Ontology persistence in Postgres profile no longer depends on TerminusDB runtime in OMS (legacy/hybrid adapter retained by feature mode).
- [x] OMS boot path supports Postgres-first startup without eager Terminus initialization (`resource_storage_backend=postgres`).
- [x] BFF query/runtime naming is migrated to Foundry semantics (`FoundryQueryService`) and legacy aliases removed.
- [x] Docker/service topology no longer includes TerminusDB in the default core dependency graph (legacy adapter exposed only via `legacy-terminus` compose profile).

Execution roadmap:

Phase 1: Compatibility naming cleanup (low risk)
- [x] Rename BFF query wrapper semantics to `FoundryQueryService` and remove legacy alias exports.
- [x] Rename remaining user-facing messages/docs that still imply Terminus-first query semantics.
- [x] Graph query response/query metadata no longer exposes `terminus_branch` / `es_*` keys; Foundry-neutral keys (`graph_branch`, `read_model_*`) are used, with provenance normalized to `graph` key.

Phase 2: Remove ontology runtime dependency on TerminusDB (high impact)
- [x] Define canonical ontology registry backend (Postgres + versioned records).
- [x] Implement ontology resource CRUD adapter on canonical backend (Postgres runtime).
- [x] Keep Terminus adapter only as migration fallback behind feature flag.
- [x] Migrate OMS ontology routers from `AsyncTerminusService` dependency to backend-agnostic service for write/read validation paths (legacy graph relationship validation path removed).
- [x] Ontology read endpoints (`GET /database/{db_name}/ontology`, `GET /database/{db_name}/ontology/{class_id}`) support Postgres resource-service reads without hard Terminus dependency.
- [x] Ontology write endpoints (`POST/PUT/DELETE /database/{db_name}/ontology...`) are canonical event-sourcing APIs and are no longer marked deprecated.
- [x] Ontology write command processing now persists `object_type` resources via Postgres resource registry (`CREATE/UPDATE/DELETE_ONTOLOGY_CLASS`) with no Terminus write branch in worker runtime.
- [x] Legacy Terminus backfill scripts were removed from runtime repo paths after Postgres registry became canonical.
- [x] Resource CRUD write-path no longer hard-requires Terminus when `resource_storage_backend=postgres` (optional dependency + OCC fallback).
- [x] Default ontology resource backend switched to `postgres` in settings + env examples.
- [x] `action-worker` runtime is fixed to Postgres resource-registry mode; Terminus init path is removed from default execution.
- [x] `instance_async` command APIs no longer hard-require Terminus in postgres profile (ontology stamp falls back to `ref=branch:<name>` without commit).
- [x] Ontology health read path supports Postgres resource-only mode without Terminus (schema checks are skipped when legacy adapter is absent).
- [x] Ontology extensions proposal/deploy/health routes no longer inject optional Terminus dependencies; health gates run on resource-registry validation only.
- [x] Proposal approve/deploy legacy branch-head verification is removed from runtime (no Terminus branch-head lookup path).
- [x] `ontology-worker` runtime is fixed to Postgres mode; startup no longer initializes Terminus adapters.
- [x] Legacy branch/version surfaces (`/branch`, `/version`, and ontology extension `/branches`) are removed from runtime paths.
- [x] OMS `/database/list|create|exists` is migrated to Postgres-backed registry semantics (no Terminus guard/profile gate).
- [x] OMS `ensure_database_exists` dependency now validates exclusively against Postgres access registry (no Terminus list-databases fallback).
- [x] OMS dependency provider no longer exposes Terminus DI accessors (`get_terminus_service`, `get_optional_terminus_service`).
- [x] OMS app runtime no longer initializes or health-checks Terminus services; runtime backend is fixed to Postgres.
- [x] Proposal service (`PullRequestService`) no longer depends on Terminus base/version-control classes; proposal create/approve/merge is Postgres metadata-driven.
- [x] `OntologyResourceService` runtime path is fixed to Postgres canonical backend (legacy terminus/hybrid runtime branches disabled).
- [x] Ontology resource registry now persists explicit `version` on current records and immutable history in `ontology_resource_versions`.
- [x] Ontology resource validator reference checks no longer fallback to Terminus ontology reads; validation is based on resource registry lookups.
- [x] OMS ontology write paths removed no-op legacy relationship-validation gate (`_validate_relationships_gate`); write validation now relies on registry/lint/interface checks only.
- [x] OMS app no longer mounts legacy `/branch` and `/version` routers in any profile (legacy surfaces removed from runtime OpenAPI).
- [x] OMS database routes (`/database/list|create|exists`) are always exposed with Foundry-style Postgres behavior; legacy branch/version routes remain removed from OpenAPI.
- [x] BFF legacy branch/version wrappers (`/databases/{db}/branches*`, `/databases/{db}/versions`) are code-deleted from runtime handlers and OpenAPI.
- [x] BFF ontology legacy branch endpoints (`/databases/{db}/ontology/branches`) are code-deleted from runtime handlers and OpenAPI.
- [x] BFF merge conflict legacy endpoints (`/databases/{db}/merge/simulate`, `/databases/{db}/merge/resolve`) are code-deleted from runtime handlers and OpenAPI.
- [x] BFF summary endpoint no longer calls legacy OMS branch-info API in any profile (`branch_info` is intentionally null).
- [x] BFF summary ontology backend is now fixed to Foundry-style `postgres` (no profile-driven hybrid/terminus branching surface).
- [x] BFF `FoundryQueryService` legacy branch/version/merge helpers are removed.
- [x] BFF ontology OCC guard no longer depends on legacy branch head-commit APIs in any profile (optional OCC token only).
- [x] BFF `OMSClient` legacy branch/version helper methods are removed to prevent `/branch` or `/version` usage from new code paths.
- [x] BFF pipeline build/promote no longer gates on legacy ontology head-commit APIs (`/version/head`); build metadata uses Foundry-style branch refs.
- [x] Worker runtimes (`objectify-worker`, `instance-worker`) no longer call legacy OMS `/version/{db}/head`; ontology stamps are generated from branch refs (`branch:<name>`).
- [x] BFF pipeline ontology mismatch error payload no longer uses Terminus-specific field names (`bundle_terminus_commit_id`); generic ontology version/ref keys are used.
- [x] Shared ontology version resolver no longer performs legacy branch/version lookups (`version_control_service` / `get_branch_info`); Foundry-style branch ref is canonical.
- [x] Action target runtime contract resolution is resource-registry only; Terminus ontology fallback is removed.
- [x] `action_async` simulate `use_branch_head` mode no longer depends on Terminus branch-head APIs; branch refs (`branch:<name>`) are used as ontology commit tokens.
- [x] Pipeline proposal bundle now normalizes ontology metadata to include `ontology.ref` with Foundry-style branch fallback (`branch:<name>`), and falls back `ontology.commit` to ref when commit is absent.
- [x] BFF summary payload removed Terminus-specific key naming (`data.terminus` -> `data.branch_info`) and branch-info is now permanently disabled.
- [x] Ontology worker lineage/audit write metadata migrated from Terminus naming to Foundry-neutral graph naming (`artifact:graph:*`, `event_wrote_graph_document`, `graph_schema`, `ONTOLOGY_GRAPH_*`).
- [x] Shared relationship extraction utility no longer uses Terminus-specific schema naming (`_extract_from_graph_schema` only; legacy alias removed).
- [x] Graph query provenance and lineage remediation are graph-only (`artifact:graph:*`, `event_wrote_graph_document`); Terminus lineage alias keys are removed.
- [x] AI graph query grounding no longer reads legacy `provenance.terminus`; graph provenance is canonical.
- [x] ES lineage naming is canonicalized to Foundry-neutral forms (`artifact:es:*`, `event_materialized_es_document`) across writers and storage; runtime rejects legacy aliases and one-time DB rewrite is handled by migration script (`backend/scripts/migrations/migrate_lineage_es_aliases_to_canonical.py`).
- [x] Lineage read APIs support branch-scoped and as-of traversal (`/api/v1/lineage/graph`, `/api/v1/lineage/impact` with `branch`, `as_of`), and lineage storage persists `branch` as a first-class column for nodes/edges.
- [x] Lineage writers for instance/projection/objectify/pipeline promote paths now persist `branch` metadata/column to improve branch-scoped impact analysis fidelity.
- [x] Lineage navigation now includes Foundry-style operational analysis endpoints: shortest-path tracing (`/api/v1/lineage/path`) and as-of graph diff (`/api/v1/lineage/diff`) for change impact investigation.
- [x] Lineage timeline endpoint (`/api/v1/lineage/timeline`) provides build-window trend buckets, spike detection, and top edge/projection/service contributors for ops triage.
- [x] Lineage out-of-date diagnostics endpoint (`/api/v1/lineage/out-of-date`) classifies stale artifacts/projections against freshness SLO and returns remediation hints.
- [x] Lineage out-of-date diagnostics now includes latest writer cause context (`event_id`, `run_id`, producer metadata) for stale artifacts/projections to support root-cause triage.
- [x] Lineage out-of-date diagnostics now includes upstream freshness drift signals (`upstream_latest_event_at`, `upstream_gap_minutes`, `staleness_reason`) to identify whether stale outputs are lagging behind newer upstream events.
- [x] Lineage out-of-date diagnostics now distinguishes stale scope (`out_of_date_scope`: `parent|ancestor|none`) using aggregate-emitted lineage edges (direct parent drift vs upstream ancestor drift), with scope counts for triage.
- [x] Lineage out-of-date diagnostics now separates stale reason by lineage scope (`parent_has_newer_events`, `ancestor_has_newer_events`) and provides update type hints (`update_type`: `data|logic|none`) using projection writer code-sha drift.
- [x] Lineage out-of-date diagnostics now evaluates full stale sets with batched cause/scope lookup (not preview-only), then applies preview truncation only to response payload rows.
- [x] Lineage out-of-date diagnostics computation has been moved to a dedicated service module (`bff/services/lineage_out_of_date_service.py`) so router endpoints remain thin.
- [x] Lineage column-level diagnostics endpoint is available (`/api/v1/lineage/column-lineage`) and resolves mapping-spec references from lineage edge metadata.
- [x] Objectify lineage writers now stamp Foundry-style column-lineage metadata references (`column_lineage_ref`, storage/schema markers) on header and instance edges.
- [x] Linked-object and outgoing-link-type v2 pagination is filter-first (apply type/link filtering before page window) with dedicated regression tests.
- [x] Lineage run-impact endpoint (`/api/v1/lineage/run-impact`) supports run/build-scoped blast-radius analysis from lineage edges.
- [x] Lineage run-impact and runs impact previews now attach latest writer cause context per impacted artifact for faster triage.
- [x] Lineage run-impact and runs impact previews now classify impacted artifacts by latest-writer state (`current|superseded|unknown`) relative to the selected run.
- [x] Lineage runs endpoint (`/api/v1/lineage/runs`) provides run/build timeline summaries with per-run impacted artifact/projection counts and optional impact previews.
- [x] Lineage backfill queue runner supports branch-scoped claims (`backend/scripts/backfill_lineage.py --branch`) for branch-isolated recovery operations.
- [x] Instance-worker S3 lineage edge type is canonicalized to `event_stored_in_object_store` (legacy `event_wrote_s3_object` writer usage removed), and one-time DB rewrite is available via `backend/scripts/migrations/migrate_lineage_s3_edge_alias_to_canonical.py`.
- [x] Runtime lineage edge type literals are centralized in `shared/models/lineage_edge_types.py` to eliminate duplicated string implementations across workers/services.
- [x] Type-inference mapping request default target is Foundry (`target_system=foundry`), not legacy Terminus.
- [x] Tabular type inference/sheet structure analysis runtime is internal-only (in-process ASGI); external Funnel HTTP transport mode is removed from default runtime configuration.
- [x] Default gateway/proxy, compose topology, and deploy/test bootstrap scripts no longer expose or start external Funnel HTTP surfaces (`/api/funnel/*`, `/health/funnel`, separate `funnel` service).
- [x] Foundry API v2 overview/index has no Funnel/type-inference public resource group; tabular inference remains private runtime behavior only.
- [x] Error taxonomy no longer emits Terminus-specific runtime codes (`TERMINUS_CONFLICT`, `TERMINUS_UNAVAILABLE`); upstream failures are classified with generic Foundry-neutral codes.
- [x] Legacy Terminus service modules (`oms/services/async_terminus.py`, `oms/services/terminus/*`) and branch-name adapter helpers were removed from backend runtime.
- [x] Terminus-specific MCP server/config (`backend/mcp_servers/terminus_mcp_server.py`, `mcp-config.json` `terminusdb`) was removed.
- [x] `DatabaseSettings` no longer exposes `terminus_*` runtime configuration fields; Foundry/Postgres runtime has no Terminus config surface.

Phase 3: Foundry-style write path hardening
- [x] Ensure edit queue semantics and checkpoint contracts are documented and testable.
- [x] Add replay/rebuild runbook from durable source to ES read model.
- [x] Add consistency checks between durable event source and ES projection.
- [x] Projection consistency helper module and unit tests are in place (`shared/services/core/projection_consistency.py`, `tests/unit/services/test_projection_consistency.py`).
- [x] Projection consistency operational script is available (`backend/scripts/verify_projection_consistency.py`) and documented in `docs/PROJECTION_CONSISTENCY_RUNBOOK.md`.

Phase 4: Topology cleanup
- [x] Remove `terminusdb` from default local compose dependencies (enabled only with `legacy-terminus` profile).
- [x] OMS service compose dependency on `terminusdb` removed; Postgres-first mode enabled by default in full-stack compose.
- [x] `instance-worker` compose dependency/env no longer references `terminusdb` (worker is ES/OMS-driven).
- [x] `ontology-worker` compose dependency/env no longer references `terminusdb` in default Postgres profile.
- [x] Remove Terminus-specific settings/env vars from required startup path (Postgres profile blocks Terminus DI creation; action-worker default env no longer injects `TERMINUS_*`).
- [x] OMS compose runtime no longer injects `TERMINUS_*` env vars in default Postgres-first startup path.
- [x] Mark Terminus integration as optional legacy adapter only (runtime + compose profile gating).
- [x] OMS image startup checks no longer treat `terminusdb-client` as a critical always-on dependency.
- [x] Shared monitoring/config drift endpoints no longer expose TerminusDB runtime config as a required/primary service surface.
- [x] Shared health-check module no longer carries unused `TerminusDBHealthCheck` runtime class.
- [x] Shared config facade/service URL maps no longer expose Terminus as a first-class runtime service (`Config` / `ServiceConfig`).
- [x] Ontology backend setting normalization is fixed to `postgres` (legacy `terminus|hybrid` values are downgraded and runtime no longer branches on backend profile).
- [x] Environment templates (`.env.example`, `backend/.env.example`) now document Postgres-only ontology backend in the default Foundry runtime path.
- [x] Production test harness (`backend/run_production_tests.sh`) no longer executes legacy branch/version runtime suites by profile; legacy suites are explicitly skipped in Foundry/Postgres runtime.
- [x] Legacy branch virtualization E2E suite (`tests/test_branch_virtualization_e2e.py`) is explicitly skipped because legacy branch virtualization runtime is removed from the default Foundry path.
- [x] Default compose topology removed the `terminusdb` service/volume entirely (`backend/docker-compose.yml`, `docker-compose.full.yml`, `backend/docker-compose-https.yml`).
- [x] Nginx proxy configs no longer expose `/terminusdb/` routes (`backend/nginx.conf`, `backend/nginx-dev.conf`).
- [x] Legacy Terminus volume backup/restore ops scripts were removed (`scripts/ops/backup_terminusdb_volume.sh`, `scripts/ops/restore_terminusdb_volume.sh`), and generated architecture/file inventory docs were refreshed.

Definition of done:
- [x] No production-critical path requires TerminusDB at runtime in Postgres profile (legacy DB/branch/version paths remain profile-gated compatibility surfaces).
- [x] Ontology and object read APIs match Foundry v2 contracts where implemented.
- [x] Foundry-surface deprecated compatibility endpoints are code-deleted from runtime and OpenAPI.
- [x] Replay and projection consistency checks are automated in CI.

P0 strict-compat hardening (2026-02-16):
- [x] Strict compat is fixed-on runtime behavior (legacy opt-out gates removed).
- [x] Objectify execution mode precedence is fixed end-to-end:
  - enqueue default `full`
  - worker resolution `options.execution_mode -> job.execution_mode -> "full"`
- [x] v2 strict response normalization is active by default:
  - `GET /api/v2/ontologies/{ontology}/fullMetadata` branch shape is fixed to `{"rid": ...}`
  - object/link required fields are synthesized
  - unresolved outgoing link type entries are dropped in strict list and mapped to `404 LinkTypeNotFound` in strict get
  - preview endpoints (`fullMetadata`, `interfaceTypes*`, `sharedPropertyTypes*`, `valueTypes*`, `objectTypes/*/fullMetadata`) require `preview=true` and return `400 INVALID_ARGUMENT` + `ApiFeaturePreviewUsageOnly` when omitted
- [x] OCC expected-head validation now uses deployed commit + branch tokens:
  - blank token -> `400 INVALID_ARGUMENT`, mismatch -> `409 CONFLICT`
- [x] E2E/contract test helpers no longer depend on legacy OMS `/api/v1/version/{db_name}/head` calls for OCC tokens; `branch:<name>` tokens are used.
- [x] OMS smoke no longer calls legacy `/api/v1/version/{db_name}/history|diff`; legacy version routes are asserted absent via OpenAPI.
- [x] Core Foundry migration E2E suites no longer call legacy OMS `/api/v1/branch/*` APIs; branch-specific tests were normalized to `main` in Foundry/Postgres runtime.
- [x] OpenAPI contract smoke protected-branch fallback no longer uses legacy branch-management routes (`/api/v1/databases/{db_name}/ontology/branches`, `/api/v1/databases/{db_name}/branches*`).
- [x] Observability added:
  - v2 strict normalization summary logs (`route`, `db`, `branch`, `fixes`, `dropped`)
  - OCC mismatch logs (`db`, `branch`, `expected`, `allowed_count`)

Validation commands (latest run: 2026-02-16):
- `cd backend && pytest -q tests/unit/workers/test_objectify_incremental_default.py tests/unit/openapi/test_foundry_ontology_v2_contract.py tests/unit/oms/test_ontology_extensions_occ.py`
- `cd backend && pytest -q tests/unit/openapi/test_removed_v1_compat_guard.py` (full BFF OpenAPI 기준 제거된 v1 op 재노출 + Foundry v2 파라미터 계약 가드)
- `cd backend && pytest -q tests/unit/openapi/test_openapi_smoke_recipe_guard.py` (OpenAPI smoke recipe 누락/preview 파라미터 타입 회귀 방지)
- `cd backend && pytest -q tests/unit/openapi/test_openapi_command_status_parser.py` (command status envelope 파싱 회귀 방지)
- `cd backend && pytest -q tests/test_openapi_contract_smoke.py`
- `cd backend && OPENAPI_STRICT_REMOVED_V1_OPS=true pytest -q tests/test_openapi_contract_smoke.py` (legacy removed-operation 재노출을 실패로 강제)

Recommended rollout:
1. Verify v2 consumers in staging/prod with strict contracts and monitor strict normalization/OCC mismatch logs.
2. Maintain `/api/v2/ontologies/*` contract tests as required CI gates.
3. Reject any reintroduction of legacy v1 compat/strict-opt-out paths in code review and static guards.
