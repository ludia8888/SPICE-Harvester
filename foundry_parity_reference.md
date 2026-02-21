# Foundry Parity Reference (official-doc based)

Last verified: 2026-02-21

## Official references
- Palantir Foundry docs homepage: https://www.palantir.com/docs/foundry/
- Foundry overview (compounding/dynamic core): https://www.palantir.com/docs/foundry/getting-started/overview
- Ontology overview (semantic + kinetic elements): https://www.palantir.com/docs/foundry/ontology/overview/
- Ontology architecture (data/logic/action/security integration): https://www.palantir.com/docs/foundry/ontology/architecture-center/ontology-architecture/
- Actions overview: https://www.palantir.com/docs/foundry/ontology-sdk/actions/overview
- Action type lifecycle (versioning): https://www.palantir.com/docs/foundry/ontology-sdk/actions/create-action-type-version
- Ontology objects overview: https://www.palantir.com/docs/foundry/object/overview
- Pipeline Builder overview: https://www.palantir.com/docs/foundry/pipeline-builder/overview
- Data connections core concepts: https://www.palantir.com/docs/foundry/data-connection/core-concepts/
- Set up syncs: https://www.palantir.com/docs/foundry/data-connection/set-up-sync/
- Set up sources: https://www.palantir.com/docs/foundry/data-connection/set-up-source/
- Export overview: https://www.palantir.com/docs/foundry/data-connection/export-overview/
- Set up export policy: https://www.palantir.com/docs/foundry/data-connection/set-up-export-policy/
- Export to file: https://www.palantir.com/docs/foundry/data-connection/export-to-file/
- Custom JDBC sources: https://www.palantir.com/docs/foundry/data-connection/custom-jdbc-sources/
- Data connection agent worker: https://www.palantir.com/docs/foundry/data-connection/data-connection-agent-worker/

## Layer parity map (Foundry terms)
1. Ontology layer (semantic model) -> `Improved`.
- Product coverage: object types, link types, value/shared/interface types, object search/object sets.
- Public surface: `/api/v2/ontologies/{ontology}/objectTypes*`, `/objects/*`, `/objectSets/*`.

2. Kinetic layer (actions + business logic/query functions) -> `Improved`.
- Product coverage: action types/apply/applyBatch + query types(list/get/execute) with FE/BFF contract.
- Public surface: `/api/v2/ontologies/{ontology}/actionTypes*`, `/actions/{action}/apply*`, `/queryTypes*`, `/queries/{queryApiName}/execute`.

3. Dynamic layer (closed-loop + dynamic security) -> `Improved`.
- Product coverage: action side effects, audit chain, lineage diagnostics, role-scoped API checks, project-level policy inheritance contract.
- Public surface: `/api/v1/access-policies`, `/api/v2/ontologies/{ontology}/actionTypes*`, `/api/v2/ontologies/{ontology}/actions/{action}/apply`.
- Remaining gap: multi-level (org/project/object) inheritance conflict resolution is not yet exposed as a separate policy simulation surface.

## Product gates aligned to references
1. Objectify run is version-pinned.
- Gate: `POST /api/v1/objectify/datasets/{datasetId}/run` requires `dataset_version_id` unless artifact mode is used.
- Why: deterministic lineage/object materialization on a specific dataset version.

2. Raw ingest response exposes version evidence.
- Gate: `GET /api/v1/pipelines/datasets/{datasetId}/raw-file` includes `version_id`, `lakefs_commit_id`, `version_created_at`.
- Why: UI/BFF-only flow can carry the same immutable version into objectify/action/lineage checks.

3. Action response evidence is standardized.
- Gate: action apply returns `auditLogId`, `sideEffectDelivery`, `writebackStatus`.
- Why: action execution and external side effects can be closed-loop verified with audit evidence.

4. Legacy contract re-entry is blocked at FE runtime.
- Gate: FE API client blocks removed endpoint families (`merge`, Google-Sheets legacy suggest/import).
- Why: removed APIs cannot silently re-enter UI flows.

5. E2E API allowlist is hard-enforced.
- Gate: Playwright QA fails on non-allowlisted API calls from UI/browser and test API client.
- Why: prevents backend-internal or undocumented path usage during QA.

6. Kinetic query type contract is E2E-verified.
- Gate: frontend QA creates ontology `functions` resource, then verifies `queryTypes` list/get and `queries/{queryApiName}/execute`.
- Why: ensures semantic ontology resources are executable business logic through v2 kinetic surface.

7. Dynamic security inheritance contract is E2E-verified.
- Gate: frontend QA upserts project-scope access policy, lists it through BFF, and verifies action type exposes `permissionModel` + `projectPolicyInheritance`.
- Why: closes FE/BFF contract gap for action permission inheritance and action-only edit guardrails.

## Connector parity snapshot (official-doc baseline, 2026-02-20)
1. Source/connection CRUD + runtime test: `Implemented`.
- Evidence: `/api/v2/connectivity/connections` CRUD, `/test`, `/getConfiguration`, `/getConfigurationBatch`.
- Code: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/bff/routers/foundry_connectivity_v2.py`.

2. Supported connector families: `Partial`.
- Foundry docs expose broad source catalog (batch/streaming/CDC/file-export capability matrix).
- Product currently implements 6 kinds in adapter factory: `google_sheets`, `postgresql`, `mysql`, `oracle`, `sqlserver`, `snowflake`.
- Code: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/data_connector/adapters/factory.py`.

3. Sync modes (snapshot/incremental/cdc/streaming/file): `Improved`.
- Implemented modes: `SNAPSHOT`, `APPEND`, `UPDATE`, `INCREMENTAL`, `CDC`, `STREAMING`.
- `STREAMING` uses the CDC extraction/runtime path and is feature-gated with the same CDC controls.
- Code: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/data_connector/adapters/import_config_validators.py`.

4. CDC support: `Implemented (scope-limited)`.
- Implemented per-adapter CDC extraction + token handling (`lsn/binlog/scn/change_tracking` paths).
- Scope-limited to currently supported 6 connectors.
- Code: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/data_connector/postgresql/service.py`, `/Users/isihyeon/Desktop/SPICE-Harvester/backend/data_connector/mysql/service.py`, `/Users/isihyeon/Desktop/SPICE-Harvester/backend/data_connector/oracle/service.py`, `/Users/isihyeon/Desktop/SPICE-Harvester/backend/data_connector/sqlserver/service.py`, `/Users/isihyeon/Desktop/SPICE-Harvester/backend/data_connector/snowflake/service.py`.

5. Export capability: `Improved`.
- Implemented: connection-level export settings API (`updateExportSettings`) + async export run APIs (`POST/GET /exportRuns`).
- Guardrail: non-dry-run export runs require connection `markings` unless `exportEnabledWithoutMarkingsValidation=true`.
- Remaining parity gap: typed destination presets and broader export target catalog.
- Code: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/bff/routers/foundry_connectivity_v2.py`.

6. Security guardrails: `Implemented`.
- AES-GCM connector secret encryption + production plaintext guard.
- SQL single-statement guard + identifier sanitization + blocking query timeout.
- Code: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/shared/services/registries/connector_registry.py`, `/Users/isihyeon/Desktop/SPICE-Harvester/backend/data_connector/adapters/sql_query_guard.py`, `/Users/isihyeon/Desktop/SPICE-Harvester/backend/data_connector/adapters/blocking_query.py`.

7. Runtime topology parity (Agent worker/direct modes): `Partial`.
- Foundry docs describe agent-worker/direct-connection operational model and direct-mode sunset guidance.
- Product runtime currently operates with internal trigger/sync workers and direct adapter access, without Foundry-equivalent agent-worker enrollment/control-plane APIs.
- Code: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/connector_trigger_service/main.py`, `/Users/isihyeon/Desktop/SPICE-Harvester/backend/connector_sync_worker/main.py`.

8. FE/BFF public surface parity for connectivity QA: `Improved`.
- Added FE API wrappers for `/api/v2/connectivity/*` and added E2E allowlist prefix.
- Code: `/Users/isihyeon/Desktop/SPICE-Harvester/frontend/src/api/bff.ts`, `/Users/isihyeon/Desktop/SPICE-Harvester/frontend/tests/e2e/foundryApiAllowlist.ts`, `/Users/isihyeon/Desktop/SPICE-Harvester/frontend/tests/e2e/foundryApiContract.ts`.
