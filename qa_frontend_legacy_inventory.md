# Frontend Legacy / Duplicate Inventory (2026-02-20, refreshed)

## Runtime entrypoint (active)
- `frontend/src/main.tsx` -> `frontend/src/AppShell.tsx` (active shell)
- `frontend/src/AppShell.tsx` -> `frontend/src/app/AppRouter.tsx`

## Removed legacy surfaces (completed)
- App shells and duplicate state:
  - removed `frontend/src/App.tsx`
  - removed `frontend/src/app/AppShell.tsx`
  - removed `frontend/src/state/store.ts`
- Legacy-only pages/components not reachable from active router:
  - removed `frontend/src/pages/HomePage.tsx`
  - removed `frontend/src/pages/DatasetsPage.tsx`
  - removed `frontend/src/pages/ConnectorsPage.tsx`
  - removed `frontend/src/pages/GraphPage.tsx`
  - removed `frontend/src/pages/ExplorerPage.tsx`
  - removed `frontend/src/pages/WorkshopPage.tsx`
  - removed `frontend/src/pages/PlaceholderPage.tsx`
  - removed `frontend/src/pages/AIAgentPage.tsx`
  - removed `frontend/src/pages/ImportSheetsPage.tsx`
  - removed `frontend/src/pages/ImportExcelPage.tsx`
  - removed `frontend/src/pages/SchemaSuggestionPage.tsx`
  - removed `frontend/src/pages/SheetsHubPage.tsx`
  - removed `frontend/src/components/explorer/*`
  - removed `frontend/src/components/pipeline/*`
  - removed `frontend/src/components/ontology/*` (legacy widget set)
  - removed `frontend/src/components/lineage/*` (legacy widget set)
  - removed `frontend/src/components/UploadFilesDialog.tsx`
  - removed `frontend/src/components/SidebarRail.tsx` (legacy variant)
- Legacy tests and runner drift:
  - removed `frontend/tests/pages/DatasetsPage.test.tsx` (targeted deleted page)
  - migrated `frontend/tests/pages/GraphPage.import.test.ts` -> `frontend/tests/pages/GraphExplorerPage.import.test.ts`
  - migrated test helpers and store tests from `frontend/src/state/store` to `frontend/src/store/useAppStore.ts`
  - migrated `frontend/tests/components/SidebarRail.test.tsx` to `frontend/src/components/layout/SidebarRail.tsx`
  - vitest now excludes Playwright specs (`frontend/vite.config.ts` uses `.test.*` include + `tests/e2e/**` exclude)
- Active pages cleaned:
  - `frontend/src/pages/OntologyPage.tsx` migrated to Foundry v2 object type contract flow
  - `frontend/src/pages/LineagePage.tsx` legacy `LineageExplorerPage` block removed
  - removed `frontend/src/pages/BranchesPage.tsx` (no branch-management route/UI surface)
  - removed `frontend/src/pages/LegacyDataToolsRemovedPage.tsx` (legacy tombstone page)
  - removed `frontend/src/pages/MergePage.tsx` and `/db/:db/merge` route
- Removed mock-only API surface:
  - removed `frontend/src/api/mockData.ts`
  - removed mock-mode loader branch from `frontend/src/api/bff.ts`

## State duplication status
- Primary store: `frontend/src/store/useAppStore.ts`
- Legacy store usage in active route files: none

## API contract status (`frontend/src/api/bff.ts`)

### Active Foundry QA path (frontend-exposed)
- Ingest/transform/objectify/graph/action/closed-loop paths are exercised through:
  - `/api/v2/datasets/*` (dataset lifecycle ingest)
  - `/api/v1/pipelines/*`
  - `/api/v1/objectify/*`
  - `/api/v1/graph-query/*`
  - `/api/v1/admin/recompute-projection`
  - `/api/v1/audit/logs`
  - `/api/v2/ontologies/*`

### Legacy API wrapper removal (completed)
- Removed from `frontend/src/api/bff.ts`:
  - `simulateMerge()` -> `POST /api/v1/databases/{db}/merge/simulate`
  - `resolveMerge()` -> `POST /api/v1/databases/{db}/merge/resolve`
  - legacy Google Sheets suggestion/import wrappers:
    - `suggestMappingsFromGoogleSheets()`
    - `dryRunImportFromGoogleSheets()`
    - `commitImportFromGoogleSheets()`
    - `suggestSchemaFromGoogleSheets()`
  - unused Google Sheets connector wrappers:
    - `listRegisteredGoogleSheets()`
    - `registerGoogleSheet()`
    - `previewRegisteredGoogleSheet()`
    - `startPipeliningGoogleSheet()`
    - `unregisterGoogleSheet()`
    - `previewGoogleSheet()`
    - `gridGoogleSheet()`
    - `registerGoogleSheetCtx()`
    - `listRegisteredSheets()`
    - `previewRegisteredSheet()`
    - `unregisterSheet()`
  - non-context legacy wrapper block removed (unused in active router):
    - datasets/pipelines upload helpers (`uploadDataset`, `approveDatasetSchema`, `listDatasets`, `listPipelines` ...)
    - pipeline proposal/build/deploy legacy wrappers
    - legacy AI intent/query wrappers (`aiIntent`, `aiQuery`)
    - legacy agent/pipeline preview wrappers (`runPipelineAgent*`, `previewPipelinePlan`)
    - legacy UDF wrappers (`listUdfs`, `createUdf`, `getUdf`, `createUdfVersion`, `getUdfVersion`)
  - removed ontology v1 write/schema wrappers:
    - `validateOntologyCreate()`
    - `createOntology()`
    - `validateOntologyUpdate()`
    - `updateOntology()`
    - `deleteOntology()`
    - `getOntologySchema()`
  - removed unsupported branch wrappers:
    - `listBranches()`
    - `createBranch()`
    - `deleteBranch()`

### Frontend parity gate (active)
- E2E allowlist guard enabled:
  - `frontend/tests/e2e/foundryApiAllowlist.ts`
  - `frontend/tests/e2e/foundryQaUtils.ts` (`apiCall` hard-fails on out-of-contract endpoint)
  - `frontend/tests/e2e/live_foundry_qa.spec.ts` (browser UI request allowlist violation captured as P0 bug)
  - `frontend/scripts/run_live_foundry_qa.sh` supports `E2E_REPEAT` for soak runs.
  - `frontend/package.json` includes `test:e2e:live:soak` (`E2E_REPEAT=3`) for flake detection.
- Strong data-integrity gate enabled in live E2E:
  - uploaded source CSV SHA-256/byte-length must match `GET /api/v1/pipelines/datasets/{datasetId}/raw-file` payload bytes.
  - v2 lifecycle dataset `readTable.totalRowCount` is asserted for deterministic ingest.
  - closed-loop `Order` search response now asserts primary-key uniqueness (duplicate PKs -> P1).
- Runtime legacy API block guard enabled in FE API client:
  - `frontend/src/api/bff.ts` blocks removed endpoint families before network call.
  - blocklist source is now centralized in `frontend/src/api/legacyEndpointBlocklist.ts` and reused by E2E allowlist.
  - includes `/api/v1/databases/{db}/branches*` runtime hard-block.
- Branch APIs are now removed from FE public contract:
  - branch management route (`/db/:db/branches`) is no longer exposed.
  - router now rejects `/db/:db/branches` and `/db/:db/data/*` as unsupported legacy paths.
- Objectify version pin gate enabled:
  - `POST /api/v1/objectify/datasets/{datasetId}/run` requires `dataset_version_id` unless artifact mode is used.
  - `GET /api/v1/pipelines/datasets/{datasetId}/raw-file` now returns `version_id` so FE can run version-pinned objectify.
  - `frontend/src/pages/OntologyPage.tsx` now hard-fails apply/validate early when backing/mapping metadata is set without `datasetVersionId` (prevents backend 409 contract failures).
- Explicitly blocked endpoint family:
  - `/api/v1/databases/{db}/branches*`
  - `/api/v1/databases/{db}/merge/*`
  - `/api/v1/databases/{db}/suggest-mappings-from-google-sheets`
  - `/api/v1/databases/{db}/import-from-google-sheets/*`
  - `/api/v1/databases/{db}/suggest-schema-from-google-sheets`
  - `/api/v1/pipelines/datasets/csv-upload`
  - `/api/v1/pipelines/datasets/excel-upload`
  - `/api/v1/pipelines/datasets/media-upload`
  - `/api/v1/pipelines/datasets/ingest-requests/{id}/schema/approve`
  - `/api/v1/agent/pipeline-runs*`
  - `/api/v1/pipeline-plans/{planId}/preview`
  - `/api/v1/pipelines/udfs*`
- Legacy automation drift removed:
  - `scripts/smoke_agent_progress.sh` no longer calls removed `/api/v1/agent/runs`; it now validates async instance flow via `/api/v1/databases/{db}/instances/{class}/bulk-create` + `/api/v1/commands/{id}/status`.
  - `scripts/smoke_agent_registry.sh` now validates current BFF agent surface through `/api/v1/agent/pipeline-runs` (contract smoke, non-404 gate).

### Remaining cleanup candidate
- API 함수 기준으로는 `/api/v1/ontology/*` write/schema 레거시 wrapper 제거 완료.
- `frontend/src/api/bff.ts` 내 미사용 레거시 타입(`ActionType`, `ActionTypeDetail`, `SimulationResult`, `ActionLog`, `LinkType`) 제거 완료.

## Route-level legacy handling
- removed explicit legacy pages and routes:
  - `/db/:db/data/*` (legacy data tools)
  - `/db/:db/branches` (unsupported branch management)
- fallback behavior:
  - legacy route hits now resolve to `NotFoundPage` (strict removal, no silent compatibility rewrite).
