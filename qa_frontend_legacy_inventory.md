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
  - removed `frontend/src/components/SidebarRail.tsx` (legacy variant)
- Active pages cleaned:
  - `frontend/src/pages/OntologyPage.tsx` legacy `OntologyManagerPage` block removed
  - `frontend/src/pages/LineagePage.tsx` legacy `LineageExplorerPage` block removed

## State duplication status
- Primary store: `frontend/src/store/useAppStore.ts`
- Legacy store usage in active route files: none

## API contract status (`frontend/src/api/bff.ts`)

### Active Foundry QA path (frontend-exposed)
- Ingest/transform/objectify/graph/action/closed-loop paths are exercised through:
  - `/api/v1/pipelines/*`
  - `/api/v1/objectify/*`
  - `/api/v1/graph-query/*`
  - `/api/v1/admin/recompute-projection`
  - `/api/v1/audit/logs`
  - `/api/v2/ontologies/*`

### Remaining cleanup candidate (unused legacy types)
- `ActionType`, `ActionTypeDetail`, `ActionLog`, `LinkType`, `DetectedRelationships` 계열 타입은 현재 mock/legacy typing 호환 목적만 남아 있음.
- API 함수 기준으로는 `/api/v1/ontology/*` 레거시 wrapper 제거 완료.

## Route-level legacy handling
- `/db/:db/data/*` -> `frontend/src/pages/LegacyDataToolsRemovedPage.tsx` 유지
- 목적: 제거된 v1 data-tools 경로 접근을 명시적으로 차단
