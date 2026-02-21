# Foundry Connector Parity Report (2026-02-21 refresh)

## Scope
- Baseline: Palantir Foundry official data-connection docs.
- Product under test: `/Users/isihyeon/Desktop/SPICE-Harvester`.
- Constraint: UI/BFF public API surface only (no backend-internal direct calls for QA contract).

## Official baseline references
- Core concepts: https://www.palantir.com/docs/foundry/data-connection/core-concepts/
- Set up source: https://www.palantir.com/docs/foundry/data-connection/set-up-source/
- Set up sync: https://www.palantir.com/docs/foundry/data-connection/set-up-sync/
- Export overview: https://www.palantir.com/docs/foundry/data-connection/export-overview/
- Set up export policy: https://www.palantir.com/docs/foundry/data-connection/set-up-export-policy/
- Export to file: https://www.palantir.com/docs/foundry/data-connection/export-to-file/
- Custom JDBC sources: https://www.palantir.com/docs/foundry/data-connection/custom-jdbc-sources/
- Data connection agent worker: https://www.palantir.com/docs/foundry/data-connection/data-connection-agent-worker/

## Evidence used from product
- Connectivity router: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/bff/routers/foundry_connectivity_v2.py`
- Connector adapters: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/data_connector/adapters/factory.py`
- JDBC/CDC implementations: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/data_connector/postgresql/service.py`, `/Users/isihyeon/Desktop/SPICE-Harvester/backend/data_connector/mysql/service.py`, `/Users/isihyeon/Desktop/SPICE-Harvester/backend/data_connector/oracle/service.py`, `/Users/isihyeon/Desktop/SPICE-Harvester/backend/data_connector/sqlserver/service.py`, `/Users/isihyeon/Desktop/SPICE-Harvester/backend/data_connector/snowflake/service.py`
- Secret encryption: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/shared/services/registries/connector_registry.py`
- SQL guard + timeout: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/data_connector/adapters/sql_query_guard.py`, `/Users/isihyeon/Desktop/SPICE-Harvester/backend/data_connector/adapters/blocking_query.py`
- Trigger/sync workers: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/connector_trigger_service/main.py`, `/Users/isihyeon/Desktop/SPICE-Harvester/backend/connector_sync_worker/main.py`
- FE contract/allowlist: `/Users/isihyeon/Desktop/SPICE-Harvester/frontend/src/api/bff.ts`, `/Users/isihyeon/Desktop/SPICE-Harvester/frontend/tests/e2e/foundryApiAllowlist.ts`, `/Users/isihyeon/Desktop/SPICE-Harvester/frontend/tests/e2e/foundryApiContract.ts`

## Parity matrix

1. Connection lifecycle (create/list/get/delete/test/config/secrets)
- Foundry baseline: supported.
- Product: `PASS`.
- Evidence: `/api/v2/connectivity/connections*`, `/test`, `/getConfiguration`, `/getConfigurationBatch`, `/updateSecrets`.

2. Import resource model (table/file/virtual table CRUD + execute)
- Foundry baseline: supported.
- Product: `PASS`.
- Evidence: `/tableImports`, `/fileImports`, `/virtualTables`, and `.../execute` endpoints.

3. Sync modes
- Foundry baseline: batch/streaming/CDC/file export model.
- Product: `IMPROVED`.
- Implemented: `SNAPSHOT`, `APPEND`, `UPDATE`, `INCREMENTAL`, `CDC`, `STREAMING`.
- Note: `STREAMING` currently executes through CDC extraction/runtime and follows the same feature gate.

4. CDC capability
- Foundry baseline: connector-dependent CDC support.
- Product: `PASS (current connector scope)`.
- Evidence: LSN/binlog/SCN/change-tracking token logic in adapters + CDC flag allowlist gating.

5. Export capability
- Foundry baseline: file/table export flows.
- Product: `IMPROVED`.
- Implemented: `updateExportSettings` + async export-run APIs (`POST/GET /exportRuns`) with task/audit linkage.
- Guardrail added: non-dry-run exports require connection `markings` unless `exportEnabledWithoutMarkingsValidation=true`.
- Remaining gap: typed table/file export presets and richer delivery targets are still limited vs Foundry catalog breadth.

6. Runtime topology (agent worker/direct mode operational parity)
- Foundry baseline: agent-worker and direct-connection operational model documented.
- Product: `PARTIAL`.
- Implemented: internal trigger/sync workers and adapter polling.
- Gap: no BFF/UI-facing agent enrollment/lifecycle control plane.

7. Security guardrails
- Foundry baseline: secure credential/runtime handling.
- Product: `PASS`.
- Evidence: AES-GCM secrets with prod plaintext guard, SQL single-statement guard, identifier sanitization, query timeout.

8. FE public contract parity (UI-callable API)
- Foundry baseline: data-connection operations are UI-operable.
- Product before this pass: `FAIL` (connectivity routes not in FE allowlist/contract).
- Product after this pass: `PASS (surface level)`.
- Evidence: FE wrappers added for `/api/v2/connectivity/*`, allowlist includes `/api/v2/connectivity`.

9. Legacy removal in connector surface
- Foundry baseline: avoid stale APIs in production UX.
- Product: `PASS (ongoing)`.
- Evidence: legacy patterns blocked in FE runtime guard; connectivity wrappers now standardized under v2.

## QA commands executed in this pass
- `pytest -q backend/tests/unit/services/test_connector_ingest_service_core.py` -> `7 passed`.
- `pytest -q backend/tests/unit/workers/test_connector_sync_worker.py` -> `8 passed`.
- `pytest -q backend/tests/unit/openapi/test_foundry_platform_v2_contract.py -k "connectivity and (table_import or file_import or virtual_table)"` -> `3 passed`.
- `pytest -q backend/tests/unit/openapi/test_foundry_platform_v2_contract.py -k "connectivity"` -> `3 passed`.
- `pnpm -C frontend exec tsc --noEmit` -> `passed`.
- `pnpm -C frontend exec playwright test tests/e2e/live_foundry_qa.spec.ts --project=chromium` -> `failed` (raw version chain / transform / objectify contract bugs logged).

## New gaps recorded
- `frontend-qa-connector-parity-export-exec` (P1)
- `frontend-qa-connector-parity-agent-runtime` (P2)
- File: `/Users/isihyeon/Desktop/SPICE-Harvester/qa_bugs.json`

## Immediate migration priorities
1. P2: Introduce agent-runtime control plane APIs (register, heartbeat, capability report, policy).
2. P1: Fix raw version chain regression surfaced by live E2E (`raw-file`, `readTable`, transform/objectify prerequisites).
3. P2: Expand export target presets (typed destination schemas / retry-policy templates) to reduce custom webhook-only operation.
