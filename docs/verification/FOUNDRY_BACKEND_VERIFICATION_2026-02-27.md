---
orphan: true
---

# Foundry Backend Verification — 2026-02-27

Repo: `/Users/isihyeon/Desktop/SPICE-Harvester`  
Branch/Commit: `codex/foundry-strict-verification` @ `991cf821501dc68275a021ef96b5a015e91ee425`  
Generated at (local): `2026-02-28 16:57 KST`

## Goal
Validate that the backend MSA + infra stack actually runs end‑to‑end in a Foundry‑like lifecycle:

1) ingest → 2) pipeline build/deploy → 3) ontology → 4) objectify/materialize → 5) search + graph → 6) actions → 7) closed‑loop → 8) live cross‑domain.

“Foundry Strict” here means: the **implemented** `/api/v2` surface (and required supporting `/api/v1`) behaves consistently and deterministically; we do **not** claim to implement all of Foundry.

## Runtime topology (single source of truth)
- Compose file: `/Users/isihyeon/Desktop/SPICE-Harvester/docker-compose.full.yml`

## Reproducibility / safety defaults used
- LLM disabled for QA runs: `LLM_PROVIDER=disabled`, `LLM_API_KEY=` (no outbound dependency).
- Auth: used repo `.env` `ADMIN_TOKEN` via `X-Admin-Token` (token value redacted).

## Gate Results

### Gate A — Production Gate (PASS)
Command:
```bash
cd /Users/isihyeon/Desktop/SPICE-Harvester/backend
./run_production_tests.sh --full
```
Result: **PASS** (includes service health checks, ES cleanup, Kafka topic partition check, and full production test suite).

### Gate B — Foundry Backend E2E QA (PASS)
Pre‑run hygiene:
- `qa_bugs.json` archived/reset under `/Users/isihyeon/Desktop/SPICE-Harvester/qa_bugs_archive/`

Command:
```bash
PYTHONUNBUFFERED=1 \
RUN_FOUNDRY_E2E_QA=true QA_FAIL_SEVERITY=P1 \
PYTHONPATH=/Users/isihyeon/Desktop/SPICE-Harvester/backend \
python -m pytest /Users/isihyeon/Desktop/SPICE-Harvester/backend/tests/test_foundry_e2e_qa.py -v -s --tb=short
```
Result: **PASS** — `1 passed in 150.84s`, `0 bugs` written to `/Users/isihyeon/Desktop/SPICE-Harvester/qa_bugs.json` (`[]`).  
DB used by the run: `qa_7164870ac1`.

## 2026-02-28 Update — “Real Foundry User” Hardening (Gate B)

This round strengthens Gate B so **PASS ≈ real user workflow**, not just “endpoints return 200”.

### Gate A — Production Gate (PASS, after scope enforcement compatibility fixes)
Command:
```bash
cd /Users/isihyeon/Desktop/SPICE-Harvester/backend
./run_production_tests.sh --full
```
Result: **PASS** (latest run on `2026-02-28`).

Notable fixes required by enabling strict BFF project scope enforcement:
- OpenAPI contract smoke now supplies `X-DB-Name` for scope-enforced ops:
  - `/Users/isihyeon/Desktop/SPICE-Harvester/backend/tests/test_openapi_contract_smoke.py`
- Financial investigation workflow test now refreshes ES once before graph traversal to avoid flakiness (GET is realtime; SEARCH is NRT):
  - `/Users/isihyeon/Desktop/SPICE-Harvester/backend/tests/test_financial_investigation_workflow_e2e.py`

### Gate B — Foundry Backend E2E QA (PASS, hardened)
Pre‑run hygiene:
- `qa_bugs.json` archived/reset under `/Users/isihyeon/Desktop/SPICE-Harvester/qa_bugs_archive/`

Command:
```bash
PYTHONUNBUFFERED=1 \
RUN_FOUNDRY_E2E_QA=true QA_FAIL_SEVERITY=P1 \
PYTHONPATH=/Users/isihyeon/Desktop/SPICE-Harvester/backend \
python -m pytest /Users/isihyeon/Desktop/SPICE-Harvester/backend/tests/test_foundry_e2e_qa.py -v -s --tb=short
```
Result: **PASS** — `1 passed in 161.01s`, `0 bugs` written to `/Users/isihyeon/Desktop/SPICE-Harvester/qa_bugs.json` (`[]`).  
DB used by the run: `qa_ccefbe168f`.

New hard gates added/strengthened in Gate B:
- Phase 0: 401/403/409/400 negative cases + v2 error shape (no 500 leakage).
- RBAC personas: distinct JWT subjects + DB role grants; viewer cannot write, intruder cannot read, security-only governance upserts.
- Branch separation: action writeback branch ≠ view branch, verified via `ActionLogRegistry` + ES `_source.branch`.
- Incremental update (2nd version): watermark seeding + `max_rows=1` to catch prune/dedup bugs; count invariant (no duplicates).
- Deletion handling (3rd version): delta-mode removes missing entity; count decreases by exactly 1.
- Teardown: best-effort DB deletion + ES index cleanup so frequent re-runs don’t accumulate trash.

## What was validated (Gate B phases)
- Phase 1 (Ingestion): DB create, role grant, CSV uploads (5 Kaggle + 3 live APIs), dataset listing, v2 dataset `readTable`.
- Phase 2 (Pipelines): create/preview/build/deploy pipelines; verified output dataset created (`order_items_with_pk`).
- Phase 3 (Ontology): create 5 object types + 3 action types; v2 objectTypes/actionTypes queries; **outgoingLinkTypes** present.
- Phase 4 (Objectify + ES materialization): mapping specs, objectify DAG, ES reached exact expected counts:
  - total `2378` docs: `Customer=500, Order=500, Product=490, Seller=320, OrderItem=568`
- Phase 5 (Search/Graph/Links): filtering, `orderBy price DESC`, pagination, count, and v2 links:
  - `Order → Customer` and `OrderItem → Product (+Order)` resolve correctly.
- Phase 6–7 (Actions + Closed loop): simulate/apply/batch apply; per‑order convergence verified; graph query works after action.
- Phase 8 (Live cross-domain): weather/exchange/earthquake ontology+objectify+search + cross-domain pipeline creation.

## Changes made to reach green gates (high‑impact)
- Fix Parquet objectify for pipeline outputs: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/objectify_worker/main.py`
- Ensure DuckDB dependency available in service installs: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/shared/setup.py`
- v2 ontology linkTypes/links fallback derived from relationships when link_type resources absent:
  - `/Users/isihyeon/Desktop/SPICE-Harvester/backend/bff/routers/foundry_ontology_v2.py`
- v2 search `orderBy` robustness over nested `properties` mapping:
  - `/Users/isihyeon/Desktop/SPICE-Harvester/backend/oms/routers/query.py`
- Closed-loop determinism: ActionApplied projection upserts by `instance_id` (no duplicate overlay docs):
  - `/Users/isihyeon/Desktop/SPICE-Harvester/backend/projection_worker/main.py`
- Separate overlay branch from lakeFS writeback target branch (avoid overriding writeback target with view branch):
  - `/Users/isihyeon/Desktop/SPICE-Harvester/backend/oms/services/action_submit_service.py`
- Remove a `shared -> data_connector` dependency cycle by moving import mode constants into shared:
  - `/Users/isihyeon/Desktop/SPICE-Harvester/backend/shared/models/import_modes.py`
  - `/Users/isihyeon/Desktop/SPICE-Harvester/backend/data_connector/adapters/import_config_validators.py`
  - `/Users/isihyeon/Desktop/SPICE-Harvester/backend/shared/services/core/connector_ingest_service.py`
- Keep OpenAPI public surface stable: hide legacy pipeline-branch routes from OpenAPI schema:
  - `/Users/isihyeon/Desktop/SPICE-Harvester/backend/bff/routers/pipeline_datasets_catalog.py`
- Regenerate architecture reference to keep checks consistent:
  - `/Users/isihyeon/Desktop/SPICE-Harvester/docs/ARCHITECTURE.md`

## Remaining notes / limitations
- Gate B is “black‑box” for most lifecycle steps (HTTP‑only), but still uses a direct Postgres role grant during setup (no public ACL API available in this stack).
- This verification proves functional correctness for the exercised lifecycle and dataset sizes; it is **not** a performance/load benchmark and does not establish throughput/latency SLAs.
