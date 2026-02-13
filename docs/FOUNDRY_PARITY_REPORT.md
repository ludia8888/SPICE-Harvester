# Foundry Parity Report (Code-Backed, End-to-End)

> ⚠️ Disclaimer (important)
>
> Palantir Foundry is proprietary/closed-source. This repo cannot verify Foundry’s internal
> implementation details or guarantee “byte-for-byte identical behavior”.
>
> This report defines **a practical parity target**: Foundry’s publicly-known “core product
> capabilities” (data onboarding → versioned transforms → ontology/semantic layer → governance
> → lineage/audit → operational runtime) and verifies them **end-to-end** using:
> - this repo’s architecture/contracts
> - automated integration/E2E tests
> - live-stack smoke checks (HTTP + Kafka + storage)

## 1) Parity Scope (what we mean by “core Foundry”)

The checklist below models Foundry core as these capability groups:

1. **Data onboarding**: ingest datasets + connectors; deterministic parsing; idempotent retries.
2. **Versioning & branching**: “what changed?” and safe promotion/rollback semantics.
3. **Pipelines / transforms**: graph-based transforms with preview/build/deploy semantics.
4. **Ontology / semantic layer**: object types + mappings; objectification (rows → entities/relationships).
5. **Governance & access policy**: permission gates, safe-by-default behavior, explicit failures.
6. **Lineage & audit**: provenance edges + audit logs stored durably and queryable.
7. **Operational runtime**: workers, retries, DLQ, leases/heartbeats, at-least-once transport.
8. **Search / read models**: projection into ES/OpenSearch-like store (idempotent).
9. **Writeback** (Foundry-like operationalization): governed patchset writeback + materialization.
10. **Agent tooling** (optional extension): deterministic tool layer + single-loop agent runtime.

## 2) SPICE-Harvester mapping (implementation → tests)

| Capability | SPICE implementation (code) | Primary E2E verification |
|---|---|---|
| Data onboarding | BFF ingest + registries + object store/lakeFS | `backend/tests/test_core_functionality.py` |
| Versioning & branching | TerminusDB + lakeFS branching + promotion rules | `backend/tests/test_terminus_version_control.py`, `backend/tests/test_branch_virtualization_e2e.py` |
| Pipelines/transforms | Spark pipeline-worker + scheduler + pipeline plans | `backend/tests/test_pipeline_transform_cleansing_e2e.py`, `backend/tests/test_pipeline_execution_semantics_e2e.py` |
| Ontology/semantic layer | OMS + Terminus schema + mapping spec + objectify-worker | `backend/tests/test_pipeline_objectify_es_e2e.py` |
| Governance & access policy | Access policies + proposal/approval + explicit error envelopes | `backend/tests/test_auth_hardening_e2e.py`, access-policy E2E tests |
| Lineage & audit | Postgres lineage/audit stores + BFF APIs | `docs/DATA_LINEAGE.md`, lineage/audit tests (if present) |
| Operational runtime | SafeKafkaConsumer + ProcessedEventRegistry + DLQ + heartbeat | `backend/tests/test_consistency_e2e_smoke.py`, `backend/tests/test_worker_lease_safety_e2e.py`, `backend/tests/test_idempotency_chaos.py` |
| Search/read models | projection-worker + search-projection-worker | `backend/tests/test_core_functionality.py` + projection-specific tests |
| Writeback | action-worker/outbox + lakeFS patchset + materializer + ES overlay | `backend/tests/test_action_writeback_e2e_smoke.py` |
| Agent tooling | agent service + MCP servers + allowlist | `scripts/e2e_agent_pipeline_demo.sh`, tooling generated docs under `docs/reference/_generated/` |

## 3) How to run the verification suite (recommended)

### 3.1 Full “production” gate (expects local docker stack)

```bash
PYTHON_BIN="$PWD/.venv-ci/bin/python" ./backend/run_production_tests.sh --full
```

### 3.2 Extra E2E coverage (recommended add-ons)

```bash
PYTHONPATH=backend "$PWD/.venv-ci/bin/python" -m pytest -q \
  backend/tests/test_action_writeback_e2e_smoke.py \
  backend/tests/test_access_policy_link_indexing_e2e.py \
  backend/tests/test_pipeline_execution_semantics_e2e.py \
  backend/tests/test_pipeline_streaming_semantics_e2e.py \
  backend/tests/test_pipeline_type_mismatch_guard_e2e.py
```

### 3.3 Agent smoke (optional)

```bash
./scripts/e2e_agent_pipeline_demo.sh
./scripts/smoke_agent_progress.sh
```

## 4) Results

Latest verified run (local dev stack):

- Date: Fri Feb 13 2026 (KST)
- Git baseline: `b33b56a` (post-parity hardening)
- Host: macOS (Apple Silicon), Python 3.11 + Java 17 + PySpark
- Notes:
  - Elasticsearch runs as `linux/amd64` locally for stability on Apple Silicon (aarch64 images can SIGILL).
  - Spark unit regressions use Java 17 (`/opt/homebrew/opt/openjdk@17`) with local IP pinning.
  - Core parity regression bundle passed: `116 passed`.

### 4.1 Production suite

- ✅ `./backend/run_production_tests.sh --full`

### 4.2 Workshop/writeback parity (operational actions)

- ✅ `RUN_LIVE_ACTION_WRITEBACK_SMOKE=true PYTHONPATH=backend pytest -q -c backend/pytest.ini backend/tests/test_action_writeback_e2e_smoke.py`

### 4.3 Recommended extended coverage (optional)

These are high-signal but can be slow. Run on a warmed stack.

- `backend/tests/test_pipeline_execution_semantics_e2e.py` (Spark + retries/idempotency)
- `backend/tests/test_pipeline_streaming_semantics_e2e.py`
- `backend/tests/test_pipeline_type_mismatch_guard_e2e.py`
- `backend/tests/test_access_policy_link_indexing_e2e.py`

## 5) Functions Matrix (Foundry Index Snapshot)

Pipeline function compatibility is now tracked from a single snapshot source:

- Snapshot: `backend/tests/fixtures/foundry_functions_index_snapshot_2026_02_13.yaml`
- Adapter: `backend/shared/tools/foundry_functions_compat.py`
- Contract gate: `backend/tests/unit/pipeline_functions/test_functions_matrix_contract.py`
- Preview gate: `backend/tests/unit/pipeline_functions/test_functions_preview_compat.py`
- Spark gate: `backend/tests/unit/pipeline_functions/test_functions_spark_compat.py`

Compatibility status taxonomy is fixed to:

- `supported`: expected to work and has executable regression tests.
- `partial`: supported only in limited paths/semantics (documented in snapshot notes).
- `unsupported`: intentionally unsupported in current engine path.

This gives a deterministic, updateable matrix that can be re-baselined by updating
the snapshot file and corresponding tests together.

## 6) Dataset Output Parity (Foundry-aligned)

Dataset outputs now run through a single policy layer:

- Source of truth: `backend/shared/services/pipeline/dataset_output_semantics.py`
- Enforced in:
  - `backend/shared/services/pipeline/output_plugins.py`
  - `backend/shared/services/pipeline/pipeline_definition_validator.py`
  - `backend/shared/services/pipeline/pipeline_preflight_utils.py`
  - `backend/pipeline_worker/main.py`
  - `backend/bff/services/pipeline_execution_service.py`
  - MCP input normalization (`plan_add_output`)

Implemented write modes:

- `default`
- `always_append`
- `append_only_new_rows`
- `changelog`
- `snapshot_difference`
- `snapshot_replace`
- `snapshot_replace_and_remove`

Required metadata semantics:

- `append_only_new_rows/changelog/snapshot_difference/snapshot_replace_and_remove` require `primary_key_columns`
- `snapshot_replace_and_remove` requires `post_filtering_column`
- Output formats are constrained to `parquet|json|csv|avro|orc`

Default mode resolution semantics (aligned with Foundry docs):

- `write_mode=default` resolves to `snapshot_replace` when additive incremental signal is unavailable.
- `write_mode=default` resolves to append family only when incremental input is explicitly additive.
- PK presence then chooses `append_only_new_rows`; PK absence chooses `always_append` (with warning).

Runtime semantics hardening:

- `changelog` keeps changed/new rows without input-PK dedupe so repeated updates are preserved as events.
- `streamJoin(strategy=left_lookup)` is fail-closed when right input is not a direct input node.
- UDF is reference-only (`udfId` + optional `udfVersion`); inline `udfCode` is rejected in validator/runtime.

Build/deploy consistency:

- build artifacts include resolved write policy metadata and policy hash
- deploy promotion verifies policy mismatch and blocks on conflict (fail-closed)
- dataset version `sample_json` includes resolved mode/pk/hash context for debugging

## 7) Known limitations vs “real Foundry”

Even with all tests passing, these items are typically Foundry-specific and may not be
implemented 1:1 in an open repo:

- Full UI parity (Contour/Quiver/workbooks), advanced governance workflows, enterprise identity
  integrations, and proprietary optimizations.
- Feature-by-feature behavior equivalence for every edge case without an official Foundry spec.

<!-- DOC_SYNC: 2026-02-13 Foundry pipeline parity + runtime consistency sweep -->
