# Release Operations Policy

## Goal

This document locks in the release rules that move the backend from "works in development" to
"safe to ship repeatedly":

- migration-first schema changes
- no runtime DDL fallback in CI/staging/production
- automated release smoke gates
- explicit infra-required CI tier
- forward-compatible schema rollout rules
- concrete rollback and failure-budget criteria

## 1. Migration-First Policy

The source of truth for schema creation and schema evolution is:

- `backend/database/migrations`
- `backend/database/migrate/apply_migrations.sh`

Runtime code may not introduce new schema objects as part of normal production execution.

Allowed exception:

- temporary compatibility fallback modules that already exist today and are guarded by
  `ALLOW_RUNTIME_DDL_BOOTSTRAP`

Guardrail:

- `backend/scripts/runtime_ddl_audit.py` fails CI if a new Python runtime DDL callsite appears
  outside the approved compatibility list.
- `backend/scripts/release_regression_gate.py` and `backend/release_regression_gate.json` are the
  executable source of truth for backend release gates.

## 2. Runtime DDL Fallback Removal Schedule

The schedule is fixed as follows.

- `2026-04-04`: CI, release-smoke, infra-tier, staging, and production must run with
  `ALLOW_RUNTIME_DDL_BOOTSTRAP=false`.
- `2026-04-18`: no new runtime DDL callsites may be added; the audit gate is mandatory on every PR.
- `2026-05-02`: every remaining runtime bootstrap path must have matching migration coverage and an
  owner-tracked removal issue.
- `2026-05-30`: remove runtime DDL fallback from production code paths; dev/test-only compatibility
  bootstrap is the latest acceptable end state if any fallback still remains.

Interpretation:

- staging/production rollback may redeploy older application code
- staging/production may not rely on runtime table creation to become healthy
- if a migration is missing, the release is blocked rather than self-healed

## 3. Backward-Compatible Schema Rollout Rules

Schema rollout is forward-only by default.

Rules:

1. Additive changes first.
   - add nullable columns
   - add new tables
   - add new indexes
   - do not remove or rename old columns in the same release

2. Code must tolerate both old and new shapes during the rollout window.
   - readers should prefer new columns when present
   - writers may dual-write if required
   - no release may assume an immediate all-node cutover

3. Backfills are separate from schema introduction.
   - migration creates shape
   - backfill populates data
   - cleanup/drop happens only after validation

4. Destructive changes require a second release.
   - remove columns only after at least one deployed release has stopped reading them
   - tighten nullability only after data and writers are already compliant

5. Rollback posture is application-first, not schema-first.
   - prefer redeploying the last known good app against a backward-compatible schema
   - avoid schema rollback except for restore-based emergency recovery

## 4. CI Tiers

### Non-Infra Tier

Purpose:

- fast feedback
- unit tests
- architecture and audit guards

Entry points:

- `make backend-unit`
- `make backend-coverage`

### Infra Tier

Purpose:

- run all `requires_infra` regressions that are not part of the curated release smoke

Entry point:

- `make backend-infra-tier`

CI job:

- `Backend Infra Tier`

Environment rule:

- `ALLOW_RUNTIME_DDL_BOOTSTRAP=false`

### Release Smoke Tier

Purpose:

- prove the deployable stack is healthy enough to ship
- keep the suite intentionally smaller than the full production gate

Entry point:

- `make backend-release-smoke`

Current suite:

- `backend/tests/test_oms_smoke.py`
- `backend/tests/test_openapi_contract_smoke.py`
- `backend/tests/test_command_status_ttl_e2e.py`
- `backend/tests/test_worker_lease_safety_e2e.py`
- `backend/tests/integration/test_pipeline_branch_lifecycle.py`

CI job:

- `Backend Release Smoke`

Environment rule:

- `ALLOW_RUNTIME_DDL_BOOTSTRAP=false`

### Release Regression Gate

Purpose:

- collapse the blocking backend release checks into one executable contract
- make local release validation and CI release validation use the same gate manifest
- produce a machine-readable report for operator review

Source of truth:

- `backend/release_regression_gate.json`

Local entry point:

- `make backend-release-gate`

Implementation:

- `backend/scripts/release_regression_gate.py --execute`

Current blocking gates:

- `backend-runtime-ddl-audit`
- `backend-error-taxonomy`
- `backend-unit`
- `backend-boundary-smoke`
- `backend-release-smoke`
- `backend-infra-tier`

## 5. Release Blocking Conditions

A release is blocked when any of the following is true:

- migrations fail or `schema_migrations` is missing
- runtime DDL audit fails
- release smoke fails
- infra tier fails on a regression introduced by the release branch
- health/readiness shows `hard_down` for required dependencies

## 6. Failure Budget and Rollback Criteria

These are the concrete thresholds for rollback decisions after deployment.

Immediate rollback:

- correctness bug with irreversible writes
- schema mismatch causing request failures across a core surface
- duplicate or missing command/event processing confirmed in production

Rollback within 15 minutes:

- sustained `5xx` rate above `1%` for `10` consecutive minutes on BFF or OMS
- sustained command/worker backlog growth with no recovery trend for `15` minutes
- release smoke equivalent customer paths fail in production after deploy

Forward-fix allowed instead of rollback only when all are true:

- no correctness breach
- no destructive migration dependency
- error rate stays below `1%`
- mitigation can be deployed within `30` minutes

## 7. Rollback Rules

Application rollback:

- preferred first action
- redeploy the last known good image/tag
- only valid when schema rollout followed the backward-compatible rules above

Schema rollback:

- not the default plan
- use restore/recovery only for emergency cases
- destructive "down migrations" are not relied on for normal rollback

Ontology rollback:

- remains non-prod/admin-only by default
- keep `ENABLE_OMS_ROLLBACK=false` in production unless an explicit incident plan says otherwise

## 8. Operator Checklist

Before release:

1. migrations applied successfully
2. `make backend-release-gate` green, or the CI `Backend Release Gate` job green
3. rollback target identified

After release:

1. watch health/readiness/status for required dependencies
2. check command backlog and worker leases
3. confirm no schema mismatch or replay/order regressions
4. decide rollback vs forward-fix using the failure-budget rules above
