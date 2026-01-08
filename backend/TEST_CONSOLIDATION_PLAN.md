# Test Suite Overview — Event Sourcing Steady State
> Status: Historical snapshot. Content reflects the state at the time it was written and may be outdated.

> Updated: 2025-12-17  
> Status: consolidation complete (legacy suites removed/archived)

## Where tests live

- Maintained tests are under `backend/tests/`.
- Live/destructive smoke tests are **skipped by default** (opt-in via env var).

## Key tests (current)

- `backend/tests/test_idempotency_chaos.py` — Postgres `processed_events` lease/idempotency + ordering guard.
- `backend/tests/test_sequence_allocator.py` — Postgres atomic per-aggregate seq allocator + OCC primitives.
- `backend/tests/test_oms_smoke.py` — OMS end-to-end smoke via HTTP (creates/deletes real DBs; opt-in).
- `backend/tests/test_core_functionality.py` — full stack checks (OMS/BFF/Funnel + infra).
- `backend/tests/test_terminus_version_control.py` — TerminusDB branch/version primitives used by OMS.

## How to run

```bash
# Minimal correctness layer (Postgres only)
pytest backend/tests/test_sequence_allocator.py -q
pytest backend/tests/test_idempotency_chaos.py -q

# OMS end-to-end smoke (destructive)
RUN_LIVE_OMS_SMOKE=true pytest backend/tests/test_oms_smoke.py -q

# Full suite (will skip tests when infra/services are missing)
pytest backend/tests -q
```

## Notes

- There is no migration-helper / dual-write test matrix anymore.
- If you need chaos tests for worker crash points (claim/apply/mark_done), add them beside `backend/tests/test_idempotency_chaos.py`.
