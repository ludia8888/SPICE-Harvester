# Projection Consistency Runbook

This runbook defines how we verify and recover consistency between:

- Durable source: Event Store (`S3/MinIO`, immutable event envelopes)
- Read model: Elasticsearch instance projection indices

Foundry references used as baseline:

- Query basics (Search Objects, v2): https://www.palantir.com/docs/foundry/api/ontology-resources/query-basics/
- Search Objects (v2): https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-objects/search-objects
- Object Edits overview: https://www.palantir.com/docs/foundry/object-edits/overview
- How edits are applied: https://www.palantir.com/docs/foundry/object-edits/how-edits-applied/

## Contracts

### 1) Edit queue semantics

- Commands are persisted as immutable event envelopes in Event Store.
- Projection writes are derived data and must be rebuildable from Event Store.
- Outbox-style delivery is at-least-once; idempotency is enforced by:
  - deterministic `event_id`
  - processed-event registry
  - projection overwrite semantics by `instance_id`

### 2) Checkpoint contracts

- `processed_event_registry`: worker delivery/processing checkpoint.
- `objectify_watermarks`: incremental objectify checkpoint.
- `pipeline_watermarks`: incremental pipeline checkpoint.
- Event Store by-date / by-aggregate indexes are replay checkpoints for recovery scans.

## Verification Procedure

Run projection consistency check:

```bash
cd backend
python scripts/verify_projection_consistency.py --from-minutes-ago 240 --max-events 10000
```

Behavior:

- Replays `INSTANCE_CREATED|INSTANCE_UPDATED|INSTANCE_DELETED` from Event Store.
- Computes expected latest state per `(db_name, branch, class_id, instance_id)`.
- Verifies ES document presence/absence and key identity fields.
- Exits non-zero on mismatch.

## Replay / Rebuild Procedure

1. Detect mismatches
- Run the consistency script above (or with `--json` for machine parsing).

2. Rebuild projection
- Use admin recompute projection flow (`/api/v1/admin/recompute-projection`) for affected projection scope.
- Keep Event Store as source of truth; do not patch ES manually except emergency mitigation.

3. Re-verify
- Re-run consistency script for same window/scope.

4. If lineage side-effects are missing
- Run:

```bash
cd backend
python scripts/backfill_lineage.py --mode replay --from <ISO_TIMESTAMP>
```

## CI Automation

- `backend/run_production_tests.sh` runs:
  - `tests/test_consistency_e2e_smoke.py`
  - `scripts/verify_projection_consistency.py`

This provides automated regression coverage for replay/projection consistency.
