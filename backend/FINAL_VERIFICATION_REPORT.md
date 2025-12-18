# Final Verification Checklist — Event Sourcing Steady State

> Updated: 2025-12-17  
> Scope: S3/MinIO Event Store (SSoT) + EventPublisher (S3 tail → Kafka) + Kafka consumers (Workers/Projection).  
> Contract: `docs/IDEMPOTENCY_CONTRACT.md`

## What “PASS” means

- Same `event_id` creates **at most one** side-effect (TerminusDB updates, Elasticsearch indexing, Redis cache updates, external API calls).
- Out-of-order events never regress state (`sequence_number` stale-guard).
- OCC rejects stale writes (`expected_seq` mismatch → **409 Conflict**, and no command is appended).
- Publisher/Kafka remain **at-least-once**; duplicates are safe by consumer idempotency.

## Prerequisites

- MinIO/S3, Kafka, Postgres, TerminusDB running.
- For end-to-end checks: OMS + EventPublisher + workers + projection running.

## Verification commands

### A) Postgres correctness layer (fast, infra-only)

```bash
pytest backend/tests/test_sequence_allocator.py -q
pytest backend/tests/test_idempotency_chaos.py -q
```

### B) TerminusDB branch/version primitives (Terminus-only)

```bash
pytest backend/tests/test_terminus_version_control.py -q
```

### C) OMS live smoke (creates/deletes real DBs)

```bash
RUN_LIVE_OMS_SMOKE=true pytest backend/tests/test_oms_smoke.py -q
```

### D) Full stack core flow (requires OMS/BFF/Funnel running)

```bash
pytest backend/tests/test_core_functionality.py -q
```

## Manual spot-checks (optional)

- Publisher checkpoint exists and advances: `checkpoints/event_publisher.json` in `EVENT_STORE_BUCKET`.
- Consumers write registry rows: `spice_event_registry.processed_events` shows `status=done`.

## Notes

- Elasticsearch is optional for write correctness; projection failures must not break write-side idempotency.
- Legacy dual-write flags (`ENABLE_S3_EVENT_STORE`, `ENABLE_DUAL_WRITE`) and Kafka wrappers (`message_type/payload`, `s3_reference`, `storage_mode`) are removed.
