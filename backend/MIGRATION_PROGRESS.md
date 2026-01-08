# S3/MinIO Event Store Migration (Historical) — Completed
> Status: Historical snapshot. Content reflects the state at the time it was written and may be outdated.

> Updated: 2025-12-17  
> Status: **COMPLETE** — legacy dual-write flags and Kafka wrapper formats were removed.

This file used to track the cutover from a legacy PostgreSQL-centric “event store” concept to the current production architecture.  
It is kept for context, but **the system no longer supports the dual-write migration modes described in older versions of this document**.

## ✅ Current Production Architecture (Steady State)

1. **S3/MinIO Event Store (SSoT)**
   - Immutable append-only `EventEnvelope` JSON (commands + domain events)
   - Indexes + durable publisher checkpoint stored in the same bucket

2. **EventPublisher (message_relay)**
   - Tails `indexes/by-date/...` and publishes to Kafka
   - Delivery semantics: **at-least-once**
   - Best-effort dedup (process-local + checkpoint snapshot)
   - Batched produce/flush + metrics logging

3. **Consumers (Workers / Projection)**
   - **Kafka payload schema is unified**: `EventEnvelope` JSON only
   - Idempotency + ordering is enforced via Postgres registry:
     - `processed_events` (handler,event_id) with lease/heartbeat
     - `aggregate_versions` monotonic seq guard per handler

4. **Postgres role**
   - **Not** an event store
   - Used for:
     - `processed_events` registry (durable idempotency)
     - write-side atomic `sequence_number` allocation (true concurrency safety)

Reference contracts:
- Idempotency/ordering: `docs/IDEMPOTENCY_CONTRACT.md`
- Architecture overview: `docs/architecture/README.md`

## ✅ What Changed vs. Legacy Migration Plan

- Removed feature flags:
  - `ENABLE_S3_EVENT_STORE` (removed)
  - `ENABLE_DUAL_WRITE` (removed)
- Removed legacy Kafka wrapper formats:
  - `{"message_type": "...", "payload": ...}` wrapper
  - `s3_reference` indirection payloads
  - `metadata.storage_mode` migration hints
- Removed `migration_helper.py` dual-write path (no longer exists)
- Strengthened correctness:
  - **Atomic write-side `sequence_number` reservation in Postgres** before S3 append
  - **`expected_seq` Optimistic Concurrency** for command append (OMS returns **409** on conflict)

## ✅ Operational Checklist (Must-Pass)

1. Publisher duplicate publish / checkpoint rollback → consumer side-effects happen once.
2. Worker crash across `claim`/side-effect/`mark_done` boundaries → lease TTL enables recovery.
3. Out-of-order events by `sequence_number` → stale events ignored.
4. Concurrent delivery (2 consumers) → only one claim succeeds.
5. OCC:
   - UPDATE/DELETE without correct `expected_seq` → OMS returns **409** (no command appended).

## ✅ Current Env Vars (Relevant)

Event sourcing:
- `ENABLE_EVENT_SOURCING=true` (production default)
- `EVENT_STORE_BUCKET=spice-event-store`

Idempotency registry:
- `ENABLE_PROCESSED_EVENT_REGISTRY=true`
- `PROCESSED_EVENT_LEASE_TIMEOUT_SECONDS=900`
- `PROCESSED_EVENT_HEARTBEAT_INTERVAL_SECONDS=30`

Write-side sequencing:
- `EVENT_STORE_SEQUENCE_ALLOCATOR_MODE=postgres` (default)
- `EVENT_STORE_SEQUENCE_SCHEMA=spice_event_registry`
- `EVENT_STORE_SEQUENCE_HANDLER_PREFIX=write_side`

Publisher batching/metrics:
- `EVENT_PUBLISHER_KAFKA_FLUSH_BATCH_SIZE` (default: `EVENT_PUBLISHER_BATCH_SIZE`)
- `EVENT_PUBLISHER_KAFKA_FLUSH_TIMEOUT_SECONDS=10`
- `EVENT_PUBLISHER_METRICS_LOG_INTERVAL_SECONDS=30`
