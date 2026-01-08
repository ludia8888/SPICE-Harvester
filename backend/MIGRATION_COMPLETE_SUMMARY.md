# 🎉 Event Sourcing Migration — Complete (Updated)
> Status: Historical snapshot. Content reflects the state at the time it was written and may be outdated.

> Updated: 2025-12-17  
> Status: **STEADY STATE** — dual-write flags and wrapper formats removed.

## Executive Summary

SPICE-Harvester now uses:
- **S3/MinIO Event Store (SSoT)** as the immutable append-only log
- **EventPublisher** (S3 tail → Kafka) for transport
- **PostgreSQL** for:
  - `processed_events` durable idempotency registry (lease/heartbeat)
  - write-side atomic `sequence_number` allocation

## What’s Different from the Legacy Migration Docs

Legacy migration artifacts are no longer supported:
- `ENABLE_S3_EVENT_STORE` / `ENABLE_DUAL_WRITE` (removed)
- Kafka wrapper formats:
  - `message_type/payload` wrapper
  - `s3_reference` indirection
  - `metadata.storage_mode` migration marker
- `migration_helper.py` dual-write path (removed)

Kafka payloads are now **always** `EventEnvelope` JSON.

## Current System State (Production Defaults)

```bash
ENABLE_EVENT_SOURCING=true
EVENT_STORE_BUCKET=spice-event-store

# Consumers (must be on in production)
ENABLE_PROCESSED_EVENT_REGISTRY=true
PROCESSED_EVENT_LEASE_TIMEOUT_SECONDS=900
PROCESSED_EVENT_HEARTBEAT_INTERVAL_SECONDS=30

# Write-side sequencing
EVENT_STORE_SEQUENCE_ALLOCATOR_MODE=postgres
EVENT_STORE_SEQUENCE_SCHEMA=spice_event_registry
EVENT_STORE_SEQUENCE_HANDLER_PREFIX=write_side
```

## Verification (Steady State)

1. **Publisher**
   - Runs `backend/message_relay/main.py` (S3 tail → Kafka).
   - Produces `EventEnvelope` bytes to Kafka (batched flush).

2. **Consumers**
   - Workers/projection consume only `EventEnvelope` JSON.
   - Side-effects are guarded by Postgres `processed_events`.

3. **OCC**
   - Update/delete commands require `expected_seq`.
   - On mismatch: OMS returns **409 Conflict** (command is not appended).

Reference contracts:
- `docs/IDEMPOTENCY_CONTRACT.md`
- `docs/architecture/README.md`

## Key Files (Current)

- Event Store: `backend/oms/services/event_store.py`
- Publisher: `backend/message_relay/main.py`
- Registry: `backend/shared/services/processed_event_registry.py`
- Seq allocator: `backend/shared/services/aggregate_sequence_allocator.py`
- Instance async API (expected_seq): `backend/oms/routers/instance_async.py`
