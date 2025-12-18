# S3/MinIO Event Store Migration Summary (Updated)

> Updated: 2025-12-17  
> Status: **COMPLETE** — legacy dual-write flags and Kafka wrapper formats were removed.

## Executive Summary

SPICE-Harvester now runs a **single production path**:

OMS/Workers → **S3/MinIO Event Store (SSoT)** → **EventPublisher (S3 tail → Kafka)** → Kafka → Consumers  

PostgreSQL is used **only** for:
- `processed_events` (durable idempotency + lease/heartbeat)
- write-side **atomic** `sequence_number` allocation (true concurrency safety)

## Key Architectural Correction

### Before (legacy / wrong mental model)
- PostgreSQL treated as an event store / delivery buffer
- Kafka messages had multiple shapes (wrappers, S3 references, etc.)

### After (current / correct)
- **S3/MinIO = the immutable event log (SSoT)**
- Kafka payload schema is unified: **`EventEnvelope` JSON only**
- Consumers must be idempotent by `event_id` and ordered by `sequence_number`

## Current Message Schema (Kafka)

All command/domain topics carry an `EventEnvelope` JSON:

```json
{
  "event_id": "…",
  "event_type": "CREATE_INSTANCE_REQUESTED",
  "aggregate_type": "Instance",
  "aggregate_id": "db:Class:instance_id",
  "occurred_at": "2025-12-17T00:00:00Z",
  "actor": "user_id",
  "data": { "command_id": "…", "command_type": "CREATE_INSTANCE", "expected_seq": 0, "payload": { } },
  "metadata": { "kind": "command", "kafka_topic": "instance_commands", "service": "oms" },
  "schema_version": "1",
  "sequence_number": 1
}
```

Notes:
- `metadata.kind` distinguishes envelopes:
  - `command` for command topics
  - `domain` for projection topics
- `sequence_number` is allocated **atomically** per aggregate before append.

## Correctness Guarantees Added (Post-Migration)

1. **Idempotency contract**
   - Same `event_id` produces **at most one side-effect** across the system.
   - Enforced by `processed_events` registry (handler-scoped).

2. **Ordering rule**
   - Per-aggregate `sequence_number` is truth; stale events are ignored.

3. **Optimistic Concurrency (OCC)**
   - Commands include `expected_seq`.
   - On mismatch, OMS returns **409 Conflict** (command is not appended).

See: `docs/IDEMPOTENCY_CONTRACT.md`

## Key Components (Current)

- Event Store: `backend/oms/services/event_store.py`
- Publisher: `backend/message_relay/main.py`
- Write-side consumers: `backend/instance_worker/main.py`, `backend/ontology_worker/main.py`
- Projection consumer: `backend/projection_worker/main.py`
- Registry: `backend/shared/services/processed_event_registry.py`
- Seq allocator: `backend/shared/services/aggregate_sequence_allocator.py`

## Legacy Migration Artifacts Removed

- `ENABLE_S3_EVENT_STORE` / `ENABLE_DUAL_WRITE` feature flags (removed)
- `migration_helper.py` dual-write path (removed)
- Kafka wrapper formats:
  - `message_type/payload` wrapper
  - `s3_reference` indirection
  - `metadata.storage_mode` migration marker

## Operational Notes

- Publisher/Kafka are **at-least-once** by design; downstream idempotency is the contract.
- Postgres is now a **core dependency** for consumer correctness (`processed_events` + seq allocator).
