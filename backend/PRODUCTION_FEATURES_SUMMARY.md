# Production Features Implementation Summary
## Event Sourcing + CQRS (Steady-State)

> NOTE (2025-12): This summary describes the current steady-state path:
> **S3/MinIO Event Store (SSoT) + EventPublisher (S3 tail → Kafka)** with durable idempotency/ordering in Postgres.
> Contract: `docs/IDEMPOTENCY_CONTRACT.md`.

### ✅ Completed Production Features

#### 1) Durable consumer idempotency (exactly-once side-effect per handler)
- **Location**: `backend/shared/services/processed_event_registry.py`
- **Storage**: Postgres `spice_event_registry.processed_events` keyed by `(handler, event_id)`
- **Failure safety**: lease + heartbeat + reclaim to avoid “stuck forever” after worker crash
- **Integration points**:
  - `backend/ontology_worker/main.py`
  - `backend/instance_worker/main.py`
  - `backend/projection_worker/main.py`

#### 2) Per-aggregate ordering guard (stale events cannot regress state)
- **Location**: `backend/shared/services/processed_event_registry.py`
- **Storage**: Postgres `spice_event_registry.aggregate_versions` keyed by `(handler, aggregate_id)`
- **Atomicity**: single-SQL monotonic advance (`ON CONFLICT … WHERE last_sequence < …`)

#### 3) Write-side optimistic concurrency (OCC) with atomic seq allocator
- **Location**: `backend/shared/services/aggregate_sequence_allocator.py`
- **Used by**: `backend/oms/services/event_store.py` (write-side seq reservation)
- **Behavior**:
  - Commands carry `expected_seq` (current aggregate seq expected by client)
  - On mismatch, OMS returns **409 Conflict** and does not append the command event

#### 4) S3/MinIO Event Store (SSoT) + EventPublisher checkpointing
- **Event Store**: `backend/oms/services/event_store.py`
  - stable by-event-id index + idempotency mismatch detection (`EVENT_STORE_IDEMPOTENCY_MISMATCH_MODE`)
- **Publisher**: `backend/message_relay/main.py`
  - durable checkpoint in S3 (`checkpoints/...`)
  - best-effort in-memory dedup window (publisher remains at-least-once)
  - batch produce + flush metrics (log-based)

### 📊 Production Invariants Guaranteed (by contract)
1. **Delivery semantics**: Publisher/Kafka are at-least-once; consumers must be idempotent.
2. **Idempotency key**: `(handler, event_id)` is the global dedup key for side-effects.
3. **Ordering rule**: per-aggregate `sequence_number` is truth; stale events are ignored.

### 🚀 How to Run (local)
```bash
# Full local stack
docker compose -f docker-compose.full.yml up -d

# Optional: run Postgres-based contract tests
cd backend && ./run_production_tests.sh --quick
```
