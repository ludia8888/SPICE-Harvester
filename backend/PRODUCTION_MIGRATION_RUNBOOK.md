# Production Runbook — Event Sourcing Steady State
## S3/MinIO Event Store (SSoT) + EventPublisher (S3 tail → Kafka)

> Status: Operational guidance. Validate ports and environment variables against your deployment.

> Updated: 2025-12-17  
> This document replaces the legacy “dual-write migration runbook”. Dual-write flags and wrapper formats were removed.

## 📋 Table of Contents
1. [System Overview](#system-overview)
2. [Prerequisites](#prerequisites)
3. [Configuration](#configuration)
4. [Startup / Deploy Order](#startup--deploy-order)
5. [Validation Checklist](#validation-checklist)
6. [Operational Semantics](#operational-semantics)
7. [Troubleshooting](#troubleshooting)
8. [Maintenance](#maintenance)

---

## System Overview

**Single canonical data path**:

OMS/Workers → **S3/MinIO Event Store (SSoT)** → **EventPublisher** → Kafka → Consumers

Key contracts:
- Kafka delivery is **at-least-once**
- Consumers must be idempotent by `event_id` and ordered by `sequence_number`
- PostgreSQL is required for:
  - `processed_events` registry (lease/heartbeat)
  - write-side atomic `sequence_number` reservation

Reference: `docs/IDEMPOTENCY_CONTRACT.md`

---

## Prerequisites

Infrastructure:
- MinIO/S3 (Event Store)
- Kafka
- PostgreSQL (registry + seq allocator)
- TerminusDB (write-side graph)
- Elasticsearch (read model; optional for write correctness)

Quick checks (adjust ports/hosts for your environment):
```bash
curl -fsS http://localhost:9000/minio/health/live
pg_isready -h 127.0.0.1 -p ${POSTGRES_PORT_HOST:-5433}
kafka-topics --bootstrap-server 127.0.0.1:${KAFKA_PORT_HOST:-39092} --list >/dev/null
curl -fsS http://localhost:6363/api/info >/dev/null
```

---

## Configuration

### Core toggles
```bash
ENABLE_EVENT_SOURCING=true
EVENT_STORE_BUCKET=spice-event-store
```

### MinIO/S3 (defaults match `backend/docker-compose.yml`)
```bash
MINIO_ENDPOINT_URL=http://127.0.0.1:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
```

### PostgreSQL (registry + allocator)
```bash
POSTGRES_URL=postgresql://spiceadmin:spicepass123@127.0.0.1:5432/spicedb
ENABLE_PROCESSED_EVENT_REGISTRY=true
PROCESSED_EVENT_LEASE_TIMEOUT_SECONDS=900
PROCESSED_EVENT_HEARTBEAT_INTERVAL_SECONDS=30
```

### Write-side sequencing (Event Store)
```bash
EVENT_STORE_SEQUENCE_ALLOCATOR_MODE=postgres
EVENT_STORE_SEQUENCE_SCHEMA=spice_event_registry
EVENT_STORE_SEQUENCE_HANDLER_PREFIX=write_side
```

### Publisher batching/metrics
```bash
EVENT_PUBLISHER_BATCH_SIZE=200
EVENT_PUBLISHER_KAFKA_FLUSH_BATCH_SIZE=200
EVENT_PUBLISHER_KAFKA_FLUSH_TIMEOUT_SECONDS=10
EVENT_PUBLISHER_METRICS_LOG_INTERVAL_SECONDS=30
```

---

## Startup / Deploy Order

1. Start infra: Postgres → MinIO → Kafka → TerminusDB (→ Elasticsearch)
2. Start OMS (`backend/oms/main.py`)
3. Start EventPublisher (`backend/message_relay/main.py`)
4. Start workers (`backend/ontology_worker/main.py`, `backend/instance_worker/main.py`)
5. Start projection (`backend/projection_worker/main.py`)

---

## Validation Checklist

1. **OMS health**
```bash
curl -fsS http://localhost:8000/health | jq .
```

2. **Event store write path**
- Create a command via OMS async endpoint; confirm an object appears in `EVENT_STORE_BUCKET` under `events/...`
- Confirm an index entry exists in `indexes/by-date/...`

3. **Publisher**
- Confirm publisher logs show batched publishes and checkpoint updates.
- Confirm `checkpoints/event_publisher.json` exists and advances monotonically.

4. **Consumers**
- Confirm `spice_event_registry.processed_events` receives rows with `status=done`.

5. **OCC**
- UPDATE/DELETE calls without correct `expected_seq` must return **409 Conflict** (no command appended).

---

## Operational Semantics

### Idempotency
- Same `event_id` must produce **at most one** side-effect.
- Enforced by Postgres `processed_events` registry (per handler).

### Ordering
- `sequence_number` is the per-aggregate truth.
- Consumers ignore stale events (`incoming_seq <= current_seq`).

### Optimistic Concurrency (OCC)
- Commands carry `expected_seq`.
- OMS returns **409 Conflict** on mismatch (prevents accepting a command that would overwrite a newer state).

---

## Troubleshooting

### 409 Conflict spikes
Cause: clients are sending stale `expected_seq`.  
Action: read current aggregate seq (from your read model / domain query) and retry with updated value.

### Events “stuck” in processing
Cause: worker crashed after `claim`.  
Action: confirm `PROCESSED_EVENT_LEASE_TIMEOUT_SECONDS` and heartbeat are configured; stuck events should be re-claimable after TTL.

### Postgres outage
Consumers rely on Postgres for correctness.  
Recommended: treat Postgres outage as “pause consumption” (do not apply side-effects without the registry).

### Publisher replays duplicates
Expected under at-least-once.  
Consumers must remain idempotent; investigate only if side-effects are duplicating (registry misconfig / disabled).

---

## Maintenance

- Plan retention/archival for `processed_events` (growth is proportional to throughput).
- Monitor registry indexes and consider partitioning if needed.
- Keep S3/MinIO lifecycle policies for long-term event storage as required by compliance.
