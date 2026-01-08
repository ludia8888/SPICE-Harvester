# Idempotency & Ordering Contract

> Updated: 2026-01-08

## 1) Idempotency (as a contract)

Same `event_id` produces **at most one side-effect** across the whole system.

Side-effects include TerminusDB updates, Elasticsearch indexing, Redis cache updates, and any external API calls.  
Duplicates can happen anywhere (S3 tail → Kafka publish, Kafka re-delivery, worker restarts/replays), but results must converge to the same outcome.

## 2) Delivery semantics

- **Publisher / Kafka**: `at-least-once`
- **Consumers (Worker / Projection)**: idempotency is mandatory (contract)

## 3) Keys and ordering rules

- **Idempotency key**: `event_id`
- **Ordering rule**: per-aggregate `sequence_number` is the source of truth; events with `incoming_seq <= current_seq` are ignored.
- **Write-side sequencing**: `sequence_number` is reserved atomically in Postgres (per aggregate) before appending to the Event Store.
- **Optimistic concurrency (OCC)**: update/delete commands must carry `expected_seq`; on mismatch OMS returns **409 Conflict** and the command is not appended.

## 3.1) Event immutability / event_id reuse

Deterministic `event_id` is only safe if **the same `event_id` is never reused for a different payload**.

- Command envelopes (`metadata.kind=command`) treat **“same event_id, different payload”** as a contract violation.
- The Event Store enforces this at append time (default: **raise**).
- Runtime knob: `EVENT_STORE_IDEMPOTENCY_MISMATCH_MODE` = `error` (default) or `warn`.

## 4) Defense-in-depth (Publisher / Worker / Projection)

### A) Publisher (SSoT → Kafka)

Goal: reduce duplicates, but accept that duplicates still happen.

- Durable checkpointing (where we last published) in S3/MinIO.
- Optional lookback scan (bounded) to reduce the risk of missing late-arriving index entries.
- Best-effort dedup is optional; contract is still `at-least-once`.

### B) Worker (write-side side-effects)

Goal: prevent duplicated side-effects even under duplicates/restarts/replays.

- Use a **durable** processed-events registry (`Postgres`) keyed by `(handler, event_id)`.
- Processing flow:
  1) `claim(event_id)` (UNIQUE gate + lease)
  2) if already done/stale → ACK/skip
  3) if claimed → perform side-effects
  4) `mark_done` (and advance per-aggregate `last_sequence`)
  5) on failure → `mark_failed` (retry path)

### C) Projection (read-model side-effects)

Goal: keep read-models converging to the latest state under duplicates/out-of-order.

- Apply the same `event_id` registry + per-aggregate `sequence_number` guard.
- Elasticsearch documents are keyed by stable IDs (e.g., `instance_id`) and updated idempotently.

## 5) Code locations

- Postgres registry: `backend/shared/services/processed_event_registry.py`
- Publisher (S3 tail → Kafka): `backend/message_relay/main.py`
- Write-side consumers: `backend/instance_worker/main.py`, `backend/ontology_worker/main.py`
- Projection consumer: `backend/projection_worker/main.py`

## 6) Operational acceptance tests (must-pass)

1. Publisher duplicate publish (checkpoint rollback/restart) → Worker/Projection side-effects happen once.
2. Worker crash at 4 points (replay-safe):
   - right after `claim`
   - right before Terminus apply
   - right after Terminus apply (before commit/next step)
   - right before `mark_done`
3. Projection crash at 3 points:
   - right before ES write
   - right after ES write
   - right before Kafka offset commit
4. Out-of-order events (inject shuffled `sequence_number`) → stale events are ignored.
5. Same `event_id` concurrent delivery (two consumers receive) → only one `claim` succeeds.
6. Retry policy after `mark_failed` (backoff/retry/manual intervention) behaves as intended.
7. `aggregate_id` normalization edge cases (special chars/case/whitespace) → no unintended collisions.
8. Registry outage (Postgres down briefly) → behavior is explicit (fail-fast vs DLQ vs pause).

## 7) Runtime knobs (env)

- `ENABLE_PROCESSED_EVENT_REGISTRY` (default: `true`): if enabled, Postgres becomes a hard dependency for consumers.
- `PROCESSED_EVENT_LEASE_TIMEOUT_SECONDS` (default: `900`): processing lease TTL (re-claim allowed after TTL).
- `PROCESSED_EVENT_HEARTBEAT_INTERVAL_SECONDS` (default: `30`): heartbeat interval while processing.
- `PROCESSED_EVENT_OWNER` (optional): override worker identity for debugging.
- `INSTANCE_WORKER_MAX_RETRY_ATTEMPTS` (default: `5`): max retries before skipping a poison command event.
- `ONTOLOGY_WORKER_MAX_RETRY_ATTEMPTS` (default: `5`): max retries before skipping a poison command event.
- `PROJECTION_WORKER_MAX_RETRIES` (default: `5`): max retries before sending an event to the projection DLQ.

### Publisher (S3 tail → Kafka)
- `EVENT_PUBLISHER_LOOKBACK_SECONDS` (default: `600`): bounded rescan window to reduce missed late arrivals (still at-least-once).
- `EVENT_PUBLISHER_LOOKBACK_MAX_KEYS` (default: `50`): max number of lookback keys sampled per batch.
- `EVENT_PUBLISHER_DEDUP_MAX_EVENTS` (default: `10000`): in-memory LRU dedup window (best-effort).
- `EVENT_PUBLISHER_DEDUP_CHECKPOINT_MAX_EVENTS` (default: `2000`): persist a bounded recent-event-id list into the checkpoint (best-effort).
- `EVENT_PUBLISHER_BATCH_SIZE` (default: `200`): batch size for index-key scanning / publish loop.
- `EVENT_PUBLISHER_KAFKA_FLUSH_BATCH_SIZE` (default: `EVENT_PUBLISHER_BATCH_SIZE`): flush after N produces.
- `EVENT_PUBLISHER_KAFKA_FLUSH_TIMEOUT_SECONDS` (default: `10`): flush timeout.
- `EVENT_PUBLISHER_METRICS_LOG_INTERVAL_SECONDS` (default: `30`): periodic metrics log interval.

### Event Store write-side sequencing
- `EVENT_STORE_SEQUENCE_ALLOCATOR_MODE` (default: `postgres`): `postgres` (atomic), `legacy`/`s3` (best-effort), `off` (require caller).
- `EVENT_STORE_SEQUENCE_SCHEMA` (default: `spice_event_registry`): Postgres schema for the allocator table.
- `EVENT_STORE_SEQUENCE_HANDLER_PREFIX` (default: `write_side`): handler prefix namespace for allocator rows.
