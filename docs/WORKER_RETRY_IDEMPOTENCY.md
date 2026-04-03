# Worker Retry and Idempotency Rules

## Goal

Workers in this codebase are intentionally at-least-once. Correctness comes from durable guards,
not from assuming a message is delivered exactly once.

This document defines the primitives that make that safe and the rules new workers must follow.

## Core Primitives

### `ProcessedEventKafkaWorker`

Primary implementation:

- `backend/shared/services/kafka/processed_event_worker.py`

What it provides:

- durable `claim -> process -> mark_done/mark_failed`
- lease heartbeat while work is running
- retry/backoff
- DLQ publish hooks
- aggregate ordering through `RegistryKey(aggregate_id, sequence_number)`

Use this base when:

- the worker consumes Kafka
- the worker performs non-trivial side effects
- duplicate delivery would otherwise be unsafe

### `ProcessedEventRegistry`

Primary implementation:

- `backend/shared/services/registries/processed_event_registry.py`

Contract:

- idempotency key is `(handler, event_id)`
- ordering key is `(handler, aggregate_id, sequence_number)` when provided
- `CLAIMED` means the worker may perform the side effect
- `DUPLICATE_DONE`, `STALE`, and `IN_PROGRESS` mean the worker must not apply the effect again

### Redis Idempotency

Primary implementation:

- `backend/shared/services/events/idempotency_service.py`

Use this when:

- the flow is not a Kafka processed-event worker
- you need explicit request-level dedupe keyed by caller-supplied idempotency keys

### Outbox / Reconciler Patterns

Primary implementations:

- `backend/shared/services/events/dataset_ingest_outbox.py`
- `backend/shared/services/events/objectify_outbox.py`
- `backend/shared/services/events/dataset_ingest_reconciler.py`
- `backend/shared/services/events/objectify_reconciler.py`

Use this when:

- an authoritative durable write needs a derived async publish
- the publish can fail after the write commits

## Current Worker Families

| Worker | Primary File | Guard Style | Notes |
| --- | --- | --- | --- |
| Action worker | `backend/action_worker/main.py` | `ProcessedEventKafkaWorker` | Action command execution + writeback |
| Ontology worker | `backend/ontology_worker/main.py` | `ProcessedEventKafkaWorker` | Ontology command processing with DLQ/retry |
| Pipeline worker | `backend/pipeline_worker/main.py` | `ProcessedEventKafkaWorker` | Long-running dataset transform runtime |
| Objectify worker | `backend/objectify_worker/main.py` | `ProcessedEventKafkaWorker` | Mapping-spec driven bulk objectification |
| Instance worker | `backend/instance_worker/main.py` | `ProcessedEventKafkaWorker` | Instance projection/write updates |
| Projection worker | `backend/projection_worker/main.py` | `ProcessedEventKafkaWorker` | Read-model projection maintenance |
| Connector sync worker | `backend/connector_sync_worker/main.py` | `ProcessedEventKafkaWorker` | Connector update -> dataset ingest flow |
| Action outbox worker | `backend/action_outbox_worker/main.py` | processed-event + outbox semantics | Derived publication flow |
| Message relay | `backend/message_relay/main.py` | checkpoint + dedup window | At-least-once publisher from event-store indexes |
| Connector trigger service | `backend/connector_trigger_service/main.py` | schedule/poll state | Not a processed-event worker; relies on sync state/backoff |
| Pipeline scheduler | `backend/pipeline_scheduler/main.py` | schedule tick state | Not a processed-event worker; due-run detection must be idempotent |

## Retry Rules

### Retryable Errors

Retry only when the error is plausibly transient.

Typical retryable classes:

- upstream unavailable
- temporary lock / lease conflict
- network timeout
- storage/transient Kafka/Redis/Postgres outage

Typical non-retryable classes:

- validation error
- malformed payload
- schema/contract mismatch
- explicit business rejection

The concrete classifier often lives in the worker itself via:

- `_is_retryable_error(...)`
- retry profiles in `backend/shared/services/kafka/retry_classifier.py`

### Backoff Rules

Workers should use bounded backoff, not tight loops.

Common fields:

- `max_retry_attempts`
- `backoff_base`
- `backoff_max`
- DLQ topic and contextual failure payload

### Heartbeat Rules

If work can outlive the lease timeout:

- heartbeat must be enabled
- the worker must not assume exclusive ownership without renewing the lease

This is especially important for:

- `pipeline_worker`
- `objectify_worker`
- other long-running workers using remote storage or Spark

## Idempotency Rules

1. A worker may perform the domain side effect only after `CLAIMED`.
2. A worker must call `mark_done` only after the side effect commits.
3. A worker must not treat `mark_done` itself as the authoritative write.
4. If a side effect can succeed before a later bookkeeping step fails, the retry path must converge onto the already-committed state.
5. Derived publishes should use deterministic ids, dedupe keys, or durable outbox rows.

Examples already present in the codebase:

- pipeline job ids include idempotency inputs in `backend/shared/models/pipeline_job.py`
- agent tool calls persist idempotency records in `AgentRegistry`
- connector/objectify outbox flows use durable publish attempts + next-attempt timestamps

## DLQ Rules

Every worker with retry exhaustion should publish enough context to diagnose or replay the failure.

Minimum DLQ context:

- event/message id
- handler name
- attempt count
- stage of failure
- normalized error message
- enough payload metadata to identify the original job or command

If DLQ publish itself fails:

- log it loudly
- do not pretend the original message succeeded

## Adding a New Worker

Use this checklist.

1. Decide whether it is Kafka/event-driven.
2. If yes, start with `ProcessedEventKafkaWorker` unless you have a very strong reason not to.
3. Define the `RegistryKey` carefully.
   - include `aggregate_id` and `sequence_number` if ordering matters
4. Implement `_is_retryable_error(...)` explicitly.
5. Decide what the authoritative write is.
6. Mark done only after that write commits.
7. Add tests for:
   - duplicate delivery
   - retryable failure
   - non-retryable failure
   - lease heartbeat on long-running work
   - DLQ after max retries

## Review Checklist

Before merging worker changes, confirm:

- what prevents duplicate side effects
- what keeps ordering stable
- what extends the lease during long work
- what happens if the durable write succeeds and cleanup fails
- what lands in DLQ after retries are exhausted
