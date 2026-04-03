# Write Path Contracts

## Goal

This document defines the write-order contract for the backend.

The core rule is simple:

1. validate and authorize
2. acquire idempotency / lease / OCC guard
3. perform the authoritative durable write
4. run derived side effects
5. update audit / status / notifications
6. leave a recovery path if step 4 or 5 fails

The system should not report success before the authoritative write, and it should not report false
failure after the authoritative write already committed.

## Global Contract

### Phase 1: Validation and Authorization

This phase must be side-effect free.

Allowed work:

- request validation
- role and permission checks
- existence checks
- schema / contract validation
- idempotency-key validation

Not allowed:

- queue publish
- event append
- registry mutation
- audit log writes that imply success

### Phase 2: Concurrency / Idempotency Guard

This phase reserves the right to process exactly once or in the correct order.

Examples:

- `ProcessedEventRegistry.claim(...)`
- Redis-backed idempotency keys
- optimistic concurrency version checks
- advisory locks for per-aggregate ordering

This phase may return:

- duplicate already done
- in progress
- stale / out-of-order
- claimed

### Phase 3: Authoritative Durable Write

This is the point after which the request or message is considered committed.

Examples by domain:

- append source-of-truth event to event store
- commit a pipeline definition/version row in `PipelineRegistry`
- persist an ontology resource mutation
- insert a dataset version / objectify job / connector sync state row

Rules:

- do not emit derived notifications before this step succeeds
- do not mask failures in this step as success
- once this step succeeds, later failure handling must preserve the committed state

### Phase 4: Derived Side Effects

These steps follow the authoritative write and must be retryable or reconstructable.

Examples:

- Kafka publish
- lineage edge writes
- best-effort permission grants
- enqueue outbox records
- writeback/materialization follow-ups
- metrics and non-authoritative status markers

If these fail after the authoritative write:

- do not return a false “nothing happened” response
- persist enough state for retry or reconciliation
- log and classify clearly

### Canonical Post-Commit Contract Shape

Code, logs, and tests should use the same post-commit write contract vocabulary.

```json
{
  "authoritative_state": "committed",
  "authoritative_write": "domain_specific_commit_name",
  "derived_side_effects": [
    {
      "name": "side_effect_name",
      "status": "completed"
    },
    {
      "name": "side_effect_name",
      "status": "degraded",
      "error": "dependency unavailable"
    },
    {
      "name": "side_effect_name",
      "status": "skipped",
      "details": {
        "reason": "feature_disabled"
      }
    }
  ]
}
```

Allowed vocabulary:

- authoritative state: `committed`
- derived side effect status: `completed`, `degraded`, `skipped`

Every write path that crosses an authoritative commit boundary should expose this same shape in the
most natural surface for that path:

- persisted run metadata
- audit metadata
- operator-facing success logs
- regression tests

### Phase 5: Audit / Status / Notification

Audit/status updates happen after authoritative state exists.

Examples:

- command-status updates
- action/worker status updates
- websocket notifications
- operator-facing audit records

These should describe the committed state, not speculate about it.

## Error Classification

Write paths should classify failures as follows.

| Class | Meaning | Typical Response |
| --- | --- | --- |
| `validation` | request/payload/contract is wrong and retry will not converge | `4xx` / no worker retry |
| `unavailable` | dependency outage or temporary upstream failure | `503`/`502` / retryable |
| `conflict` | OCC/idempotency/version mismatch | `409` |
| `internal` | local bug or unexpected invariant break | `500` / page operator |

Rules:

- do not collapse `internal` into fake `unavailable`
- do not turn `unavailable` into fake empty/success payloads
- do not return `500` when the durable write already committed and the correct action is reconciliation

## Current Domain Contracts

### Pipeline Control Plane

Primary files:

- `backend/bff/services/pipeline_catalog_service.py`
- `backend/bff/services/pipeline_execution_service.py`
- `backend/bff/services/pipeline_execution_deploy.py`
- `backend/shared/services/registries/pipeline_registry.py`

Contract:

1. validate request, idempotency key, permissions, and dependency payload
2. create or update authoritative pipeline/pipeline-version rows in `PipelineRegistry`
3. run derived steps such as permissions, dependency replacement, proposal metadata, and audit
4. if derived steps fail after the authoritative row exists, keep the authoritative state and recover forward

Implication:

- permission grants and similar follow-ups are best-effort
- the API must not claim “pipeline was not created” after the row already exists

### OMS Action Submit / Apply

Primary files:

- `backend/oms/routers/action_async.py`
- `backend/oms/services/action_submit_service.py`
- `backend/action_worker/main.py`
- `backend/shared/services/registries/action_log_registry.py`

Contract:

1. validate action payload, role, dataset access, and runtime contracts
2. create only the action-log rows that correspond to durable command intent
3. append root/per-object command events to the event store as the authoritative commit point
4. only clean up non-emitted rows if a batch fails before command emission completes
5. let worker-side writeback, lineage, and notifications be derived side effects

Implication:

- emitted commands are not retroactively treated as “never happened”
- batch failure handling must distinguish emitted from non-emitted work

### Ontology Graph Mutations

Primary files:

- `backend/ontology_worker/main.py`
- `backend/oms/services/ontology_resources.py`
- `backend/shared/services/registries/ontology_key_spec_registry.py`

Contract:

1. validate mutation and command intent
2. persist ontology resource change as authoritative state
3. publish success event immediately after the authoritative mutation
4. run key-spec sync, lineage, and command completion as derived follow-ups

Implication:

- follow-up sync failure must not rewrite a committed ontology mutation into “did not happen”
- reconciliation is preferred over false failure

### Connector Ingest

Primary files:

- `backend/connector_sync_worker/main.py`
- `backend/shared/services/core/connector_ingest_service.py`
- `backend/shared/services/registries/connector_registry.py`
- `backend/shared/services/registries/dataset_registry.py`

Contract:

1. claim connector update event
2. resolve source runtime credentials and extract rows
3. persist authoritative dataset/version ingest result
4. persist connector sync state and enqueue objectify/lineage follow-ups

Implication:

- if ingest succeeded but sync-state persistence fails, retry must be idempotent against the existing durable ingest result

### Worker Projection / Objectify / Pipeline Runtime

Primary files:

- `backend/pipeline_worker/main.py`
- `backend/objectify_worker/main.py`
- `backend/instance_worker/main.py`
- `backend/projection_worker/main.py`

Contract:

1. claim message via `ProcessedEventRegistry`
2. do the domain write once
3. mark processed-event done only after the domain write commits
4. route transient failure into retry/backoff and permanent failure into DLQ or terminal status

Implication:

- processed-event completion is never the source of truth
- it is the guard around a separate source-of-truth write

## Testing Requirements

Every new write path or major write-path change should have these tests.

1. Validation failure before authoritative write
2. Dependency failure before authoritative write
3. Failure immediately after authoritative write but before a derived side effect completes
4. Retry / idempotent resume against an already-committed write
5. Conflict / OCC handling

## Review Checklist

Before merging a write-path change, confirm all of the following.

- Which line of code is the authoritative durable write?
- What state exists if the next statement throws?
- Which follow-ups are retryable or reconstructable?
- Will the API/worker report false failure or false success in that case?
- Does the test suite cover the post-commit failure window?
