# Service Boundaries

## Goal

This document defines the intended runtime boundaries for the backend services and shared modules.
It exists to answer one question quickly:

- which process owns this behavior
- which module is allowed to write durable state
- where new logic should go without widening blast radius

This document is grounded in the current service entrypoints under `backend/*/main.py`,
the dependency container under `backend/shared/dependencies`, and the shared registries under
`backend/shared/services/registries`.

## Boundary Rules

The backend uses a few stable layers. New code should stay inside these boundaries.

1. Routers define HTTP contracts, auth, request parsing, and response shaping.
2. Service modules orchestrate business flows and call shared registries or upstream services.
3. Registries and stores own durable control-plane persistence in Postgres.
4. Workers consume durable work, apply side effects, and update durable worker state.
5. Shared infrastructure modules provide container wiring, storage, observability, and retry primitives.

Rules:

- Do not put new business rules directly into service entrypoints like `backend/bff/main.py` or `backend/oms/main.py`.
- Do not put orchestration logic directly into registry classes unless the logic is purely about persistence semantics.
- Do not import router-private helpers into shared modules.
- Do not make workers reach across service boundaries when a registry, event, or upstream API already exists.
- Treat `backend/shared/services/registries/*` as persistence adapters, not generic dumping grounds.

## Runtime Topology

| Runtime | Entry Point | Owns | Reads/Writes | Should Not Own |
| --- | --- | --- | --- | --- |
| `BFF` | `backend/bff/main.py` | Frontend-facing orchestration, policy enforcement, Foundry-compatible API shaping, pipeline control-plane workflows | Shared registries, OMS HTTP/gRPC, Funnel in-process app | Source-of-truth event append, projection rebuilding, worker-only side effects |
| `OMS` | `backend/oms/main.py` | Ontology/instance/action command ingress, event-sourced source-of-truth APIs, query surface | Event store, Postgres runtime, Elasticsearch query path, optional command-status Redis | Frontend-specific payload shaping, long-running derived processing |
| `Agent` | `backend/agent/main.py` | Deterministic tool execution, run/session control plane | Agent registries, event store, audit store | Domain-specific pipeline/objectify writes outside tool calls |
| `Funnel` | `backend/funnel/main.py` | Structure analysis for tabular intake, in-process helper app | Redis-backed patch store, sheet analysis services | External public API surface, durable domain state outside patch/analysis scope |
| `Action Worker` | `backend/action_worker/main.py` | Action command execution and writeback patch generation | Processed-event registry, action log registry, event store, lakeFS/storage | Public API shaping, synchronous command ingress |
| `Ontology Worker` | `backend/ontology_worker/main.py` | Ontology graph mutation execution and follow-up sync | Processed-event registry, ontology resource/deployment stores, lineage, key-spec sync | User-facing request parsing |
| `Pipeline Worker` | `backend/pipeline_worker/main.py` | Dataset transform execution, output preparation/materialization, publish/deploy flow | Pipeline/dataset/objectify registries, lineage, lakeFS, Spark | Pipeline catalog CRUD, frontend API logic |
| `Objectify Worker` | `backend/objectify_worker/main.py` | Dataset-to-instance bulk objectification, relationship extraction, objectify job lifecycle | Objectify/dataset/pipeline registries, lineage, OMS gateway, Elasticsearch | Mapping-spec CRUD, user-facing preflight |
| `Instance Worker` | `backend/instance_worker/main.py` | Instance projection/update application from event history | Processed-event registry, dataset/objectify registries, lineage, Elasticsearch | Command ingress |
| `Projection Worker` | `backend/projection_worker/main.py` | Projection refresh from event stream | Processed-event registry, lineage, Elasticsearch | Public query API |
| `Connector Trigger` | `backend/connector_trigger_service/main.py` | Poll scheduling and due-source detection | Connector registry | Dataset version writes |
| `Connector Sync` | `backend/connector_sync_worker/main.py` | Connector extraction + durable ingest handoff | Connector/dataset/pipeline/objectify registries, lineage | User-facing connector setup |
| `Message Relay` | `backend/message_relay/main.py` | Event-store to Kafka publication | Event-store bucket, Kafka, relay checkpoint | Domain writes outside relay checkpointing |
| `Scheduler` | `backend/pipeline_scheduler/main.py` | Schedule due-run detection and enqueue | Pipeline registry, queue publish | Pipeline definition mutation |

## Shared Module Boundaries

### `backend/shared/dependencies`

Owns:

- process-local service container
- FastAPI dependency providers
- singleton creation rules

Do:

- register shared infra services
- expose typed lookup and `ServiceToken`-based aliasing

Do not:

- encode domain logic
- hide dependency failures as fake business results

### `backend/shared/services/registries`

Owns:

- durable Postgres control-plane state
- optimistic concurrency / uniqueness / ordering primitives
- persistence-specific read/write helpers

Do:

- expose typed records and narrow persistence methods
- keep schema ownership aligned with `backend/database/migrations`

Do not:

- embed HTTP concerns
- call routers
- become the primary place for cross-registry orchestration

### `backend/shared/services/events`

Owns:

- outbox/reconciler patterns
- event-store append/replay helpers
- idempotency primitives

Do:

- provide at-least-once-safe building blocks

Do not:

- hide authoritative write order behind implicit side effects

### `backend/shared/services/kafka`

Owns:

- consumer runtime
- processed-event worker template
- retry, seek, heartbeat, and DLQ behavior

Do:

- centralize retry/idempotency behavior for workers

Do not:

- carry domain-specific parsing rules that belong in concrete workers

## Composition Boundaries for Large Modules

The following files are now intended to be composition layers, not the primary place for new business rules:

- `backend/bff/routers/foundry_ontology_v2.py`
- `backend/bff/services/pipeline_execution_service.py`
- `backend/shared/services/registries/dataset_registry.py`
- `backend/pipeline_worker/main.py`
- `backend/objectify_worker/main.py`

Expected pattern:

- keep route/entrypoint/facade signatures stable
- move domain logic into sibling helper modules
- leave thin delegation in the original facade file
- treat current file length as a ratchet ceiling; if a facade grows, move logic out before merging

Current ratchet ceilings:

- `backend/bff/routers/foundry_ontology_v2.py`: `3021` lines
- `backend/pipeline_worker/main.py`: `3071` lines
- `backend/objectify_worker/main.py`: `774` lines
- `backend/shared/services/registries/dataset_registry.py`: `121` lines

Examples already in place:

- `backend/bff/routers/foundry_ontology_v2_object_sets.py`
- `backend/bff/routers/foundry_ontology_v2_object_rows.py`
- `backend/bff/routers/foundry_ontology_v2_read_routes.py`
- `backend/bff/routers/foundry_ontology_v2_models.py`
- `backend/bff/services/pipeline_execution_deploy.py`
- `backend/bff/services/pipeline_execution_preview_build.py`
- `backend/shared/services/registries/dataset_registry_catalog.py`
- `backend/shared/services/registries/dataset_registry_ingest.py`
- `backend/shared/services/registries/dataset_registry_governance.py`
- `backend/shared/services/registries/dataset_registry_relationships.py`
- `backend/shared/services/registries/dataset_registry_catalog_mixin.py`
- `backend/shared/services/registries/dataset_registry_governance_mixin.py`
- `backend/shared/services/registries/dataset_registry_edits_mixin.py`
- `backend/shared/services/registries/dataset_registry_ingest_mixin.py`
- `backend/pipeline_worker/input_loading.py`
- `backend/pipeline_worker/output_preparation.py`
- `backend/pipeline_worker/runtime_mixin.py`
- `backend/objectify_worker/ontology_contracts.py`
- `backend/objectify_worker/lineage_helpers.py`
- `backend/objectify_worker/runtime_helpers.py`
- `backend/objectify_worker/runtime_mixin.py`

## Boundary Decision Guide

If you are unsure where code belongs, use this order:

1. If it changes HTTP contract or auth, put it in the router layer.
2. If it coordinates multiple steps or registries, put it in a service or worker helper.
3. If it only persists or queries durable control-plane state, put it in a registry/store.
4. If it is retry/lease/DLQ infrastructure, put it in shared Kafka/event infrastructure.
5. If it must be reused across services, prefer `backend/shared/...` over cross-importing another service's router/service internals.

## Non-Goals

These boundaries do not mean:

- every file must be tiny
- every durable read/write must move out of registries
- every worker needs a new service layer

They do mean:

- new domain logic should not grow the old giant files
- write ownership must stay explicit
- service responsibilities must stay legible to the next maintainer
