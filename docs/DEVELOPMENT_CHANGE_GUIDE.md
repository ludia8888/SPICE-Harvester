# Development Change Guide

## Goal

This guide answers the practical question:

- if I want to add or change feature X, where should I actually make the change?

It is intentionally repo-specific and follows the current backend layout.

## First Principle

Start from the runtime that owns the contract, not from the biggest file you can find.

In practice:

- add HTTP contract changes in routers
- add orchestration in services
- add durable state changes in registries/stores
- add async side effects in workers/outbox flows

## If You Are Adding...

### 1. A new BFF endpoint

Start here:

- `backend/bff/routers/`

Usually also touch:

- `backend/bff/services/`
- `backend/bff/routers/registry_deps.py` or other dependency helpers
- unit tests under `backend/tests/unit/routers/` or `backend/bff/tests/`

Do not:

- put multi-step orchestration directly into the router function

### 2. A new OMS command or query surface

Start here:

- `backend/oms/routers/`

Then follow through:

- `backend/oms/services/`
- relevant worker if the command becomes async
- event models / envelopes if a new command/event type is needed

Remember:

- OMS ingress owns validation and command intent
- workers own long-running mutation execution

### 3. A pipeline control-plane feature

Start here:

- `backend/bff/services/pipeline_*`
- `backend/bff/routers/pipeline_*`

Durable state usually lives in:

- `backend/shared/services/registries/pipeline_registry.py`
- `backend/shared/services/registries/pipeline_plan_registry.py`
- `backend/shared/services/registries/dataset_registry.py`

Runtime execution usually lives in:

- `backend/pipeline_worker/`

### 4. Dataset metadata, ingest, governance, or relationship logic

Start here:

- `backend/shared/services/registries/dataset_registry_catalog.py`
- `backend/shared/services/registries/dataset_registry_ingest.py`
- `backend/shared/services/registries/dataset_registry_governance.py`
- `backend/shared/services/registries/dataset_registry_relationships.py`

Use `backend/shared/services/registries/dataset_registry.py` only as the facade/orchestrator layer.

### 5. Objectify mapping or objectification behavior

Start here:

- BFF CRUD/preflight: `backend/bff/services/objectify_*` and `backend/bff/routers/objectify_*`
- durable state: `backend/shared/services/registries/objectify_registry.py`
- runtime execution: `backend/objectify_worker/`

Common helper modules:

- `backend/objectify_worker/ontology_contracts.py`
- `backend/objectify_worker/lineage_helpers.py`
- `backend/objectify_worker/write_paths.py`

### 6. Pipeline runtime input/output behavior

Start here:

- input resolution: `backend/pipeline_worker/input_loading.py`
- output prep/materialization staging: `backend/pipeline_worker/output_preparation.py`
- orchestration: `backend/pipeline_worker/main.py`
- transform semantics: `backend/pipeline_worker/spark_transform_engine.py`

Try not to grow `backend/pipeline_worker/main.py` directly unless you are only wiring a helper in.

### 7. A new connector kind

Start here:

- adapter/runtime logic: `backend/data_connector/<kind>/service.py`
- registration/factory wiring: `backend/data_connector/adapters/factory.py`
- durable source/mapping state: `backend/shared/services/registries/connector_registry.py`
- ingest/runtime path: `backend/connector_sync_worker/main.py`
- trigger/poll semantics: `backend/connector_trigger_service/main.py`

### 8. Agent planning/tool/session behavior

Start here:

- runtime API: `backend/agent/routers/agent.py`
- orchestration/runtime: `backend/agent/services/`
- durable state: `backend/shared/services/registries/agent_registry.py`
- session state: `backend/shared/services/registries/agent_session_registry.py`
- tool policy: `backend/shared/services/registries/agent_tool_registry.py`

### 9. Monitoring / health / readiness / status behavior

Start here:

- `backend/shared/routers/monitoring.py`
- service runtime status wiring in each `backend/*/main.py`

Do not:

- invent service-local health shapes that diverge from the shared monitoring surface

### 10. A new registry or durable store

Start here:

- `backend/shared/services/registries/` or `backend/shared/services/core/`

Also required:

- SQL migration under `backend/database/migrations/`
- runtime DDL audit consideration in `backend/scripts/runtime_ddl_audit.py`
- schema tests
- owner documentation in `docs/REGISTRY_OWNERSHIP.md`

## Before You Touch a Giant File

Treat these as facades/composition layers first:

- `backend/bff/routers/foundry_ontology_v2.py`
- `backend/shared/services/registries/dataset_registry.py`
- `backend/pipeline_worker/main.py`
- `backend/objectify_worker/main.py`

Preferred approach:

1. create or extend a sibling helper module
2. move responsibility there
3. keep the original file as thin delegation

## Change-Type Checklists

### Schema Change

Required:

1. add migration under `backend/database/migrations/`
2. preserve backward-compatible rollout rules from `docs/RELEASE_OPERATIONS_POLICY.md`
3. add/update registry schema tests
4. make sure `backend/scripts/runtime_ddl_audit.py` still passes

### Write Path Change

Required:

1. identify the authoritative write point
2. confirm post-commit failure behavior
3. add tests for pre-commit and post-commit failure windows
4. update `docs/WRITE_PATH_CONTRACTS.md` if the contract changes materially

### Dependency / Degrade Change

Required:

1. decide whether the dependency is required, optional, or feature-scoped
2. keep health/readiness/status honest
3. add a contract test for the degraded path
4. update `docs/DEPENDENCY_DEGRADE_POLICY.md` if policy meaning changes

### Worker Change

Required:

1. confirm retryability classification
2. confirm idempotency/lease behavior
3. add DLQ/retry/duplicate tests
4. update `docs/WORKER_RETRY_IDEMPOTENCY.md` if the worker family changes

## Default Development Flow

1. Find the owning runtime and registry first.
2. Prefer extending an existing helper module over growing a facade.
3. Add tests in the owner runtime first.
4. Run the smallest meaningful regression set.
5. Update the docs if you changed a contract, owner, or degrade rule.

## Quick Map

| If you need to change... | Start in... |
| --- | --- |
| frontend-facing API contract | `backend/bff/routers/` |
| ontology/instance/action ingress | `backend/oms/routers/` |
| service orchestration | `backend/bff/services/`, `backend/oms/services/`, worker helper modules |
| durable metadata / control plane | `backend/shared/services/registries/` |
| async execution / retries / DLQ | worker `main.py` + shared Kafka helpers |
| health / status / degrade | `backend/shared/routers/monitoring.py` and service `main.py` |
| migrations / release safety | `backend/database/migrations/`, `docs/RELEASE_OPERATIONS_POLICY.md` |

## When in Doubt

If you cannot tell where a change belongs, answer these two questions first:

1. Which process owns the contract?
2. Which durable store is authoritative for the state?

Those two answers usually tell you the right file family immediately.
