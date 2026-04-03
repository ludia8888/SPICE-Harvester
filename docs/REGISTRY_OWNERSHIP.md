# Registry Ownership

## Goal

This table answers a common maintenance question:

- which registry/store is the durable owner of this control-plane state
- which runtime is allowed to write it authoritatively
- which services mainly read from it

“Owner” here means the primary writer that defines the contract for the data, not the only reader.

## Ownership Table

| Registry / Store | Schema | Authoritative Writers | Primary Readers | Notes |
| --- | --- | --- | --- | --- |
| `DatasetRegistry` | `spice_datasets` | BFF pipeline/data-connector flows, connector ingest/runtime services, dataset ingest reconciliation | BFF graph/governance/object-type services, action/objectify/pipeline workers, OMS simulation paths | Durable dataset metadata, versions, ingest requests/outbox, key specs, access policies, relationship specs |
| `DatasetProfileRegistry` | `spice_datasets` | BFF profile/stat generation flows, MCP/reporting helpers | BFF/MCP profile readers | Derived dataset profile state; not the source of truth for dataset versions |
| `PipelineRegistry` | `spice_pipelines` | BFF pipeline catalog/execution/proposal flows, pipeline deploy/update control-plane writes | BFF pipeline/detail/history routers, pipeline/objectify/connector workers | Durable pipeline definitions, versions, dependencies, permissions, schedule metadata |
| `PipelinePlanRegistry` | `spice_pipelines` | BFF plan preview/compiler/autonomous plan flows | Agent proxy, plan preview, MCP helpers | Stores draft/plan-stage control-plane state, not executed outputs |
| `ObjectifyRegistry` | `spice_objectify` | BFF mapping-spec CRUD, connector/pipeline enqueue paths, objectify worker lifecycle updates | BFF objectify services, objectify worker, instance/pipeline workers | Owns mapping specs, objectify jobs, outbox, and objectify watermarks |
| `ConnectorRegistry` | `spice_connectors` | BFF connector registration/connectivity flows, connector trigger/sync services | BFF Foundry connectivity and ops routers, Funnel Google Sheets resolution, connector workers | Owns connector sources, mappings, sync state, and connector update outbox |
| `ActionLogRegistry` | `spice_action_logs` | OMS action ingress/services, action worker, action outbox worker | BFF action/instance views, tests and admin tooling | Durable action lifecycle and result state |
| `ProcessedEventRegistry` | `spice_event_registry` | Kafka workers via `ProcessedEventKafkaWorker` claim/done/fail flow | OMS command-status fallback, worker monitoring, lease smoke tests | Owns idempotency + ordering guard for at-least-once consumers |
| `LineageStore` | `spice_lineage` | Pipeline/objectify/instance/ontology workers, connector ingest services | BFF lineage/graph/admin replay services | Derived lineage graph; important but not the source of truth for domain state |
| `OntologyKeySpecRegistry` | service-local store | Ontology worker follow-up sync | OMS/BFF object-type and governance consumers | Keeps ontology key-spec projections in sync with ontology mutations |
| `AgentRegistry` | `spice_agent` | Agent service runtime | Agent service, BFF AI surfaces, tooling | Owns runs, steps, approvals, and tool idempotency records |
| `AgentSessionRegistry` | `spice_agent` | Agent service runtime, CI webhook/session update flows | BFF AI routers/services, retention worker | Owns session/message/tool-call/job state for long-lived agent sessions |
| `AgentPolicyRegistry` | `spice_agent` | Policy/bootstrap/admin flows | BFF auth/doc-bundle/registry dependency surfaces | Agent policy catalog |
| `AgentToolRegistry` | `spice_agent` | allowlist bootstrap/sync flows | BFF auth middleware, agent runtime, planner validation | Tool allowlist and retry/approval policy metadata |
| `AgentFunctionRegistry` | `spice_agent` | function catalog bootstrap/admin flows | agent/BFF tool discovery | Catalog of callable agent functions |
| `AgentModelRegistry` | `spice_agent` | model catalog/bootstrap flows | agent/BFF model selection flows | Catalog of allowed models and metadata |
| `AuditLogStore` | `spice_audit` | services/workers that emit audit records | operators, admin/debug paths | Supporting store for audit events, not a business source of truth |
| `OntologyDeploymentRegistry` / `OntologyDeploymentRegistryV2` | OMS-specific Postgres schema | OMS deployment/outbox flows | OMS/BFF ontology deployment consumers | Keeps deployment/version metadata for ontology rollout |

## Ownership Rules

1. The owner defines the write contract.
2. Readers may cache or derive from registry data, but they do not redefine its meaning.
3. Cross-registry orchestration belongs in service/worker layers, not inside a registry unless it is purely persistence glue.
4. Schema ownership lives in `backend/database/migrations`, not in runtime bootstrap code.

## Safe Change Rules

When changing a registry:

1. Identify the authoritative writer first.
2. Confirm whether readers depend on exact column semantics or just typed record fields.
3. Add migrations before changing runtime write behavior.
4. Update or add tests in the owner runtime first, then reader regressions.

## Practical Ownership Guide

If you need to change:

- dataset/version/governance metadata: start with `DatasetRegistry` collaborators
- pipeline definitions, versions, permissions, or schedules: start with `PipelineRegistry`
- objectify mapping specs or job lifecycle: start with `ObjectifyRegistry`
- connector source/mapping/polling state: start with `ConnectorRegistry`
- worker exactly-once/idempotency: start with `ProcessedEventRegistry`
- agent runs/sessions/tools/policies: start with the corresponding `spice_agent` registry
