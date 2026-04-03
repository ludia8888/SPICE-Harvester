# Dependency Degrade Policy

## Goal

The backend should degrade explicitly, not silently.

This document defines how services report dependency problems and when they are allowed to keep
serving traffic in a degraded mode.

The shared language is:

- `ready`: safe to receive normal traffic
- `degraded`: service is up, but one or more optional or feature-scoped dependencies are impaired
- `hard_down`: service should not receive traffic for the affected surface

These states are surfaced through:

- `/health`
- `/health/detailed`
- `/health/readiness`
- `/status`

via `backend/shared/routers/monitoring.py`.

## Core Rules

1. Required dependencies fail closed.
2. Optional dependencies may degrade only if the affected feature can respond honestly.
3. Feature-scoped dependencies may leave the process up, but the feature must return explicit `503`/`502`, not fake empty/success results.
4. Internal code bugs are not “degrade”; they are `internal` failures.
5. Health/status endpoints must reflect degrade state consistently.

## Dependency Classes

| Class | Meaning | Expected Behavior |
| --- | --- | --- |
| Required process dependency | The service cannot do its core job without it | startup fails or readiness becomes `hard_down` |
| Optional process dependency | Service can still serve core traffic without it | process stays up, readiness may stay `ready`, status shows `degraded` |
| Feature-scoped required dependency | Only some routes/features need it | process may stay up, affected feature returns explicit unavailable response |

## Service Matrix

### BFF

Primary entrypoint:

- `backend/bff/main.py`

Core role:

- frontend-facing orchestration over registries, OMS, Foundry compatibility, and pipeline control plane

| Dependency | Class | Policy |
| --- | --- | --- |
| service container | required process dependency | if unavailable, readiness is `hard_down` |
| core registries used by a specific route | feature-scoped required dependency | affected route fails explicitly; do not fake empty results |
| OMS upstream | feature-scoped required dependency | OMS-backed routes return upstream unavailable; registry-only routes may still work |
| Redis-backed websocket / temp object set helpers | feature-scoped required dependency | affected feature returns `503`; process may stay up |
| rate limiter | optional process dependency | service may stay up but status should expose degrade |

### OMS

Primary entrypoint:

- `backend/oms/main.py`

Core role:

- ontology and instance source-of-truth ingress plus query surface

| Dependency | Class | Policy |
| --- | --- | --- |
| event store | required process dependency | startup/readiness fail closed |
| Postgres runtime state | required process dependency | startup/readiness fail closed |
| Redis command-status service | optional / feature-scoped | command-status tracking may degrade; command writes must remain honest |
| Elasticsearch query surface | feature-scoped required dependency | query/index-backed endpoints fail explicitly; do not return fake empty data |
| rate limiter | optional process dependency | status shows degraded when unavailable |

### Agent

Primary entrypoint:

- `backend/agent/main.py`

Core role:

- deterministic tool execution and session/run control plane

| Dependency | Class | Policy |
| --- | --- | --- |
| event store | required or optional depending on `agent.require_event_store` | if required, fail closed; otherwise start degraded and expose status |
| `AgentRegistry` / `AgentSessionRegistry` | required for run/session features | fail feature or readiness honestly; do not create phantom sessions/runs |
| audit store | optional process dependency | degraded allowed; tool execution must not pretend audit succeeded |
| rate limiter | optional process dependency | status surfaces degrade |

### Funnel

Primary entrypoint:

- `backend/funnel/main.py`

Core role:

- structure analysis helper app called by BFF

| Dependency | Class | Policy |
| --- | --- | --- |
| Redis-backed structure patch store | feature-scoped required dependency | patch-dependent flows return explicit unavailable; service may still analyze structure |
| rate limiter | optional process dependency | degrade only |

### Workers

Workers are allowed to keep running only when they can preserve correctness.

| Dependency | Class | Policy |
| --- | --- | --- |
| `ProcessedEventRegistry` for Kafka workers | required | fail worker startup/processing if unavailable |
| lineage store | optional unless `lineage_required_effective` | if optional, keep processing and log degrade; if required, fail closed |
| DLQ producer | optional but strongly recommended | terminal failures must still be surfaced; do not silently claim successful recovery |
| lakeFS / storage / external source | feature-scoped required dependency | current job/message should retry or fail honestly, not convert into fake success |

## Honest Degrade vs Silent Fallback

Allowed:

- readiness remains up while a non-core auxiliary feature is degraded
- feature returns `503` with a clear upstream/dependency error
- health/status expose `issues`, `dependency_status`, and `background_tasks`

Not allowed:

- returning `[]`, `["main"]`, or zero counts for dependency failures
- returning `200` with fake default data when the real dependency is unavailable
- swallowing container/registration bugs as optional degrade

## Response and Logging Policy

When a dependency is down:

- classify it as `unavailable`
- return `503` or `502` as appropriate
- preserve a machine-readable error envelope
- add service/runtime issue entries where the process remains up

When the problem is a local bug:

- classify it as `internal`
- do not rewrite it as dependency degrade

## Review Checklist

Before merging a dependency-related change, confirm:

1. Is this dependency required for the whole process or only one feature?
2. If it fails, does the process stay honest?
3. Does health/readiness/status reflect the impairment?
4. Are clients seeing explicit unavailable responses instead of fake empty/default data?
5. Are logs distinguishing `unavailable` from `internal`?
