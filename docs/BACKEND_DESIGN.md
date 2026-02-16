# Backend Design Reference

> Generated: 2026-02-16T15:26:32+09:00
> Scope: backend/**/*.py (including scripts and tests, excluding __pycache__)
> Source: AST + docstring extraction (module/class/function) via `scripts/generate_backend_methods.py`.

## Coverage Summary

- Modules scanned: **983**
- Modules with module docstring: **558/983**
- Modules with broad `except Exception`: **281**
- Modules with bare `except:`: **0**
- Modules with `return` inside `finally`: **0**
- Total code lines (non-empty, non-comment): **225338**

## Package Scoreboard

| Package | Modules | Module Doc Coverage | Broad-Except Modules | Broad Except Count | Async Functions | Public API | Code Lines |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `action_outbox_worker` | 2 | 2/2 (100%) | 1 | 7 | 9 | 2 | 378 |
| `action_worker` | 2 | 2/2 (100%) | 1 | 15 | 42 | 2 | 2589 |
| `agent` | 10 | 4/10 (40%) | 3 | 16 | 16 | 16 | 2771 |
| `analysis` | 1 | 1/1 (100%) | 0 | 0 | 3 | 2 | 334 |
| `bff` | 250 | 194/250 (77%) | 90 | 433 | 1037 | 750 | 50064 |
| `conftest.py` | 1 | 0/1 (0%) | 0 | 0 | 0 | 0 | 65 |
| `connector_sync_worker` | 2 | 2/2 (100%) | 1 | 5 | 13 | 1 | 456 |
| `connector_trigger_service` | 2 | 2/2 (100%) | 1 | 9 | 8 | 1 | 305 |
| `data_connector` | 6 | 4/6 (66%) | 1 | 1 | 12 | 22 | 710 |
| `examples` | 1 | 1/1 (100%) | 1 | 2 | 0 | 2 | 115 |
| `funnel` | 22 | 16/22 (72%) | 4 | 35 | 45 | 47 | 6280 |
| `ingest_reconciler_worker` | 2 | 2/2 (100%) | 1 | 3 | 5 | 3 | 216 |
| `instance_worker` | 2 | 2/2 (100%) | 1 | 35 | 27 | 2 | 2287 |
| `mcp_servers` | 18 | 6/18 (33%) | 11 | 56 | 135 | 36 | 7114 |
| `message_relay` | 2 | 1/2 (50%) | 1 | 13 | 10 | 2 | 664 |
| `monitoring` | 1 | 1/1 (100%) | 1 | 5 | 9 | 2 | 305 |
| `objectify_worker` | 4 | 4/4 (100%) | 2 | 26 | 45 | 7 | 4304 |
| `oms` | 49 | 37/49 (75%) | 19 | 105 | 158 | 193 | 14543 |
| `ontology_worker` | 2 | 1/2 (50%) | 1 | 28 | 15 | 1 | 1190 |
| `perf` | 1 | 1/1 (100%) | 1 | 1 | 3 | 1 | 106 |
| `pipeline_scheduler` | 1 | 1/1 (100%) | 0 | 0 | 1 | 1 | 26 |
| `pipeline_worker` | 5 | 5/5 (100%) | 3 | 50 | 44 | 3 | 6453 |
| `projection_worker` | 2 | 1/2 (50%) | 1 | 26 | 26 | 1 | 1793 |
| `scripts` | 20 | 19/20 (95%) | 12 | 23 | 20 | 41 | 2382 |
| `shared` | 298 | 202/298 (67%) | 103 | 374 | 851 | 1129 | 79062 |
| `tests` | 275 | 45/275 (16%) | 20 | 51 | 1202 | 1226 | 40533 |
| `writeback_materializer_worker` | 2 | 2/2 (100%) | 1 | 4 | 8 | 2 | 293 |

## Engineering Hotspots

| Module | Risk Score | Broad Except | Bare Except | Finally Return | Try | Raise | Code Lines |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `backend/bff/routers/foundry_ontology_v2.py` | 294 | 53 | 0 | 0 | 62 | 33 | 3295 |
| `backend/pipeline_worker/main.py` | 235 | 47 | 0 | 0 | 62 | 81 | 5250 |
| `backend/bff/main.py` | 214 | 40 | 0 | 0 | 42 | 28 | 832 |
| `backend/instance_worker/main.py` | 175 | 35 | 0 | 0 | 42 | 69 | 2283 |
| `backend/shared/observability/tracing.py` | 174 | 29 | 0 | 0 | 30 | 1 | 516 |
| `backend/funnel/services/structure_analysis.py` | 144 | 24 | 0 | 0 | 24 | 0 | 2644 |
| `backend/ontology_worker/main.py` | 142 | 28 | 0 | 0 | 30 | 28 | 1190 |
| `backend/shared/observability/metrics.py` | 140 | 23 | 0 | 0 | 26 | 1 | 884 |
| `backend/projection_worker/main.py` | 131 | 26 | 0 | 0 | 29 | 28 | 1793 |
| `backend/shared/services/kafka/processed_event_worker.py` | 123 | 22 | 0 | 0 | 29 | 16 | 1391 |
| `backend/objectify_worker/main.py` | 120 | 24 | 0 | 0 | 32 | 36 | 3815 |
| `backend/bff/services/oms_client.py` | 110 | 22 | 0 | 0 | 23 | 24 | 526 |
| `backend/shared/services/storage/event_store.py` | 108 | 20 | 0 | 0 | 24 | 16 | 1048 |
| `backend/bff/services/ai_service.py` | 105 | 21 | 0 | 0 | 25 | 25 | 1571 |
| `backend/bff/services/graph_query_service.py` | 88 | 17 | 0 | 0 | 18 | 15 | 877 |

## Entrypoint Risk Map

| Entrypoint | Async Functions | Broad Except | Try | Raise | Code Lines |
| --- | --- | --- | --- | --- | --- |
| `backend/action_outbox_worker/main.py` | 9 | 7 | 9 | 9 | 377 |
| `backend/action_worker/main.py` | 42 | 15 | 26 | 51 | 2588 |
| `backend/agent/main.py` | 1 | 5 | 5 | 1 | 80 |
| `backend/bff/main.py` | 33 | 40 | 42 | 28 | 832 |
| `backend/connector_sync_worker/main.py` | 13 | 5 | 5 | 11 | 455 |
| `backend/connector_trigger_service/main.py` | 8 | 9 | 10 | 3 | 304 |
| `backend/funnel/main.py` | 3 | 1 | 1 | 0 | 61 |
| `backend/ingest_reconciler_worker/main.py` | 5 | 3 | 5 | 1 | 215 |
| `backend/instance_worker/main.py` | 27 | 35 | 42 | 69 | 2283 |
| `backend/message_relay/main.py` | 10 | 13 | 22 | 10 | 664 |
| `backend/objectify_worker/main.py` | 38 | 24 | 32 | 36 | 3815 |
| `backend/oms/main.py` | 15 | 13 | 13 | 5 | 457 |
| `backend/ontology_worker/main.py` | 15 | 28 | 30 | 28 | 1190 |
| `backend/pipeline_scheduler/main.py` | 1 | 0 | 0 | 0 | 26 |
| `backend/pipeline_worker/main.py` | 44 | 47 | 62 | 81 | 5250 |
| `backend/projection_worker/main.py` | 26 | 26 | 29 | 28 | 1793 |
| `backend/writeback_materializer_worker/main.py` | 8 | 4 | 6 | 4 | 292 |

## New Developer Read Order (First 60-90 Minutes)

> [!TIP]
> Start with lifecycle entrypoints, then API routers, then domain services and storage adapters.

| Priority | Module | Why First |
| --- | --- | --- |
| 1 | `backend/bff/routers/foundry_ontology_v2.py` | API contract surface, Foundry v2 compatibility, ontology model contract |
| 2 | `backend/ontology_worker/main.py` | entrypoint lifecycle, ontology model contract |
| 3 | `backend/oms/routers/ontology.py` | API contract surface, ontology model contract, high-impact module size |
| 4 | `backend/pipeline_worker/main.py` | entrypoint lifecycle, high-impact module size |
| 5 | `backend/instance_worker/main.py` | entrypoint lifecycle, high-impact module size |
| 6 | `backend/projection_worker/main.py` | entrypoint lifecycle, high-impact module size |
| 7 | `backend/objectify_worker/main.py` | entrypoint lifecycle, high-impact module size |
| 8 | `backend/action_worker/main.py` | entrypoint lifecycle, high-impact module size |
| 9 | `backend/shared/services/registries/ontology_key_spec_registry.py` | domain/service orchestration, storage adapter, ontology model contract |
| 10 | `backend/oms/routers/ontology_extensions.py` | API contract surface, ontology model contract |
| 11 | `backend/bff/routers/ontology_agent.py` | API contract surface, ontology model contract |
| 12 | `backend/bff/routers/ontology_metadata.py` | API contract surface, ontology model contract |
| 13 | `backend/bff/routers/ontology_extensions.py` | API contract surface, ontology model contract |
| 14 | `backend/bff/routers/ontology_imports.py` | API contract surface, ontology model contract |
| 15 | `backend/bff/routers/ontology_suggestions.py` | API contract surface, ontology model contract |
| 16 | `backend/bff/routers/ontology_crud.py` | API contract surface, ontology model contract |
| 17 | `backend/bff/routers/ontology_ops.py` | API contract surface, ontology model contract |
| 18 | `backend/bff/routers/ontology.py` | API contract surface, ontology model contract |
| 19 | `backend/bff/main.py` | entrypoint lifecycle |
| 20 | `backend/message_relay/main.py` | entrypoint lifecycle |

## action_outbox_worker

### `backend/action_outbox_worker/__init__.py`
- Module summary: Action outbox/reconciler worker (P0 writeback recovery).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=2 | code_lines=1 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/action_outbox_worker/main.py`
- Module summary: Action Outbox Worker (reconciler).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service entrypoint and lifecycle wiring
- Source footprint: total_lines=432 | code_lines=377 | risk_score=35
- API surface: public=2 | top-level functions=2 | classes=1 | methods=9
- Runtime signals: async_functions=9 | try=9 | raise=9 | broad_except=7 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/1 (0%) | methods=0/9 (0%)
- Internal imports (19): shared.config.app_config; shared.config.settings; shared.models.event_envelope; shared.models.events; shared.observability.context_propagation; shared.observability.metrics; shared.observability.tracing; shared.services.registries.action_log_registry (+11 more)
- External imports (5): __future__; asyncio; logging; time; typing
- Public API names: ActionOutboxWorker; main

## action_worker

### `backend/action_worker/__init__.py`
- Module summary: Action worker package (async writeback execution).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=2 | code_lines=1 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/action_worker/main.py`
- Module summary: Action Worker Service
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service entrypoint and lifecycle wiring
- Source footprint: total_lines=2781 | code_lines=2588 | risk_score=75
- API surface: public=2 | top-level functions=1 | classes=4 | methods=58
- Runtime signals: async_functions=42 | try=26 | raise=51 | broad_except=15 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=1/4 (25%) | methods=1/58 (1%)
- Internal imports (45): oms.services.ontology_resources; shared.config.app_config; shared.config.settings; shared.errors.enterprise_catalog; shared.errors.error_types; shared.models.commands; shared.models.event_envelope; shared.models.events (+37 more)
- External imports (8): __future__; asyncio; collections; dataclasses; json; logging; typing; uuid
- Public API names: ActionWorker; main

## agent

### `backend/agent/__init__.py`
- Module summary: Agent service package.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=1 | code_lines=1 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/agent/main.py`
- Module summary: Agent Service - deterministic tool runner (single sequential loop).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service entrypoint and lifecycle wiring
- Source footprint: total_lines=99 | code_lines=80 | risk_score=29
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=5 | raise=1 | broad_except=5 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (9): agent.routers.agent; shared.config.settings; shared.middleware.rate_limiter; shared.services.core.audit_log_store; shared.services.core.service_factory; shared.services.registries.agent_registry; shared.services.registries.agent_session_registry; shared.services.storage.event_store (+1 more)
- External imports (2): contextlib; fastapi
- Public API names: lifespan

### `backend/agent/models.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=51 | code_lines=41 | risk_score=0
- API surface: public=4 | top-level functions=0 | classes=4 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/4 (0%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (4): __future__; datetime; pydantic; typing
- Public API names: AgentRunRequest; AgentRunResponse; AgentRunSummary; AgentToolCall

### `backend/agent/models_pipeline.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=10 | code_lines=8 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.models.pipeline_agent; shared.models.pipeline_plan
- External imports (1): __future__
- Public API names: not documented

### `backend/agent/routers/__init__.py`
- Module summary: Agent routers package.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=1 | code_lines=1 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/agent/routers/agent.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=444 | code_lines=399 | risk_score=20
- API surface: public=3 | top-level functions=14 | classes=0 | methods=0
- Runtime signals: async_functions=5 | try=4 | raise=4 | broad_except=4 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/14 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (9): agent.models; agent.services.agent_run_loop; agent.services.agent_runtime; shared.config.settings; shared.errors.error_types; shared.models.responses; shared.security.principal_utils; shared.services.registries.agent_registry (+1 more)
- External imports (7): __future__; asyncio; datetime; fastapi; logging; typing; uuid
- Public API names: create_agent_run; get_agent_run; list_agent_run_events

### `backend/agent/services/__init__.py`
- Module summary: Agent service utilities.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=1 | code_lines=1 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/agent/services/agent_policy.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=295 | code_lines=259 | risk_score=1
- API surface: public=4 | top-level functions=9 | classes=1 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/9 (0%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (1): agent.models
- External imports (4): __future__; dataclasses; hashlib; typing
- Public API names: AgentPolicyDecision; compute_backoff_s; compute_retry_delay_s; decide_policy

### `backend/agent/services/agent_run_loop.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=270 | code_lines=238 | risk_score=4
- API surface: public=2 | top-level functions=2 | classes=1 | methods=0
- Runtime signals: async_functions=3 | try=4 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=2/2 (100%) | classes=1/1 (100%) | methods=0/0 (n/a)
- Internal imports (3): agent.models; agent.services.agent_policy; agent.services.agent_runtime
- External imports (3): __future__; asyncio; typing
- Public API names: AgentState; run_agent_steps

### `backend/agent/services/agent_runtime.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=1920 | code_lines=1743 | risk_score=35
- API surface: public=2 | top-level functions=39 | classes=2 | methods=17
- Runtime signals: async_functions=7 | try=12 | raise=18 | broad_except=7 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=5/39 (12%) | classes=0/2 (0%) | methods=0/17 (0%)
- Internal imports (7): agent.models; shared.config.settings; shared.errors.error_types; shared.models.event_envelope; shared.services.core.audit_log_store; shared.services.storage.event_store; shared.utils.llm_safety
- External imports (14): __future__; aiohttp; asyncio; dataclasses; datetime; email; httpx; json (+6 more)
- Public API names: AgentRuntime; AgentRuntimeConfig

## analysis

### `backend/analysis/system_improvement_analysis.py`
- Module summary: System Improvement Analysis using Context7 MCP
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=377 | code_lines=334 | risk_score=0
- API surface: public=2 | top-level functions=1 | classes=1 | methods=3
- Runtime signals: async_functions=3 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=1/1 (100%) | methods=3/3 (100%)
- Internal imports (0): not documented
- External imports (4): asyncio; datetime; json; typing
- Public API names: SystemAnalyzer; main

## bff

### `backend/bff/__init__.py`
- Module summary: BFF (Backend For Frontend) package.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=5 | code_lines=4 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/bff/dependencies.py`
- Module summary: BFF Dependencies - Modernized Version
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=441 | code_lines=364 | risk_score=30
- API surface: public=4 | top-level functions=2 | classes=2 | methods=16
- Runtime signals: async_functions=14 | try=8 | raise=13 | broad_except=6 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/2 (100%) | classes=2/2 (100%) | methods=13/16 (81%)
- Internal imports (12): bff.services.oms_client; shared.config.settings; shared.dependencies; shared.dependencies.providers; shared.errors.error_envelope; shared.errors.error_types; shared.observability.request_context; shared.services.registries.action_log_registry (+4 more)
- External imports (4): fastapi; httpx; logging; typing
- Public API names: BFFDependencyProvider; FoundryQueryService; check_bff_dependencies_health; get_foundry_query_service

### `backend/bff/main.py`
- Module summary: BFF (Backend for Frontend) Service - Modernized Version
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service entrypoint and lifecycle wiring
- Source footprint: total_lines=1057 | code_lines=832 | risk_score=214
- API surface: public=15 | top-level functions=14 | classes=1 | methods=34
- Runtime signals: async_functions=33 | try=42 | raise=28 | broad_except=40 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=10/14 (71%) | classes=1/1 (100%) | methods=33/34 (97%)
- Internal imports (37): bff.middleware.auth; bff.routers; bff.services.funnel_type_inference_adapter; bff.services.oms_client; data_connector.google_sheets.service; shared.config.settings; shared.dependencies; shared.errors.error_types (+29 more)
- External imports (6): asyncio; contextlib; fastapi; httpx; logging; typing
- Public API names: BFFServiceContainer; get_agent_policy_registry; get_agent_registry; get_agent_session_registry; get_connector_registry; get_dataset_profile_registry; get_dataset_registry; get_google_sheets_service; get_label_mapper; get_objectify_registry; get_oms_client; get_pipeline_executor (+3 more)

### `backend/bff/middleware/__init__.py`
- Module summary: BFF middleware exports.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=13 | code_lines=11 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): .auth
- External imports (0): not documented
- Public API names: not documented

### `backend/bff/middleware/auth.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=1454 | code_lines=1306 | risk_score=64
- API surface: public=3 | top-level functions=36 | classes=1 | methods=0
- Runtime signals: async_functions=18 | try=28 | raise=4 | broad_except=8 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/36 (2%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (10): shared.config.settings; shared.errors.error_envelope; shared.errors.error_types; shared.observability.request_context; shared.security.auth_utils; shared.security.user_context; shared.services.registries.agent_tool_registry; shared.utils.llm_safety (+2 more)
- External imports (12): __future__; asyncio; dataclasses; datetime; fastapi; hmac; json; logging (+4 more)
- Public API names: enforce_bff_websocket_auth; ensure_bff_auth_configured; install_bff_auth_middleware

### `backend/bff/routers/__init__.py`
- Module summary: API 라우터 모듈
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=34 | code_lines=33 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/bff/routers/actions.py`
- Module summary: Actions API (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=239 | code_lines=212 | risk_score=0
- API surface: public=10 | top-level functions=10 | classes=0 | methods=0
- Runtime signals: async_functions=10 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/10 (10%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (7): bff.dependencies; bff.schemas.actions_requests; bff.services; bff.services.oms_client; shared.observability.tracing; shared.security.database_access; shared.services.registries.action_log_registry
- External imports (2): fastapi; typing
- Public API names: get_action_log; get_action_simulation; get_action_simulation_version; list_action_logs; list_action_simulation_versions; list_action_simulations; simulate_action; submit_action; submit_action_batch; undo_action

### `backend/bff/routers/admin.py`
- Module summary: Admin API router (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=21 | code_lines=15 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): bff.routers; bff.routers.admin_deps
- External imports (1): fastapi
- Public API names: not documented

### `backend/bff/routers/admin_deps.py`
- Module summary: Admin router dependency providers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=53 | code_lines=39 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): shared.config.settings; shared.errors.error_types; shared.security.auth_utils
- External imports (4): fastapi; hmac; logging; typing
- Public API names: require_admin

### `backend/bff/routers/admin_instance_rebuild.py`
- Module summary: Admin instance index rebuild endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=121 | code_lines=104 | risk_score=6
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/2 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (6): bff.dependencies; shared.dependencies.providers; shared.models.background_task; shared.observability.tracing; shared.services.core.instance_index_rebuild_service; shared.services.storage.elasticsearch_service
- External imports (4): fastapi; logging; typing; uuid
- Public API names: get_rebuild_status; rebuild_instance_index_endpoint

### `backend/bff/routers/admin_lakefs.py`
- Module summary: Admin lakeFS credential endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=55 | code_lines=41 | risk_score=0
- API surface: public=3 | top-level functions=2 | classes=1 | methods=0
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=1/1 (100%) | methods=0/0 (n/a)
- Internal imports (3): bff.routers.registry_deps; shared.observability.tracing; shared.services.registries.pipeline_registry
- External imports (3): fastapi; pydantic; typing
- Public API names: LakeFSCredentialsUpsertRequest; list_lakefs_credentials; upsert_lakefs_credentials

### `backend/bff/routers/admin_recompute_projection.py`
- Module summary: Admin projection recompute endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=101 | code_lines=85 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/3 (33%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (7): bff.dependencies; bff.schemas.admin_projection_requests; bff.services; shared.dependencies.providers; shared.middleware.rate_limiter; shared.observability.tracing; shared.services.storage.elasticsearch_service
- External imports (2): fastapi; typing
- Public API names: get_recompute_projection_result; recompute_projection; reindex_instances_endpoint

### `backend/bff/routers/admin_replay.py`
- Module summary: Admin replay endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=111 | code_lines=96 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (7): bff.dependencies; bff.schemas.admin_replay_requests; bff.services; shared.dependencies.providers; shared.models.lineage; shared.observability.tracing; shared.services.storage.storage_service
- External imports (2): fastapi; typing
- Public API names: cleanup_old_replay_results; get_replay_result; get_replay_trace; replay_instance_state

### `backend/bff/routers/admin_system.py`
- Module summary: Admin system endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=55 | code_lines=44 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.dependencies.providers; shared.observability.tracing
- External imports (3): datetime; fastapi; typing
- Public API names: get_system_health

### `backend/bff/routers/admin_task_monitor.py`
- Module summary: Admin background-task monitoring helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=40 | code_lines=29 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.models.background_task; shared.services.core.background_task_manager
- External imports (2): asyncio; logging
- Public API names: monitor_admin_task

### `backend/bff/routers/agent_proxy.py`
- Module summary: Agent router (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=221 | code_lines=192 | risk_score=10
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=4 | raise=4 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/2 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (13): bff.routers.pipeline_plans; bff.services.pipeline_agent_autonomous_loop; shared.config.settings; shared.dependencies.providers; shared.errors.error_types; shared.models.pipeline_agent; shared.models.responses; shared.observability.tracing (+5 more)
- External imports (3): fastapi; json; logging
- Public API names: create_pipeline_run; stream_pipeline_run

### `backend/bff/routers/ai.py`
- Module summary: AI API (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=111 | code_lines=101 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (10): bff.dependencies; bff.routers.registry_deps; bff.services; bff.services.oms_client; shared.dependencies.providers; shared.middleware.rate_limiter; shared.models.ai; shared.observability.tracing (+2 more)
- External imports (1): fastapi
- Public API names: ai_intent; ai_query; translate_query_plan

### `backend/bff/routers/audit.py`
- Module summary: Audit log query router for BFF.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=78 | code_lines=67 | risk_score=10
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=2 | raise=4 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (4): shared.dependencies.providers; shared.errors.error_types; shared.models.requests; shared.observability.tracing
- External imports (3): datetime; fastapi; typing
- Public API names: get_chain_head; list_audit_logs

### `backend/bff/routers/ci_webhooks.py`
- Module summary: CI integration endpoints (webhook/polling ingestion).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=140 | code_lines=120 | risk_score=10
- API surface: public=2 | top-level functions=1 | classes=1 | methods=0
- Runtime signals: async_functions=1 | try=2 | raise=2 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (7): bff.routers.registry_deps; shared.errors.error_types; shared.models.requests; shared.observability.tracing; shared.security.input_sanitizer; shared.services.registries.agent_session_registry; shared.utils.uuid_utils
- External imports (6): datetime; fastapi; logging; pydantic; typing; uuid
- Public API names: AgentSessionCIResultIngestRequest; ingest_ci_result

### `backend/bff/routers/command_status.py`
- Module summary: Command status router (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=73 | code_lines=62 | risk_score=15
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=3 | raise=5 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): bff.dependencies; bff.services.oms_client; shared.errors.error_types; shared.models.commands; shared.observability.tracing
- External imports (4): fastapi; httpx; logging; uuid
- Public API names: get_command_status

### `backend/bff/routers/context7.py`
- Module summary: Context7 integration endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=109 | code_lines=83 | risk_score=10
- API surface: public=8 | top-level functions=9 | classes=0 | methods=0
- Runtime signals: async_functions=8 | try=2 | raise=3 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/9 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (6): bff.dependencies; bff.schemas.context7_requests; bff.services; bff.services.oms_client; shared.errors.error_types; shared.observability.tracing
- External imports (3): fastapi; logging; typing
- Public API names: add_knowledge; analyze_ontology; check_context7_health; create_entity_link; get_context7_client; get_entity_context; get_ontology_suggestions; search_context7

### `backend/bff/routers/context_tools.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=175 | code_lines=144 | risk_score=5
- API surface: public=4 | top-level functions=5 | classes=2 | methods=0
- Runtime signals: async_functions=2 | try=1 | raise=4 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/2 (0%) | methods=0/0 (n/a)
- Internal imports (10): bff.dependencies; bff.routers.registry_deps; bff.services.oms_client; bff.services.pipeline_plan_tenant_service; shared.errors.error_types; shared.models.responses; shared.observability.tracing; shared.security.input_sanitizer (+2 more)
- External imports (4): fastapi; logging; pydantic; typing
- Public API names: DatasetDescribeRequest; OntologySnapshotRequest; describe_datasets; snapshot_ontology

### `backend/bff/routers/data_connector.py`
- Module summary: Data connector API (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=49 | code_lines=40 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): bff.routers; bff.routers.data_connector_deps
- External imports (1): fastapi
- Public API names: not documented

### `backend/bff/routers/data_connector_browse.py`
- Module summary: Google Sheets browsing endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=84 | code_lines=70 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (8): bff.routers.data_connector_deps; bff.routers.data_connector_ops; data_connector.google_sheets.service; shared.errors.error_types; shared.middleware.rate_limiter; shared.models.requests; shared.observability.tracing; shared.services.registries.connector_registry
- External imports (3): fastapi; logging; typing
- Public API names: list_google_sheets_spreadsheets; list_google_sheets_worksheets

### `backend/bff/routers/data_connector_connections.py`
- Module summary: Google Sheets connection endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=70 | code_lines=59 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (6): bff.routers.data_connector_deps; shared.errors.error_types; shared.middleware.rate_limiter; shared.models.requests; shared.observability.tracing; shared.services.registries.connector_registry
- External imports (4): datetime; fastapi; logging; typing
- Public API names: delete_google_sheets_connection; list_google_sheets_connections

### `backend/bff/routers/data_connector_deps.py`
- Module summary: Data connector dependency providers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=34 | code_lines=25 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/2 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (4): bff.routers.objectify_job_queue_deps; bff.routers.registry_deps; data_connector.google_sheets.service; shared.services.registries.connector_registry
- External imports (0): not documented
- Public API names: get_connector_registry; get_google_sheets_service

### `backend/bff/routers/data_connector_oauth.py`
- Module summary: Google Sheets OAuth endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=114 | code_lines=96 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=0 | raise=4 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (8): bff.routers.data_connector_deps; bff.routers.data_connector_ops; shared.errors.error_types; shared.middleware.rate_limiter; shared.models.requests; shared.observability.tracing; shared.security.input_sanitizer; shared.services.registries.connector_registry
- External imports (4): fastapi; logging; typing; uuid
- Public API names: google_sheets_oauth_callback; start_google_sheets_oauth

### `backend/bff/routers/data_connector_ops.py`
- Module summary: Data connector helpers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=103 | code_lines=81 | risk_score=5
- API surface: public=0 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=1 | raise=2 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): data_connector.google_sheets.auth; shared.errors.error_types; shared.services.registries.connector_registry
- External imports (4): fastapi; logging; typing; urllib
- Public API names: not documented

### `backend/bff/routers/data_connector_pipelining.py`
- Module summary: Google Sheets -> Pipeline Builder endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=67 | code_lines=59 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (11): bff.routers.data_connector_deps; bff.services; data_connector.google_sheets.service; shared.dependencies.providers; shared.middleware.rate_limiter; shared.observability.tracing; shared.services.events.objectify_job_queue; shared.services.registries.connector_registry (+3 more)
- External imports (2): fastapi; typing
- Public API names: start_pipelining_google_sheet

### `backend/bff/routers/data_connector_registration.py`
- Module summary: Google Sheets registration/monitoring endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=124 | code_lines=109 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=4/4 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (8): bff.routers.data_connector_deps; bff.services; data_connector.google_sheets.service; shared.dependencies.providers; shared.middleware.rate_limiter; shared.observability.tracing; shared.services.registries.connector_registry; shared.services.registries.dataset_registry
- External imports (2): fastapi; typing
- Public API names: list_registered_sheets; preview_google_sheet; register_google_sheet; unregister_google_sheet

### `backend/bff/routers/data_connector_sheet_tools.py`
- Module summary: Google Sheets extraction/preview endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=124 | code_lines=111 | risk_score=10
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=2 | raise=4 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (10): bff.routers.data_connector_deps; bff.routers.data_connector_ops; data_connector.google_sheets.service; shared.errors.error_types; shared.middleware.rate_limiter; shared.models.google_sheets; shared.models.sheet_grid; shared.observability.tracing (+2 more)
- External imports (2): fastapi; logging
- Public API names: extract_google_sheet_grid; preview_google_sheet_for_funnel

### `backend/bff/routers/database.py`
- Module summary: Database management router for BFF
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=126 | code_lines=105 | risk_score=0
- API surface: public=8 | top-level functions=8 | classes=0 | methods=0
- Runtime signals: async_functions=8 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=8/8 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (7): bff.dependencies; bff.routers.registry_deps; bff.services; bff.services.oms_client; shared.models.requests; shared.observability.tracing; shared.services.registries.dataset_registry
- External imports (2): fastapi; typing
- Public API names: create_class; create_database; delete_database; get_class; get_database; get_database_expected_seq; list_classes; list_databases

### `backend/bff/routers/document_bundles.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=142 | code_lines=120 | risk_score=15
- API surface: public=2 | top-level functions=2 | classes=1 | methods=0
- Runtime signals: async_functions=1 | try=3 | raise=4 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (8): bff.routers.context7; bff.routers.registry_deps; bff.services.pipeline_plan_tenant_service; shared.errors.error_types; shared.models.responses; shared.observability.tracing; shared.security.input_sanitizer; shared.services.registries.agent_policy_registry
- External imports (4): fastapi; logging; pydantic; typing
- Public API names: DocumentBundleSearchRequest; search_document_bundle

### `backend/bff/routers/foundry_ontology_v2.py`
- Module summary: Foundry Ontologies v2 read-compat router.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=3600 | code_lines=3295 | risk_score=294
- API surface: public=26 | top-level functions=93 | classes=2 | methods=0
- Runtime signals: async_functions=31 | try=62 | raise=33 | broad_except=53 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/93 (1%) | classes=0/2 (0%) | methods=0/0 (n/a)
- Internal imports (9): bff.dependencies; bff.routers.link_types_read; bff.routers.object_types; bff.services.oms_client; shared.config.settings; shared.observability.tracing; shared.security.database_access; shared.security.input_sanitizer (+1 more)
- External imports (6): asyncio; fastapi; httpx; logging; typing; uuid
- Public API names: OntologyNotFoundError; PermissionDeniedError; get_action_type_by_rid_v2; get_action_type_v2; get_full_metadata_v2; get_interface_type_v2; get_linked_object_v2; get_object_type_full_metadata_v2; get_object_type_v2; get_object_v2; get_ontology_v2; get_outgoing_link_type_v2 (+14 more)

### `backend/bff/routers/governance.py`
- Module summary: Governance endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=269 | code_lines=233 | risk_score=0
- API surface: public=15 | top-level functions=15 | classes=0 | methods=0
- Runtime signals: async_functions=15 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/15 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (6): bff.routers.registry_deps; bff.schemas.governance_requests; bff.services; shared.models.requests; shared.observability.tracing; shared.services.registries.dataset_registry
- External imports (2): fastapi; typing
- Public API names: create_backing_datasource; create_backing_datasource_version; create_key_spec; get_backing_datasource; get_backing_datasource_version; get_key_spec; list_access_policies; list_backing_datasource_versions; list_backing_datasources; list_gate_policies; list_gate_results; list_key_specs (+3 more)

### `backend/bff/routers/graph.py`
- Module summary: Graph Query Router (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=228 | code_lines=198 | risk_score=15
- API surface: public=10 | top-level functions=8 | classes=2 | methods=0
- Runtime signals: async_functions=8 | try=3 | raise=3 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/8 (0%) | classes=0/2 (0%) | methods=0/0 (n/a)
- Internal imports (10): bff.routers.registry_deps; bff.services.graph_federation_provider; bff.services.graph_query_service; shared.dependencies.providers; shared.errors.error_types; shared.models.graph_query; shared.observability.tracing; shared.security.input_sanitizer (+2 more)
- External imports (4): fastapi; logging; pydantic; typing
- Public API names: ProjectionQueryRequest; ProjectionRegistrationRequest; execute_graph_query; execute_multi_hop_query; execute_simple_graph_query; find_relationship_paths; graph_service_health; list_projections; query_projection; register_projection

### `backend/bff/routers/health.py`
- Module summary: 헬스체크 및 기본 라우터
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=70 | code_lines=51 | risk_score=6
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/2 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): bff.dependencies; bff.services.oms_client; shared.observability.tracing
- External imports (2): fastapi; logging
- Public API names: health_check; root

### `backend/bff/routers/instance_async.py`
- Module summary: Async instance endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=164 | code_lines=142 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=4/4 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (7): bff.dependencies; bff.schemas.instance_async_requests; bff.services; bff.services.oms_client; shared.models.commands; shared.observability.tracing; shared.utils.label_mapper
- External imports (2): fastapi; typing
- Public API names: bulk_create_instances_async; create_instance_async; delete_instance_async; update_instance_async

### `backend/bff/routers/instances.py`
- Module summary: 인스턴스 관련 API 라우터
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=156 | code_lines=141 | risk_score=15
- API surface: public=3 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=4 | try=4 | raise=6 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (9): bff.dependencies; bff.routers.registry_deps; bff.services.instances_service; shared.dependencies; shared.errors.error_types; shared.observability.tracing; shared.services.registries.action_log_registry; shared.services.registries.dataset_registry (+1 more)
- External imports (3): fastapi; logging; typing
- Public API names: get_class_instances; get_class_sample_values; get_instance

### `backend/bff/routers/lineage.py`
- Module summary: Lineage (provenance) query router for BFF.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=1854 | code_lines=1665 | risk_score=50
- API surface: public=10 | top-level functions=47 | classes=0 | methods=0
- Runtime signals: async_functions=14 | try=13 | raise=21 | broad_except=10 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=10/47 (21%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (11): bff.routers.registry_deps; bff.services.lineage_out_of_date_service; shared.config.settings; shared.dependencies.providers; shared.errors.error_types; shared.models.lineage; shared.models.lineage_edge_types; shared.models.requests (+3 more)
- External imports (6): asyncpg; collections; datetime; fastapi; statistics; typing
- Public API names: get_lineage_column_lineage; get_lineage_diff; get_lineage_graph; get_lineage_impact; get_lineage_metrics; get_lineage_out_of_date; get_lineage_path; get_lineage_run_impact; get_lineage_runs; get_lineage_timeline

### `backend/bff/routers/link_types.py`
- Module summary: Link types router composition (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=55 | code_lines=41 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (7): bff.routers; bff.routers.link_types_deps; bff.routers.link_types_edits; bff.routers.link_types_read; bff.routers.link_types_write; bff.services.link_types_mapping_service; shared.security.database_access
- External imports (1): fastapi
- Public API names: not documented

### `backend/bff/routers/link_types_deps.py`
- Module summary: Link types dependency providers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=13 | code_lines=9 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.routers.registry_deps
- External imports (0): not documented
- Public API names: not documented

### `backend/bff/routers/link_types_edits.py`
- Module summary: Link type edit endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=134 | code_lines=117 | risk_score=10
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=2 | raise=13 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (7): bff.routers.link_types_deps; bff.schemas.link_types_requests; shared.errors.error_types; shared.models.requests; shared.observability.tracing; shared.security.input_sanitizer; shared.services.registries.dataset_registry
- External imports (2): fastapi; logging
- Public API names: create_link_edit; list_link_edits

### `backend/bff/routers/link_types_ops.py`
- Module summary: Link type + relationship spec helpers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=41 | code_lines=37 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.services.link_types_mapping_service
- External imports (0): not documented
- Public API names: not documented

### `backend/bff/routers/link_types_read.py`
- Module summary: Link type read endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=216 | code_lines=179 | risk_score=10
- API surface: public=2 | top-level functions=8 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=3 | raise=7 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/8 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (10): bff.dependencies; bff.routers.link_types_deps; bff.routers.role_deps; bff.services.oms_client; shared.errors.error_types; shared.models.requests; shared.observability.tracing; shared.security.database_access (+2 more)
- External imports (3): fastapi; httpx; logging
- Public API names: get_link_type; list_link_types

### `backend/bff/routers/link_types_write.py`
- Module summary: Link type write endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=110 | code_lines=98 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (12): bff.dependencies; bff.routers.link_types_deps; bff.routers.objectify_job_ops; bff.routers.role_deps; bff.schemas.link_types_requests; bff.services; bff.services.objectify_mapping_spec_service; bff.services.oms_client (+4 more)
- External imports (2): fastapi; typing
- Public API names: create_link_type; reindex_link_type; update_link_type

### `backend/bff/routers/mapping.py`
- Module summary: 레이블 매핑 관리 라우터
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=68 | code_lines=52 | risk_score=0
- API surface: public=5 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=5 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (4): bff.dependencies; bff.services; shared.models.requests; shared.observability.tracing
- External imports (2): fastapi; logging
- Public API names: clear_mappings; export_mappings; get_mappings_summary; import_mappings; validate_mappings

### `backend/bff/routers/object_types.py`
- Module summary: Object type contract endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=309 | code_lines=263 | risk_score=0
- API surface: public=2 | top-level functions=11 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=1 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/11 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (12): bff.dependencies; bff.routers.object_types_deps; bff.schemas.object_types_requests; bff.services; bff.services.oms_client; shared.errors.error_types; shared.models.requests; shared.observability.tracing (+4 more)
- External imports (3): fastapi; logging; typing
- Public API names: create_object_type_contract; update_object_type_contract

### `backend/bff/routers/object_types_deps.py`
- Module summary: Object types dependency providers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=13 | code_lines=9 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.routers.registry_deps
- External imports (0): not documented
- Public API names: not documented

### `backend/bff/routers/objectify.py`
- Module summary: Objectify (Dataset -> Ontology) API (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=40 | code_lines=33 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): bff.routers; bff.routers.objectify_deps
- External imports (1): fastapi
- Public API names: not documented

### `backend/bff/routers/objectify_dag.py`
- Module summary: Objectify DAG orchestration endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=46 | code_lines=36 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (9): bff.dependencies; bff.routers.objectify_deps; bff.schemas.objectify_requests; bff.services; bff.services.oms_client; shared.observability.tracing; shared.services.events.objectify_job_queue; shared.services.registries.dataset_registry (+1 more)
- External imports (2): fastapi; typing
- Public API names: run_objectify_dag

### `backend/bff/routers/objectify_deps.py`
- Module summary: Objectify dependency providers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=34 | code_lines=27 | risk_score=0
- API surface: public=0 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=1 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): bff.routers.objectify_job_queue_deps; bff.routers.registry_deps; bff.routers.role_deps; shared.errors.error_types; shared.security.database_access
- External imports (1): fastapi
- Public API names: not documented

### `backend/bff/routers/objectify_enterprise.py`
- Module summary: Objectify enterprise helper endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=115 | code_lines=98 | risk_score=5
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=1 | raise=3 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (8): bff.routers.objectify_deps; bff.schemas.objectify_requests; shared.errors.error_types; shared.models.requests; shared.observability.tracing; shared.security.database_access; shared.security.input_sanitizer; shared.services.registries.dataset_registry
- External imports (3): fastapi; logging; typing
- Public API names: detect_relationships

### `backend/bff/routers/objectify_incremental.py`
- Module summary: Objectify incremental execution endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=160 | code_lines=135 | risk_score=10
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=2 | raise=6 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/2 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (11): bff.routers.objectify_deps; bff.schemas.objectify_requests; shared.errors.error_types; shared.models.objectify_job; shared.models.requests; shared.observability.tracing; shared.security.database_access; shared.security.input_sanitizer (+3 more)
- External imports (4): fastapi; logging; typing; uuid
- Public API names: get_mapping_spec_watermark; trigger_incremental_objectify

### `backend/bff/routers/objectify_job_ops.py`
- Module summary: Objectify job helper functions (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=115 | code_lines=102 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (4): shared.errors.error_types; shared.models.objectify_job; shared.services.registries.dataset_registry; shared.services.registries.objectify_registry
- External imports (3): fastapi; typing; uuid
- Public API names: enqueue_objectify_job_for_mapping_spec

### `backend/bff/routers/objectify_job_queue_deps.py`
- Module summary: Shared Objectify job queue dependency providers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=15 | code_lines=11 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): bff.routers.registry_deps; shared.services.events.objectify_job_queue; shared.services.registries.objectify_registry
- External imports (1): fastapi
- Public API names: get_objectify_job_queue

### `backend/bff/routers/objectify_mapping_specs.py`
- Module summary: Objectify mapping spec endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=61 | code_lines=50 | risk_score=5
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=1 | raise=1 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (9): bff.dependencies; bff.routers.objectify_deps; bff.schemas.objectify_requests; bff.services.objectify_mapping_spec_service; shared.errors.error_types; shared.models.requests; shared.observability.tracing; shared.services.registries.dataset_registry (+1 more)
- External imports (3): fastapi; logging; typing
- Public API names: create_mapping_spec; list_mapping_specs

### `backend/bff/routers/objectify_ops.py`
- Module summary: Objectify helpers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=32 | code_lines=29 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.services.objectify_ops_service
- External imports (0): not documented
- Public API names: not documented

### `backend/bff/routers/objectify_runs.py`
- Module summary: Objectify run endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=53 | code_lines=44 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (10): bff.dependencies; bff.routers.objectify_deps; bff.schemas.objectify_requests; bff.services; bff.services.oms_client; shared.observability.tracing; shared.services.events.objectify_job_queue; shared.services.registries.dataset_registry (+2 more)
- External imports (2): fastapi; typing
- Public API names: run_objectify

### `backend/bff/routers/ontology.py`
- Module summary: Ontology router composition (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=25 | code_lines=18 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (4): bff.routers.ontology_crud; bff.routers.ontology_imports; bff.routers.ontology_metadata; bff.routers.ontology_suggestions
- External imports (1): fastapi
- Public API names: not documented

### `backend/bff/routers/ontology_agent.py`
- Module summary: Ontology Agent API Router
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=223 | code_lines=179 | risk_score=21
- API surface: public=2 | top-level functions=4 | classes=1 | methods=0
- Runtime signals: async_functions=1 | try=4 | raise=3 | broad_except=4 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=4/4 (100%) | classes=1/1 (100%) | methods=0/0 (n/a)
- Internal imports (12): bff.services.pipeline_agent_autonomous_loop; shared.config.settings; shared.errors.error_types; shared.models.pipeline_plan; shared.models.requests; shared.observability.tracing; shared.security.auth_utils; shared.services.agent.llm_gateway (+4 more)
- External imports (4): fastapi; logging; pydantic; typing
- Public API names: OntologyAgentRunRequest; run_ontology_agent

### `backend/bff/routers/ontology_crud.py`
- Module summary: Ontology CRUD endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=93 | code_lines=81 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (6): bff.dependencies; bff.services; bff.services.oms_client; shared.models.ontology; shared.models.responses; shared.observability.tracing
- External imports (1): fastapi
- Public API names: create_ontology; get_ontology_schema; validate_ontology_create_bff

### `backend/bff/routers/ontology_extensions.py`
- Module summary: Ontology extension endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=205 | code_lines=180 | risk_score=0
- API surface: public=5 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=10 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): bff.dependencies; bff.schemas.ontology_extensions_requests; bff.services; bff.services.oms_client; shared.observability.tracing
- External imports (2): fastapi; typing
- Public API names: approve_ontology_proposal; create_ontology_proposal; deploy_ontology; list_ontology_proposals; ontology_health

### `backend/bff/routers/ontology_imports.py`
- Module summary: Ontology import endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=136 | code_lines=121 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): bff.dependencies; bff.schemas.ontology_requests; bff.services; bff.services.oms_client; shared.observability.tracing
- External imports (2): fastapi; typing
- Public API names: commit_import_from_excel; commit_import_from_google_sheets; dry_run_import_from_excel; dry_run_import_from_google_sheets

### `backend/bff/routers/ontology_metadata.py`
- Module summary: Ontology metadata endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=167 | code_lines=133 | risk_score=5
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=4 | raise=4 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): bff.dependencies; bff.services.oms_client; shared.errors.error_types; shared.observability.tracing; shared.security.input_sanitizer
- External imports (5): datetime; fastapi; httpx; logging; typing
- Public API names: save_mapping_metadata

### `backend/bff/routers/ontology_ops.py`
- Module summary: Ontology router helpers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=32 | code_lines=29 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.services.ontology_ops_service
- External imports (0): not documented
- Public API names: not documented

### `backend/bff/routers/ontology_suggestions.py`
- Module summary: Ontology suggestion endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=132 | code_lines=113 | risk_score=0
- API surface: public=6 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=6 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): bff.schemas.ontology_requests; bff.services; shared.observability.tracing
- External imports (3): fastapi; logging; typing
- Public API names: suggest_mappings; suggest_mappings_from_excel; suggest_mappings_from_google_sheets; suggest_schema_from_data; suggest_schema_from_excel; suggest_schema_from_google_sheets

### `backend/bff/routers/ops.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=33 | code_lines=25 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): bff.routers.registry_deps; shared.models.requests; shared.observability.tracing; shared.services.registries.dataset_registry; shared.services.registries.objectify_registry
- External imports (3): fastapi; logging; typing
- Public API names: ops_status

### `backend/bff/routers/pipeline.py`
- Module summary: Pipeline Builder API (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=40 | code_lines=29 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.routers
- External imports (1): fastapi
- Public API names: not documented

### `backend/bff/routers/pipeline_branches.py`
- Module summary: Pipeline branch endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=192 | code_lines=175 | risk_score=20
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=4 | try=4 | raise=17 | broad_except=4 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (10): bff.routers.pipeline_deps; bff.routers.pipeline_shared; shared.dependencies.providers; shared.errors.error_types; shared.models.requests; shared.observability.tracing; shared.security.input_sanitizer; shared.services.registries.pipeline_registry (+2 more)
- External imports (3): fastapi; logging; typing
- Public API names: archive_pipeline_branch; create_pipeline_branch; list_pipeline_branches; restore_pipeline_branch

### `backend/bff/routers/pipeline_catalog.py`
- Module summary: Pipeline catalog endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=56 | code_lines=47 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (8): bff.routers.pipeline_deps; bff.services; shared.dependencies.providers; shared.models.requests; shared.observability.tracing; shared.services.registries.dataset_registry; shared.services.registries.pipeline_registry; shared.services.storage.event_store
- External imports (2): fastapi; typing
- Public API names: create_pipeline; list_pipelines

### `backend/bff/routers/pipeline_datasets.py`
- Module summary: Pipeline datasets router composition (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=32 | code_lines=22 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): bff.routers; bff.routers.pipeline_datasets_uploads_csv; bff.routers.pipeline_datasets_versions
- External imports (1): fastapi
- Public API names: not documented

### `backend/bff/routers/pipeline_datasets_catalog.py`
- Module summary: Pipeline dataset read endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=156 | code_lines=137 | risk_score=15
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=5 | raise=11 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (10): bff.routers.pipeline_datasets_ops; bff.routers.pipeline_deps; shared.errors.error_types; shared.models.requests; shared.observability.tracing; shared.security.auth_utils; shared.security.input_sanitizer; shared.services.registries.dataset_registry (+2 more)
- External imports (5): base64; fastapi; logging; mimetypes; typing
- Public API names: get_dataset_raw_file; list_datasets

### `backend/bff/routers/pipeline_datasets_deps.py`
- Module summary: Pipeline datasets dependencies (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=9 | code_lines=6 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.routers.objectify_job_queue_deps
- External imports (0): not documented
- Public API names: not documented

### `backend/bff/routers/pipeline_datasets_ingest.py`
- Module summary: Pipeline dataset ingest request endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=95 | code_lines=85 | risk_score=10
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=4 | raise=9 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (9): bff.routers.pipeline_datasets_ops; bff.routers.pipeline_deps; bff.schemas.pipeline_datasets; shared.errors.error_types; shared.models.requests; shared.observability.tracing; shared.security.auth_utils; shared.security.input_sanitizer (+1 more)
- External imports (3): fastapi; logging; typing
- Public API names: approve_dataset_schema; get_dataset_ingest_request

### `backend/bff/routers/pipeline_datasets_ops.py`
- Module summary: Pipeline datasets helpers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=85 | code_lines=80 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (7): bff.routers.pipeline_datasets_ops_funnel; bff.routers.pipeline_datasets_ops_ingest; bff.routers.pipeline_datasets_ops_lakefs; bff.routers.pipeline_datasets_ops_objectify; bff.routers.pipeline_datasets_ops_parsing; shared.services.events.dataset_ingest_outbox; shared.services.storage.event_store
- External imports (0): not documented
- Public API names: not documented

### `backend/bff/routers/pipeline_datasets_ops_funnel.py`
- Module summary: Pipeline dataset funnel/schema helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=222 | code_lines=191 | risk_score=5
- API surface: public=0 | top-level functions=9 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=1 | raise=2 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/9 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.config.settings; shared.errors.error_types
- External imports (3): fastapi; logging; typing
- Public API names: not documented

### `backend/bff/routers/pipeline_datasets_ops_ingest.py`
- Module summary: Pipeline dataset ingest helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=65 | code_lines=50 | risk_score=7
- API surface: public=0 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=2 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.services.registries.dataset_registry; shared.utils.path_utils
- External imports (5): hashlib; json; logging; typing; urllib
- Public API names: not documented

### `backend/bff/routers/pipeline_datasets_ops_lakefs.py`
- Module summary: Pipeline dataset lakeFS helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=219 | code_lines=196 | risk_score=15
- API surface: public=0 | top-level functions=8 | classes=0 | methods=0
- Runtime signals: async_functions=5 | try=6 | raise=7 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/8 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (6): shared.config.settings; shared.errors.error_types; shared.services.storage.lakefs_client; shared.services.storage.redis_service; shared.utils.path_utils; shared.utils.s3_uri
- External imports (6): asyncio; fastapi; logging; time; typing; uuid
- Public API names: not documented

### `backend/bff/routers/pipeline_datasets_ops_objectify.py`
- Module summary: Pipeline dataset Objectify helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=110 | code_lines=103 | risk_score=6
- API surface: public=0 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): shared.models.objectify_job; shared.services.events.objectify_job_queue; shared.services.registries.dataset_registry; shared.services.registries.objectify_registry; shared.utils.schema_hash
- External imports (3): logging; typing; uuid
- Public API names: not documented

### `backend/bff/routers/pipeline_datasets_ops_parsing.py`
- Module summary: Pipeline dataset parsing helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=217 | code_lines=182 | risk_score=6
- API surface: public=0 | top-level functions=7 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=7 | raise=6 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/7 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.errors.error_types
- External imports (6): csv; fastapi; hashlib; io; logging; typing
- Public API names: not documented

### `backend/bff/routers/pipeline_datasets_uploads.py`
- Module summary: Pipeline dataset upload endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=33 | code_lines=23 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (4): bff.routers; bff.routers.pipeline_datasets_uploads_csv; bff.routers.pipeline_datasets_uploads_excel; bff.routers.pipeline_datasets_uploads_media
- External imports (1): fastapi
- Public API names: not documented

### `backend/bff/routers/pipeline_datasets_uploads_csv.py`
- Module summary: Pipeline dataset CSV upload endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=181 | code_lines=164 | risk_score=10
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=2 | raise=6 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (14): bff.routers.pipeline_datasets_deps; bff.routers.pipeline_datasets_ops; bff.routers.pipeline_deps; bff.services.pipeline_dataset_upload_context; bff.services.pipeline_tabular_upload_facade; shared.config.settings; shared.dependencies.providers; shared.errors.error_types (+6 more)
- External imports (4): asyncio; fastapi; logging; typing
- Public API names: upload_csv_dataset

### `backend/bff/routers/pipeline_datasets_uploads_excel.py`
- Module summary: Pipeline dataset Excel upload endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=209 | code_lines=191 | risk_score=10
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=4 | raise=6 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (14): bff.routers.pipeline_datasets_deps; bff.routers.pipeline_datasets_ops; bff.routers.pipeline_deps; bff.services.pipeline_dataset_upload_context; bff.services.pipeline_tabular_upload_facade; shared.config.settings; shared.dependencies.providers; shared.errors.error_types (+6 more)
- External imports (5): asyncio; fastapi; io; logging; typing
- Public API names: upload_excel_dataset

### `backend/bff/routers/pipeline_datasets_uploads_media.py`
- Module summary: Pipeline dataset media upload endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=55 | code_lines=48 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (11): bff.routers.pipeline_datasets_deps; bff.routers.pipeline_datasets_ops; bff.routers.pipeline_deps; bff.services; shared.dependencies.providers; shared.models.requests; shared.observability.tracing; shared.services.events.objectify_job_queue (+3 more)
- External imports (2): fastapi; typing
- Public API names: upload_media_dataset

### `backend/bff/routers/pipeline_datasets_versions.py`
- Module summary: Pipeline dataset write/version endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=162 | code_lines=147 | risk_score=20
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=5 | raise=10 | broad_except=4 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (18): bff.routers.pipeline_datasets_deps; bff.routers.pipeline_datasets_ops; bff.routers.pipeline_deps; bff.schemas.pipeline_datasets; bff.services; shared.config.app_config; shared.dependencies.providers; shared.errors.error_types (+10 more)
- External imports (5): asyncpg; fastapi; logging; typing; uuid
- Public API names: create_dataset; create_dataset_version; reanalyze_dataset_version

### `backend/bff/routers/pipeline_deps.py`
- Module summary: Pipeline Builder dependency providers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=30 | code_lines=20 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): bff.routers.registry_deps; shared.services.pipeline.pipeline_executor; shared.services.pipeline.pipeline_job_queue
- External imports (0): not documented
- Public API names: get_pipeline_executor; get_pipeline_job_queue

### `backend/bff/routers/pipeline_detail.py`
- Module summary: Pipeline detail endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=82 | code_lines=69 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (8): bff.routers.pipeline_deps; bff.services; shared.dependencies.providers; shared.models.requests; shared.observability.tracing; shared.services.registries.dataset_registry; shared.services.registries.pipeline_registry; shared.services.storage.event_store
- External imports (3): fastapi; logging; typing
- Public API names: get_pipeline; get_pipeline_readiness; update_pipeline

### `backend/bff/routers/pipeline_execution.py`
- Module summary: Pipeline execution endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=122 | code_lines=107 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (14): bff.dependencies; bff.routers.pipeline_deps; bff.routers.pipeline_ops; bff.services; bff.services.oms_client; shared.dependencies.providers; shared.models.requests; shared.observability.tracing (+6 more)
- External imports (3): fastapi; logging; typing
- Public API names: build_pipeline; deploy_pipeline; preview_pipeline

### `backend/bff/routers/pipeline_history.py`
- Module summary: Pipeline run/artifact endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=140 | code_lines=127 | risk_score=15
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=4 | raise=10 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (6): bff.routers.pipeline_deps; bff.routers.pipeline_shared; shared.errors.error_types; shared.models.requests; shared.observability.tracing; shared.services.registries.pipeline_registry
- External imports (4): asyncio; fastapi; logging; typing
- Public API names: get_pipeline_artifact; list_pipeline_artifacts; list_pipeline_runs

### `backend/bff/routers/pipeline_ops.py`
- Module summary: Pipeline Builder helper operations.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=48 | code_lines=43 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (7): bff.routers.pipeline_ops_augmentation; bff.routers.pipeline_ops_definition; bff.routers.pipeline_ops_dependencies; bff.routers.pipeline_ops_locks; bff.routers.pipeline_ops_policy; bff.routers.pipeline_ops_preflight; bff.routers.pipeline_ops_schema
- External imports (0): not documented
- Public API names: not documented

### `backend/bff/routers/pipeline_ops_augmentation.py`
- Module summary: Pipeline Builder definition augmentation.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=14 | code_lines=9 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): bff.routers.pipeline_ops_augmentation_casts; bff.routers.pipeline_ops_augmentation_contract
- External imports (0): not documented
- Public API names: not documented

### `backend/bff/routers/pipeline_ops_augmentation_casts.py`
- Module summary: Pipeline Builder cast augmentation (BFF facade).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=12 | code_lines=7 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline.pipeline_definition_augmentation
- External imports (0): not documented
- Public API names: not documented

### `backend/bff/routers/pipeline_ops_augmentation_contract.py`
- Module summary: Pipeline Builder canonical contract augmentation (BFF facade).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=12 | code_lines=7 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline.pipeline_definition_augmentation
- External imports (0): not documented
- Public API names: not documented

### `backend/bff/routers/pipeline_ops_definition.py`
- Module summary: Pipeline Builder definition helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=108 | code_lines=90 | risk_score=5
- API surface: public=0 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=2 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.errors.error_types
- External imports (5): fastapi; hashlib; json; logging; typing
- Public API names: not documented

### `backend/bff/routers/pipeline_ops_dependencies.py`
- Module summary: Pipeline Builder dependency helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=83 | code_lines=69 | risk_score=5
- API surface: public=0 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=2 | raise=6 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): shared.errors.error_types; shared.services.pipeline.pipeline_dependency_utils; shared.services.registries.pipeline_registry
- External imports (3): fastapi; typing; uuid
- Public API names: not documented

### `backend/bff/routers/pipeline_ops_locks.py`
- Module summary: Pipeline Builder publish locks.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=69 | code_lines=58 | risk_score=10
- API surface: public=0 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=2 | raise=2 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (4): shared.config.settings; shared.errors.error_types; shared.services.storage.redis_service; shared.utils.path_utils
- External imports (6): asyncio; fastapi; logging; time; typing; uuid
- Public API names: not documented

### `backend/bff/routers/pipeline_ops_policy.py`
- Module summary: Pipeline Builder governance helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=19 | code_lines=12 | risk_score=0
- API surface: public=0 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.config.settings
- External imports (0): not documented
- Public API names: not documented

### `backend/bff/routers/pipeline_ops_preflight.py`
- Module summary: Pipeline Builder preflight + definition validation.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=256 | code_lines=230 | risk_score=6
- API surface: public=0 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=2 | raise=1 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (7): shared.config.settings; shared.errors.error_types; shared.services.pipeline.pipeline_definition_validator; shared.services.pipeline.pipeline_preflight_utils; shared.services.pipeline.pipeline_transform_spec; shared.services.registries.dataset_registry; shared.services.registries.pipeline_registry
- External imports (3): fastapi; logging; typing
- Public API names: not documented

### `backend/bff/routers/pipeline_ops_schema.py`
- Module summary: Pipeline Builder schema helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=79 | code_lines=63 | risk_score=0
- API surface: public=0 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): shared.services.pipeline.pipeline_definition_utils; shared.services.pipeline.pipeline_graph_utils; shared.services.pipeline.pipeline_schema_utils
- External imports (1): typing
- Public API names: not documented

### `backend/bff/routers/pipeline_plans.py`
- Module summary: Pipeline Plans API (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=39 | code_lines=30 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (7): bff.routers.pipeline_deps; bff.routers.pipeline_plans_compile; bff.routers.pipeline_plans_deps; bff.routers.pipeline_plans_ops; bff.routers.pipeline_plans_preview; bff.routers.pipeline_plans_read; bff.schemas.pipeline_plans_requests
- External imports (1): fastapi
- Public API names: not documented

### `backend/bff/routers/pipeline_plans_compile.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=167 | code_lines=142 | risk_score=10
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=3 | raise=5 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (17): bff.routers.pipeline_deps; bff.routers.pipeline_plans_deps; bff.routers.pipeline_plans_ops; bff.schemas.pipeline_plans_requests; bff.services.pipeline_plan_autonomous_compiler; bff.services.pipeline_plan_models; shared.config.settings; shared.dependencies.providers (+9 more)
- External imports (2): fastapi; logging
- Public API names: compile_plan

### `backend/bff/routers/pipeline_plans_deps.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=15 | code_lines=8 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.services.registries.dataset_profile_registry; shared.services.registries.pipeline_plan_registry
- External imports (0): not documented
- Public API names: get_dataset_profile_registry; get_pipeline_plan_registry

### `backend/bff/routers/pipeline_plans_ops.py`
- Module summary: Pipeline plan helper façade (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=28 | code_lines=25 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): bff.services.pipeline_plan_preview_utils; bff.services.pipeline_plan_scoping_service; bff.services.pipeline_plan_tenant_service
- External imports (0): not documented
- Public API names: not documented

### `backend/bff/routers/pipeline_plans_preview.py`
- Module summary: Pipeline plan preview endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=84 | code_lines=73 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (9): bff.routers.pipeline_deps; bff.routers.pipeline_plans_deps; bff.schemas.pipeline_plans_requests; bff.services; shared.models.responses; shared.observability.tracing; shared.services.registries.dataset_registry; shared.services.registries.pipeline_plan_registry (+1 more)
- External imports (1): fastapi
- Public API names: evaluate_joins; inspect_plan_preview; preview_plan

### `backend/bff/routers/pipeline_plans_read.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=27 | code_lines=20 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): bff.routers.pipeline_plans_deps; bff.routers.pipeline_plans_ops; shared.models.responses; shared.observability.tracing; shared.services.registries.pipeline_plan_registry
- External imports (1): fastapi
- Public API names: get_plan

### `backend/bff/routers/pipeline_proposals.py`
- Module summary: Pipeline proposal endpoints (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=98 | code_lines=84 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (8): bff.routers.pipeline_deps; bff.services; shared.dependencies.providers; shared.models.requests; shared.observability.tracing; shared.services.registries.dataset_registry; shared.services.registries.objectify_registry; shared.services.registries.pipeline_registry
- External imports (2): fastapi; typing
- Public API names: approve_pipeline_proposal; list_pipeline_proposals; reject_pipeline_proposal; submit_pipeline_proposal

### `backend/bff/routers/pipeline_shared.py`
- Module summary: Shared helpers for Pipeline Builder routers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=168 | code_lines=146 | risk_score=0
- API surface: public=0 | top-level functions=7 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=1 | raise=4 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/7 (14%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): bff.services.http_idempotency; shared.dependencies.providers; shared.errors.error_types; shared.security.principal_utils; shared.services.registries.pipeline_registry
- External imports (3): fastapi; typing; uuid
- Public API names: not documented

### `backend/bff/routers/pipeline_simulation.py`
- Module summary: Pipeline simulation endpoint (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=150 | code_lines=132 | risk_score=5
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=3 | raise=5 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (10): bff.routers.pipeline_deps; bff.routers.pipeline_ops; shared.errors.error_types; shared.models.requests; shared.observability.tracing; shared.security.auth_utils; shared.security.input_sanitizer; shared.services.pipeline.pipeline_executor (+2 more)
- External imports (3): fastapi; logging; typing
- Public API names: simulate_pipeline_definition

### `backend/bff/routers/pipeline_udfs.py`
- Module summary: Pipeline UDF API (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=96 | code_lines=77 | risk_score=0
- API surface: public=7 | top-level functions=5 | classes=2 | methods=0
- Runtime signals: async_functions=5 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/5 (0%) | classes=0/2 (0%) | methods=0/0 (n/a)
- Internal imports (5): bff.routers.pipeline_deps; bff.services; shared.models.requests; shared.observability.tracing; shared.services.registries.pipeline_registry
- External imports (3): fastapi; pydantic; typing
- Public API names: UdfCreateRequest; UdfVersionCreateRequest; create_udf; create_udf_version; get_udf; get_udf_version; list_udfs

### `backend/bff/routers/query.py`
- Module summary: 쿼리 라우터
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=89 | code_lines=83 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.observability.tracing
- External imports (1): fastapi
- Public API names: query_builder_info

### `backend/bff/routers/registry_deps.py`
- Module summary: Shared BFF registry dependency providers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=47 | code_lines=28 | risk_score=0
- API surface: public=5 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=5 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): shared.services.registries.agent_policy_registry; shared.services.registries.agent_session_registry; shared.services.registries.dataset_registry; shared.services.registries.objectify_registry; shared.services.registries.pipeline_registry
- External imports (0): not documented
- Public API names: get_agent_policy_registry; get_agent_session_registry; get_dataset_registry; get_objectify_registry; get_pipeline_registry

### `backend/bff/routers/role_deps.py`
- Module summary: Role enforcement helpers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=31 | code_lines=19 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=1 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.errors.error_types; shared.security.database_access
- External imports (3): collections; fastapi; typing
- Public API names: enforce_required_database_role; require_database_role

### `backend/bff/routers/schema_changes.py`
- Module summary: Schema Changes API Router
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=206 | code_lines=173 | risk_score=0
- API surface: public=7 | top-level functions=7 | classes=0 | methods=0
- Runtime signals: async_functions=7 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=7/7 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): bff.routers.registry_deps; bff.schemas.schema_changes_requests; bff.services; shared.observability.tracing; shared.services.core.schema_drift_detector
- External imports (4): datetime; fastapi; logging; typing
- Public API names: acknowledge_drift; check_mapping_compatibility; create_subscription; delete_subscription; get_schema_change_stats; list_schema_changes; list_subscriptions

### `backend/bff/routers/summary.py`
- Module summary: Frontend-facing system summary router (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=79 | code_lines=64 | risk_score=6
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): shared.dependencies.providers; shared.models.requests; shared.observability.tracing; shared.security.input_sanitizer; shared.utils.branch_utils
- External imports (3): fastapi; logging; typing
- Public API names: get_summary

### `backend/bff/routers/tasks.py`
- Module summary: Background Task Management Router
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=154 | code_lines=117 | risk_score=0
- API surface: public=6 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=6 | try=0 | raise=3 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=6/6 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (6): bff.schemas.tasks_requests; bff.services; shared.dependencies.providers; shared.errors.error_types; shared.models.background_task; shared.observability.tracing
- External imports (2): fastapi; typing
- Public API names: cancel_task; get_task_metrics; get_task_result; get_task_status; list_tasks; retry_task

### `backend/bff/routers/websocket.py`
- Module summary: BFF WebSocket Router for Real-time Command Status Updates
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=72 | code_lines=57 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=3/3 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): bff.services; shared.services.core.websocket_service
- External imports (2): fastapi; typing
- Public API names: get_ws_manager; websocket_command_updates; websocket_user_commands

### `backend/bff/schemas/__init__.py`
- Module summary: BFF Schemas package.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=5 | code_lines=4 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/bff/schemas/actions_requests.py`
- Module summary: Action-related request schemas (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=121 | code_lines=92 | risk_score=0
- API surface: public=11 | top-level functions=0 | classes=11 | methods=1
- Runtime signals: async_functions=0 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=2/11 (18%) | methods=0/1 (0%)
- Internal imports (1): shared.utils.action_simulation_utils
- External imports (3): __future__; pydantic; typing
- Public API names: ActionSimulateAssumptions; ActionSimulateObservedBaseOverrides; ActionSimulateRequest; ActionSimulateScenarioRequest; ActionSimulateStatePatch; ActionSimulateTargetAssumption; ActionSubmitBatchDependencyRequest; ActionSubmitBatchItemRequest; ActionSubmitBatchRequest; ActionSubmitRequest; ActionUndoRequest

### `backend/bff/schemas/admin_projection_requests.py`
- Module summary: Admin projection request/response schemas (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=44 | code_lines=34 | risk_score=0
- API surface: public=2 | top-level functions=0 | classes=2 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=2/2 (100%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (4): __future__; datetime; pydantic; typing
- Public API names: RecomputeProjectionRequest; RecomputeProjectionResponse

### `backend/bff/schemas/admin_replay_requests.py`
- Module summary: Admin replay request/response schemas (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=24 | code_lines=16 | risk_score=0
- API surface: public=2 | top-level functions=0 | classes=2 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=2/2 (100%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; pydantic
- Public API names: ReplayInstanceStateRequest; ReplayInstanceStateResponse

### `backend/bff/schemas/context7_requests.py`
- Module summary: Context7 request schemas (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=43 | code_lines=28 | risk_score=0
- API surface: public=4 | top-level functions=0 | classes=4 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=4/4 (100%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; pydantic; typing
- Public API names: EntityLinkRequest; KnowledgeRequest; OntologyAnalysisRequest; SearchRequest

### `backend/bff/schemas/governance_requests.py`
- Module summary: Governance request schemas (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=50 | code_lines=35 | risk_score=0
- API surface: public=5 | top-level functions=0 | classes=5 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/5 (0%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; pydantic; typing
- Public API names: AccessPolicyRequest; CreateBackingDatasourceRequest; CreateBackingDatasourceVersionRequest; CreateKeySpecRequest; GatePolicyRequest

### `backend/bff/schemas/instance_async_requests.py`
- Module summary: Async instance request schemas (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=33 | code_lines=19 | risk_score=0
- API surface: public=3 | top-level functions=0 | classes=3 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=3/3 (100%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; pydantic; typing
- Public API names: BulkInstanceCreateRequest; InstanceCreateRequest; InstanceUpdateRequest

### `backend/bff/schemas/link_types_requests.py`
- Module summary: Link type request schemas (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=80 | code_lines=62 | risk_score=0
- API surface: public=6 | top-level functions=0 | classes=6 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/6 (0%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; pydantic; typing
- Public API names: ForeignKeyRelationshipSpec; JoinTableRelationshipSpec; LinkEditRequest; LinkTypeRequest; LinkTypeUpdateRequest; ObjectBackedRelationshipSpec

### `backend/bff/schemas/object_types_requests.py`
- Module summary: Object type contract request schemas (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=37 | code_lines=29 | risk_score=0
- API surface: public=2 | top-level functions=0 | classes=2 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/2 (0%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; pydantic; typing
- Public API names: ObjectTypeContractRequest; ObjectTypeContractUpdate

### `backend/bff/schemas/objectify_requests.py`
- Module summary: Objectify-related request schemas (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=83 | code_lines=63 | risk_score=0
- API surface: public=7 | top-level functions=0 | classes=7 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/7 (14%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; pydantic; typing
- Public API names: CreateMappingSpecRequest; DetectRelationshipsRequest; DetectRelationshipsResponse; MappingSpecField; RunObjectifyDAGRequest; TriggerIncrementalRequest; TriggerObjectifyRequest

### `backend/bff/schemas/ontology_extensions_requests.py`
- Module summary: Ontology extension request schemas (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=44 | code_lines=31 | risk_score=0
- API surface: public=4 | top-level functions=0 | classes=4 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/4 (0%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; pydantic; typing
- Public API names: OntologyApproveRequest; OntologyDeployRequest; OntologyProposalRequest; OntologyResourceRequest

### `backend/bff/schemas/ontology_requests.py`
- Module summary: Ontology-related request schemas (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=110 | code_lines=73 | risk_score=0
- API surface: public=7 | top-level functions=0 | classes=7 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=7/7 (100%) | methods=0/0 (n/a)
- Internal imports (1): shared.models.structure_analysis
- External imports (3): __future__; pydantic; typing
- Public API names: ImportFieldMapping; ImportFromGoogleSheetsRequest; ImportTargetField; MappingFromGoogleSheetsRequest; MappingSuggestionRequest; SchemaFromDataRequest; SchemaFromGoogleSheetsRequest

### `backend/bff/schemas/pipeline_datasets.py`
- Module summary: Pipeline datasets schemas (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=30 | code_lines=20 | risk_score=0
- API surface: public=2 | top-level functions=0 | classes=2 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/2 (0%) | methods=0/0 (n/a)
- Internal imports (1): shared.models.type_inference
- External imports (3): __future__; pydantic; typing
- Public API names: FunnelAnalysisApiResponse; FunnelAnalysisData

### `backend/bff/schemas/pipeline_plans_requests.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=35 | code_lines=23 | risk_score=0
- API surface: public=4 | top-level functions=0 | classes=4 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/4 (0%) | methods=0/0 (n/a)
- Internal imports (1): shared.models.pipeline_plan
- External imports (3): __future__; pydantic; typing
- Public API names: PipelinePlanCompileRequest; PipelinePlanEvaluateJoinsRequest; PipelinePlanInspectPreviewRequest; PipelinePlanPreviewRequest

### `backend/bff/schemas/schema_changes_requests.py`
- Module summary: Schema changes request/response schemas (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=64 | code_lines=48 | risk_score=0
- API surface: public=6 | top-level functions=0 | classes=6 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/6 (0%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (4): __future__; datetime; pydantic; typing
- Public API names: AcknowledgeRequest; CompatibilityCheckRequest; CompatibilityCheckResponse; SchemaChangeItem; SubscriptionCreateRequest; SubscriptionResponse

### `backend/bff/schemas/tasks_requests.py`
- Module summary: Background task request/response schemas (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=48 | code_lines=33 | risk_score=0
- API surface: public=3 | top-level functions=0 | classes=3 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=3/3 (100%) | methods=0/0 (n/a)
- Internal imports (1): shared.models.background_task
- External imports (4): __future__; datetime; pydantic; typing
- Public API names: TaskListResponse; TaskMetricsResponse; TaskStatusResponse

### `backend/bff/services/__init__.py`
- Module summary: BFF Services package.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=10 | code_lines=9 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/bff/services/actions_service.py`
- Module summary: Actions service (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=437 | code_lines=372 | risk_score=5
- API surface: public=10 | top-level functions=18 | classes=0 | methods=0
- Runtime signals: async_functions=13 | try=4 | raise=10 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/18 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (10): bff.schemas.actions_requests; bff.services.input_validation_service; bff.services.oms_client; bff.utils.action_log_serialization; bff.utils.httpx_exceptions; shared.errors.error_types; shared.observability.tracing; shared.security.database_access (+2 more)
- External imports (6): __future__; contextlib; fastapi; httpx; typing; uuid
- Public API names: get_action_log; get_action_simulation; get_action_simulation_version; list_action_logs; list_action_simulation_versions; list_action_simulations; simulate_action; submit_action; submit_action_batch; undo_action

### `backend/bff/services/admin_recompute_projection_service.py`
- Module summary: Admin projection recompute service (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=638 | code_lines=549 | risk_score=5
- API surface: public=7 | top-level functions=11 | classes=4 | methods=10
- Runtime signals: async_functions=9 | try=1 | raise=7 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/11 (0%) | classes=0/4 (0%) | methods=0/10 (0%)
- Internal imports (14): bff.routers.admin_task_monitor; bff.schemas.admin_projection_requests; shared.config.search_config; shared.dependencies.providers; shared.errors.error_types; shared.models.background_task; shared.models.lineage_edge_types; shared.observability.tracing (+6 more)
- External imports (9): __future__; dataclasses; datetime; fastapi; json; logging; os; typing (+1 more)
- Public API names: IndexDecision; InstancesProjectionStrategy; OntologiesProjectionStrategy; ProjectionStrategy; get_recompute_projection_result; recompute_projection_task; start_recompute_projection

### `backend/bff/services/admin_reindex_instances_service.py`
- Module summary: Admin Reindex Instances Service.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=186 | code_lines=157 | risk_score=24
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=4 | raise=0 | broad_except=4 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.models.objectify_job; shared.observability.tracing
- External imports (5): __future__; datetime; logging; typing; uuid
- Public API names: reindex_all_instances

### `backend/bff/services/admin_replay_service.py`
- Module summary: Admin replay service (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=416 | code_lines=344 | risk_score=25
- API surface: public=5 | top-level functions=13 | classes=0 | methods=0
- Runtime signals: async_functions=8 | try=5 | raise=7 | broad_except=5 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/13 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (12): bff.routers.admin_task_monitor; bff.schemas.admin_replay_requests; shared.config.settings; shared.errors.error_types; shared.models.background_task; shared.models.lineage; shared.observability.tracing; shared.services.core.audit_log_store (+4 more)
- External imports (6): __future__; datetime; fastapi; logging; typing; uuid
- Public API names: cleanup_old_replay_results; get_replay_result; get_replay_trace; replay_instance_state_task; start_replay_instance_state

### `backend/bff/services/ai_service.py`
- Module summary: AI domain logic (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=1707 | code_lines=1571 | risk_score=105
- API surface: public=3 | top-level functions=24 | classes=0 | methods=0
- Runtime signals: async_functions=7 | try=25 | raise=25 | broad_except=21 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=5/24 (20%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (17): bff.dependencies; bff.services.graph_federation_provider; bff.services.graph_query_service; bff.services.oms_client; shared.dependencies.providers; shared.errors.error_types; shared.models.ai; shared.models.graph_query (+9 more)
- External imports (7): datetime; fastapi; httpx; json; logging; typing; uuid
- Public API names: ai_intent; ai_query; translate_query_plan

### `backend/bff/services/base_http_client.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=12 | code_lines=7 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=1
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/1 (0%) | methods=0/1 (0%)
- Internal imports (1): shared.utils.async_utils
- External imports (2): __future__; typing
- Public API names: ManagedAsyncClient

### `backend/bff/services/context7_service.py`
- Module summary: Context7 service (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=139 | code_lines=111 | risk_score=10
- API surface: public=7 | top-level functions=8 | classes=0 | methods=0
- Runtime signals: async_functions=8 | try=2 | raise=2 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/8 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (4): bff.schemas.context7_requests; bff.services.oms_client; shared.errors.error_types; shared.observability.tracing
- External imports (5): __future__; datetime; fastapi; logging; typing
- Public API names: add_knowledge; analyze_ontology; check_context7_health; create_entity_link; get_entity_context; get_ontology_suggestions; search_context7

### `backend/bff/services/data_connector_pipelining_service.py`
- Module summary: Google Sheets -> Pipeline Builder service (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=278 | code_lines=253 | risk_score=25
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=5 | raise=5 | broad_except=5 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (16): bff.routers.data_connector_ops; bff.routers.pipeline_datasets_ops; data_connector.google_sheets.service; shared.config.app_config; shared.errors.error_types; shared.observability.tracing; shared.security.input_sanitizer; shared.services.events.objectify_job_queue (+8 more)
- External imports (6): __future__; csv; fastapi; io; logging; typing
- Public API names: start_pipelining_google_sheet

### `backend/bff/services/data_connector_registration_service.py`
- Module summary: Google Sheets registration/monitoring service (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=349 | code_lines=303 | risk_score=20
- API surface: public=4 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=5 | try=4 | raise=15 | broad_except=4 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (11): bff.routers.data_connector_ops; data_connector.google_sheets.models; data_connector.google_sheets.service; shared.errors.error_types; shared.models.requests; shared.observability.tracing; shared.security.input_sanitizer; shared.services.registries.connector_registry (+3 more)
- External imports (5): __future__; datetime; fastapi; logging; typing
- Public API names: list_registered_sheets; preview_google_sheet; register_google_sheet; unregister_google_sheet

### `backend/bff/services/database_error_policy.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=75 | code_lines=62 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=1 | methods=1
- Runtime signals: async_functions=0 | try=0 | raise=4 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/1 (0%) | methods=0/1 (0%)
- Internal imports (2): shared.errors.error_types; shared.security.input_sanitizer
- External imports (5): __future__; dataclasses; fastapi; logging; typing
- Public API names: MessageErrorPolicy; apply_message_error_policies

### `backend/bff/services/database_service.py`
- Module summary: Database domain logic (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=581 | code_lines=507 | risk_score=60
- API surface: public=8 | top-level functions=13 | classes=0 | methods=0
- Runtime signals: async_functions=9 | try=14 | raise=23 | broad_except=12 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=8/13 (61%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (9): bff.services.database_error_policy; bff.services.oms_client; shared.config.settings; shared.errors.error_types; shared.models.requests; shared.observability.tracing; shared.security.database_access; shared.security.input_sanitizer (+1 more)
- External imports (6): asyncpg; fastapi; httpx; logging; re; typing
- Public API names: create_class; create_database; delete_database; get_class; get_database; get_database_expected_seq; list_classes; list_databases

### `backend/bff/services/dataset_ingest_commit_service.py`
- Module summary: Shared dataset ingest commit helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=117 | code_lines=100 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=1 | methods=0
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/3 (0%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (3): bff.routers.pipeline_datasets_ops; shared.observability.tracing; shared.utils.s3_uri
- External imports (3): __future__; dataclasses; typing
- Public API names: LakeFSCommitArtifact; ensure_lakefs_commit_artifact; persist_ingest_commit_state

### `backend/bff/services/dataset_ingest_failures.py`
- Module summary: Shared helpers for dataset ingest workflows (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=39 | code_lines=31 | risk_score=6
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.observability.tracing
- External imports (3): __future__; logging; typing
- Public API names: mark_ingest_failed

### `backend/bff/services/dataset_ingest_idempotency.py`
- Module summary: Dataset ingest idempotency helpers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=50 | code_lines=36 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=3 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.errors.error_types; shared.observability.tracing
- External imports (2): __future__; typing
- Public API names: resolve_existing_version_or_raise

### `backend/bff/services/dataset_ingest_outbox_builder.py`
- Module summary: Dataset ingest outbox helpers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=118 | code_lines=109 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=3
- Runtime signals: async_functions=0 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/1 (0%) | methods=0/3 (0%)
- Internal imports (3): shared.config.settings; shared.utils.s3_uri; shared.utils.time_utils
- External imports (2): dataclasses; typing
- Public API names: DatasetIngestOutboxBuilder

### `backend/bff/services/dataset_ingest_outbox_flusher.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=41 | code_lines=31 | risk_score=0
- API surface: public=1 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/2 (50%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.config.settings; shared.observability.tracing
- External imports (3): __future__; logging; typing
- Public API names: maybe_flush_dataset_ingest_outbox_inline

### `backend/bff/services/funnel_client.py`
- Module summary: Funnel Service 클라이언트
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=828 | code_lines=731 | risk_score=55
- API surface: public=1 | top-level functions=0 | classes=1 | methods=22
- Runtime signals: async_functions=15 | try=12 | raise=13 | broad_except=11 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=15/22 (68%)
- Internal imports (2): bff.services.base_http_client; shared.config.settings
- External imports (5): hashlib; httpx; io; logging; typing
- Public API names: FunnelClient

### `backend/bff/services/funnel_type_inference_adapter.py`
- Module summary: 🔥 THINK ULTRA! Funnel HTTP Type Inference Service Adapter
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=229 | code_lines=194 | risk_score=6
- API surface: public=1 | top-level functions=0 | classes=1 | methods=11
- Runtime signals: async_functions=8 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=8/11 (72%)
- Internal imports (2): bff.services.funnel_client; shared.interfaces.type_inference
- External imports (2): logging; typing
- Public API names: FunnelHTTPTypeInferenceAdapter

### `backend/bff/services/governance_service.py`
- Module summary: Governance service (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=437 | code_lines=384 | risk_score=5
- API surface: public=16 | top-level functions=16 | classes=0 | methods=0
- Runtime signals: async_functions=16 | try=1 | raise=24 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/16 (6%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (10): bff.routers.role_deps; bff.schemas.governance_requests; bff.services.input_validation_service; shared.errors.error_types; shared.models.requests; shared.observability.tracing; shared.security.database_access; shared.security.input_sanitizer (+2 more)
- External imports (4): __future__; fastapi; logging; typing
- Public API names: create_backing_datasource; create_backing_datasource_version; create_key_spec; get_backing_datasource; get_backing_datasource_version; get_key_spec; handle_request_errors; list_access_policies; list_backing_datasource_versions; list_backing_datasources; list_gate_policies; list_gate_results (+4 more)

### `backend/bff/services/graph_federation_provider.py`
- Module summary: Graph federation dependency provider (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=50 | code_lines=39 | risk_score=1
- API surface: public=1 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/3 (33%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): shared.config.settings; shared.dependencies.container; shared.observability.tracing; shared.services.core.graph_federation_service_es; shared.services.storage.elasticsearch_service
- External imports (1): logging
- Public API names: get_graph_federation_service

### `backend/bff/services/graph_query_service.py`
- Module summary: Graph query domain logic (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=987 | code_lines=877 | risk_score=88
- API surface: public=6 | top-level functions=14 | classes=1 | methods=0
- Runtime signals: async_functions=7 | try=18 | raise=15 | broad_except=17 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=7/14 (50%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (14): shared.config.app_config; shared.config.settings; shared.errors.error_types; shared.models.graph_query; shared.models.lineage_edge_types; shared.observability.tracing; shared.security.input_sanitizer; shared.services.core.graph_federation_service_es (+6 more)
- External imports (5): dataclasses; datetime; fastapi; logging; typing
- Public API names: GraphBranchContext; execute_graph_query; execute_multi_hop_query; execute_simple_graph_query; find_relationship_paths; graph_service_health

### `backend/bff/services/http_idempotency.py`
- Module summary: HTTP idempotency header helpers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=28 | code_lines=19 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.errors.error_types
- External imports (3): __future__; fastapi; typing
- Public API names: get_idempotency_key; require_idempotency_key

### `backend/bff/services/input_validation_service.py`
- Module summary: Shared input validation helpers for BFF service layer.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=52 | code_lines=37 | risk_score=0
- API surface: public=4 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=4 | raise=4 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): shared.errors.error_types; shared.security.auth_utils; shared.security.input_sanitizer
- External imports (3): __future__; fastapi; typing
- Public API names: enforce_db_scope_or_403; sanitized_payload; validated_branch_name; validated_db_name

### `backend/bff/services/instance_async_service.py`
- Module summary: Async instance service (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=335 | code_lines=285 | risk_score=10
- API surface: public=6 | top-level functions=9 | classes=0 | methods=0
- Runtime signals: async_functions=11 | try=2 | raise=6 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/9 (11%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (8): bff.services.oms_client; bff.utils.httpx_exceptions; shared.errors.error_types; shared.models.commands; shared.observability.tracing; shared.security.input_sanitizer; shared.utils.label_mapper; shared.utils.language
- External imports (5): __future__; fastapi; httpx; logging; typing
- Public API names: bulk_create_instances_async; convert_labels_to_ids; create_instance_async; delete_instance_async; resolve_class_id; update_instance_async

### `backend/bff/services/instances_service.py`
- Module summary: Instance domain logic (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=1044 | code_lines=954 | risk_score=55
- API surface: public=4 | top-level functions=13 | classes=1 | methods=0
- Runtime signals: async_functions=5 | try=16 | raise=40 | broad_except=11 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/13 (7%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (16): bff.utils.action_log_serialization; shared.config.app_config; shared.config.search_config; shared.config.settings; shared.errors.error_types; shared.observability.tracing; shared.security.database_access; shared.security.input_sanitizer (+8 more)
- External imports (6): dataclasses; elasticsearch; fastapi; logging; typing; uuid
- Public API names: OverlayContext; get_class_sample_values; get_instance_detail; list_class_instances

### `backend/bff/services/label_mapping_service.py`
- Module summary: Label mapping domain logic (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=646 | code_lines=548 | risk_score=60
- API surface: public=7 | top-level functions=15 | classes=5 | methods=4
- Runtime signals: async_functions=14 | try=15 | raise=26 | broad_except=12 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=9/15 (60%) | classes=2/5 (40%) | methods=0/4 (0%)
- Internal imports (6): bff.services.oms_client; shared.errors.error_envelope; shared.errors.error_types; shared.observability.request_context; shared.security.input_sanitizer; shared.utils.label_mapper
- External imports (10): __future__; abc; dataclasses; datetime; fastapi; hashlib; json; logging (+2 more)
- Public API names: MappingBundleContext; MappingImportPayload; clear_mappings; export_mappings; get_mappings_summary; import_mappings; validate_mappings

### `backend/bff/services/lineage_out_of_date_service.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=810 | code_lines=745 | risk_score=1
- API surface: public=2 | top-level functions=19 | classes=2 | methods=14
- Runtime signals: async_functions=15 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/19 (0%) | classes=1/2 (50%) | methods=0/14 (0%)
- Internal imports (2): shared.config.settings; shared.models.lineage_edge_types
- External imports (3): __future__; datetime; typing
- Public API names: LineageOutOfDateService; LineageOutOfDateStore

### `backend/bff/services/link_types_mapping_service.py`
- Module summary: Link type relationship-spec mapping helpers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=677 | code_lines=601 | risk_score=0
- API surface: public=14 | top-level functions=15 | classes=5 | methods=4
- Runtime signals: async_functions=7 | try=0 | raise=29 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/15 (0%) | classes=0/5 (0%) | methods=0/4 (0%)
- Internal imports (16): bff.schemas.link_types_requests; bff.schemas.objectify_requests; bff.services.oms_client; shared.errors.error_types; shared.errors.external_codes; shared.observability.tracing; shared.security.auth_utils; shared.services.pipeline.pipeline_schema_utils (+8 more)
- External imports (5): __future__; dataclasses; fastapi; typing; uuid
- Public API names: build_join_schema; build_mapping_request; compute_schema_hash; ensure_join_dataset; extract_ontology_properties; extract_ontology_relationships; extract_schema_columns; extract_schema_types; normalize_pk_fields; normalize_policy; normalize_spec_type; resolve_dataset_and_version (+2 more)

### `backend/bff/services/link_types_write_service.py`
- Module summary: Link type write service (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=554 | code_lines=496 | risk_score=15
- API surface: public=3 | top-level functions=7 | classes=0 | methods=0
- Runtime signals: async_functions=5 | try=7 | raise=25 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/7 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (13): bff.schemas.link_types_requests; bff.services.input_validation_service; bff.services.link_types_mapping_service; bff.services.oms_client; bff.services.ontology_occ_guard_service; bff.utils.httpx_exceptions; shared.errors.error_types; shared.models.requests (+5 more)
- External imports (6): __future__; fastapi; httpx; logging; typing; uuid
- Public API names: create_link_type; reindex_link_type; update_link_type

### `backend/bff/services/mapping_suggestion_service.py`
- Module summary: 온톨로지 매핑 제안 서비스
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=1035 | code_lines=759 | risk_score=2
- API surface: public=3 | top-level functions=0 | classes=3 | methods=26
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=3/3 (100%) | methods=25/26 (96%)
- Internal imports (0): not documented
- External imports (9): collections; dataclasses; difflib; logging; math; re; statistics; typing (+1 more)
- Public API names: MappingCandidate; MappingSuggestion; MappingSuggestionService

### `backend/bff/services/object_type_contract_service.py`
- Module summary: Object type contract service (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=879 | code_lines=821 | risk_score=45
- API surface: public=2 | top-level functions=9 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=9 | raise=37 | broad_except=9 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/9 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (18): bff.routers.objectify_job_ops; bff.schemas.object_types_requests; bff.schemas.objectify_requests; bff.services.mapping_suggestion_service; bff.services.objectify_mapping_spec_service; bff.services.oms_client; bff.services.ontology_occ_guard_service; shared.errors.error_types (+10 more)
- External imports (5): __future__; fastapi; httpx; logging; typing
- Public API names: create_object_type_contract; update_object_type_contract

### `backend/bff/services/objectify_dag_service.py`
- Module summary: Objectify DAG orchestration service (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=674 | code_lines=605 | risk_score=10
- API surface: public=1 | top-level functions=1 | classes=2 | methods=18
- Runtime signals: async_functions=13 | try=6 | raise=21 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=0/2 (0%) | methods=0/18 (0%)
- Internal imports (14): bff.schemas.objectify_requests; bff.services.objectify_ops_service; bff.services.oms_client; bff.utils.httpx_exceptions; shared.errors.error_types; shared.models.objectify_job; shared.models.requests; shared.observability.tracing (+6 more)
- External imports (11): __future__; asyncio; collections; dataclasses; fastapi; heapq; httpx; logging (+3 more)
- Public API names: run_objectify_dag

### `backend/bff/services/objectify_mapping_spec_service.py`
- Module summary: Objectify mapping spec service (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=638 | code_lines=609 | risk_score=15
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=5 | raise=42 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (15): bff.routers.objectify_deps; bff.schemas.objectify_requests; bff.services.objectify_ops_service; bff.services.oms_client; shared.errors.error_types; shared.errors.external_codes; shared.models.requests; shared.observability.tracing (+7 more)
- External imports (4): fastapi; httpx; logging; typing
- Public API names: create_mapping_spec

### `backend/bff/services/objectify_ops_service.py`
- Module summary: Objectify helper utilities (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=126 | code_lines=95 | risk_score=0
- API surface: public=0 | top-level functions=10 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/10 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (8): bff.services.link_types_mapping_service; shared.services.pipeline.pipeline_schema_utils; shared.utils.import_type_normalization; shared.utils.objectify_outputs; shared.utils.payload_utils; shared.utils.schema_columns; shared.utils.schema_hash; shared.utils.schema_type_compatibility
- External imports (2): __future__; typing
- Public API names: not documented

### `backend/bff/services/objectify_run_service.py`
- Module summary: Objectify run orchestration (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=309 | code_lines=284 | risk_score=10
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=3 | raise=28 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (16): bff.routers.objectify_deps; bff.schemas.objectify_requests; shared.errors.error_types; shared.models.objectify_job; shared.models.requests; shared.observability.tracing; shared.security.auth_utils; shared.security.database_access (+8 more)
- External imports (5): __future__; fastapi; logging; typing; uuid
- Public API names: run_objectify

### `backend/bff/services/oms_client.py`
- Module summary: OMS (Ontology Management Service) 클라이언트
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=595 | code_lines=526 | risk_score=110
- API surface: public=2 | top-level functions=0 | classes=2 | methods=32
- Runtime signals: async_functions=30 | try=23 | raise=24 | broad_except=22 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=2/2 (100%) | methods=28/32 (87%)
- Internal imports (2): bff.services.base_http_client; shared.config.settings
- External imports (4): dataclasses; httpx; logging; typing
- Public API names: OMSClient; OntologyRef

### `backend/bff/services/oms_error_policy.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=57 | code_lines=46 | risk_score=0
- API surface: public=1 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=4 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): bff.utils.httpx_exceptions; shared.errors.error_types; shared.security.input_sanitizer
- External imports (5): __future__; fastapi; httpx; logging; typing
- Public API names: raise_oms_boundary_exception

### `backend/bff/services/ontology_class_id_service.py`
- Module summary: Shared ontology class id resolution helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=28 | code_lines=21 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (4): bff.services.ontology_ops_service; shared.errors.error_types; shared.security.input_sanitizer; shared.utils.id_generator
- External imports (2): __future__; typing
- Public API names: resolve_or_generate_class_id

### `backend/bff/services/ontology_crud_service.py`
- Module summary: Ontology CRUD service (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=172 | code_lines=143 | risk_score=16
- API surface: public=3 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=4 | try=3 | raise=2 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=3/4 (75%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (12): bff.routers.ontology_ops; bff.services.oms_client; bff.services.oms_error_policy; bff.services.ontology_class_id_service; bff.services.ontology_label_mapper_service; shared.errors.error_types; shared.models.ontology; shared.models.responses (+4 more)
- External imports (4): __future__; fastapi; logging; typing
- Public API names: create_ontology; get_ontology_schema; validate_ontology_create

### `backend/bff/services/ontology_extensions_service.py`
- Module summary: Ontology extensions service (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=264 | code_lines=234 | risk_score=5
- API surface: public=10 | top-level functions=11 | classes=0 | methods=0
- Runtime signals: async_functions=11 | try=1 | raise=4 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/11 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (7): bff.schemas.ontology_extensions_requests; bff.services.oms_client; bff.services.ontology_occ_guard_service; bff.utils.httpx_exceptions; shared.errors.error_types; shared.observability.tracing; shared.security.input_sanitizer
- External imports (5): __future__; fastapi; httpx; logging; typing
- Public API names: approve_ontology_proposal; create_ontology_proposal; create_resource; delete_resource; deploy_ontology; get_resource; list_ontology_proposals; list_resources; ontology_health; update_resource

### `backend/bff/services/ontology_imports_service.py`
- Module summary: Ontology import service (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=665 | code_lines=578 | risk_score=20
- API surface: public=4 | top-level functions=4 | classes=7 | methods=17
- Runtime signals: async_functions=10 | try=4 | raise=18 | broad_except=4 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=4/4 (100%) | classes=0/7 (0%) | methods=0/17 (0%)
- Internal imports (9): bff.routers.ontology_ops; bff.schemas.ontology_requests; bff.services.oms_client; bff.services.sheet_import_parsing; bff.services.sheet_import_service; bff.utils.httpx_exceptions; shared.errors.error_types; shared.observability.tracing (+1 more)
- External imports (6): __future__; dataclasses; fastapi; httpx; logging; typing
- Public API names: commit_import_from_excel; commit_import_from_google_sheets; dry_run_import_from_excel; dry_run_import_from_google_sheets

### `backend/bff/services/ontology_label_mapper_service.py`
- Module summary: Ontology label mapper helpers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=70 | code_lines=59 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/2 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.observability.tracing; shared.utils.label_mapper
- External imports (2): __future__; typing
- Public API names: map_relationship_targets; register_ontology_label_mappings

### `backend/bff/services/ontology_occ_guard_service.py`
- Module summary: Ontology optimistic concurrency guard helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=74 | code_lines=59 | risk_score=0
- API surface: public=3 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): bff.services.oms_client; shared.observability.tracing
- External imports (2): __future__; typing
- Public API names: fetch_branch_head_commit_id; resolve_branch_head_commit_with_bootstrap; resolve_expected_head_commit

### `backend/bff/services/ontology_ops_service.py`
- Module summary: Ontology helper utilities (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=286 | code_lines=231 | risk_score=0
- API surface: public=0 | top-level functions=10 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/10 (20%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.id_generator
- External imports (3): __future__; logging; typing
- Public API names: not documented

### `backend/bff/services/ontology_suggestions_service.py`
- Module summary: Ontology suggestion service (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=500 | code_lines=427 | risk_score=35
- API surface: public=6 | top-level functions=9 | classes=0 | methods=0
- Runtime signals: async_functions=6 | try=7 | raise=16 | broad_except=7 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/9 (11%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (7): bff.routers.ontology_ops; bff.schemas.ontology_requests; bff.services.sheet_import_parsing; bff.utils.httpx_exceptions; shared.errors.error_types; shared.observability.tracing; shared.security.input_sanitizer
- External imports (7): __future__; fastapi; httpx; json; logging; pathlib; typing
- Public API names: suggest_mappings; suggest_mappings_from_excel; suggest_mappings_from_google_sheets; suggest_schema_from_data; suggest_schema_from_excel; suggest_schema_from_google_sheets

### `backend/bff/services/pipeline_agent_autonomous_loop.py`
- Module summary: Autonomous Pipeline Agent loop (single agent + tools).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=3328 | code_lines=2875 | risk_score=73
- API surface: public=5 | top-level functions=39 | classes=4 | methods=6
- Runtime signals: async_functions=7 | try=15 | raise=7 | broad_except=13 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=24/39 (61%) | classes=1/4 (25%) | methods=0/6 (0%)
- Internal imports (14): bff.services.pipeline_plan_models; bff.services.pipeline_plan_validation; shared.config.model_context_limits; shared.models.pipeline_plan; shared.observability.tracing; shared.services.agent.llm_gateway; shared.services.agent.llm_quota; shared.services.core.audit_log_store (+6 more)
- External imports (7): __future__; dataclasses; json; logging; pydantic; typing; uuid
- Public API names: AutonomousPipelineAgentDecision; AutonomousPipelineAgentToolCall; StreamEvent; run_pipeline_agent_mcp_autonomous; run_pipeline_agent_streaming

### `backend/bff/services/pipeline_catalog_service.py`
- Module summary: Pipeline catalog domain logic (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=280 | code_lines=263 | risk_score=25
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=5 | raise=12 | broad_except=5 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (11): bff.routers.pipeline_ops; bff.routers.pipeline_ops_preflight; bff.routers.pipeline_shared; shared.config.app_config; shared.errors.error_types; shared.models.requests; shared.observability.tracing; shared.security.input_sanitizer (+3 more)
- External imports (4): fastapi; logging; typing; uuid
- Public API names: create_pipeline; list_pipelines

### `backend/bff/services/pipeline_cleansing_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=274 | code_lines=244 | risk_score=0
- API surface: public=1 | top-level functions=8 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/8 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline.pipeline_graph_utils
- External imports (2): __future__; typing
- Public API names: apply_cleansing_transforms

### `backend/bff/services/pipeline_dataset_media_upload_service.py`
- Module summary: Pipeline dataset media upload domain logic (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=370 | code_lines=347 | risk_score=5
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=2 | raise=7 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (12): bff.routers.pipeline_datasets_ops; bff.services.dataset_ingest_commit_service; bff.services.dataset_ingest_failures; bff.services.dataset_ingest_idempotency; bff.services.dataset_ingest_outbox_builder; bff.services.dataset_ingest_outbox_flusher; bff.services.pipeline_dataset_upload_context; shared.errors.error_types (+4 more)
- External imports (5): fastapi; hashlib; logging; typing; uuid
- Public API names: upload_media_dataset

### `backend/bff/services/pipeline_dataset_upload_context.py`
- Module summary: Shared helpers for pipeline dataset uploads (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=90 | code_lines=74 | risk_score=0
- API surface: public=0 | top-level functions=2 | classes=1 | methods=0
- Runtime signals: async_functions=1 | try=1 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (6): bff.services.http_idempotency; shared.errors.error_types; shared.security.auth_utils; shared.security.input_sanitizer; shared.services.registries.pipeline_registry; shared.utils.path_utils
- External imports (4): __future__; dataclasses; fastapi; typing
- Public API names: not documented

### `backend/bff/services/pipeline_dataset_upload_service.py`
- Module summary: Pipeline dataset upload domain logic (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=328 | code_lines=306 | risk_score=5
- API surface: public=3 | top-level functions=2 | classes=2 | methods=0
- Runtime signals: async_functions=2 | try=3 | raise=4 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/2 (0%) | methods=0/0 (n/a)
- Internal imports (8): bff.routers.pipeline_datasets_ops; bff.services.dataset_ingest_commit_service; bff.services.dataset_ingest_failures; bff.services.dataset_ingest_idempotency; bff.services.dataset_ingest_outbox_builder; bff.services.dataset_ingest_outbox_flusher; shared.errors.error_types; shared.observability.tracing
- External imports (6): __future__; asyncio; dataclasses; fastapi; logging; typing
- Public API names: TabularDatasetUploadInput; TabularDatasetUploadResult; upload_tabular_dataset

### `backend/bff/services/pipeline_dataset_version_service.py`
- Module summary: Pipeline dataset version domain logic (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=310 | code_lines=292 | risk_score=10
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=4 | raise=9 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (13): bff.routers.pipeline_datasets_ops; bff.services.dataset_ingest_commit_service; bff.services.dataset_ingest_failures; bff.services.dataset_ingest_idempotency; bff.services.dataset_ingest_outbox_builder; bff.services.dataset_ingest_outbox_flusher; shared.errors.error_types; shared.models.requests (+5 more)
- External imports (4): fastapi; logging; typing; uuid
- Public API names: create_dataset_version

### `backend/bff/services/pipeline_detail_service.py`
- Module summary: Pipeline detail domain logic (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=502 | code_lines=464 | risk_score=30
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=6 | raise=18 | broad_except=6 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (12): bff.routers.pipeline_ops; bff.routers.pipeline_ops_preflight; bff.routers.pipeline_shared; shared.config.app_config; shared.config.settings; shared.errors.error_types; shared.models.requests; shared.observability.tracing (+4 more)
- External imports (3): fastapi; logging; typing
- Public API names: get_pipeline; get_pipeline_readiness; update_pipeline

### `backend/bff/services/pipeline_execution_service.py`
- Module summary: Pipeline execution domain logic (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=1667 | code_lines=1579 | risk_score=55
- API surface: public=3 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=15 | raise=71 | broad_except=11 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (27): bff.routers.pipeline_ops; bff.routers.pipeline_shared; bff.services.oms_client; bff.services.ontology_occ_guard_service; shared.dependencies.providers; shared.errors.error_envelope; shared.errors.error_types; shared.models.lineage_edge_types (+19 more)
- External imports (5): fastapi; httpx; logging; typing; uuid
- Public API names: build_pipeline; deploy_pipeline; preview_pipeline

### `backend/bff/services/pipeline_join_evaluator.py`
- Module summary: Pipeline join evaluator.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=337 | code_lines=284 | risk_score=7
- API surface: public=2 | top-level functions=5 | classes=1 | methods=0
- Runtime signals: async_functions=1 | try=2 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/5 (0%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (6): shared.observability.tracing; shared.services.pipeline.pipeline_executor; shared.services.pipeline.pipeline_graph_utils; shared.services.pipeline.pipeline_math_utils; shared.services.pipeline.pipeline_transform_spec; shared.services.storage.storage_service
- External imports (4): __future__; dataclasses; logging; typing
- Public API names: JoinEvaluation; evaluate_pipeline_joins

### `backend/bff/services/pipeline_plan_autonomous_compiler.py`
- Module summary: Pipeline plan compiler (single autonomous loop + MCP tools).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=212 | code_lines=183 | risk_score=19
- API surface: public=1 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=4 | raise=0 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/4 (25%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (10): bff.services.pipeline_agent_autonomous_loop; bff.services.pipeline_plan_models; shared.models.pipeline_plan; shared.observability.tracing; shared.services.agent.llm_gateway; shared.services.core.audit_log_store; shared.services.registries.dataset_registry; shared.services.registries.pipeline_plan_registry (+2 more)
- External imports (4): __future__; logging; typing; uuid
- Public API names: compile_pipeline_plan_mcp_autonomous

### `backend/bff/services/pipeline_plan_models.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=60 | code_lines=48 | risk_score=0
- API surface: public=2 | top-level functions=0 | classes=2 | methods=1
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/2 (0%) | methods=1/1 (100%)
- Internal imports (3): shared.models.agent_plan_report; shared.models.pipeline_plan; shared.services.agent.llm_gateway
- External imports (4): __future__; dataclasses; pydantic; typing
- Public API names: PipelineClarificationQuestion; PipelinePlanCompileResult

### `backend/bff/services/pipeline_plan_preview_service.py`
- Module summary: Pipeline plan preview service (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=339 | code_lines=304 | risk_score=7
- API surface: public=3 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=5 | try=2 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (16): bff.schemas.pipeline_plans_requests; bff.services.pipeline_join_evaluator; bff.services.pipeline_plan_preview_utils; bff.services.pipeline_plan_scoping_service; bff.services.pipeline_plan_validation; shared.models.pipeline_plan; shared.models.responses; shared.observability.tracing (+8 more)
- External imports (4): __future__; fastapi; logging; typing
- Public API names: evaluate_joins; inspect_plan_preview; preview_plan

### `backend/bff/services/pipeline_plan_preview_utils.py`
- Module summary: Pipeline plan preview utilities (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=207 | code_lines=179 | risk_score=0
- API surface: public=0 | top-level functions=10 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=12 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/10 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.security.input_sanitizer; shared.utils.canonical_json
- External imports (3): __future__; re; typing
- Public API names: not documented

### `backend/bff/services/pipeline_plan_scoping_service.py`
- Module summary: Pipeline plan scoping helpers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=83 | code_lines=66 | risk_score=10
- API surface: public=1 | top-level functions=5 | classes=1 | methods=0
- Runtime signals: async_functions=2 | try=2 | raise=5 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/5 (0%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (5): bff.services.input_validation_service; bff.services.pipeline_plan_tenant_service; shared.errors.error_types; shared.models.pipeline_plan; shared.services.registries.pipeline_plan_registry
- External imports (5): __future__; dataclasses; fastapi; typing; uuid
- Public API names: PipelinePlanRequestContext

### `backend/bff/services/pipeline_plan_tenant_service.py`
- Module summary: Pipeline plan tenant helpers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=76 | code_lines=57 | risk_score=5
- API surface: public=5 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=1 | raise=1 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): bff.routers.registry_deps; shared.errors.error_types; shared.observability.tracing
- External imports (4): __future__; fastapi; logging; typing
- Public API names: require_verified_user; resolve_actor; resolve_tenant_id; resolve_tenant_policy; resolve_verified_tenant_user

### `backend/bff/services/pipeline_plan_validation.py`
- Module summary: Pipeline plan validation.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=168 | code_lines=141 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=1 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (11): bff.routers; shared.models.agent_plan_report; shared.models.pipeline_plan; shared.models.pipeline_task_spec; shared.observability.tracing; shared.services.pipeline.output_plugins; shared.services.pipeline.pipeline_graph_utils; shared.services.pipeline.pipeline_task_spec_policy (+3 more)
- External imports (3): __future__; dataclasses; typing
- Public API names: PipelinePlanValidationResult; validate_pipeline_plan

### `backend/bff/services/pipeline_proposal_service.py`
- Module summary: Pipeline proposal domain logic (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=468 | code_lines=436 | risk_score=20
- API surface: public=5 | top-level functions=7 | classes=0 | methods=0
- Runtime signals: async_functions=5 | try=5 | raise=18 | broad_except=4 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/7 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (9): bff.routers.pipeline_shared; shared.errors.error_types; shared.models.requests; shared.observability.tracing; shared.security.input_sanitizer; shared.services.registries.pipeline_registry; shared.services.storage.lakefs_client; shared.utils.schema_hash (+1 more)
- External imports (3): fastapi; logging; typing
- Public API names: approve_pipeline_proposal; list_pipeline_proposals; normalize_mapping_spec_ids; reject_pipeline_proposal; submit_pipeline_proposal

### `backend/bff/services/pipeline_tabular_upload_facade.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=85 | code_lines=78 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): bff.routers.pipeline_datasets_ops; bff.services.pipeline_dataset_upload_context; bff.services.pipeline_dataset_upload_service; shared.models.requests; shared.observability.tracing
- External imports (2): __future__; typing
- Public API names: finalize_tabular_upload

### `backend/bff/services/pipeline_udf_service.py`
- Module summary: Pipeline UDF domain logic (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=208 | code_lines=185 | risk_score=30
- API surface: public=5 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=5 | try=8 | raise=16 | broad_except=6 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): shared.errors.error_types; shared.models.requests; shared.observability.tracing; shared.security.input_sanitizer; shared.services.pipeline.pipeline_udf_runtime
- External imports (4): fastapi; logging; typing; uuid
- Public API names: create_udf; create_udf_version; get_udf; get_udf_version; list_udfs

### `backend/bff/services/relationship_reconciler_service.py`
- Module summary: Relationship Reconciler Service.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=466 | code_lines=378 | risk_score=32
- API surface: public=1 | top-level functions=8 | classes=0 | methods=0
- Runtime signals: async_functions=7 | try=7 | raise=0 | broad_except=5 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=8/8 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): bff.services.oms_client; shared.config.search_config; shared.observability.tracing; shared.services.registries.objectify_registry; shared.services.storage.elasticsearch_service
- External imports (5): __future__; collections; elasticsearch; logging; typing
- Public API names: reconcile_relationships

### `backend/bff/services/schema_changes_service.py`
- Module summary: Schema changes domain logic (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=474 | code_lines=410 | risk_score=35
- API surface: public=7 | top-level functions=7 | classes=0 | methods=0
- Runtime signals: async_functions=7 | try=7 | raise=15 | broad_except=7 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/7 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): shared.errors.error_types; shared.models.requests; shared.observability.tracing
- External imports (5): datetime; fastapi; logging; typing; uuid
- Public API names: acknowledge_drift; check_mapping_compatibility; create_subscription; delete_subscription; get_schema_change_stats; list_schema_changes; list_subscriptions

### `backend/bff/services/sheet_import_parsing.py`
- Module summary: Sheet import parsing helpers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=105 | code_lines=87 | risk_score=10
- API surface: public=5 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=2 | raise=9 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.errors.error_types; shared.observability.tracing
- External imports (4): __future__; fastapi; json; typing
- Public API names: parse_json_array; parse_json_object; parse_table_bbox; parse_target_schema_json; read_excel_upload

### `backend/bff/services/sheet_import_service.py`
- Module summary: Backward-compatible import location.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=11 | code_lines=7 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.core.sheet_import_service
- External imports (0): not documented
- Public API names: not documented

### `backend/bff/services/tasks_service.py`
- Module summary: Background task domain logic (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=102 | code_lines=80 | risk_score=0
- API surface: public=5 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=5 | try=0 | raise=4 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (4): bff.schemas.tasks_requests; shared.errors.error_types; shared.models.background_task; shared.observability.tracing
- External imports (3): __future__; datetime; typing
- Public API names: cancel_task; get_task_metrics; get_task_result; get_task_status; list_tasks

### `backend/bff/services/websocket_service.py`
- Module summary: WebSocket session orchestration (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=258 | code_lines=221 | risk_score=12
- API surface: public=3 | top-level functions=8 | classes=0 | methods=0
- Runtime signals: async_functions=7 | try=2 | raise=0 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/8 (12%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): bff.middleware.auth; shared.observability.tracing; shared.services.core.websocket_service
- External imports (7): __future__; fastapi; json; logging; re; typing; uuid
- Public API names: handle_client_message; run_command_updates; run_user_updates

### `backend/bff/tests/test_actions_batch_and_undo_proxy.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=63 | code_lines=53 | risk_score=2
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): bff.dependencies; bff.main
- External imports (2): fastapi; unittest
- Public API names: test_action_submit_batch_forwards_actor_identity; test_action_undo_forwards_actor_identity

### `backend/bff/tests/test_actions_submit_actor_metadata.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=33 | code_lines=25 | risk_score=1
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): bff.dependencies; bff.main
- External imports (2): fastapi; unittest
- Public API names: test_action_submit_forwards_actor_identity_from_headers

### `backend/bff/tests/test_ai_query_router.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=464 | code_lines=402 | risk_score=8
- API surface: public=10 | top-level functions=11 | classes=2 | methods=3
- Runtime signals: async_functions=3 | try=9 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/11 (0%) | classes=0/2 (0%) | methods=0/3 (0%)
- Internal imports (7): bff.dependencies; bff.main; bff.routers; shared.dependencies.providers; shared.errors.error_types; shared.models.ai; shared.services.agent.llm_gateway
- External imports (6): dataclasses; datetime; fastapi; httpx; pytest; unittest
- Public API names: client; test_ai_intent_does_not_override_greeting_route; test_ai_intent_passes_through_llm_response; test_ai_query_dataset_list_executes_and_answers; test_ai_query_label_query_executes_and_answers; test_ai_query_label_query_maps_upstream_5xx_to_503; test_ai_query_label_query_propagates_http_errors; test_ai_query_unsupported_returns_guidance_templates; test_translate_query_plan_returns_graph_query_with_paths; test_translate_query_plan_returns_plan

### `backend/bff/tests/test_ci_webhooks_router.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=88 | code_lines=75 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=2 | methods=5
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/2 (0%) | methods=0/5 (0%)
- Internal imports (2): bff.routers.ci_webhooks; shared.services.registries.agent_session_registry
- External imports (4): __future__; datetime; pytest; types
- Public API names: test_ci_webhook_ingest_records_ci_result

### `backend/bff/tests/test_command_status_router.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=25 | code_lines=19 | risk_score=0
- API surface: public=2 | top-level functions=1 | classes=1 | methods=2
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (1): bff.routers.command_status
- External imports (1): pytest
- Public API names: DummyOMSClient; test_command_status_proxies_to_api_v1_commands_status_path

### `backend/bff/tests/test_context_tools_router.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=127 | code_lines=103 | risk_score=0
- API surface: public=3 | top-level functions=4 | classes=4 | methods=6
- Runtime signals: async_functions=6 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/4 (0%) | methods=0/6 (0%)
- Internal imports (2): bff.routers.context_tools; shared.security.user_context
- External imports (5): __future__; datetime; fastapi; pytest; types
- Public API names: test_context_tools_dataset_describe_constrains_allowed_dataset_ids; test_context_tools_dataset_describe_enforces_db_policy; test_context_tools_ontology_snapshot_enforces_policy

### `backend/bff/tests/test_dataset_ingest_idempotency.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=345 | code_lines=297 | risk_score=0
- API surface: public=1 | top-level functions=3 | classes=9 | methods=20
- Runtime signals: async_functions=19 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/9 (0%) | methods=0/20 (0%)
- Internal imports (2): bff.routers; bff.routers.pipeline_datasets
- External imports (8): __future__; dataclasses; fastapi; io; pytest; starlette; typing; uuid
- Public API names: test_csv_upload_idempotency_key_reuses_version

### `backend/bff/tests/test_document_bundles_router.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=79 | code_lines=59 | risk_score=0
- API surface: public=2 | top-level functions=3 | classes=3 | methods=5
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/3 (0%) | methods=0/5 (0%)
- Internal imports (2): bff.routers.document_bundles; shared.security.user_context
- External imports (4): __future__; fastapi; pytest; types
- Public API names: test_document_bundle_search_enforces_allowed_bundle_ids; test_document_bundle_search_returns_citations

### `backend/bff/tests/test_foundry_ontology_v2_router.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=1843 | code_lines=1648 | risk_score=41
- API surface: public=52 | top-level functions=53 | classes=8 | methods=15
- Runtime signals: async_functions=68 | try=46 | raise=5 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/53 (0%) | classes=0/8 (0%) | methods=0/15 (0%)
- Internal imports (2): bff.routers; shared.utils.foundry_page_token
- External imports (6): __future__; fastapi; httpx; json; pytest; starlette
- Public API names: test_get_action_type_by_rid_v2_returns_foundry_raw_shape; test_get_action_type_v2_returns_foundry_raw_shape; test_get_full_metadata_v2_allows_preview_false; test_get_full_metadata_v2_omits_partial_entities_when_upstream_unavailable; test_get_full_metadata_v2_returns_foundry_full_metadata_shape; test_get_linked_object_v2_not_found_returns_foundry_error; test_get_linked_object_v2_returns_foundry_raw_shape; test_get_object_type_full_metadata_v2_missing_returns_object_type_not_found; test_get_object_type_full_metadata_v2_returns_foundry_shape; test_get_object_type_v2_missing_returns_object_type_not_found; test_get_object_type_v2_returns_foundry_raw_shape; test_get_object_v2_not_found_returns_foundry_error (+40 more)

### `backend/bff/tests/test_funnel_client_structure_selection.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=124 | code_lines=110 | risk_score=1
- API surface: public=1 | top-level functions=0 | classes=1 | methods=6
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/1 (0%) | methods=0/6 (0%)
- Internal imports (1): bff.services.funnel_client
- External imports (0): not documented
- Public API names: TestFunnelClientStructureSelection

### `backend/bff/tests/test_i18n_language_selection.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=57 | code_lines=41 | risk_score=2
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=2 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): bff.dependencies; bff.main; bff.routers
- External imports (3): elasticsearch; fastapi; unittest
- Public API names: test_bff_health_message_localizes_by_lang_param; test_bff_http_exception_detail_localizes_by_lang_param

### `backend/bff/tests/test_import_commit_wiring.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=122 | code_lines=98 | risk_score=2
- API surface: public=2 | top-level functions=2 | classes=1 | methods=4
- Runtime signals: async_functions=4 | try=2 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/1 (0%) | methods=0/4 (0%)
- Internal imports (2): bff.dependencies; bff.main
- External imports (3): fastapi; json; unittest
- Public API names: test_excel_import_commit_submits_to_oms; test_google_sheets_import_commit_submits_to_oms

### `backend/bff/tests/test_instance_async_label_payload.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=53 | code_lines=41 | risk_score=1
- API surface: public=1 | top-level functions=1 | classes=1 | methods=2
- Runtime signals: async_functions=2 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (2): bff.dependencies; bff.main
- External imports (2): fastapi; unittest
- Public API names: test_instance_create_allows_label_keys_with_spaces

### `backend/bff/tests/test_instances_access_policy.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=68 | code_lines=50 | risk_score=2
- API surface: public=2 | top-level functions=3 | classes=3 | methods=5
- Runtime signals: async_functions=3 | try=2 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/3 (0%) | methods=0/5 (0%)
- Internal imports (3): bff.dependencies; bff.main; bff.routers
- External imports (2): fastapi; types
- Public API names: test_instance_get_hides_rows_blocked_by_access_policy; test_instances_list_masks_fields_with_access_policy

### `backend/bff/tests/test_link_types_auto_join_table.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=109 | code_lines=94 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=1 | methods=7
- Runtime signals: async_functions=7 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/1 (0%) | methods=0/7 (0%)
- Internal imports (1): bff.routers
- External imports (3): pytest; starlette; types
- Public API names: test_ensure_join_dataset_auto_creates_dataset_and_version

### `backend/bff/tests/test_link_types_fk_validation.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=124 | code_lines=109 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=1 | methods=7
- Runtime signals: async_functions=7 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/1 (0%) | methods=0/7 (0%)
- Internal imports (1): bff.routers
- External imports (4): fastapi; pytest; starlette; types
- Public API names: test_fk_type_mismatch_is_rejected

### `backend/bff/tests/test_link_types_join_table_validation.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=152 | code_lines=135 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=1 | methods=4
- Runtime signals: async_functions=5 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/1 (0%) | methods=0/4 (0%)
- Internal imports (1): bff.routers
- External imports (4): fastapi; pytest; starlette; types
- Public API names: test_join_table_missing_target_column_is_rejected; test_join_table_source_type_mismatch_is_rejected

### `backend/bff/tests/test_link_types_link_edits.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=72 | code_lines=56 | risk_score=1
- API surface: public=2 | top-level functions=3 | classes=1 | methods=3
- Runtime signals: async_functions=2 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/1 (0%) | methods=0/3 (0%)
- Internal imports (2): bff.main; bff.routers
- External imports (2): fastapi; types
- Public API names: test_link_edit_records_when_enabled; test_link_edit_rejected_when_disabled

### `backend/bff/tests/test_link_types_retrieval.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=88 | code_lines=69 | risk_score=1
- API surface: public=2 | top-level functions=2 | classes=2 | methods=5
- Runtime signals: async_functions=6 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/2 (0%) | methods=0/5 (0%)
- Internal imports (1): bff.routers
- External imports (3): pytest; starlette; types
- Public API names: test_get_link_type_includes_relationship_spec_status; test_list_link_types_includes_relationship_spec_status

### `backend/bff/tests/test_mapping_suggestion_service.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=24 | code_lines=18 | risk_score=0
- API surface: public=1 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.services.mapping_suggestion_service
- External imports (0): not documented
- Public API names: test_mapping_suggestion_is_deterministic

### `backend/bff/tests/test_mapping_suggestion_service_domain_neutral.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=33 | code_lines=23 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.services.mapping_suggestion_service
- External imports (0): not documented
- Public API names: test_label_is_used_for_matching_but_id_is_returned; test_semantic_match_disabled_by_default; test_semantic_match_is_opt_in_and_domain_neutral

### `backend/bff/tests/test_object_types_edit_migration.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=174 | code_lines=143 | risk_score=0
- API surface: public=2 | top-level functions=3 | classes=3 | methods=14
- Runtime signals: async_functions=14 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/3 (0%) | methods=0/14 (0%)
- Internal imports (1): bff.routers
- External imports (3): pytest; starlette; types
- Public API names: test_edit_policy_moves_drops_invalidates_are_applied_and_recorded; test_pk_change_with_id_remap_records_plan

### `backend/bff/tests/test_object_types_key_spec_required.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=203 | code_lines=175 | risk_score=2
- API surface: public=2 | top-level functions=3 | classes=3 | methods=11
- Runtime signals: async_functions=14 | try=2 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/3 (0%) | methods=0/11 (0%)
- Internal imports (1): bff.routers
- External imports (4): fastapi; pytest; starlette; types
- Public API names: test_object_type_requires_primary_key; test_object_type_requires_title_key

### `backend/bff/tests/test_object_types_migration_gate.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=198 | code_lines=166 | risk_score=0
- API surface: public=2 | top-level functions=3 | classes=3 | methods=17
- Runtime signals: async_functions=17 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/3 (0%) | methods=0/17 (0%)
- Internal imports (1): bff.routers
- External imports (4): fastapi; pytest; starlette; types
- Public API names: test_object_type_migration_plan_is_recorded; test_object_type_migration_requires_approval

### `backend/bff/tests/test_object_types_swap_reindex.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=167 | code_lines=137 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=3 | methods=19
- Runtime signals: async_functions=16 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/3 (0%) | methods=0/19 (0%)
- Internal imports (1): bff.routers
- External imports (3): pytest; starlette; types
- Public API names: test_object_type_swap_enqueues_reindex

### `backend/bff/tests/test_objectify_mapping_spec_preflight.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=514 | code_lines=434 | risk_score=0
- API surface: public=11 | top-level functions=16 | classes=4 | methods=17
- Runtime signals: async_functions=15 | try=1 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/16 (0%) | classes=0/4 (0%) | methods=0/17 (0%)
- Internal imports (3): bff.dependencies; bff.main; bff.routers
- External imports (3): datetime; fastapi; types
- Public API names: test_mapping_spec_change_summary_is_recorded; test_mapping_spec_dataset_pk_target_mismatch_is_rejected; test_mapping_spec_primary_key_missing_is_rejected; test_mapping_spec_relationship_target_is_rejected; test_mapping_spec_required_missing_is_rejected; test_mapping_spec_source_missing_is_rejected; test_mapping_spec_source_type_incompatible_is_rejected; test_mapping_spec_source_type_unsupported_is_rejected; test_mapping_spec_target_type_mismatch_is_rejected; test_mapping_spec_target_unknown_is_rejected; test_mapping_spec_unsupported_type_is_rejected

### `backend/bff/tests/test_oms_client_http_helpers.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=31 | code_lines=23 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.services.oms_client
- External imports (2): httpx; pytest
- Public API names: test_oms_client_http_helpers_roundtrip_json

### `backend/bff/tests/test_ontology_router_helpers.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=54 | code_lines=40 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.routers
- External imports (1): __future__
- Public API names: test_build_source_schema_and_samples; test_localized_to_string; test_normalize_mapping_type_and_import_target; test_transform_properties_for_oms

### `backend/bff/tests/test_ontology_validate_proxy.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=53 | code_lines=42 | risk_score=2
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): bff.dependencies; bff.main
- External imports (2): fastapi; unittest
- Public API names: test_ontology_validate_create_proxies_to_oms; test_ontology_validate_update_endpoint_removed

### `backend/bff/tests/test_pipeline_audit_logging.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=92 | code_lines=62 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=6 | methods=12
- Runtime signals: async_functions=11 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/6 (0%) | methods=0/12 (0%)
- Internal imports (2): bff.routers; bff.routers.pipeline_detail
- External imports (4): __future__; dataclasses; pytest; typing
- Public API names: test_pipeline_update_writes_audit_log

### `backend/bff/tests/test_pipeline_dataset_version_materialization.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=157 | code_lines=124 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=6 | methods=11
- Runtime signals: async_functions=10 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/6 (0%) | methods=0/11 (0%)
- Internal imports (1): bff.routers.pipeline_datasets
- External imports (5): __future__; dataclasses; datetime; pytest; typing
- Public API names: test_create_dataset_version_materializes_manual_sample_to_artifact

### `backend/bff/tests/test_pipeline_permissions_enforced.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=100 | code_lines=74 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=5 | methods=8
- Runtime signals: async_functions=9 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/5 (0%) | methods=0/8 (0%)
- Internal imports (1): bff.routers.pipeline_detail
- External imports (5): __future__; dataclasses; fastapi; pytest; typing
- Public API names: test_get_pipeline_bootstraps_permissions_when_missing; test_get_pipeline_requires_read_permission

### `backend/bff/tests/test_pipeline_promotion_semantics.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=757 | code_lines=637 | risk_score=0
- API surface: public=10 | top-level functions=13 | classes=11 | methods=33
- Runtime signals: async_functions=40 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/13 (0%) | classes=0/11 (0%) | methods=0/33 (0%)
- Internal imports (1): bff.routers.pipeline_execution
- External imports (5): __future__; dataclasses; fastapi; pytest; typing
- Public API names: lakefs_merge_stub; test_build_enqueues_job_and_records_run; test_build_postgres_profile_uses_branch_ref_when_head_commit_unavailable; test_preview_enqueues_job_with_node_id_and_records_preview_and_run; test_promote_build_allows_breaking_schema_changes_with_replay_flag; test_promote_build_blocks_deploy_when_expectations_failed; test_promote_build_merges_build_branch_to_main_and_registers_version; test_promote_build_rejects_non_staged_artifact_key; test_promote_build_requires_replay_for_breaking_schema_changes; test_promote_build_surfaces_build_errors_when_build_failed

### `backend/bff/tests/test_pipeline_proposal_governance.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=302 | code_lines=256 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=7 | methods=11
- Runtime signals: async_functions=12 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/7 (0%) | methods=0/11 (0%)
- Internal imports (1): bff.routers.pipeline_proposals
- External imports (6): __future__; dataclasses; fastapi; pytest; typing; uuid
- Public API names: test_pipeline_proposal_requires_approve_role; test_pipeline_proposal_requires_pending_status; test_pipeline_proposal_submit_and_approve_flow

### `backend/bff/tests/test_pipeline_router_helpers.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=97 | code_lines=70 | risk_score=0
- API surface: public=10 | top-level functions=10 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/10 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.routers
- External imports (5): __future__; fastapi; pytest; starlette; uuid
- Public API names: test_csv_helpers; test_definition_diff_and_bbox; test_dependency_payload_normalization; test_format_dependencies_for_api; test_idempotency_key_required; test_location_and_dataset_name_helpers; test_normalize_mapping_spec_ids; test_pipeline_protected_branches; test_resolve_principal_and_actor_label; test_schema_change_detection

### `backend/bff/tests/test_pipeline_router_uploads.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=805 | code_lines=690 | risk_score=0
- API surface: public=10 | top-level functions=11 | classes=6 | methods=34
- Runtime signals: async_functions=48 | try=0 | raise=4 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/11 (0%) | classes=0/6 (0%) | methods=0/34 (0%)
- Internal imports (2): bff.routers; shared.services.registries.dataset_registry
- External imports (6): dataclasses; datetime; io; pytest; starlette; types
- Public API names: test_approve_dataset_schema_updates_dataset; test_get_ingest_request_funnel_failure_uses_fallback; test_get_ingest_request_includes_funnel_analysis; test_maybe_enqueue_objectify_job; test_pipeline_helpers_normalize_inputs; test_reanalyze_dataset_version_returns_funnel_analysis; test_upload_csv_dataset_creates_version; test_upload_csv_dataset_funnel_failure_uses_fallback; test_upload_excel_dataset_commits_preview; test_upload_media_dataset_stores_files

### `backend/bff/tests/test_query_builder_info.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=21 | code_lines=15 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.routers.query
- External imports (2): __future__; pytest
- Public API names: test_query_builder_info_exposes_canonical_text_operators_only

### `backend/bff/tests/test_query_foundry_adapter.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=99 | code_lines=84 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): bff.dependencies; shared.utils.foundry_page_token
- External imports (3): __future__; pytest; unittest
- Public API names: test_query_database_adapts_to_foundry_where_and_page_token; test_query_database_maps_not_in_and_not_null_filters; test_query_database_rejects_invalid_order_direction

### `backend/bff/tests/test_security_information_leakage.py`
- Module summary: Security tests for information leakage prevention.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=183 | code_lines=156 | risk_score=5
- API surface: public=1 | top-level functions=0 | classes=2 | methods=9
- Runtime signals: async_functions=6 | try=5 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/2 (0%) | methods=0/9 (0%)
- Internal imports (3): bff.dependencies; bff.main; bff.routers
- External imports (4): elasticsearch; fastapi; pytest; unittest
- Public API names: TestInformationLeakagePrevention

### `backend/bff/tests/test_sheet_import_service.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=60 | code_lines=50 | risk_score=0
- API surface: public=5 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.services.sheet_import_service
- External imports (0): not documented
- Public API names: test_boolean_parsing; test_coerce_date_accepts_common_separators; test_coerce_decimal_with_currency_symbol; test_coerce_integer_with_currency_suffix; test_error_rows_are_reported_and_can_be_filtered

### `backend/bff/tests/test_terminus_service_db_info.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=19 | code_lines=12 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.dependencies
- External imports (3): __future__; pytest; unittest
- Public API names: test_get_database_info_uses_oms_get_database

### `backend/bff/utils/__init__.py`
- Module summary: 유틸리티 모듈
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=3 | code_lines=3 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/bff/utils/action_log_serialization.py`
- Module summary: Action log serialization helpers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=51 | code_lines=41 | risk_score=6
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.registries.action_log_registry
- External imports (3): __future__; logging; typing
- Public API names: dt_iso; serialize_action_log_record

### `backend/bff/utils/httpx_exceptions.py`
- Module summary: HTTPX error helpers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=36 | code_lines=26 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.errors.error_types
- External imports (5): __future__; fastapi; httpx; logging; typing
- Public API names: extract_httpx_detail; raise_httpx_as_http_exception

### `backend/bff/utils/request_headers.py`
- Module summary: Request header helpers (BFF).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=32 | code_lines=24 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; starlette; typing
- Public API names: extract_forward_headers

## conftest.py

### `backend/conftest.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=78 | code_lines=65 | risk_score=0
- API surface: public=0 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.config.settings; shared.utils.repo_dotenv
- External imports (3): __future__; os; pathlib
- Public API names: not documented

## connector_sync_worker

### `backend/connector_sync_worker/__init__.py`
- Module summary: Connector Sync Worker package.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=2 | code_lines=1 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/connector_sync_worker/main.py`
- Module summary: Connector Sync Worker (shared runtime).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service entrypoint and lifecycle wiring
- Source footprint: total_lines=527 | code_lines=455 | risk_score=25
- API surface: public=1 | top-level functions=0 | classes=1 | methods=20
- Runtime signals: async_functions=13 | try=5 | raise=11 | broad_except=5 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/1 (0%) | methods=0/20 (0%)
- Internal imports (24): data_connector.google_sheets.service; data_connector.google_sheets.utils; shared.config.app_config; shared.config.settings; shared.errors.error_types; shared.errors.runtime_exception_policy; shared.models.event_envelope; shared.observability.metrics (+16 more)
- External imports (7): __future__; asyncio; confluent_kafka; datetime; httpx; logging; typing
- Public API names: ConnectorSyncWorker

## connector_trigger_service

### `backend/connector_trigger_service/__init__.py`
- Module summary: Connector Trigger Service package.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=2 | code_lines=1 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/connector_trigger_service/main.py`
- Module summary: Connector Trigger Service (shared runtime).
- Responsibilities: Read connector sources from Postgres registry; Detect external changes (polling/webhooks; v1 implements Google Sheets polling); Transactionally enqueue connector update events into Postgres outbox; Publish outbox to Kafka `connector-updates` as EventEnvelope (metadata.kind='connector_update'); Connector libraries must not decide ontology/mapping. This service only emits change signals.
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service entrypoint and lifecycle wiring
- Source footprint: total_lines=354 | code_lines=304 | risk_score=52
- API surface: public=1 | top-level functions=0 | classes=1 | methods=10
- Runtime signals: async_functions=8 | try=10 | raise=3 | broad_except=9 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/1 (0%) | methods=0/10 (0%)
- Internal imports (13): data_connector.google_sheets.service; data_connector.google_sheets.utils; shared.config.app_config; shared.config.settings; shared.observability.context_propagation; shared.observability.metrics; shared.observability.tracing; shared.services.kafka.producer_factory (+5 more)
- External imports (7): __future__; asyncio; confluent_kafka; json; logging; time; typing
- Public API names: ConnectorTriggerService

## data_connector

### `backend/data_connector/__init__.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=0 | code_lines=0 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/data_connector/google_sheets/__init__.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=0 | code_lines=0 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/data_connector/google_sheets/auth.py`
- Module summary: Google Sheets Connector - Authentication Module (for future OAuth2 support)
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=271 | code_lines=206 | risk_score=6
- API surface: public=2 | top-level functions=0 | classes=2 | methods=12
- Runtime signals: async_functions=4 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=2/2 (100%) | methods=12/12 (100%)
- Internal imports (1): shared.config.settings
- External imports (5): datetime; httpx; logging; typing; urllib
- Public API names: APIKeyAuth; GoogleOAuth2Client

### `backend/data_connector/google_sheets/models.py`
- Module summary: Google Sheets Connector - Request/Response Models
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=89 | code_lines=64 | risk_score=0
- API surface: public=6 | top-level functions=0 | classes=6 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=6/6 (100%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): pydantic; typing
- Public API names: GoogleSheetPreviewRequest; GoogleSheetPreviewResponse; GoogleSheetRegisterRequest; GoogleSheetRegisterResponse; RegisteredSheet; SheetMetadata

### `backend/data_connector/google_sheets/service.py`
- Module summary: Google Sheets Connector - Service Layer (connector library).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=281 | code_lines=245 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=9
- Runtime signals: async_functions=8 | try=5 | raise=5 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=1/9 (11%)
- Internal imports (3): .models; .utils; shared.config.settings
- External imports (4): __future__; httpx; logging; typing
- Public API names: GoogleSheetsService

### `backend/data_connector/google_sheets/utils.py`
- Module summary: Google Sheets Connector - Utility Functions
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=270 | code_lines=195 | risk_score=0
- API surface: public=13 | top-level functions=13 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=13/13 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (6): datetime; hashlib; json; re; typing; urllib
- Public API names: build_sheets_api_url; build_sheets_metadata_url; calculate_data_hash; convert_column_letter_to_index; convert_index_to_column_letter; estimate_data_size; extract_gid; extract_sheet_id; format_datetime_iso; normalize_sheet_data; parse_range_notation; sanitize_worksheet_name (+1 more)

## examples

### `backend/examples/kafka_consumer/consumer_example.py`
- Module summary: Kafka Consumer Example
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=167 | code_lines=115 | risk_score=12
- API surface: public=2 | top-level functions=1 | classes=1 | methods=8
- Runtime signals: async_functions=0 | try=3 | raise=1 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=1/1 (100%) | methods=7/8 (87%)
- Internal imports (1): shared.config.service_config
- External imports (5): confluent_kafka; json; logging; signal; typing
- Public API names: OntologyEventConsumer; main

## funnel

### `backend/funnel/__init__.py`
- Module summary: Funnel Service - Data Processing and Type Inference Layer
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=5 | code_lines=4 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/funnel/main.py`
- Module summary: 🔥 THINK ULTRA! Funnel Service - 독립 마이크로서비스
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service entrypoint and lifecycle wiring
- Source footprint: total_lines=95 | code_lines=61 | risk_score=6
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=3/3 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (4): funnel.routers.type_inference_router; shared.middleware.rate_limiter; shared.services.core.service_factory; shared.utils.app_logger
- External imports (3): contextlib; fastapi; typing
- Public API names: health_check; lifespan; root

### `backend/funnel/routers/__init__.py`
- Module summary: Funnel Routers
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=3 | code_lines=3 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/funnel/routers/type_inference_router.py`
- Module summary: 🔥 THINK ULTRA! Funnel Type Inference Router
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=427 | code_lines=369 | risk_score=45
- API surface: public=11 | top-level functions=11 | classes=0 | methods=0
- Runtime signals: async_functions=10 | try=9 | raise=16 | broad_except=9 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=9/11 (81%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (11): funnel.services.data_processor; funnel.services.structure_analysis; funnel.services.structure_patch; funnel.services.structure_patch_store; shared.errors.error_types; shared.models.sheet_grid; shared.models.structure_analysis; shared.models.structure_patch (+3 more)
- External imports (2): fastapi; typing
- Public API names: analyze_dataset; analyze_excel_structure; analyze_google_sheets_structure; analyze_sheet_structure; delete_structure_patch; get_data_processor; get_structure_patch; health_check; preview_google_sheets_with_inference; suggest_schema; upsert_structure_patch

### `backend/funnel/services/__init__.py`
- Module summary: Funnel Services
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=3 | code_lines=3 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/funnel/services/data_processor.py`
- Module summary: 🔥 THINK ULTRA! Funnel Data Processor Service
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=288 | code_lines=233 | risk_score=1
- API surface: public=1 | top-level functions=2 | classes=1 | methods=6
- Runtime signals: async_functions=2 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=1/1 (100%) | methods=5/6 (83%)
- Internal imports (6): funnel.services.risk_assessor; funnel.services.schema_utils; funnel.services.type_inference; shared.config.settings; shared.models.google_sheets; shared.models.type_inference
- External imports (3): datetime; httpx; typing
- Public API names: FunnelDataProcessor

### `backend/funnel/services/risk_assessor.py`
- Module summary: Funnel risk assessor (suggestion-only, sample-based).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=347 | code_lines=303 | risk_score=1
- API surface: public=1 | top-level functions=9 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/9 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): funnel.services.schema_utils; shared.models.common; shared.models.type_inference
- External imports (2): __future__; typing
- Public API names: assess_dataset_risks

### `backend/funnel/services/schema_utils.py`
- Module summary: Helpers for Funnel schema-related normalization.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=17 | code_lines=14 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (1): __future__
- Public API names: normalize_property_name

### `backend/funnel/services/structure_analysis.py`
- Module summary: 🔥 THINK ULTRA! Structure Analysis Engine (Funnel)
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=3060 | code_lines=2644 | risk_score=144
- API surface: public=1 | top-level functions=0 | classes=2 | methods=66
- Runtime signals: async_functions=0 | try=24 | raise=0 | broad_except=24 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/2 (50%) | methods=13/66 (19%)
- Internal imports (4): funnel.services.type_inference; shared.models.common; shared.models.structure_analysis; shared.utils.blank_utils
- External imports (11): __future__; dataclasses; functools; hashlib; json; logging; math; re (+3 more)
- Public API names: FunnelStructureAnalyzer

### `backend/funnel/services/structure_patch.py`
- Module summary: Apply human-in-the-loop patches to structure analysis outputs.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=137 | code_lines=113 | risk_score=6
- API surface: public=1 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/2 (50%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): funnel.services.structure_analysis; shared.models.structure_analysis; shared.models.structure_patch
- External imports (3): __future__; logging; typing
- Public API names: apply_structure_patch

### `backend/funnel/services/structure_patch_store.py`
- Module summary: In-memory store for structure-analysis patches.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=33 | code_lines=19 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.models.structure_patch
- External imports (3): __future__; time; typing
- Public API names: delete_patch; get_patch; upsert_patch

### `backend/funnel/services/type_inference.py`
- Module summary: 🔥 THINK ULTRA! Funnel Type Inference Service
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=1819 | code_lines=1484 | risk_score=12
- API surface: public=1 | top-level functions=0 | classes=1 | methods=29
- Runtime signals: async_functions=0 | try=12 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=27/29 (93%)
- Internal imports (4): shared.models.common; shared.models.type_inference; shared.validators.complex_type_validator; shared.validators.money_validator
- External imports (7): collections; datetime; decimal; logging; re; statistics; typing
- Public API names: PatternBasedTypeDetector

### `backend/funnel/services/type_inference_adapter.py`
- Module summary: 🔥 THINK ULTRA! Type Inference Service Adapter
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=66 | code_lines=57 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=3
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=2/3 (66%)
- Internal imports (2): shared.interfaces.type_inference; shared.models.type_inference
- External imports (1): typing
- Public API names: FunnelTypeInferenceAdapter

### `backend/funnel/tests/__init__.py`
- Module summary: Funnel Service Tests
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=3 | code_lines=3 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/funnel/tests/test_data_processor.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=134 | code_lines=103 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=2 | methods=7
- Runtime signals: async_functions=6 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/2 (0%) | methods=0/7 (0%)
- Internal imports (2): funnel.services.data_processor; shared.models.type_inference
- External imports (3): __future__; httpx; pytest
- Public API names: test_data_processor_analyze_dataset_metadata; test_generate_schema_suggestion_handles_confidence; test_process_google_sheets_preview_failure; test_process_google_sheets_preview_success

### `backend/funnel/tests/test_funnel_main.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=40 | code_lines=27 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=1 | methods=3
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/1 (0%) | methods=0/3 (0%)
- Internal imports (1): funnel
- External imports (2): __future__; pytest
- Public API names: test_funnel_lifespan_initializes_rate_limiter; test_funnel_root_and_health

### `backend/funnel/tests/test_risk_assessor.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=65 | code_lines=56 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): funnel.services.risk_assessor; shared.models.type_inference
- External imports (1): __future__
- Public API names: test_assess_dataset_risks_low_confidence_and_nulls; test_assess_dataset_risks_name_collision

### `backend/funnel/tests/test_sheet_grid_parser.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=99 | code_lines=78 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=5
- Runtime signals: async_functions=0 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/1 (0%) | methods=0/5 (0%)
- Internal imports (2): shared.models.structure_analysis; shared.services.core.sheet_grid_parser
- External imports (3): importlib; io; pytest
- Public API names: TestSheetGridParser

### `backend/funnel/tests/test_structure_analysis.py`
- Module summary: 🔥 THINK ULTRA! Structure Analysis tests
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=212 | code_lines=184 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=10
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/1 (0%) | methods=5/10 (50%)
- Internal imports (2): funnel.services.structure_analysis; shared.models.structure_analysis
- External imports (0): not documented
- Public API names: TestStructureAnalysis

### `backend/funnel/tests/test_type_inference.py`
- Module summary: 🔥 THINK ULTRA! Type Inference 테스트
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=399 | code_lines=292 | risk_score=0
- API surface: public=2 | top-level functions=1 | classes=1 | methods=27
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=1/1 (100%) | methods=27/27 (100%)
- Internal imports (2): funnel.services.type_inference; shared.models.common
- External imports (2): pytest; typing
- Public API names: TestTypeInference; test_parametrized_type_detection

### `backend/funnel/tests/test_type_inference_adapter.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=53 | code_lines=39 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): funnel.services.type_inference_adapter; shared.models.common
- External imports (2): asyncio; pytest
- Public API names: test_analyze_dataset_uses_metadata_sample_size; test_infer_column_type_respects_metadata_override; test_infer_single_value_type_returns_type

### `backend/funnel/tests/test_type_inference_router.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=244 | code_lines=191 | risk_score=0
- API surface: public=8 | top-level functions=8 | classes=2 | methods=6
- Runtime signals: async_functions=15 | try=0 | raise=3 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/8 (0%) | classes=0/2 (0%) | methods=0/6 (0%)
- Internal imports (5): funnel.routers; shared.models.sheet_grid; shared.models.structure_analysis; shared.models.structure_patch; shared.models.type_inference
- External imports (6): __future__; fastapi; io; pytest; starlette; types
- Public API names: test_analyze_dataset_success_and_error; test_analyze_excel_structure_errors; test_analyze_excel_structure_happy_path; test_analyze_google_sheets_structure; test_analyze_sheet_structure_applies_patch; test_preview_and_suggest_schema; test_router_health_check; test_structure_patch_endpoints

## ingest_reconciler_worker

### `backend/ingest_reconciler_worker/__init__.py`
- Module summary: Dataset ingest reconciler worker package.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=1 | code_lines=1 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/ingest_reconciler_worker/main.py`
- Module summary: Dataset ingest reconciler worker.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service entrypoint and lifecycle wiring
- Source footprint: total_lines=249 | code_lines=215 | risk_score=19
- API surface: public=3 | top-level functions=2 | classes=1 | methods=11
- Runtime signals: async_functions=5 | try=5 | raise=1 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/1 (0%) | methods=0/11 (0%)
- Internal imports (10): shared.config.settings; shared.errors.error_envelope; shared.errors.error_types; shared.observability.metrics; shared.observability.request_context; shared.observability.tracing; shared.services.core.service_factory; shared.services.registries.dataset_registry (+2 more)
- External imports (8): __future__; asyncio; contextlib; fastapi; httpx; logging; time; typing
- Public API names: IngestReconcilerWorker; lifespan; main

## instance_worker

### `backend/instance_worker/__init__.py`
- Module summary: Instance Worker Service
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=4 | code_lines=4 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/instance_worker/main.py`
- Module summary: Instance Worker — Direct ES Write (Foundry-aligned runtime)
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service entrypoint and lifecycle wiring
- Source footprint: total_lines=2577 | code_lines=2283 | risk_score=175
- API surface: public=2 | top-level functions=1 | classes=3 | methods=43
- Runtime signals: async_functions=27 | try=42 | raise=69 | broad_except=35 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=1/3 (33%) | methods=18/43 (41%)
- Internal imports (32): objectify_worker.write_paths; oms.services.event_store; shared.config.app_config; shared.config.search_config; shared.config.settings; shared.errors.runtime_exception_policy; shared.models.event_envelope; shared.models.lineage_edge_types (+24 more)
- External imports (11): asyncio; boto3; botocore; dataclasses; datetime; httpx; json; logging (+3 more)
- Public API names: StrictInstanceWorker; main

## mcp_servers

### `backend/mcp_servers/__init__.py`
- Module summary: MCP (Model Context Protocol) package.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=7 | code_lines=6 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/mcp_servers/bff_auth.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=15 | code_lines=8 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.config.settings
- External imports (2): __future__; typing
- Public API names: bff_admin_token; bff_api_base_url

### `backend/mcp_servers/context7_development.py`
- Module summary: Context7 Development Helper
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=397 | code_lines=302 | risk_score=24
- API surface: public=5 | top-level functions=4 | classes=1 | methods=11
- Runtime signals: async_functions=9 | try=4 | raise=0 | broad_except=4 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=4/4 (100%) | classes=1/1 (100%) | methods=10/11 (90%)
- Internal imports (1): backend.mcp_servers.mcp_client
- External imports (3): datetime; logging; typing
- Public API names: Context7Developer; analyze_feature; document_feature; get_context7_developer; validate_code

### `backend/mcp_servers/mcp_client.py`
- Module summary: MCP Client for integrating with various MCP servers
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=407 | code_lines=334 | risk_score=30
- API surface: public=5 | top-level functions=2 | classes=3 | methods=14
- Runtime signals: async_functions=10 | try=6 | raise=8 | broad_except=6 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/2 (100%) | classes=3/3 (100%) | methods=11/14 (78%)
- Internal imports (1): shared.config.settings
- External imports (9): anyio; contextlib; dataclasses; json; logging; mcp; os; pathlib (+1 more)
- Public API names: Context7Client; MCPClientManager; MCPServerConfig; get_context7_client; get_mcp_manager

### `backend/mcp_servers/ontology_mcp_server.py`
- Module summary: Ontology MCP Server
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=1423 | code_lines=1222 | risk_score=72
- API surface: public=2 | top-level functions=5 | classes=1 | methods=27
- Runtime signals: async_functions=27 | try=12 | raise=0 | broad_except=12 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=4/5 (80%) | classes=1/1 (100%) | methods=25/27 (92%)
- Internal imports (3): mcp_servers.pipeline_mcp_errors; shared.errors.error_types; shared.utils.llm_safety
- External imports (7): __future__; json; logging; mcp; pathlib; sys; typing
- Public API names: OntologyMCPServer; main

### `backend/mcp_servers/pipeline_mcp_errors.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=108 | code_lines=96 | risk_score=0
- API surface: public=2 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): shared.errors.error_envelope; shared.errors.error_types; shared.observability.request_context
- External imports (4): __future__; hashlib; json; typing
- Public API names: missing_required_params; tool_error

### `backend/mcp_servers/pipeline_mcp_helpers.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=128 | code_lines=102 | risk_score=0
- API surface: public=5 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=2/5 (40%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: extract_spark_error_details; normalize_aggregates; normalize_string_list; trim_build_output; trim_preview_payload

### `backend/mcp_servers/pipeline_mcp_http.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=142 | code_lines=124 | risk_score=5
- API surface: public=5 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=1 | raise=1 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=3/6 (50%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): mcp_servers.bff_auth
- External imports (6): __future__; httpx; logging; os; typing; uuid
- Public API names: bff_headers; bff_json; http_json; oms_api_base_url; oms_json

### `backend/mcp_servers/pipeline_mcp_server.py`
- Module summary: Pipeline MCP Server
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=1542 | code_lines=1469 | risk_score=12
- API surface: public=2 | top-level functions=2 | classes=2 | methods=9
- Runtime signals: async_functions=8 | try=2 | raise=0 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/2 (50%) | classes=1/2 (50%) | methods=2/9 (22%)
- Internal imports (15): mcp_servers.pipeline_tools.dataset_tools; mcp_servers.pipeline_tools.debug_tools; mcp_servers.pipeline_tools.objectify_tools; mcp_servers.pipeline_tools.ontology_tools; mcp_servers.pipeline_tools.pipeline_tools; mcp_servers.pipeline_tools.plan_tools; mcp_servers.pipeline_tools.registry; mcp_servers.pipeline_tools.schema_tools (+7 more)
- External imports (8): __future__; asyncio; logging; mcp; os; pathlib; sys; typing
- Public API names: PipelineMCPServer; main

### `backend/mcp_servers/pipeline_tools/__init__.py`
- Module summary: Pipeline MCP tool handlers (Command/registry modules).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=2 | code_lines=1 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/mcp_servers/pipeline_tools/dataset_tools.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=639 | code_lines=531 | risk_score=54
- API surface: public=1 | top-level functions=16 | classes=0 | methods=0
- Runtime signals: async_functions=9 | try=14 | raise=0 | broad_except=8 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=10/16 (62%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (6): mcp_servers.pipeline_mcp_errors; shared.errors.error_types; shared.observability.tracing; shared.services.pipeline.pipeline_profiler; shared.utils.llm_safety; shared.utils.s3_uri
- External imports (8): __future__; csv; io; json; logging; os; re; typing
- Public API names: build_dataset_tool_handlers

### `backend/mcp_servers/pipeline_tools/debug_tools.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=140 | code_lines=110 | risk_score=0
- API surface: public=1 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): mcp_servers.pipeline_mcp_errors; shared.errors.error_types; shared.observability.tracing
- External imports (2): __future__; typing
- Public API names: build_debug_tool_handlers

### `backend/mcp_servers/pipeline_tools/objectify_tools.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=674 | code_lines=578 | risk_score=36
- API surface: public=1 | top-level functions=9 | classes=0 | methods=0
- Runtime signals: async_functions=8 | try=6 | raise=0 | broad_except=6 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/9 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): mcp_servers.pipeline_mcp_errors; mcp_servers.pipeline_mcp_http; shared.errors.error_types; shared.models.objectify_job; shared.observability.tracing
- External imports (4): __future__; asyncio; logging; typing
- Public API names: build_objectify_tool_handlers

### `backend/mcp_servers/pipeline_tools/ontology_tools.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=410 | code_lines=357 | risk_score=30
- API surface: public=1 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=4 | try=5 | raise=0 | broad_except=5 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): mcp_servers.pipeline_mcp_errors; mcp_servers.pipeline_mcp_http; shared.errors.error_types; shared.observability.tracing; shared.utils.llm_safety
- External imports (3): __future__; logging; typing
- Public API names: build_ontology_tool_handlers

### `backend/mcp_servers/pipeline_tools/pipeline_tools.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=653 | code_lines=600 | risk_score=18
- API surface: public=1 | top-level functions=13 | classes=0 | methods=0
- Runtime signals: async_functions=8 | try=3 | raise=0 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/13 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (9): mcp_servers.pipeline_mcp_errors; mcp_servers.pipeline_mcp_helpers; mcp_servers.pipeline_mcp_http; shared.errors.error_types; shared.errors.external_codes; shared.models.pipeline_plan; shared.observability.tracing; shared.services.pipeline.pipeline_preview_inspector (+1 more)
- External imports (4): __future__; asyncio; logging; typing
- Public API names: build_pipeline_tool_handlers

### `backend/mcp_servers/pipeline_tools/plan_tools.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=1230 | code_lines=1090 | risk_score=34
- API surface: public=1 | top-level functions=46 | classes=0 | methods=0
- Runtime signals: async_functions=45 | try=6 | raise=2 | broad_except=6 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/46 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (12): ..pipeline_mcp_errors; ..pipeline_mcp_helpers; bff.services.pipeline_join_evaluator; bff.services.pipeline_plan_validation; shared.errors.error_types; shared.models.pipeline_plan; shared.observability.tracing; shared.services.pipeline.pipeline_claim_refuter (+4 more)
- External imports (4): __future__; dataclasses; logging; typing
- Public API names: build_plan_tool_handlers

### `backend/mcp_servers/pipeline_tools/registry.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=22 | code_lines=14 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: merge_tool_handlers

### `backend/mcp_servers/pipeline_tools/schema_tools.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=204 | code_lines=170 | risk_score=18
- API surface: public=1 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=3 | raise=0 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): mcp_servers.pipeline_mcp_errors; shared.errors.error_types; shared.observability.tracing
- External imports (3): __future__; logging; typing
- Public API names: build_schema_tool_handlers

## message_relay

### `backend/message_relay/__init__.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=0 | code_lines=0 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/message_relay/main.py`
- Module summary: Event Publisher Service
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service entrypoint and lifecycle wiring
- Source footprint: total_lines=804 | code_lines=664 | risk_score=77
- API surface: public=2 | top-level functions=1 | classes=2 | methods=19
- Runtime signals: async_functions=10 | try=22 | raise=10 | broad_except=13 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=2/2 (100%) | methods=6/19 (31%)
- Internal imports (9): shared.config.app_config; shared.config.settings; shared.models.event_envelope; shared.observability.context_propagation; shared.observability.metrics; shared.observability.tracing; shared.services.kafka.producer_factory; shared.services.storage.s3_client_config (+1 more)
- External imports (11): aioboto3; asyncio; botocore; collections; confluent_kafka; datetime; json; logging (+3 more)
- Public API names: EventPublisher; main

## monitoring

### `backend/monitoring/s3_event_store_dashboard.py`
- Module summary: 🔥 THINK ULTRA! S3/MinIO Event Store Monitoring Dashboard
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=399 | code_lines=305 | risk_score=30
- API surface: public=2 | top-level functions=1 | classes=1 | methods=10
- Runtime signals: async_functions=9 | try=5 | raise=0 | broad_except=5 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=1/1 (100%) | methods=9/10 (90%)
- Internal imports (3): oms.services.event_store; shared.config.service_config; shared.config.settings
- External imports (8): aioboto3; asyncio; collections; datetime; json; logging; prometheus_client; typing
- Public API names: S3EventStoreDashboard; main

## objectify_worker

### `backend/objectify_worker/__init__.py`
- Module summary: Objectify worker package.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=1 | code_lines=1 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/objectify_worker/main.py`
- Module summary: Objectify Worker
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service entrypoint and lifecycle wiring
- Source footprint: total_lines=4137 | code_lines=3815 | risk_score=120
- API surface: public=3 | top-level functions=4 | classes=2 | methods=59
- Runtime signals: async_functions=38 | try=32 | raise=36 | broad_except=24 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=3/4 (75%) | classes=1/2 (50%) | methods=3/59 (5%)
- Internal imports (42): objectify_worker.validation_codes; objectify_worker.write_paths; shared.config.app_config; shared.config.search_config; shared.config.settings; shared.errors.error_envelope; shared.errors.error_types; shared.errors.runtime_exception_policy (+34 more)
- External imports (13): __future__; asyncio; confluent_kafka; csv; datetime; hashlib; httpx; io (+5 more)
- Public API names: ObjectifyNonRetryableError; ObjectifyWorker; main

### `backend/objectify_worker/validation_codes.py`
- Module summary: Formal enum for objectify worker validation codes.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=55 | code_lines=50 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (1): enum
- Public API names: ObjectifyValidationCode

### `backend/objectify_worker/write_paths.py`
- Module summary: Objectify write path strategies.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=500 | code_lines=438 | risk_score=10
- API surface: public=3 | top-level functions=0 | classes=3 | methods=9
- Runtime signals: async_functions=7 | try=2 | raise=4 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=3/3 (100%) | methods=1/9 (11%)
- Internal imports (4): shared.config.search_config; shared.models.objectify_job; shared.services.storage.elasticsearch_service; shared.utils.deterministic_ids
- External imports (5): __future__; dataclasses; datetime; logging; typing
- Public API names: DatasetPrimaryIndexWritePath; ObjectifyWriteBatchResult; ObjectifyWritePath

## oms

### `backend/oms/__init__.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=2 | code_lines=0 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/oms/database/__init__.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=3 | code_lines=2 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): .postgres
- External imports (0): not documented
- Public API names: not documented

### `backend/oms/database/decorators.py`
- Module summary: Database operation decorators for MVCC and retry logic.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=275 | code_lines=205 | risk_score=5
- API surface: public=6 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=7 | try=2 | raise=2 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=6/6 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): .mvcc; .retry_handler
- External imports (4): asyncio; functools; logging; typing
- Public API names: monitor_transaction_time; with_deadlock_retry; with_mvcc_retry; with_optimistic_lock; with_serialization_retry; with_transaction

### `backend/oms/database/mvcc.py`
- Module summary: MVCC Transaction Manager for PostgreSQL
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=222 | code_lines=169 | risk_score=5
- API surface: public=6 | top-level functions=0 | classes=6 | methods=5
- Runtime signals: async_functions=3 | try=3 | raise=4 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=6/6 (100%) | methods=5/5 (100%)
- Internal imports (0): not documented
- External imports (6): asyncio; asyncpg; contextlib; enum; logging; typing
- Public API names: IsolationLevel; MVCCDeadlockError; MVCCError; MVCCMaxRetriesError; MVCCSerializationError; MVCCTransactionManager

### `backend/oms/database/postgres.py`
- Module summary: PostgreSQL database connection and session management for OMS
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=170 | code_lines=132 | risk_score=5
- API surface: public=2 | top-level functions=1 | classes=1 | methods=8
- Runtime signals: async_functions=8 | try=2 | raise=7 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=1/1 (100%) | methods=7/8 (87%)
- Internal imports (3): .decorators; .mvcc; shared.config.settings
- External imports (5): asyncio; asyncpg; contextlib; logging; typing
- Public API names: PostgresDatabase; get_db

### `backend/oms/database/retry_handler.py`
- Module summary: Retry Strategy Pattern Implementation for Database Operations
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=295 | code_lines=219 | risk_score=5
- API surface: public=6 | top-level functions=0 | classes=6 | methods=17
- Runtime signals: async_functions=5 | try=1 | raise=3 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=6/6 (100%) | methods=15/17 (88%)
- Internal imports (0): not documented
- External imports (7): abc; asyncio; asyncpg; enum; logging; random; typing
- Public API names: CompositeRetryStrategy; DeadlockRetryStrategy; RetryExecutor; RetryStrategy; RetryableError; SerializationRetryStrategy

### `backend/oms/dependencies.py`
- Module summary: OMS Dependencies - Modernized Version
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=271 | code_lines=208 | risk_score=51
- API surface: public=5 | top-level functions=4 | classes=1 | methods=3
- Runtime signals: async_functions=5 | try=9 | raise=3 | broad_except=9 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=4/4 (100%) | classes=1/1 (100%) | methods=2/3 (66%)
- Internal imports (16): oms.services.event_store; shared.config.settings; shared.dependencies; shared.dependencies.providers; shared.errors.error_envelope; shared.errors.error_types; shared.observability.request_context; shared.security.database_access (+8 more)
- External imports (3): fastapi; logging; typing
- Public API names: OMSDependencyProvider; ValidatedClassId; ValidatedDatabaseName; check_oms_dependencies_health; ensure_database_exists

### `backend/oms/entities/__init__.py`
- Module summary: OMS Entities package.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=5 | code_lines=4 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/oms/entities/ontology.py`
- Module summary: 온톨로지 도메인 엔티티
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=146 | code_lines=122 | risk_score=0
- API surface: public=3 | top-level functions=0 | classes=3 | methods=6
- Runtime signals: async_functions=0 | try=0 | raise=3 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=3/3 (100%) | methods=6/6 (100%)
- Internal imports (1): shared.models.ontology_validation_mixin
- External imports (3): dataclasses; datetime; typing
- Public API names: Ontology; Property; Relationship

### `backend/oms/exceptions.py`
- Module summary: Exceptions for OMS (Ontology Management Service)
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=110 | code_lines=57 | risk_score=0
- API surface: public=17 | top-level functions=0 | classes=17 | methods=1
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=17/17 (100%) | methods=0/1 (0%)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: AtomicUpdateError; BackupCreationError; BackupRestoreError; CircularReferenceError; ConnectionError; CriticalDataLossRisk; DatabaseError; DatabaseNotFoundError; DuplicateOntologyError; InvalidRelationshipError; OmsBaseException; OntologyNotFoundError (+5 more)

### `backend/oms/main.py`
- Module summary: OMS (Ontology Management Service) - Modernized Version
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service entrypoint and lifecycle wiring
- Source footprint: total_lines=619 | code_lines=457 | risk_score=73
- API surface: public=5 | top-level functions=6 | classes=1 | methods=15
- Runtime signals: async_functions=15 | try=13 | raise=5 | broad_except=13 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=5/6 (83%) | classes=1/1 (100%) | methods=14/15 (93%)
- Internal imports (23): oms.database.postgres; oms.exceptions; oms.middleware.auth; oms.routers; oms.services.event_store; oms.services.ontology_deploy_outbox; oms.services.ontology_deployment_registry; oms.services.ontology_deployment_registry_v2 (+15 more)
- External imports (5): asyncio; contextlib; fastapi; logging; typing
- Public API names: OMSServiceContainer; container_health_check; health_check; lifespan; root

### `backend/oms/middleware/__init__.py`
- Module summary: OMS Middleware package.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=5 | code_lines=4 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/oms/middleware/auth.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=68 | code_lines=55 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.config.settings; shared.security.auth_utils
- External imports (3): __future__; fastapi; hmac
- Public API names: ensure_oms_auth_configured; install_oms_auth_middleware

### `backend/oms/routers/__init__.py`
- Module summary: API 라우터 모듈
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=14 | code_lines=12 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): .database; .ontology; .ontology_extensions
- External imports (0): not documented
- Public API names: not documented

### `backend/oms/routers/_event_sourcing.py`
- Module summary: OMS router helpers for Event Sourcing command emission.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=89 | code_lines=74 | risk_score=6
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=2 | raise=1 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (4): shared.errors.error_types; shared.models.commands; shared.models.event_envelope; shared.services.events.aggregate_sequence_allocator
- External imports (4): __future__; fastapi; logging; typing
- Public API names: append_event_sourcing_command; build_command_status_metadata

### `backend/oms/routers/action_async.py`
- Module summary: OMS async Action router - Action-only writeback submission path.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=1589 | code_lines=1441 | risk_score=15
- API surface: public=18 | top-level functions=8 | classes=14 | methods=1
- Runtime signals: async_functions=6 | try=15 | raise=50 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/8 (25%) | classes=2/14 (14%) | methods=0/1 (0%)
- Internal imports (32): oms.dependencies; oms.services.action_simulation_service; oms.services.ontology_deployment_registry_v2; oms.services.ontology_resources; shared.config.app_config; shared.config.settings; shared.errors.error_types; shared.models.commands (+24 more)
- External imports (7): __future__; datetime; fastapi; logging; pydantic; typing; uuid
- Public API names: ActionSimulateAssumptions; ActionSimulateObservedBaseOverrides; ActionSimulateRequest; ActionSimulateScenarioRequest; ActionSimulateStatePatch; ActionSimulateTargetAssumption; ActionSubmitBatchDependencyRequest; ActionSubmitBatchItemRequest; ActionSubmitBatchItemResponse; ActionSubmitBatchRequest; ActionSubmitBatchResponse; ActionSubmitRequest (+6 more)

### `backend/oms/routers/command_status.py`
- Module summary: Command status router (read-only).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=189 | code_lines=163 | risk_score=37
- API surface: public=1 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=7 | raise=5 | broad_except=7 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/2 (50%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (8): oms.dependencies; oms.services.event_store; oms.utils.command_status_utils; shared.errors.error_types; shared.models.commands; shared.observability.tracing; shared.services.core.command_status_service; shared.services.registries.processed_event_registry
- External imports (5): __future__; fastapi; logging; typing; uuid
- Public API names: get_command_status

### `backend/oms/routers/database.py`
- Module summary: 데이터베이스 관리 라우터
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=285 | code_lines=239 | risk_score=25
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=4 | try=5 | raise=15 | broad_except=5 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=4/4 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (9): oms.dependencies; oms.routers._event_sourcing; shared.config.app_config; shared.errors.error_types; shared.models.commands; shared.models.requests; shared.observability.tracing; shared.security.database_access (+1 more)
- External imports (2): fastapi; logging
- Public API names: create_database; database_exists; delete_database; list_databases

### `backend/oms/routers/instance.py`
- Module summary: Instance Management Router
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=227 | code_lines=199 | risk_score=20
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=4 | raise=12 | broad_except=4 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=3/3 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (6): oms.dependencies; shared.config.search_config; shared.dependencies.providers; shared.errors.error_types; shared.observability.tracing; shared.security.input_sanitizer
- External imports (3): fastapi; logging; typing
- Public API names: get_class_instance_count; get_class_instances; get_instance

### `backend/oms/routers/instance_async.py`
- Module summary: OMS 비동기 인스턴스 라우터 - Command Pattern 기반
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=947 | code_lines=816 | risk_score=75
- API surface: public=12 | top-level functions=14 | classes=5 | methods=0
- Runtime signals: async_functions=11 | try=15 | raise=24 | broad_except=15 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=11/14 (78%) | classes=5/5 (100%) | methods=0/0 (n/a)
- Internal imports (12): oms.dependencies; oms.routers._event_sourcing; oms.utils.command_status_utils; oms.utils.ontology_stamp; shared.config.app_config; shared.errors.error_types; shared.models.commands; shared.observability.tracing (+4 more)
- External imports (6): datetime; fastapi; logging; pydantic; typing; uuid
- Public API names: BulkInstanceCreateRequest; BulkInstanceUpdateRequest; InstanceCreateRequest; InstanceDeleteRequest; InstanceUpdateRequest; bulk_create_instances_async; bulk_create_instances_with_tracking; bulk_update_instances_async; create_instance_async; delete_instance_async; get_instance_command_status; update_instance_async

### `backend/oms/routers/ontology.py`
- Module summary: OMS 온톨로지 라우터 - 내부 ID 기반 온톨로지 관리
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=1551 | code_lines=1385 | risk_score=60
- API surface: public=6 | top-level functions=25 | classes=0 | methods=0
- Runtime signals: async_functions=11 | try=12 | raise=44 | broad_except=12 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=6/25 (24%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (24): oms.dependencies; oms.routers._event_sourcing; oms.services.ontology_interface_contract; oms.services.ontology_resources; oms.services.property_to_relationship_converter; oms.validation_codes; shared.config.app_config; shared.config.settings (+16 more)
- External imports (4): fastapi; hmac; logging; typing
- Public API names: create_ontology; delete_ontology; get_ontology; list_ontologies; update_ontology; validate_ontology_create

### `backend/oms/routers/ontology_extensions.py`
- Module summary: Ontology extensions router (resources, governance, health).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=1198 | code_lines=1077 | risk_score=60
- API surface: public=15 | top-level functions=29 | classes=4 | methods=0
- Runtime signals: async_functions=14 | try=12 | raise=49 | broad_except=12 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/29 (0%) | classes=0/4 (0%) | methods=0/0 (n/a)
- Internal imports (19): oms.database.postgres; oms.exceptions; oms.services.ontology_deployment_registry_v2; oms.services.ontology_health_issue_registry; oms.services.ontology_interface_contract; oms.services.ontology_resource_validator; oms.services.ontology_resources; oms.services.pull_request_service (+11 more)
- External imports (5): fastapi; json; logging; pydantic; typing
- Public API names: OntologyApproveRequest; OntologyDeployRequest; OntologyProposalRequest; OntologyResourceRequest; approve_ontology_proposal; create_ontology_proposal; create_resource; delete_resource; deploy_ontology; get_resource; list_ontology_proposals; list_resources (+3 more)

### `backend/oms/routers/pull_request.py`
- Module summary: Pull Request Router for OMS
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=409 | code_lines=333 | risk_score=30
- API surface: public=10 | top-level functions=7 | classes=3 | methods=0
- Runtime signals: async_functions=7 | try=6 | raise=18 | broad_except=6 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=7/7 (100%) | classes=3/3 (100%) | methods=0/0 (n/a)
- Internal imports (9): oms.database.postgres; oms.exceptions; oms.services.pull_request_service; shared.errors.error_types; shared.models.base; shared.models.requests; shared.observability.tracing; shared.security.input_sanitizer (+1 more)
- External imports (4): fastapi; logging; pydantic; typing
- Public API names: PRCloseRequest; PRCreateRequest; PRMergeRequest; close_pull_request; create_pull_request; get_pr_service; get_pull_request; get_pull_request_diff; list_pull_requests; merge_pull_request

### `backend/oms/routers/query.py`
- Module summary: Foundry-style Object Search Router (OMS).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=511 | code_lines=423 | risk_score=5
- API surface: public=4 | top-level functions=14 | classes=3 | methods=3
- Runtime signals: async_functions=1 | try=1 | raise=18 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/14 (7%) | classes=1/3 (33%) | methods=0/3 (0%)
- Internal imports (6): oms.dependencies; shared.config.search_config; shared.dependencies.providers; shared.observability.tracing; shared.security.input_sanitizer; shared.utils.foundry_page_token
- External imports (8): fastapi; hashlib; json; logging; pydantic; re; typing; uuid
- Public API names: SearchJsonQueryV2; SearchObjectsRequestV2; SearchObjectsResponseV2; search_objects_v2

### `backend/oms/routers/tasks.py`
- Module summary: OMS Background Task Management Router
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=171 | code_lines=134 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=4 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=4/4 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (4): shared.dependencies.providers; shared.errors.error_types; shared.models.background_task; shared.observability.tracing
- External imports (2): fastapi; typing
- Public API names: cleanup_old_tasks; get_active_tasks; get_internal_task_status; task_service_health

### `backend/oms/services/__init__.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=2 | code_lines=0 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/oms/services/action_simulation_service.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=1495 | code_lines=1385 | risk_score=35
- API surface: public=8 | top-level functions=13 | classes=4 | methods=1
- Runtime signals: async_functions=5 | try=15 | raise=66 | broad_except=7 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/13 (0%) | classes=0/4 (0%) | methods=0/1 (0%)
- Internal imports (27): oms.services.ontology_resources; shared.config.app_config; shared.errors.enterprise_catalog; shared.errors.error_types; shared.observability.tracing; shared.security.database_access; shared.services.core.object_type_meta_resolver; shared.services.core.writeback_merge_service (+19 more)
- External imports (4): __future__; dataclasses; logging; typing
- Public API names: ActionPreflight; ActionSimulationRejected; ActionSimulationScenario; TargetPreflight; build_patchset_for_scenario; enforce_action_permission; preflight_action_writeback; simulate_effects_for_patchset

### `backend/oms/services/event_store.py`
- Module summary: Backward-compatibility shim for Event Store.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=12 | code_lines=8 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.storage.event_store
- External imports (0): not documented
- Public API names: not documented

### `backend/oms/services/ontology_deploy_outbox.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=118 | code_lines=107 | risk_score=6
- API surface: public=2 | top-level functions=1 | classes=1 | methods=3
- Runtime signals: async_functions=3 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/1 (0%) | methods=0/3 (0%)
- Internal imports (7): oms.services.ontology_deployment_registry; shared.config.settings; shared.models.event_envelope; shared.observability.tracing; shared.services.events.outbox_runtime; shared.services.storage.event_store; shared.utils.backoff_utils
- External imports (5): __future__; asyncio; datetime; logging; typing
- Public API names: OntologyDeployOutboxPublisher; run_ontology_deploy_outbox_worker

### `backend/oms/services/ontology_deploy_outbox_store.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=163 | code_lines=147 | risk_score=0
- API surface: public=3 | top-level functions=0 | classes=3 | methods=4
- Runtime signals: async_functions=3 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=1/3 (33%) | methods=0/4 (0%)
- Internal imports (2): oms.database.postgres; shared.observability.tracing
- External imports (4): __future__; dataclasses; datetime; typing
- Public API names: OntologyDeployOutboxItem; OntologyDeployOutboxStore; OntologyDeployOutboxTableSpec

### `backend/oms/services/ontology_deployment_registry.py`
- Module summary: Ontology deployment registry (Postgres SSoT).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=175 | code_lines=164 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=2
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=0/2 (0%)
- Internal imports (4): oms.database.postgres; oms.services.ontology_deploy_outbox_store; oms.services.ontology_deployment_registry_base; shared.observability.tracing
- External imports (5): __future__; datetime; logging; typing; uuid
- Public API names: OntologyDeploymentRegistry

### `backend/oms/services/ontology_deployment_registry_base.py`
- Module summary: Shared Template Method for ontology deployment registries.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=153 | code_lines=137 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=10
- Runtime signals: async_functions=6 | try=0 | raise=3 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/1 (0%) | methods=0/10 (0%)
- Internal imports (4): oms.database.postgres; oms.services.ontology_deploy_outbox_store; shared.config.app_config; shared.utils.deterministic_ids
- External imports (4): __future__; abc; datetime; typing
- Public API names: BaseOntologyDeploymentRegistry

### `backend/oms/services/ontology_deployment_registry_v2.py`
- Module summary: Ontology deployment registry v2 (Postgres SSoT).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=240 | code_lines=226 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=4
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=1/4 (25%)
- Internal imports (5): oms.database.postgres; oms.services.ontology_deploy_outbox_store; oms.services.ontology_deployment_registry_base; shared.observability.tracing; shared.utils.json_utils
- External imports (6): __future__; datetime; json; logging; typing; uuid
- Public API names: OntologyDeploymentRegistryV2

### `backend/oms/services/ontology_health_issue_registry.py`
- Module summary: Ontology health issue catalog and normalization utilities.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=185 | code_lines=160 | risk_score=0
- API surface: public=6 | top-level functions=8 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/8 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: build_link_type_ref; build_object_type_ref; build_ontology_resource_ref; normalize_issue; normalize_issue_code; normalize_severity

### `backend/oms/services/ontology_interface_contract.py`
- Module summary: Interface contract validation for ontology classes.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=275 | code_lines=246 | risk_score=0
- API surface: public=10 | top-level functions=10 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/10 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): oms.services.ontology_health_issue_registry; oms.validation_codes
- External imports (2): __future__; typing
- Public API names: build_property_map; build_relationship_map; collect_interface_contract_issues; extract_entry_value; extract_interface_refs; extract_property_type; extract_relationship_target; extract_required_entries; normalize_reference_value; strip_interface_prefix

### `backend/oms/services/ontology_resource_validator.py`
- Module summary: Ontology resource validation (required spec + reference checks).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=1053 | code_lines=962 | risk_score=1
- API surface: public=6 | top-level functions=20 | classes=2 | methods=0
- Runtime signals: async_functions=4 | try=5 | raise=4 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/20 (0%) | classes=2/2 (100%) | methods=0/0 (n/a)
- Internal imports (8): oms.services.ontology_resources; oms.validation_codes; shared.observability.tracing; shared.utils.action_input_schema; shared.utils.action_permission_profile; shared.utils.action_template_engine; shared.utils.key_spec; shared.utils.safe_bool_expression
- External imports (4): __future__; logging; re; typing
- Public API names: ResourceReferenceError; ResourceSpecError; check_required_fields; collect_reference_values; find_missing_references; validate_resource

### `backend/oms/services/ontology_resources.py`
- Module summary: Ontology resource storage service.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=993 | code_lines=921 | risk_score=0
- API surface: public=2 | top-level functions=7 | classes=1 | methods=24
- Runtime signals: async_functions=19 | try=7 | raise=8 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/7 (0%) | classes=1/1 (100%) | methods=0/24 (0%)
- Internal imports (3): oms.exceptions; shared.config.settings; shared.observability.tracing
- External imports (7): __future__; asyncio; asyncpg; datetime; json; logging; typing
- Public API names: OntologyResourceService; normalize_resource_type

### `backend/oms/services/property_to_relationship_converter.py`
- Module summary: Property to Relationship 자동 변환기
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=189 | code_lines=125 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=6
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=5/6 (83%)
- Internal imports (1): shared.models.ontology
- External imports (2): logging; typing
- Public API names: PropertyToRelationshipConverter

### `backend/oms/services/pull_request_service.py`
- Module summary: Pull Request Service for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=413 | code_lines=344 | risk_score=25
- API surface: public=2 | top-level functions=0 | classes=2 | methods=8
- Runtime signals: async_functions=7 | try=5 | raise=8 | broad_except=5 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=2/2 (100%) | methods=8/8 (100%)
- Internal imports (7): oms.database.decorators; oms.database.mvcc; oms.exceptions; shared.models.base; shared.observability.tracing; shared.utils.diff_utils; shared.utils.json_utils
- External imports (5): datetime; json; logging; typing; uuid
- Public API names: PullRequestService; PullRequestStatus

### `backend/oms/services/relationship_manager.py`
- Module summary: 🔥 THINK ULTRA! RelationshipManager Service
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=354 | code_lines=254 | risk_score=0
- API surface: public=2 | top-level functions=0 | classes=2 | methods=13
- Runtime signals: async_functions=0 | try=1 | raise=4 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=2/2 (100%) | methods=12/13 (92%)
- Internal imports (1): shared.models.ontology
- External imports (3): dataclasses; logging; typing
- Public API names: RelationshipManager; RelationshipPair

### `backend/oms/utils/__init__.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=1 | code_lines=0 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/oms/utils/cardinality_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=15 | code_lines=11 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (1): __future__
- Public API names: inverse_cardinality

### `backend/oms/utils/circular_reference_detector.py`
- Module summary: 🔥 THINK ULTRA! CircularReferenceDetector
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=633 | code_lines=471 | risk_score=1
- API surface: public=4 | top-level functions=0 | classes=4 | methods=22
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=4/4 (100%) | methods=22/22 (100%)
- Internal imports (2): oms.utils.cardinality_utils; shared.models.ontology
- External imports (5): collections; dataclasses; enum; logging; typing
- Public API names: CircularReferenceDetector; CycleInfo; CycleType; RelationshipEdge

### `backend/oms/utils/command_status_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=18 | code_lines=15 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.models.commands
- External imports (1): __future__
- Public API names: map_registry_status

### `backend/oms/utils/ontology_stamp.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=18 | code_lines=13 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.ontology_version
- External imports (2): __future__; typing
- Public API names: merge_ontology_stamp

### `backend/oms/utils/relationship_path_tracker.py`
- Module summary: 🔥 THINK ULTRA! RelationshipPathTracker
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=597 | code_lines=439 | risk_score=1
- API surface: public=6 | top-level functions=0 | classes=6 | methods=21
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=6/6 (100%) | methods=20/21 (95%)
- Internal imports (2): oms.utils.cardinality_utils; shared.models.ontology
- External imports (6): collections; dataclasses; enum; heapq; logging; typing
- Public API names: PathQuery; PathType; RelationshipHop; RelationshipPath; RelationshipPathTracker; TraversalDirection

### `backend/oms/validation_codes.py`
- Module summary: Formal enum for ontology validation and health issue codes.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=18 | code_lines=14 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (1): enum
- Public API names: OntologyValidationCode

### `backend/oms/validators/__init__.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=1 | code_lines=0 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/oms/validators/relationship_validator.py`
- Module summary: 🔥 THINK ULTRA! RelationshipValidator
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=630 | code_lines=464 | risk_score=0
- API surface: public=3 | top-level functions=0 | classes=3 | methods=20
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=3/3 (100%) | methods=19/20 (95%)
- Internal imports (1): shared.models.ontology
- External imports (4): dataclasses; enum; logging; typing
- Public API names: RelationshipValidator; ValidationResult; ValidationSeverity

## ontology_worker

### `backend/ontology_worker/__init__.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=0 | code_lines=0 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/ontology_worker/main.py`
- Module summary: Ontology Worker Service
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service entrypoint and lifecycle wiring
- Source footprint: total_lines=1365 | code_lines=1190 | risk_score=142
- API surface: public=1 | top-level functions=0 | classes=3 | methods=28
- Runtime signals: async_functions=15 | try=30 | raise=28 | broad_except=28 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/3 (33%) | methods=12/28 (42%)
- Internal imports (27): oms.services.event_store; oms.services.ontology_resources; shared.config.app_config; shared.config.settings; shared.models.commands; shared.models.event_envelope; shared.models.events; shared.models.lineage_edge_types (+19 more)
- External imports (8): asyncio; confluent_kafka; dataclasses; datetime; json; logging; os; typing
- Public API names: OntologyWorker

## perf

### `backend/perf/cleanup_perf_databases.py`
- Module summary: Cleanup helper for perf-created databases.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=139 | code_lines=106 | risk_score=5
- API surface: public=1 | top-level functions=7 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=2 | raise=3 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/7 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.config.settings; shared.tools.bff_admin_api
- External imports (8): __future__; argparse; asyncio; asyncpg; httpx; logging; re; typing
- Public API names: main

## pipeline_scheduler

### `backend/pipeline_scheduler/main.py`
- Module summary: Pipeline Scheduler Service.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service entrypoint and lifecycle wiring
- Source footprint: total_lines=34 | code_lines=26 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (7): shared.config.settings; shared.observability.metrics; shared.observability.tracing; shared.services.pipeline.pipeline_job_queue; shared.services.pipeline.pipeline_scheduler; shared.services.registries.pipeline_registry; shared.utils.app_logger
- External imports (2): __future__; asyncio
- Public API names: main

## pipeline_worker

### `backend/pipeline_worker/__init__.py`
- Module summary: Pipeline worker package.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=1 | code_lines=1 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/pipeline_worker/main.py`
- Module summary: Pipeline Worker (Spark/Flink-ready execution runtime).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service entrypoint and lifecycle wiring
- Source footprint: total_lines=5617 | code_lines=5250 | risk_score=235
- API surface: public=2 | top-level functions=4 | classes=2 | methods=100
- Runtime signals: async_functions=44 | try=62 | raise=81 | broad_except=47 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/4 (0%) | classes=0/2 (0%) | methods=5/100 (5%)
- Internal imports (52): data_connector.google_sheets.service; pipeline_worker.spark_schema_helpers; pipeline_worker.spark_transform_engine; pipeline_worker.worker_helpers; shared.config.app_config; shared.config.settings; shared.errors.error_envelope; shared.errors.error_types (+44 more)
- External imports (17): __future__; asyncio; base64; concurrent; confluent_kafka; datetime; functools; httpx (+9 more)
- Public API names: PipelineWorker; main

### `backend/pipeline_worker/spark_schema_helpers.py`
- Module summary: Spark-specific schema and file helpers for the pipeline worker.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=94 | code_lines=76 | risk_score=1
- API surface: public=0 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.services.pipeline.pipeline_type_utils; shared.utils.schema_hash
- External imports (3): __future__; os; typing
- Public API names: not documented

### `backend/pipeline_worker/spark_transform_engine.py`
- Module summary: Spark transform engine for the pipeline worker.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=1037 | code_lines=892 | risk_score=5
- API surface: public=1 | top-level functions=30 | classes=1 | methods=0
- Runtime signals: async_functions=0 | try=4 | raise=28 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/30 (3%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (3): shared.services.pipeline.pipeline_parameter_utils; shared.services.pipeline.pipeline_transform_spec; shared.services.pipeline.pipeline_udf_runtime
- External imports (5): __future__; dataclasses; logging; math; typing
- Public API names: apply_spark_transform

### `backend/pipeline_worker/worker_helpers.py`
- Module summary: Pipeline worker helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=272 | code_lines=234 | risk_score=11
- API surface: public=0 | top-level functions=15 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=4 | raise=3 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/15 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): shared.config.settings; shared.models.pipeline_job; shared.services.pipeline.pipeline_definition_utils
- External imports (3): __future__; logging; typing
- Public API names: not documented

## projection_worker

### `backend/projection_worker/__init__.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=0 | code_lines=0 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/projection_worker/main.py`
- Module summary: Projection Worker Service
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service entrypoint and lifecycle wiring
- Source footprint: total_lines=2075 | code_lines=1793 | risk_score=131
- API surface: public=1 | top-level functions=0 | classes=1 | methods=43
- Runtime signals: async_functions=26 | try=29 | raise=28 | broad_except=26 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=21/43 (48%)
- Internal imports (30): shared.config.app_config; shared.config.search_config; shared.config.settings; shared.models.event_envelope; shared.models.events; shared.models.lineage_edge_types; shared.observability.metrics; shared.observability.tracing (+22 more)
- External imports (7): asyncio; confluent_kafka; datetime; json; logging; os; typing
- Public API names: ProjectionWorker

## scripts

### `backend/scripts/backfill_lineage.py`
- Module summary: Lineage backfill utilities.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: operational or migration automation
- Source footprint: total_lines=130 | code_lines=101 | risk_score=16
- API surface: public=1 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=3 | raise=2 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): oms.services.event_store; shared.services.registries.lineage_store
- External imports (5): __future__; argparse; asyncio; datetime; typing
- Public API names: main

### `backend/scripts/dependency_parsing.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: operational or migration automation
- Source footprint: total_lines=70 | code_lines=59 | risk_score=8
- API surface: public=2 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=3 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; pathlib; typing
- Public API names: parse_pyproject_toml; parse_requirements_txt

### `backend/scripts/ghost_dependency_audit.py`
- Module summary: 🔥 THINK ULTRA! Ghost Dependency Audit Script
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: operational or migration automation
- Source footprint: total_lines=187 | code_lines=136 | risk_score=6
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=3/3 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (4): pathlib; scripts; sys; typing
- Public API names: audit_service; check_service_imports; main

### `backend/scripts/import_performance_test.py`
- Module summary: 🔥 THINK ULTRA! Import Performance & Memory Efficiency Test
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: operational or migration automation
- Source footprint: total_lines=178 | code_lines=132 | risk_score=6
- API surface: public=5 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=5/5 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (5): importlib; pathlib; sys; time; tracemalloc
- Public API names: main; measure_import_performance; test_bulk_import_simulation; test_direct_import; test_single_service_need

### `backend/scripts/migrations/create_minio_bucket.py`
- Module summary: Create MinIO events bucket with correct credentials
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: operational or migration automation
- Source footprint: total_lines=68 | code_lines=51 | risk_score=7
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): boto3; botocore; sys
- Public API names: not documented

### `backend/scripts/migrations/create_test_schema.py`
- Module summary: Create test database and schema with system fields
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: operational or migration automation
- Source footprint: total_lines=135 | code_lines=106 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): aiohttp; asyncio; json
- Public API names: create_test_environment

### `backend/scripts/migrations/fix_all_datetime_utc.py`
- Module summary: Fix all datetime.UTC usage to timezone.utc
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: operational or migration automation
- Source footprint: total_lines=75 | code_lines=54 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/2 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): os; re
- Public API names: fix_file; main

### `backend/scripts/migrations/fix_datetime_deprecation.py`
- Module summary: Fix datetime.utcnow() deprecation warnings
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: operational or migration automation
- Source footprint: total_lines=91 | code_lines=70 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/2 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): os; re
- Public API names: fix_datetime_in_file; main

### `backend/scripts/migrations/migrate_lineage_es_aliases_to_canonical.py`
- Module summary: One-time migration: normalize compatibility ES lineage aliases to canonical names.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: operational or migration automation
- Source footprint: total_lines=115 | code_lines=103 | risk_score=0
- API surface: public=1 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=1 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (5): __future__; argparse; asyncio; asyncpg; os
- Public API names: main

### `backend/scripts/migrations/migrate_lineage_s3_edge_alias_to_canonical.py`
- Module summary: One-time migration: normalize compatibility S3 lineage edge alias to canonical name.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: operational or migration automation
- Source footprint: total_lines=49 | code_lines=37 | risk_score=0
- API surface: public=1 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=1 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (5): __future__; argparse; asyncio; asyncpg; os
- Public API names: main

### `backend/scripts/migrations/update_imports.py`
- Module summary: 🔥 THINK ULTRA!! Import Update Script
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: operational or migration automation
- Source footprint: total_lines=66 | code_lines=48 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/3 (66%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): os; pathlib; re
- Public API names: find_python_files; main; update_imports_in_file

### `backend/scripts/processed_event_registry_smoke.py`
- Module summary: Smoke tests for ProcessedEventRegistry (Postgres).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: operational or migration automation
- Source footprint: total_lines=90 | code_lines=67 | risk_score=5
- API surface: public=0 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=2 | raise=4 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.services.registries.processed_event_registry; shared.services.registries.processed_event_registry_factory
- External imports (5): __future__; asyncio; contextlib; os; sys
- Public API names: not documented

### `backend/scripts/run_coverage_report.py`
- Module summary: 🔥 THINK ULTRA! Comprehensive Test Coverage Reporter
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: operational or migration automation
- Source footprint: total_lines=543 | code_lines=405 | risk_score=14
- API surface: public=2 | top-level functions=1 | classes=1 | methods=10
- Runtime signals: async_functions=0 | try=4 | raise=0 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=1/1 (100%) | methods=9/10 (90%)
- Internal imports (0): not documented
- External imports (10): argparse; datetime; json; os; pathlib; subprocess; sys; time (+2 more)
- Public API names: CoverageReporter; main

### `backend/scripts/run_message_relay_local.py`
- Module summary: EventPublisher를 로컬에서 직접 실행
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: operational or migration automation
- Source footprint: total_lines=62 | code_lines=44 | risk_score=6
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): asyncio; logging; os
- Public API names: main

### `backend/scripts/single_source_of_truth_audit.py`
- Module summary: 🎯 THINK ULTRA! Single Source of Truth Verification
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: operational or migration automation
- Source footprint: total_lines=204 | code_lines=152 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=4/4 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (6): os; pathlib; re; scripts; sys; typing
- Public API names: check_duplicate_dependencies; check_single_source_compliance; check_version_consistency; main

### `backend/scripts/start_services.py`
- Module summary: 🔥 THINK ULTRA! SPICE HARVESTER 서비스 시작 스크립트
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: operational or migration automation
- Source footprint: total_lines=233 | code_lines=176 | risk_score=13
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=3 | raise=0 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/3 (66%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.config.settings
- External imports (9): argparse; os; pathlib; requests; signal; subprocess; sys; time (+1 more)
- Public API names: main; start_service; stop_services

### `backend/scripts/sync_agent_tool_allowlist.py`
- Module summary: Sync the canonical agent tool allowlist bundle into Postgres.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: operational or migration automation
- Source footprint: total_lines=56 | code_lines=43 | risk_score=0
- API surface: public=1 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=1 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.services.agent.agent_tool_allowlist; shared.services.registries.agent_tool_registry
- External imports (4): __future__; argparse; asyncio; json
- Public API names: main

### `backend/scripts/validate_environment.py`
- Module summary: SPICE Harvester Environment Validator (SSoT)
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: operational or migration automation
- Source footprint: total_lines=313 | code_lines=256 | risk_score=41
- API surface: public=2 | top-level functions=2 | classes=1 | methods=12
- Runtime signals: async_functions=7 | try=7 | raise=1 | broad_except=7 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/1 (0%) | methods=0/12 (0%)
- Internal imports (1): shared.config.settings
- External imports (10): __future__; aiohttp; asyncio; asyncpg; confluent_kafka; elasticsearch; os; redis (+2 more)
- Public API names: EnvironmentValidator; main

### `backend/scripts/verify-imports.py`
- Module summary: Import Verification Script
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: operational or migration automation
- Source footprint: total_lines=214 | code_lines=151 | risk_score=13
- API surface: public=6 | top-level functions=5 | classes=1 | methods=3
- Runtime signals: async_functions=0 | try=3 | raise=0 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=5/5 (100%) | classes=1/1 (100%) | methods=2/3 (66%)
- Internal imports (0): not documented
- External imports (6): ast; importlib; os; pathlib; sys; typing
- Public API names: ImportChecker; check_conditional_imports; extract_imports; main; verify_import; verify_service

### `backend/scripts/verify_projection_consistency.py`
- Module summary: Verify projection consistency between Event Store (durable source) and Elasticsearch.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: operational or migration automation
- Source footprint: total_lines=222 | code_lines=191 | risk_score=6
- API surface: public=1 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=2 | raise=1 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): shared.config.search_config; shared.config.settings; shared.services.core.projection_consistency; shared.services.storage.elasticsearch_service; shared.services.storage.event_store
- External imports (9): __future__; argparse; asyncio; collections; datetime; json; pathlib; sys (+1 more)
- Public API names: main

## shared

### `backend/shared/__init__.py`
- Module summary: Shared modules for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=3 | code_lines=3 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/config/__init__.py`
- Module summary: Unified Configuration Access Point
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=186 | code_lines=134 | risk_score=5
- API surface: public=1 | top-level functions=1 | classes=1 | methods=11
- Runtime signals: async_functions=0 | try=1 | raise=1 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=1/1 (100%) | methods=11/11 (100%)
- Internal imports (4): .app_config; .search_config; .service_config; .settings
- External imports (1): logging
- Public API names: Config

### `backend/shared/config/app_config.py`
- Module summary: Application Configuration
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=370 | code_lines=257 | risk_score=1
- API surface: public=1 | top-level functions=0 | classes=2 | methods=23
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=2/2 (100%) | methods=17/23 (73%)
- Internal imports (1): .settings
- External imports (3): operator; re; typing
- Public API names: AppConfig

### `backend/shared/config/kafka_config.py`
- Module summary: Kafka Configuration for EOS v2 (Exactly-Once Semantics)
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=315 | code_lines=239 | risk_score=5
- API surface: public=4 | top-level functions=2 | classes=2 | methods=10
- Runtime signals: async_functions=0 | try=1 | raise=1 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/2 (100%) | classes=2/2 (100%) | methods=10/10 (100%)
- Internal imports (1): shared.config.settings
- External imports (2): typing; uuid
- Public API names: KafkaEOSConfig; TransactionalProducer; create_eos_consumer; create_eos_producer

### `backend/shared/config/model_context_limits.py`
- Module summary: Model-specific context window configurations.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=139 | code_lines=100 | risk_score=0
- API surface: public=3 | top-level functions=1 | classes=2 | methods=5
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=2/2 (100%) | methods=5/5 (100%)
- Internal imports (0): not documented
- External imports (2): dataclasses; typing
- Public API names: ModelContextConfig; PromptBudget; get_model_context_config

### `backend/shared/config/rate_limit_config.py`
- Module summary: Rate Limiting Configuration
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=292 | code_lines=241 | risk_score=0
- API surface: public=4 | top-level functions=0 | classes=4 | methods=4
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=4/4 (100%) | methods=4/4 (100%)
- Internal imports (0): not documented
- External imports (3): enum; pydantic; typing
- Public API names: EndpointCategory; RateLimitConfig; RateLimitRule; RateLimitStrategy

### `backend/shared/config/search_config.py`
- Module summary: Search Configuration
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=199 | code_lines=141 | risk_score=0
- API surface: public=5 | top-level functions=7 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=6/7 (85%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.config.settings
- External imports (3): hashlib; re; typing
- Public API names: get_default_index_settings; get_index_alias_name; get_instances_index_name; get_ontologies_index_name; sanitize_index_name

### `backend/shared/config/service_config.py`
- Module summary: 🔥 THINK ULTRA! Service Configuration
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=523 | code_lines=411 | risk_score=0
- API surface: public=5 | top-level functions=4 | classes=2 | methods=47
- Runtime signals: async_functions=0 | try=1 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=4/4 (100%) | classes=1/2 (50%) | methods=46/47 (97%)
- Internal imports (1): shared.config.settings
- External imports (3): logging; pydantic; typing
- Public API names: ServiceConfig; get_agent_url; get_bff_url; get_funnel_url; get_oms_url

### `backend/shared/config/settings.py`
- Module summary: Centralized Configuration System for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=4416 | code_lines=3848 | risk_score=14
- API surface: public=53 | top-level functions=18 | classes=46 | methods=207
- Runtime signals: async_functions=0 | try=13 | raise=4 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=6/18 (33%) | classes=46/46 (100%) | methods=17/207 (8%)
- Internal imports (0): not documented
- External imports (7): enum; json; logging; os; pydantic; pydantic_settings; typing
- Public API names: ActionOutboxSettings; ActionWorkerSettings; AgentPlanSettings; AgentRetentionWorkerSettings; AgentRuntimeSettings; ApplicationSettings; AuthSettings; BranchVirtualizationSettings; CacheSettings; ChaosSettings; ClientSettings; ConnectorSyncSettings (+41 more)

### `backend/shared/dependencies/__init__.py`
- Module summary: Modern Dependency Injection System for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=73 | code_lines=55 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): .container; .providers; .type_inference
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/dependencies/container.py`
- Module summary: Modern Dependency Injection Container for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=428 | code_lines=320 | risk_score=15
- API surface: public=9 | top-level functions=5 | classes=4 | methods=16
- Runtime signals: async_functions=11 | try=4 | raise=8 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=5/5 (100%) | classes=4/4 (100%) | methods=16/16 (100%)
- Internal imports (1): shared.config.settings
- External imports (5): asyncio; contextlib; dataclasses; logging; typing
- Public API names: ServiceContainer; ServiceFactory; ServiceLifecycle; ServiceRegistration; container_lifespan; get_container; get_settings_from_container; initialize_container; shutdown_container

### `backend/shared/dependencies/providers.py`
- Module summary: Service Providers for Dependency Injection
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=276 | code_lines=210 | risk_score=11
- API surface: public=14 | top-level functions=14 | classes=0 | methods=0
- Runtime signals: async_functions=13 | try=2 | raise=1 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=12/14 (85%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (13): shared.config.settings; shared.dependencies.container; shared.errors.error_types; shared.services.agent.llm_gateway; shared.services.core.audit_log_store; shared.services.core.background_task_manager; shared.services.registries.lineage_store; shared.services.storage.elasticsearch_service (+5 more)
- External imports (3): fastapi; logging; typing
- Public API names: get_audit_log_store; get_background_task_manager; get_elasticsearch_service; get_initialized_background_task_manager; get_jsonld_converter; get_label_mapper; get_lakefs_storage_service; get_lineage_store; get_llm_gateway; get_redis_service; get_settings_dependency; get_storage_service (+2 more)

### `backend/shared/dependencies/type_inference.py`
- Module summary: 🔥 THINK ULTRA! Type Inference Service Dependencies
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=97 | code_lines=66 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=4/4 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): ..interfaces.type_inference
- External imports (1): typing
- Public API names: configure_type_inference_service; get_type_inference_service; reset_type_inference_service; type_inference_dependency

### `backend/shared/errors/__init__.py`
- Module summary: Shared Errors package.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=6 | code_lines=5 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/errors/enterprise_catalog.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=2706 | code_lines=2606 | risk_score=3
- API surface: public=15 | top-level functions=20 | classes=11 | methods=1
- Runtime signals: async_functions=0 | try=3 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/20 (0%) | classes=0/11 (0%) | methods=0/1 (0%)
- Internal imports (3): shared.config.settings; shared.errors.error_types; shared.utils.canonical_json
- External imports (4): __future__; dataclasses; enum; typing
- Public API names: EnterpriseAction; EnterpriseClass; EnterpriseDomain; EnterpriseError; EnterpriseErrorSpec; EnterpriseJitterStrategy; EnterpriseOwner; EnterpriseRetryPolicy; EnterpriseSafeNextAction; EnterpriseSeverity; EnterpriseSubsystem; enterprise_catalog_fingerprint (+3 more)

### `backend/shared/errors/error_envelope.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=198 | code_lines=179 | risk_score=0
- API surface: public=1 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (4): shared.errors.enterprise_catalog; shared.errors.error_types; shared.observability.tracing; shared.utils.canonical_json
- External imports (2): __future__; typing
- Public API names: build_error_envelope

### `backend/shared/errors/error_response.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=505 | code_lines=450 | risk_score=14
- API surface: public=1 | top-level functions=17 | classes=0 | methods=0
- Runtime signals: async_functions=8 | try=4 | raise=0 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/17 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (7): shared.config.settings; shared.errors.enterprise_catalog; shared.errors.error_envelope; shared.errors.error_types; shared.observability.request_context; shared.security.input_sanitizer; shared.utils.app_logger
- External imports (6): __future__; fastapi; httpx; json; logging; typing
- Public API names: install_error_handlers

### `backend/shared/errors/error_types.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=137 | code_lines=107 | risk_score=0
- API surface: public=3 | top-level functions=1 | classes=2 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/1 (100%) | classes=0/2 (0%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): enum; fastapi; typing
- Public API names: ErrorCategory; ErrorCode; classified_http_exception

### `backend/shared/errors/external_codes.py`
- Module summary: Canonical external error code namespace.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=57 | code_lines=49 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (1): enum
- Public API names: ExternalErrorCode

### `backend/shared/errors/runtime_exception_policy.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=327 | code_lines=278 | risk_score=5
- API surface: public=9 | top-level functions=9 | classes=5 | methods=0
- Runtime signals: async_functions=1 | try=4 | raise=4 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/9 (0%) | classes=2/5 (40%) | methods=0/0 (n/a)
- Internal imports (2): shared.errors.error_types; shared.observability.request_context
- External imports (8): __future__; dataclasses; enum; hashlib; logging; threading; time; typing
- Public API names: FallbackPolicy; LineageRecordError; LineageUnavailableError; RuntimeZone; assert_lineage_available; fallback_value; log_exception_rate_limited; preserve_primary_exception; record_lineage_or_raise

### `backend/shared/i18n/__init__.py`
- Module summary: Shared i18n helpers (EN/KR).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=20 | code_lines=16 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): .context; .translator
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/i18n/context.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=21 | code_lines=11 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.language
- External imports (3): __future__; contextvars; typing
- Public API names: get_language; reset_language; set_language

### `backend/shared/i18n/middleware.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=198 | code_lines=168 | risk_score=9
- API surface: public=1 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=4 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/2 (50%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): shared.i18n.context; shared.i18n.translator; shared.utils.language
- External imports (6): __future__; fastapi; json; logging; starlette; typing
- Public API names: install_i18n_middleware

### `backend/shared/i18n/translator.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=200 | code_lines=165 | risk_score=6
- API surface: public=2 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=3/6 (50%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.language
- External imports (4): __future__; logging; re; typing
- Public API names: localize_free_text; m

### `backend/shared/interfaces/__init__.py`
- Module summary: Interface definitions for SPICE HARVESTER services
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=3 | code_lines=3 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/interfaces/type_inference.py`
- Module summary: 🔥 THINK ULTRA! Type Inference Interface
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=195 | code_lines=152 | risk_score=0
- API surface: public=4 | top-level functions=2 | classes=2 | methods=6
- Runtime signals: async_functions=5 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/2 (100%) | classes=2/2 (100%) | methods=6/6 (100%)
- Internal imports (1): shared.models.type_inference
- External imports (2): abc; typing
- Public API names: RealTypeInferenceService; TypeInferenceInterface; get_mock_type_inference_service; get_production_type_inference_service

### `backend/shared/middleware/__init__.py`
- Module summary: Shared middleware package.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=2 | code_lines=1 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/middleware/rate_limiter.py`
- Module summary: Rate Limiting Middleware for API Protection
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=604 | code_lines=482 | risk_score=17
- API surface: public=7 | top-level functions=4 | classes=4 | methods=12
- Runtime signals: async_functions=8 | try=4 | raise=2 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/4 (50%) | classes=4/4 (100%) | methods=8/12 (66%)
- Internal imports (5): shared.config.rate_limit_config; shared.config.settings; shared.errors.error_types; shared.security.auth_utils; shared.utils.app_logger
- External imports (8): fastapi; functools; hashlib; hmac; logging; redis; time; typing
- Public API names: LocalTokenBucket; RateLimitPresets; RateLimiter; TokenBucket; get_rate_limiter; install_rate_limit_headers_middleware; rate_limit

### `backend/shared/models/__init__.py`
- Module summary: Shared model definitions for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=44 | code_lines=37 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): .common; .google_sheets; .ontology; .ontology_resources; .pipeline_job
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/models/agent_plan_report.py`
- Module summary: Agent plan compilation/validation report models.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=113 | code_lines=89 | risk_score=0
- API surface: public=7 | top-level functions=0 | classes=7 | methods=3
- Runtime signals: async_functions=0 | try=0 | raise=5 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=2/7 (28%) | methods=0/3 (0%)
- Internal imports (0): not documented
- External imports (4): __future__; enum; pydantic; typing
- Public API names: PlanCompilationReport; PlanDiagnostic; PlanDiagnosticSeverity; PlanPatchOp; PlanPatchProposal; PlanPolicySnapshot; PlanRequiredControl

### `backend/shared/models/ai.py`
- Module summary: AI/LLM API models (domain-neutral).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=147 | code_lines=113 | risk_score=0
- API surface: public=12 | top-level functions=0 | classes=12 | methods=2
- Runtime signals: async_functions=0 | try=0 | raise=4 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/12 (8%) | methods=0/2 (0%)
- Internal imports (2): shared.models.graph_query; shared.models.ontology
- External imports (4): __future__; enum; pydantic; typing
- Public API names: AIAnswer; AIIntentDraft; AIIntentRequest; AIIntentResponse; AIIntentRoute; AIIntentType; AIQueryMode; AIQueryPlan; AIQueryRequest; AIQueryResponse; AIQueryTool; DatasetListQuery

### `backend/shared/models/audit_log.py`
- Module summary: Audit log models.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=44 | code_lines=28 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (4): __future__; datetime; pydantic; typing
- Public API names: AuditLogEntry

### `backend/shared/models/background_task.py`
- Module summary: Background Task Models
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=151 | code_lines=115 | risk_score=0
- API surface: public=7 | top-level functions=0 | classes=7 | methods=6
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=7/7 (100%) | methods=6/6 (100%)
- Internal imports (0): not documented
- External imports (4): datetime; enum; pydantic; typing
- Public API names: BackgroundTask; TaskFilter; TaskMetrics; TaskProgress; TaskResult; TaskStatus; TaskUpdateNotification

### `backend/shared/models/base.py`
- Module summary: Base models with MVCC support for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=129 | code_lines=104 | risk_score=0
- API surface: public=3 | top-level functions=0 | classes=3 | methods=6
- Runtime signals: async_functions=0 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=3/3 (100%) | methods=5/6 (83%)
- Internal imports (0): not documented
- External imports (2): pydantic; typing
- Public API names: ConcurrencyControl; OptimisticLockError; VersionedModelMixin

### `backend/shared/models/commands.py`
- Module summary: Command Models for Command/Event Sourcing Pattern
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=184 | code_lines=139 | risk_score=0
- API surface: public=11 | top-level functions=0 | classes=11 | methods=7
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=11/11 (100%) | methods=0/7 (0%)
- Internal imports (0): not documented
- External imports (5): datetime; enum; pydantic; typing; uuid
- Public API names: ActionCommand; BaseCommand; BranchCommand; CommandResult; CommandStatus; CommandType; DatabaseCommand; InstanceCommand; OntologyCommand; PropertyCommand; RelationshipCommand

### `backend/shared/models/common.py`
- Module summary: Common data types and enums for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=328 | code_lines=280 | risk_score=6
- API surface: public=4 | top-level functions=1 | classes=4 | methods=8
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=4/4 (100%) | methods=7/8 (87%)
- Internal imports (2): .query_operator_mixin; .responses
- External imports (5): dataclasses; enum; logging; typing; warnings
- Public API names: BaseResponse; Cardinality; DataType; QueryOperator

### `backend/shared/models/event_envelope.py`
- Module summary: Event envelope model used across services.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=229 | code_lines=200 | risk_score=5
- API surface: public=1 | top-level functions=0 | classes=1 | methods=6
- Runtime signals: async_functions=0 | try=1 | raise=2 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=1/6 (16%)
- Internal imports (3): .commands; .events; shared.config.settings
- External imports (6): __future__; datetime; logging; pydantic; typing; uuid
- Public API names: EventEnvelope

### `backend/shared/models/events.py`
- Module summary: Event Models for Command/Event Sourcing Pattern
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=195 | code_lines=145 | risk_score=0
- API surface: public=10 | top-level functions=0 | classes=10 | methods=8
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=10/10 (100%) | methods=0/8 (0%)
- Internal imports (0): not documented
- External imports (5): datetime; enum; pydantic; typing; uuid
- Public API names: ActionAppliedEvent; BaseEvent; BranchEvent; CommandFailedEvent; DatabaseEvent; EventType; InstanceEvent; OntologyEvent; PropertyEvent; RelationshipEvent

### `backend/shared/models/google_sheets.py`
- Module summary: Google Sheets models for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=140 | code_lines=111 | risk_score=0
- API surface: public=6 | top-level functions=1 | classes=6 | methods=6
- Runtime signals: async_functions=0 | try=0 | raise=6 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=5/6 (83%) | methods=5/6 (83%)
- Internal imports (1): shared.errors.external_codes
- External imports (2): pydantic; typing
- Public API names: GoogleSheetError; GoogleSheetPreviewRequest; GoogleSheetPreviewResponse; GoogleSheetRegisterRequest; GoogleSheetRegisterResponse; GoogleSheetUrlValidatedModel

### `backend/shared/models/graph_query.py`
- Module summary: Graph query models shared across routers/services.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=92 | code_lines=68 | risk_score=0
- API surface: public=6 | top-level functions=0 | classes=6 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=6/6 (100%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; pydantic; typing
- Public API names: GraphEdge; GraphHop; GraphNode; GraphQueryRequest; GraphQueryResponse; SimpleGraphQueryRequest

### `backend/shared/models/i18n.py`
- Module summary: I18N / localized-text primitives (model layer).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=16 | code_lines=8 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: not documented

### `backend/shared/models/lineage.py`
- Module summary: Lineage (provenance) models.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=51 | code_lines=36 | risk_score=0
- API surface: public=3 | top-level functions=0 | classes=3 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/3 (0%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (4): __future__; datetime; pydantic; typing
- Public API names: LineageEdge; LineageGraph; LineageNode

### `backend/shared/models/lineage_edge_types.py`
- Module summary: Canonical lineage edge type constants.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=30 | code_lines=27 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/models/objectify_job.py`
- Module summary: Objectify job payload shared between BFF and objectify worker.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=68 | code_lines=57 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=1
- Runtime signals: async_functions=0 | try=0 | raise=3 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/1 (0%) | methods=0/1 (0%)
- Internal imports (0): not documented
- External imports (4): __future__; datetime; pydantic; typing
- Public API names: ObjectifyJob

### `backend/shared/models/ontology.py`
- Module summary: Ontology models for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=541 | code_lines=430 | risk_score=0
- API surface: public=10 | top-level functions=4 | classes=10 | methods=30
- Runtime signals: async_functions=0 | try=0 | raise=22 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/4 (0%) | classes=10/10 (100%) | methods=17/30 (56%)
- Internal imports (4): .base; .i18n; .ontology_validation_mixin; .query_operator_mixin
- External imports (4): datetime; enum; pydantic; typing
- Public API names: Cardinality; OntologyBase; OntologyCreateRequest; OntologyResponse; OntologyUpdateRequest; Property; QueryFilter; QueryInput; QueryOperator; Relationship

### `backend/shared/models/ontology_lint.py`
- Module summary: Ontology linting models (domain-neutral).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=40 | code_lines=30 | risk_score=0
- API surface: public=3 | top-level functions=0 | classes=3 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/3 (0%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): enum; pydantic; typing
- Public API names: LintIssue; LintReport; LintSeverity

### `backend/shared/models/ontology_resources.py`
- Module summary: Ontology resource models (shared properties, value types, interfaces, groups, functions, action types).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=120 | code_lines=86 | risk_score=0
- API surface: public=9 | top-level functions=0 | classes=9 | methods=2
- Runtime signals: async_functions=0 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=9/9 (100%) | methods=0/2 (0%)
- Internal imports (2): .i18n; .ontology
- External imports (4): __future__; datetime; pydantic; typing
- Public API names: ActionTypeDefinition; FunctionDefinition; GroupDefinition; InterfaceDefinition; LinkTypeDefinition; OntologyResourceBase; OntologyResourceRecord; SharedPropertyDefinition; ValueTypeDefinition

### `backend/shared/models/ontology_validation_mixin.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=47 | code_lines=34 | risk_score=0
- API surface: public=2 | top-level functions=0 | classes=2 | methods=2
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/2 (0%) | methods=0/2 (0%)
- Internal imports (0): not documented
- External imports (3): __future__; re; typing
- Public API names: CardinalityValidationMixin; PropertyValueValidationMixin

### `backend/shared/models/pipeline_agent.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=45 | code_lines=37 | risk_score=0
- API surface: public=2 | top-level functions=0 | classes=2 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/2 (0%) | methods=0/0 (n/a)
- Internal imports (1): shared.models.pipeline_plan
- External imports (3): __future__; pydantic; typing
- Public API names: PipelineAgentRunRequest; PipelineOutputBinding

### `backend/shared/models/pipeline_job.py`
- Module summary: Pipeline job message payload.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=84 | code_lines=72 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=2
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/1 (0%) | methods=2/2 (100%)
- Internal imports (0): not documented
- External imports (5): __future__; datetime; hashlib; pydantic; typing
- Public API names: PipelineJob

### `backend/shared/models/pipeline_plan.py`
- Module summary: Pipeline plan schema for LLM-generated pipeline definitions.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=174 | code_lines=153 | risk_score=11
- API surface: public=5 | top-level functions=0 | classes=5 | methods=3
- Runtime signals: async_functions=0 | try=2 | raise=1 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/5 (0%) | methods=0/3 (0%)
- Internal imports (2): shared.models.pipeline_task_spec; shared.services.pipeline.output_plugins
- External imports (7): __future__; datetime; enum; logging; pydantic; typing; uuid
- Public API names: PipelinePlan; PipelinePlanAssociation; PipelinePlanDataScope; PipelinePlanOutput; PipelinePlanOutputKind

### `backend/shared/models/pipeline_task_spec.py`
- Module summary: Pipeline task spec (intent/scope contract).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=55 | code_lines=39 | risk_score=0
- API surface: public=3 | top-level functions=0 | classes=3 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/3 (33%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (4): __future__; enum; pydantic; typing
- Public API names: PipelineTaskIntent; PipelineTaskScope; PipelineTaskSpec

### `backend/shared/models/query_operator_mixin.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=10 | code_lines=6 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=1
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/1 (0%) | methods=0/1 (0%)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: QueryOperatorApplicabilityMixin

### `backend/shared/models/requests.py`
- Module summary: Request models for SPICE HARVESTER services
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=70 | code_lines=44 | risk_score=0
- API surface: public=7 | top-level functions=0 | classes=7 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=7/7 (100%) | methods=0/0 (n/a)
- Internal imports (1): .responses
- External imports (2): pydantic; typing
- Public API names: BranchCreateRequest; CheckoutRequest; CommitRequest; DatabaseCreateRequest; MappingImportRequest; MergeRequest; RollbackRequest

### `backend/shared/models/responses.py`
- Module summary: Response models for SPICE HARVESTER services
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=93 | code_lines=74 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=12
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=12/12 (100%)
- Internal imports (0): not documented
- External imports (2): dataclasses; typing
- Public API names: ApiResponse

### `backend/shared/models/sheet_grid.py`
- Module summary: Sheet grid extraction models.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=66 | code_lines=46 | risk_score=0
- API surface: public=3 | top-level functions=0 | classes=3 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=3/3 (100%) | methods=0/0 (n/a)
- Internal imports (1): shared.models.structure_analysis
- External imports (3): __future__; pydantic; typing
- Public API names: GoogleSheetGridRequest; GoogleSheetStructureAnalysisRequest; SheetGrid

### `backend/shared/models/structure_analysis.py`
- Module summary: Structure analysis models for SPICE HARVESTER.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=167 | code_lines=118 | risk_score=0
- API surface: public=10 | top-level functions=0 | classes=10 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=10/10 (100%) | methods=0/0 (n/a)
- Internal imports (1): shared.models.type_inference
- External imports (3): __future__; pydantic; typing
- Public API names: BoundingBox; CellAddress; CellEvidence; ColumnProvenance; DetectedTable; HeaderTreeNode; KeyValueItem; MergeRange; SheetStructureAnalysisRequest; SheetStructureAnalysisResponse

### `backend/shared/models/structure_patch.py`
- Module summary: Structure analysis patch models.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=56 | code_lines=34 | risk_score=0
- API surface: public=2 | top-level functions=0 | classes=2 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=2/2 (100%) | methods=0/0 (n/a)
- Internal imports (1): shared.models.structure_analysis
- External imports (3): __future__; pydantic; typing
- Public API names: SheetStructurePatch; SheetStructurePatchOp

### `backend/shared/models/sync_wrapper.py`
- Module summary: Synchronous API Wrapper Models
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=96 | code_lines=74 | risk_score=0
- API surface: public=3 | top-level functions=0 | classes=3 | methods=1
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=3/3 (100%) | methods=0/1 (0%)
- Internal imports (0): not documented
- External imports (2): pydantic; typing
- Public API names: SyncOptions; SyncResult; TimeoutError

### `backend/shared/models/type_inference.py`
- Module summary: Funnel Service Models
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=181 | code_lines=136 | risk_score=0
- API surface: public=13 | top-level functions=1 | classes=13 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=13/13 (100%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): datetime; pydantic; typing
- Public API names: ColumnAnalysisResult; ColumnProfile; DatasetAnalysisRequest; DatasetAnalysisResponse; FunnelAnalysisPayload; FunnelPreviewRequest; FunnelPreviewResponse; FunnelRiskItem; SchemaGenerationRequest; SchemaGenerationResponse; TypeInferenceResult; TypeMappingRequest (+1 more)

### `backend/shared/observability/__init__.py`
- Module summary: Shared observability helpers (tracing/metrics/context).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=2 | code_lines=1 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/observability/config_monitor.py`
- Module summary: Configuration Monitoring and Observability
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=589 | code_lines=467 | risk_score=18
- API surface: public=6 | top-level functions=0 | classes=6 | methods=25
- Runtime signals: async_functions=0 | try=3 | raise=0 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=6/6 (100%) | methods=24/25 (96%)
- Internal imports (1): shared.config.settings
- External imports (7): dataclasses; datetime; enum; hashlib; json; logging; typing
- Public API names: ConfigChange; ConfigChangeType; ConfigSecurityAudit; ConfigSeverity; ConfigValidationRule; ConfigurationMonitor

### `backend/shared/observability/context_propagation.py`
- Module summary: Trace context propagation helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=403 | code_lines=338 | risk_score=12
- API surface: public=9 | top-level functions=12 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=12 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=7/12 (58%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.observability.request_context
- External imports (5): __future__; contextlib; logging; typing; urllib
- Public API names: attach_context_from_carrier; attach_context_from_kafka; carrier_from_envelope_metadata; carrier_from_kafka_headers; enrich_metadata_with_current_trace; kafka_headers_from_carrier; kafka_headers_from_current_context; kafka_headers_from_envelope_metadata; kafka_headers_with_dedup

### `backend/shared/observability/logging.py`
- Module summary: Logging helpers for observability.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=202 | code_lines=154 | risk_score=6
- API surface: public=3 | top-level functions=2 | classes=1 | methods=1
- Runtime signals: async_functions=0 | try=6 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/2 (100%) | classes=1/1 (100%) | methods=0/1 (0%)
- Internal imports (1): shared.observability.request_context
- External imports (3): __future__; logging; typing
- Public API names: TraceContextFilter; install_trace_context_filter; install_trace_context_record_factory

### `backend/shared/observability/metrics.py`
- Module summary: Metrics Collection for OpenTelemetry
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=1046 | code_lines=884 | risk_score=140
- API surface: public=8 | top-level functions=11 | classes=4 | methods=16
- Runtime signals: async_functions=2 | try=26 | raise=1 | broad_except=23 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=4/11 (36%) | classes=2/4 (50%) | methods=11/16 (68%)
- Internal imports (2): shared.config.settings; shared.utils.app_logger
- External imports (7): contextlib; functools; logging; opentelemetry; prometheus_client; time; typing
- Public API names: MetricsCollector; OpenTelemetryMetricsConfig; RequestMetricsMiddleware; get_metrics_collector; get_metrics_runtime_status; initialize_metrics_provider; measure_time; prometheus_latest

### `backend/shared/observability/request_context.py`
- Module summary: Request/operation context for debugging.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=198 | code_lines=156 | risk_score=25
- API surface: public=9 | top-level functions=10 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=5 | raise=0 | broad_except=4 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/10 (20%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.blank_utils
- External imports (7): __future__; contextlib; contextvars; logging; typing; urllib; uuid
- Public API names: context_from_headers; context_from_metadata; generate_request_id; get_correlation_id; get_db_name; get_principal; get_request_id; parse_baggage_header; request_context

### `backend/shared/observability/tracing.py`
- Module summary: OpenTelemetry tracing helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=643 | code_lines=516 | risk_score=174
- API surface: public=8 | top-level functions=8 | classes=3 | methods=17
- Runtime signals: async_functions=2 | try=30 | raise=1 | broad_except=29 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=4/8 (50%) | classes=1/3 (33%) | methods=0/17 (0%)
- Internal imports (2): shared.config.settings; shared.utils.app_logger
- External imports (6): __future__; contextlib; functools; logging; os; typing
- Public API names: OpenTelemetryConfig; TracingService; get_tracing_service; trace_db_operation; trace_endpoint; trace_external_call; trace_kafka_operation; trace_storage_operation

### `backend/shared/routers/__init__.py`
- Module summary: Shared FastAPI routers (monitoring/config).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=2 | code_lines=1 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/routers/config_monitoring.py`
- Module summary: Configuration Monitoring Router
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=568 | code_lines=466 | risk_score=45
- API surface: public=10 | top-level functions=10 | classes=0 | methods=0
- Runtime signals: async_functions=10 | try=12 | raise=14 | broad_except=9 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=10/10 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (4): shared.config.settings; shared.dependencies.providers; shared.errors.error_types; shared.observability.config_monitor
- External imports (4): datetime; fastapi; logging; typing
- Public API names: analyze_configuration_health_impact; analyze_environment_drift; check_configuration_changes; get_config_monitor; get_configuration_changes; get_configuration_report; get_current_configuration; get_monitoring_status; perform_security_audit; validate_configuration

### `backend/shared/routers/monitoring.py`
- Module summary: Monitoring and Observability Router
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=638 | code_lines=538 | risk_score=28
- API surface: public=12 | top-level functions=14 | classes=0 | methods=0
- Runtime signals: async_functions=13 | try=5 | raise=2 | broad_except=5 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=13/14 (92%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (6): shared.config.settings; shared.dependencies; shared.dependencies.providers; shared.errors.error_envelope; shared.errors.error_types; shared.observability.request_context
- External imports (6): datetime; fastapi; logging; starlette; time; typing
- Public API names: basic_health_check; detailed_health_check; get_active_background_tasks; get_background_task_health; get_background_task_metrics; get_configuration_overview; get_service_dependencies; get_service_metrics; get_service_status; liveness_probe; readiness_probe; restart_service

### `backend/shared/security/__init__.py`
- Module summary: Security utilities for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=19 | code_lines=17 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): .input_sanitizer
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/security/auth_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=127 | code_lines=101 | risk_score=0
- API surface: public=8 | top-level functions=8 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/8 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.config.settings
- External imports (3): __future__; os; typing
- Public API names: auth_disable_allowed; auth_required; enforce_db_scope; extract_presented_token; get_db_scope; get_exempt_paths; get_expected_token; is_exempt_path

### `backend/shared/security/data_encryption.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=170 | code_lines=139 | risk_score=15
- API surface: public=6 | top-level functions=8 | classes=1 | methods=7
- Runtime signals: async_functions=0 | try=3 | raise=8 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/8 (12%) | classes=0/1 (0%) | methods=0/7 (0%)
- Internal imports (0): not documented
- External imports (8): __future__; base64; cryptography; dataclasses; json; logging; os; typing
- Public API names: DataEncryptor; encryptor_from_keys; is_encrypted_bytes; is_encrypted_json; is_encrypted_text; parse_encryption_keys

### `backend/shared/security/database_access.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=312 | code_lines=269 | risk_score=9
- API surface: public=12 | top-level functions=12 | classes=0 | methods=0
- Runtime signals: async_functions=9 | try=12 | raise=3 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/12 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.config.settings
- External imports (4): __future__; asyncpg; collections; typing
- Public API names: delete_database_access_entries; enforce_database_role; ensure_database_access_table; fetch_database_access_entries; get_database_access_role; has_database_access_config; list_database_names; normalize_database_role; resolve_database_actor; resolve_database_actor_with_name; upsert_database_access_entry; upsert_database_owner

### `backend/shared/security/input_sanitizer.py`
- Module summary: Comprehensive Input Sanitization Module
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=897 | code_lines=653 | risk_score=0
- API surface: public=9 | top-level functions=7 | classes=2 | methods=24
- Runtime signals: async_functions=0 | try=3 | raise=72 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=7/7 (100%) | classes=2/2 (100%) | methods=23/24 (95%)
- Internal imports (1): shared.config.settings
- External imports (4): logging; re; typing; urllib
- Public API names: InputSanitizer; SecurityViolationError; sanitize_es_query; sanitize_input; sanitize_label_input; validate_branch_name; validate_class_id; validate_db_name; validate_instance_id

### `backend/shared/security/principal_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=57 | code_lines=46 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: actor_label; resolve_principal_from_headers

### `backend/shared/security/user_context.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=247 | code_lines=206 | risk_score=0
- API surface: public=4 | top-level functions=8 | classes=2 | methods=0
- Runtime signals: async_functions=2 | try=3 | raise=14 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/8 (12%) | classes=2/2 (100%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (6): __future__; dataclasses; httpx; jose; time; typing
- Public API names: UserPrincipal; UserTokenError; extract_bearer_token; verify_user_token

### `backend/shared/serializers/__init__.py`
- Module summary: Serializers for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=7 | code_lines=5 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): .complex_type_serializer
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/serializers/complex_type_serializer.py`
- Module summary: Complex type serializer for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=450 | code_lines=394 | risk_score=25
- API surface: public=1 | top-level functions=0 | classes=1 | methods=22
- Runtime signals: async_functions=0 | try=15 | raise=0 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=22/22 (100%)
- Internal imports (1): ..models.common
- External imports (3): json; logging; typing
- Public API names: ComplexTypeSerializer

### `backend/shared/services/__init__.py`
- Module summary: Shared services module
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=28 | code_lines=21 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/services/agent/__init__.py`
- Module summary: Package: $dir
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=8 | code_lines=6 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/services/agent/agent_retention_worker.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=331 | code_lines=303 | risk_score=45
- API surface: public=1 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=10 | raise=0 | broad_except=7 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/4 (25%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.services.registries.agent_session_registry; shared.services.storage.storage_service
- External imports (6): __future__; asyncio; datetime; json; logging; typing
- Public API names: run_agent_session_retention_worker

### `backend/shared/services/agent/agent_tool_allowlist.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=142 | code_lines=125 | risk_score=0
- API surface: public=2 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=2 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.registries.agent_tool_registry
- External imports (4): __future__; json; pathlib; typing
- Public API names: bootstrap_agent_tool_allowlist; load_agent_tool_allowlist_bundle

### `backend/shared/services/agent/llm_gateway.py`
- Module summary: LLM Gateway (domain-neutral, enterprise-safe).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=1453 | code_lines=1288 | risk_score=60
- API surface: public=8 | top-level functions=17 | classes=7 | methods=15
- Runtime signals: async_functions=6 | try=15 | raise=32 | broad_except=12 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=6/17 (35%) | classes=2/7 (28%) | methods=0/15 (0%)
- Internal imports (4): shared.config.settings; shared.services.core.audit_log_store; shared.services.storage.redis_service; shared.utils.llm_safety
- External imports (10): __future__; asyncio; dataclasses; httpx; json; logging; pydantic; re (+2 more)
- Public API names: LLMCallMeta; LLMGateway; LLMHTTPStatusError; LLMOutputValidationError; LLMPolicyError; LLMRequestError; LLMUnavailableError; create_llm_gateway

### `backend/shared/services/agent/llm_quota.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=172 | code_lines=140 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=2 | methods=1
- Runtime signals: async_functions=2 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/3 (33%) | classes=0/2 (0%) | methods=0/1 (0%)
- Internal imports (2): shared.services.storage.redis_service; shared.utils.token_count
- External imports (5): __future__; dataclasses; datetime; re; typing
- Public API names: LLMQuotaExceededError; LLMQuotaSpec; enforce_llm_quota

### `backend/shared/services/core/__init__.py`
- Module summary: Package: $dir
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=8 | code_lines=6 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/services/core/audit_log_store.py`
- Module summary: First-class audit log store (Postgres).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=403 | code_lines=365 | risk_score=0
- API surface: public=2 | top-level functions=1 | classes=1 | methods=9
- Runtime signals: async_functions=6 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/1 (0%) | methods=0/9 (0%)
- Internal imports (4): shared.config.settings; shared.models.audit_log; shared.services.registries.postgres_schema_registry; shared.utils.sql_filter_builder
- External imports (7): __future__; asyncpg; datetime; hashlib; json; typing; uuid
- Public API names: AuditLogStore; create_audit_log_store

### `backend/shared/services/core/background_task_manager.py`
- Module summary: Background Task Manager Service
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=678 | code_lines=534 | risk_score=29
- API surface: public=3 | top-level functions=1 | classes=2 | methods=26
- Runtime signals: async_functions=22 | try=6 | raise=2 | broad_except=5 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=2/2 (100%) | methods=26/26 (100%)
- Internal imports (3): shared.models.background_task; shared.services.core.websocket_service; shared.services.storage.redis_service
- External imports (7): asyncio; datetime; enum; logging; traceback; typing; uuid
- Public API names: BackgroundTaskManager; TaskPriority; create_background_task_manager

### `backend/shared/services/core/command_status_service.py`
- Module summary: Command Status Tracking Service
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=462 | code_lines=371 | risk_score=0
- API surface: public=2 | top-level functions=0 | classes=2 | methods=13
- Runtime signals: async_functions=12 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=2/2 (100%) | methods=12/13 (92%)
- Internal imports (3): shared.config.app_config; shared.config.settings; shared.services.storage.redis_service
- External imports (4): datetime; enum; logging; typing
- Public API names: CommandStatus; CommandStatusService

### `backend/shared/services/core/consistency_token.py`
- Module summary: Consistency Token Service
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=408 | code_lines=302 | risk_score=10
- API surface: public=4 | top-level functions=1 | classes=3 | methods=15
- Runtime signals: async_functions=11 | try=2 | raise=2 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=3/3 (100%) | methods=13/15 (86%)
- Internal imports (0): not documented
- External imports (9): asyncio; dataclasses; datetime; hashlib; json; logging; redis; time (+1 more)
- Public API names: CommandResponseWithToken; ConsistencyToken; ConsistencyTokenService; demo_consistency_token

### `backend/shared/services/core/graph_federation_service_es.py`
- Module summary: ES-Native Graph Federation Service — Search Arounds (Link Traversal).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=637 | code_lines=517 | risk_score=11
- API surface: public=1 | top-level functions=0 | classes=1 | methods=17
- Runtime signals: async_functions=9 | try=2 | raise=1 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=15/17 (88%)
- Internal imports (4): shared.config.search_config; shared.config.settings; shared.security.input_sanitizer; shared.services.storage.elasticsearch_service
- External imports (4): __future__; collections; logging; typing
- Public API names: GraphFederationServiceES

### `backend/shared/services/core/health_check.py`
- Module summary: Service Health Check System
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=500 | code_lines=393 | risk_score=31
- API surface: public=10 | top-level functions=0 | classes=10 | methods=28
- Runtime signals: async_functions=7 | try=6 | raise=0 | broad_except=5 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=10/10 (100%) | methods=14/28 (50%)
- Internal imports (0): not documented
- External imports (8): abc; asyncio; dataclasses; datetime; enum; logging; time; typing
- Public API names: AggregatedHealthStatus; DatabaseHealthCheck; ElasticsearchHealthCheck; HealthCheckAggregator; HealthCheckInterface; HealthCheckResult; HealthStatus; RedisHealthCheck; ServiceType; StorageHealthCheck

### `backend/shared/services/core/instance_index_rebuild_service.py`
- Module summary: Instance index rebuild service.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=254 | code_lines=211 | risk_score=29
- API surface: public=4 | top-level functions=4 | classes=3 | methods=0
- Runtime signals: async_functions=4 | try=5 | raise=1 | broad_except=5 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=4/4 (100%) | classes=1/3 (33%) | methods=0/0 (n/a)
- Internal imports (3): objectify_worker.write_paths; shared.config.search_config; shared.services.storage.elasticsearch_service
- External imports (6): __future__; dataclasses; datetime; logging; typing; uuid
- Public API names: RebuildClassResult; RebuildIndexRequest; RebuildIndexResult; rebuild_instance_index

### `backend/shared/services/core/object_type_meta_resolver.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=60 | code_lines=48 | risk_score=1
- API surface: public=2 | top-level functions=1 | classes=1 | methods=0
- Runtime signals: async_functions=1 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (2): shared.utils.resource_rid; shared.utils.writeback_conflicts
- External imports (3): __future__; logging; typing
- Public API names: ObjectTypeMeta; build_object_type_meta_resolver

### `backend/shared/services/core/ontology_linter.py`
- Module summary: Domain-neutral ontology linter (backend guardrails).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=553 | code_lines=501 | risk_score=0
- API surface: public=5 | top-level functions=8 | classes=1 | methods=1
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=3/8 (37%) | classes=1/1 (100%) | methods=0/1 (0%)
- Internal imports (5): shared.config.settings; shared.i18n; shared.models.ontology; shared.models.ontology_lint; shared.utils.branch_utils
- External imports (4): __future__; dataclasses; re; typing
- Public API names: OntologyLinterConfig; compute_risk_score; lint_ontology_create; lint_ontology_update; risk_level

### `backend/shared/services/core/projection_consistency.py`
- Module summary: Projection consistency helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=202 | code_lines=157 | risk_score=0
- API surface: public=7 | top-level functions=7 | classes=3 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=4/7 (57%) | classes=0/3 (0%) | methods=0/0 (n/a)
- Internal imports (1): shared.models.event_envelope
- External imports (4): __future__; dataclasses; datetime; typing
- Public API names: ExpectationBuildResult; InstanceProjectionExpectation; InstanceProjectionKey; build_instance_expectations; evaluate_projection_document; key_from_instance_event; parse_instance_aggregate_id

### `backend/shared/services/core/projection_position_tracker.py`
- Module summary: Projection Position Tracker.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=182 | code_lines=170 | risk_score=24
- API surface: public=1 | top-level functions=0 | classes=1 | methods=6
- Runtime signals: async_functions=5 | try=4 | raise=0 | broad_except=4 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=6/6 (100%)
- Internal imports (0): not documented
- External imports (3): __future__; logging; typing
- Public API names: ProjectionPositionTracker

### `backend/shared/services/core/relationship_extractor.py`
- Module summary: Relationship extraction from instance payload using ontology schema.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=202 | code_lines=170 | risk_score=10
- API surface: public=1 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=2 | raise=6 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=6/6 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; logging; typing
- Public API names: extract_relationships

### `backend/shared/services/core/schema_change_monitor.py`
- Module summary: Schema Change Monitor Service
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=426 | code_lines=330 | risk_score=44
- API surface: public=2 | top-level functions=0 | classes=2 | methods=13
- Runtime signals: async_functions=7 | try=9 | raise=0 | broad_except=7 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=2/2 (100%) | methods=12/13 (92%)
- Internal imports (3): shared.errors.error_envelope; shared.errors.error_types; shared.services.core.schema_drift_detector
- External imports (5): asyncio; dataclasses; datetime; logging; typing
- Public API names: MonitorConfig; SchemaChangeMonitor

### `backend/shared/services/core/schema_drift_detector.py`
- Module summary: Schema Drift Detector Service
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=468 | code_lines=372 | risk_score=0
- API surface: public=5 | top-level functions=0 | classes=5 | methods=13
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=5/5 (100%) | methods=10/13 (76%)
- Internal imports (1): shared.utils.schema_hash
- External imports (5): dataclasses; datetime; difflib; logging; typing
- Public API names: ImpactedMapping; SchemaChange; SchemaDrift; SchemaDriftConfig; SchemaDriftDetector

### `backend/shared/services/core/schema_versioning.py`
- Module summary: Schema Versioning Service
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=440 | code_lines=348 | risk_score=5
- API surface: public=5 | top-level functions=0 | classes=5 | methods=18
- Runtime signals: async_functions=0 | try=1 | raise=6 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=5/5 (100%) | methods=14/18 (77%)
- Internal imports (0): not documented
- External imports (3): enum; logging; typing
- Public API names: MigrationStrategy; SchemaMigration; SchemaRegistry; SchemaVersion; SchemaVersioningService

### `backend/shared/services/core/sequence_service.py`
- Module summary: Sequence Service for Per-Aggregate Ordering
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=283 | code_lines=206 | risk_score=0
- API surface: public=2 | top-level functions=0 | classes=2 | methods=12
- Runtime signals: async_functions=8 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=2/2 (100%) | methods=12/12 (100%)
- Internal imports (0): not documented
- External imports (3): logging; redis; typing
- Public API names: SequenceService; SequenceValidator

### `backend/shared/services/core/service_container_common.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=44 | code_lines=36 | risk_score=12
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=2 | raise=0 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=2/2 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): shared.config.settings; shared.middleware.rate_limiter; shared.services.storage.elasticsearch_service
- External imports (3): __future__; logging; typing
- Public API names: initialize_elasticsearch_service; initialize_rate_limiter_service

### `backend/shared/services/core/service_factory.py`
- Module summary: Service Factory Module
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=595 | code_lines=491 | risk_score=37
- API surface: public=8 | top-level functions=14 | classes=1 | methods=1
- Runtime signals: async_functions=7 | try=7 | raise=0 | broad_except=6 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=10/14 (71%) | classes=1/1 (100%) | methods=0/1 (0%)
- Internal imports (6): shared.config.settings; shared.errors.error_response; shared.i18n.middleware; shared.middleware.rate_limiter; shared.models.requests; shared.observability.request_context
- External imports (8): contextlib; datetime; fastapi; logging; starlette; time; typing; uvicorn
- Public API names: ServiceInfo; create_fastapi_service; create_uvicorn_config; get_agent_service_info; get_bff_service_info; get_funnel_service_info; get_oms_service_info; run_service

### `backend/shared/services/core/sheet_grid_parser.py`
- Module summary: Sheet grid parser/extractor.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=552 | code_lines=441 | risk_score=60
- API surface: public=2 | top-level functions=0 | classes=2 | methods=11
- Runtime signals: async_functions=0 | try=11 | raise=1 | broad_except=10 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=2/2 (100%) | methods=4/11 (36%)
- Internal imports (2): shared.models.sheet_grid; shared.models.structure_analysis
- External imports (8): __future__; dataclasses; datetime; decimal; io; logging; re; typing
- Public API names: SheetGridParseOptions; SheetGridParser

### `backend/shared/services/core/sheet_import_service.py`
- Module summary: Sheet import service (shared).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=365 | code_lines=297 | risk_score=37
- API surface: public=2 | top-level functions=0 | classes=2 | methods=5
- Runtime signals: async_functions=0 | try=7 | raise=0 | broad_except=6 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/2 (50%) | methods=3/5 (60%)
- Internal imports (3): shared.errors.external_codes; shared.utils.blank_utils; shared.validators.money_validator
- External imports (8): __future__; dataclasses; datetime; decimal; json; logging; re; typing
- Public API names: FieldMapping; SheetImportService

### `backend/shared/services/core/sync_wrapper_service.py`
- Module summary: Synchronous API Wrapper Service
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=240 | code_lines=179 | risk_score=10
- API surface: public=1 | top-level functions=0 | classes=1 | methods=5
- Runtime signals: async_functions=4 | try=2 | raise=3 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=4/5 (80%)
- Internal imports (3): shared.models.commands; shared.models.sync_wrapper; shared.services.core.command_status_service
- External imports (5): asyncio; datetime; logging; time; typing
- Public API names: SyncWrapperService

### `backend/shared/services/core/watermark_monitor.py`
- Module summary: Global Watermark Monitoring Service
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=481 | code_lines=365 | risk_score=19
- API surface: public=4 | top-level functions=1 | classes=3 | methods=14
- Runtime signals: async_functions=10 | try=4 | raise=0 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=3/3 (100%) | methods=14/14 (100%)
- Internal imports (1): shared.services.kafka.safe_consumer
- External imports (9): asyncio; confluent_kafka; dataclasses; datetime; json; logging; redis; time (+1 more)
- Public API names: GlobalWatermark; PartitionWatermark; WatermarkMonitor; create_watermark_monitor

### `backend/shared/services/core/websocket_service.py`
- Module summary: WebSocket Service for Real-time Command Status Updates
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=640 | code_lines=488 | risk_score=41
- API surface: public=6 | top-level functions=3 | classes=3 | methods=25
- Runtime signals: async_functions=20 | try=8 | raise=2 | broad_except=7 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/3 (66%) | classes=3/3 (100%) | methods=23/25 (92%)
- Internal imports (1): shared.services.storage.redis_service
- External imports (7): asyncio; dataclasses; datetime; fastapi; json; logging; typing
- Public API names: WebSocketConnection; WebSocketConnectionManager; WebSocketNotificationService; get_connection_manager; get_notification_service; utc_now

### `backend/shared/services/core/worker_stores.py`
- Module summary: Worker store bootstrap helpers (Facade).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=138 | code_lines=117 | risk_score=17
- API surface: public=3 | top-level functions=1 | classes=2 | methods=2
- Runtime signals: async_functions=4 | try=3 | raise=1 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=1/2 (50%) | methods=0/2 (0%)
- Internal imports (7): shared.config.settings; shared.errors.error_types; shared.errors.runtime_exception_policy; shared.services.core.audit_log_store; shared.services.registries.lineage_store; shared.services.registries.processed_event_registry; shared.services.registries.processed_event_registry_factory
- External imports (4): __future__; dataclasses; logging; typing
- Public API names: WorkerObservability; WorkerStores; initialize_worker_stores

### `backend/shared/services/core/writeback_merge_service.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=264 | code_lines=237 | risk_score=2
- API surface: public=2 | top-level functions=2 | classes=2 | methods=2
- Runtime signals: async_functions=1 | try=5 | raise=3 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=1/2 (50%) | methods=0/2 (0%)
- Internal imports (6): shared.config.app_config; shared.services.storage.lakefs_storage_service; shared.services.storage.storage_service; shared.utils.writeback_lifecycle; shared.utils.writeback_patch_apply; shared.utils.writeback_paths
- External imports (3): __future__; dataclasses; typing
- Public API names: WritebackMergeService; WritebackMergedInstance

### `backend/shared/services/events/__init__.py`
- Module summary: Package: $dir
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=8 | code_lines=6 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/services/events/aggregate_sequence_allocator.py`
- Module summary: Postgres-backed per-aggregate sequence allocator (write-side).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=318 | code_lines=283 | risk_score=0
- API surface: public=2 | top-level functions=0 | classes=2 | methods=10
- Runtime signals: async_functions=7 | try=0 | raise=22 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=2/2 (100%) | methods=4/10 (40%)
- Internal imports (1): shared.config.settings
- External imports (3): __future__; asyncpg; typing
- Public API names: AggregateSequenceAllocator; OptimisticConcurrencyError

### `backend/shared/services/events/dataset_ingest_outbox.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=342 | code_lines=319 | risk_score=10
- API surface: public=4 | top-level functions=4 | classes=1 | methods=7
- Runtime signals: async_functions=8 | try=3 | raise=4 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/1 (0%) | methods=0/7 (0%)
- Internal imports (13): shared.config.app_config; shared.config.settings; shared.models.event_envelope; shared.observability.context_propagation; shared.observability.tracing; shared.services.events.outbox_runtime; shared.services.kafka.dlq_publisher; shared.services.kafka.producer_factory (+5 more)
- External imports (6): __future__; asyncio; confluent_kafka; datetime; logging; typing
- Public API names: DatasetIngestOutboxPublisher; build_dataset_event_payload; flush_dataset_ingest_outbox; run_dataset_ingest_outbox_worker

### `backend/shared/services/events/dataset_ingest_reconciler.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=35 | code_lines=29 | risk_score=7
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=2 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.registries.dataset_registry
- External imports (4): __future__; asyncio; logging; typing
- Public API names: run_dataset_ingest_reconciler

### `backend/shared/services/events/dlq_handler_fixed.py`
- Module summary: Dead Letter Queue Handler Service - FIXED VERSION
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=459 | code_lines=341 | risk_score=35
- API surface: public=4 | top-level functions=0 | classes=4 | methods=16
- Runtime signals: async_functions=10 | try=7 | raise=2 | broad_except=6 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=4/4 (100%) | methods=16/16 (100%)
- Internal imports (3): shared.services.kafka.consumer_ops; shared.services.kafka.producer_factory; shared.services.kafka.safe_consumer
- External imports (10): asyncio; confluent_kafka; dataclasses; datetime; enum; hashlib; json; logging (+2 more)
- Public API names: DLQHandlerFixed; FailedMessage; RetryPolicy; RetryStrategy

### `backend/shared/services/events/event_replay.py`
- Module summary: Event Replay Service
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=371 | code_lines=291 | risk_score=0
- API surface: public=2 | top-level functions=1 | classes=1 | methods=7
- Runtime signals: async_functions=6 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=1/1 (100%) | methods=6/7 (85%)
- Internal imports (0): not documented
- External imports (8): asyncio; boto3; botocore; collections; datetime; hashlib; json; typing
- Public API names: EventReplayService; demo_replay

### `backend/shared/services/events/idempotency_service.py`
- Module summary: Idempotency Service for Event Processing
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=340 | code_lines=262 | risk_score=5
- API surface: public=2 | top-level functions=0 | classes=2 | methods=10
- Runtime signals: async_functions=6 | try=1 | raise=1 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=2/2 (100%) | methods=10/10 (100%)
- Internal imports (0): not documented
- External imports (6): datetime; hashlib; json; logging; redis; typing
- Public API names: IdempotencyService; IdempotentEventProcessor

### `backend/shared/services/events/objectify_job_queue.py`
- Module summary: Objectify job queue publisher using a Postgres outbox.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=38 | code_lines=29 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=4
- Runtime signals: async_functions=3 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/1 (0%) | methods=0/4 (0%)
- Internal imports (2): shared.models.objectify_job; shared.services.registries.objectify_registry
- External imports (3): __future__; logging; typing
- Public API names: ObjectifyJobQueue

### `backend/shared/services/events/objectify_outbox.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=267 | code_lines=222 | risk_score=6
- API surface: public=2 | top-level functions=1 | classes=1 | methods=5
- Runtime signals: async_functions=5 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/1 (0%) | methods=1/5 (20%)
- Internal imports (8): shared.config.app_config; shared.config.settings; shared.observability.context_propagation; shared.observability.tracing; shared.services.events.outbox_runtime; shared.services.kafka.producer_factory; shared.services.registries.objectify_registry; shared.utils.backoff_utils
- External imports (6): __future__; asyncio; datetime; json; logging; typing
- Public API names: ObjectifyOutboxPublisher; run_objectify_outbox_worker

### `backend/shared/services/events/objectify_reconciler.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=222 | code_lines=200 | risk_score=15
- API surface: public=2 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=5 | raise=10 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (6): shared.config.settings; shared.models.objectify_job; shared.services.registries.dataset_registry; shared.services.registries.objectify_registry; shared.services.registries.pipeline_registry; shared.utils.objectify_outputs
- External imports (5): __future__; asyncio; datetime; logging; typing
- Public API names: reconcile_objectify_jobs; run_objectify_reconciler

### `backend/shared/services/events/outbox_runtime.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=151 | code_lines=135 | risk_score=23
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=7 | raise=4 | broad_except=4 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/4 (25%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (7): __future__; asyncio; datetime; inspect; logging; os; typing
- Public API names: build_outbox_worker_id; flush_outbox_until_empty; maybe_purge_with_interval; run_outbox_poll_loop

### `backend/shared/services/kafka/__init__.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=0 | code_lines=0 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/services/kafka/consumer_ops.py`
- Module summary: Kafka consumer call strategies (Strategy pattern).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=153 | code_lines=115 | risk_score=0
- API surface: public=3 | top-level functions=0 | classes=3 | methods=22
- Runtime signals: async_functions=21 | try=2 | raise=7 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=3/3 (100%) | methods=1/22 (4%)
- Internal imports (2): shared.observability.tracing; shared.utils.executor_utils
- External imports (6): __future__; abc; concurrent; confluent_kafka; dataclasses; typing
- Public API names: ExecutorKafkaConsumerOps; InlineKafkaConsumerOps; KafkaConsumerOps

### `backend/shared/services/kafka/dlq_publisher.py`
- Module summary: Shared DLQ publisher for Kafka workers (Facade).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=390 | code_lines=341 | risk_score=15
- API surface: public=10 | top-level functions=9 | classes=3 | methods=3
- Runtime signals: async_functions=4 | try=5 | raise=0 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/9 (0%) | classes=0/3 (0%) | methods=0/3 (0%)
- Internal imports (4): shared.models.event_envelope; shared.observability.context_propagation; shared.observability.tracing; shared.utils.time_utils
- External imports (7): __future__; asyncio; contextlib; dataclasses; json; logging; typing
- Public API names: DlqPublishSpec; EnvelopeDlqSpec; build_envelope_dlq_event; build_standard_dlq_payload; default_dlq_span_attributes; default_envelope_dlq_span_attributes; publish_contextual_dlq_json; publish_dlq_json; publish_envelope_dlq; publish_standard_dlq

### `backend/shared/services/kafka/processed_event_worker.py`
- Module summary: Kafka worker runtime helpers (Template Method + Strategy).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=1574 | code_lines=1391 | risk_score=123
- API surface: public=10 | top-level functions=0 | classes=10 | methods=84
- Runtime signals: async_functions=36 | try=29 | raise=16 | broad_except=22 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=5/10 (50%) | methods=9/84 (10%)
- Internal imports (9): shared.models.event_envelope; shared.observability.context_propagation; shared.observability.tracing; shared.services.kafka.consumer_ops; shared.services.kafka.dlq_publisher; shared.services.kafka.safe_consumer; shared.services.kafka.worker_consumer_runtime; shared.services.registries.processed_event_heartbeat (+1 more)
- External imports (11): __future__; abc; asyncio; collections; confluent_kafka; contextlib; dataclasses; json (+3 more)
- Public API names: CommandParseError; EventEnvelopeKafkaWorker; HeartbeatOptions; ParseErrorContext; ProcessedEventKafkaWorker; RegistryKey; StrictHeartbeatEventEnvelopeKafkaWorker; StrictHeartbeatKafkaWorker; StrictHeartbeatPolicyMixin; WorkerRuntimeConfig

### `backend/shared/services/kafka/producer_factory.py`
- Module summary: Kafka producer factory (Factory Method).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=83 | code_lines=71 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (4): __future__; collections; confluent_kafka; typing
- Public API names: create_kafka_dlq_producer; create_kafka_producer

### `backend/shared/services/kafka/producer_ops.py`
- Module summary: Kafka producer call strategies (Strategy pattern).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=127 | code_lines=100 | risk_score=6
- API surface: public=4 | top-level functions=1 | classes=3 | methods=10
- Runtime signals: async_functions=10 | try=4 | raise=3 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=3/3 (100%) | methods=0/10 (0%)
- Internal imports (2): shared.observability.tracing; shared.utils.executor_utils
- External imports (7): __future__; abc; asyncio; concurrent; dataclasses; logging; typing
- Public API names: ExecutorKafkaProducerOps; InlineKafkaProducerOps; KafkaProducerOps; close_kafka_producer

### `backend/shared/services/kafka/retry_classifier.py`
- Module summary: Shared retry classification helpers for Kafka workers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=254 | code_lines=210 | risk_score=0
- API surface: public=7 | top-level functions=7 | classes=1 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/7 (14%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (1): shared.errors.error_types
- External imports (3): __future__; dataclasses; typing
- Public API names: RetryPolicyProfile; classify_retryable_by_error_code; classify_retryable_by_markers; classify_retryable_with_profile; contains_marker; create_retry_policy_profile; normalize_error_message

### `backend/shared/services/kafka/safe_consumer.py`
- Module summary: Safe Kafka Consumer with Strong Consistency Guarantees.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=568 | code_lines=453 | risk_score=33
- API surface: public=6 | top-level functions=2 | classes=4 | methods=22
- Runtime signals: async_functions=0 | try=7 | raise=4 | broad_except=6 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/2 (100%) | classes=4/4 (100%) | methods=19/22 (86%)
- Internal imports (2): shared.config.settings; shared.observability.tracing
- External imports (9): __future__; confluent_kafka; dataclasses; datetime; enum; logging; threading; time (+1 more)
- Public API names: ConsumerState; PartitionState; RebalanceHandler; SafeKafkaConsumer; create_safe_consumer; validate_consumer_config

### `backend/shared/services/kafka/worker_consumer_runtime.py`
- Module summary: Kafka worker consumer runtime helpers (Facade over Strategy).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=125 | code_lines=104 | risk_score=12
- API surface: public=1 | top-level functions=0 | classes=1 | methods=5
- Runtime signals: async_functions=5 | try=2 | raise=0 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/1 (0%) | methods=0/5 (0%)
- Internal imports (2): shared.observability.tracing; shared.services.kafka.consumer_ops
- External imports (5): __future__; confluent_kafka; dataclasses; logging; typing
- Public API names: WorkerConsumerRuntime

### `backend/shared/services/pipeline/__init__.py`
- Module summary: Package: $dir
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=8 | code_lines=6 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/services/pipeline/dataset_output_semantics.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=523 | code_lines=450 | risk_score=0
- API surface: public=7 | top-level functions=16 | classes=3 | methods=2
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/16 (0%) | classes=0/3 (0%) | methods=0/2 (0%)
- Internal imports (1): shared.services.pipeline.pipeline_definition_utils
- External imports (6): __future__; dataclasses; enum; hashlib; json; typing
- Public API names: DatasetWriteMode; NormalizedDatasetOutputMetadata; ResolvedDatasetWritePolicy; normalize_dataset_output_metadata; resolve_dataset_write_policy; validate_dataset_output_format_constraints; validate_dataset_output_metadata

### `backend/shared/services/pipeline/fk_pattern_detector.py`
- Module summary: Foreign Key Pattern Detector Service
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=437 | code_lines=324 | risk_score=0
- API surface: public=4 | top-level functions=0 | classes=4 | methods=14
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=4/4 (100%) | methods=13/14 (92%)
- Internal imports (0): not documented
- External imports (4): dataclasses; logging; re; typing
- Public API names: FKDetectionConfig; ForeignKeyPattern; ForeignKeyPatternDetector; TargetCandidate

### `backend/shared/services/pipeline/objectify_delta_utils.py`
- Module summary: Objectify Delta Utilities
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=386 | code_lines=300 | risk_score=6
- API surface: public=4 | top-level functions=1 | classes=3 | methods=13
- Runtime signals: async_functions=4 | try=2 | raise=1 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=3/3 (100%) | methods=9/13 (69%)
- Internal imports (0): not documented
- External imports (6): dataclasses; datetime; hashlib; json; logging; typing
- Public API names: DeltaResult; ObjectifyDeltaComputer; WatermarkState; create_delta_computer_for_mapping_spec

### `backend/shared/services/pipeline/output_plugins.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=278 | code_lines=223 | risk_score=0
- API surface: public=8 | top-level functions=7 | classes=9 | methods=10
- Runtime signals: async_functions=0 | try=1 | raise=4 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/7 (0%) | classes=0/9 (0%) | methods=0/10 (0%)
- Internal imports (1): shared.services.pipeline.dataset_output_semantics
- External imports (3): __future__; dataclasses; typing
- Public API names: OntologyOutputSemantics; OutputPlugin; ResolvedOutputKind; get_output_plugin; normalize_output_kind; resolve_ontology_output_semantics; resolve_output_kind; validate_output_payload

### `backend/shared/services/pipeline/pipeline_artifact_store.py`
- Module summary: Pipeline artifact store for S3/MinIO.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=39 | code_lines=32 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=2
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (2): shared.services.storage.storage_service; shared.utils.s3_uri
- External imports (3): __future__; datetime; typing
- Public API names: PipelineArtifactStore

### `backend/shared/services/pipeline/pipeline_claim_refuter.py`
- Module summary: Claim-based refuter for pipeline plans.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=1312 | code_lines=1137 | risk_score=9
- API surface: public=1 | top-level functions=24 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=4 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=5/24 (20%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (7): shared.models.pipeline_plan; shared.services.pipeline.pipeline_executor; shared.services.pipeline.pipeline_graph_utils; shared.services.pipeline.pipeline_transform_spec; shared.services.pipeline.pipeline_type_utils; shared.services.registries.dataset_registry; shared.utils.llm_safety
- External imports (5): __future__; datetime; logging; re; typing
- Public API names: refute_pipeline_plan_claims

### `backend/shared/services/pipeline/pipeline_control_plane_events.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=63 | code_lines=52 | risk_score=6
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): shared.config.app_config; shared.config.settings; shared.models.event_envelope; shared.services.storage.event_store; shared.utils.time_utils
- External imports (3): __future__; logging; typing
- Public API names: emit_pipeline_control_plane_event; pipeline_control_plane_events_enabled; sanitize_event_id

### `backend/shared/services/pipeline/pipeline_dataset_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=146 | code_lines=123 | risk_score=0
- API surface: public=6 | top-level functions=4 | classes=2 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/2 (0%) | methods=0/0 (n/a)
- Internal imports (1): shared.config.settings
- External imports (3): __future__; dataclasses; typing
- Public API names: DatasetResolution; DatasetSelection; build_branch_candidates; normalize_dataset_selection; resolve_dataset_version; resolve_fallback_branches

### `backend/shared/services/pipeline/pipeline_definition_augmentation.py`
- Module summary: Pipeline Builder definition augmentation helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=591 | code_lines=541 | risk_score=6
- API surface: public=2 | top-level functions=13 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/13 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (7): shared.services.pipeline.pipeline_dataset_utils; shared.services.pipeline.pipeline_definition_utils; shared.services.pipeline.pipeline_graph_utils; shared.services.pipeline.pipeline_schema_casts; shared.services.pipeline.pipeline_transform_spec; shared.services.registries.dataset_registry; shared.utils.time_utils
- External imports (3): __future__; logging; typing
- Public API names: augment_definition_with_canonical_contract; augment_definition_with_casts

### `backend/shared/services/pipeline/pipeline_definition_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=350 | code_lines=312 | risk_score=0
- API surface: public=15 | top-level functions=15 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/15 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline.pipeline_schema_utils
- External imports (2): __future__; typing
- Public API names: build_expectations_with_pk; coerce_pk_columns; collect_pk_columns; is_truthy; match_output_declaration; normalize_expectation_columns; normalize_pk_semantics; resolve_delete_column; resolve_execution_semantics; resolve_incremental_config; resolve_incremental_watermark_column; resolve_pk_columns (+3 more)

### `backend/shared/services/pipeline/pipeline_definition_validator.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=525 | code_lines=475 | risk_score=3
- API surface: public=4 | top-level functions=4 | classes=2 | methods=0
- Runtime signals: async_functions=0 | try=3 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/2 (0%) | methods=0/0 (n/a)
- Internal imports (5): shared.services.pipeline.output_plugins; shared.services.pipeline.pipeline_definition_utils; shared.services.pipeline.pipeline_graph_utils; shared.services.pipeline.pipeline_kafka_avro; shared.services.pipeline.pipeline_transform_spec
- External imports (3): __future__; dataclasses; typing
- Public API names: PipelineDefinitionValidationPolicy; PipelineDefinitionValidationResult; normalize_transform_metadata; validate_pipeline_definition

### `backend/shared/services/pipeline/pipeline_dependency_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=32 | code_lines=28 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=3 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: normalize_dependency_entries

### `backend/shared/services/pipeline/pipeline_executor.py`
- Module summary: Pipeline Executor - Minimal transform engine for Pipeline Builder.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=2463 | code_lines=2210 | risk_score=20
- API surface: public=5 | top-level functions=52 | classes=5 | methods=16
- Runtime signals: async_functions=9 | try=26 | raise=33 | broad_except=4 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=3/52 (5%) | classes=0/5 (0%) | methods=0/16 (0%)
- Internal imports (16): shared.config.settings; shared.services.pipeline.pipeline_dataset_utils; shared.services.pipeline.pipeline_definition_utils; shared.services.pipeline.pipeline_graph_utils; shared.services.pipeline.pipeline_join_keys; shared.services.pipeline.pipeline_parameter_utils; shared.services.pipeline.pipeline_profiler; shared.services.pipeline.pipeline_schema_utils (+8 more)
- External imports (14): __future__; ast; csv; dataclasses; datetime; difflib; io; json (+6 more)
- Public API names: PipelineArtifactStore; PipelineExecutor; PipelineExpectationError; PipelineRunResult; PipelineTable

### `backend/shared/services/pipeline/pipeline_funnel_fallback.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=367 | code_lines=314 | risk_score=0
- API surface: public=2 | top-level functions=10 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=2/10 (20%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.errors.external_codes; shared.services.pipeline.pipeline_type_utils
- External imports (4): __future__; datetime; re; typing
- Public API names: build_funnel_analysis_fallback; infer_type_fallback

### `backend/shared/services/pipeline/pipeline_graph_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=96 | code_lines=82 | risk_score=6
- API surface: public=5 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/5 (20%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; logging; typing
- Public API names: build_incoming; normalize_edges; normalize_nodes; topological_sort; unique_node_id

### `backend/shared/services/pipeline/pipeline_job_queue.py`
- Module summary: Pipeline job queue publisher using Kafka.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=102 | code_lines=83 | risk_score=5
- API surface: public=1 | top-level functions=0 | classes=1 | methods=3
- Runtime signals: async_functions=1 | try=1 | raise=4 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/1 (0%) | methods=0/3 (0%)
- Internal imports (5): shared.config.app_config; shared.config.settings; shared.models.pipeline_job; shared.observability.context_propagation; shared.services.kafka.producer_factory
- External imports (8): __future__; asyncio; confluent_kafka; json; logging; threading; time; typing
- Public API names: PipelineJobQueue

### `backend/shared/services/pipeline/pipeline_join_keys.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=13 | code_lines=10 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: normalize_join_key_list

### `backend/shared/services/pipeline/pipeline_kafka_avro.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=188 | code_lines=155 | risk_score=0
- API surface: public=6 | top-level functions=10 | classes=1 | methods=2
- Runtime signals: async_functions=0 | try=1 | raise=5 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/10 (0%) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (0): not documented
- External imports (5): __future__; dataclasses; httpx; typing; urllib
- Public API names: KafkaAvroSchemaRegistryReference; fetch_kafka_avro_schema_from_registry; parse_kafka_avro_schema_registry_response; resolve_inline_avro_schema; resolve_kafka_avro_schema_registry_reference; validate_kafka_avro_schema_config

### `backend/shared/services/pipeline/pipeline_lock.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=83 | code_lines=71 | risk_score=11
- API surface: public=2 | top-level functions=0 | classes=2 | methods=6
- Runtime signals: async_functions=4 | try=3 | raise=2 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/2 (0%) | methods=0/6 (0%)
- Internal imports (0): not documented
- External imports (4): __future__; asyncio; logging; typing
- Public API names: PipelineLock; PipelineLockError

### `backend/shared/services/pipeline/pipeline_math_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=7 | code_lines=5 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (1): __future__
- Public API names: safe_ratio

### `backend/shared/services/pipeline/pipeline_parameter_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=36 | code_lines=31 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: apply_parameters; normalize_parameters

### `backend/shared/services/pipeline/pipeline_plan_builder.py`
- Module summary: Deterministic pipeline plan builder helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=1858 | code_lines=1642 | risk_score=0
- API surface: public=41 | top-level functions=49 | classes=2 | methods=0
- Runtime signals: async_functions=0 | try=6 | raise=66 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=24/49 (48%) | classes=0/2 (0%) | methods=0/0 (n/a)
- Internal imports (4): shared.services.pipeline.dataset_output_semantics; shared.services.pipeline.output_plugins; shared.services.pipeline.pipeline_graph_utils; shared.services.pipeline.pipeline_transform_spec
- External imports (4): __future__; dataclasses; re; typing
- Public API names: PipelinePlanBuilderError; PlanMutation; add_cast; add_compute; add_compute_assignments; add_compute_column; add_dedupe; add_drop; add_edge; add_explode; add_external_input; add_filter (+29 more)

### `backend/shared/services/pipeline/pipeline_preflight_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=1827 | code_lines=1679 | risk_score=5
- API surface: public=3 | top-level functions=26 | classes=1 | methods=0
- Runtime signals: async_functions=2 | try=5 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=6/26 (23%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (10): shared.services.pipeline.dataset_output_semantics; shared.services.pipeline.output_plugins; shared.services.pipeline.pipeline_dataset_utils; shared.services.pipeline.pipeline_definition_utils; shared.services.pipeline.pipeline_graph_utils; shared.services.pipeline.pipeline_kafka_avro; shared.services.pipeline.pipeline_schema_utils; shared.services.pipeline.pipeline_transform_spec (+2 more)
- External imports (3): __future__; dataclasses; typing
- Public API names: SchemaInfo; compute_pipeline_preflight; compute_schema_by_node

### `backend/shared/services/pipeline/pipeline_preview_inspector.py`
- Module summary: Preview inspector for cleansing suggestions.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=458 | code_lines=389 | risk_score=0
- API surface: public=1 | top-level functions=14 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=3/14 (21%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.services.pipeline.pipeline_math_utils; shared.services.pipeline.pipeline_value_predicates
- External imports (3): __future__; re; typing
- Public API names: inspect_preview

### `backend/shared/services/pipeline/pipeline_preview_policy.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=437 | code_lines=379 | risk_score=12
- API surface: public=2 | top-level functions=8 | classes=1 | methods=1
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/8 (12%) | classes=0/1 (0%) | methods=0/1 (0%)
- Internal imports (2): shared.services.pipeline.pipeline_definition_utils; shared.services.pipeline.pipeline_transform_spec
- External imports (6): __future__; ast; dataclasses; logging; re; typing
- Public API names: PreviewPolicyIssue; evaluate_preview_policy

### `backend/shared/services/pipeline/pipeline_profiler.py`
- Module summary: Pipeline Profiler - lightweight column statistics for Pipeline Builder previews.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=178 | code_lines=150 | risk_score=7
- API surface: public=1 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/5 (20%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (4): __future__; collections; json; typing
- Public API names: compute_column_stats

### `backend/shared/services/pipeline/pipeline_scheduler.py`
- Module summary: Pipeline Scheduler - periodic triggering for pipeline jobs.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=565 | code_lines=526 | risk_score=29
- API surface: public=3 | top-level functions=8 | classes=3 | methods=6
- Runtime signals: async_functions=7 | try=15 | raise=1 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/8 (0%) | classes=0/3 (0%) | methods=0/6 (0%)
- Internal imports (8): shared.errors.error_envelope; shared.errors.error_types; shared.models.pipeline_job; shared.services.pipeline.pipeline_control_plane_events; shared.services.pipeline.pipeline_dependency_utils; shared.services.pipeline.pipeline_job_queue; shared.services.registries.pipeline_registry; shared.utils.time_utils
- External imports (7): __future__; asyncio; contextlib; dataclasses; datetime; logging; typing
- Public API names: DependencyEvaluation; PipelineScheduler; ScheduledPipeline

### `backend/shared/services/pipeline/pipeline_schema_casts.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=30 | code_lines=25 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline.pipeline_schema_utils
- External imports (2): __future__; typing
- Public API names: extract_schema_casts

### `backend/shared/services/pipeline/pipeline_schema_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=142 | code_lines=123 | risk_score=1
- API surface: public=9 | top-level functions=6 | classes=3 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/6 (0%) | classes=0/3 (0%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; dataclasses; typing
- Public API names: ExpectationSpec; SchemaCheckSpec; SchemaContractSpec; normalize_expectations; normalize_number; normalize_schema_checks; normalize_schema_contract; normalize_schema_type; normalize_value_list

### `backend/shared/services/pipeline/pipeline_task_spec_policy.py`
- Module summary: TaskSpec policy helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=122 | code_lines=98 | risk_score=6
- API surface: public=3 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/4 (50%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): shared.models.pipeline_plan; shared.models.pipeline_task_spec; shared.services.pipeline.pipeline_transform_spec
- External imports (3): __future__; logging; typing
- Public API names: clamp_task_spec; normalize_task_spec; validate_plan_against_task_spec

### `backend/shared/services/pipeline/pipeline_transform_spec.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=235 | code_lines=202 | risk_score=0
- API surface: public=10 | top-level functions=8 | classes=2 | methods=0
- Runtime signals: async_functions=0 | try=2 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/8 (0%) | classes=0/2 (0%) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline.pipeline_join_keys
- External imports (3): __future__; dataclasses; typing
- Public API names: JoinSpec; StreamJoinSpec; is_stream_like_input_node; normalize_operation; normalize_union_mode; resolve_input_read_format; resolve_input_read_mode; resolve_join_spec; resolve_stream_join_effective_join_type; resolve_stream_join_spec

### `backend/shared/services/pipeline/pipeline_type_inference.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=124 | code_lines=95 | risk_score=0
- API surface: public=4 | top-level functions=8 | classes=1 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=2/8 (25%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (2): shared.services.pipeline.pipeline_schema_utils; shared.services.pipeline.pipeline_value_predicates
- External imports (3): __future__; dataclasses; typing
- Public API names: TypeInferenceResult; common_join_key_type; infer_xsd_type_with_confidence; normalize_declared_type

### `backend/shared/services/pipeline/pipeline_type_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=420 | code_lines=354 | risk_score=4
- API surface: public=12 | top-level functions=11 | classes=2 | methods=0
- Runtime signals: async_functions=0 | try=4 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=4/11 (36%) | classes=2/2 (100%) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline.pipeline_schema_utils
- External imports (5): __future__; dataclasses; datetime; re; typing
- Public API names: DateParseResult; NumericParseResult; infer_xsd_type_from_values; normalize_cast_mode; normalize_cast_target; parse_datetime_text; parse_datetime_text_with_ambiguity; parse_decimal_text; parse_decimal_text_with_ambiguity; parse_int_text; spark_type_to_xsd; xsd_to_spark_type

### `backend/shared/services/pipeline/pipeline_udf_runtime.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=280 | code_lines=224 | risk_score=5
- API surface: public=9 | top-level functions=10 | classes=4 | methods=4
- Runtime signals: async_functions=3 | try=3 | raise=25 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/10 (10%) | classes=0/4 (0%) | methods=0/4 (0%)
- Internal imports (0): not documented
- External imports (5): __future__; ast; dataclasses; hashlib; typing
- Public API names: PipelineUdfError; PipelineUdfRegistry; ResolvedPipelineUdf; build_udf_cache_key; compile_row_udf; compile_udf; normalize_udf_output_rows; resolve_udf_reference; validate_udf_output_schema

### `backend/shared/services/pipeline/pipeline_unit_test_runner.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=172 | code_lines=140 | risk_score=15
- API surface: public=2 | top-level functions=7 | classes=1 | methods=0
- Runtime signals: async_functions=1 | try=3 | raise=4 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/7 (0%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline.pipeline_executor
- External imports (6): __future__; collections; dataclasses; json; logging; typing
- Public API names: PipelineUnitTestResult; run_unit_tests

### `backend/shared/services/pipeline/pipeline_validation_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=175 | code_lines=159 | risk_score=2
- API surface: public=4 | top-level functions=4 | classes=1 | methods=0
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (2): shared.services.pipeline.pipeline_definition_utils; shared.services.pipeline.pipeline_schema_utils
- External imports (4): __future__; dataclasses; re; typing
- Public API names: TableOps; validate_expectations; validate_schema_checks; validate_schema_contract

### `backend/shared/services/pipeline/pipeline_value_predicates.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=57 | code_lines=46 | risk_score=6
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline.pipeline_type_utils
- External imports (4): __future__; datetime; logging; typing
- Public API names: is_bool_like; is_datetime_like; is_decimal_like; is_int_like

### `backend/shared/services/registries/__init__.py`
- Module summary: Package: $dir
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=8 | code_lines=6 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/services/registries/action_log_registry.py`
- Module summary: Durable Action log registry (Postgres).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=660 | code_lines=607 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=4 | methods=16
- Runtime signals: async_functions=13 | try=3 | raise=8 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/4 (0%) | classes=1/4 (25%) | methods=3/16 (18%)
- Internal imports (2): shared.config.settings; shared.services.registries.postgres_schema_registry
- External imports (8): __future__; asyncpg; dataclasses; datetime; enum; json; typing; uuid
- Public API names: ActionDependencyRecord; ActionLogRecord; ActionLogRegistry; ActionLogStatus

### `backend/shared/services/registries/action_simulation_registry.py`
- Module summary: Action simulation registry (Postgres).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=408 | code_lines=380 | risk_score=0
- API surface: public=3 | top-level functions=0 | classes=3 | methods=15
- Runtime signals: async_functions=12 | try=0 | raise=8 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/3 (0%) | methods=0/15 (0%)
- Internal imports (2): shared.config.settings; shared.utils.json_utils
- External imports (5): __future__; asyncpg; dataclasses; datetime; typing
- Public API names: ActionSimulationRecord; ActionSimulationRegistry; ActionSimulationVersionRecord

### `backend/shared/services/registries/agent_function_registry.py`
- Module summary: Agent function registry (Postgres).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=206 | code_lines=192 | risk_score=0
- API surface: public=2 | top-level functions=0 | classes=2 | methods=5
- Runtime signals: async_functions=4 | try=0 | raise=5 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/2 (0%) | methods=0/5 (0%)
- Internal imports (2): shared.services.registries.postgres_schema_registry; shared.utils.json_utils
- External imports (5): __future__; asyncpg; dataclasses; datetime; typing
- Public API names: AgentFunctionRecord; AgentFunctionRegistry

### `backend/shared/services/registries/agent_model_registry.py`
- Module summary: Global agent model registry (Postgres).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=217 | code_lines=204 | risk_score=0
- API surface: public=2 | top-level functions=0 | classes=2 | methods=5
- Runtime signals: async_functions=4 | try=0 | raise=5 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/2 (0%) | methods=0/5 (0%)
- Internal imports (2): shared.services.registries.postgres_schema_registry; shared.utils.json_utils
- External imports (5): __future__; asyncpg; dataclasses; datetime; typing
- Public API names: AgentModelRecord; AgentModelRegistry

### `backend/shared/services/registries/agent_policy_registry.py`
- Module summary: Tenant-scoped agent policy registry (Postgres).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=146 | code_lines=132 | risk_score=0
- API surface: public=2 | top-level functions=0 | classes=2 | methods=5
- Runtime signals: async_functions=4 | try=0 | raise=3 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/2 (0%) | methods=0/5 (0%)
- Internal imports (2): shared.services.registries.postgres_schema_registry; shared.utils.json_utils
- External imports (5): __future__; asyncpg; dataclasses; datetime; typing
- Public API names: AgentPolicyRegistry; AgentTenantPolicyRecord

### `backend/shared/services/registries/agent_registry.py`
- Module summary: Agent registry for runs/steps/approvals (Postgres).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=929 | code_lines=882 | risk_score=0
- API surface: public=6 | top-level functions=0 | classes=6 | methods=22
- Runtime signals: async_functions=17 | try=0 | raise=16 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/6 (0%) | methods=1/22 (4%)
- Internal imports (2): shared.services.registries.postgres_schema_registry; shared.utils.json_utils
- External imports (5): __future__; asyncpg; dataclasses; datetime; typing
- Public API names: AgentApprovalRecord; AgentApprovalRequestRecord; AgentRegistry; AgentRunRecord; AgentStepRecord; AgentToolIdempotencyRecord

### `backend/shared/services/registries/agent_session_registry.py`
- Module summary: Agent session registry (Postgres).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=2315 | code_lines=2167 | risk_score=30
- API surface: public=11 | top-level functions=4 | classes=10 | methods=37
- Runtime signals: async_functions=29 | try=6 | raise=54 | broad_except=6 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/4 (0%) | classes=0/10 (0%) | methods=3/37 (8%)
- Internal imports (4): shared.config.settings; shared.security.data_encryption; shared.services.registries.postgres_schema_registry; shared.utils.json_utils
- External imports (6): __future__; asyncpg; dataclasses; datetime; logging; typing
- Public API names: AgentSessionCIResultRecord; AgentSessionContextItemRecord; AgentSessionEventRecord; AgentSessionJobRecord; AgentSessionLLMCallRecord; AgentSessionLLMUsageAggregateRecord; AgentSessionMessageRecord; AgentSessionRecord; AgentSessionRegistry; AgentSessionToolCallRecord; validate_session_status_transition

### `backend/shared/services/registries/agent_tool_registry.py`
- Module summary: Agent tool allowlist registry (Postgres).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=251 | code_lines=238 | risk_score=0
- API surface: public=2 | top-level functions=0 | classes=2 | methods=5
- Runtime signals: async_functions=4 | try=0 | raise=3 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/2 (0%) | methods=0/5 (0%)
- Internal imports (2): shared.services.registries.postgres_schema_registry; shared.utils.json_utils
- External imports (5): __future__; asyncpg; dataclasses; datetime; typing
- Public API names: AgentToolPolicyRecord; AgentToolRegistry

### `backend/shared/services/registries/backing_source_adapter.py`
- Module summary: Backing Source Adapter — OMS-first mapping spec resolution.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=319 | code_lines=269 | risk_score=18
- API surface: public=6 | top-level functions=5 | classes=2 | methods=3
- Runtime signals: async_functions=4 | try=3 | raise=0 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=5/5 (100%) | classes=2/2 (100%) | methods=2/3 (66%)
- Internal imports (0): not documented
- External imports (6): __future__; dataclasses; datetime; httpx; logging; typing
- Public API names: BackingSourceMappingSpec; MappingSpecResolver; extract_class_id_from_oms_spec_id; find_class_id_by_dataset; get_mapping_from_oms; is_oms_mapping_spec

### `backend/shared/services/registries/changelog_store.py`
- Module summary: Objectify Changelog Store.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=161 | code_lines=149 | risk_score=18
- API surface: public=1 | top-level functions=0 | classes=1 | methods=4
- Runtime signals: async_functions=3 | try=3 | raise=0 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=4/4 (100%)
- Internal imports (0): not documented
- External imports (5): __future__; json; logging; typing; uuid
- Public API names: ChangelogStore

### `backend/shared/services/registries/connector_registry.py`
- Module summary: Connector Registry - durable state in Postgres.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=767 | code_lines=699 | risk_score=0
- API surface: public=5 | top-level functions=1 | classes=5 | methods=15
- Runtime signals: async_functions=13 | try=0 | raise=19 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/5 (0%) | methods=1/15 (6%)
- Internal imports (5): shared.config.settings; shared.models.event_envelope; shared.services.registries.postgres_schema_registry; shared.utils.json_utils; shared.utils.time_utils
- External imports (6): __future__; asyncpg; dataclasses; datetime; typing; uuid
- Public API names: ConnectorMapping; ConnectorRegistry; ConnectorSource; OutboxItem; SyncState

### `backend/shared/services/registries/dataset_profile_registry.py`
- Module summary: Dataset profile registry (Postgres).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=194 | code_lines=177 | risk_score=0
- API surface: public=2 | top-level functions=0 | classes=2 | methods=5
- Runtime signals: async_functions=3 | try=0 | raise=3 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/2 (0%) | methods=0/5 (0%)
- Internal imports (4): shared.config.settings; shared.services.registries.postgres_schema_registry; shared.utils.canonical_json; shared.utils.json_utils
- External imports (6): __future__; asyncpg; dataclasses; datetime; typing; uuid
- Public API names: DatasetProfileRecord; DatasetProfileRegistry

### `backend/shared/services/registries/dataset_registry.py`
- Module summary: Dataset Registry - durable dataset metadata in Postgres.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=4392 | code_lines=4208 | risk_score=55
- API surface: public=17 | top-level functions=1 | classes=17 | methods=84
- Runtime signals: async_functions=72 | try=14 | raise=96 | broad_except=11 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=0/17 (0%) | methods=1/84 (1%)
- Internal imports (7): shared.config.settings; shared.observability.context_propagation; shared.services.registries.postgres_schema_registry; shared.utils.json_utils; shared.utils.s3_uri; shared.utils.schema_hash; shared.utils.time_utils
- External imports (7): __future__; asyncpg; dataclasses; datetime; logging; typing; uuid
- Public API names: AccessPolicyRecord; BackingDatasourceRecord; BackingDatasourceVersionRecord; DatasetIngestOutboxItem; DatasetIngestRequestRecord; DatasetIngestTransactionRecord; DatasetRecord; DatasetRegistry; DatasetVersionRecord; GatePolicyRecord; GateResultRecord; InstanceEditRecord (+5 more)

### `backend/shared/services/registries/lineage_store.py`
- Module summary: First-class lineage (provenance) store.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=1859 | code_lines=1689 | risk_score=0
- API surface: public=2 | top-level functions=1 | classes=1 | methods=35
- Runtime signals: async_functions=20 | try=8 | raise=13 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=1/1 (100%) | methods=12/35 (34%)
- Internal imports (7): shared.config.settings; shared.models.event_envelope; shared.models.lineage; shared.models.lineage_edge_types; shared.services.registries.postgres_schema_registry; shared.utils.ontology_version; shared.utils.sql_filter_builder
- External imports (7): __future__; asyncpg; datetime; json; logging; typing; uuid
- Public API names: LineageStore; create_lineage_store

### `backend/shared/services/registries/objectify_registry.py`
- Module summary: Objectify registry (mapping specs + objectify jobs) backed by Postgres.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=1507 | code_lines=1444 | risk_score=5
- API surface: public=5 | top-level functions=0 | classes=5 | methods=30
- Runtime signals: async_functions=24 | try=2 | raise=31 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/5 (20%) | methods=5/30 (16%)
- Internal imports (5): shared.config.settings; shared.models.objectify_job; shared.observability.context_propagation; shared.services.registries.postgres_schema_registry; shared.utils.json_utils
- External imports (7): __future__; asyncpg; dataclasses; datetime; logging; typing; uuid
- Public API names: OCCConflictError; ObjectifyJobRecord; ObjectifyOutboxItem; ObjectifyRegistry; OntologyMappingSpecRecord

### `backend/shared/services/registries/ontology_key_spec_registry.py`
- Module summary: Ontology KeySpec Registry (Postgres SSoT).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=255 | code_lines=230 | risk_score=6
- API surface: public=2 | top-level functions=1 | classes=2 | methods=9
- Runtime signals: async_functions=7 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=1/2 (50%) | methods=1/9 (11%)
- Internal imports (2): shared.config.settings; shared.utils.json_utils
- External imports (7): __future__; asyncpg; dataclasses; datetime; json; logging; typing
- Public API names: OntologyKeySpec; OntologyKeySpecRegistry

### `backend/shared/services/registries/pipeline_plan_registry.py`
- Module summary: Pipeline plan registry (Postgres).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=198 | code_lines=182 | risk_score=0
- API surface: public=2 | top-level functions=0 | classes=2 | methods=6
- Runtime signals: async_functions=4 | try=0 | raise=4 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/2 (0%) | methods=0/6 (0%)
- Internal imports (4): shared.config.settings; shared.services.registries.postgres_schema_registry; shared.utils.canonical_json; shared.utils.json_utils
- External imports (5): __future__; asyncpg; dataclasses; datetime; typing
- Public API names: PipelinePlanRecord; PipelinePlanRegistry

### `backend/shared/services/registries/pipeline_registry.py`
- Module summary: Pipeline Registry - durable pipeline metadata in Postgres.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=3253 | code_lines=3062 | risk_score=10
- API surface: public=11 | top-level functions=14 | classes=11 | methods=57
- Runtime signals: async_functions=54 | try=10 | raise=104 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/14 (0%) | classes=1/11 (9%) | methods=3/57 (5%)
- Internal imports (7): shared.config.settings; shared.services.registries.postgres_schema_registry; shared.services.storage.lakefs_client; shared.services.storage.lakefs_storage_service; shared.utils.json_utils; shared.utils.path_utils; shared.utils.time_utils
- External imports (10): __future__; asyncio; asyncpg; cryptography; dataclasses; datetime; json; logging (+2 more)
- Public API names: LakeFSCredentials; PipelineAlreadyExistsError; PipelineArtifactRecord; PipelineMergeNotSupportedError; PipelineOCCConflictError; PipelineRecord; PipelineRegistry; PipelineUdfRecord; PipelineUdfVersionRecord; PipelineVersionRecord; PromotionManifestRecord

### `backend/shared/services/registries/postgres_schema_registry.py`
- Module summary: Postgres registry base (Template Method).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=83 | code_lines=67 | risk_score=5
- API surface: public=1 | top-level functions=0 | classes=1 | methods=8
- Runtime signals: async_functions=7 | try=1 | raise=2 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/1 (0%) | methods=0/8 (0%)
- Internal imports (1): shared.config.settings
- External imports (5): __future__; abc; asyncpg; logging; typing
- Public API names: PostgresSchemaRegistry

### `backend/shared/services/registries/processed_event_heartbeat.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=62 | code_lines=49 | risk_score=10
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=2 | raise=2 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.config.settings; shared.services.registries.processed_event_registry
- External imports (4): __future__; asyncio; logging; typing
- Public API names: run_processed_event_heartbeat_loop

### `backend/shared/services/registries/processed_event_registry.py`
- Module summary: Durable processed-events registry (Postgres).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=621 | code_lines=554 | risk_score=0
- API surface: public=5 | top-level functions=2 | classes=3 | methods=12
- Runtime signals: async_functions=11 | try=0 | raise=15 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=1/3 (33%) | methods=3/12 (25%)
- Internal imports (1): shared.config.settings
- External imports (8): __future__; asyncpg; dataclasses; datetime; enum; os; typing; uuid
- Public API names: ClaimDecision; ClaimResult; ProcessedEventRegistry; validate_lease_settings; validate_registry_enabled

### `backend/shared/services/registries/processed_event_registry_factory.py`
- Module summary: ProcessedEventRegistry factory (Factory Method).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=37 | code_lines=29 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.registries.processed_event_registry
- External imports (2): __future__; typing
- Public API names: create_processed_event_registry

### `backend/shared/services/storage/__init__.py`
- Module summary: Package: $dir
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=8 | code_lines=6 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/services/storage/connectivity.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=20 | code_lines=14 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=2
- Runtime signals: async_functions=1 | try=1 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (1): shared.observability.tracing
- External imports (2): __future__; typing
- Public API names: AsyncClientPingMixin

### `backend/shared/services/storage/elasticsearch_service.py`
- Module summary: Elasticsearch Client Service
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=692 | code_lines=591 | risk_score=22
- API surface: public=3 | top-level functions=2 | classes=1 | methods=21
- Runtime signals: async_functions=20 | try=20 | raise=18 | broad_except=4 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/2 (100%) | classes=1/1 (100%) | methods=20/21 (95%)
- Internal imports (2): shared.observability.tracing; shared.services.storage.connectivity
- External imports (3): elasticsearch; logging; typing
- Public API names: ElasticsearchService; create_elasticsearch_service; promote_alias_to_index

### `backend/shared/services/storage/event_store.py`
- Module summary: 🔥 THINK ULTRA! Shared Event Store (S3/MinIO)
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=1253 | code_lines=1048 | risk_score=108
- API surface: public=2 | top-level functions=1 | classes=1 | methods=31
- Runtime signals: async_functions=21 | try=24 | raise=16 | broad_except=20 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=1/1 (100%) | methods=13/31 (41%)
- Internal imports (9): shared.config.settings; shared.errors.error_types; shared.errors.runtime_exception_policy; shared.models.event_envelope; shared.observability.context_propagation; shared.observability.tracing; shared.services.events.aggregate_sequence_allocator; shared.services.storage.s3_client_config (+1 more)
- External imports (9): aioboto3; asyncio; botocore; datetime; hashlib; json; logging; typing (+1 more)
- Public API names: EventStore; get_event_store

### `backend/shared/services/storage/lakefs_branch_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=23 | code_lines=18 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=1 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.observability.tracing; shared.services.storage.lakefs_client
- External imports (2): __future__; typing
- Public API names: ensure_lakefs_branch

### `backend/shared/services/storage/lakefs_client.py`
- Module summary: lakeFS client (REST) for repository/branch/commit/merge operations.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=330 | code_lines=296 | risk_score=5
- API surface: public=6 | top-level functions=2 | classes=6 | methods=9
- Runtime signals: async_functions=6 | try=1 | raise=40 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/2 (50%) | classes=1/6 (16%) | methods=0/9 (0%)
- Internal imports (2): shared.config.settings; shared.observability.tracing
- External imports (6): __future__; dataclasses; httpx; json; logging; typing
- Public API names: LakeFSAuthError; LakeFSClient; LakeFSConfig; LakeFSConflictError; LakeFSError; LakeFSNotFoundError

### `backend/shared/services/storage/lakefs_storage_service.py`
- Module summary: StorageService wrapper configured for lakeFS S3 Gateway.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=54 | code_lines=40 | risk_score=0
- API surface: public=2 | top-level functions=1 | classes=1 | methods=2
- Runtime signals: async_functions=2 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (3): shared.config.settings; shared.observability.tracing; shared.services.storage.storage_service
- External imports (2): __future__; typing
- Public API names: LakeFSStorageService; create_lakefs_storage_service

### `backend/shared/services/storage/redis_service.py`
- Module summary: Redis Client Service
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=472 | code_lines=389 | risk_score=22
- API surface: public=2 | top-level functions=1 | classes=1 | methods=24
- Runtime signals: async_functions=21 | try=6 | raise=4 | broad_except=4 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=1/1 (100%) | methods=23/24 (95%)
- Internal imports (2): shared.observability.tracing; shared.services.storage.connectivity
- External imports (6): asyncio; datetime; json; logging; redis; typing
- Public API names: RedisService; create_redis_service

### `backend/shared/services/storage/s3_client_config.py`
- Module summary: S3 client configuration helpers (shared).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=48 | code_lines=37 | risk_score=1
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; typing; urllib
- Public API names: build_s3_client_config

### `backend/shared/services/storage/storage_service.py`
- Module summary: Storage Service for S3/MinIO operations
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=937 | code_lines=786 | risk_score=10
- API surface: public=2 | top-level functions=1 | classes=1 | methods=22
- Runtime signals: async_functions=18 | try=15 | raise=17 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=1/1 (100%) | methods=22/22 (100%)
- Internal imports (3): shared.models.commands; shared.observability.tracing; shared.services.storage.s3_client_config
- External imports (6): asyncio; datetime; hashlib; json; logging; typing
- Public API names: StorageService; create_storage_service

### `backend/shared/setup.py`
- Module summary: Setup script for spice-shared package
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=94 | code_lines=63 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (1): setuptools
- Public API names: not documented

### `backend/shared/testing/__init__.py`
- Module summary: Shared Testing package.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=5 | code_lines=4 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/testing/config_fixtures.py`
- Module summary: Test Configuration Fixtures
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=397 | code_lines=312 | risk_score=17
- API surface: public=15 | top-level functions=12 | classes=3 | methods=12
- Runtime signals: async_functions=5 | try=4 | raise=2 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=12/12 (100%) | classes=3/3 (100%) | methods=7/12 (58%)
- Internal imports (7): shared.config.settings; shared.services.core.command_status_service; shared.services.storage.elasticsearch_service; shared.services.storage.redis_service; shared.services.storage.storage_service; shared.utils.jsonld; shared.utils.label_mapper
- External imports (7): asyncio; contextlib; logging; os; pytest; typing; unittest
- Public API names: ConfigOverride; MockServiceContainer; TestApplicationSettings; create_mock_elasticsearch_service; create_mock_jsonld_converter; create_mock_label_mapper; create_mock_redis_service; create_mock_storage_service; isolated_test_environment; mock_command_status_service; mock_container; setup_test_database_config (+3 more)

### `backend/shared/tools/__init__.py`
- Module summary: Utility tools for dev/ops workflows.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=1 | code_lines=1 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/tools/bff_admin_api.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=124 | code_lines=102 | risk_score=0
- API surface: public=5 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/5 (20%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (5): __future__; asyncio; httpx; time; typing
- Public API names: delete_database; extract_command_id; list_databases; normalize_base_url; wait_for_command

### `backend/shared/tools/error_taxonomy_audit.py`
- Module summary: Audit enterprise error taxonomy consistency across the codebase.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=1274 | code_lines=1156 | risk_score=9
- API surface: public=4 | top-level functions=30 | classes=3 | methods=0
- Runtime signals: async_functions=0 | try=10 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/30 (0%) | classes=0/3 (0%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (11): __future__; argparse; ast; collections; dataclasses; fnmatch; pathlib; re (+3 more)
- Public API names: CodeSpec; Mismatch; RawHttpCall; main

### `backend/shared/tools/foundry_functions_compat.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=185 | code_lines=142 | risk_score=0
- API surface: public=5 | top-level functions=7 | classes=1 | methods=1
- Runtime signals: async_functions=0 | try=1 | raise=11 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/7 (0%) | classes=0/1 (0%) | methods=0/1 (0%)
- Internal imports (0): not documented
- External imports (4): __future__; dataclasses; pathlib; typing
- Public API names: FunctionCompatibility; default_snapshot_path; filter_functions; load_default_foundry_functions_snapshot; load_foundry_functions_snapshot

### `backend/shared/utils/__init__.py`
- Module summary: Utility functions for SPICE HARVESTER services
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=15 | code_lines=13 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): .jsonld; .s3_uri
- External imports (0): not documented
- Public API names: not documented

### `backend/shared/utils/access_policy.py`
- Module summary: Access policy evaluation helpers (row/column masking).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=169 | code_lines=145 | risk_score=0
- API surface: public=1 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/5 (20%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.collection_utils
- External imports (2): __future__; typing
- Public API names: apply_access_policy

### `backend/shared/utils/action_audit_policy.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=150 | code_lines=122 | risk_score=1
- API surface: public=5 | top-level functions=8 | classes=2 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/8 (0%) | classes=0/2 (0%) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.canonical_json
- External imports (3): __future__; dataclasses; typing
- Public API names: ActionAuditPolicyError; NormalizedAuditPolicy; audit_action_log_input; audit_action_log_result; normalize_audit_policy

### `backend/shared/utils/action_data_access.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=283 | code_lines=249 | risk_score=12
- API surface: public=2 | top-level functions=8 | classes=2 | methods=1
- Runtime signals: async_functions=4 | try=2 | raise=0 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/8 (0%) | classes=0/2 (0%) | methods=0/1 (0%)
- Internal imports (3): shared.utils.access_policy; shared.utils.ontology_type_normalization; shared.utils.principal_policy
- External imports (4): __future__; dataclasses; logging; typing
- Public API names: ActionTargetDataAccessReport; evaluate_action_target_data_access

### `backend/shared/utils/action_input_schema.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=328 | code_lines=268 | risk_score=5
- API surface: public=4 | top-level functions=7 | classes=3 | methods=0
- Runtime signals: async_functions=0 | try=2 | raise=45 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=2/7 (28%) | classes=0/3 (0%) | methods=0/0 (n/a)
- Internal imports (1): shared.security.input_sanitizer
- External imports (5): __future__; dataclasses; json; logging; typing
- Public API names: ActionInputSchemaError; ActionInputValidationError; normalize_input_schema; validate_action_input

### `backend/shared/utils/action_permission_profile.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=83 | code_lines=66 | risk_score=0
- API surface: public=4 | top-level functions=3 | classes=2 | methods=3
- Runtime signals: async_functions=0 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/2 (0%) | methods=0/3 (0%)
- Internal imports (0): not documented
- External imports (3): __future__; dataclasses; typing
- Public API names: ActionPermissionProfile; ActionPermissionProfileError; requires_action_data_access_enforcement; resolve_action_permission_profile

### `backend/shared/utils/action_runtime_contracts.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=190 | code_lines=159 | risk_score=0
- API surface: public=6 | top-level functions=8 | classes=1 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/8 (0%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.ontology_type_normalization
- External imports (3): __future__; dataclasses; typing
- Public API names: ActionTargetRuntimeContract; build_property_type_map_from_properties; extract_interfaces_from_metadata; extract_required_action_interfaces; load_action_target_runtime_contract; strip_interface_prefix

### `backend/shared/utils/action_simulation_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=9 | code_lines=6 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: reject_simulation_delete_flag

### `backend/shared/utils/action_template_engine.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=1040 | code_lines=898 | risk_score=15
- API surface: public=8 | top-level functions=44 | classes=2 | methods=0
- Runtime signals: async_functions=0 | try=5 | raise=80 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=7/44 (15%) | classes=0/2 (0%) | methods=0/0 (n/a)
- Internal imports (2): shared.security.input_sanitizer; shared.utils.time_utils
- External imports (5): __future__; dataclasses; datetime; re; typing
- Public API names: ActionImplementationError; CompiledTarget; compile_action_change_shape; compile_action_implementation; compile_template_v1; compile_template_v1_change_shape; validate_action_implementation_definition; validate_template_v1_definition

### `backend/shared/utils/action_writeback.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=29 | code_lines=21 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.deterministic_ids
- External imports (3): __future__; typing; uuid
- Public API names: action_applied_event_id; is_noop_changes; safe_str

### `backend/shared/utils/app_logger.py`
- Module summary: 🔥 THINK ULTRA! Logging utilities for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=141 | code_lines=89 | risk_score=0
- API surface: public=5 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=5/6 (83%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.observability.logging
- External imports (3): logging; sys; typing
- Public API names: configure_logging; get_bff_logger; get_funnel_logger; get_logger; get_oms_logger

### `backend/shared/utils/async_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=27 | code_lines=18 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; inspect; typing
- Public API names: aclose_if_present; await_if_needed; raise_for_status_async; response_json_async

### `backend/shared/utils/backoff_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=32 | code_lines=27 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; datetime
- Public API names: exponential_backoff_seconds; next_exponential_backoff_at

### `backend/shared/utils/blank_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=14 | code_lines=9 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: is_blank_value; strip_to_none

### `backend/shared/utils/branch_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=29 | code_lines=21 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.config.settings
- External imports (2): __future__; typing
- Public API names: get_protected_branches; protected_branch_write_message

### `backend/shared/utils/canonical_json.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=46 | code_lines=34 | risk_score=0
- API surface: public=3 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/4 (25%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (5): __future__; datetime; hashlib; json; typing
- Public API names: canonical_json_dumps; sha256_canonical_json; sha256_canonical_json_prefixed

### `backend/shared/utils/chaos.py`
- Module summary: Chaos/fault injection helpers (test-only).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=79 | code_lines=61 | risk_score=3
- API surface: public=2 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=3 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/3 (33%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.config.settings
- External imports (6): __future__; logging; os; pathlib; re; typing
- Public API names: chaos_enabled; maybe_crash

### `backend/shared/utils/collection_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=11 | code_lines=8 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: ensure_list

### `backend/shared/utils/commit_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=17 | code_lines=14 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: coerce_commit_id

### `backend/shared/utils/deterministic_ids.py`
- Module summary: Deterministic ID helpers (uuid5).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=23 | code_lines=13 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; uuid
- Public API names: deterministic_uuid5; deterministic_uuid5_hex_prefix; deterministic_uuid5_str

### `backend/shared/utils/diff_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=92 | code_lines=81 | risk_score=6
- API surface: public=3 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (4): __future__; json; logging; typing
- Public API names: normalize_diff_changes; normalize_diff_response; summarize_diff_changes

### `backend/shared/utils/env_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=31 | code_lines=24 | risk_score=6
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (4): __future__; logging; os; typing
- Public API names: parse_bool; parse_bool_env; parse_int_env

### `backend/shared/utils/event_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=46 | code_lines=42 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.models.event_envelope; shared.utils.time_utils
- External imports (3): __future__; typing; uuid
- Public API names: build_command_event

### `backend/shared/utils/executor_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=20 | code_lines=15 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (5): __future__; asyncio; concurrent; functools; typing
- Public API names: call_in_executor

### `backend/shared/utils/foundry_page_token.py`
- Module summary: Foundry-style opaque page token helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=91 | code_lines=74 | risk_score=15
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=3 | raise=9 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/2 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (5): __future__; base64; json; time; typing
- Public API names: decode_offset_page_token; encode_offset_page_token

### `backend/shared/utils/id_generator.py`
- Module summary: ID Generator 유틸리티
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=395 | code_lines=272 | risk_score=24
- API surface: public=9 | top-level functions=13 | classes=1 | methods=0
- Runtime signals: async_functions=0 | try=4 | raise=0 | broad_except=4 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=13/13 (100%) | classes=1/1 (100%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (5): datetime; logging; re; typing; unicodedata
- Public API names: IDGenerationError; generate_class_id; generate_instance_id; generate_ontology_id; generate_property_id; generate_relationship_id; generate_simple_id; generate_unique_id; validate_generated_id

### `backend/shared/utils/import_type_normalization.py`
- Module summary: Import type normalization (domain-neutral).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=114 | code_lines=101 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/2 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: normalize_import_target_type; resolve_import_type

### `backend/shared/utils/json_patch.py`
- Module summary: Minimal JSON Patch (RFC6902-ish) applier for control-plane artifacts.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=117 | code_lines=98 | risk_score=0
- API surface: public=2 | top-level functions=4 | classes=1 | methods=0
- Runtime signals: async_functions=0 | try=5 | raise=15 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/4 (0%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; copy; typing
- Public API names: JsonPatchError; apply_json_patch

### `backend/shared/utils/json_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=245 | code_lines=226 | risk_score=8
- API surface: public=8 | top-level functions=8 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=9 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/8 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.errors.error_types; shared.errors.runtime_exception_policy
- External imports (5): __future__; datetime; json; logging; typing
- Public API names: coerce_json_dataset; coerce_json_dict; coerce_json_list; coerce_json_pipeline; coerce_json_strict; json_default; maybe_decode_json; normalize_json_payload

### `backend/shared/utils/jsonld.py`
- Module summary: JSON-LD utilities for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=252 | code_lines=172 | risk_score=6
- API surface: public=2 | top-level functions=1 | classes=1 | methods=9
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=1/1 (100%) | methods=9/9 (100%)
- Internal imports (0): not documented
- External imports (3): json; logging; typing
- Public API names: JSONToJSONLDConverter; get_default_converter

### `backend/shared/utils/key_spec.py`
- Module summary: Key spec normalization helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=128 | code_lines=115 | risk_score=0
- API surface: public=3 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: normalize_key_columns; normalize_key_spec; normalize_unique_keys

### `backend/shared/utils/label_mapper.py`
- Module summary: Label Mapper 유틸리티
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=1259 | code_lines=1021 | risk_score=46
- API surface: public=1 | top-level functions=0 | classes=1 | methods=33
- Runtime signals: async_functions=27 | try=11 | raise=10 | broad_except=9 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=33/33 (100%)
- Internal imports (2): shared.config.settings; shared.utils.language
- External imports (8): aiosqlite; asyncio; contextlib; datetime; logging; os; pathlib; typing
- Public API names: LabelMapper

### `backend/shared/utils/language.py`
- Module summary: Language utilities for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=366 | code_lines=266 | risk_score=12
- API surface: public=11 | top-level functions=12 | classes=1 | methods=6
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=12/12 (100%) | classes=1/1 (100%) | methods=6/6 (100%)
- Internal imports (0): not documented
- External imports (4): __future__; fastapi; logging; typing
- Public API names: MultilingualText; coerce_localized_text; detect_language_from_text; fallback_languages; get_accept_language; get_default_language; get_language_name; get_supported_languages; is_supported_language; normalize_language; select_localized_text

### `backend/shared/utils/llm_safety.py`
- Module summary: LLM safety utilities (domain-neutral).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=466 | code_lines=367 | risk_score=0
- API surface: public=12 | top-level functions=15 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=6/15 (40%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.errors.error_envelope; shared.errors.error_types
- External imports (5): __future__; hashlib; json; re; typing
- Public API names: build_agent_error_response; build_column_semantic_observations; build_relationship_observations; detect_value_pattern; digest_for_audit; extract_column_value_patterns; mask_pii; mask_pii_text; sample_items; sha256_hex; stable_json_dumps; truncate_text

### `backend/shared/utils/log_rotation.py`
- Module summary: 🔥 THINK ULTRA! Log Rotation Manager for Test Services
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=353 | code_lines=253 | risk_score=38
- API surface: public=2 | top-level functions=1 | classes=1 | methods=10
- Runtime signals: async_functions=0 | try=8 | raise=0 | broad_except=6 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=1/1 (100%) | methods=9/10 (90%)
- Internal imports (0): not documented
- External imports (9): datetime; glob; gzip; logging; os; pathlib; shutil; stat (+1 more)
- Public API names: LogRotationManager; create_default_rotation_manager

### `backend/shared/utils/number_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=14 | code_lines=11 | risk_score=6
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; logging; typing
- Public API names: to_int_or_none

### `backend/shared/utils/objectify_outputs.py`
- Module summary: Shared helpers for objectify artifact outputs.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=16 | code_lines=12 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: match_output_name

### `backend/shared/utils/ontology_type_normalization.py`
- Module summary: Ontology type normalization helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=88 | code_lines=72 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: normalize_ontology_base_type

### `backend/shared/utils/ontology_version.py`
- Module summary: Ontology (semantic contract) version helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=92 | code_lines=73 | risk_score=0
- API surface: public=5 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=3/5 (60%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; logging; typing
- Public API names: build_ontology_version; extract_ontology_version; normalize_ontology_version; resolve_ontology_version; split_ref_commit

### `backend/shared/utils/path_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=36 | code_lines=31 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (1): __future__
- Public API names: safe_lakefs_ref; safe_path_segment

### `backend/shared/utils/payload_utils.py`
- Module summary: Shared helpers for normalizing payload shapes.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=14 | code_lines=10 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: unwrap_data_payload

### `backend/shared/utils/principal_policy.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=80 | code_lines=62 | risk_score=0
- API surface: public=3 | top-level functions=2 | classes=1 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/2 (50%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: PrincipalPolicyError; build_principal_tags; policy_allows

### `backend/shared/utils/pythonpath_setup.py`
- Module summary: 🔥 THINK ULTRA!! Unified PYTHONPATH Configuration for Python Scripts
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=180 | code_lines=129 | risk_score=0
- API surface: public=5 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=5/5 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (4): os; pathlib; sys; typing
- Public API names: configure_python_environment; detect_backend_directory; ensure_backend_in_path; setup_pythonpath; validate_pythonpath

### `backend/shared/utils/repo_dotenv.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=38 | code_lines=30 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; pathlib; typing
- Public API names: load_repo_dotenv

### `backend/shared/utils/resource_rid.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=42 | code_lines=34 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/3 (33%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: format_resource_rid; parse_metadata_rev; strip_rid_revision

### `backend/shared/utils/s3_uri.py`
- Module summary: Helpers for working with s3://bucket/key style URIs.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=41 | code_lines=31 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; typing; urllib
- Public API names: build_s3_uri; is_s3_uri; normalize_s3_uri; parse_s3_uri

### `backend/shared/utils/safe_bool_expression.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=226 | code_lines=187 | risk_score=0
- API surface: public=5 | top-level functions=5 | classes=3 | methods=0
- Runtime signals: async_functions=0 | try=4 | raise=24 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/5 (20%) | classes=0/3 (0%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; ast; typing
- Public API names: BoolExpressionError; BoolExpressionEvaluationError; UnsafeBoolExpressionError; safe_eval_bool_expression; validate_bool_expression_syntax

### `backend/shared/utils/schema_columns.py`
- Module summary: Shared helpers for extracting schema column definitions.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=141 | code_lines=124 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/3 (33%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: extract_schema_column_names; extract_schema_columns; extract_schema_type_map

### `backend/shared/utils/schema_hash.py`
- Module summary: Shared helpers for computing stable schema hashes.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=38 | code_lines=28 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/3 (33%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.schema_columns
- External imports (4): __future__; hashlib; json; typing
- Public API names: compute_schema_hash; compute_schema_hash_from_payload; compute_schema_hash_from_sample

### `backend/shared/utils/schema_type_compatibility.py`
- Module summary: Schema type compatibility helpers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=26 | code_lines=19 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: is_type_compatible

### `backend/shared/utils/spice_event_ids.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=14 | code_lines=9 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.deterministic_ids
- External imports (1): __future__
- Public API names: spice_event_id

### `backend/shared/utils/sql_filter_builder.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=20 | code_lines=14 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=3
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/1 (0%) | methods=0/3 (0%)
- Internal imports (0): not documented
- External imports (3): __future__; dataclasses; typing
- Public API names: SqlFilterBuilder

### `backend/shared/utils/string_list_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=14 | code_lines=10 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: normalize_string_list

### `backend/shared/utils/submission_criteria_diagnostics.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=71 | code_lines=55 | risk_score=1
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; ast; typing
- Public API names: infer_submission_criteria_failure_reason

### `backend/shared/utils/time_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=7 | code_lines=4 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; datetime
- Public API names: utcnow

### `backend/shared/utils/token_count.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=33 | code_lines=28 | risk_score=6
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (4): __future__; json; logging; typing
- Public API names: approx_token_count; approx_token_count_json

### `backend/shared/utils/uuid_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=17 | code_lines=13 | risk_score=6
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (4): __future__; logging; typing; uuid
- Public API names: safe_uuid

### `backend/shared/utils/worker_runner.py`
- Module summary: Async worker entrypoint runner (Template Method).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=114 | code_lines=98 | risk_score=12
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=4 | raise=2 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (7): __future__; asyncio; contextlib; inspect; logging; signal; typing
- Public API names: run_component_lifecycle; run_worker_until_stopped

### `backend/shared/utils/writeback_conflicts.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=232 | code_lines=193 | risk_score=0
- API surface: public=9 | top-level functions=10 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=7 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=4/10 (40%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.canonical_json
- External imports (2): __future__; typing
- Public API names: compute_base_token; compute_observed_base; detect_overlap_fields; detect_overlap_links; extract_action_targets; normalize_changes; normalize_conflict_policy; parse_conflict_policy; resolve_applied_changes

### `backend/shared/utils/writeback_governance.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=38 | code_lines=31 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: extract_backing_dataset_id; format_acl_alignment_result; policies_aligned

### `backend/shared/utils/writeback_lifecycle.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=71 | code_lines=56 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=2/2 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: derive_lifecycle_id; overlay_doc_id

### `backend/shared/utils/writeback_patch_apply.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=77 | code_lines=63 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/1 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): __future__; typing
- Public API names: apply_changes_to_payload

### `backend/shared/utils/writeback_paths.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=73 | code_lines=55 | risk_score=0
- API surface: public=9 | top-level functions=9 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/9 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (1): __future__
- Public API names: queue_compaction_marker_key; queue_entry_key; queue_entry_prefix; ref_key; snapshot_latest_pointer_key; snapshot_manifest_key; snapshot_object_key; writeback_patchset_key; writeback_patchset_metadata_key

### `backend/shared/validators/__init__.py`
- Module summary: Validators for SPICE HARVESTER services
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=141 | code_lines=123 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=3/3 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (24): .address_validator; .array_validator; .base_validator; .cipher_validator; .complex_type_validator; .coordinate_validator; .email_validator; .enum_validator (+16 more)
- External imports (1): typing
- Public API names: get_composite_validator; get_validator; register_validator

### `backend/shared/validators/address_validator.py`
- Module summary: Address validator for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=218 | code_lines=176 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=11
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=11/11 (100%)
- Internal imports (2): ..models.common; .base_validator
- External imports (2): re; typing
- Public API names: AddressValidator

### `backend/shared/validators/array_validator.py`
- Module summary: Array type validator for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=136 | code_lines=105 | risk_score=2
- API surface: public=1 | top-level functions=0 | classes=1 | methods=5
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=5/5 (100%)
- Internal imports (3): ..models.common; .base_validator; .constraint_validator
- External imports (3): json; logging; typing
- Public API names: ArrayValidator

### `backend/shared/validators/base_validator.py`
- Module summary: Base validator interface for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=134 | code_lines=103 | risk_score=0
- API surface: public=3 | top-level functions=0 | classes=3 | methods=12
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=3/3 (100%) | methods=11/12 (91%)
- Internal imports (0): not documented
- External imports (3): abc; dataclasses; typing
- Public API names: BaseValidator; CompositeValidator; ValidationResult

### `backend/shared/validators/cipher_validator.py`
- Module summary: Cipher text validator for SPICE HARVESTER.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=35 | code_lines=24 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=3
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=0/3 (0%)
- Internal imports (2): .base_validator; .constraint_validator
- External imports (2): __future__; typing
- Public API names: CipherValidator

### `backend/shared/validators/complex_type_validator.py`
- Module summary: Refactored Complex Type Validator
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=402 | code_lines=344 | risk_score=8
- API surface: public=2 | top-level functions=0 | classes=2 | methods=25
- Runtime signals: async_functions=0 | try=3 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=2/2 (100%) | methods=25/25 (100%)
- Internal imports (1): ..models.common
- External imports (3): dataclasses; logging; typing
- Public API names: ComplexTypeConstraints; ComplexTypeValidator

### `backend/shared/validators/constraint_validator.py`
- Module summary: Constraint validator for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=334 | code_lines=261 | risk_score=7
- API surface: public=1 | top-level functions=0 | classes=1 | methods=9
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=9/9 (100%)
- Internal imports (1): .base_validator
- External imports (4): decimal; logging; re; typing
- Public API names: ConstraintValidator

### `backend/shared/validators/coordinate_validator.py`
- Module summary: Coordinate validator for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=139 | code_lines=108 | risk_score=3
- API surface: public=1 | top-level functions=0 | classes=1 | methods=4
- Runtime signals: async_functions=0 | try=3 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=4/4 (100%)
- Internal imports (2): ..models.common; .base_validator
- External imports (1): typing
- Public API names: CoordinateValidator

### `backend/shared/validators/email_validator.py`
- Module summary: Email validator for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=87 | code_lines=61 | risk_score=1
- API surface: public=1 | top-level functions=0 | classes=1 | methods=4
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=4/4 (100%)
- Internal imports (2): ..models.common; .base_validator
- External imports (3): email_validator; re; typing
- Public API names: EmailValidator

### `backend/shared/validators/enum_validator.py`
- Module summary: Enum validator for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=63 | code_lines=47 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=4
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=4/4 (100%)
- Internal imports (2): ..models.common; .base_validator
- External imports (1): typing
- Public API names: EnumValidator

### `backend/shared/validators/file_validator.py`
- Module summary: File validator for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=198 | code_lines=155 | risk_score=1
- API surface: public=1 | top-level functions=0 | classes=1 | methods=5
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=5/5 (100%)
- Internal imports (2): ..models.common; .base_validator
- External imports (3): logging; typing; urllib
- Public API names: FileValidator

### `backend/shared/validators/geopoint_validator.py`
- Module summary: GeoPoint validator for SPICE HARVESTER.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=79 | code_lines=66 | risk_score=12
- API surface: public=1 | top-level functions=0 | classes=1 | methods=3
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=0/3 (0%)
- Internal imports (1): .base_validator
- External imports (4): __future__; logging; re; typing
- Public API names: GeoPointValidator

### `backend/shared/validators/geoshape_validator.py`
- Module summary: GeoShape (GeoJSON) validator for SPICE HARVESTER.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=86 | code_lines=68 | risk_score=6
- API surface: public=1 | top-level functions=1 | classes=1 | methods=3
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/1 (0%) | classes=1/1 (100%) | methods=0/3 (0%)
- Internal imports (2): .base_validator; shared.utils.json_utils
- External imports (4): __future__; json; logging; typing
- Public API names: GeoShapeValidator

### `backend/shared/validators/google_sheets_validator.py`
- Module summary: Google Sheets URL validator for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=108 | code_lines=75 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=3
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=3/3 (100%)
- Internal imports (1): .base_validator
- External imports (2): re; typing
- Public API names: GoogleSheetsValidator

### `backend/shared/validators/image_validator.py`
- Module summary: Image URL validator for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=154 | code_lines=113 | risk_score=1
- API surface: public=1 | top-level functions=0 | classes=1 | methods=4
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=4/4 (100%)
- Internal imports (2): ..models.common; .base_validator
- External imports (2): typing; urllib
- Public API names: ImageValidator

### `backend/shared/validators/ip_validator.py`
- Module summary: IP address validator for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=136 | code_lines=98 | risk_score=4
- API surface: public=1 | top-level functions=0 | classes=1 | methods=3
- Runtime signals: async_functions=0 | try=4 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=3/3 (100%)
- Internal imports (1): .base_validator
- External imports (3): ipaddress; logging; typing
- Public API names: IpValidator

### `backend/shared/validators/marking_validator.py`
- Module summary: Marking validator for SPICE HARVESTER.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=35 | code_lines=24 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=3
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=0/3 (0%)
- Internal imports (2): .base_validator; .constraint_validator
- External imports (2): __future__; typing
- Public API names: MarkingValidator

### `backend/shared/validators/money_validator.py`
- Module summary: Money validator for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=353 | code_lines=270 | risk_score=2
- API surface: public=1 | top-level functions=0 | classes=1 | methods=6
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=6/6 (100%)
- Internal imports (3): ..models.common; .base_validator; .constraint_validator
- External imports (4): decimal; logging; re; typing
- Public API names: MoneyValidator

### `backend/shared/validators/name_validator.py`
- Module summary: Name validator for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=210 | code_lines=162 | risk_score=2
- API surface: public=2 | top-level functions=0 | classes=2 | methods=5
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=2/2 (100%) | methods=5/5 (100%)
- Internal imports (1): .base_validator
- External imports (3): enum; re; typing
- Public API names: NameValidator; NamingConvention

### `backend/shared/validators/object_validator.py`
- Module summary: Object/JSON validator for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=145 | code_lines=117 | risk_score=2
- API surface: public=1 | top-level functions=0 | classes=1 | methods=4
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=4/4 (100%)
- Internal imports (2): ..models.common; .base_validator
- External imports (3): json; logging; typing
- Public API names: ObjectValidator

### `backend/shared/validators/phone_validator.py`
- Module summary: Phone number validator for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=119 | code_lines=91 | risk_score=7
- API surface: public=1 | top-level functions=0 | classes=1 | methods=4
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=4/4 (100%)
- Internal imports (2): ..models.common; .base_validator
- External imports (4): logging; phonenumbers; re; typing
- Public API names: PhoneValidator

### `backend/shared/validators/string_validator.py`
- Module summary: String validator for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=109 | code_lines=78 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=3
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=3/3 (100%)
- Internal imports (1): .base_validator
- External imports (1): typing
- Public API names: StringValidator

### `backend/shared/validators/struct_validator.py`
- Module summary: Struct validator for SPICE HARVESTER.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=51 | code_lines=38 | risk_score=0
- API surface: public=1 | top-level functions=0 | classes=1 | methods=3
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=0/3 (0%)
- Internal imports (2): .base_validator; .object_validator
- External imports (2): __future__; typing
- Public API names: StructValidator

### `backend/shared/validators/url_validator.py`
- Module summary: URL validator for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=115 | code_lines=82 | risk_score=1
- API surface: public=1 | top-level functions=0 | classes=1 | methods=3
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=3/3 (100%)
- Internal imports (1): .base_validator
- External imports (4): logging; re; typing; urllib
- Public API names: UrlValidator

### `backend/shared/validators/uuid_validator.py`
- Module summary: UUID validator for SPICE HARVESTER
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=127 | code_lines=92 | risk_score=3
- API surface: public=1 | top-level functions=0 | classes=1 | methods=3
- Runtime signals: async_functions=0 | try=3 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=3/3 (100%)
- Internal imports (1): .base_validator
- External imports (4): logging; re; typing; uuid
- Public API names: UuidValidator

### `backend/shared/validators/vector_validator.py`
- Module summary: Vector validator for SPICE HARVESTER.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=74 | code_lines=60 | risk_score=12
- API surface: public=1 | top-level functions=0 | classes=1 | methods=3
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/1 (100%) | methods=0/3 (0%)
- Internal imports (3): .base_validator; .constraint_validator; shared.utils.json_utils
- External imports (4): __future__; json; logging; typing
- Public API names: VectorValidator

## tests

### `backend/tests/__init__.py`
- Module summary: SPICE HARVESTER test package
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=3 | code_lines=3 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/tests/chaos_lite.py`
- Module summary: Chaos-lite integration validation (no mocks).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=911 | code_lines=715 | risk_score=15
- API surface: public=8 | top-level functions=37 | classes=1 | methods=0
- Runtime signals: async_functions=0 | try=7 | raise=27 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/37 (2%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (12): __future__; argparse; dataclasses; json; os; pathlib; random; subprocess (+4 more)
- Public API names: Endpoints; main; scenario_es_down_then_recover; scenario_instance_worker_crash_after_claim; scenario_kafka_down_then_recover; scenario_out_of_order_delivery; scenario_redis_down_then_recover; scenario_soak_random_failures

### `backend/tests/conftest.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=80 | code_lines=60 | risk_score=0
- API surface: public=1 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=2/4 (50%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.repo_dotenv
- External imports (4): __future__; os; pathlib; sys
- Public API names: pytest_configure

### `backend/tests/connectors/__init__.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=0 | code_lines=0 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/tests/integration/test_pipeline_branch_lifecycle.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=53 | code_lines=39 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.config.service_config; shared.services.registries.pipeline_registry
- External imports (3): __future__; pytest; uuid
- Public API names: test_pipeline_branch_lifecycle

### `backend/tests/test_access_policy_link_indexing_e2e.py`
- Module summary: AccessPolicy + LinkIndexing E2E coverage (no mocks).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=980 | code_lines=881 | risk_score=5
- API surface: public=3 | top-level functions=26 | classes=0 | methods=0
- Runtime signals: async_functions=22 | try=6 | raise=14 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/26 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (6): shared.config.service_config; shared.config.settings; shared.services.registries.dataset_registry; shared.services.storage.lakefs_storage_service; shared.utils.s3_uri; tests.utils.auth
- External imports (12): __future__; aiohttp; asyncio; asyncpg; csv; io; os; pytest (+4 more)
- Public API names: test_access_policy_filters_and_masks_query_results; test_link_indexing_updates_relationships_and_status; test_object_type_migration_requires_edit_reset

### `backend/tests/test_action_writeback_e2e_smoke.py`
- Module summary: E2E smoke test: BFF → OMS → EventStore → action-worker → lakeFS → ES overlay.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=1794 | code_lines=1539 | risk_score=11
- API surface: public=3 | top-level functions=34 | classes=0 | methods=0
- Runtime signals: async_functions=27 | try=16 | raise=15 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=4/34 (11%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (5): shared.config.search_config; shared.services.storage.event_store; shared.utils.repo_dotenv; shared.utils.writeback_lifecycle; tests.utils.auth
- External imports (13): __future__; aiohttp; asyncio; contextlib; json; os; pathlib; platform (+5 more)
- Public API names: test_action_batch_dependency_undo_e2e; test_action_writeback_e2e_smoke; test_action_writeback_e2e_verification_suite

### `backend/tests/test_auth_hardening_e2e.py`
- Module summary: Auth hardening E2E tests (no mocks).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=81 | code_lines=64 | risk_score=0
- API surface: public=2 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): tests.utils.auth
- External imports (7): __future__; aiohttp; os; pytest; subprocess; sys; typing
- Public API names: test_auth_disabled_requires_explicit_allow; test_oms_write_requires_auth

### `backend/tests/test_command_status_ttl_e2e.py`
- Module summary: Command status TTL E2E tests (no mocks).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=94 | code_lines=82 | risk_score=2
- API surface: public=1 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=2 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): shared.config.app_config; shared.services.core.command_status_service; shared.services.storage.redis_service
- External imports (8): __future__; asyncio; contextlib; os; pytest; typing; urllib; uuid
- Public API names: test_command_status_ttl_configurable

### `backend/tests/test_consistency_e2e_smoke.py`
- Module summary: E2E Smoke Test for Strong Consistency Fixes.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=1382 | code_lines=1080 | risk_score=74
- API surface: public=14 | top-level functions=4 | classes=14 | methods=50
- Runtime signals: async_functions=19 | try=25 | raise=1 | broad_except=10 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=3/4 (75%) | classes=14/14 (100%) | methods=41/50 (82%)
- Internal imports (1): shared.config.settings
- External imports (12): __future__; aiohttp; ast; asyncio; confluent_kafka; contextlib; os; pytest (+4 more)
- Public API names: TestBFFHealth; TestConsistencySummary; TestDatabaseConnectivity; TestEndToEndFlowSimulation; TestInfraConnectivity; TestKafkaConnectivity; TestKafkaProducerConfiguration; TestMSAServiceHealth; TestOutboxPatternVerification; TestPipelineJobQueue; TestProcessedEventRegistryIdempotency; TestSafeKafkaConsumerConfiguration (+2 more)

### `backend/tests/test_core_functionality.py`
- Module summary: 🔥 SPICE HARVESTER CORE FUNCTIONALITY TESTS
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=904 | code_lines=763 | risk_score=15
- API surface: public=5 | top-level functions=7 | classes=5 | methods=15
- Runtime signals: async_functions=18 | try=7 | raise=14 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=3/7 (42%) | classes=5/5 (100%) | methods=15/15 (100%)
- Internal imports (2): shared.config.search_config; tests.utils.auth
- External imports (9): aiohttp; asyncio; datetime; json; os; pytest; time; typing (+1 more)
- Public API names: TestBFFGraphFederation; TestComplexTypes; TestCoreOntologyManagement; TestEventSourcingInfrastructure; TestHealthEndpoints

### `backend/tests/test_critical_fixes_e2e.py`
- Module summary: Critical fixes E2E validation (no mocks).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=297 | code_lines=244 | risk_score=5
- API surface: public=7 | top-level functions=16 | classes=0 | methods=0
- Runtime signals: async_functions=11 | try=3 | raise=7 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/16 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): tests.utils.auth
- External imports (10): __future__; aiohttp; asyncio; contextlib; os; pytest; subprocess; time (+2 more)
- Public API names: test_bff_sensitive_get_requires_auth; test_command_status_dual_outage_returns_503; test_config_monitor_current_returns_payload; test_i18n_translates_health_description; test_openapi_excludes_wip_projections; test_rate_limit_headers_present_on_success; test_redis_down_rate_limit_and_command_status_fallback

### `backend/tests/test_event_store_tls_guard.py`
- Module summary: Event store TLS guard tests (no mocks).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=42 | code_lines=34 | risk_score=1
- API surface: public=1 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.storage.event_store
- External imports (4): __future__; contextlib; os; pytest
- Public API names: test_event_store_tls_requirement

### `backend/tests/test_financial_investigation_workflow_e2e.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=609 | code_lines=549 | risk_score=10
- API surface: public=1 | top-level functions=9 | classes=0 | methods=0
- Runtime signals: async_functions=7 | try=4 | raise=7 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/9 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): shared.config.search_config; shared.security.database_access; tests.utils.auth
- External imports (9): __future__; asyncio; httpx; io; os; pytest; time; typing (+1 more)
- Public API names: test_financial_investigation_workflow_e2e

### `backend/tests/test_idempotency_chaos.py`
- Module summary: Chaos tests for the idempotency contract.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=443 | code_lines=364 | risk_score=11
- API surface: public=7 | top-level functions=11 | classes=1 | methods=7
- Runtime signals: async_functions=14 | try=7 | raise=1 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/11 (0%) | classes=0/1 (0%) | methods=0/7 (0%)
- Internal imports (1): shared.services.registries.processed_event_registry
- External imports (7): __future__; asyncio; contextlib; os; pytest; typing; uuid
- Public API names: test_command_status_endpoint_exposes_failure_reason; test_event_store_rejects_event_id_reuse_with_different_command_payload; test_registry_concurrent_claim_has_single_winner; test_registry_duplicate_delivery_causes_one_side_effect; test_registry_mark_failed_owner_mismatch_raises; test_registry_reclaims_stuck_processing_after_lease_timeout; test_registry_sequence_guard_is_monotonic

### `backend/tests/test_oms_smoke.py`
- Module summary: OMS module smoke tests (no direct infra credentials required).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=377 | code_lines=321 | risk_score=15
- API surface: public=1 | top-level functions=7 | classes=0 | methods=0
- Runtime signals: async_functions=7 | try=4 | raise=6 | broad_except=3 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/7 (14%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): oms.services.event_store; tests.utils.auth
- External imports (8): __future__; aiohttp; asyncio; os; pytest; time; typing; uuid
- Public API names: test_oms_end_to_end_smoke

### `backend/tests/test_openapi_contract_smoke.py`
- Module summary: OpenAPI-driven, no-mock contract smoke test for BFF.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=2746 | code_lines=2329 | risk_score=60
- API surface: public=4 | top-level functions=24 | classes=3 | methods=3
- Runtime signals: async_functions=9 | try=17 | raise=49 | broad_except=12 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=6/24 (25%) | classes=0/3 (0%) | methods=0/3 (0%)
- Internal imports (1): tests.utils.auth
- External imports (14): __future__; aiohttp; asyncio; dataclasses; datetime; io; jose; json (+6 more)
- Public API names: Operation; RequestPlan; SmokeContext; test_openapi_stable_contract_smoke

### `backend/tests/test_pipeline_execution_semantics_e2e.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=2262 | code_lines=2062 | risk_score=10
- API surface: public=14 | top-level functions=28 | classes=0 | methods=0
- Runtime signals: async_functions=23 | try=13 | raise=14 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=15/28 (53%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (4): shared.models.pipeline_job; shared.services.pipeline.pipeline_job_queue; shared.services.storage.lakefs_client; shared.utils.path_utils
- External imports (9): __future__; asyncio; httpx; json; os; pytest; time; typing (+1 more)
- Public API names: test_composite_pk_unique_perf; test_executor_vs_worker_validation_consistency; test_incremental_appends_outputs_and_preserves_previous_parts; test_incremental_empty_diff_noop; test_incremental_removed_files_noop; test_incremental_small_files_compaction_metrics; test_incremental_watermark_boundary_includes_equal_timestamp_rows; test_partition_column_special_chars_roundtrip; test_pk_semantics_append_log_allows_duplicate_ids; test_pk_semantics_append_state_blocks_duplicate_ids; test_pk_semantics_remove_requires_delete_column; test_run_branch_conflict_fallback_and_cleanup (+2 more)

### `backend/tests/test_pipeline_objectify_es_e2e.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=588 | code_lines=539 | risk_score=10
- API surface: public=1 | top-level functions=12 | classes=0 | methods=0
- Runtime signals: async_functions=10 | try=11 | raise=12 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/12 (8%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): shared.config.search_config; shared.services.registries.dataset_registry; shared.utils.s3_uri
- External imports (8): __future__; asyncio; httpx; os; pytest; time; typing; uuid
- Public API names: test_pipeline_objectify_es_projection

### `backend/tests/test_pipeline_streaming_semantics_e2e.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=409 | code_lines=371 | risk_score=0
- API surface: public=2 | top-level functions=8 | classes=0 | methods=0
- Runtime signals: async_functions=6 | try=5 | raise=5 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=2/8 (25%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.registries.dataset_registry
- External imports (8): __future__; asyncio; httpx; os; pytest; time; typing; uuid
- Public API names: test_streaming_build_deploy_promotes_all_outputs; test_streaming_build_fails_as_job_group_on_contract_mismatch

### `backend/tests/test_pipeline_transform_cleansing_e2e.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=697 | code_lines=646 | risk_score=0
- API surface: public=1 | top-level functions=11 | classes=0 | methods=0
- Runtime signals: async_functions=7 | try=7 | raise=10 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/11 (9%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.s3_uri
- External imports (9): __future__; asyncio; httpx; json; os; pytest; time; typing (+1 more)
- Public API names: test_pipeline_transform_cleansing_and_validation_e2e

### `backend/tests/test_pipeline_type_mismatch_guard_e2e.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=267 | code_lines=240 | risk_score=0
- API surface: public=1 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=5 | try=3 | raise=5 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/6 (16%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (8): __future__; asyncio; httpx; os; pytest; time; typing; uuid
- Public API names: test_preview_rejects_type_mismatch_in_compute_expression

### `backend/tests/test_sequence_allocator.py`
- Module summary: Integration tests for the Postgres-backed write-side sequence allocator.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=185 | code_lines=139 | risk_score=9
- API surface: public=4 | top-level functions=8 | classes=0 | methods=0
- Runtime signals: async_functions=7 | try=5 | raise=1 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/8 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.events.aggregate_sequence_allocator
- External imports (5): __future__; asyncio; os; pytest; uuid
- Public API names: test_allocator_catches_up_when_seed_is_ahead_of_db_state; test_allocator_concurrent_reservation_is_unique_and_monotonic; test_allocator_occ_reserves_only_when_expected_matches; test_allocator_seeding_starts_after_existing_stream_max

### `backend/tests/test_websocket_auth_e2e.py`
- Module summary: WebSocket auth E2E tests (no mocks).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=64 | code_lines=49 | risk_score=1
- API surface: public=2 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): tests.utils.auth
- External imports (6): __future__; aiohttp; json; os; pytest; uuid
- Public API names: test_ws_allows_token; test_ws_requires_token

### `backend/tests/test_worker_lease_safety_e2e.py`
- Module summary: Worker lease safety tests (no mocks).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=91 | code_lines=70 | risk_score=1
- API surface: public=3 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.services.kafka.consumer_ops; shared.services.registries.processed_event_registry
- External imports (5): __future__; asyncio; contextlib; pytest; time
- Public API names: test_heartbeat_not_blocked_by_poll; test_invalid_lease_settings_fail_fast; test_registry_disable_rejected

### `backend/tests/unit/config/test_app_config_topics.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=12 | code_lines=7 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.config.app_config
- External imports (1): __future__
- Public API names: test_get_all_topics_includes_command_dlqs

### `backend/tests/unit/config/test_config_drift_guards.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=129 | code_lines=98 | risk_score=0
- API surface: public=4 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.config.app_config
- External imports (4): __future__; ast; pathlib; pytest
- Public API names: test_app_config_reflects_current_settings; test_no_application_settings_instantiation_outside_settings_module; test_no_import_global_settings_symbol_outside_settings_module; test_no_os_getenv_calls_outside_settings_module

### `backend/tests/unit/config/test_kafka_config.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=83 | code_lines=56 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=1 | methods=7
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/1 (0%) | methods=0/7 (0%)
- Internal imports (1): shared.config.kafka_config
- External imports (2): __future__; pytest
- Public API names: test_kafka_eos_consumer_config; test_kafka_eos_producer_config; test_transactional_producer_batch_no_transactions; test_transactional_producer_batch_success

### `backend/tests/unit/config/test_ontology_settings_backend_normalization.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=10 | code_lines=7 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.config.settings
- External imports (1): __future__
- Public API names: test_ontology_settings_backend_normalization_forces_postgres

### `backend/tests/unit/config/test_settings_ssot.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=115 | code_lines=81 | risk_score=0
- API surface: public=7 | top-level functions=8 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/8 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.config.settings
- External imports (2): __future__; pytest
- Public API names: test_agent_bff_token_and_command_timeout_fallback; test_client_token_fallbacks; test_lakefs_repository_defaults; test_local_port_host_aliases; test_objectify_dataset_primary_chunk_size_clamps_lower_bound; test_objectify_dataset_primary_chunk_size_defaults; test_pipeline_publish_lock_timeout_fallback

### `backend/tests/unit/errors/__init__.py`
- Module summary: Unit tests for enterprise error taxonomy and handlers.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=2 | code_lines=1 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/tests/unit/errors/test_error_envelope_lint.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=135 | code_lines=109 | risk_score=5
- API surface: public=3 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=2 | raise=3 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.errors.error_types; shared.services.core.service_factory
- External imports (5): __future__; ast; fastapi; pathlib; pytest
- Public API names: test_classified_http_exception_preserves_enterprise_and_external_codes; test_error_response_contains_enterprise_metadata; test_no_direct_status_error_dicts

### `backend/tests/unit/errors/test_error_taxonomy_audit_guard.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=92 | code_lines=86 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (5): __future__; pathlib; pytest; subprocess; sys
- Public API names: test_error_taxonomy_audit_guard

### `backend/tests/unit/errors/test_error_taxonomy_coverage.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=122 | code_lines=94 | risk_score=6
- API surface: public=1 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=2 | raise=1 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (5): __future__; ast; pathlib; pytest; re
- Public API names: test_error_taxonomy_covers_all_code_like_literals

### `backend/tests/unit/errors/test_external_error_code_sync.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=51 | code_lines=39 | risk_score=0
- API surface: public=1 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; pathlib; pytest
- Public API names: test_external_error_code_enum_is_registered_in_catalog

### `backend/tests/unit/errors/test_policy_drift_guards.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=27 | code_lines=19 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.errors.enterprise_catalog; shared.utils.canonical_json
- External imports (3): __future__; json; pathlib
- Public API names: test_agent_tool_allowlist_bundle_hash_is_pinned; test_enterprise_catalog_fingerprint_is_pinned

### `backend/tests/unit/errors/test_runtime_silent_failure_guards.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=135 | code_lines=119 | risk_score=0
- API surface: public=2 | top-level functions=3 | classes=1 | methods=2
- Runtime signals: async_functions=0 | try=1 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (0): not documented
- External imports (6): __future__; ast; pathlib; pytest; subprocess; sys
- Public API names: test_runtime_has_no_return_in_finally; test_runtime_scope_disallows_silent_failures

### `backend/tests/unit/errors/test_service_factory_error_handlers.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=89 | code_lines=77 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.errors; shared.services.core.service_factory
- External imports (2): fastapi; pytest
- Public API names: test_error_handler_records_error_taxonomy_metrics; test_service_factory_installs_error_handlers_by_default

### `backend/tests/unit/idempotency/__init__.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=0 | code_lines=0 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/tests/unit/idempotency/test_enterprise_idempotency.py`
- Module summary: Enterprise-level idempotency tests for all 5 gap implementations.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=640 | code_lines=475 | risk_score=0
- API surface: public=6 | top-level functions=0 | classes=6 | methods=32
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=6/6 (100%) | methods=32/32 (100%)
- Internal imports (0): not documented
- External imports (7): __future__; datetime; hashlib; pydantic; pytest; typing; unittest
- Public API names: TestIdempotencyIntegration; TestKafkaHeadersWithDedup; TestPipelineAPIIdempotencyKeys; TestPipelineJobDedupeKey; TestPipelineRegistryOCC; TestWorkerAggregateOrdering

### `backend/tests/unit/kafka/__init__.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=0 | code_lines=0 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/tests/unit/kafka/test_event_envelope_worker_dlq.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=176 | code_lines=146 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=5 | methods=11
- Runtime signals: async_functions=7 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/5 (0%) | methods=0/11 (0%)
- Internal imports (3): shared.models.event_envelope; shared.services.kafka.dlq_publisher; shared.services.kafka.processed_event_worker
- External imports (5): __future__; contextlib; json; pytest; typing
- Public API names: test_publish_envelope_failure_to_dlq_noops_without_producer; test_publish_envelope_failure_to_dlq_uses_default_key_and_shape; test_send_envelope_failure_to_dlq_builds_key_with_fallback

### `backend/tests/unit/kafka/test_processed_event_worker_bootstrap.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=110 | code_lines=88 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=2 | methods=6
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/2 (0%) | methods=0/6 (0%)
- Internal imports (1): shared.services.kafka.processed_event_worker
- External imports (2): __future__; typing
- Public API names: test_initialize_safe_consumer_runtime_sets_consumer_ops; test_initialize_safe_consumer_runtime_without_partition_reset

### `backend/tests/unit/kafka/test_processed_event_worker_dlq_normalization.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=336 | code_lines=272 | risk_score=0
- API surface: public=10 | top-level functions=10 | classes=5 | methods=18
- Runtime signals: async_functions=10 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/10 (0%) | classes=0/5 (0%) | methods=0/18 (0%)
- Internal imports (2): shared.services.kafka.dlq_publisher; shared.services.kafka.processed_event_worker
- External imports (3): __future__; pytest; typing
- Public API names: test_fallback_metadata_from_raw_payload_extracts_metadata_dict; test_fallback_metadata_from_raw_payload_returns_none_for_invalid_payload; test_normalize_dlq_publish_inputs_preserves_explicit_values; test_normalize_dlq_publish_inputs_uses_inferred_defaults; test_parse_error_context_extracts_custom_error_fields; test_publish_parse_error_to_dlq_calls_sender; test_publish_parse_error_to_dlq_raise_toggle; test_publish_standard_dlq_record_missing_producer_modes; test_publish_standard_dlq_record_success; test_send_standard_dlq_record_normalizes_and_invokes_publisher

### `backend/tests/unit/kafka/test_processed_event_worker_silent_failure_guards.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=201 | code_lines=152 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=4 | methods=23
- Runtime signals: async_functions=16 | try=0 | raise=4 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/4 (0%) | methods=0/23 (0%)
- Internal imports (1): shared.services.kafka.processed_event_worker
- External imports (7): __future__; asyncio; collections; dataclasses; logging; pytest; typing
- Public API names: test_handle_claimed_logs_warning_on_heartbeat_join_failure; test_handle_partition_message_keeps_primary_error_when_cleanup_fails; test_handle_partition_message_propagates_error_when_partition_revoked

### `backend/tests/unit/kafka/test_producer_ops_shutdown.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=86 | code_lines=59 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=3 | methods=6
- Runtime signals: async_functions=6 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/3 (0%) | methods=0/6 (0%)
- Internal imports (1): shared.services.kafka.producer_ops
- External imports (3): __future__; logging; pytest
- Public API names: test_close_kafka_producer_flushes_raw_producer; test_close_kafka_producer_handles_noarg_flush_signature; test_close_kafka_producer_prefers_ops_when_present; test_close_kafka_producer_swallows_close_errors

### `backend/tests/unit/kafka/test_retry_classifier.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=73 | code_lines=61 | risk_score=0
- API surface: public=5 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.kafka.retry_classifier
- External imports (1): __future__
- Public API names: test_classify_retryable_by_markers_priority_and_default; test_classify_retryable_with_profile_uses_predefined_profile; test_contains_marker_trims_and_matches_case_insensitive; test_create_retry_policy_profile_normalizes_markers; test_normalize_error_message_lowercases

### `backend/tests/unit/kafka/test_safe_consumer.py`
- Module summary: Unit tests for SafeKafkaConsumer - Strong Consistency Guarantees.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=331 | code_lines=237 | risk_score=0
- API surface: public=5 | top-level functions=0 | classes=5 | methods=16
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=5/5 (100%) | methods=16/16 (100%)
- Internal imports (0): not documented
- External imports (3): __future__; pytest; unittest
- Public API names: TestConsumerState; TestPartitionState; TestRebalanceHandler; TestSafeKafkaConsumerConfig; TestSafeKafkaConsumerIntegration

### `backend/tests/unit/kafka/test_worker_consumer_runtime.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=199 | code_lines=148 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=3 | methods=20
- Runtime signals: async_functions=11 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=1/3 (33%) | methods=0/20 (0%)
- Internal imports (2): shared.services.kafka.consumer_ops; shared.services.kafka.worker_consumer_runtime
- External imports (4): __future__; asyncio; pytest; typing
- Public API names: test_commit_checks_revoked_at_execution_time; test_commit_updates_commit_state_after_commit; test_seek_checks_revoked_at_execution_time; test_seek_updates_commit_state_after_seek

### `backend/tests/unit/mcp/test_critical_gap_fixes.py`
- Module summary: Unit tests for Critical E2E Gap Fix tools in Pipeline MCP Server.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=384 | code_lines=290 | risk_score=1
- API surface: public=6 | top-level functions=0 | classes=6 | methods=31
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=6/6 (100%) | methods=31/31 (100%)
- Internal imports (0): not documented
- External imports (5): dataclasses; datetime; pytest; typing; unittest
- Public API names: TestColumnValidationLogic; TestDatasetLookupLogic; TestExtractSparkErrorDetailsLogic; TestMCPServerIntegration; TestObjectifyWaitLogic; TestOntologyQueryLogic

### `backend/tests/unit/mcp/test_debug_mcp_tools.py`
- Module summary: Unit tests for Pipeline Agent debugging tools.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=633 | code_lines=491 | risk_score=2
- API surface: public=21 | top-level functions=20 | classes=1 | methods=1
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=20/20 (100%) | classes=1/1 (100%) | methods=0/1 (0%)
- Internal imports (0): not documented
- External imports (3): json; pytest; typing
- Public API names: MockAgentState; test_debug_dry_run_disconnected_nodes; test_debug_dry_run_empty_plan; test_debug_dry_run_missing_input; test_debug_dry_run_missing_output; test_debug_dry_run_no_nodes; test_debug_dry_run_valid_plan; test_debug_explain_failure_diagnoses_errors; test_debug_explain_failure_empty_errors; test_debug_explain_failure_handles_permission_error; test_debug_explain_failure_handles_timeout; test_debug_get_errors_empty (+9 more)

### `backend/tests/unit/mcp/test_objectify_mcp_tools.py`
- Module summary: Unit tests for Objectify MCP tools in Pipeline MCP Server.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=550 | code_lines=424 | risk_score=0
- API surface: public=15 | top-level functions=9 | classes=6 | methods=15
- Runtime signals: async_functions=20 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=9/9 (100%) | classes=0/6 (0%) | methods=0/15 (0%)
- Internal imports (0): not documented
- External imports (5): dataclasses; datetime; pytest; typing; unittest
- Public API names: MockDataset; MockDatasetRegistry; MockDatasetVersion; MockMappingSpec; MockObjectifyJob; MockObjectifyRegistry; test_objectify_create_mapping_spec; test_objectify_create_mapping_spec_validates_mappings; test_objectify_get_status; test_objectify_get_status_not_found; test_objectify_list_mapping_specs; test_objectify_run (+3 more)

### `backend/tests/unit/mcp/test_pipeline_plan_add_output_contract.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=63 | code_lines=53 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): mcp_servers.pipeline_tools.plan_tools; shared.services.pipeline.pipeline_plan_builder
- External imports (2): __future__; pytest
- Public API names: test_plan_add_output_accepts_dataset_canonical_fields; test_plan_add_output_normalizes_dataset_camel_case_aliases

### `backend/tests/unit/mcp/test_pipeline_plan_add_udf_contract.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=71 | code_lines=58 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): mcp_servers.pipeline_tools.plan_tools; shared.services.pipeline.pipeline_plan_builder
- External imports (2): __future__; pytest
- Public API names: test_plan_add_udf_accepts_camel_case_aliases; test_plan_add_udf_accepts_reference_fields; test_plan_add_udf_requires_pinned_version

### `backend/tests/unit/middleware/test_middleware_fixes.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=970 | code_lines=801 | risk_score=1
- API surface: public=21 | top-level functions=22 | classes=0 | methods=0
- Runtime signals: async_functions=33 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/22 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (6): bff.middleware.auth; bff.routers.admin; oms.middleware.auth; shared.i18n.middleware; shared.middleware.rate_limiter; shared.services.registries.agent_tool_registry
- External imports (7): contextlib; datetime; fastapi; jose; os; pytest; types
- Public API names: test_bff_admin_guard_allows_dev_master_without_token_in_development; test_bff_admin_guard_requires_token_in_production_even_with_dev_master_flag; test_bff_admin_token_requires_user_jwt_for_agent_endpoints_when_enabled; test_bff_agent_auth_accepts_rotated_agent_tokens; test_bff_agent_auth_requires_delegated_user_jwt_when_enabled; test_bff_agent_auth_requires_user_jwt_enabled_for_agent_calls; test_bff_agent_tool_idempotency_replays_without_reexecution; test_bff_agent_tool_policy_enforced_via_tool_registry; test_bff_agent_tool_policy_enforces_action_type_and_ontology_abac; test_bff_agent_tool_policy_enforces_session_enabled_tools_and_abac; test_bff_auth_accepts_rotated_admin_tokens; test_bff_auth_allows_user_jwt_when_enabled (+9 more)

### `backend/tests/unit/models/test_type_inference_model_defaults.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: domain/request-response schema definitions
- Source footprint: total_lines=8 | code_lines=5 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.models.type_inference
- External imports (1): __future__
- Public API names: test_type_mapping_request_default_target_system_is_foundry

### `backend/tests/unit/monitoring/test_monitoring_configs.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=135 | code_lines=106 | risk_score=0
- API surface: public=5 | top-level functions=7 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=2/7 (28%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (5): __future__; json; pathlib; pytest; yaml
- Public API names: test_alert_rules_yaml_is_valid_and_has_minimum_alerts; test_alertmanager_yaml_is_valid_and_references_default_receiver; test_grafana_dashboard_json_is_valid; test_operations_doc_mentions_backup_scripts; test_prometheus_config_yaml_is_valid_and_wired

### `backend/tests/unit/observability/test_config_monitor.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=18 | code_lines=14 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.routers
- External imports (2): fastapi; pytest
- Public API names: test_config_monitor_current_endpoint_ok

### `backend/tests/unit/observability/test_context_propagation.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=44 | code_lines=32 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): importlib; pytest
- Public API names: test_kafka_headers_from_envelope_metadata_only_emits_known_keys; test_kafka_headers_roundtrip_via_attached_context

### `backend/tests/unit/observability/test_observability_status.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=50 | code_lines=41 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.observability.metrics; shared.services.core.service_factory
- External imports (3): __future__; fastapi; pytest
- Public API names: test_metrics_collector_is_scoped_per_service_name; test_service_factory_exposes_observability_status_endpoint

### `backend/tests/unit/observability/test_request_context.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=109 | code_lines=82 | risk_score=0
- API surface: public=6 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): logging; pytest
- Public API names: test_attach_context_from_kafka_uses_fallback_metadata_for_contextvars; test_carrier_from_envelope_metadata_appends_baggage_from_fields; test_event_envelope_from_command_includes_context_correlation_id; test_event_envelope_from_command_prefers_command_metadata_over_context; test_parse_baggage_header_best_effort; test_trace_context_filter_includes_request_context_fields

### `backend/tests/unit/observability/test_tracing_config.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=109 | code_lines=81 | risk_score=1
- API surface: public=3 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.observability.tracing
- External imports (4): contextlib; importlib; os; pytest
- Public API names: test_otlp_export_disabled_when_no_endpoint; test_span_omits_kind_when_none; test_span_passes_kind_when_set

### `backend/tests/unit/oms/test_action_async_batch_and_undo.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=195 | code_lines=161 | risk_score=0
- API surface: public=3 | top-level functions=4 | classes=1 | methods=2
- Runtime signals: async_functions=14 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (3): oms.dependencies; oms.routers.action_async; shared.services.registries.action_log_registry
- External imports (8): __future__; datetime; fastapi; httpx; pytest; types; typing; uuid
- Public API names: app_with_router; test_submit_batch_registers_dependencies_and_defers_children; test_undo_action_creates_pending_undo_command

### `backend/tests/unit/oms/test_action_async_permission_profile_api.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=355 | code_lines=284 | risk_score=0
- API surface: public=6 | top-level functions=8 | classes=1 | methods=1
- Runtime signals: async_functions=36 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/8 (0%) | classes=0/1 (0%) | methods=0/1 (0%)
- Internal imports (3): oms.dependencies; oms.routers.action_async; oms.services.action_simulation_service
- External imports (6): __future__; fastapi; httpx; pytest; types; typing
- Public API names: action_async_app; test_simulate_returns_503_when_datasource_derived_data_access_is_unverifiable; test_simulate_use_branch_head_no_longer_requires_terminus; test_submit_returns_403_for_datasource_derived_without_data_engineer_role; test_submit_returns_403_when_target_class_misses_required_interface; test_submit_returns_503_when_target_edit_access_is_unverifiable

### `backend/tests/unit/oms/test_database_router_backend_profile.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=10 | code_lines=7 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): oms.routers
- External imports (1): __future__
- Public API names: test_oms_database_routes_are_foundry_style_without_profile_gates

### `backend/tests/unit/oms/test_dependencies_database_exists.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=57 | code_lines=44 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=7 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): oms.dependencies
- External imports (3): __future__; fastapi; pytest
- Public API names: test_ensure_database_exists_raises_404_when_missing; test_ensure_database_exists_raises_404_when_missing_in_postgres_registry; test_ensure_database_exists_returns_name_when_present; test_ensure_database_exists_uses_postgres_registry_when_terminus_missing

### `backend/tests/unit/oms/test_dependencies_terminus_profile_gate.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=8 | code_lines=5 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): oms.dependencies
- External imports (1): __future__
- Public API names: test_legacy_terminus_dependency_accessors_are_removed

### `backend/tests/unit/oms/test_instance_async_permission_profile.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=43 | code_lines=33 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): oms.routers; shared.models.commands
- External imports (2): __future__; pytest
- Public API names: test_create_instance_async_allows_missing_terminus_in_postgres_profile

### `backend/tests/unit/oms/test_instance_router.py`
- Module summary: Tests for OMS instance router (ES-backed, Phase 2).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=137 | code_lines=102 | risk_score=0
- API surface: public=9 | top-level functions=10 | classes=0 | methods=0
- Runtime signals: async_functions=8 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/10 (10%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): oms.dependencies; oms.routers.instance; shared.dependencies.providers
- External imports (5): __future__; fastapi; httpx; pytest; unittest
- Public API names: mock_es; override_deps; test_get_class_instance_count; test_get_class_instances; test_get_class_instances_with_search; test_get_instance; test_get_instance_class_mismatch; test_get_instance_not_found; test_sparql_endpoint_removed_returns_404

### `backend/tests/unit/oms/test_main_backend_profile.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=37 | code_lines=26 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): oms
- External imports (3): __future__; pytest; types
- Public API names: test_root_forces_postgres_backend_when_legacy_profile_is_configured; test_root_reports_postgres_profile_features

### `backend/tests/unit/oms/test_object_search_router.py`
- Module summary: Tests for OMS Foundry-style object search router.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=398 | code_lines=329 | risk_score=0
- API surface: public=20 | top-level functions=20 | classes=0 | methods=0
- Runtime signals: async_functions=19 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/20 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (4): oms.dependencies; oms.routers.query; shared.dependencies.providers; shared.utils.foundry_page_token
- External imports (5): __future__; fastapi; httpx; pytest; unittest
- Public API names: mock_es; override_deps; test_search_objects_v2_accepts_contains_any_term_operator; test_search_objects_v2_accepts_deprecated_startswith_alias; test_search_objects_v2_accepts_foundry_branch_rid; test_search_objects_v2_accepts_non_deprecated_operator; test_search_objects_v2_accepts_scope_matched_page_token; test_search_objects_v2_allows_missing_where_with_match_all_fallback; test_search_objects_v2_expired_page_token_returns_foundry_error; test_search_objects_v2_invalid_page_token_returns_foundry_error; test_search_objects_v2_is_null_false_maps_to_exists_clause; test_search_objects_v2_rejects_excessive_nesting_depth (+8 more)

### `backend/tests/unit/oms/test_ontology_extensions_health.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=35 | code_lines=28 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): oms.routers.ontology_extensions
- External imports (2): __future__; pytest
- Public API names: test_compute_ontology_health_supports_resource_only_mode

### `backend/tests/unit/oms/test_ontology_extensions_occ.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=98 | code_lines=80 | risk_score=0
- API surface: public=6 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=8 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): oms.routers.ontology_extensions
- External imports (3): __future__; fastapi; pytest
- Public API names: test_assert_expected_head_commit_returns_trimmed_token_when_allowed_by_branch; test_assert_expected_head_commit_skips_when_blank; test_assert_expected_head_commit_skips_when_unset; test_assert_expected_head_commit_strict_mode_accepts_latest_deployed_commit; test_assert_expected_head_commit_strict_mode_rejects_blank; test_assert_expected_head_commit_strict_mode_rejects_mismatch

### `backend/tests/unit/oms/test_ontology_extensions_resource_promotion.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=117 | code_lines=95 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=1 | methods=3
- Runtime signals: async_functions=8 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/1 (0%) | methods=0/3 (0%)
- Internal imports (1): oms.routers
- External imports (3): __future__; pytest; typing
- Public API names: test_approve_proposal_promotes_ontology_resources; test_deploy_materializes_commit_snapshot_for_resources

### `backend/tests/unit/oms/test_ontology_router_resource_helpers.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=67 | code_lines=54 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): oms.routers.ontology
- External imports (2): __future__; pytest
- Public API names: test_coerce_property_models_backfills_label_for_resource_payload; test_coerce_relationship_models_backfills_label_for_resource_payload; test_is_internal_ontology_detects_internal_dict_payload; test_ontology_from_resource_payload_maps_object_type_contract

### `backend/tests/unit/oms/test_ontology_router_write_backend.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=49 | code_lines=41 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=1 | methods=1
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/1 (0%) | methods=0/1 (0%)
- Internal imports (1): oms.routers
- External imports (2): __future__; pytest
- Public API names: test_load_existing_ontology_for_write_reads_from_resource_registry_without_terminus

### `backend/tests/unit/oms/test_openapi_legacy_visibility.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=23 | code_lines=18 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): oms.main
- External imports (1): __future__
- Public API names: test_oms_legacy_routes_removed_from_openapi

### `backend/tests/unit/openapi/test_foundry_ontology_v2_contract.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=633 | code_lines=517 | risk_score=0
- API surface: public=18 | top-level functions=20 | classes=0 | methods=0
- Runtime signals: async_functions=29 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/20 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): bff.dependencies; bff.routers
- External imports (4): fastapi; httpx; pytest; unittest
- Public API names: test_foundry_v2_full_metadata_branch_contract; test_foundry_v2_list_linked_objects_includes_foundry_query_params; test_foundry_v2_list_objects_includes_foundry_query_params; test_foundry_v2_object_type_list_keeps_pagination_and_branch_params; test_foundry_v2_ontology_list_has_no_pagination_params; test_foundry_v2_ontology_read_paths_include_branch_when_supported; test_foundry_v2_route_full_metadata_strict_off_keeps_legacy_branch_and_payload; test_foundry_v2_route_full_metadata_strict_on_applies_branch_and_required_fields; test_foundry_v2_route_get_object_type_strict_off_keeps_legacy_payload; test_foundry_v2_route_get_object_type_strict_on_applies_required_fields; test_foundry_v2_route_get_outgoing_link_type_strict_off_unresolved_returns_payload; test_foundry_v2_route_get_outgoing_link_type_strict_on_unresolved_returns_not_found (+6 more)

### `backend/tests/unit/openapi/test_removed_v1_compat_guard.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=56 | code_lines=40 | risk_score=0
- API surface: public=3 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.routers
- External imports (3): fastapi; pathlib; pytest
- Public API names: test_migration_docs_synced_to_code_deleted_status; test_removed_v1_compat_operations_absent_from_openapi; test_removed_v1_query_path_not_allowlisted_for_agent_tools

### `backend/tests/unit/openapi/test_wip_hidden.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=17 | code_lines=12 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.routers
- External imports (2): fastapi; pytest
- Public API names: test_wip_projection_endpoints_hidden_from_openapi

### `backend/tests/unit/pipeline_functions/test_functions_matrix_contract.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=41 | code_lines=30 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.tools.foundry_functions_compat
- External imports (2): __future__; pytest
- Public API names: test_functions_snapshot_entries_have_valid_classification; test_functions_snapshot_has_no_unclassified_rows; test_functions_snapshot_loader_fallback_parser_without_pyyaml

### `backend/tests/unit/pipeline_functions/test_functions_preview_compat.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=129 | code_lines=106 | risk_score=0
- API surface: public=1 | top-level functions=3 | classes=3 | methods=4
- Runtime signals: async_functions=6 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/3 (0%) | methods=0/4 (0%)
- Internal imports (2): shared.services.pipeline.pipeline_executor; shared.tools.foundry_functions_compat
- External imports (4): __future__; dataclasses; pytest; typing
- Public API names: test_functions_preview_supported_matrix_contract

### `backend/tests/unit/pipeline_functions/test_functions_spark_compat.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=110 | code_lines=89 | risk_score=7
- API surface: public=3 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): pipeline_worker.main; shared.tools.foundry_functions_compat
- External imports (6): __future__; importlib; os; pyspark; pytest; sys
- Public API names: spark; test_functions_spark_supported_matrix_contract; worker

### `backend/tests/unit/routers/test_database_router_openapi_visibility.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=17 | code_lines=14 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.main
- External imports (1): __future__
- Public API names: test_removed_legacy_routes_not_exposed_in_openapi

### `backend/tests/unit/routers/test_legacy_router_backend_profile.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=10 | code_lines=7 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.main
- External imports (1): __future__
- Public API names: test_removed_legacy_ontology_merge_routes_not_mounted

### `backend/tests/unit/routers/test_lineage_router_column_lineage.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=231 | code_lines=204 | risk_score=0
- API surface: public=5 | top-level functions=5 | classes=2 | methods=4
- Runtime signals: async_functions=5 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/2 (0%) | methods=0/4 (0%)
- Internal imports (1): bff.routers.lineage
- External imports (3): __future__; pytest; types
- Public API names: test_extract_column_lineage_ref_entry_from_mapping_spec_fields; test_get_lineage_column_lineage_marks_version_mismatch_unresolved; test_get_lineage_column_lineage_resolves_mapping_pairs; test_get_lineage_column_lineage_uses_inline_pairs_when_ref_unresolved; test_parse_objectify_mapping_spec_ref

### `backend/tests/unit/routers/test_lineage_router_naming.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=19 | code_lines=15 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.routers.lineage
- External imports (1): __future__
- Public API names: test_suggest_remediation_actions_uses_graph_target_kind

### `backend/tests/unit/routers/test_lineage_router_out_of_date.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=379 | code_lines=317 | risk_score=0
- API surface: public=18 | top-level functions=18 | classes=1 | methods=4
- Runtime signals: async_functions=5 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/18 (0%) | classes=0/1 (0%) | methods=0/4 (0%)
- Internal imports (1): bff.routers.lineage
- External imports (4): __future__; datetime; pytest; typing
- Public API names: test_artifact_latest_writer_state; test_batched_latest_edges_helpers_chunk_large_inputs; test_chunked_helper; test_edge_cause_payload_extracts_latest_writer_context; test_edge_cause_payload_returns_none_for_invalid_input; test_enrich_artifacts_with_latest_writer; test_event_node_id_from_latest_writer; test_extract_impacted_artifacts_from_edges_keeps_latest_per_artifact; test_freshness_status_healthy_warning_critical; test_freshness_status_no_data; test_normalize_window_defaults_and_order; test_out_of_date_scope_resolution (+6 more)

### `backend/tests/unit/routers/test_lineage_router_path_diff.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=92 | code_lines=76 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): bff.routers.lineage; shared.models.lineage
- External imports (1): __future__
- Public API names: test_count_artifact_kinds; test_edge_signature_uses_projection_name_metadata; test_find_shortest_path_downstream; test_find_shortest_path_upstream_uses_reverse_traversal

### `backend/tests/unit/routers/test_lineage_router_timeline.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=67 | code_lines=53 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.routers.lineage
- External imports (2): __future__; datetime
- Public API names: test_bucket_start_floors_to_interval; test_build_timeline_summary_counts_and_spikes; test_compact_edge_metadata_whitelists_keys

### `backend/tests/unit/routers/test_monitoring_router_deprecation_headers.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=35 | code_lines=23 | risk_score=0
- API surface: public=2 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.routers.monitoring
- External imports (2): __future__; fastapi
- Public API names: test_metrics_redirect_with_deprecated_query_sets_deprecation_headers; test_metrics_redirect_without_deprecated_query_has_no_deprecation_headers

### `backend/tests/unit/routers/test_schema_changes.py`
- Module summary: Unit tests for schema_changes router.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=398 | code_lines=315 | risk_score=0
- API surface: public=3 | top-level functions=0 | classes=3 | methods=20
- Runtime signals: async_functions=12 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=3/3 (100%) | methods=20/20 (100%)
- Internal imports (1): bff.routers.schema_changes
- External imports (4): __future__; datetime; pytest; unittest
- Public API names: TestMappingCompatibility; TestSchemaChangesEndpoints; TestSchemaChangesModels

### `backend/tests/unit/routers/test_summary_backend_profile.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: HTTP contract/endpoint routing
- Source footprint: total_lines=58 | code_lines=41 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=2 | methods=5
- Runtime signals: async_functions=7 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/2 (0%) | methods=0/5 (0%)
- Internal imports (1): bff.routers.summary
- External imports (2): __future__; pytest
- Public API names: test_summary_keeps_branch_info_disabled_in_hybrid_profile; test_summary_skips_branch_info_in_postgres_profile

### `backend/tests/unit/security/__init__.py`
- Module summary: Unit tests for security modules
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=3 | code_lines=3 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/tests/unit/security/test_data_encryption.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=147 | code_lines=123 | risk_score=0
- API surface: public=6 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.security.data_encryption; shared.services.registries.agent_session_registry
- External imports (3): __future__; datetime; pytest
- Public API names: test_agent_session_registry_decrypts_message_content; test_agent_session_registry_decrypts_tool_call_payloads; test_data_encryptor_round_trip_bytes; test_data_encryptor_round_trip_json; test_data_encryptor_round_trip_text; test_data_encryptor_supports_key_rotation

### `backend/tests/unit/security/test_database_access.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=99 | code_lines=70 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=1 | methods=3
- Runtime signals: async_functions=11 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/1 (0%) | methods=0/3 (0%)
- Internal imports (1): shared.security.database_access
- External imports (2): asyncpg; pytest
- Public API names: test_enforce_database_role_allows_when_unconfigured_and_flag_unset; test_enforce_database_role_denies_when_flag_true_and_no_role; test_get_database_access_role_returns_none_when_table_missing; test_has_database_access_config_returns_false_when_table_missing

### `backend/tests/unit/security/test_input_sanitizer_branch.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=15 | code_lines=9 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.security.input_sanitizer
- External imports (2): __future__; pytest
- Public API names: test_validate_branch_name_accepts_foundry_branch_rid; test_validate_branch_name_rejects_reserved_head

### `backend/tests/unit/security/test_principal_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=44 | code_lines=31 | risk_score=0
- API surface: public=6 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.security.principal_utils
- External imports (0): not documented
- Public API names: test_actor_label_defaults; test_resolve_principal_defaults_when_missing_headers; test_resolve_principal_enforces_allowed_types; test_resolve_principal_supports_custom_defaults; test_resolve_principal_uses_custom_header_keys; test_resolve_principal_uses_header_values

### `backend/tests/unit/serializers/test_complex_type_serializer.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=57 | code_lines=39 | risk_score=0
- API surface: public=6 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.models.common; shared.serializers.complex_type_serializer
- External imports (1): __future__
- Public API names: test_array_roundtrip; test_coordinate_string_deserialization; test_enum_serialization; test_image_and_file_serialization; test_money_serialization_object; test_object_roundtrip

### `backend/tests/unit/services/__init__.py`
- Module summary: Unit tests for SPICE HARVESTER shared services
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=3 | code_lines=3 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/tests/unit/services/fake_async_redis.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=101 | code_lines=79 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=2 | methods=12
- Runtime signals: async_functions=11 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=1/2 (50%) | methods=0/12 (0%)
- Internal imports (0): not documented
- External imports (3): __future__; dataclasses; typing
- Public API names: FakeAsyncRedis

### `backend/tests/unit/services/test_action_simulation_assumptions.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=57 | code_lines=45 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): oms.services.action_simulation_service; shared.utils.writeback_conflicts
- External imports (2): __future__; pytest
- Public API names: test_apply_assumption_patch_applies_set_unset_and_links; test_apply_assumption_patch_rejects_forbidden_field; test_apply_observed_base_overrides_rejects_unknown_field; test_observed_base_override_can_create_conflict

### `backend/tests/unit/services/test_action_simulation_permission_profile.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=132 | code_lines=106 | risk_score=0
- API surface: public=6 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=12 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): oms.services.action_simulation_service
- External imports (2): __future__; pytest
- Public API names: test_enforce_action_permission_allows_datasource_derived_without_policy; test_enforce_action_permission_allows_ontology_roles_model; test_enforce_action_permission_rejects_datasource_derived_for_non_engineer_role; test_enforce_action_permission_rejects_edits_beyond_actions_without_engineer_role; test_enforce_action_permission_rejects_invalid_permission_profile; test_enforce_action_permission_rejects_policy_mismatch

### `backend/tests/unit/services/test_action_simulation_scenarios.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=103 | code_lines=90 | risk_score=0
- API surface: public=4 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): oms.services.action_simulation_service
- External imports (2): __future__; pytest
- Public API names: test_conflict_policy_base_wins_skips; test_conflict_policy_fail_rejects; test_conflict_policy_writeback_wins_applies; test_no_conflict_does_not_reject_under_fail

### `backend/tests/unit/services/test_admin_recompute_projection_service.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=99 | code_lines=81 | risk_score=0
- API surface: public=3 | top-level functions=5 | classes=1 | methods=2
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (2): bff.schemas.admin_projection_requests; bff.services.admin_recompute_projection_service
- External imports (5): __future__; datetime; fastapi; pytest; types
- Public API names: test_recompute_projection_task_blocks_instances_in_dataset_primary_mode; test_start_recompute_projection_allows_ontologies_in_dataset_primary_mode; test_start_recompute_projection_blocks_instances_in_dataset_primary_mode

### `backend/tests/unit/services/test_admin_reindex_instances.py`
- Module summary: Tests for admin_reindex_instances_service.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=128 | code_lines=98 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=1 | methods=1
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/4 (0%) | classes=0/1 (0%) | methods=0/1 (0%)
- Internal imports (1): bff.services.admin_reindex_instances_service
- External imports (4): __future__; pytest; typing; unittest
- Public API names: test_reindex_handles_missing_dataset_version; test_reindex_no_mapping_specs; test_reindex_submits_jobs; test_reindex_with_multiple_specs

### `backend/tests/unit/services/test_agent_graph_retry.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=279 | code_lines=255 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=1 | methods=3
- Runtime signals: async_functions=6 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/1 (0%) | methods=0/3 (0%)
- Internal imports (2): agent.models; agent.services.agent_run_loop
- External imports (2): __future__; pytest
- Public API names: test_agent_graph_does_not_retry_writes_by_default; test_agent_graph_respects_enterprise_max_attempts; test_agent_graph_retries_transient_read_failure; test_agent_graph_uses_retry_after_when_allowed

### `backend/tests/unit/services/test_agent_graph_simulation_gate.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=98 | code_lines=85 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=1 | methods=3
- Runtime signals: async_functions=3 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/1 (0%) | methods=0/3 (0%)
- Internal imports (2): agent.models; agent.services.agent_run_loop
- External imports (2): __future__; pytest
- Public API names: test_simulation_rejection_stops_before_submit

### `backend/tests/unit/services/test_agent_overlay_policy.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=66 | code_lines=57 | risk_score=0
- API surface: public=2 | top-level functions=1 | classes=1 | methods=2
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (2): agent.models; agent.services.agent_runtime
- External imports (2): __future__; pytest
- Public API names: DummyEventStore; test_agent_blocks_write_when_overlay_degraded

### `backend/tests/unit/services/test_agent_policy.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=164 | code_lines=150 | risk_score=0
- API surface: public=6 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): agent.models; agent.services.agent_policy
- External imports (2): __future__; pytest
- Public API names: test_policy_idempotency_in_progress_is_safe_retry; test_policy_overlay_degraded_safe_mode; test_policy_submission_criteria_failed_includes_reason; test_policy_submission_criteria_failed_state_mismatch_proposes_check_state; test_policy_timeout_retry_for_reads; test_policy_validation_no_retry

### `backend/tests/unit/services/test_agent_retention_worker.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=88 | code_lines=68 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.agent.agent_retention_worker
- External imports (5): __future__; asyncio; json; pytest; unittest
- Public API names: test_agent_retention_worker_calls_apply_retention_once; test_agent_retention_worker_supports_policy_per_object_type

### `backend/tests/unit/services/test_agent_runtime_artifacts.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=342 | code_lines=298 | risk_score=0
- API surface: public=4 | top-level functions=3 | classes=1 | methods=2
- Runtime signals: async_functions=10 | try=0 | raise=3 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/3 (33%) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (2): agent.models; agent.services.agent_runtime
- External imports (3): __future__; pytest; uuid
- Public API names: DummyEventStore; test_agent_runtime_blocks_missing_required_artifacts; test_agent_runtime_compacts_large_tool_payload_instead_of_omitting; test_agent_runtime_stores_produced_artifacts_and_resolves_templates

### `backend/tests/unit/services/test_agent_runtime_delegated_auth.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=104 | code_lines=85 | risk_score=0
- API surface: public=2 | top-level functions=1 | classes=1 | methods=2
- Runtime signals: async_functions=5 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (2): agent.models; agent.services.agent_runtime
- External imports (3): __future__; pytest; uuid
- Public API names: DummyEventStore; test_agent_runtime_sends_service_token_and_delegated_user_token

### `backend/tests/unit/services/test_agent_runtime_pipeline_wait.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=136 | code_lines=116 | risk_score=0
- API surface: public=2 | top-level functions=1 | classes=1 | methods=2
- Runtime signals: async_functions=6 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (2): agent.models; agent.services.agent_runtime
- External imports (3): __future__; pytest; uuid
- Public API names: DummyEventStore; test_agent_runtime_waits_for_pipeline_job_completion

### `backend/tests/unit/services/test_agent_runtime_simulation_signals.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=81 | code_lines=69 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): agent.services.agent_runtime
- External imports (2): __future__; pytest
- Public API names: test_extract_action_simulation_rejection_returns_enterprise; test_extract_action_simulation_rejection_returns_none_when_accepted; test_extract_action_simulation_signals_rejected_includes_reason

### `backend/tests/unit/services/test_agent_runtime_templating.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=137 | code_lines=113 | risk_score=0
- API surface: public=2 | top-level functions=1 | classes=1 | methods=2
- Runtime signals: async_functions=5 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (2): agent.models; agent.services.agent_runtime
- External imports (2): __future__; pytest
- Public API names: DummyEventStore; test_agent_runtime_resolves_step_output_templates

### `backend/tests/unit/services/test_agent_session_state_machine.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=52 | code_lines=44 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.registries.agent_session_registry
- External imports (2): __future__; pytest
- Public API names: test_validate_session_status_transition_allows_valid_transitions; test_validate_session_status_transition_rejects_invalid_transitions; test_validate_session_status_transition_rejects_unknown_states

### `backend/tests/unit/services/test_ai_service_graph_provenance.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=54 | code_lines=45 | risk_score=0
- API surface: public=2 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): bff.services.ai_service; shared.models.graph_query
- External imports (1): __future__
- Public API names: test_ground_graph_query_result_ignores_legacy_terminus_key; test_ground_graph_query_result_prefers_graph_provenance

### `backend/tests/unit/services/test_changelog_store.py`
- Module summary: Tests for ChangelogStore.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=139 | code_lines=102 | risk_score=0
- API surface: public=6 | top-level functions=6 | classes=2 | methods=8
- Runtime signals: async_functions=14 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/6 (16%) | classes=1/2 (50%) | methods=0/8 (0%)
- Internal imports (1): shared.services.registries.changelog_store
- External imports (4): __future__; pytest; typing; unittest
- Public API names: test_get_changelog; test_get_changelog_not_found; test_list_changelogs; test_list_changelogs_with_class_filter; test_record_changelog; test_record_changelog_failure_handled

### `backend/tests/unit/services/test_command_status_fallback.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=69 | code_lines=53 | risk_score=0
- API surface: public=4 | top-level functions=2 | classes=2 | methods=4
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/2 (0%) | methods=0/4 (0%)
- Internal imports (2): oms.routers.command_status; shared.models.commands
- External imports (2): pytest; uuid
- Public API names: DummyEventStore; DummyRegistry; test_command_status_falls_back_to_event_store_when_registry_has_no_record; test_command_status_falls_back_to_registry

### `backend/tests/unit/services/test_consistency_token.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=63 | code_lines=50 | risk_score=1
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.services.core.consistency_token; tests.unit.services.fake_async_redis
- External imports (3): __future__; pytest; uuid
- Public API names: test_consistency_token_service_creates_metadata; test_token_roundtrip

### `backend/tests/unit/services/test_database_error_policy.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=48 | code_lines=41 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.services.database_error_policy
- External imports (4): __future__; fastapi; logging; pytest
- Public API names: test_apply_message_error_policies_raises_policy_http_exception; test_apply_message_error_policies_returns_fallback_when_configured

### `backend/tests/unit/services/test_dataset_ingest_commit_service.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=179 | code_lines=146 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=3 | methods=3
- Runtime signals: async_functions=10 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/3 (0%) | methods=0/3 (0%)
- Internal imports (1): bff.services.dataset_ingest_commit_service
- External imports (4): __future__; dataclasses; pytest; typing
- Public API names: test_ensure_lakefs_commit_artifact_commits_when_missing; test_ensure_lakefs_commit_artifact_uses_existing_values; test_persist_ingest_commit_state_marks_and_updates_transaction; test_persist_ingest_commit_state_skips_when_unchanged

### `backend/tests/unit/services/test_dataset_output_semantics.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=240 | code_lines=204 | risk_score=0
- API surface: public=17 | top-level functions=17 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/17 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline.dataset_output_semantics
- External imports (2): __future__; pytest
- Public API names: test_dataset_write_policy_hash_stable; test_normalize_dataset_output_metadata_legacy_aliases; test_resolve_dataset_write_policy_changelog_uses_append_runtime; test_resolve_dataset_write_policy_default_incremental_additive_without_pk; test_resolve_dataset_write_policy_default_incremental_with_additive_updates_false; test_resolve_dataset_write_policy_default_incremental_with_additive_updates_true; test_resolve_dataset_write_policy_default_incremental_with_pk_without_additive_signal; test_resolve_dataset_write_policy_default_incremental_without_pk; test_resolve_dataset_write_policy_default_snapshot; test_resolve_dataset_write_policy_default_streaming_with_pk_uses_append_only_new_rows; test_resolve_dataset_write_policy_default_streaming_without_pk_uses_always_append; test_resolve_dataset_write_policy_default_without_incremental_inputs (+5 more)

### `backend/tests/unit/services/test_dlq_handler_fixed.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=165 | code_lines=132 | risk_score=2
- API surface: public=2 | top-level functions=3 | classes=2 | methods=12
- Runtime signals: async_functions=11 | try=2 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/2 (0%) | methods=0/12 (0%)
- Internal imports (1): shared.services.events.dlq_handler_fixed
- External imports (6): __future__; datetime; json; pytest; typing; uuid
- Public API names: test_dlq_handler_retry_and_poison_flow; test_dlq_handler_retry_success_records_recovery

### `backend/tests/unit/services/test_dlq_payload_shapes.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=205 | code_lines=164 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=4 | methods=13
- Runtime signals: async_functions=3 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/4 (0%) | methods=0/13 (0%)
- Internal imports (0): not documented
- External imports (5): __future__; contextlib; json; pytest; typing
- Public API names: test_action_worker_send_to_dlq_payload_shape; test_instance_worker_send_to_dlq_payload_shape; test_ontology_worker_send_to_dlq_payload_shape

### `backend/tests/unit/services/test_envelope_dlq_publisher.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=121 | code_lines=100 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=3 | methods=6
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/3 (0%) | methods=0/6 (0%)
- Internal imports (2): shared.models.event_envelope; shared.services.kafka.dlq_publisher
- External imports (5): __future__; contextlib; json; pytest; typing
- Public API names: test_build_envelope_dlq_event_applies_standard_metadata; test_publish_envelope_dlq_uses_producer_ops_and_flushes

### `backend/tests/unit/services/test_event_replay.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=160 | code_lines=126 | risk_score=7
- API surface: public=2 | top-level functions=6 | classes=1 | methods=7
- Runtime signals: async_functions=2 | try=3 | raise=1 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/6 (0%) | classes=0/1 (0%) | methods=0/7 (0%)
- Internal imports (1): shared.services.events.event_replay
- External imports (6): __future__; datetime; io; json; pytest; uuid
- Public API names: test_event_replay_aggregate_and_history; test_event_replay_all_and_determinism

### `backend/tests/unit/services/test_event_store_connect_idempotent.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=129 | code_lines=92 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=3 | methods=9
- Runtime signals: async_functions=12 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/3 (0%) | methods=0/9 (0%)
- Internal imports (4): shared.errors.runtime_exception_policy; shared.models.event_envelope; shared.services.storage; shared.services.storage.event_store
- External imports (4): __future__; asyncio; pytest; typing
- Public API names: test_event_store_connect_fails_when_lineage_required_and_store_unavailable; test_event_store_connect_is_idempotent_under_concurrency; test_event_store_lineage_record_failure_propagates_when_required

### `backend/tests/unit/services/test_fk_pattern_detector.py`
- Module summary: Unit tests for FK Pattern Detector service.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=246 | code_lines=206 | risk_score=0
- API surface: public=2 | top-level functions=0 | classes=2 | methods=14
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/2 (0%) | methods=12/14 (85%)
- Internal imports (1): shared.services.pipeline.fk_pattern_detector
- External imports (2): __future__; pytest
- Public API names: TestFKDetectionConfig; TestForeignKeyPatternDetector

### `backend/tests/unit/services/test_funnel_data_processor.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=60 | code_lines=48 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): httpx; pytest
- Public API names: test_process_google_sheets_preview_sets_explicit_timeout

### `backend/tests/unit/services/test_graph_federation_service_es.py`
- Module summary: Tests for GraphFederationServiceES — ES-native Search Arounds.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=432 | code_lines=277 | risk_score=0
- API surface: public=15 | top-level functions=18 | classes=0 | methods=0
- Runtime signals: async_functions=9 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=8/18 (44%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.core.graph_federation_service_es
- External imports (2): pytest; unittest
- Public API names: test_empty_relationships; test_empty_start_returns_empty; test_fan_out_cap; test_find_paths_es_sampling; test_forward_single_hop; test_get_relationship_refs; test_multi_hop_3_layers; test_no_cycles; test_node_id; test_normalize_hops_dict; test_normalize_hops_empty; test_normalize_hops_tuple (+3 more)

### `backend/tests/unit/services/test_graph_query_paths_normalization.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=36 | code_lines=27 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.services.graph_query_service
- External imports (1): __future__
- Public API names: test_normalize_es_doc_id_strips_class_prefix; test_normalize_paths_converts_list_paths_to_object_shape; test_normalize_paths_preserves_existing_object_paths

### `backend/tests/unit/services/test_graph_query_service_naming.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=190 | code_lines=168 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=5 | methods=5
- Runtime signals: async_functions=8 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/5 (0%) | methods=0/5 (0%)
- Internal imports (2): bff.services.graph_query_service; shared.models.graph_query
- External imports (3): __future__; pytest; typing
- Public API names: test_execute_graph_query_provenance_uses_graph_key; test_execute_multi_hop_query_uses_foundry_naming; test_execute_simple_graph_query_uses_foundry_naming

### `backend/tests/unit/services/test_graph_service_health.py`
- Module summary: Tests for graph_service_health (ES-only mode).
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=59 | code_lines=41 | risk_score=0
- API surface: public=4 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/5 (20%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.services.graph_query_service
- External imports (4): __future__; pytest; typing; unittest
- Public API names: test_health_connection_failure; test_health_green; test_health_red; test_health_yellow

### `backend/tests/unit/services/test_health_check_redis.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=44 | code_lines=29 | risk_score=0
- API surface: public=4 | top-level functions=2 | classes=2 | methods=4
- Runtime signals: async_functions=6 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/2 (0%) | methods=0/4 (0%)
- Internal imports (1): shared.services.core.health_check
- External imports (1): pytest
- Public API names: RedisServiceInfoError; RedisServiceWithInfo; test_redis_health_check_ignores_info_errors; test_redis_health_check_includes_info_details

### `backend/tests/unit/services/test_idempotency_service.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=63 | code_lines=50 | risk_score=2
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=2 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.services.events.idempotency_service; tests.unit.services.fake_async_redis
- External imports (3): __future__; pytest; uuid
- Public API names: test_idempotency_service_detects_duplicates; test_idempotency_service_marks_processed_and_failed

### `backend/tests/unit/services/test_input_validation_service.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=50 | code_lines=35 | risk_score=0
- API surface: public=5 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=3 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): bff.services; shared.security.input_sanitizer
- External imports (3): __future__; fastapi; pytest
- Public API names: test_sanitized_payload_converts_security_violation; test_sanitized_payload_returns_sanitized_data; test_validated_branch_name_converts_security_violation; test_validated_db_name_converts_value_error_to_http_400; test_validated_db_name_returns_valid_value

### `backend/tests/unit/services/test_instance_index_rebuild_service.py`
- Module summary: Tests for instance index rebuild service.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=191 | code_lines=146 | risk_score=0
- API surface: public=9 | top-level functions=10 | classes=0 | methods=0
- Runtime signals: async_functions=9 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=10/10 (100%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.core.instance_index_rebuild_service
- External imports (3): __future__; pytest; unittest
- Public API names: test_get_class_counts; test_get_class_counts_empty_index; test_rebuild_alias_swap_failure; test_rebuild_success_concrete_index; test_rebuild_success_no_existing_data; test_rebuild_success_with_existing_alias; test_reindex_from_source; test_resolve_alias_targets; test_resolve_alias_targets_no_alias

### `backend/tests/unit/services/test_instances_service_projection_mode.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=173 | code_lines=144 | risk_score=0
- API surface: public=5 | top-level functions=5 | classes=2 | methods=4
- Runtime signals: async_functions=8 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/2 (0%) | methods=0/4 (0%)
- Internal imports (2): bff.services.instances_service; shared.config.search_config
- External imports (4): __future__; fastapi; pytest; typing
- Public API names: test_get_instance_es_error_fails_closed_without_fallback; test_get_instance_missing_doc_returns_404_without_fallback; test_list_instances_es_error_fails_closed_without_fallback; test_sample_values_es_error_fails_closed; test_sample_values_reads_from_es

### `backend/tests/unit/services/test_lakefs_branch_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=39 | code_lines=29 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=1 | methods=2
- Runtime signals: async_functions=4 | try=1 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (2): shared.services.storage.lakefs_branch_utils; shared.services.storage.lakefs_client
- External imports (1): pytest
- Public API names: test_ensure_lakefs_branch_creates_branch; test_ensure_lakefs_branch_ignores_conflict; test_ensure_lakefs_branch_requires_client

### `backend/tests/unit/services/test_lineage_store_es_alias_normalization.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=99 | code_lines=73 | risk_score=0
- API surface: public=7 | top-level functions=7 | classes=1 | methods=3
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/7 (0%) | classes=0/1 (0%) | methods=0/3 (0%)
- Internal imports (1): shared.services.registries.lineage_store
- External imports (4): __future__; datetime; pytest; uuid
- Public API names: test_canonicalize_edge_type_keeps_canonical_value; test_canonicalize_edge_type_rejects_deprecated_alias; test_canonicalize_edge_type_rejects_deprecated_s3_alias; test_infer_branch_from_es_node_id; test_parse_node_id_rejects_deprecated_es_artifact_kind; test_record_link_accepts_canonical_es_ids_and_edge_type; test_record_link_rejects_deprecated_es_ids_and_edge_type

### `backend/tests/unit/services/test_lineage_store_latest_projection_edges.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=130 | code_lines=105 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=3 | methods=7
- Runtime signals: async_functions=6 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/3 (0%) | methods=0/7 (0%)
- Internal imports (1): shared.services.registries.lineage_store
- External imports (4): __future__; datetime; pytest; typing
- Public API names: test_get_latest_edges_for_projections_returns_empty_for_blank_names; test_get_latest_edges_for_projections_returns_latest_writer_context; test_get_latest_edges_from_returns_latest_per_parent_node

### `backend/tests/unit/services/test_llm_gateway_resilience.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=296 | code_lines=238 | risk_score=0
- API surface: public=7 | top-level functions=7 | classes=1 | methods=0
- Runtime signals: async_functions=18 | try=0 | raise=5 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/7 (0%) | classes=0/1 (0%) | methods=0/0 (n/a)
- Internal imports (2): shared.services.agent.llm_gateway; shared.utils.llm_safety
- External imports (4): __future__; json; pydantic; pytest
- Public API names: test_llm_gateway_cache_key_is_partition_scoped; test_llm_gateway_circuit_breaker_opens; test_llm_gateway_native_tool_calling_falls_back_on_unsupported; test_llm_gateway_provider_policy_can_disable_cache; test_llm_gateway_provider_policy_can_set_actor_isolation_for_anthropic; test_llm_gateway_provider_policy_can_set_no_store_and_partition_isolation; test_llm_gateway_retries_on_http_5xx

### `backend/tests/unit/services/test_llm_quota.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=74 | code_lines=59 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=2 | methods=3
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/2 (0%) | methods=0/3 (0%)
- Internal imports (1): shared.services.agent.llm_quota
- External imports (3): __future__; datetime; pytest
- Public API names: test_llm_quota_consumes_for_tenant_and_user; test_llm_quota_is_noop_without_policy; test_llm_quota_raises_when_denied

### `backend/tests/unit/services/test_object_type_meta_resolver.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=60 | code_lines=44 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=1 | methods=2
- Runtime signals: async_functions=4 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (1): shared.services.core.object_type_meta_resolver
- External imports (1): pytest
- Public API names: test_object_type_meta_resolver_blank_id_uses_default; test_object_type_meta_resolver_on_error_returns_default_and_caches; test_object_type_meta_resolver_parses_and_caches

### `backend/tests/unit/services/test_objectify_dag_service.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=71 | code_lines=54 | risk_score=0
- API surface: public=3 | top-level functions=4 | classes=1 | methods=1
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/1 (0%) | methods=0/1 (0%)
- Internal imports (2): bff.schemas.objectify_requests; bff.services.objectify_dag_service
- External imports (5): __future__; dataclasses; pytest; types; typing
- Public API names: test_wait_for_objectify_submitted_allows_dataset_primary_completed_without_commands; test_wait_for_objectify_submitted_defaults_to_dataset_primary_when_report_missing; test_wait_for_objectify_submitted_requires_commands_for_submitted_status

### `backend/tests/unit/services/test_objectify_delta_utils.py`
- Module summary: Unit tests for Objectify Delta Utils.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=240 | code_lines=185 | risk_score=0
- API surface: public=4 | top-level functions=0 | classes=4 | methods=18
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/4 (0%) | methods=18/18 (100%)
- Internal imports (1): shared.services.pipeline.objectify_delta_utils
- External imports (3): __future__; datetime; pytest
- Public API names: TestCreateDeltaComputerForMappingSpec; TestDeltaResult; TestObjectifyDeltaComputer; TestWatermarkState

### `backend/tests/unit/services/test_occ_patterns.py`
- Module summary: Unit tests for Optimistic Concurrency Control (OCC) patterns.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=185 | code_lines=156 | risk_score=0
- API surface: public=4 | top-level functions=0 | classes=4 | methods=8
- Runtime signals: async_functions=0 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=1/4 (25%) | methods=8/8 (100%)
- Internal imports (1): shared.services.registries.objectify_registry
- External imports (2): __future__; pytest
- Public API names: TestOCCConflictError; TestOCCPatternIntegration; TestObjectifyJobRecordOCC; TestOntologyMappingSpecRecordOCC

### `backend/tests/unit/services/test_oms_error_policy.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=52 | code_lines=39 | risk_score=0
- API surface: public=3 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.services.oms_error_policy
- External imports (5): __future__; fastapi; httpx; logging; pytest
- Public API names: test_raise_oms_boundary_exception_maps_generic_to_500; test_raise_oms_boundary_exception_maps_value_error_to_400; test_raise_oms_boundary_exception_with_custom_http_status_detail

### `backend/tests/unit/services/test_ontology_class_id_service.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=43 | code_lines=30 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): bff.services; shared.security.input_sanitizer
- External imports (3): __future__; fastapi; pytest
- Public API names: test_resolve_or_generate_class_id_converts_security_violation; test_resolve_or_generate_class_id_converts_validation_error; test_resolve_or_generate_class_id_generates_when_missing; test_resolve_or_generate_class_id_returns_validated_id

### `backend/tests/unit/services/test_ontology_deployment_registry_template_method.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=125 | code_lines=98 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=1 | methods=4
- Runtime signals: async_functions=10 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/1 (0%) | methods=0/4 (0%)
- Internal imports (3): oms.services.ontology_deploy_outbox_store; oms.services.ontology_deployment_registry; oms.services.ontology_deployment_registry_v2
- External imports (4): __future__; datetime; pytest; typing
- Public API names: test_registry_ensure_schema_builds_indexes_for_v1; test_registry_ensure_schema_builds_indexes_for_v2; test_registry_purge_outbox_uses_registry_table; test_registry_v2_claim_batch_normalizes_json_payload

### `backend/tests/unit/services/test_ontology_interface_contract.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=27 | code_lines=23 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): oms.services.ontology_interface_contract; shared.models.ontology
- External imports (0): not documented
- Public API names: test_interface_contract_missing_interface_is_reported; test_interface_contract_missing_property_is_reported

### `backend/tests/unit/services/test_ontology_linter_pk_branching.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=98 | code_lines=80 | risk_score=0
- API surface: public=3 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.models.ontology; shared.services.core.ontology_linter
- External imports (2): __future__; os
- Public API names: test_linter_allows_implicit_pk_on_dev_branch; test_linter_blocks_implicit_pk_on_protected_branch; test_linter_requires_explicit_title_key_when_disabled

### `backend/tests/unit/services/test_ontology_occ_guard_service.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=82 | code_lines=60 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=1 | methods=3
- Runtime signals: async_functions=6 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/1 (0%) | methods=0/3 (0%)
- Internal imports (1): bff.services.ontology_occ_guard_service
- External imports (2): __future__; pytest
- Public API names: test_fetch_branch_head_commit_id_is_disabled_in_foundry_profile; test_resolve_branch_head_commit_with_bootstrap_is_disabled; test_resolve_expected_head_commit_prefers_explicit_value; test_resolve_expected_head_commit_returns_none_without_legacy_lookup

### `backend/tests/unit/services/test_ontology_resource_service_versioning.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=144 | code_lines=119 | risk_score=0
- API surface: public=5 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=6 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): oms.services.ontology_resources
- External imports (2): __future__; pytest
- Public API names: test_get_resource_falls_back_to_deployed_target_branch; test_materialize_commit_snapshot_delegates_to_promote; test_normalize_branch_for_write_strips_branch_prefix; test_payload_to_document_increments_version_and_rev_from_existing; test_payload_to_document_sets_initial_version_and_rev

### `backend/tests/unit/services/test_ontology_resource_validator.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=256 | code_lines=214 | risk_score=0
- API surface: public=19 | top-level functions=19 | classes=1 | methods=2
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/19 (0%) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (1): oms.services.ontology_resource_validator
- External imports (1): pytest
- Public API names: test_action_type_accepts_template_v2_implementation; test_action_type_datasource_derived_allows_missing_permission_policy; test_action_type_rejects_invalid_permission_model; test_action_type_rejects_invalid_validation_rules; test_action_type_rejects_non_boolean_edits_beyond_actions; test_action_type_rejects_unsafe_submission_criteria_expression; test_action_type_requires_input_schema_and_policy; test_function_requires_expression_and_return_type_ref; test_link_type_invalid_predicate_is_reported; test_link_type_missing_refs_are_reported; test_object_type_requires_pk_spec_and_backing_source; test_permission_policy_rejects_legacy_roles_alias (+7 more)

### `backend/tests/unit/services/test_ontology_router_helpers.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=117 | code_lines=102 | risk_score=0
- API surface: public=5 | top-level functions=5 | classes=1 | methods=2
- Runtime signals: async_functions=5 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (2): oms.routers.ontology; shared.models.ontology
- External imports (1): pytest
- Public API names: test_apply_shared_properties_merges_and_tracks_duplicates; test_collect_interface_issues_reports_missing_property; test_extract_group_refs_dedupes; test_validate_group_refs_reports_missing; test_validate_value_type_refs_detects_base_type_mismatch

### `backend/tests/unit/services/test_ontology_value_type_immutability.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=32 | code_lines=19 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): oms.routers.ontology_extensions
- External imports (2): fastapi; pytest
- Public API names: test_value_type_immutability_allows_same_spec; test_value_type_immutability_blocks_base_type_change; test_value_type_immutability_blocks_constraint_change

### `backend/tests/unit/services/test_outbox_runtime.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=196 | code_lines=152 | risk_score=0
- API surface: public=9 | top-level functions=9 | classes=0 | methods=0
- Runtime signals: async_functions=21 | try=0 | raise=5 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/9 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.events.outbox_runtime
- External imports (5): __future__; asyncio; datetime; logging; pytest
- Public API names: test_build_outbox_worker_id_prefers_configured_value; test_build_outbox_worker_id_uses_service_and_hostname; test_flush_outbox_until_empty_keeps_primary_error_when_close_also_fails; test_flush_outbox_until_empty_propagates_primary_error_without_close; test_flush_outbox_until_empty_raises_close_error_when_no_primary_error; test_flush_outbox_until_empty_stops_and_closes; test_maybe_purge_with_interval_executes_and_logs; test_run_outbox_poll_loop_raises_close_error_when_no_primary_error; test_run_outbox_poll_loop_runs_flush_purge_and_close

### `backend/tests/unit/services/test_output_plugins.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=237 | code_lines=195 | risk_score=0
- API surface: public=20 | top-level functions=20 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/20 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline.output_plugins
- External imports (2): __future__; pytest
- Public API names: test_normalize_output_kind_rejects_unknown_kind; test_normalize_output_kind_supports_legacy_aliases; test_resolve_ontology_output_semantics_exposes_required_columns_for_link; test_resolve_output_kind_reports_alias_usage; test_validate_output_payload_dataset_accepts_full_metadata; test_validate_output_payload_dataset_has_no_required_fields; test_validate_output_payload_dataset_rejects_partitioned_csv; test_validate_output_payload_dataset_rejects_partitioned_json; test_validate_output_payload_dataset_rejects_unsupported_output_format; test_validate_output_payload_dataset_requires_pk_for_append_only_new_rows; test_validate_output_payload_dataset_requires_post_filtering_for_snapshot_remove; test_validate_output_payload_geotemporal_accepts_camel_case (+8 more)

### `backend/tests/unit/services/test_pipeline_advanced_transforms.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=466 | code_lines=437 | risk_score=0
- API surface: public=11 | top-level functions=11 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/11 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): shared.services.pipeline.pipeline_definition_validator; shared.services.pipeline.pipeline_plan_builder; shared.services.pipeline.pipeline_transform_spec
- External imports (2): __future__; pytest
- Public API names: test_add_split_expands_to_true_false_filter_nodes; test_builder_helpers_add_geospatial_pattern_mining_stream_join; test_validate_pipeline_definition_accepts_advanced_transforms; test_validate_pipeline_definition_rejects_dynamic_stream_join_without_event_time_fields; test_validate_pipeline_definition_rejects_invalid_stream_join_strategy; test_validate_pipeline_definition_rejects_invalid_stream_join_time_direction; test_validate_pipeline_definition_rejects_left_lookup_with_streaming_right_input; test_validate_pipeline_definition_rejects_left_lookup_with_transformed_right_input; test_validate_pipeline_definition_rejects_static_non_left_join_type; test_validate_pipeline_definition_rejects_static_with_streaming_right_input; test_validate_pipeline_definition_rejects_static_with_transformed_right_input

### `backend/tests/unit/services/test_pipeline_agent_allowed_tools.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=52 | code_lines=42 | risk_score=0
- API surface: public=2 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (5): __future__; ast; pathlib; pytest; re
- Public API names: test_agent_allowed_tools_cover_pipeline_plan_tools; test_agent_allowed_tools_include_advanced_plan_tools

### `backend/tests/unit/services/test_pipeline_claim_refuter.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=410 | code_lines=386 | risk_score=0
- API surface: public=10 | top-level functions=10 | classes=0 | methods=0
- Runtime signals: async_functions=10 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/10 (10%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.models.pipeline_plan; shared.services.pipeline.pipeline_claim_refuter
- External imports (2): __future__; pytest
- Public API names: test_refuter_cast_lossless_allows_strip_leading_zeros; test_refuter_cast_lossless_detects_leading_zero_loss; test_refuter_cast_success_finds_parse_failure; test_refuter_fails_open_when_unable_to_execute_preview; test_refuter_filter_min_retain_rate_finds_low_retention; test_refuter_filter_only_nulls_finds_removed_non_null_row; test_refuter_fk_hard_is_downgraded_to_soft; test_refuter_join_functional_right_detects_duplicate_right_matches_left; test_refuter_pk_duplicate_is_hard_failure; test_refuter_union_row_lossless_finds_missing_input_row

### `backend/tests/unit/services/test_pipeline_cleansing_upstream_push.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=51 | code_lines=44 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): bff.services.pipeline_cleansing_utils; shared.services.pipeline.pipeline_preflight_utils
- External imports (2): __future__; pytest
- Public API names: test_cleansing_upstream_push_common_ancestor

### `backend/tests/unit/services/test_pipeline_control_plane_events.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=60 | code_lines=44 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=6 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline
- External imports (2): __future__; pytest
- Public API names: test_control_plane_event_emits_with_topic; test_control_plane_events_always_on

### `backend/tests/unit/services/test_pipeline_definition_utils_columns.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=13 | code_lines=7 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline.pipeline_definition_utils
- External imports (0): not documented
- Public API names: test_normalize_expectation_columns_with_csv; test_normalize_expectation_columns_with_list; test_normalize_expectation_columns_with_none

### `backend/tests/unit/services/test_pipeline_definition_validator.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=375 | code_lines=334 | risk_score=0
- API surface: public=19 | top-level functions=20 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/20 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.services.pipeline.pipeline_definition_validator; shared.services.pipeline.pipeline_transform_spec
- External imports (1): __future__
- Public API names: test_validate_pipeline_definition_allows_batch_kafka_without_checkpoint; test_validate_pipeline_definition_allows_kafka_avro_with_schema_registry_reference; test_validate_pipeline_definition_detects_missing_edge_nodes; test_validate_pipeline_definition_normalizes_metadata_fields_to_columns; test_validate_pipeline_definition_rejects_invalid_dataset_output_metadata; test_validate_pipeline_definition_rejects_kafka_avro_with_latest_registry_version; test_validate_pipeline_definition_rejects_kafka_avro_with_missing_registry_version; test_validate_pipeline_definition_rejects_kafka_avro_without_schema_or_registry; test_validate_pipeline_definition_rejects_kafka_json_without_schema; test_validate_pipeline_definition_rejects_streaming_non_kafka_external_input; test_validate_pipeline_definition_rejects_streaming_without_checkpoint_location; test_validate_pipeline_definition_reports_missing_columns_for_normalize (+7 more)

### `backend/tests/unit/services/test_pipeline_execution_service_dataset_policy.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=80 | code_lines=71 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): bff.services.pipeline_execution_service; shared.services.pipeline.dataset_output_semantics
- External imports (2): __future__; pytest
- Public API names: test_dataset_write_policy_hash_consistent_for_definition_contract; test_resolve_output_contract_from_definition_merges_declared_metadata

### `backend/tests/unit/services/test_pipeline_executor_csv_parsing.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=143 | code_lines=115 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=4 | methods=8
- Runtime signals: async_functions=7 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/4 (0%) | methods=0/8 (0%)
- Internal imports (1): shared.services.pipeline.pipeline_executor
- External imports (4): __future__; dataclasses; pytest; typing
- Public API names: test_executor_parses_quoted_csv_headers_and_joins_correctly

### `backend/tests/unit/services/test_pipeline_executor_function_categories.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=134 | code_lines=113 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=3 | methods=4
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/1 (100%) | classes=0/3 (0%) | methods=0/4 (0%)
- Internal imports (1): shared.services.pipeline.pipeline_executor
- External imports (4): __future__; dataclasses; pytest; typing
- Public API names: test_function_categories_row_aggregation_generator_are_distinct_and_work

### `backend/tests/unit/services/test_pipeline_executor_normalize.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=101 | code_lines=83 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=3 | methods=4
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/3 (0%) | methods=0/4 (0%)
- Internal imports (1): shared.services.pipeline.pipeline_executor
- External imports (4): __future__; dataclasses; pytest; typing
- Public API names: test_normalize_transform_trims_and_nulls

### `backend/tests/unit/services/test_pipeline_executor_preview.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=753 | code_lines=682 | risk_score=0
- API surface: public=11 | top-level functions=11 | classes=3 | methods=4
- Runtime signals: async_functions=14 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/11 (0%) | classes=0/3 (0%) | methods=0/4 (0%)
- Internal imports (1): shared.services.pipeline.pipeline_executor
- External imports (4): __future__; dataclasses; pytest; typing
- Public API names: test_executor_compute_equals_is_treated_as_comparison_when_lhs_exists; test_executor_compute_structured_target_column_overwrites_existing; test_executor_preview_supports_node_level_preview_and_row_count; test_executor_stream_join_dynamic_applies_cache_expiration_window; test_executor_stream_join_dynamic_emits_unmatched_rows_as_outer_join; test_executor_stream_join_dynamic_selects_single_best_match_per_left_row; test_executor_stream_join_dynamic_supports_forward_direction; test_executor_stream_join_dynamic_uses_backward_time_direction_by_default; test_executor_stream_join_left_lookup_forces_left_join_semantics; test_executor_stream_join_left_lookup_picks_latest_right_row_per_key_without_event_time; test_executor_stream_join_static_forces_left_join_semantics_even_when_full_requested

### `backend/tests/unit/services/test_pipeline_executor_transform_safety.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=79 | code_lines=56 | risk_score=0
- API surface: public=6 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline.pipeline_executor
- External imports (1): pytest
- Public API names: test_cross_join_explicit_opt_in_produces_cartesian_product_and_preserves_column_mapping; test_join_allow_cross_join_requires_cross_type; test_join_requires_keys_by_default; test_union_common_only_keeps_only_shared_columns; test_union_pad_missing_nulls_includes_superset_columns; test_union_strict_raises_on_schema_mismatch

### `backend/tests/unit/services/test_pipeline_expectations_and_contracts.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=53 | code_lines=36 | risk_score=0
- API surface: public=5 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.services.pipeline.pipeline_executor; shared.services.pipeline.pipeline_validation_utils
- External imports (1): pytest
- Public API names: test_expectations_row_count_bounds; test_expectations_unique_detects_duplicate_composite_key; test_expectations_unique_detects_duplicate_primary_key; test_schema_contract_missing_required_column_is_reported; test_schema_contract_type_mismatch_is_reported

### `backend/tests/unit/services/test_pipeline_join_evaluator.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=155 | code_lines=132 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=3 | methods=4
- Runtime signals: async_functions=5 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/3 (0%) | methods=0/4 (0%)
- Internal imports (1): bff.services.pipeline_join_evaluator
- External imports (4): __future__; dataclasses; pytest; typing
- Public API names: test_join_evaluator_aligns_inputs_to_join_keys; test_join_evaluator_reports_coverage_and_explosion

### `backend/tests/unit/services/test_pipeline_kafka_avro_schema.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=111 | code_lines=92 | risk_score=0
- API surface: public=7 | top-level functions=7 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/7 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline.pipeline_kafka_avro
- External imports (2): __future__; pytest
- Public API names: test_fetch_kafka_avro_schema_from_registry_parses_schema_payload; test_resolve_inline_avro_schema_prefers_inline_metadata; test_resolve_kafka_avro_schema_registry_reference_supports_options_aliases; test_validate_kafka_avro_schema_rejects_invalid_registry_version; test_validate_kafka_avro_schema_rejects_latest_registry_version_explicitly; test_validate_kafka_avro_schema_rejects_missing_registry_subject; test_validate_kafka_avro_schema_requires_inline_or_registry

### `backend/tests/unit/services/test_pipeline_ops_preflight.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=159 | code_lines=133 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=11 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): bff.routers
- External imports (4): __future__; fastapi; pytest; types
- Public API names: test_run_pipeline_preflight_blocks_missing_udf_reference; test_run_pipeline_preflight_blocks_unpinned_udf_version; test_run_pipeline_preflight_fail_closed_raises_http_exception; test_run_pipeline_preflight_fail_open_returns_warning_payload

### `backend/tests/unit/services/test_pipeline_plan_builder.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=529 | code_lines=435 | risk_score=0
- API surface: public=33 | top-level functions=33 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/33 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline.pipeline_plan_builder
- External imports (2): __future__; pytest
- Public API names: test_add_cast_requires_column_and_type; test_add_edge_is_idempotent; test_add_explode_builds_metadata; test_add_external_input_creates_input_node_without_dataset_selection; test_add_input_and_output_wires_edges; test_add_input_supports_read_config; test_add_join_accepts_hints_and_broadcast_flags; test_add_join_rejects_cross_join; test_add_join_requires_two_inputs_and_keys; test_add_output_normalizes_dataset_write_metadata_aliases; test_add_output_persists_output_metadata_into_outputs_entry; test_add_output_warns_when_legacy_alias_kind_used (+21 more)

### `backend/tests/unit/services/test_pipeline_preflight_dataset_output.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=1040 | code_lines=967 | risk_score=0
- API surface: public=22 | top-level functions=22 | classes=0 | methods=0
- Runtime signals: async_functions=35 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/22 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline
- External imports (3): __future__; pytest; types
- Public API names: test_compute_pipeline_preflight_accepts_valid_dataset_write_metadata; test_compute_pipeline_preflight_allows_batch_kafka_without_checkpoint; test_compute_pipeline_preflight_allows_kafka_avro_with_schema_registry_reference; test_compute_pipeline_preflight_blocks_geotemporal_missing_required_columns; test_compute_pipeline_preflight_blocks_invalid_ontology_relationship_spec_type; test_compute_pipeline_preflight_blocks_kafka_avro_with_latest_registry_version; test_compute_pipeline_preflight_blocks_kafka_avro_with_missing_registry_version; test_compute_pipeline_preflight_blocks_kafka_avro_without_schema_or_registry; test_compute_pipeline_preflight_blocks_kafka_json_without_schema; test_compute_pipeline_preflight_blocks_left_lookup_with_streaming_right_input; test_compute_pipeline_preflight_blocks_left_lookup_with_transformed_right_input; test_compute_pipeline_preflight_blocks_ontology_link_missing_required_columns (+10 more)

### `backend/tests/unit/services/test_pipeline_preview_inspector.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=93 | code_lines=76 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.services.pipeline.pipeline_preview_inspector; shared.services.pipeline.pipeline_profiler
- External imports (2): __future__; pytest
- Public API names: test_preview_inspector_no_cleansing_needed_for_clean_columns; test_preview_inspector_suggests_dedupe_for_duplicate_rows; test_preview_inspector_suggests_normalize_and_casts; test_preview_inspector_suggests_regex_replace_for_phone

### `backend/tests/unit/services/test_pipeline_profiler.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=49 | code_lines=42 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline.pipeline_profiler
- External imports (1): pytest
- Public API names: test_compute_column_stats_numeric_min_max_mean_from_mixed_values; test_compute_column_stats_string_column_counts_null_empty_whitespace_and_top_values

### `backend/tests/unit/services/test_pipeline_proposal_bundle_ontology_ref.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=97 | code_lines=75 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=4 | methods=6
- Runtime signals: async_functions=7 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/4 (0%) | methods=0/6 (0%)
- Internal imports (1): bff.services.pipeline_proposal_service
- External imports (4): __future__; dataclasses; pytest; typing
- Public API names: test_build_proposal_bundle_adds_ontology_ref_fallback_when_commit_missing; test_build_proposal_bundle_preserves_ontology_ref_when_present

### `backend/tests/unit/services/test_pipeline_registry_branch_idempotency.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=112 | code_lines=92 | risk_score=0
- API surface: public=1 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=7 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/2 (50%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.registries.pipeline_registry
- External imports (4): __future__; datetime; pytest; types
- Public API names: test_create_branch_is_idempotent_when_db_unique_violation_races

### `backend/tests/unit/services/test_pipeline_registry_commit_predicate_fallback.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=156 | code_lines=121 | risk_score=0
- API surface: public=1 | top-level functions=2 | classes=2 | methods=5
- Runtime signals: async_functions=11 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/2 (50%) | classes=0/2 (0%) | methods=0/5 (0%)
- Internal imports (2): shared.services.registries.pipeline_registry; shared.services.storage.lakefs_client
- External imports (4): __future__; datetime; pytest; types
- Public API names: test_add_version_handles_lakefs_predicate_failed_by_resolving_head_commit

### `backend/tests/unit/services/test_pipeline_scheduler_control_plane_events.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=47 | code_lines=33 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=2 | methods=3
- Runtime signals: async_functions=4 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/2 (0%) | methods=0/3 (0%)
- Internal imports (1): shared.services.pipeline.pipeline_scheduler
- External imports (3): __future__; datetime; pytest
- Public API names: test_scheduler_emits_ignored_event

### `backend/tests/unit/services/test_pipeline_scheduler_ignored_runs.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=331 | code_lines=271 | risk_score=0
- API surface: public=6 | top-level functions=6 | classes=3 | methods=7
- Runtime signals: async_functions=11 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/6 (0%) | classes=0/3 (0%) | methods=0/7 (0%)
- Internal imports (1): shared.services.pipeline.pipeline_scheduler
- External imports (5): __future__; dataclasses; datetime; pytest; typing
- Public API names: test_scheduler_does_not_trigger_dependency_only_when_pipeline_is_newer_than_deps; test_scheduler_records_ignored_when_schedule_due_but_dependencies_up_to_date; test_scheduler_records_ignored_when_schedule_due_but_dependency_not_satisfied; test_scheduler_triggers_cron_schedule_when_matches; test_scheduler_triggers_interval_schedule_when_due; test_scheduler_triggers_when_dependency_is_newer_than_pipeline_build

### `backend/tests/unit/services/test_pipeline_scheduler_validation.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=72 | code_lines=57 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=1 | methods=1
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/1 (0%) | methods=0/1 (0%)
- Internal imports (1): shared.services.pipeline.pipeline_scheduler
- External imports (1): pytest
- Public API names: test_cron_expression_validation_matches_supported_subset; test_dependencies_satisfied_raises_when_dependency_pipeline_missing; test_normalize_dependencies_accepts_pipeline_id_variants; test_normalize_dependencies_reports_invalid_entries

### `backend/tests/unit/services/test_pipeline_schema_casts.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=42 | code_lines=32 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline.pipeline_schema_casts
- External imports (1): __future__
- Public API names: test_extract_schema_casts_extracts_from_properties; test_extract_schema_casts_falls_back_to_fields; test_extract_schema_casts_parses_columns_dicts_and_strings; test_extract_schema_casts_returns_empty_for_non_dict_inputs

### `backend/tests/unit/services/test_pipeline_task_spec_policy.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=58 | code_lines=46 | risk_score=0
- API surface: public=4 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): shared.models.pipeline_plan; shared.models.pipeline_task_spec; shared.services.pipeline.pipeline_task_spec_policy
- External imports (1): __future__
- Public API names: test_clamp_task_spec_disables_join_for_single_dataset; test_policy_rejects_advanced_transform_when_disallowed; test_policy_rejects_join_when_disallowed; test_policy_rejects_report_only_scope

### `backend/tests/unit/services/test_pipeline_type_inference.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=45 | code_lines=31 | risk_score=0
- API surface: public=6 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline.pipeline_type_inference
- External imports (1): __future__
- Public API names: test_common_join_key_type_biases_to_string_on_mismatch; test_infer_xsd_type_with_confidence_boolean; test_infer_xsd_type_with_confidence_datetime; test_infer_xsd_type_with_confidence_decimal; test_infer_xsd_type_with_confidence_falls_back_to_string_for_mixed_values; test_infer_xsd_type_with_confidence_integer

### `backend/tests/unit/services/test_pipeline_udf_runtime.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=189 | code_lines=145 | risk_score=0
- API surface: public=12 | top-level functions=12 | classes=2 | methods=4
- Runtime signals: async_functions=7 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/12 (0%) | classes=0/2 (0%) | methods=0/4 (0%)
- Internal imports (1): shared.services.pipeline.pipeline_udf_runtime
- External imports (1): pytest
- Public API names: test_compile_row_udf_accepts_escaped_newline_source; test_compile_row_udf_accepts_flat_map_output; test_compile_row_udf_accepts_simple_transform; test_compile_row_udf_rejects_imports; test_compile_row_udf_rejects_loops; test_compile_row_udf_rejects_private_attribute_access; test_compile_row_udf_rejects_top_level_statements; test_resolve_udf_reference_allows_latest_when_version_pinning_disabled; test_resolve_udf_reference_rejects_inline_code_even_when_reference_flag_disabled; test_resolve_udf_reference_requires_udf_id_when_policy_enabled; test_resolve_udf_reference_requires_version_when_pinning_enabled; test_resolve_udf_reference_uses_registry_and_cache_key

### `backend/tests/unit/services/test_pipeline_udf_versioning.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=151 | code_lines=120 | risk_score=1
- API surface: public=1 | top-level functions=2 | classes=3 | methods=4
- Runtime signals: async_functions=4 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/2 (50%) | classes=0/3 (0%) | methods=0/4 (0%)
- Internal imports (2): shared.services.pipeline.pipeline_executor; shared.services.registries.pipeline_registry
- External imports (5): __future__; dataclasses; os; pytest; typing
- Public API names: test_udf_can_be_created_reused_and_version_upgraded

### `backend/tests/unit/services/test_pipeline_unit_test_runner.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=125 | code_lines=109 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=1 | methods=3
- Runtime signals: async_functions=5 | try=0 | raise=3 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=2/2 (100%) | classes=0/1 (0%) | methods=0/3 (0%)
- Internal imports (1): shared.services.pipeline.pipeline_executor
- External imports (1): pytest
- Public API names: test_pipeline_unit_tests_define_inputs_and_expected_outputs; test_pipeline_unit_tests_report_diffs_for_breaking_changes

### `backend/tests/unit/services/test_pipeline_value_predicates.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=35 | code_lines=26 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline.pipeline_value_predicates
- External imports (1): datetime
- Public API names: test_is_bool_like; test_is_datetime_like_iso_only_and_general_modes; test_is_decimal_like_include_int_policy; test_is_int_like

### `backend/tests/unit/services/test_pipeline_worker_diff_handling.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=134 | code_lines=106 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=10 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): pipeline_worker.main
- External imports (4): __future__; pytest; types; unittest
- Public API names: test_list_lakefs_diff_paths_ignores_removed; test_load_input_dataframe_fallback_on_diff_failure; test_load_input_dataframe_removed_only_diff_returns_empty

### `backend/tests/unit/services/test_postgres_schema_registry.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=65 | code_lines=43 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=4 | methods=8
- Runtime signals: async_functions=7 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/4 (0%) | methods=0/8 (0%)
- Internal imports (1): shared.services.registries.postgres_schema_registry
- External imports (2): __future__; pytest
- Public API names: test_health_check_connects_when_pool_missing; test_health_check_returns_false_on_query_failure

### `backend/tests/unit/services/test_projection_consistency.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=123 | code_lines=107 | risk_score=0
- API surface: public=5 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.models.event_envelope; shared.services.core.projection_consistency
- External imports (2): __future__; datetime
- Public API names: test_build_instance_expectations_prefers_higher_sequence_number; test_build_instance_expectations_uses_aggregate_id_fallback; test_evaluate_projection_document_detects_deleted_document_present; test_evaluate_projection_document_reports_missing_document; test_parse_instance_aggregate_id_handles_instance_id_with_colon

### `backend/tests/unit/services/test_projection_position_tracker.py`
- Module summary: Tests for ProjectionPositionTracker.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=202 | code_lines=159 | risk_score=0
- API surface: public=7 | top-level functions=7 | classes=2 | methods=9
- Runtime signals: async_functions=12 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/7 (14%) | classes=1/2 (50%) | methods=0/9 (0%)
- Internal imports (1): shared.services.core.projection_position_tracker
- External imports (3): __future__; pytest; typing
- Public API names: test_compute_lag; test_compute_lag_unhealthy; test_get_position_empty; test_list_positions; test_reset_position; test_update_and_get_position; test_update_position_monotonic

### `backend/tests/unit/services/test_relationship_extractor.py`
- Module summary: Tests for shared relationship extractor.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=145 | code_lines=119 | risk_score=0
- API surface: public=4 | top-level functions=0 | classes=4 | methods=15
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=4/4 (100%) | methods=0/15 (0%)
- Internal imports (1): shared.services.core.relationship_extractor
- External imports (2): __future__; pytest
- Public API names: TestEdgeCases; TestExtractRelationshipsWithOntologyData; TestExtractRelationshipsWithRelMap; TestPatternFallback

### `backend/tests/unit/services/test_relationship_reconciler.py`
- Module summary: Tests for relationship_reconciler_service.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=194 | code_lines=155 | risk_score=0
- API surface: public=6 | top-level functions=6 | classes=1 | methods=6
- Runtime signals: async_functions=8 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/6 (0%) | classes=1/1 (100%) | methods=0/6 (0%)
- Internal imports (1): bff.services.relationship_reconciler_service
- External imports (4): __future__; pytest; typing; unittest
- Public API names: test_bulk_update_relationships; test_collect_relationships_deduplicates; test_collect_relationships_extracts_from_class_defs; test_detect_fk_field_via_pattern; test_reconcile_returns_no_classes_when_oms_empty; test_scan_and_group_fk

### `backend/tests/unit/services/test_schema_drift_detector.py`
- Module summary: Unit tests for Schema Drift Detector service.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=320 | code_lines=267 | risk_score=0
- API surface: public=2 | top-level functions=0 | classes=2 | methods=15
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/2 (0%) | methods=14/15 (93%)
- Internal imports (1): shared.services.core.schema_drift_detector
- External imports (2): __future__; pytest
- Public API names: TestSchemaDriftConfig; TestSchemaDriftDetector

### `backend/tests/unit/services/test_schema_versioning.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=67 | code_lines=50 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.core.schema_versioning
- External imports (2): __future__; pytest
- Public API names: test_schema_registry_register_and_migrate; test_schema_version_parsing_and_comparison; test_schema_versioning_service_event_helpers

### `backend/tests/unit/services/test_sequence_service.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=100 | code_lines=73 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=1 | methods=8
- Runtime signals: async_functions=9 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/1 (0%) | methods=0/8 (0%)
- Internal imports (1): shared.services.core.sequence_service
- External imports (3): __future__; fnmatch; pytest
- Public API names: test_sequence_service_increments_and_caches; test_sequence_service_lists_sequences; test_sequence_service_set_reset_and_batch

### `backend/tests/unit/services/test_standard_dlq_publisher.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=118 | code_lines=92 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=3 | methods=12
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/3 (0%) | methods=0/12 (0%)
- Internal imports (1): shared.services.kafka.dlq_publisher
- External imports (5): __future__; contextlib; json; pytest; typing
- Public API names: test_publish_contextual_dlq_json_uses_synthetic_source_key; test_publish_standard_dlq_builds_payload_and_flushes

### `backend/tests/unit/services/test_storage_service.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=124 | code_lines=93 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=1 | methods=2
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (1): shared.services.storage.storage_service
- External imports (2): datetime; pytest
- Public API names: test_list_command_files_paginates_filters_and_sorts; test_storage_service_does_not_disable_tls_verify_by_default; test_storage_service_respects_explicit_tls_verify_flag; test_storage_service_supports_tls_ca_bundle_path

### `backend/tests/unit/services/test_sync_wrapper_service.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=79 | code_lines=55 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=2 | methods=2
- Runtime signals: async_functions=6 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/2 (0%) | methods=0/2 (0%)
- Internal imports (3): shared.models.commands; shared.models.sync_wrapper; shared.services.core.sync_wrapper_service
- External imports (5): __future__; asyncio; dataclasses; pytest; typing
- Public API names: test_execute_sync_calls_wait; test_wait_for_command_failure; test_wait_for_command_success; test_wait_for_command_timeout

### `backend/tests/unit/services/test_watermark_monitor.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=86 | code_lines=74 | risk_score=1
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=1 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): shared.services.core.watermark_monitor; tests.unit.services.fake_async_redis
- External imports (2): __future__; pytest
- Public API names: test_partition_and_global_watermark_helpers; test_watermark_monitor_metrics_and_alerts

### `backend/tests/unit/services/test_worker_stores_lineage_policy.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service/domain orchestration
- Source footprint: total_lines=66 | code_lines=50 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=1 | methods=2
- Runtime signals: async_functions=5 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (1): shared.services.core
- External imports (2): __future__; pytest
- Public API names: test_initialize_worker_stores_allows_fail_open_override; test_initialize_worker_stores_fails_when_lineage_required

### `backend/tests/unit/utils/__init__.py`
- Module summary: Unit tests for utility modules
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=3 | code_lines=3 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/tests/unit/utils/test_access_policy.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=37 | code_lines=25 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.access_policy
- External imports (0): not documented
- Public API names: test_access_policy_allows_matching_rows; test_access_policy_denies_matching_rows; test_access_policy_masks_columns

### `backend/tests/unit/utils/test_action_audit_policy.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=31 | code_lines=24 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.action_audit_policy
- External imports (0): not documented
- Public API names: test_audit_action_log_input_redacts_keys_recursively; test_audit_action_log_input_truncates_when_exceeds_max_bytes; test_audit_action_log_result_summarizes_large_change_arrays

### `backend/tests/unit/utils/test_action_data_access.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=190 | code_lines=172 | risk_score=0
- API surface: public=6 | top-level functions=6 | classes=1 | methods=2
- Runtime signals: async_functions=7 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/6 (0%) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (1): shared.utils.action_data_access
- External imports (3): __future__; pytest; types
- Public API names: test_evaluate_action_target_data_access_allows_when_policy_missing; test_evaluate_action_target_data_access_attachment_policy_missing_is_unverifiable; test_evaluate_action_target_data_access_denied; test_evaluate_action_target_data_access_edit_denied_by_object_edit_policy; test_evaluate_action_target_data_access_marks_unverifiable_on_registry_error; test_evaluate_action_target_data_access_object_set_policy_enforced_for_link_changes

### `backend/tests/unit/utils/test_action_input_schema.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=38 | code_lines=28 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.action_input_schema
- External imports (1): pytest
- Public API names: test_validate_action_input_rejects_reserved_internal_keys_anywhere; test_validate_action_input_rejects_unknown_fields_by_default; test_validate_action_input_reports_invalid_schema; test_validate_action_input_validates_and_normalizes_object_ref

### `backend/tests/unit/utils/test_action_permission_profile.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=58 | code_lines=45 | risk_score=0
- API surface: public=5 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.action_permission_profile
- External imports (2): __future__; pytest
- Public API names: test_requires_action_data_access_enforcement_for_profile_or_global_flag; test_resolve_action_permission_profile_defaults; test_resolve_action_permission_profile_rejects_invalid_model; test_resolve_action_permission_profile_rejects_non_boolean_edits_flag; test_resolve_action_permission_profile_supports_aliases

### `backend/tests/unit/utils/test_action_runtime_contracts.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=154 | code_lines=133 | risk_score=0
- API surface: public=7 | top-level functions=7 | classes=0 | methods=0
- Runtime signals: async_functions=8 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/7 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.action_runtime_contracts
- External imports (3): __future__; pytest; types
- Public API names: test_build_property_type_map_from_properties_handles_models_and_dicts; test_extract_interfaces_from_metadata_supports_aliases; test_extract_required_action_interfaces_normalizes_prefix_and_dedupes; test_load_action_target_runtime_contract_extracts_metadata_and_properties; test_load_action_target_runtime_contract_reads_object_type_resource_first; test_load_action_target_runtime_contract_returns_none_when_class_missing; test_load_action_target_runtime_contract_returns_none_when_resource_missing

### `backend/tests/unit/utils/test_action_template_engine.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=204 | code_lines=187 | risk_score=0
- API surface: public=7 | top-level functions=7 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/7 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.action_template_engine
- External imports (2): datetime; pytest
- Public API names: test_compile_function_v1_change_shape_and_builtin_math; test_compile_template_v1_change_shape_merges_and_tracks_touched_fields; test_compile_template_v1_rejects_delete_plus_edits_for_same_target; test_compile_template_v1_resolves_refs_and_now; test_compile_template_v1_supports_bulk_targets_from_list; test_compile_template_v2_resolves_if_switch_and_calls; test_template_v1_rejects_v2_directive

### `backend/tests/unit/utils/test_blank_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=16 | code_lines=12 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.blank_utils
- External imports (0): not documented
- Public API names: test_is_blank_value; test_strip_to_none

### `backend/tests/unit/utils/test_canonical_json.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=20 | code_lines=12 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.canonical_json
- External imports (1): datetime
- Public API names: test_canonical_json_dumps_normalizes_datetime_to_utc; test_canonical_json_dumps_sorts_keys_and_is_compact; test_sha256_prefixed_has_expected_prefix

### `backend/tests/unit/utils/test_common_base_response_deprecation.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=24 | code_lines=17 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (3): __future__; logging; warnings
- Public API names: test_base_response_emits_deprecation_once

### `backend/tests/unit/utils/test_dependency_parsing.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=43 | code_lines=37 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (2): pathlib; scripts
- Public API names: test_parse_pyproject_toml_extracts_dependencies; test_parse_requirements_txt_extracts_versions

### `backend/tests/unit/utils/test_deprecation_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=36 | code_lines=26 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): oms.utils
- External imports (2): __future__; pytest
- Public API names: test_deprecated_decorator_sync; test_legacy_and_experimental_decorators

### `backend/tests/unit/utils/test_foundry_page_token.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=31 | code_lines=20 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.foundry_page_token
- External imports (3): __future__; pytest; time
- Public API names: test_foundry_page_token_empty_defaults_to_zero_offset; test_foundry_page_token_rejects_expired_token; test_foundry_page_token_round_trip; test_foundry_page_token_scope_mismatch_rejected

### `backend/tests/unit/utils/test_json_utils_coercion.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=34 | code_lines=19 | risk_score=0
- API surface: public=7 | top-level functions=7 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/7 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.json_utils
- External imports (0): not documented
- Public API names: test_coerce_json_dict_can_disable_parsed_fallback; test_coerce_json_dict_from_dict_and_string; test_coerce_json_dict_wraps_non_dict_json_by_default; test_coerce_json_list_from_list; test_coerce_json_list_from_string_list; test_coerce_json_list_from_wrapped_dict; test_coerce_json_list_wrap_dict_when_requested

### `backend/tests/unit/utils/test_label_mapper_i18n.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=65 | code_lines=44 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.label_mapper
- External imports (1): pytest
- Public API names: test_label_mapper_batch_fallback_returns_best_available; test_label_mapper_detects_language_for_string_and_falls_back; test_label_mapper_supports_language_map_and_reverse_lookup

### `backend/tests/unit/utils/test_llm_safety.py`
- Module summary: Tests for shared/utils/llm_safety.py
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=287 | code_lines=191 | risk_score=0
- API surface: public=7 | top-level functions=3 | classes=4 | methods=29
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=1/3 (33%) | classes=0/4 (0%) | methods=0/29 (0%)
- Internal imports (1): shared.utils.llm_safety
- External imports (2): __future__; pytest
- Public API names: TestBuildColumnSemanticObservations; TestBuildRelationshipObservations; TestDetectValuePattern; TestExtractColumnValuePatterns; test_mask_pii_dict; test_mask_pii_text_masks_email; test_mask_pii_text_preserves_uuid

### `backend/tests/unit/utils/test_log_rotation.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=59 | code_lines=41 | risk_score=0
- API surface: public=2 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.log_rotation
- External imports (3): __future__; os; pathlib
- Public API names: test_compress_and_cleanup; test_rotate_and_limit_logs

### `backend/tests/unit/utils/test_number_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=15 | code_lines=9 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.number_utils
- External imports (0): not documented
- Public API names: test_to_int_or_none_with_invalid_values; test_to_int_or_none_with_none; test_to_int_or_none_with_valid_values

### `backend/tests/unit/utils/test_ontology_stamp.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=21 | code_lines=14 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): oms.utils.ontology_stamp
- External imports (1): __future__
- Public API names: test_merge_ontology_stamp_fills_missing; test_merge_ontology_stamp_prefers_existing

### `backend/tests/unit/utils/test_ontology_version.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=91 | code_lines=67 | risk_score=0
- API surface: public=5 | top-level functions=5 | classes=3 | methods=4
- Runtime signals: async_functions=9 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/3 (0%) | methods=0/4 (0%)
- Internal imports (1): shared.utils.ontology_version
- External imports (2): __future__; pytest
- Public API names: test_resolve_ontology_version_falls_back_to_ref_on_source_failures; test_resolve_ontology_version_ignores_branch_info_source; test_resolve_ontology_version_ignores_branch_list_source; test_resolve_ontology_version_ignores_legacy_like_source; test_resolve_ontology_version_without_source_returns_ref_only

### `backend/tests/unit/utils/test_principal_policy.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=45 | code_lines=31 | risk_score=0
- API surface: public=7 | top-level functions=7 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/7 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.principal_policy
- External imports (0): not documented
- Public API names: test_build_principal_tags_defaults_to_user_prefix; test_build_principal_tags_emits_typed_principal; test_policy_allows_matches_typed_principal; test_policy_fails_closed_for_unsupported_legacy_fields_even_with_principals; test_policy_fails_closed_for_unsupported_legacy_policy_shapes; test_policy_rejects_legacy_roles_alias; test_policy_rejects_legacy_roles_alias_even_for_deny_effect

### `backend/tests/unit/utils/test_pythonpath_setup.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=58 | code_lines=42 | risk_score=2
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils
- External imports (5): __future__; os; pathlib; pytest; sys
- Public API names: test_configure_python_environment_success; test_detect_backend_directory_with_markers; test_setup_pythonpath_invalid_directory; test_setup_pythonpath_updates_env

### `backend/tests/unit/utils/test_resource_rid.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=25 | code_lines=16 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.resource_rid
- External imports (0): not documented
- Public API names: test_format_resource_rid_always_includes_revision; test_parse_metadata_rev_defaults_to_one; test_parse_metadata_rev_parses_int; test_strip_rid_revision_handles_prefixed_and_unprefixed

### `backend/tests/unit/utils/test_safe_bool_expression.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=64 | code_lines=41 | risk_score=0
- API surface: public=11 | top-level functions=11 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/11 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.safe_bool_expression
- External imports (1): pytest
- Public API names: test_safe_eval_bool_expression_errors_on_non_boolean_result; test_safe_eval_bool_expression_errors_on_not_non_boolean_operand; test_safe_eval_bool_expression_errors_on_unknown_identifier; test_safe_eval_bool_expression_rejects_calls; test_safe_eval_bool_expression_rejects_non_constant_subscript; test_safe_eval_bool_expression_rejects_private_attribute_access; test_safe_eval_bool_expression_supports_attribute_compare; test_safe_eval_bool_expression_supports_boolean_ops; test_safe_eval_bool_expression_supports_subscript; test_validate_bool_expression_syntax_accepts_safe_expressions; test_validate_bool_expression_syntax_rejects_calls

### `backend/tests/unit/utils/test_schema_columns_hash_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=34 | code_lines=25 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (3): shared.services.pipeline.pipeline_schema_utils; shared.utils.schema_columns; shared.utils.schema_hash
- External imports (0): not documented
- Public API names: test_compute_schema_hash_from_payload_matches_columns_hash; test_extract_schema_columns_supports_dict_shapes; test_extract_schema_columns_supports_list_payload; test_extract_schema_names_and_types

### `backend/tests/unit/utils/test_submission_criteria_diagnostics.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=27 | code_lines=18 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.submission_criteria_diagnostics
- External imports (2): __future__; pytest
- Public API names: test_submission_criteria_reason_missing_role; test_submission_criteria_reason_mixed; test_submission_criteria_reason_state_mismatch

### `backend/tests/unit/utils/test_token_count.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=23 | code_lines=15 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.token_count
- External imports (0): not documented
- Public API names: test_approx_token_count_collection_policy_toggle; test_approx_token_count_counts_payload; test_approx_token_count_handles_none_and_empty_string; test_approx_token_count_json_matches_json_serialized_size

### `backend/tests/unit/utils/test_utils_core.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=41 | code_lines=30 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils
- External imports (1): os
- Public API names: test_branch_utils_defaults; test_parse_bool_env_and_int_env; test_s3_uri_helpers; test_safe_path_helpers

### `backend/tests/unit/utils/test_uuid_utils.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=12 | code_lines=7 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.uuid_utils
- External imports (0): not documented
- Public API names: test_safe_uuid_accepts_valid_uuid; test_safe_uuid_rejects_invalid_values

### `backend/tests/unit/utils/test_worker_runner.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=57 | code_lines=39 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=11 | try=0 | raise=4 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.worker_runner
- External imports (3): __future__; logging; pytest
- Public API names: test_run_component_lifecycle_keeps_primary_error_when_close_also_fails; test_run_component_lifecycle_propagates_run_error_without_close; test_run_component_lifecycle_raises_close_error_when_no_primary_error

### `backend/tests/unit/utils/test_writeback_conflicts.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=108 | code_lines=82 | risk_score=0
- API surface: public=13 | top-level functions=13 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/13 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.writeback_conflicts
- External imports (0): not documented
- Public API names: test_detect_overlap_fields_compares_current_to_observed; test_detect_overlap_links_flags_base_added_patch_removes; test_detect_overlap_links_flags_base_removed_patch_adds; test_normalize_conflict_policy_accepts_known_values_case_insensitive; test_normalize_conflict_policy_defaults_to_fail; test_parse_conflict_policy_accepts_known_values_case_insensitive; test_parse_conflict_policy_returns_none_for_missing_or_unknown; test_resolve_applied_changes_base_wins_skips_on_conflict; test_resolve_applied_changes_base_wins_skips_on_link_conflict; test_resolve_applied_changes_fail_rejects_on_conflict; test_resolve_applied_changes_fail_rejects_on_link_conflict; test_resolve_applied_changes_no_conflict_always_applies (+1 more)

### `backend/tests/unit/utils/test_writeback_governance.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=19 | code_lines=13 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.writeback_governance
- External imports (0): not documented
- Public API names: test_extract_backing_dataset_id_reads_object_type_spec; test_extract_backing_dataset_id_returns_none_for_missing_shape; test_policies_aligned_requires_dicts_and_exact_match

### `backend/tests/unit/utils/test_writeback_lifecycle.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=38 | code_lines=24 | risk_score=0
- API surface: public=6 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/6 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.writeback_lifecycle
- External imports (1): pytest
- Public API names: test_derive_lifecycle_id_defaults_when_missing; test_derive_lifecycle_id_prefers_top_level_value; test_derive_lifecycle_id_reads_metadata_value; test_derive_lifecycle_id_uses_last_create_command_id; test_overlay_doc_id_composes_instance_and_lifecycle; test_overlay_doc_id_rejects_delimiter_collision

### `backend/tests/unit/utils/test_writeback_patch_apply.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=41 | code_lines=27 | risk_score=0
- API surface: public=5 | top-level functions=5 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.writeback_patch_apply
- External imports (1): pytest
- Public API names: test_apply_changes_delete_short_circuits; test_apply_changes_link_add_and_remove; test_apply_changes_link_scalar_coerces_to_list; test_apply_changes_set_unset; test_apply_changes_type_validation

### `backend/tests/unit/utils/test_writeback_paths.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=25 | code_lines=20 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.writeback_paths
- External imports (0): not documented
- Public API names: test_queue_entry_prefix_builds_expected_path; test_snapshot_keys_match_design_layout

### `backend/tests/unit/validators/test_base_type_validators.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=104 | code_lines=74 | risk_score=0
- API surface: public=15 | top-level functions=15 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/15 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.validators
- External imports (0): not documented
- Public API names: test_array_validator_rejects_nested_arrays; test_array_validator_rejects_null_items; test_attachment_uses_string_validator; test_cipher_validator_rejects_non_string; test_geopoint_validator_accepts_latlon; test_geopoint_validator_rejects_out_of_range; test_geoshape_validator_accepts_point; test_geoshape_validator_rejects_invalid_type; test_marking_validator_rejects_non_string; test_media_uses_string_validator; test_struct_validator_rejects_array_field; test_struct_validator_rejects_nested_struct (+3 more)

### `backend/tests/unit/workers/__init__.py`
- Module summary: Unit tests for SPICE HARVESTER worker services
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=3 | code_lines=3 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/tests/unit/workers/test_action_worker_dependency_triggers.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=195 | code_lines=167 | risk_score=0
- API surface: public=3 | top-level functions=5 | classes=2 | methods=7
- Runtime signals: async_functions=8 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/5 (0%) | classes=0/2 (0%) | methods=0/7 (0%)
- Internal imports (2): action_worker.main; shared.services.registries.action_log_registry
- External imports (4): __future__; datetime; pytest; typing
- Public API names: test_trigger_dependent_actions_emits_child_command; test_trigger_dependent_actions_marks_failed_when_dependency_impossible; test_trigger_dependent_actions_waits_until_all_parents_terminal

### `backend/tests/unit/workers/test_action_worker_permission_profile.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=79 | code_lines=63 | risk_score=0
- API surface: public=3 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=6 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): action_worker.main; shared.utils.action_permission_profile
- External imports (2): __future__; pytest
- Public API names: test_action_worker_enforce_permission_allows_datasource_derived_without_policy; test_action_worker_enforce_permission_rejects_edits_beyond_actions_without_engineer_role; test_action_worker_enforce_permission_rejects_invalid_permission_profile

### `backend/tests/unit/workers/test_action_worker_submission_validation_rules.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=166 | code_lines=136 | risk_score=0
- API surface: public=5 | top-level functions=7 | classes=1 | methods=2
- Runtime signals: async_functions=6 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/7 (0%) | classes=0/1 (0%) | methods=0/2 (0%)
- Internal imports (1): action_worker.main
- External imports (3): __future__; pytest; typing
- Public API names: test_enforce_each_target_validation_rule_includes_target_context; test_enforce_submission_and_validation_rules_passes_for_valid_inputs; test_enforce_submission_rules_rejects_false_submission_criteria; test_enforce_submission_rules_requires_user_for_submission_criteria; test_enforce_validation_rules_requires_list_type

### `backend/tests/unit/workers/test_connector_sync_worker.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=295 | code_lines=219 | risk_score=0
- API surface: public=6 | top-level functions=6 | classes=9 | methods=33
- Runtime signals: async_functions=21 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/6 (0%) | classes=0/9 (0%) | methods=0/33 (0%)
- Internal imports (5): connector_sync_worker; connector_sync_worker.main; shared.models.event_envelope; shared.services.registries.connector_registry; shared.services.registries.processed_event_registry
- External imports (6): __future__; asyncio; contextlib; datetime; json; pytest
- Public API names: test_sync_worker_bff_scope_headers; test_sync_worker_fetch_schema_and_target_types; test_sync_worker_handle_envelope_rejects_unknown; test_sync_worker_heartbeat_loop_stops; test_sync_worker_process_google_sheets_update; test_sync_worker_run_processes_message

### `backend/tests/unit/workers/test_connector_trigger_service.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=234 | code_lines=179 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=4 | methods=17
- Runtime signals: async_functions=16 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/4 (0%) | methods=0/17 (0%)
- Internal imports (4): connector_trigger_service; connector_trigger_service.main; shared.models.event_envelope; shared.services.registries.connector_registry
- External imports (5): __future__; asyncio; contextlib; datetime; pytest
- Public API names: test_trigger_service_initialize_and_close; test_trigger_service_is_due; test_trigger_service_poll_google_sheets_refreshes_token; test_trigger_service_publish_outbox

### `backend/tests/unit/workers/test_instance_worker_helpers.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=60 | code_lines=44 | risk_score=0
- API surface: public=4 | top-level functions=4 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): instance_worker.main; shared.models.event_envelope
- External imports (2): __future__; pytest
- Public API names: test_extract_payload_from_message_rejects_non_command; test_extract_payload_from_message_success; test_primary_key_and_objectify_helpers; test_retryable_error_detection

### `backend/tests/unit/workers/test_instance_worker_objectify_gates.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=33 | code_lines=25 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): instance_worker.main
- External imports (1): pytest
- Public API names: test_primary_key_required_when_generation_disabled; test_relationship_fallback_can_be_disabled

### `backend/tests/unit/workers/test_instance_worker_s3.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=30 | code_lines=22 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): instance_worker.main
- External imports (3): asyncio; pytest; time
- Public API names: test_s3_call_does_not_block_event_loop

### `backend/tests/unit/workers/test_message_relay_process.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=85 | code_lines=69 | risk_score=13
- API surface: public=1 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=1 | try=3 | raise=0 | broad_except=2 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): message_relay.main; shared.config.app_config
- External imports (8): __future__; boto3; botocore; datetime; os; pytest; time; uuid
- Public API names: test_event_publisher_processes_index

### `backend/tests/unit/workers/test_objectify_delta_lakefs.py`
- Module summary: Tests for LakeFS diff-based delta computation and worker integration.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=209 | code_lines=164 | risk_score=0
- API surface: public=12 | top-level functions=12 | classes=0 | methods=0
- Runtime signals: async_functions=8 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/12 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.services.pipeline.objectify_delta_utils
- External imports (4): __future__; pytest; typing; unittest
- Public API names: test_compute_delta_from_lakefs_diff_added_file; test_compute_delta_from_lakefs_diff_changed_file; test_compute_delta_from_lakefs_diff_removed_file; test_compute_delta_from_snapshots; test_compute_row_hash_deterministic; test_compute_row_hash_differs_on_change; test_compute_row_key; test_create_delta_computer_fallback_pk; test_create_delta_computer_from_mapping_spec; test_delta_result_has_changes_false; test_delta_result_has_changes_true; test_delta_result_stats_auto_computed

### `backend/tests/unit/workers/test_objectify_incremental_default.py`
- Module summary: Tests for incremental mode default and auto-watermark detection.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=121 | code_lines=92 | risk_score=0
- API surface: public=7 | top-level functions=8 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=2/8 (25%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.models.objectify_job
- External imports (2): __future__; pytest
- Public API names: test_auto_detect_watermark_column; test_default_execution_mode_is_full; test_execution_mode_falls_back_to_full_for_invalid_values; test_execution_mode_prefers_options_over_job_mode; test_explicit_delta_mode; test_explicit_full_mode; test_watermark_fields

### `backend/tests/unit/workers/test_objectify_worker_helpers.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=62 | code_lines=48 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): objectify_worker.main; shared.services.core.sheet_import_service
- External imports (1): __future__
- Public API names: test_objectify_worker_field_helpers; test_objectify_worker_row_key_derivation

### `backend/tests/unit/workers/test_objectify_worker_lineage_dataset_version.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=63 | code_lines=52 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=1 | methods=3
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/1 (0%) | methods=0/3 (0%)
- Internal imports (2): objectify_worker.main; shared.models.objectify_job
- External imports (2): pytest; types
- Public API names: test_instance_lineage_records_dataset_version

### `backend/tests/unit/workers/test_objectify_worker_link_index_dangling.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=505 | code_lines=439 | risk_score=0
- API surface: public=10 | top-level functions=13 | classes=4 | methods=14
- Runtime signals: async_functions=20 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/13 (0%) | classes=0/4 (0%) | methods=0/14 (0%)
- Internal imports (3): objectify_worker.main; shared.models.objectify_job; shared.services.core.sheet_import_service
- External imports (2): pytest; types
- Public API names: test_link_edits_are_applied_to_updates; test_link_index_creates_link_when_fk_matches_target; test_link_index_dedupes_duplicate_pairs_for_join_table; test_link_index_fails_on_missing_target_when_policy_fail; test_link_index_records_fail_when_dangling_policy_fail; test_link_index_records_pass_result_with_lineage; test_link_index_records_warn_when_dangling_policy_warn; test_link_index_warns_on_missing_target_when_policy_warn; test_object_backed_full_sync_clears_links_when_no_rows; test_object_backed_link_index_creates_link

### `backend/tests/unit/workers/test_objectify_worker_p0_gates.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=216 | code_lines=186 | risk_score=0
- API surface: public=9 | top-level functions=10 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/10 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): objectify_worker.main; shared.services.core.sheet_import_service
- External imports (1): pytest
- Public API names: test_duplicate_primary_key_is_blocked; test_instance_id_requires_row_key; test_missing_source_column_is_fatal; test_primary_key_missing_when_source_blank; test_required_field_missing_is_reported; test_value_constraints_fail_fast; test_value_constraints_format_failures; test_value_constraints_min_length_enforced; test_value_constraints_pattern_enforced

### `backend/tests/unit/workers/test_objectify_worker_pk_uniqueness.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=135 | code_lines=104 | risk_score=0
- API surface: public=1 | top-level functions=1 | classes=4 | methods=20
- Runtime signals: async_functions=18 | try=0 | raise=4 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/1 (0%) | classes=0/4 (0%) | methods=0/20 (0%)
- Internal imports (2): objectify_worker.main; shared.models.objectify_job
- External imports (2): pytest; types
- Public API names: test_pk_duplicates_fail_before_writes

### `backend/tests/unit/workers/test_objectify_write_paths.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=250 | code_lines=212 | risk_score=0
- API surface: public=6 | top-level functions=7 | classes=1 | methods=8
- Runtime signals: async_functions=13 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=4/7 (57%) | classes=0/1 (0%) | methods=0/8 (0%)
- Internal imports (2): objectify_worker.write_paths; shared.models.objectify_job
- External imports (3): __future__; pytest; typing
- Public API names: test_build_document_populates_properties_from_flat_instance; test_build_document_preserves_existing_properties; test_build_document_skips_none_values; test_dataset_primary_finalize_prunes_stale_docs_on_full; test_dataset_primary_write_path_indexes_instances_directly; test_write_instances_passes_target_field_types

### `backend/tests/unit/workers/test_ontology_worker_graph_lineage_naming.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=172 | code_lines=145 | risk_score=0
- API surface: public=2 | top-level functions=3 | classes=3 | methods=8
- Runtime signals: async_functions=7 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/3 (0%) | methods=0/8 (0%)
- Internal imports (1): ontology_worker.main
- External imports (4): __future__; pytest; typing; unittest
- Public API names: test_create_ontology_records_graph_lineage_and_audit_naming; test_delete_ontology_records_graph_lineage_and_audit_naming

### `backend/tests/unit/workers/test_ontology_worker_helpers.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=34 | code_lines=23 | risk_score=0
- API surface: public=3 | top-level functions=3 | classes=0 | methods=0
- Runtime signals: async_functions=3 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): ontology_worker.main; shared.services.kafka.consumer_ops
- External imports (3): __future__; asyncio; pytest
- Public API names: test_consumer_ops_defaults_to_inline; test_heartbeat_loop_no_registry; test_resource_registry_mode_is_default_and_active

### `backend/tests/unit/workers/test_ontology_worker_legacy_backend.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=39 | code_lines=29 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=0 | methods=0
- Runtime signals: async_functions=2 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (2): ontology_worker.main; shared.models.commands
- External imports (3): __future__; pytest; unittest
- Public API names: test_process_command_allows_database_command_without_adapter_in_postgres_profile; test_process_command_allows_ontology_command_without_adapter_in_postgres_profile

### `backend/tests/unit/workers/test_ontology_worker_resource_registry.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=238 | code_lines=207 | risk_score=0
- API surface: public=3 | top-level functions=4 | classes=3 | methods=9
- Runtime signals: async_functions=9 | try=0 | raise=2 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/4 (0%) | classes=0/3 (0%) | methods=0/9 (0%)
- Internal imports (1): ontology_worker.main
- External imports (4): __future__; pytest; typing; unittest
- Public API names: test_create_ontology_uses_object_type_resource_registry_in_postgres_profile; test_delete_ontology_uses_object_type_resource_registry_in_postgres_profile; test_update_ontology_uses_object_type_resource_registry_in_postgres_profile

### `backend/tests/unit/workers/test_pipeline_worker_helpers.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=191 | code_lines=158 | risk_score=2
- API surface: public=12 | top-level functions=12 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/12 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): pipeline_worker.main
- External imports (3): __future__; os; pytest
- Public API names: test_resolve_code_version_and_sensitive_keys; test_resolve_external_read_mode_streaming_aliases; test_resolve_kafka_avro_schema_accepts_schema_registry_reference; test_resolve_kafka_avro_schema_rejects_incomplete_schema_registry_reference; test_resolve_kafka_avro_schema_rejects_latest_registry_version; test_resolve_kafka_value_format_and_checkpoint_policy; test_resolve_lakefs_repository; test_resolve_output_format_and_partitions; test_resolve_streaming_trigger_mode_and_timeout; test_streaming_external_source_requires_kafka_format; test_streaming_external_source_respects_global_toggle; test_watermark_snapshot_helpers

### `backend/tests/unit/workers/test_pipeline_worker_objectify_auto_enqueue.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=150 | code_lines=122 | risk_score=0
- API surface: public=2 | top-level functions=2 | classes=3 | methods=11
- Runtime signals: async_functions=9 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/2 (0%) | classes=0/3 (0%) | methods=0/11 (0%)
- Internal imports (1): pipeline_worker.main
- External imports (4): __future__; datetime; pytest; types
- Public API names: test_pipeline_worker_enqueues_objectify_job; test_pipeline_worker_schema_mismatch_records_gate

### `backend/tests/unit/workers/test_pipeline_worker_objectify_auto_enqueue_nospark.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=193 | code_lines=155 | risk_score=0
- API surface: public=2 | top-level functions=3 | classes=3 | methods=11
- Runtime signals: async_functions=9 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/3 (0%) | classes=0/3 (0%) | methods=0/11 (0%)
- Internal imports (1): pipeline_worker.main
- External imports (6): __future__; datetime; importlib; pytest; sys; types
- Public API names: test_pipeline_worker_enqueues_objectify_job_without_pyspark; test_pipeline_worker_schema_mismatch_records_gate_without_pyspark

### `backend/tests/unit/workers/test_pipeline_worker_transforms.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=221 | code_lines=176 | risk_score=7
- API surface: public=6 | top-level functions=7 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/7 (0%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): pipeline_worker.main
- External imports (6): __future__; importlib; os; pyspark; pytest; sys
- Public API names: spark; test_apply_transform_basic_ops; test_apply_transform_join_union_groupby_pivot_window; test_pipeline_worker_file_helpers; test_watermark_helpers; worker

### `backend/tests/unit/workers/test_spark_advanced_transforms.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=835 | code_lines=743 | risk_score=7
- API surface: public=24 | top-level functions=25 | classes=1 | methods=4
- Runtime signals: async_functions=12 | try=2 | raise=0 | broad_except=1 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=0/25 (0%) | classes=0/1 (0%) | methods=0/4 (0%)
- Internal imports (1): pipeline_worker.main
- External imports (7): __future__; importlib; json; os; pyspark; pytest; sys
- Public API names: spark; test_geospatial_geohash; test_geospatial_point_and_distance; test_materialize_dataset_output_changelog_appends_only_new_or_changed_rows; test_materialize_dataset_output_deduplicates_duplicate_primary_keys_for_append_only_new_rows; test_materialize_dataset_output_default_incremental_additive_updates_uses_append_runtime; test_materialize_dataset_output_default_incremental_without_additive_signal_uses_snapshot_runtime; test_materialize_dataset_output_snapshot_difference_keeps_current_transaction_duplicates; test_materialize_output_dataframe_rejects_partitioned_json; test_materialize_virtual_output_writes_manifest_artifact; test_pattern_mining_contains_and_extract; test_select_new_or_changed_rows_can_preserve_input_duplicates_for_changelog (+12 more)

### `backend/tests/utils/__init__.py`
- Module summary: Test utilities for SPICE HARVESTER.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=48 | code_lines=42 | risk_score=2
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=2 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (1): logging
- Public API names: not documented

### `backend/tests/utils/auth.py`
- Module summary: no docstring
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: general backend module
- Source footprint: total_lines=95 | code_lines=76 | risk_score=0
- API surface: public=5 | top-level functions=6 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=1 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=no | top-level functions=1/6 (16%) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (1): shared.utils.repo_dotenv
- External imports (4): __future__; jose; os; typing
- Public API names: bff_auth_headers; build_smoke_user_jwt; oms_auth_headers; require_token; with_delegated_user

## writeback_materializer_worker

### `backend/writeback_materializer_worker/__init__.py`
- Module summary: Writeback materializer worker package.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: asynchronous background processing
- Source footprint: total_lines=2 | code_lines=1 | risk_score=0
- API surface: public=0 | top-level functions=0 | classes=0 | methods=0
- Runtime signals: async_functions=0 | try=0 | raise=0 | broad_except=0 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/0 (n/a) | classes=0/0 (n/a) | methods=0/0 (n/a)
- Internal imports (0): not documented
- External imports (0): not documented
- Public API names: not documented

### `backend/writeback_materializer_worker/main.py`
- Module summary: Writeback Materializer Worker.
- Responsibilities: not documented
- Invariants: not documented
- Failure modes: not documented
- Extension points: not documented
- Dependencies (doc): not documented
- Inferred role: service entrypoint and lifecycle wiring
- Source footprint: total_lines=344 | code_lines=292 | risk_score=22
- API surface: public=2 | top-level functions=4 | classes=1 | methods=8
- Runtime signals: async_functions=8 | try=6 | raise=4 | broad_except=4 | bare_except=0 | finally_return=0
- Doc coverage: module=yes | top-level functions=0/4 (0%) | classes=0/1 (0%) | methods=0/8 (0%)
- Internal imports (14): shared.config.app_config; shared.config.settings; shared.observability.metrics; shared.observability.tracing; shared.services.core.writeback_merge_service; shared.services.storage.lakefs_branch_utils; shared.services.storage.lakefs_client; shared.services.storage.lakefs_storage_service (+6 more)
- External imports (7): __future__; asyncio; hashlib; logging; time; typing; uuid
- Public API names: WritebackMaterializerWorker; main
