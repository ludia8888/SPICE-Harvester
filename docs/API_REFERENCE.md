# SPICE Harvester — BFF API Reference (v1)

> **Last Updated**: 2025-12-18  
> **Frontend Contract**: ✅ BFF is the only supported API surface for FE  
> **Base URL (local)**: `http://localhost:8002/api/v1`  
> **Swagger UI (local)**: `http://localhost:8002/docs`  
> **OpenAPI JSON (local)**: `http://localhost:8002/openapi.json`

## Scope (Frontend Contract)

- Frontend **MUST** call only BFF endpoints under `/api/v1`.
- OMS / Funnel / workers are **internal** dependencies and **not** part of the frontend contract.
- Endpoints are labeled as:
  - **Stable**: safe for FE integration
  - **Operator-only**: requires admin token / intended for ops
  - **🚧 WIP**: exists in API surface but not production-ready (do not use in FE)

## Quick Start (Local)

```bash
docker compose -f docker-compose.full.yml up -d --build
```

Then open:
- Swagger: `http://localhost:8002/docs`

## Conventions

### Content-Type

- Requests: `Content-Type: application/json`
- Responses: `application/json` (unless file upload endpoints)

### Paths (⚠️ naming is currently mixed)

BFF has two URL “shapes” today:

- Collection-style (plural): `/api/v1/databases`, `/api/v1/databases/{db_name}/branches`, ...
- DB-scoped (legacy singular): `/api/v1/database/{db_name}/ontology`, `/api/v1/database/{db_name}/query`, ...

This mix is historical (routers were added at different times) and is a **known UX papercut**.

**Frontend guidance**
- Use the endpoints exactly as documented in this file (they are part of the FE contract).
- When in doubt:
  - **DB CRUD / branches** live under `/api/v1/databases...`
  - **ontology/query/instances/mappings** mostly live under `/api/v1/database/{db_name}...`

**Deprecation plan (doc-level, until code adds aliases)**
- We intend to converge to `/api/v1/databases/{db_name}/...` for all DB-scoped resources.
- Until then, the singular `/api/v1/database/{db_name}/...` paths should be treated as **stable-but-legacy**.

### Time

- All timestamps are ISO8601.
- Prefer UTC (`...Z`). (Some internal responses may include offset-aware timestamps.)

### Core IDs

- `db_name`: lowercase + digits + `_`/`-` (validated)
- `branch`: alphanumerics + `_`/`-`/`/`
- `class_id`: ID form (alphanumerics + `_`/`-`/`:`), **internal**
- `class_label`: human label used in BFF async instance endpoints (BFF resolves it to `class_id`)
- `instance_id`: alphanumerics + `_`/`-`/`:` (validated)
- `command_id`: UUID

### Glossary (terms that are easy to mix up)

| Term | Meaning | Stability |
|------|---------|-----------|
| `class_id` | Internal, stable identifier for a class (schema). Used in graph docs and relationship refs. | **Stable** |
| `label` | UI-facing display name (can be multilingual). Should not be used as a key. | **Mutable** |
| `class_label` | A label string accepted by some BFF endpoints for convenience; BFF maps it to `class_id` via label mappings. | **Convenience** |
| `instance_id` | Internal, stable identifier for an instance (entity). Typically derived from `{class_id.lower()}_id`. | **Stable** |
| `expected_seq` | Optimistic concurrency token for write commands (409 on mismatch). | **Stable** |

### Language / i18n (EN + KO)

- Supported languages: `en`, `ko`
- For UI-facing text fields, the API accepts **either**:
  - a plain string (legacy), or
  - a language map: `{"en": "...", "ko": "..."}` (recommended)
- Output language selection:
  - Recommended: `?lang=en|ko` (query param override)
  - Also supported: `Accept-Language: en-US,en;q=0.9,ko;q=0.8`
  - If both are present, `?lang` wins.
- When a translation is missing, the API falls back to the other supported language.
- Default (when neither is provided): `ko`

### Write mode (202 vs 200/201)

All “write” endpoints in the supported system posture are **async**:

- Writes submit a command and return HTTP `202` + `command_id` (poll required)
- Direct write mode (`ENABLE_EVENT_SOURCING=false`) is **not supported** for core write paths (you may see `5xx` if you try)

Notes:
- This API reference assumes **event-sourcing mode** as the only supported production posture.

### Standard Response: `ApiResponse`

Most BFF HTTP endpoints (especially database/ontology writes) return:

```json
{
  "status": "success|created|accepted|warning|partial|error",
  "message": "Human readable message",
  "data": { "..." : "..." },
  "errors": ["..."]
}
```

Notes:
- `status="accepted"` + HTTP `202` means “command accepted, work continues asynchronously”.
- Some endpoints return domain-specific shapes (e.g., graph query), and some return `CommandResult` (below).

### Async Command Result: `CommandResult`

Some write endpoints (notably async instance commands) return:

```json
{
  "command_id": "uuid",
  "status": "PENDING|PROCESSING|COMPLETED|FAILED|CANCELLED|RETRYING",
  "result": { "..." : "..." },
  "error": "error message",
  "completed_at": "2025-01-01T00:00:00Z",
  "retry_count": 0
}
```

### Auto-generated `class_id` (when `id` is omitted in BFF)

Some BFF ontology endpoints will generate `id` from `label` when `id` is missing.

Current implementation details (so users can predict collisions):
- Text source selection:
  - If `label` is a string: use it
  - If `label` is an object: prefer `en` → `ko` → first available value
- Normalization:
  - Remove punctuation/special chars
  - Convert whitespace-separated words into `CamelCase`
  - If the result starts with a digit, prefix `Class`
  - Truncate to ~50 chars
- Korean labels:
  - Best-effort romanization (small mapping) + **timestamp suffix** to reduce collisions

Collision policy:
- If the generated `id` already exists, the create call will fail with `409 Conflict`.
- For production schemas, prefer supplying an explicit, stable `id` and treat `label` as a UI string.

## Reliability Contract (Async Writes)

### Delivery semantics

- Publisher/Kafka delivery is **at-least-once**.
- Consumers (workers/projections) implement **idempotency by `event_id`**.
- Ordering is enforced per-aggregate by `seq` (stale events are ignored).

See:
- `docs/IDEMPOTENCY_CONTRACT.md`

### Observability for async writes

If an endpoint returns **202 Accepted** with `data.command_id`, the frontend should:

1. Store `command_id`
2. Poll:
   - `GET /api/v1/commands/{command_id}/status`
3. Render state transitions:
   - `PENDING → PROCESSING → COMPLETED | FAILED`

### Failure / retry UX (what users should do)

`GET /api/v1/commands/{command_id}/status` may show:

- `RETRYING`: the worker detected a **transient** failure and is retrying with backoff.
  - FE should show “retrying” and keep polling.
- `FAILED`: the command became a terminal failure (non-retryable error or max retries reached).
  - FE should surface `error` (string) and offer:
    - “Retry” (re-submit the command) **only after** user fixes input or the system recovers
    - “View audit/lineage” links if available (ops workflows)
- `CANCELLED`: reserved for explicit cancellation flows (may not be emitted in all deployments yet).

Common operator guidance:
- `400`-class errors: input/schema mismatch → user must edit payload/schema and re-submit.
- `409` errors: OCC mismatch (`expected_seq`) → refresh state and retry with correct token.
- `5xx`/timeouts: transient infra issues → wait; retries may auto-resolve; otherwise re-submit later.

### Optimistic Concurrency Control (OCC)

Some update/delete endpoints require `expected_seq`:

- If `expected_seq` mismatches the current aggregate sequence: HTTP `409`
- Error shape (typical):

```json
{
  "detail": {
    "error": "optimistic_concurrency_conflict",
    "aggregate_id": "...",
    "expected_seq": 3,
    "actual_seq": 4
  }
}
```

## Relationship Reference Format (Instances → Graph)

### Reference string

Relationship fields in instance payloads are stored as TerminusDB `@id` references using:

- **String reference**: `"<TargetClassID>/<instance_id>"`
  - Example: `"Customer/cust_001"`

Branch semantics:
- References are resolved **within the branch context** of the command/query (`?branch=...`).
- Cross-branch references are not supported in the value format (there is no `branch` prefix in the ref).

### Cardinality → JSON shape (recommended)

To reduce ambiguity, use a consistent JSON shape by cardinality:

- `1:1`, `n:1`: a **single** string ref  
  - `"owned_by": "Customer/cust_001"`
- `1:n`, `n:m`: an **array** of string refs  
  - `"employees": ["Person/p1", "Person/p2"]`

Referential integrity policy (current system posture):
- Relationship refs are treated as **best-effort graph links**.
- The system may accept a link even if the target entity is not yet materialized/indexed.
- In reads, this can surface as `data_status=PARTIAL|MISSING` depending on projection lag.

If you need strict integrity (reject refs to missing targets), call this out as an explicit product requirement; it is not enforced universally today.

## Authentication

### Public BFF endpoints

- Currently **no auth is enforced** for non-admin endpoints (PoC/dev posture).
- Frontend should still be built with an auth boundary in mind (RBAC/tenancy is planned).

| Surface | Intended header | Notes |
|--------|------------------|------|
| BFF (public) | *(none enforced yet)* | PoC/dev posture; expect RBAC/tenancy later |
| BFF (admin) | `X-Admin-Token` or `Authorization: Bearer` | Required when `BFF_ADMIN_TOKEN` is set |
| OMS / Terminus | *(internal only)* | FE must not call; typically Basic Auth to TerminusDB |

### Admin endpoints (Operator-only)

- Admin endpoints are disabled unless `BFF_ADMIN_TOKEN` (or `ADMIN_API_KEY` / `ADMIN_TOKEN`) is configured on the BFF service.
- Required header:
  - `X-Admin-Token: <token>` **or** `Authorization: Bearer <token>`
- Optional header:
  - `X-Admin-Actor: <name/email>` (stored into request context and may appear in audit metadata)

## Endpoint Index (BFF)

This section lists **all** BFF HTTP endpoints currently exposed (excluding WebSocket routes).

### AI (**Stable**)
- `POST /api/v1/ai/query/{db_name}` — LLM-assisted query
- `POST /api/v1/ai/translate/query-plan/{db_name}` — translate query-plan to execution

### Admin Operations (**Operator-only**)
- `POST /api/v1/admin/replay-instance-state` — reconstruct an instance state by replaying events
- `GET /api/v1/admin/replay-instance-state/{task_id}/result` — replay result
- `GET /api/v1/admin/replay-instance-state/{task_id}/trace` — trace replayed history into Audit + Lineage
- `POST /api/v1/admin/recompute-projection` — rebuild ES read model by replaying events
- `GET /api/v1/admin/recompute-projection/{task_id}/result` — recompute result
- `POST /api/v1/admin/cleanup-old-replays` — cleanup replay results stored in Redis
- `GET /api/v1/admin/system-health` — system health summary

### Async Instance Management (**Stable**)
- `POST /api/v1/database/{db_name}/instances/{class_label}/create` — **HTTP 202** submit CREATE_INSTANCE command
- `PUT /api/v1/database/{db_name}/instances/{class_label}/{instance_id}/update` — **HTTP 202** submit UPDATE_INSTANCE command (requires `expected_seq`)
- `DELETE /api/v1/database/{db_name}/instances/{class_label}/{instance_id}/delete` — **HTTP 202** submit DELETE_INSTANCE command (requires `expected_seq`)
- `POST /api/v1/database/{db_name}/instances/{class_label}/bulk-create` — **HTTP 202** submit BULK_CREATE_INSTANCES command(s)

Notes:
- `data` keys are **property labels** (human-facing). BFF resolves them to internal `property_id` via LabelMapper.
- If any label cannot be resolved, BFF returns HTTP `400` with `detail.error="unknown_label_keys"`.
- Label mappings are typically populated by ontology create/update flows (or `Label Mappings` import APIs).

### Audit (**Stable**)
- `GET /api/v1/audit/logs` — list audit logs
- `GET /api/v1/audit/chain-head` — verify audit chain head

### Background Tasks (**Stable**)
- `GET /api/v1/tasks/` — list tasks
- `GET /api/v1/tasks/{task_id}` — get task status
- `GET /api/v1/tasks/{task_id}/result` — get task result (if stored)
- `DELETE /api/v1/tasks/{task_id}` — cancel task
- `GET /api/v1/tasks/metrics/summary` — metrics summary

Notes:
- Manual task retry is intentionally **not exposed** (task specs are not durably persisted today). Re-submit a new command instead.

### Command Status (**Stable**)
- `GET /api/v1/commands/{command_id}/status` — poll async command status/result

### Config Monitoring (**Operator-only**)
- `GET /api/v1/config/config/current` — current configuration
- `GET /api/v1/config/config/report` — config report
- `GET /api/v1/config/config/validation` — config validation
- `GET /api/v1/config/config/changes` — recent changes
- `POST /api/v1/config/config/check-changes` — trigger change check
- `GET /api/v1/config/config/drift-analysis` — drift analysis
- `GET /api/v1/config/config/security-audit` — security audit view
- `GET /api/v1/config/config/health-impact` — health impact report
- `GET /api/v1/config/config/monitoring-status` — monitoring status

### Data Connectors (**Stable**)
- `POST /api/v1/data-connectors/google-sheets/grid` — extract grid + merges
- `POST /api/v1/data-connectors/google-sheets/preview` — preview sheet data (for Funnel/import)
- `POST /api/v1/data-connectors/google-sheets/register` — register sheet for monitoring
- `GET /api/v1/data-connectors/google-sheets/registered` — list registered sheets
- `GET /api/v1/data-connectors/google-sheets/{sheet_id}/preview` — preview registered sheet data
- `DELETE /api/v1/data-connectors/google-sheets/{sheet_id}` — unregister sheet

### Database Management (**Stable**)
- `GET /api/v1/databases` — list databases
- `POST /api/v1/databases` — create database (**HTTP 202**)
- `GET /api/v1/databases/{db_name}` — get database info
- `DELETE /api/v1/databases/{db_name}` — delete database (**HTTP 202**; requires `expected_seq`)
- `GET /api/v1/databases/{db_name}/branches` — list branches
- `POST /api/v1/databases/{db_name}/branches` — create branch
- `GET /api/v1/databases/{db_name}/branches/{branch_name}` — get branch info
- `DELETE /api/v1/databases/{db_name}/branches/{branch_name}` — delete branch
- `GET /api/v1/databases/{db_name}/classes` — list classes
- `POST /api/v1/databases/{db_name}/classes` — create class (legacy JSON-LD import)
- `GET /api/v1/databases/{db_name}/classes/{class_id}` — get class
- `GET /api/v1/databases/{db_name}/versions` — version info

### Graph (**Stable**, plus **🚧 WIP** endpoints)
- `GET /api/v1/graph-query/health` — graph service health
- `POST /api/v1/graph-query/{db_name}` — multi-hop traversal + ES federation
- `POST /api/v1/graph-query/{db_name}/simple` — simple single-class query
- `POST /api/v1/graph-query/{db_name}/multi-hop` — specialized multi-hop query helper
- `GET /api/v1/graph-query/{db_name}/paths` — find relationship paths
- `POST /api/v1/projections/{db_name}/register` — 🚧 (WIP) projection registration (materialized view)
- `POST /api/v1/projections/{db_name}/query` — 🚧 (WIP) projection query (materialized view)
- `GET /api/v1/projections/{db_name}/list` — 🚧 (WIP) list projections (materialized view)

### Health (**Stable**)
- `GET /api/v1/` — root
- `GET /api/v1/health` — health check

### Instance Management (**Stable**)
- `GET /api/v1/database/{db_name}/class/{class_id}/instances` — list instances (ES-first; Terminus fallback)
- `GET /api/v1/database/{db_name}/class/{class_id}/instance/{instance_id}` — get instance (ES-first; Terminus fallback)
- `GET /api/v1/database/{db_name}/class/{class_id}/sample-values` — sample values per field (for UI filters)

### Label Mappings (**Stable**)
- `GET /api/v1/database/{db_name}/mappings/` — mappings summary
- `POST /api/v1/database/{db_name}/mappings/export` — export mappings
- `POST /api/v1/database/{db_name}/mappings/import` — import mappings
- `POST /api/v1/database/{db_name}/mappings/validate` — validate mappings
- `DELETE /api/v1/database/{db_name}/mappings/` — clear mappings

### Lineage (**Stable**)
- `GET /api/v1/lineage/graph` — lineage graph
- `GET /api/v1/lineage/impact` — impact analysis
- `GET /api/v1/lineage/metrics` — lineage metrics

### Merge Conflict Resolution (**Stable**)
- `POST /api/v1/database/{db_name}/merge/simulate` — simulate merge
- `POST /api/v1/database/{db_name}/merge/resolve` — resolve conflicts

### Monitoring (**Operator-only**)
- `GET /api/v1/monitoring/health` — basic health
- `GET /api/v1/monitoring/health/detailed` — detailed health
- `GET /api/v1/monitoring/health/liveness` — k8s liveness
- `GET /api/v1/monitoring/health/readiness` — k8s readiness
- `GET /api/v1/monitoring/status` — status overview
- `GET /api/v1/monitoring/metrics` — redirects to `/metrics` (Prometheus scrape target)
- `GET /api/v1/monitoring/config` — config overview
- `GET /api/v1/monitoring/background-tasks/active` — active background tasks
- `GET /api/v1/monitoring/background-tasks/health` — background task health
- `GET /api/v1/monitoring/background-tasks/metrics` — background task metrics

Notes:
- Dependency graph and “restart service” APIs are intentionally **not exposed** (avoid fake controls). Use `/health/detailed` and `/status` instead.

### Ontology Management (**Stable**)
- `POST /api/v1/database/{db_name}/ontology?branch=<branch>` — create ontology class (**HTTP 202**)
- `POST /api/v1/database/{db_name}/ontology/validate?branch=<branch>` — validate ontology create (lint report, no write)
- `GET /api/v1/database/{db_name}/ontology/{class_label}?branch=<branch>` — get ontology class
- `POST /api/v1/database/{db_name}/ontology/{class_label}/validate?branch=<branch>` — validate ontology update (lint+diff, no write)
- `PUT /api/v1/database/{db_name}/ontology/{class_label}?branch=<branch>&expected_seq=...` — update ontology (**HTTP 202**; requires `expected_seq`)
- `DELETE /api/v1/database/{db_name}/ontology/{class_label}?branch=<branch>&expected_seq=...` — delete ontology (**HTTP 202**; requires `expected_seq`)
- `GET /api/v1/database/{db_name}/ontology/list?branch=<branch>` — list ontologies
- `GET /api/v1/database/{db_name}/ontology/{class_id}/schema?branch=<branch>&format=json|jsonld|owl` — schema export
- `POST /api/v1/database/{db_name}/ontology-advanced` — advanced relationship validation (async 202); `auto_generate_inverse=true` returns `501`
- `POST /api/v1/database/{db_name}/validate-relationships` — validate relationships (pre-flight)
- `POST /api/v1/database/{db_name}/check-circular-references` — detect circular refs (pre-flight)
- `GET /api/v1/database/{db_name}/relationship-network/analyze` — relationship network analysis
- `GET /api/v1/database/{db_name}/relationship-paths` — find relationship paths
- `POST /api/v1/database/{db_name}/suggest-schema-from-data` — suggest schema from sample rows
- `POST /api/v1/database/{db_name}/suggest-schema-from-google-sheets` — suggest schema from sheets
- `POST /api/v1/database/{db_name}/suggest-schema-from-excel` — suggest schema from excel
- `POST /api/v1/database/{db_name}/suggest-mappings` — suggest mappings (schema↔schema)
- `POST /api/v1/database/{db_name}/suggest-mappings-from-google-sheets` — suggest mappings from sheets
- `POST /api/v1/database/{db_name}/suggest-mappings-from-excel` — suggest mappings from excel
- `POST /api/v1/database/{db_name}/import-from-google-sheets/dry-run` — import dry-run (no write)
- `POST /api/v1/database/{db_name}/import-from-google-sheets/commit` — import commit (submits async writes to OMS)
- `POST /api/v1/database/{db_name}/import-from-excel/dry-run` — import dry-run (no write)
- `POST /api/v1/database/{db_name}/import-from-excel/commit` — import commit (submits async writes to OMS)
- `POST /api/v1/database/{db_name}/ontology/{class_id}/mapping-metadata` — save mapping metadata

**Protected branch policy (schema safety)**
- Default protected branches: `main`, `master`, `production`, `prod` (configurable via `ONTOLOGY_PROTECTED_BRANCHES`)
- On protected branches, **high-risk schema changes** (e.g., property removal/type change) and deletes require:
  - `X-Change-Reason: <text>` (required)
  - `X-Admin-Token: <secret>` or `Authorization: Bearer <secret>` (required)
  - Optional: `X-Admin-Actor: <name>` for audit/traceability

### Query (**Stable**)
- `POST /api/v1/database/{db_name}/query` — execute query
- `GET /api/v1/database/{db_name}/query/builder` — query builder info
- `POST /api/v1/database/{db_name}/query/raw` — raw query execution

## WebSocket (Real-time updates)

WebSocket routes are not represented in OpenAPI.

### Subscribe a command

- `WS /api/v1/ws/commands/{command_id}`
  - If auth is enabled, pass `?token=<admin_token>` or `X-Admin-Token` header.

Client receives:
- `connection_established`
- `command_update` (when status changes)

### Subscribe all commands of a user

- `WS /api/v1/ws/commands?user_id=...`
  - If auth is enabled, pass `?token=<admin_token>` or `X-Admin-Token` header.

## Key Flows (FE recipes)

### Success criteria (what “done” looks like)

Users commonly get stuck on “where do I verify it worked?”. Use these checks:

- **DB created**
  - If async: `GET /api/v1/commands/{command_id}/status` → `COMPLETED`
  - Then: `GET /api/v1/databases` contains the DB name
- **Ontology class created**
  - If async: command status → `COMPLETED`
  - Then: `GET /api/v1/database/{db_name}/ontology/list` contains the class
- **Instance created**
  - command status → `COMPLETED`
  - Then: `POST /api/v1/graph-query/{db_name}` returns the node
  - Note: ES payload may lag; UI should handle `data_status=PARTIAL|MISSING`

### 1) Create a database (async)

```bash
curl -sS -X POST 'http://localhost:8002/api/v1/databases' \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo_db","description":"demo"}'
```

Expected (event-sourcing mode): HTTP `202`

```json
{
  "status": "accepted",
  "message": "데이터베이스 'demo_db' 생성 명령이 접수되었습니다",
  "data": {
    "command_id": "uuid",
    "database_name": "demo_db",
    "status": "processing",
    "mode": "event_sourcing"
  }
}
```

Then:
- `GET /api/v1/commands/{command_id}/status`

### 2) Create an ontology class (async)

```bash
curl -sS -X POST 'http://localhost:8002/api/v1/database/demo_db/ontology?branch=main' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "Product",
    "label": "Product",
    "properties": [
      {"name":"product_id","type":"xsd:string","label":"Product ID","required":true},
      {"name":"name","type":"xsd:string","label":"Name"}
    ]
  }'
```

Expected (event-sourcing mode): HTTP `202` with `data.command_id`, then poll command status.

### 3) Create an instance (async command)

To get a **deterministic** `instance_id`, include an ID-like field:
- OMS derives `instance_id` from `{class_id}_id` (e.g. `product_id`) or any `*_id` field.

```bash
curl -sS -X POST 'http://localhost:8002/api/v1/database/demo_db/instances/Product/create?branch=main' \
  -H 'Content-Type: application/json' \
  -d '{
    "data": {"Product ID": "PROD-1", "Name": "Apple"},
    "metadata": {}
  }'
```

Expected: HTTP `202` + `CommandResult` with `command_id`, then poll:
- `GET /api/v1/commands/{command_id}/status`

### 4) Multi-hop graph query

Safety defaults matter; multi-hop can explode. Prefer setting:
- `max_nodes`, `max_edges`, `max_paths`, `no_cycles=true`, and `include_paths=false` unless needed.

```bash
curl -sS -X POST 'http://localhost:8002/api/v1/graph-query/demo_db' \
  -H 'Content-Type: application/json' \
  -d '{
    "start_class": "Product",
    "hops": [{"predicate": "owned_by", "target_class": "Client"}],
    "filters": {"product_id": "PROD-1"},
    "limit": 10,
    "max_nodes": 200,
    "max_edges": 500,
    "no_cycles": true,
    "include_documents": true
  }'
```

## WIP / Deferred (explicitly not ready for FE)

- **Projections (materialized views)** endpoints under `/api/v1/projections/...` are skeleton/fallback.
  - Use `/api/v1/graph-query/{db_name}` instead.
- RBAC/tenancy is not enforced yet.
- Field-level provenance end-to-end is not complete yet.

## Related Docs

- `docs/ARCHITECTURE.md`
- `docs/IDEMPOTENCY_CONTRACT.md`
- `docs/OPERATIONS.md`
