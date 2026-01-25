# SPICE HARVESTER

> Ontology + Event Sourcing + Data Plane + Tool-using LLM Agent

- Korean README (canonical): `README.md`
- Docs index: `docs/README.md`
- API reference (auto-generated): `docs/API_REFERENCE.md`
- Architecture (includes auto-generated sections): `docs/ARCHITECTURE.md`
- Action writeback design/philosophy: `docs/ACTION_WRITEBACK_DESIGN.md`
- Pipeline Agent (single autonomous loop + MCP tools): `docs/PIPELINE_AGENT.md`

---

## Table of contents

- [1) One-liner](#1-one-liner)
- [2) What problems it solves](#2-what-problems-it-solves)
- [3) Core philosophy: deterministic core + falsifiable decisions](#3-core-philosophy-deterministic-core--falsifiable-decisions)
- [4) System building blocks (services / SSoT / planes)](#4-system-building-blocks-services--ssot--planes)
- [5) Key capabilities (current implementation)](#5-key-capabilities-current-implementation)
- [6) Action writeback + Decision Simulation (what-if)](#6-action-writeback--decision-simulation-what-if)
- [7) Pipeline Agent (single autonomous loop + MCP tools)](#7-pipeline-agent-single-autonomous-loop--mcp-tools)
- [8) Security / governance / audit](#8-security--governance--audit)
- [9) Observability (OpenTelemetry)](#9-observability-opentelemetry)
- [10) Local quickstart](#10-local-quickstart)
- [11) Tests](#11-tests)
- [12) Docs automation (keep docs in sync)](#12-docs-automation-keep-docs-in-sync)
- [13) Repo layout](#13-repo-layout)
- [14) Limitations / roadmap](#14-limitations--roadmap)
- [15) Hands-on demos (curl)](#15-hands-on-demos-curl)

---

## 1) One-liner

SPICE HARVESTER combines **ontology/graph (SSoT) + immutable event logs + a versioned data plane (lakeFS)** and adds a **tool-using LLM Agent (single loop + deterministic tools)** so that data/ops workflows become **auditable, reproducible, and simulation-first**.

---

## 2) What problems it solves

Real data/operations are adversarial by default:

1) **At-least-once delivery is normal**: Kafka redeliveries, worker restarts, retries.
2) **Silent last-write-wins is an incident**: concurrency conflicts must be explicit (not silent overwrites).
3) **Messy inputs**: spreadsheet-first workflows and constantly changing schemas.
4) **Policy + permissions + state are entangled**: “Can we do this?” depends on the current state and governance.
5) **Partial failures are normal**: index lag, degraded overlays, missing permissions, stale reads.

SPICE HARVESTER addresses this by:
- enforcing a **correctness layer** (idempotency/ordering/OCC) on Postgres,
- treating Elasticsearch as a **rebuildable read model** (never the truth),
- forcing writes through **simulate → approve (HITL) → submit (async)** to preserve accountability and reproducibility.

---

## 3) Core philosophy: deterministic core + falsifiable decisions

### 3.1 Split: Data Plane / Control Plane / Read Model

- **Data Plane (versioned artifacts)**: raw ingest → transforms → datasets → materialization (lakeFS + MinIO)
- **Control Plane (policy/approval/orchestration)**: plan/approval/simulate/submit/run/registry (Postgres)
- **Read Model (search/projection)**: Elasticsearch overlay + lineage/audit projections

“LLM-native” does not mean the LLM executes arbitrary actions. It means:
- the LLM only produces a **typed plan**,
- the system enforces **validation / policy / approvals / simulation-first** before any execution.

### 3.2 Sources of truth (SSoT)

- Graph/ontology authority: **TerminusDB**
- Immutable log (commands + domain events): **S3/MinIO Event Store**
- Correctness + registries + gates + approvals: **Postgres**
- Data artifact versions: **lakeFS + MinIO**
- Elasticsearch is a **materialized view** (rebuildable)

---

## 4) System building blocks (services / SSoT / planes)

### 4.1 Microservices (local default ports)

- **BFF (8002)**: external entrypoint (frontend contract), routing, policy/rate limits, Pipeline Plans + Pipeline Agent API
- **OMS (8000)**: ontology/graph management (internal; use debug ports when needed)
- **Funnel (8003)**: type inference/profiling (internal)
- **Agent**: tool runner (single sequential loop, internal; invoked via BFF only)
- **Workers**: pipeline/objectify/instance/projection/action-worker (event-driven)
- **Infra**: Kafka, Postgres, Redis, MinIO(S3), lakeFS, Elasticsearch, OTel collector, Jaeger/Prometheus/Grafana

See `docs/ARCHITECTURE.md` for the full compose inventory.

### 4.2 High-level architecture

```mermaid
flowchart LR
  Client --> BFF
  BFF --> OMS
  BFF --> Funnel
  BFF --> Agent
  BFF --> PG[(Postgres: registry/outbox/gates)]
  BFF --> LFS[(lakeFS + MinIO)]

  OMS -->|append command| EventStore[(S3/MinIO Event Store)]
  EventStore --> Relay["message-relay: S3 tail to Kafka"]
  Relay --> Kafka[(Kafka)]

  Kafka --> OntologyWorker[ontology-worker]
  Kafka --> InstanceWorker[instance-worker]
  Kafka --> ProjectionWorker[projection-worker]
  Kafka --> ActionWorker[action-worker]

  OntologyWorker --> TerminusDB[(TerminusDB)]
  InstanceWorker --> TerminusDB
  ProjectionWorker --> ES[(Elasticsearch)]

  ActionWorker --> LFS
  ActionWorker --> EventStore
  ActionWorker --> ES

  Agent --> BFF
  Agent --> PG
  Agent --> EventStore
```

---

## 5) Key capabilities (current implementation)

### 5.1 Ontology / graph
- Class/property/relationship CRUD, branching/merge/rollback
- Relationship modeling: LinkType + RelationshipSpec (FK / join table / object-backed), dangling policies, link edits overlay
- Graph query federation (multi-hop), label-based query

### 5.2 Data plane (ingest → pipeline → objectify)
- CSV/Excel/media upload, Google Sheets connector
- Funnel type inference/profiling
- lakeFS dataset versioning + dataset registry/outbox/reconciler
- Spark transforms (filter/join/compute/cast/rename/union/dedupe/groupBy/aggregate/window/pivot)
- Objectify: mapping spec versioning + KeySpec (primary/title) + PK uniqueness gates + edits migration

### 5.3 Correctness (idempotency/ordering/OCC) + replayability
- `processed_event_registry` (idempotency), `aggregate_versions` (ordering), `expected_seq` (OCC) as a hard contract
- Event Store (S3/MinIO) + message-relay → Kafka for replayable pipelines
- ES/projections are rebuildable read models

Full contract: `docs/IDEMPOTENCY_CONTRACT.md`

### 5.4 Ops / audit / lineage
- Audit logs / lineage graph / health/config/monitoring endpoints
- Enterprise error taxonomy (structured `enterprise.*` payload) for deterministic ops/agent branching
- OpenTelemetry tracing/metrics (collector + jaeger/prometheus/grafana)

---

## 6) Action writeback + Decision Simulation (what-if)

Core idea: writes are declared as **intent-only**, then executed safely via **simulate → approve → submit**.

### 6.1 ActionLog is an ontology object (not just a DB row)

ActionLog captures:
- what decision was made,
- why it was made (policy/inputs/context),
- and what happened (conflicts, criteria failures, permission denials) in a structured schema.

This makes “learning from past decisions” and meta-cognition (pattern analysis) possible.

See: `docs/ACTION_WRITEBACK_DESIGN.md`

### 6.2 End-to-end flow

1) **simulate (dry-run)**: evaluate policy/permissions/criteria/conflicts, compute diffs (overlay/lakeFS/ES effects)
2) **HITL approval**: human approves/rejects based on the simulation output
3) **submit (async)**: worker applies lakeFS commit + appends EventStore event + writes ES overlay docs
4) **read path**: BFF reads use overlay and can represent “pending/partial/degraded”

### 6.3 Decision simulation levels

- **Level 1 (input injection)**: vary input variables (e.g. `discount_rate=10%` vs `15%`, conflict policy, branch options)
- **Level 2 (base-state injection, safely)**: inject assumptions into `observed_base` / base snapshots for what-if
  - assumptions never become SSoT
  - injected fields are explicitly included in the simulation result for accountability

---

## 7) Pipeline Agent (single autonomous loop + MCP tools)

Goal: execute the user’s natural-language intent **without semantic rewrites**.

### 7.1 Guarantees

- The model does not emit DSL strings; it **only** composes plans via **tool calls (MCP)**.
- The server performs **no silent heuristic rewrites**. Repairs happen only via explicit edit/patch tools.
- Performance: the loop supports **batched tool calls per inference** and keeps the prompt as **append-only JSONL** for provider-side prefix caching; the server can perform **deterministic compaction** when the prompt grows too large.

### 7.2 Key endpoints (summary)

- `POST /api/v1/agent/pipeline-runs`: (UI Chat) natural language → analysis / plan assembly → (optional) validate/preview
- `POST /api/v1/pipeline-plans/compile`: compile API (delegates to the same single-loop runtime)
- `POST /api/v1/pipeline-plans/context-pack`: deterministic dataset summary (context pack)
- `POST /api/v1/pipeline-plans/{plan_id}/preview`: plan preview (sampling-based)
- `POST /api/v1/pipeline-plans/{plan_id}/inspect-preview`: inspect preview (columns/stats)
- `POST /api/v1/pipeline-plans/{plan_id}/evaluate-joins`: join quality evaluation

Full list: `docs/API_REFERENCE.md`

---

## 8) Security / governance / audit

- Access policy: row/column masking and filtering on instance/query/graph reads
- Input sanitizer: rule-based prompt-injection and malicious payload reduction
- Admin endpoints: guarded via `X-Admin-Token` (disabled when unset)
- Audit/trace: trace context preserved on ActionLog/AgentRun for “who/when/why” reconstruction

---

## 9) Observability (OpenTelemetry)

The full stack collects traces/metrics via `otel-collector-config.yml`.

- Jaeger: traces (see ports in `docs/ARCHITECTURE.md`)
- Prometheus/Grafana: metrics/dashboards
- Cross-service correlation keys: `request_id`, `actor`, `plan_id`, `action_log_id`, etc.
- ⚠️ Limitation: batch/scheduler flows that start without an HTTP request may log `corr_id` as `-`. In that case, start tracing from `event_id`/`job_id` (or `command_id`).

Ops guide: `docs/OPERATIONS.md`

---

## 10) Local quickstart

### 10.1 Backend stack (5–10 min)

Prereq: Docker + Docker Compose

```bash
git clone https://github.com/ludia8888/SPICE-Harvester.git
cd SPICE-Harvester
cp .env.example .env  # optional
docker compose -f docker-compose.full.yml up -d
```

Health check (BFF is the only external endpoint by default):

```bash
curl -fsS http://localhost:8002/api/v1/health
```

Need direct OMS/Funnel access for debugging? (debug ports):

```bash
docker compose -f docker-compose.full.yml -f backend/docker-compose.debug-ports.yml up -d
curl -fsS http://localhost:8000/health
curl -fsS http://localhost:8003/health
```

### 10.2 Frontend dev (Vite + React)

Prereq: Node 20+

```bash
cd frontend
cp .env.example .env  # optional
npm ci
npm run dev
```

Dev/preview proxy:
- requests under `/api/*` proxy to `VITE_API_PROXY_TARGET` (or `BFF_BASE_URL`, default `http://localhost:8002`)
- client calls default to `VITE_API_BASE_URL=/api/v1`

Preview (build-like) with proxy enabled:

```bash
npm run build
npm run preview
```

---

## 11) Tests

Fast unit tests (no docker required):

```bash
make backend-unit
```

Production gate (requires the full local stack):

```bash
make backend-prod-full
```

Coverage:

```bash
make backend-coverage
```

---

## 12) Docs automation (keep docs in sync)

Some docs are generated from code to reduce drift:

```bash
python scripts/generate_api_reference.py
python scripts/generate_architecture_reference.py
python scripts/generate_backend_methods.py
python scripts/generate_error_taxonomy.py
```

---

## 13) Repo layout

- `backend/`: BFF/OMS/Funnel/Agent + shared + workers
- `frontend/`: React + Blueprint.js UI
- `docs/`: system docs (design/ops/security/architecture/api)
- `scripts/`: doc generation / smoke tests / utilities
- `docker-compose.*.yml`: local stack

---

## 14) Limitations / roadmap

- The default is **simulate-first + HITL + policy enforcement**, not “fully autonomous execution”.
- Production hardening still needs environment-specific work: authn/authz, tenant isolation, secrets, retention/partitioning, quotas, SLAs.
- See: `docs/ARCHITECTURE.md`, `docs/OPERATIONS.md`, `docs/SECURITY.md`, `docs/PIPELINE_AGENT.md`

---

## 15) Hands-on demos (curl)

### 15.1 E2E demo: Customer → Product(owned_by Customer) + multi-hop query

Scenario: create a DB, define `Customer` + `Product` ontology (with `owned_by`), create instances, then query via federation.

Tip: examples below use `jq` for convenience.

```bash
DB=demo_db_$(date +%s)

# 1) Create DB (BFF -> OMS; async 202)
DB_CMD=$(curl -fsS -X POST "http://localhost:8002/api/v1/databases" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"${DB}\",\"description\":\"demo\"}" | jq -r '.data.command_id')

curl -fsS "http://localhost:8002/api/v1/commands/${DB_CMD}/status" | jq .

# 2) Create ontologies (BFF -> OMS; async 202)
CUST_ONTO_CMD=$(curl -fsS -X POST "http://localhost:8002/api/v1/databases/${DB}/ontology" \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"Customer",
    "label":"Customer",
    "properties":[
      {"name":"customer_id","type":"xsd:string","label":"Customer ID","required":true},
      {"name":"name","type":"xsd:string","label":"Name","required":true}
    ],
    "relationships":[]
  }' | jq -r '.data.command_id')

PROD_ONTO_CMD=$(curl -fsS -X POST "http://localhost:8002/api/v1/databases/${DB}/ontology" \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"Product",
    "label":"Product",
    "properties":[
      {"name":"product_id","type":"xsd:string","label":"Product ID","required":true},
      {"name":"name","type":"xsd:string","label":"Name","required":true}
    ],
    "relationships":[
      {"predicate":"owned_by","target":"Customer","label":"Owned By","cardinality":"n:1"}
    ]
  }' | jq -r '.data.command_id')

curl -fsS "http://localhost:8002/api/v1/commands/${CUST_ONTO_CMD}/status" | jq .
curl -fsS "http://localhost:8002/api/v1/commands/${PROD_ONTO_CMD}/status" | jq .

# 3) Create instances (BFF label API; async 202)
CUST_CMD=$(curl -fsS -X POST "http://localhost:8002/api/v1/databases/${DB}/instances/Customer/create" \
  -H 'Content-Type: application/json' \
  -d '{"data":{"customer_id":"cust_001","name":"Alice"}}' | jq -r '.data.command_id // .command_id')

PROD_CMD=$(curl -fsS -X POST "http://localhost:8002/api/v1/databases/${DB}/instances/Product/create" \
  -H 'Content-Type: application/json' \
  -d '{"data":{"product_id":"prod_001","name":"Shirt","owned_by":"Customer/cust_001"}}' | jq -r '.data.command_id // .command_id')

curl -fsS "http://localhost:8002/api/v1/commands/${CUST_CMD}/status" | jq .
curl -fsS "http://localhost:8002/api/v1/commands/${PROD_CMD}/status" | jq .

# 4) Multi-hop query (BFF federation)
curl -fsS -X POST "http://localhost:8002/api/v1/graph-query/${DB}" \
  -H 'Content-Type: application/json' \
  -d '{
    "start_class":"Product",
    "hops":[{"predicate":"owned_by","target_class":"Customer"}],
    "filters":{"product_id":"prod_001"},
    "limit":10,
    "offset":0,
    "max_nodes":200,
    "max_edges":500,
    "no_cycles":true,
    "include_documents":true
  }' | jq .
```

Expected: the response includes nodes/edges and each node exposes `data_status=FULL|PARTIAL|MISSING` so the UI can distinguish “index lag” from “missing entity”.

### 15.2 Branch-based what-if (no data copy)

Branch virtualization lets you run “what if we change X?” simulations without copying data:
- Graph/schema reads run on a TerminusDB branch (copy-on-write).
- ES payload resolves with overlay: branch index → fallback to main index (best-effort).

```bash
# 1) Create a branch from main
curl -fsS -X POST "http://localhost:8002/api/v1/databases/${DB}/branches" \
  -H 'Content-Type: application/json' \
  -d '{"name":"feature/whatif","from_branch":"main"}'

# 2) Read via federation on the branch
curl -fsS -X POST "http://localhost:8002/api/v1/graph-query/${DB}?branch=feature/whatif" \
  -H 'Content-Type: application/json' \
  -d '{"start_class":"Product","hops":[],"filters":{"product_id":"prod_001"},"include_documents":true}' | jq .

# 3) Update on the branch (OCC): expected_seq comes from nodes[].index_status.event_sequence
curl -fsS -X PUT "http://localhost:8002/api/v1/databases/${DB}/instances/Product/prod_001/update?branch=feature/whatif&expected_seq=<expected_seq>" \
  -H 'Content-Type: application/json' \
  -d '{"data":{"name":"Shirt (WhatIf)"}}'
```
