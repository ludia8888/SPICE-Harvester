# SPICE HARVESTER

[한국어 README](README.ko.md)

## 1) What it is

SPICE HARVESTER is an **Event Sourcing + CQRS** platform that combines:
- **Ontology + graph relationships** (TerminusDB)
- **Search/read projections** (Elasticsearch)
- **Data plane** for raw ingest → pipeline transforms → objectify into ontology instances (lakeFS + Spark pipeline worker)
- **Governance + lineage + audit** so every change is traceable and reproducible

It is designed to survive **at-least-once** delivery (Kafka redeliveries, worker restarts, replays) without duplicate side effects.

## 2) Who it’s for / Use cases

- Teams building a **knowledge graph** (product ↔ supplier ↔ customer, lineage, ownership, dependencies)
- Data platform teams that need **raw ingest → ETL/cleansing → ontology mapping → search**
- Data governance / platform teams needing **auditability** and reproducible state
- “Spreadsheet-first” operations that want to **infer schema**, map columns, and import to a governed model
- Systems integrating multiple upstream sources where **retries/duplicates are normal** and correctness must be enforced
- PoCs that must later graduate to production without rewriting the core data model

## 3) What makes it different

- **Correctness-first**: durable idempotency (`processed_events`) + ordering guard (`aggregate_versions`) + write-side OCC (`expected_seq`) are enforced and tested with no mocks.
- **Lineage-first**: provenance/audit are first-class concepts (event → artifact links) so you can explain *why* a node/edge exists.
- **Rebuildable**: read models (ES, lineage/audit projections) can be (re)materialized by replaying the Event Store instead of treating ES as truth.
- **Data plane separation**: lakeFS + dataset registries manage raw/cleaned artifacts independent of the graph store.

## 3.1) Feature inventory (current implementation)

- **Ontology & graph**: class/property/relationship CRUD, branches/merge/rollback, query, and relationship validation via OMS/BFF.
- **Relationship modeling**: LinkType + RelationshipSpec (FK, join table, object-backed), link indexing with dangling policies, and link edits overlay.
- **Governance**: protected branches, proposals/approval, merge checks, health gates, and schema migration planning for object/backing changes.
- **Data plane ingest**: CSV/Excel/media uploads, Google Sheets connector, Funnel type inference/profiling, lakeFS dataset versioning, dataset registry + ingest outbox + reconciler.
- **Pipeline execution**: preview/build/deploy, Spark transforms (filter/join/compute/cast/rename/union/dedupe/groupBy/aggregate/window/pivot), schema contracts + expectations, pipeline scheduler.
- **Objectify**: mapping spec versioning, auto/suggested mapping, KeySpec (primary/title) enforcement, PK uniqueness gates, edits migration.
- **Event sourcing correctness**: S3/MinIO Event Store, message-relay to Kafka, processed_event_registry, sequence allocator, command status (HTTP + WS), admin replay/recompute tools.
- **Query & projection**: Elasticsearch projections, graph query federation + label query, optional search-projection worker.
- **Access policy**: row/column masking applied to instance/query/graph reads via dataset-level policies.
- **Ops & security**: audit logs, lineage graph, health/config/monitoring endpoints, tracing/metrics, rate limiting + input sanitizer, auth token guard.
- **AI/LLM**: natural-language query plan/answer endpoints + Context7 knowledge base integration + optional Agent (LangGraph) orchestration service.

Full API list: `docs/API_REFERENCE.md`

## 4) Architecture

```mermaid
flowchart LR
  Client --> BFF
  Client --> Agent
  BFF --> OMS
  BFF --> Funnel
  BFF --> PG[(Postgres: registry/outbox)]
  BFF --> LFS[(lakeFS + MinIO)]

  Agent --> BFF
  Agent --> OMS
  Agent --> Funnel
  Agent --> PG
  Agent --> EventStore

  OMS -->|append command| EventStore[(S3/MinIO Event Store)]
  EventStore --> Relay[message-relay (S3 tail -> Kafka)]
  Relay --> Kafka[(Kafka)]

  Kafka --> InstanceWorker[instance-worker]
  Kafka --> OntologyWorker[ontology-worker]
  Kafka --> ProjectionWorker[projection-worker]

  InstanceWorker -->|write graph| TerminusDB[(TerminusDB)]
  InstanceWorker -->|append domain events| EventStore
  InstanceWorker --> PG

  OntologyWorker -->|write schema| TerminusDB
  OntologyWorker -->|append domain events| EventStore
  OntologyWorker --> PG

  ProjectionWorker --> ES[(Elasticsearch)]
  ProjectionWorker --> Redis[(Redis)]
  ProjectionWorker --> PG

  LFS --> PipelineWorker[pipeline-worker]
  PipelineWorker --> LFS
  PG --> ObjectifyWorker[objectify-worker]
  ObjectifyWorker --> Kafka
```

**Truth sources (SSoT)**:
- Graph/schema authority: TerminusDB
- Immutable log: S3/MinIO Event Store (commands + domain events)
- Control plane: Postgres (registries, outbox, idempotency, ordering, gates)
- Data plane: lakeFS + MinIO (dataset/artifact versions)

## 5) Reliability Contract (short)

1) **Delivery**: Publisher/Kafka are **at-least-once**. Consumers must be idempotent.  
2) **Idempotency key**: the global idempotency key is `event_id` (same `event_id` must create side effects at most once).  
3) **Ordering**: aggregate-level `sequence_number` is the truth; stale events must be ignored.  
4) **OCC**: write-side commands carry `expected_seq`; mismatch is a real conflict (**409**) not a “silent last-write-wins”.

Full contract: `docs/IDEMPOTENCY_CONTRACT.md`

## 6) Quick Start (5 min)

Prereq: Docker + Docker Compose.

```bash
git clone https://github.com/ludia8888/SPICE-Harvester.git
cd SPICE-Harvester

# Optional: avoid local port conflicts (example only)
cp .env.example .env

docker compose -f docker-compose.full.yml up -d
```

Health:

```bash
curl -fsS http://localhost:8000/health
curl -fsS http://localhost:8002/api/v1/health
curl -fsS http://localhost:8003/health
```

## 6.1) Frontend (Vite + React)

Prereq: Node 20+.

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

## 7) E2E demo (single PoC scenario)

Scenario: **Customer + Product(owned_by Customer)**, then query the relationship via multi-hop federation.

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
      {"name":"customer_id","type":"string","required":true},
      {"name":"name","type":"string","required":true}
    ],
    "relationships":[]
  }' | jq -r '.data.command_id')

PROD_ONTO_CMD=$(curl -fsS -X POST "http://localhost:8002/api/v1/databases/${DB}/ontology" \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"Product",
    "label":"Product",
    "properties":[
      {"name":"product_id","type":"string","required":true},
      {"name":"name","type":"string","required":true}
    ],
    "relationships":[
      {"predicate":"owned_by","target":"Customer","label":"Owned By","cardinality":"n:1"}
    ]
  }' | jq -r '.data.command_id')

curl -fsS "http://localhost:8002/api/v1/commands/${CUST_ONTO_CMD}/status" | jq .
curl -fsS "http://localhost:8002/api/v1/commands/${PROD_ONTO_CMD}/status" | jq .

# 3) Create instances (BFF label API; async 202) and capture command_ids
CUST_CMD=$(curl -fsS -X POST "http://localhost:8002/api/v1/databases/${DB}/instances/Customer/create" \
  -H 'Content-Type: application/json' \
  -d '{"data":{"customer_id":"cust_001","name":"Alice"}}' | jq -r '.command_id')

PROD_CMD=$(curl -fsS -X POST "http://localhost:8002/api/v1/databases/${DB}/instances/Product/create" \
  -H 'Content-Type: application/json' \
  -d '{"data":{"product_id":"prod_001","name":"Shirt","owned_by":"Customer/cust_001"}}' | jq -r '.command_id')

# 4) Observe async completion (BFF command status proxy)
curl -fsS "http://localhost:8002/api/v1/commands/${CUST_CMD}/status" | jq .
curl -fsS "http://localhost:8002/api/v1/commands/${PROD_CMD}/status" | jq .

# 5) Multi-hop query (BFF federation)
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

Expected: the response includes nodes/edges and each node exposes `data_status=FULL|PARTIAL|MISSING` so UI can distinguish “index lag” from “missing entity”.

## 7.1) Branch-based what-if (no data copy)

Branch virtualization lets you run “what if we change X?” simulations without copying data:
- Graph/schema reads run on a TerminusDB branch (copy-on-write).
- ES payload resolves with overlay: branch index → fallback to main index (best-effort).
- Branch deletes are tombstoned to prevent fallback resurrection (tombstoned nodes return `data_status=MISSING` + `index_status.tombstoned=true`).
- Deletion is terminal per aggregate (recreating the same ID on the same branch is blocked by OCC and returns 409).

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

## 8) Testing (no mocks)

Fast unit tests (no docker):

```bash
make backend-unit
```

Production gate:

```bash
PYTHON_BIN=python3.12 ./backend/run_production_tests.sh --full
```

Pipeline transform/cleansing E2E (full stack required):

```bash
RUN_PIPELINE_TRANSFORM_E2E=true PYTHON_BIN=python3.12 ./backend/run_production_tests.sh --full
```

Chaos (destructive; stops/restarts infra and crashes workers on purpose):

```bash
PYTHON_BIN=python3.12 ./backend/run_production_tests.sh --full --chaos-lite
PYTHON_BIN=python3.12 ./backend/run_production_tests.sh --full --chaos-out-of-order
PYTHON_BIN=python3.12 SOAK_SECONDS=600 SOAK_SEED=123 ./backend/run_production_tests.sh --full --chaos-soak
```

### Coverage (pytest-cov + Codecov)

```bash
make backend-coverage
```

CI: `.github/workflows/ci.yml` uploads `coverage.xml` to Codecov.  
For private repos, set `CODECOV_TOKEN` in GitHub Actions secrets.

### Performance (k6)

Requires the full docker stack running.

```bash
make backend-perf-k6-smoke
```

See `backend/perf/README.md` for tuning / cleanup.

## 9) Limitations / PoC vs Production

- **No Saga/compensation orchestration yet** (planned): failures are observable via command status, but automated compensation is not shipped.
- **No automated drift reconciliation/backfill pipeline yet** (planned): projections can be replayed, but fully managed “reindex/backfill jobs + SLAs” are not turnkey.
- **DLQ is a topic, not a full ops workflow by default**: projection sends to DLQ after retries, but continuous DLQ reprocessing/alerting needs production wiring.
- **Security model is PoC-grade** unless you harden it (authn/authz, tenant isolation, secrets, rate limits, audit retention).
- **Capacity planning** (indexes/retention/partitioning) must be done before high TPS (especially Postgres registry growth).

## 10) Docs

- Index: `docs/README.md`
- Architecture: `docs/ARCHITECTURE.md`
- Reliability contract: `docs/IDEMPOTENCY_CONTRACT.md`
- Ops/runbook: `docs/OPERATIONS.md`, `backend/PRODUCTION_MIGRATION_RUNBOOK.md`
- Production tests: `backend/docs/testing/OMS_PRODUCTION_TEST_README.md`
