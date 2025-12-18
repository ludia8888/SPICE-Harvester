# SPICE HARVESTER

[한국어 README](README.ko.md)

## 1) What it is

SPICE HARVESTER is an **Event Sourcing + CQRS** platform for managing **ontology + graph relationships** (TerminusDB) and **document payload/search** (Elasticsearch), with a correctness layer designed to survive **at-least-once** delivery (Kafka redeliveries, worker restarts, replays) without producing duplicate side effects.

## 2) Who it’s for / Use cases

- Teams building a **knowledge graph** (product ↔ supplier ↔ customer, lineage, ownership, dependencies)
- Data governance / platform teams needing **auditability** and reproducible state
- “Spreadsheet-first” operations that want to **infer schema**, map columns, and import to a governed model
- Systems integrating multiple upstream sources where **retries/duplicates are normal** and correctness must be enforced
- PoCs that must later graduate to production without rewriting the core data model

## 3) What makes it different

- **Correctness-first**: durable idempotency (`processed_events`) + ordering guard (`aggregate_versions`) + write-side OCC (`expected_seq`) are enforced and tested with no mocks.
- **Lineage-first**: provenance/audit are first-class concepts (event → artifact links) so you can explain *why* a node/edge exists.
- **Rebuildable**: read models (ES, lineage/audit projections) can be (re)materialized by replaying the Event Store instead of treating ES as truth.

## 4) Architecture

```mermaid
flowchart LR
  Client --> BFF
  BFF --> OMS
  OMS -->|append command| EventStore[(S3/MinIO Event Store)]
  EventStore --> Relay[message-relay (S3 tail -> Kafka)]
  Relay --> Kafka[(Kafka)]

  Kafka --> InstanceWorker[instance-worker]
  Kafka --> OntologyWorker[ontology-worker]

  InstanceWorker -->|write graph| TerminusDB[(TerminusDB)]
  InstanceWorker -->|append domain events| EventStore
  InstanceWorker --> PG[(Postgres: processed_events + aggregate_versions)]

  OntologyWorker -->|write schema| TerminusDB
  OntologyWorker -->|append domain events| EventStore
  OntologyWorker --> PG

  Kafka --> ProjectionWorker[projection-worker]
  ProjectionWorker --> ES[(Elasticsearch)]
  ProjectionWorker --> Redis[(Redis)]
  ProjectionWorker --> PG
```

**Truth sources (SSoT)**:
- Graph/schema authority: TerminusDB
- Immutable log: S3/MinIO Event Store (commands + domain events)
- Correctness registry: Postgres (idempotency + ordering + seq allocator)

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
curl -fsS http://localhost:8002/health
curl -fsS http://localhost:8003/health
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
CUST_ONTO_CMD=$(curl -fsS -X POST "http://localhost:8002/api/v1/database/${DB}/ontology" \
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

PROD_ONTO_CMD=$(curl -fsS -X POST "http://localhost:8002/api/v1/database/${DB}/ontology" \
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
CUST_CMD=$(curl -fsS -X POST "http://localhost:8002/api/v1/database/${DB}/instances/Customer/create" \
  -H 'Content-Type: application/json' \
  -d '{"data":{"customer_id":"cust_001","name":"Alice"}}' | jq -r '.command_id')

PROD_CMD=$(curl -fsS -X POST "http://localhost:8002/api/v1/database/${DB}/instances/Product/create" \
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
curl -fsS -X PUT "http://localhost:8002/api/v1/database/${DB}/instances/Product/prod_001/update?branch=feature/whatif&expected_seq=<expected_seq>" \
  -H 'Content-Type: application/json' \
  -d '{"data":{"name":"Shirt (WhatIf)"}}'
```

## 8) Testing (no mocks)

Production gate:

```bash
PYTHON_BIN=python3.12 ./backend/run_production_tests.sh --full
```

Chaos (destructive; stops/restarts infra and crashes workers on purpose):

```bash
PYTHON_BIN=python3.12 ./backend/run_production_tests.sh --full --chaos-lite
PYTHON_BIN=python3.12 ./backend/run_production_tests.sh --full --chaos-out-of-order
PYTHON_BIN=python3.12 SOAK_SECONDS=600 SOAK_SEED=123 ./backend/run_production_tests.sh --full --chaos-soak
```

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
