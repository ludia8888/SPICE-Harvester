# SPICE HARVESTER Architecture Diagrams (Current)

> Updated: 2026-01-08  
> Canonical architecture reference: `docs/ARCHITECTURE.md`  
> This file focuses on diagrams and major flows that match the current code and `docker-compose.full.yml`.

## 1) Service Topology

```mermaid
graph TD
  UI[Clients] --> BFF[BFF :8002]
  BFF --> AGENT[Agent :8004]
  BFF --> OMS[OMS :8000]
  BFF --> FUNNEL[Funnel :8003]
  BFF --> PG[(Postgres)]
  BFF --> LFS[(lakeFS + MinIO)]

  AGENT --> BFF
  AGENT --> PG
  AGENT --> S3[(Event Store S3/MinIO)]

  OMS --> TDB[(TerminusDB)]
  OMS --> S3[(Event Store S3/MinIO)]

  S3 --> RELAY[message-relay]
  RELAY --> KAFKA[(Kafka)]

  KAFKA --> IW[instance-worker]
  KAFKA --> OW[ontology-worker]
  KAFKA --> PW[projection-worker]
  KAFKA --> SPW[search-projection-worker]
  KAFKA --> PLW[pipeline-worker]
  KAFKA --> OBJ[objectify-worker]
  KAFKA --> CSW[connector-sync-worker]
  PLS[pipeline-scheduler] --> KAFKA

  IW --> TDB
  OW --> TDB
  IW --> PG
  OW --> PG
  PW --> PG
  PW --> ES[(Elasticsearch)]
  SPW --> ES

  PLW --> LFS
  PLW --> PG
  OBJ --> LFS
  OBJ --> PG
  PLS --> PG
  ING[ingest-reconciler-worker] --> PG

  CTS[connector-trigger-service] --> PG
  CTS --> KAFKA
```

Notes:
- BFF runs dataset/objectify outbox workers in-process.
- search-projection-worker is optional (`ENABLE_SEARCH_PROJECTION=false` by default).
- Agent service is internal; it executes LangGraph runs, calls BFF for actions, and logs events/audit trails to S3/Postgres.

## 2) Event Sourcing Write Path

```mermaid
sequenceDiagram
  participant Client
  participant BFF
  participant OMS
  participant S3 as EventStore (S3/MinIO)
  participant Relay as message-relay
  participant Kafka
  participant Worker
  participant TerminusDB
  participant ES

  Client->>BFF: write request
  BFF->>OMS: async write
  OMS->>S3: append command envelope
  S3->>Relay: tail indexes
  Relay->>Kafka: publish command
  Kafka->>Worker: consume command
  Worker->>TerminusDB: apply write
  Worker->>S3: append domain event
  S3->>Relay: tail indexes
  Relay->>Kafka: publish domain event
  Kafka->>ES: projection-worker updates index
```

## 3) Data Plane (Ingest -> Pipeline -> Objectify)

```mermaid
flowchart LR
  Upload[CSV/Excel/Media/Connector] --> BFF
  BFF --> LFS[lakeFS + MinIO]
  BFF --> PG[(Dataset Registry + Outbox)]
  PG --> PLW[pipeline-worker]
  PLW --> LFS
  PLW --> PG
  PG --> OBJ[objectify-worker]
  OBJ --> OMS
  OMS --> S3[Event Store]
  S3 --> Relay
  Relay --> Kafka
  Kafka --> IW[instance-worker]
  IW --> TDB[TerminusDB]
  Kafka --> PW[projection-worker]
  PW --> ES[Elasticsearch]
```

Notes:
- Pipeline steps include filter/join/compute/cast/dedupe/aggregate with schema contracts and expectations.
- Objectify submits async bulk instance commands to OMS.

## 4) Connector Flow (Google Sheets)

```mermaid
flowchart LR
  Sheets[Google Sheets] --> Trigger[connector-trigger-service]
  Trigger --> PG[(Connector Registry)]
  Trigger --> Kafka[(connector-updates)]
  Kafka --> Sync[connector-sync-worker]
  Sync --> BFF
  BFF --> LFS
  BFF --> PG
```

## 5) Read/Query Path

- Primary read model: Elasticsearch projection.
- Graph queries combine TerminusDB traversal + ES document fetch.
- Fallback to OMS/TerminusDB when ES is unavailable.
- Access policy may mask/filter rows/fields in read responses.
- Audit/Lineage queries are served by BFF from Postgres.

## 6) Idempotency & Ordering

- Global idempotency key: `event_id` (same id produces side effects once).
- Aggregate ordering: `sequence_number` with `expected_seq` OCC guard.
- `ProcessedEventRegistry` ensures consumer idempotency and ordering.

## 7) Where to look in code

- `backend/bff/`: API gateway and orchestration
- `backend/oms/`: TerminusDB control + async write APIs
- `backend/agent/`: LangGraph agent runtime + audit/event logging
- `backend/pipeline_worker/`: Spark pipeline execution
- `backend/objectify_worker/`: mapping spec -> instance creation
- `backend/connector_trigger_service/`, `backend/connector_sync_worker/`: connector ingest flow
- `backend/message_relay/`: S3 tail -> Kafka publisher
