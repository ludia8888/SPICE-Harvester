# 🔥 SPICE HARVESTER ENVIRONMENT VARIABLES (Steady State)

> This document is a human-friendly index.  
> Canonical defaults live in `.env.example`, and runtime contracts are documented in `docs/IDEMPOTENCY_CONTRACT.md`.

## 1) Service base URLs (local/dev)

```bash
OMS_BASE_URL=http://localhost:8000
BFF_BASE_URL=http://localhost:8002
FUNNEL_BASE_URL=http://localhost:8003
USE_HTTPS=false
VERIFY_SSL=false
# Funnel Excel analysis timeout (seconds)
FUNNEL_EXCEL_TIMEOUT_SECONDS=120
```

## 1.1) Auth + scope guard

```bash
# BFF auth enforcement
BFF_REQUIRE_AUTH=true

# Optional per-project scope header enforcement (X-DB-Name / X-Project)
BFF_REQUIRE_DB_SCOPE=false

# Token used by BFF/OMS (or set BFF_ADMIN_TOKEN / OMS_CLIENT_TOKEN separately)
ADMIN_TOKEN=change_me
```

## 1.2) Pipeline governance

```bash
# Protected branches (disallow direct edits to definitions)
PIPELINE_PROTECTED_BRANCHES=main

# Require approved proposals before deploying to protected branches
PIPELINE_REQUIRE_PROPOSALS=false
```

## 2) Core infra

### PostgreSQL (idempotency registry + seq allocator)

```bash
# Preferred: single DSN
POSTGRES_URL=postgresql://spiceadmin:spicepass123@localhost:5433/spicedb

# Or host/port components (used when POSTGRES_URL is not set)
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_USER=spiceadmin
POSTGRES_PASSWORD=spicepass123
POSTGRES_DB=spicedb
```

### Redis (command status + caching)

```bash
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=spicepass123
```

### TerminusDB (graph: ontology/schema + lightweight instance nodes)

```bash
TERMINUS_SERVER_URL=http://localhost:6363
TERMINUS_USER=admin
TERMINUS_ACCOUNT=admin
TERMINUS_KEY=admin
```

### Elasticsearch (read model / search)

```bash
ELASTICSEARCH_URL=http://localhost:9200
# Or host/port components (used when ELASTICSEARCH_URL is not set)
ELASTICSEARCH_HOST=localhost
ELASTICSEARCH_PORT=9200
ELASTICSEARCH_USERNAME=
ELASTICSEARCH_PASSWORD=
# Defaults for new indices (dev-safe values if ENVIRONMENT!=production)
ELASTICSEARCH_DEFAULT_SHARDS=1
ELASTICSEARCH_DEFAULT_REPLICAS=0
```

### Kafka

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Topics (canonical names via shared AppConfig)
INSTANCE_COMMANDS_TOPIC=instance_commands
ONTOLOGY_COMMANDS_TOPIC=ontology_commands
DATABASE_COMMANDS_TOPIC=database_commands
INSTANCE_EVENTS_TOPIC=instance_events
ONTOLOGY_EVENTS_TOPIC=ontology_events
PROJECTION_DLQ_TOPIC=projection_failures_dlq
```

### MinIO / S3 (Event Store)

```bash
MINIO_ENDPOINT_URL=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123

EVENT_STORE_BUCKET=spice-event-store
INSTANCE_BUCKET=instance-events
```

## 3) Event Sourcing correctness layer

### Event Store sequencing + idempotency

```bash
# Write-side atomic allocator (default: postgres)
EVENT_STORE_SEQUENCE_ALLOCATOR_MODE=postgres  # postgres|legacy|off
EVENT_STORE_SEQUENCE_SCHEMA=spice_event_registry
EVENT_STORE_SEQUENCE_HANDLER_PREFIX=write_side

# Same event_id, different payload -> contract violation handling
EVENT_STORE_IDEMPOTENCY_MISMATCH_MODE=error  # error|warn
```

### Durable processed-event registry (Worker / Projection)

```bash
ENABLE_PROCESSED_EVENT_REGISTRY=true
PROCESSED_EVENT_LEASE_TIMEOUT_SECONDS=900
PROCESSED_EVENT_HEARTBEAT_INTERVAL_SECONDS=30
PROCESSED_EVENT_OWNER=  # optional
```

### Publisher (S3 tail → Kafka)

```bash
EVENT_PUBLISHER_CHECKPOINT_KEY=checkpoints/event_publisher.json
EVENT_PUBLISHER_POLL_INTERVAL=3
EVENT_PUBLISHER_BATCH_SIZE=200
EVENT_PUBLISHER_LOOKBACK_SECONDS=600
EVENT_PUBLISHER_LOOKBACK_MAX_KEYS=50
EVENT_PUBLISHER_DEDUP_MAX_EVENTS=10000
EVENT_PUBLISHER_DEDUP_CHECKPOINT_MAX_EVENTS=2000
EVENT_PUBLISHER_KAFKA_FLUSH_BATCH_SIZE=200
EVENT_PUBLISHER_KAFKA_FLUSH_TIMEOUT_SECONDS=10
EVENT_PUBLISHER_METRICS_LOG_INTERVAL_SECONDS=30
```

## 4) Workers (consumer groups + retry policy)

```bash
INSTANCE_WORKER_GROUP=instance-worker-group
ONTOLOGY_WORKER_GROUP=ontology-worker-group
PROJECTION_WORKER_GROUP=projection-worker-group

INSTANCE_WORKER_MAX_RETRY_ATTEMPTS=5
ONTOLOGY_WORKER_MAX_RETRY_ATTEMPTS=5
PROJECTION_WORKER_MAX_RETRIES=5

# Objectify worker
OBJECTIFY_JOBS_TOPIC=objectify-jobs
OBJECTIFY_JOBS_DLQ_TOPIC=objectify-jobs-dlq
OBJECTIFY_MAX_RETRIES=5
OBJECTIFY_BACKOFF_BASE_SECONDS=2
OBJECTIFY_BACKOFF_MAX_SECONDS=60
OBJECTIFY_ROW_BATCH_SIZE=1000

# Search projection worker (optional)
SEARCH_PROJECTION_GROUP=search-projection-worker
SEARCH_PROJECTION_DLQ_TOPIC=projection_failures_dlq
SEARCH_PROJECTION_MAX_RETRIES=5
SEARCH_PROJECTION_BACKOFF_BASE_SECONDS=2
SEARCH_PROJECTION_BACKOFF_MAX_SECONDS=60
```

### Dataset ingest recovery (outbox + reconciler)

```bash
# Outbox flush loop
ENABLE_DATASET_INGEST_OUTBOX_WORKER=true
DATASET_INGEST_OUTBOX_POLL_SECONDS=5

# Reconciler loop (auto-repair ingest atomicity)
ENABLE_DATASET_INGEST_RECONCILER=true
DATASET_INGEST_RECONCILER_POLL_SECONDS=60
DATASET_INGEST_RECONCILER_STALE_SECONDS=3600
DATASET_INGEST_RECONCILER_LIMIT=200
DATASET_INGEST_RECONCILER_LOCK_KEY=910214
INGEST_RECONCILER_PORT=8012
INGEST_RECONCILER_ALERT_WEBHOOK_URL=
INGEST_RECONCILER_ALERT_PUBLISHED_THRESHOLD=1
INGEST_RECONCILER_ALERT_ABORTED_THRESHOLD=1
INGEST_RECONCILER_ALERT_ON_ERROR=true
INGEST_RECONCILER_ALERT_COOLDOWN_SECONDS=300

### Objectify outbox + reconciler
# Outbox worker (Kafka publish)
ENABLE_OBJECTIFY_OUTBOX_WORKER=true
OBJECTIFY_OUTBOX_POLL_SECONDS=5
OBJECTIFY_OUTBOX_BATCH=50
OBJECTIFY_OUTBOX_FLUSH_TIMEOUT_SECONDS=10
OBJECTIFY_OUTBOX_BACKOFF_BASE_SECONDS=2
OBJECTIFY_OUTBOX_BACKOFF_MAX_SECONDS=60
OBJECTIFY_OUTBOX_CLAIM_TIMEOUT_SECONDS=300
OBJECTIFY_OUTBOX_WORKER_ID=
OBJECTIFY_OUTBOX_MAX_IN_FLIGHT=5
OBJECTIFY_OUTBOX_RETRIES=5
OBJECTIFY_OUTBOX_DELIVERY_TIMEOUT_MS=120000
OBJECTIFY_OUTBOX_REQUEST_TIMEOUT_MS=30000
OBJECTIFY_OUTBOX_PURGE_INTERVAL_SECONDS=3600
OBJECTIFY_OUTBOX_RETENTION_DAYS=7
# set to 0 to disable purge
OBJECTIFY_OUTBOX_PURGE_LIMIT=10000

# Reconciler loop (auto-repair objectify queueing)
ENABLE_OBJECTIFY_RECONCILER=true
OBJECTIFY_RECONCILER_POLL_SECONDS=60
OBJECTIFY_RECONCILER_STALE_SECONDS=600
OBJECTIFY_RECONCILER_ENQUEUED_STALE_SECONDS=900
# set to 0 to disable ENQUEUED stale republish
OBJECTIFY_RECONCILER_LOCK_KEY=910215
```

## 5) Feature flags (steady state)

```bash
ENABLE_EVENT_SOURCING=true
ENABLE_CQRS=true
```
LOG_VOLUME=/logs

# Resource Limits
MEMORY_LIMIT=2G
CPU_LIMIT=2
```

## Critical Environment Variables That Were Bug Sources

1. **DOCKER_CONTAINER=false** (for local development)
   - Must be false when running locally
   - Affects hostname resolution

2. **POSTGRES_PORT=5433 (host) vs 5432 (docker)**
   - Host port defaults to 5433 to avoid conflicts; containers use 5432.
   - Use `POSTGRES_URL` to avoid ambiguity.

3. **ELASTICSEARCH_PORT=9200** (NOT 9201)
   - Elasticsearch runs on 9200 in docker-compose.

## Example .env File

```bash
# Create a .env file in the backend directory with:

# Core
ENVIRONMENT=development
DEBUG=true
DOCKER_CONTAINER=false

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=spicedb
POSTGRES_USER=spiceadmin
POSTGRES_PASSWORD=spicepass123

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=spicepass123

# TerminusDB
TERMINUS_SERVER_URL=http://localhost:6363
TERMINUS_USER=admin
TERMINUS_KEY=admin

# Elasticsearch
ELASTICSEARCH_HOST=localhost
ELASTICSEARCH_PORT=9200
ELASTICSEARCH_USERNAME=
ELASTICSEARCH_PASSWORD=

# MinIO
MINIO_ENDPOINT_URL=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Event Sourcing
EVENT_STORE_BUCKET=spice-event-store
```

## Verification Commands

```bash
# Check all environment variables are set
./scripts/check_env.sh

# Validate database connections
python scripts/validate_connections.py

# Test service endpoints
curl http://localhost:8000/health
curl http://localhost:8002/api/v1/health
curl http://localhost:8003/health
```

## Notes

- All passwords shown here are for development only
- Use strong, unique passwords in production
- Store secrets in a secure vault (AWS Secrets Manager, HashiCorp Vault, etc.)
- Never commit .env files to version control
- Use environment-specific configuration files
