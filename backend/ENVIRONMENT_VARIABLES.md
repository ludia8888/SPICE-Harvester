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
curl http://localhost:8002/health
curl http://localhost:8003/health
```

## Notes

- All passwords shown here are for development only
- Use strong, unique passwords in production
- Store secrets in a secure vault (AWS Secrets Manager, HashiCorp Vault, etc.)
- Never commit .env files to version control
- Use environment-specific configuration files
