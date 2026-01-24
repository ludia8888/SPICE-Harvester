# SPICE HARVESTER Environment Variables (Index)

> Updated: 2026-01-08  \
> Note: This is a human-friendly index. Defaults live in `.env.example` and code in `backend/shared/config/*`. `.env` loading is opt-in via `SPICE_LOAD_DOTENV=true` (never loaded in Docker).

## Service URLs (local/dev)

BFF is the only external port by default. OMS/Funnel/Agent are internal unless you opt in to the debug ports override.

```bash
# External (host)
BFF_BASE_URL=http://localhost:8002

# Internal (docker network)
OMS_BASE_URL=http://oms:8000
FUNNEL_BASE_URL=http://funnel:8003
AGENT_BASE_URL=http://agent:8004

# Host direct access (debug ports only)
# OMS_BASE_URL=http://localhost:8000
# FUNNEL_BASE_URL=http://localhost:8003
# AGENT_BASE_URL=http://localhost:8004
```

## MCP / Context7

```bash
CONTEXT7_API_KEY=your_context7_api_key_here
CONTEXT7_WORKSPACE=your_workspace_name
MCP_CONFIG_PATH=./mcp-config.json
MCP_LOG_LEVEL=info
MCP_TIMEOUT=30000
```

## Agent Service

```bash
AGENT_PORT=8004
AGENT_HOST=127.0.0.1
AGENT_BFF_BASE_URL=http://localhost:8002
AGENT_BFF_TOKEN=change_me_agent_token
AGENT_EVENT_STORE_BUCKET=spice-agent-store
AGENT_REQUIRE_EVENT_STORE=true
AGENT_RUN_MAX_STEPS=50
AGENT_TOOL_TIMEOUT_SECONDS=30
AGENT_TOOL_MAX_PAYLOAD_BYTES=200000
AGENT_AUDIT_MAX_PREVIEW_CHARS=2000
AGENT_PROXY_TIMEOUT_SECONDS=30
```

## Auth

```bash
# BFF
BFF_REQUIRE_AUTH=true
BFF_REQUIRE_DB_SCOPE=false
BFF_ADMIN_TOKEN=change_me # comma-separated rotation supported: new_token,old_token
BFF_WRITE_TOKEN=
BFF_AGENT_TOKEN=change_me_agent_token # comma-separated rotation supported: new_token,old_token
ALLOW_INSECURE_BFF_AUTH_DISABLE=false

# OMS
OMS_REQUIRE_AUTH=true
OMS_ADMIN_TOKEN=change_me # comma-separated rotation supported: new_token,old_token
OMS_WRITE_TOKEN=
ALLOW_INSECURE_OMS_AUTH_DISABLE=false

# Shared fallback
ADMIN_API_KEY=
ADMIN_TOKEN=
```

## End-user JWT (Agent Sessions)

Agent Sessions (`/api/v1/agent-sessions/*`) run under delegated end-user auth.

```bash
USER_JWT_ENABLED=true

# Dev-only HS256 signing (the demo script can generate JWTs when this is set)
USER_JWT_HS256_SECRET=change_me_dev_secret # comma-separated rotation supported: new_secret,old_secret

# Production verification (prefer RS256 + JWKS)
USER_JWT_PUBLIC_KEY=
USER_JWT_JWKS_URL=
USER_JWT_ALGORITHMS=RS256,HS256

# Optional claims constraints
USER_JWT_ISSUER=
USER_JWT_AUDIENCE=
USER_JWT_TTL_SECONDS=3600
```

## LLM (Planner)

```bash
# OpenAI-compatible (recommended)
LLM_PROVIDER=openai_compat
LLM_BASE_URL=https://api.openai.com/v1
LLM_MODEL=gpt-5
OPENAI_API_KEY=change_me

# Alternative key name:
# LLM_API_KEY=change_me

# Dev-only (deterministic): mock planner responses (JSON, typically minified)
# LLM_PROVIDER=mock
# LLM_MOCK_JSON_AGENT_PLAN_COMPILE_V1='[{"plan":{...},"confidence":0.9,"notes":[],"warnings":[]}]'
# LLM_MOCK_DIR=./scripts/llm_mocks
```

## Rate Limiting

```bash
RATE_LIMIT_FAIL_OPEN=false
RATE_LIMIT_LOCAL_MAX_ENTRIES=10000
```

## Core Infra

```bash
# Postgres
POSTGRES_URL=postgresql://spiceadmin:spicepass123@localhost:5433/spicedb
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_USER=spiceadmin
POSTGRES_PASSWORD=spicepass123
POSTGRES_DB=spicedb

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=spicepass123

# TerminusDB
TERMINUS_SERVER_URL=http://localhost:6363
TERMINUS_USER=admin
TERMINUS_ACCOUNT=admin
TERMINUS_KEY=admin

# Elasticsearch
ELASTICSEARCH_URL=http://localhost:9200
ELASTICSEARCH_HOST=localhost
ELASTICSEARCH_PORT=9200
ELASTICSEARCH_USERNAME=
ELASTICSEARCH_PASSWORD=
ELASTICSEARCH_DEFAULT_SHARDS=1
ELASTICSEARCH_DEFAULT_REPLICAS=0

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:39092
```

Host tooling note: when running scripts outside Docker with docker-compose port overrides, settings also accepts `*_PORT_HOST` fallbacks (e.g. `POSTGRES_PORT_HOST`, `REDIS_PORT_HOST`, `ELASTICSEARCH_PORT_HOST`, `KAFKA_PORT_HOST`, `MINIO_PORT_HOST`, `LAKEFS_PORT_HOST`) when the corresponding `*_PORT`/URL vars are unset.

## Storage (S3/MinIO + lakeFS)

```bash
# MinIO
MINIO_ENDPOINT_URL=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
EVENT_STORE_BUCKET=spice-event-store
INSTANCE_BUCKET=instance-events

# TLS verification (only when using https endpoints)
# MINIO_SSL_VERIFY=true
# MINIO_SSL_CA_BUNDLE=/path/to/ca.pem

# lakeFS
LAKEFS_API_URL=http://localhost:48080
LAKEFS_S3_ENDPOINT_URL=http://localhost:48080
LAKEFS_ACCESS_KEY_ID=spice-lakefs-admin
LAKEFS_SECRET_ACCESS_KEY=spice-lakefs-admin-secret

# lakeFS S3 gateway TLS verification (only when using https endpoints)
# LAKEFS_S3_SSL_VERIFY=true
# LAKEFS_S3_SSL_CA_BUNDLE=/path/to/ca.pem
```

## Event Sourcing Correctness

```bash
ENABLE_PROCESSED_EVENT_REGISTRY=true
PROCESSED_EVENT_LEASE_TIMEOUT_SECONDS=900
PROCESSED_EVENT_HEARTBEAT_INTERVAL_SECONDS=30

EVENT_STORE_SEQUENCE_ALLOCATOR_MODE=postgres
EVENT_STORE_SEQUENCE_SCHEMA=spice_event_registry
EVENT_STORE_SEQUENCE_HANDLER_PREFIX=write_side
EVENT_STORE_IDEMPOTENCY_MISMATCH_MODE=error
```

## Chaos Injection (test-only)

Deterministic crash points for validating lease recovery/idempotency (used by `backend/tests/chaos_lite.py`).

```bash
ENABLE_CHAOS_INJECTION=false
CHAOS_CRASH_POINT=
CHAOS_CRASH_ONCE=true
CHAOS_CRASH_EXIT_CODE=42
```

## Publisher (S3 tail -> Kafka)

```bash
EVENT_PUBLISHER_CHECKPOINT_KEY=checkpoints/event_publisher.json
EVENT_PUBLISHER_POLL_INTERVAL=3
EVENT_PUBLISHER_BATCH_SIZE=200
EVENT_PUBLISHER_LOOKBACK_SECONDS=600
EVENT_PUBLISHER_LOOKBACK_MAX_KEYS=50
EVENT_PUBLISHER_DEDUP_MAX_EVENTS=10000
EVENT_PUBLISHER_DEDUP_CHECKPOINT_MAX_EVENTS=2000
```

## Connectors

```bash
# Trigger service (poll sources + enqueue connector updates)
CONNECTOR_TRIGGER_SOURCE_TYPE=google_sheets
CONNECTOR_TRIGGER_TICK_SECONDS=5
CONNECTOR_TRIGGER_POLL_CONCURRENCY=5
CONNECTOR_TRIGGER_OUTBOX_BATCH=50

# Sync worker (consume connector updates + submit writes)
CONNECTOR_SYNC_GROUP=connector-sync-worker-group
CONNECTOR_SYNC_HANDLER=connector_sync_worker
CONNECTOR_SYNC_MAX_RETRIES=5
CONNECTOR_SYNC_BACKOFF_BASE_SECONDS=2
CONNECTOR_SYNC_BACKOFF_MAX_SECONDS=60

# Google Sheets
GOOGLE_SHEETS_API_KEY=
# Fallback:
GOOGLE_API_KEY=
```

## Pipeline / Objectify

```bash
INSTANCE_EVENTS_TOPIC=instance_events
ONTOLOGY_EVENTS_TOPIC=ontology_events
ACTION_EVENTS_TOPIC=action_events

PROJECTION_DLQ_TOPIC=projection_failures_dlq
SEARCH_PROJECTION_DLQ_TOPIC=projection_failures_dlq

PIPELINE_JOBS_TOPIC=pipeline-jobs
PIPELINE_JOBS_DLQ_TOPIC=pipeline-jobs-dlq
PIPELINE_EVENTS_TOPIC=pipeline-events

OBJECTIFY_JOBS_TOPIC=objectify-jobs
OBJECTIFY_JOBS_DLQ_TOPIC=objectify-jobs-dlq
OBJECTIFY_MAX_RETRIES=5
OBJECTIFY_ROW_BATCH_SIZE=1000
```

## Commands (Instance/Ontology/Action)

```bash
# Command topics (SSoT EventStore -> message-relay -> Kafka -> workers)
INSTANCE_COMMANDS_TOPIC=instance_commands
ONTOLOGY_COMMANDS_TOPIC=ontology_commands
DATABASE_COMMANDS_TOPIC=database_commands
ACTION_COMMANDS_TOPIC=action_commands

# Command DLQ topics (poison/non-retryable/max-retry exceeded)
INSTANCE_COMMANDS_DLQ_TOPIC=instance-commands-dlq
ONTOLOGY_COMMANDS_DLQ_TOPIC=ontology-commands-dlq
ACTION_COMMANDS_DLQ_TOPIC=action-commands-dlq

# Retry controls
INSTANCE_WORKER_MAX_RETRY_ATTEMPTS=5
ONTOLOGY_WORKER_MAX_RETRY_ATTEMPTS=5
ACTION_WORKER_MAX_RETRY_ATTEMPTS=5
```

## Writeback / Overlay (lakeFS)

```bash
ONTOLOGY_WRITEBACK_REPO=ontology-writeback
ONTOLOGY_WRITEBACK_BRANCH_PREFIX=writeback
ONTOLOGY_WRITEBACK_DATASET_ID=

WRITEBACK_ENFORCE=false
WRITEBACK_ENFORCE_GOVERNANCE=false
WRITEBACK_READ_OVERLAY=false
WRITEBACK_ENABLED_OBJECT_TYPES=
WRITEBACK_DATASET_ACL_SCOPE=dataset_acl
WRITEBACK_SUBMISSION_SNAPSHOT_MAX_TARGETS=200
```

## Cache TTLs (Redis)

```bash
CLASS_LABEL_CACHE_TTL=3600
COMMAND_STATUS_CACHE_TTL=86400
USER_SESSION_CACHE_TTL=7200
WEBSOCKET_CONNECTION_TTL=3600
```

## Outbox / Reconciler

```bash
# Action writeback outbox (ActionLog reconcile; recommended ON for convergence)
ACTION_OUTBOX_POLL_SECONDS=2
ACTION_OUTBOX_BATCH_SIZE=100

# Dataset ingest outbox + reconciler
ENABLE_DATASET_INGEST_OUTBOX_WORKER=true
DATASET_INGEST_OUTBOX_POLL_SECONDS=5
DATASET_INGEST_OUTBOX_DLQ_TOPIC=dataset-ingest-outbox-dlq
ENABLE_DATASET_INGEST_RECONCILER=true
DATASET_INGEST_RECONCILER_POLL_SECONDS=60
DATASET_INGEST_RECONCILER_STALE_SECONDS=3600

# Objectify outbox + reconciler
ENABLE_OBJECTIFY_OUTBOX_WORKER=true
OBJECTIFY_OUTBOX_POLL_SECONDS=5
ENABLE_OBJECTIFY_RECONCILER=true
OBJECTIFY_RECONCILER_POLL_SECONDS=60
```

## Writeback Materializer (optional)

```bash
# When WRITEBACK_MATERIALIZER_DB_NAMES is empty, the worker is a no-op (sleeps).
WRITEBACK_MATERIALIZER_DB_NAMES=
WRITEBACK_MATERIALIZER_BASE_BRANCH=main
WRITEBACK_MATERIALIZER_INTERVAL_SECONDS=21600
WRITEBACK_MATERIALIZER_RUN_ONCE=false
```

## Governance

```bash
PIPELINE_PROTECTED_BRANCHES=main
PIPELINE_REQUIRE_PROPOSALS=false

ONTOLOGY_PROTECTED_BRANCHES=main
ONTOLOGY_REQUIRE_PROPOSALS=true
ONTOLOGY_REQUIRE_HEALTH_GATE=true
ONTOLOGY_DEPLOYMENTS_V2=true
```

## Pipeline Planner

```bash
# Enable LLM-backed pipeline planning (single autonomous loop + MCP tools).
PIPELINE_PLAN_LLM_ENABLED=true
```

## Lineage / Audit

```bash
ENABLE_LINEAGE=true
ENABLE_AUDIT_LOGS=true
```

## Debug / Observability

```bash
ENABLE_DEBUG_ENDPOINTS=false
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
OTEL_EXPORT_OTLP=true
OTEL_ENABLE_TRACING=true
```

## Notes

- `.env.example` is the authoritative dev template.
- For production, use secret management and rotate tokens regularly.
