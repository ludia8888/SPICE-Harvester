# SPICE HARVESTER Operations Manual (Code-Backed)

> Updated: 2026-01-08  \
> Scope: local/dev stack and production-hardening guidance based on current repo layout.

## 1) Runtime Topology (local)

Recommended dev stack:

```bash
docker compose -f docker-compose.full.yml up -d
```

Disk hygiene (recommended):

```bash
# Bring the stack down + clean docker caches (prevents build cache growth)
make stack-down

# Only prune docker caches (safe; keeps volumes)
make stack-gc

# Aggressive cache cleanup (also removes all unused images)
GC_MODE=aggressive make stack-down

# Optional: also destroy stack volumes (Postgres/MinIO/etc data)
WITH_VOLUMES=true make stack-down
```

Notes:
- `make stack-down` runs `./scripts/ops/compose_down_clean.sh` (which calls `./scripts/ops/docker_gc.sh`).
- Global unused volume pruning is intentionally guarded (destructive): `CONFIRM=YES ./scripts/ops/docker_gc.sh --with-volumes`.

Included background workers (no public ports):
- `message-relay`, `ontology-worker`, `instance-worker`, `projection-worker`
- Action writeback: `action-worker`, `action-outbox-worker`, `writeback-materializer-worker` (default no-op unless `WRITEBACK_MATERIALIZER_DB_NAMES` is set)

Key services/ports (defaults, override via env vars; docker-compose reads `.env` for substitution, and Python reads `.env` only when `SPICE_LOAD_DOTENV=true`). OMS/Funnel/Agent are internal by default; use the debug ports override when you need direct access.

| Component | Port |
| --- | ---: |
| OMS (internal; debug ports only) | 8000 |
| BFF (external) | 8002 |
| Funnel (internal; debug ports only) | 8003 |
| Agent (internal; debug ports only) | 8004 |
| TerminusDB | 6363 |
| Postgres | 5433 |
| Redis | 6379 |
| Kafka | 39092 |
| Elasticsearch | 9200 |
| MinIO (S3 API) | 9000 |
| MinIO Console | 9001 |
| lakeFS | 48080 |
| Jaeger | 16686 |
| OTEL Collector | 4317/4318 |
| Prometheus | 19090 |
| Alertmanager | 19093 |
| Grafana | 13000 |

## 2) Health / Monitoring

- BFF: `GET http://localhost:8002/api/v1/health`
- OMS/Funnel health checks require the debug ports override:
  - `docker compose -f docker-compose.full.yml -f backend/docker-compose.debug-ports.yml up -d`
  - `GET http://localhost:8000/health`
  - `GET http://localhost:8003/health`
- Metrics: `GET http://localhost:8002/metrics` (BFF); OMS metrics require the debug ports override

Quick smoke (external ports only):
- `HOST=127.0.0.1 ./scripts/smoke_external_ports.sh`
- Detailed monitoring: `/api/v1/monitoring/*` and `/api/v1/config/*`

Local dashboards (when using `docker-compose.full.yml`):
- Prometheus: `http://localhost:19090`
- Alertmanager: `http://localhost:19093`
- Grafana: `http://localhost:13000` (default `admin` / `admin`)
- Traces (Jaeger): `http://localhost:16686`

## 3) Authentication & Security

- BFF/OMS require a shared token by default. Set:
  - `BFF_ADMIN_TOKEN` and `OMS_ADMIN_TOKEN` (or `ADMIN_API_KEY`/`ADMIN_TOKEN`).
- Token rotation: shared-token env vars accept comma-separated lists (e.g. `new,old`).
- Disable auth only for non-production:
  - `BFF_REQUIRE_AUTH=false` + `ALLOW_INSECURE_BFF_AUTH_DISABLE=true`
  - `OMS_REQUIRE_AUTH=false` + `ALLOW_INSECURE_OMS_AUTH_DISABLE=true`
- CORS: see `backend/docs/development/CORS_CONFIGURATION_GUIDE.md`.

## 4) Environment Variables

Canonical list is maintained in:
- `backend/ENVIRONMENT_VARIABLES.md`

Core services read from environment variables. Docker Compose uses `.env` for variable substitution; Python settings only read `.env` when `SPICE_LOAD_DOTENV=true`. Avoid putting secrets in git.

## 5) Data & Persistence

Critical storage systems (backup/restore):

- **Event Store (S3/MinIO)**: bucket `spice-event-store` (command + domain events)
- **Dataset artifacts (lakeFS + MinIO)**: lakeFS metadata in Postgres, objects in MinIO buckets
- **Ontology store (TerminusDB)**: docker volume backing `/app/terminusdb/storage` (backup at volume/snapshot level)
- **Postgres**: registries, processed_events, lineage, audit logs, gate results
- **Elasticsearch**: read projections (rebuildable by replay)
- **Redis**: command status + rate limiter (recoverable)

## 6) Backup / Recovery

Minimal recovery set:

1. Postgres (registries + lineage + audit + idempotency)
2. MinIO (event store + lakeFS buckets)
3. lakeFS metadata (Postgres DB used by lakeFS)

Recovery strategy:
- Restore Postgres + MinIO + lakeFS first.
- Bring up OMS/BFF/workers.
- Rebuild projections by replay if ES is stale.

### 6.1 Code-backed backup scripts

All backup artifacts are written under `./backups/` (gitignored).

One-shot backups (recommended for drills):

```bash
./scripts/ops/backup_stack.sh
```

Component-level backups:

```bash
# Postgres dump (custom format) + retention pruning
OUT_DIR=./backups/postgres RETENTION_DAYS=14 ./scripts/ops/backup_postgres.sh

# MinIO buckets mirror + retention pruning
BACKUP_ROOT=./backups/minio RETENTION_DAYS=14 ./scripts/ops/backup_minio.sh

# TerminusDB data volume archive + retention pruning
OUT_DIR=./backups/terminusdb RETENTION_DAYS=14 ./scripts/ops/backup_terminusdb_volume.sh
```

### 6.2 Restore (Disaster Recovery)

Restores are **destructive**. Every restore requires explicit confirmation via `CONFIRM=YES`.

```bash
# Restore Postgres
CONFIRM=YES ./scripts/ops/restore_postgres.sh ./backups/postgres/<dump>.dump

# Restore MinIO buckets (safe default: overwrite only; no remote deletes)
CONFIRM=YES ./scripts/ops/restore_minio.sh ./backups/minio/<timestamp>/

# Restore TerminusDB volume (requires TerminusDB container stopped)
CONFIRM=YES AUTO_STOP=true ./scripts/ops/restore_terminusdb_volume.sh ./backups/terminusdb/<archive>.tar.gz
```

### 6.3 Rollback policy (production)

- **Service rollback**: pin image tags, keep a “last known good” tag, and roll back by redeploying the previous tag.
- **Schema rollback**: prefer forward-only migrations; for emergency rollback restore from the most recent Postgres backup.
- **Ontology rollback**: disabled by default (`ENABLE_OMS_ROLLBACK=false`); treat as non-prod/admin-only.

## 7) Operational Checks

- Kafka topics are auto-created in dev; for production, pre-create and set retention.
- Ensure `processed_events` is healthy (Postgres) to prevent duplicate side effects.
- Validate event store append health (S3/MinIO reachable).
- Verify lakeFS credentials and bucket access.

## 8) Testing

Production gate:

```bash
PYTHON_BIN=python3.12 ./backend/run_production_tests.sh --full
```

Pipeline transform/cleansing E2E (full stack required):

```bash
RUN_PIPELINE_TRANSFORM_E2E=true PYTHON_BIN=python3.12 ./backend/run_production_tests.sh --full
```

### Agent Pipeline Demo (JWT + OpenAI LLM)

End-to-end demo script:
- `./scripts/e2e_agent_pipeline_demo.sh`
- Artifacts: `test_results/agent_pipeline_demo_<RUN_ID>/`

Bring up the stack with end-user JWT enabled and an OpenAI-compatible LLM:

```bash
USER_JWT_ENABLED=true \
USER_JWT_HS256_SECRET=spice-dev-user-jwt-secret \
LLM_API_KEY=your_openai_api_key_here \
PIPELINE_PLAN_LLM_ENABLED=true \
ADMIN_TOKEN=test-token \
docker compose -f backend/docker-compose.yml up -d --build

ADMIN_TOKEN=test-token \
USER_JWT_HS256_SECRET=spice-dev-user-jwt-secret \
AUTO_APPROVE=true \
./scripts/e2e_agent_pipeline_demo.sh
```

Notes:
- Default LLM config in `backend/docker-compose.yml` is `LLM_PROVIDER=openai_compat` with `LLM_BASE_URL=https://api.openai.com/v1` and `LLM_MODEL=gpt-5` (override via env).
- Pipeline agent + pipeline plan compile are gated by `PIPELINE_PLAN_LLM_ENABLED`.
- Deterministic mode (CI/offline): set `LLM_PROVIDER=mock` (Compose mounts `scripts/llm_mocks/` into the BFF container and sets `LLM_MOCK_DIR=/app/llm_mocks`).
- Compose uses `${VAR:-default}` expansion; re-running `docker compose up` without exporting the variables will recreate containers with defaults.

## 9) Troubleshooting

- **Auth errors (401/403)**: check `BFF_ADMIN_TOKEN` / `OMS_ADMIN_TOKEN` and `BFF_REQUIRE_AUTH`.
- **Kafka not reachable**: verify broker port 39092 and container health.
- **Event store errors**: confirm MinIO health and bucket existence.
- **LakeFS failures**: confirm lakeFS health endpoint and MinIO credentials.
- **Rate limiter errors**: Redis unavailable triggers degraded mode; check Redis connectivity.
- **DLQ (poison/non-retryable/max-retry)**:
  - 주요 DLQ 토픽: `pipeline-jobs-dlq`, `objectify-jobs-dlq`, `projection_failures_dlq`, `connector-updates-dlq`, `dataset-ingest-outbox-dlq`, `instance-commands-dlq`, `ontology-commands-dlq`, `action-commands-dlq`
  - 재처리(Replay): `python3 scripts/replay_dlq.py --dlq-topic instance-commands-dlq --max-messages 50`
  - 원인 확인: DLQ payload의 `error`/`stage`/`attempt_count` 및 trace(`traceparent` header 또는 envelope `metadata`)로 상관관계 추적

### 9.1 Incident runbook (alerts)

- `SpiceServiceDown`: `docker compose ps` → `docker compose logs <service>` → `docker compose restart <service>` (and validate `GET /health` + `/metrics`)
- `SpiceHighHttp5xxRate`: check latest deploy/config changes → roll back to last known good image tag (prod) or rebuild previous commit (dev) → verify error-rate drops
- `OtelCollectorDown`: restart collector; traces/metrics degrade but core services should remain functional

## 10) Production Hardening (Checklist)

- TLS termination + network segmentation
- Secret management (no plaintext tokens)
- Authn/Authz gateway in front of BFF
- Resource limits / autoscaling for workers
- Kafka retention and DLQ policies
- Regular backup/restore drills

<!-- DOC_SYNC: 2026-02-13 Foundry pipeline parity + runtime consistency sweep -->
