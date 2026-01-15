# SPICE HARVESTER Operations Manual (Code-Backed)

> Updated: 2026-01-08  \
> Scope: local/dev stack and production-hardening guidance based on current repo layout.

## 1) Runtime Topology (local)

Recommended dev stack:

```bash
docker compose -f docker-compose.full.yml up -d
```

Included background workers (no public ports):
- `message-relay`, `ontology-worker`, `instance-worker`, `projection-worker`
- Action writeback: `action-worker`, `action-outbox-worker`, `writeback-materializer-worker` (default no-op unless `WRITEBACK_MATERIALIZER_DB_NAMES` is set)

Key services/ports (defaults, override via `.env`). OMS/Funnel/Agent are internal by default; use the debug ports override when you need direct access.

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

Core services read from `.env` and docker-compose overrides. Avoid putting secrets in git.

## 5) Data & Persistence

Critical storage systems (backup/restore):

- **Event Store (S3/MinIO)**: bucket `spice-event-store` (command + domain events)
- **Dataset artifacts (lakeFS + MinIO)**: lakeFS metadata in Postgres, objects in MinIO buckets
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

### Agent Pipeline Demo (JWT + Mock LLM)

Deterministic end-to-end demo script:
- `./scripts/e2e_agent_pipeline_demo.sh`
- Artifacts: `test_results/agent_pipeline_demo_<RUN_ID>/`

Bring up the stack with end-user JWT enabled and mock LLM responses:

```bash
USER_JWT_ENABLED=true \
USER_JWT_HS256_SECRET=spice-dev-user-jwt-secret \
LLM_PROVIDER=mock \
ADMIN_TOKEN=test-token \
docker compose -f backend/docker-compose.yml up -d --build

ADMIN_TOKEN=test-token \
USER_JWT_HS256_SECRET=spice-dev-user-jwt-secret \
AUTO_APPROVE=true \
./scripts/e2e_agent_pipeline_demo.sh
```

Notes:
- `backend/docker-compose.yml` mounts `scripts/llm_mocks/` into the BFF container and sets `LLM_MOCK_DIR=/app/llm_mocks`, so you usually don't need to export `LLM_MOCK_JSON_*` manually.
- Compose uses `${VAR:-default}` expansion; re-running `docker compose up` without exporting the variables will recreate containers with defaults (e.g. `LLM_PROVIDER=disabled`).

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

## 10) Production Hardening (Checklist)

- TLS termination + network segmentation
- Secret management (no plaintext tokens)
- Authn/Authz gateway in front of BFF
- Resource limits / autoscaling for workers
- Kafka retention and DLQ policies
- Regular backup/restore drills
