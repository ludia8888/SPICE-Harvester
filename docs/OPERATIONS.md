# SPICE HARVESTER Operations Manual (Code-Backed)

> Updated: 2026-01-08  \
> Scope: local/dev stack and production-hardening guidance based on current repo layout.

## 1) Runtime Topology (local)

Recommended dev stack:

```bash
docker compose -f docker-compose.full.yml up -d
```

Key services/ports (defaults, override via `.env`):

| Component | Port |
| --- | ---: |
| OMS | 8000 |
| BFF | 8002 |
| Funnel | 8003 |
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

## 2) Health / Monitoring

- BFF: `GET http://localhost:8002/api/v1/health`
- OMS: `GET http://localhost:8000/health`
- Funnel: `GET http://localhost:8003/health`
- Metrics: `GET http://localhost:8002/metrics` (BFF), `GET http://localhost:8000/metrics` (OMS)
- Detailed monitoring: `/api/v1/monitoring/*` and `/api/v1/config/*`

## 3) Authentication & Security

- BFF/OMS require a shared token by default. Set:
  - `BFF_ADMIN_TOKEN` and `OMS_ADMIN_TOKEN` (or `ADMIN_API_KEY`/`ADMIN_TOKEN`).
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

## 9) Troubleshooting

- **Auth errors (401/403)**: check `BFF_ADMIN_TOKEN` / `OMS_ADMIN_TOKEN` and `BFF_REQUIRE_AUTH`.
- **Kafka not reachable**: verify broker port 39092 and container health.
- **Event store errors**: confirm MinIO health and bucket existence.
- **LakeFS failures**: confirm lakeFS health endpoint and MinIO credentials.
- **Rate limiter errors**: Redis unavailable triggers degraded mode; check Redis connectivity.

## 10) Production Hardening (Checklist)

- TLS termination + network segmentation
- Secret management (no plaintext tokens)
- Authn/Authz gateway in front of BFF
- Resource limits / autoscaling for workers
- Kafka retention and DLQ policies
- Regular backup/restore drills
