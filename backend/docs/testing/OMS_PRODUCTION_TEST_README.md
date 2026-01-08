# OMS Production Readiness Tests (Steady State)

> Updated: 2026-01-08  
> Architecture: S3/MinIO Event Store (SSoT) + EventPublisher (S3 tail → Kafka) + Kafka consumers with Postgres idempotency/OCC.

This guide documents how to run the current production-readiness checks for OMS (and the surrounding stack).

## Test map (current)

- `backend/tests/test_oms_smoke.py` — OMS end-to-end smoke via HTTP (creates/deletes real DBs; opt-in).
- `backend/tests/test_core_functionality.py` — full stack checks (OMS/BFF/Funnel + infra).
- `backend/tests/test_terminus_version_control.py` — TerminusDB branch/version primitives used by OMS.
- `backend/tests/test_sequence_allocator.py` — Postgres atomic per-aggregate seq allocator + OCC primitives.
- `backend/tests/test_idempotency_chaos.py` — Postgres `processed_events` lease/idempotency + ordering guard.
- `backend/tests/chaos_lite.py` — chaos-lite: stop/restart Kafka/Redis/ES/Terminus + worker crash injection, then verify convergence (no mocks).

## Quick start

1) Start infra (MinIO/Postgres/Kafka/TerminusDB, optionally Elasticsearch).  
2) Start services (at minimum OMS; for full-stack tests also start BFF/Funnel, plus EventPublisher/workers/projection if validating async flow).  
3) If your local stack uses non-default ports, export endpoint overrides (examples).
   Default compose keeps OMS/Funnel internal; use backend/docker-compose.debug-ports.yml if you need localhost access.

```bash
export OMS_BASE_URL=http://localhost:8000
export BFF_BASE_URL=http://localhost:8002
export FUNNEL_BASE_URL=http://localhost:8003
export TERMINUS_SERVER_URL=http://localhost:6363
export MINIO_ENDPOINT_URL=http://localhost:9000
export ELASTICSEARCH_URL=http://localhost:9200
export KAFKA_BOOTSTRAP_SERVERS=localhost:39092
export POSTGRES_URL=postgresql://spiceadmin:spicepass123@localhost:5433/spicedb
```

4) Run tests from the repo root (or use the convenience runner):

```bash
# infra-only correctness layer (fast)
pytest backend/tests/test_sequence_allocator.py -q
pytest backend/tests/test_idempotency_chaos.py -q

# Terminus-only checks
pytest backend/tests/test_terminus_version_control.py -q

# OMS live smoke (destructive)
RUN_LIVE_OMS_SMOKE=true pytest backend/tests/test_oms_smoke.py -q

# Full suite (will skip when infra/services are missing)
pytest backend/tests -q

# Or run via the consolidated runner (respects the env vars above)
./backend/run_production_tests.sh --full
```

## Chaos-lite (no mocks)

`backend/tests/chaos_lite.py` runs a small suite of partial-failure scenarios by controlling docker-compose containers:
- Kafka down/up (publisher + consumers recover)
- Redis down/up (command status degraded, but write/projection still converge)
- Elasticsearch down/up (projection retries, graph becomes FULL after recovery)
- TerminusDB down/up (write-side retries, commands converge after recovery)
- instance-worker crash after claim (lease recovery via Postgres processed_events)

Run (from repo root):

```bash
python3 backend/tests/chaos_lite.py
```

Extra scenarios (opt-in):

```bash
# Kafka out-of-order seq injection (stale guard must skip)
python3 backend/tests/chaos_lite.py --skip-lite --out-of-order

# Soak: random partial failures + convergence checks (default 300s)
python3 backend/tests/chaos_lite.py --skip-lite --soak --soak-seconds 600 --soak-seed 123
```

Or via the consolidated runner:

```bash
./backend/run_production_tests.sh --chaos-out-of-order
./backend/run_production_tests.sh --chaos-soak --soak-seconds 600 --soak-seed 123
```

Notes:
- This test is intentionally “real stack”: it will stop/start containers and recreate `instance-worker`.
- Run it on an isolated dev stack; do not run against shared environments.

## Notes

- `RUN_LIVE_OMS_SMOKE=true` is intentionally opt-in because it creates and deletes real databases.
- Elasticsearch is a projection dependency; ES failures should not compromise write-side idempotency.
- Legacy migration/dual-write tests referenced by older documents (`tests/integration/*`, `migration_helper.py`) are removed/archived.
