# Backend perf tests

This folder contains **load/performance** tests that validate the async command pipeline end-to-end:

`BFF (202 Accepted) → OMS → EventStore(S3/MinIO) → Kafka → workers → command status`

## k6: async instance create flow

Script: `backend/perf/k6/async_instance_flow.js`

What it does:
- Creates a temporary DB + ontology class (`LoadTestItem`)
- Repeatedly calls `POST /api/v1/databases/{db}/instances/{class}/create?branch=main`
- Polls `GET /api/v1/commands/{command_id}/status` until `COMPLETED|FAILED`
- Deletes the temporary DB in teardown (requires `/api/v1/databases/{db}/expected-seq`; older BFF images may not have it yet)

Run (Docker):

```bash
./backend/perf/run_k6_async_instance_flow.sh
```

Tuning:

```bash
K6_VUS=10 K6_ITERATIONS=100 K6_MAX_DURATION=20m ./backend/perf/run_k6_async_instance_flow.sh
```

If your k6 runs in Docker on Linux, set:

```bash
K6_BASE_URL=http://127.0.0.1:8002/api/v1 ./backend/perf/run_k6_async_instance_flow.sh
```

(Or use `--network host` in your own wrapper.)

## Cleanup (if teardown can't delete)

If your running BFF doesn't expose `GET /api/v1/databases/{db}/expected-seq` yet, the k6 teardown will skip DB deletion.

Cleanup helper:

```bash
PYTHONPATH=backend python backend/perf/cleanup_perf_databases.py --prefix perf_ --dry-run
PYTHONPATH=backend python backend/perf/cleanup_perf_databases.py --prefix perf_ --yes
```

This script derives `expected_seq` from Postgres (`spice_event_registry.aggregate_versions`) and deletes via BFF.
