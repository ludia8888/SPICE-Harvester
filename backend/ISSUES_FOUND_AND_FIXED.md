# 🔍 Issues Found & Fixed (Historical)

> Originally written: 2024-11-14  
> Updated: 2025-12-17 (steady-state doc refresh)

This document is kept as a historical log from the **dual-write migration era**.

The system **no longer supports**:
- `ENABLE_S3_EVENT_STORE` / `ENABLE_DUAL_WRITE`
- `migration_helper.py` migration modes (`legacy` / `dual_write` / `s3_only`)
- legacy `tests/integration/*` suites referenced by older docs

Current references:
- Production runbook (steady state): `backend/PRODUCTION_MIGRATION_RUNBOOK.md`
- Idempotency/ordering/OCC contract: `docs/IDEMPOTENCY_CONTRACT.md`

## Still-relevant lessons

- **Elasticsearch auth/config** can fail independently of write correctness; treat it as an optional projection dependency.
- **MinIO endpoint differences** (Docker vs local) should be handled by explicitly setting `MINIO_ENDPOINT_URL`.
- **Timezone correctness**: `datetime.utcnow()` usages were replaced with timezone-aware `datetime.now(UTC)`.
- **Postgres port variance**: local setups often use either `5432` or `5433`; tests accept both.

## Quick verification (current)

```bash
# timezone sanity
python -c "from datetime import datetime, UTC; print(datetime.now(UTC).isoformat())"

# Postgres-backed correctness layer
pytest backend/tests/test_sequence_allocator.py -q
pytest backend/tests/test_idempotency_chaos.py -q
```
