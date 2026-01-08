# Pipeline Artifact E2E Runbook

> Status: Historical runbook. Validate endpoints and prerequisites against current code before use.

## Purpose
- Validate the artifact-first loop: ingest -> preview artifact -> build artifact -> deploy promotion -> dataset_version link -> lineage.
- Produce evidence files under `docs/foundry_checklist/evidence/pipeline_artifact_e2e/`.

## Prereqs
- Running services: BFF, pipeline_worker, lakeFS, Kafka, Postgres.
- `ADMIN_TOKEN` with pipeline write permissions.

## Environment
- `BFF_URL` (default `http://localhost:8002`)
- `ADMIN_TOKEN` (required)
- `DB_NAME` (default `demo_db`)
- `BRANCH` (default `main`)
- `POSTGRES_URL` (optional, enables DB verification queries)

## Automated Run
```bash
ADMIN_TOKEN=... ./scripts/run_pipeline_artifact_e2e.sh
```

Outputs:
- Summary file: `docs/foundry_checklist/evidence/pipeline_artifact_e2e/summary_<run_id>.txt`
- Input CSV + pipeline definition: same directory.

## Manual API Flow (Curl)

1) Ingest CSV (create dataset + dataset_version)
```bash
curl -sS -X POST "${BFF_URL}/api/v1/pipelines/datasets/csv-upload?db_name=${DB_NAME}&branch=${BRANCH}" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H "X-Admin-Token: ${ADMIN_TOKEN}" \
  -H "Idempotency-Key: ingest-${RUN_ID}" \
  -F "dataset_name=${DATASET_NAME}" \
  -F "file=@input.csv"
```

2) Create pipeline definition (must include `outputs[]`).

3) Preview -> job_id (poll `/api/v1/pipelines/{pipeline_id}/runs` until `SUCCESS`).

4) Fetch preview artifact id:
```bash
curl -sS "${BFF_URL}/api/v1/pipelines/${PIPELINE_ID}/artifacts?mode=preview&limit=1" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H "X-Admin-Token: ${ADMIN_TOKEN}"
```

5) Build -> job_id (poll runs until `SUCCESS`).

6) Fetch build artifact id:
```bash
curl -sS "${BFF_URL}/api/v1/pipelines/${PIPELINE_ID}/artifacts?mode=build&limit=1" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H "X-Admin-Token: ${ADMIN_TOKEN}"
```

7) Deploy using artifact promotion:
```bash
curl -sS -X POST "${BFF_URL}/api/v1/pipelines/${PIPELINE_ID}/deploy" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H "X-Admin-Token: ${ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "promote_build": true,
    "build_job_id": "<build_job_id>",
    "artifact_id": "<build_artifact_id>",
    "node_id": "out1",
    "output": {"db_name": "'"${DB_NAME}"'"}
  }'
```

## Evidence Checks
- `dataset_versions.promoted_from_artifact_id` equals the build artifact id.
- Lineage edge metadata includes `promoted_from_artifact_id`.
- Artifact detail endpoint returns `outputs[]` with schema and health:
```bash
curl -sS "${BFF_URL}/api/v1/pipelines/${PIPELINE_ID}/artifacts/<artifact_id>" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H "X-Admin-Token: ${ADMIN_TOKEN}"
```

Optional DB verification (requires `POSTGRES_URL`):
```bash
psql "$POSTGRES_URL" -c \
  "SELECT version_id, promoted_from_artifact_id, created_at FROM spice_datasets.dataset_versions ORDER BY created_at DESC LIMIT 5;"
psql "$POSTGRES_URL" -c \
  "SELECT edge_id, edge_type, metadata->>'promoted_from_artifact_id' AS artifact_id FROM spice_lineage.lineage_edges ORDER BY recorded_at DESC LIMIT 5;"
```
