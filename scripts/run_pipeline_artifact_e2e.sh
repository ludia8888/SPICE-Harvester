#!/usr/bin/env bash
set -euo pipefail

BFF_URL="${BFF_URL:-http://localhost:8002}"
ADMIN_TOKEN="${ADMIN_TOKEN:?ADMIN_TOKEN is required}"
DB_NAME="${DB_NAME:-demo_db}"
BRANCH="${BRANCH:-main}"
E2E_DIR="${E2E_DIR:-docs/foundry_checklist/evidence/pipeline_artifact_e2e}"

RUN_ID=$(python - <<'PY'
import uuid
print(uuid.uuid4().hex)
PY
)
DATASET_NAME="${DATASET_NAME:-artifact_input_${RUN_ID}}"
PIPELINE_NAME="${PIPELINE_NAME:-artifact_pipeline_${RUN_ID}}"

mkdir -p "$E2E_DIR"
CSV_PATH="$E2E_DIR/input_${RUN_ID}.csv"

cat > "$CSV_PATH" <<'CSV'
id,name
1,A
2,B
3,C
CSV

AUTH_HEADERS=(
  -H "Authorization: Bearer ${ADMIN_TOKEN}"
  -H "X-Admin-Token: ${ADMIN_TOKEN}"
  -H "X-DB-Name: ${DB_NAME}"
)

IDEMPOTENCY_KEY="ingest-${RUN_ID}"
INGEST_JSON=$(curl -sS -X POST "${BFF_URL}/api/v1/pipelines/datasets/csv-upload?db_name=${DB_NAME}&branch=${BRANCH}" \
  "${AUTH_HEADERS[@]}" \
  -H "Idempotency-Key: ${IDEMPOTENCY_KEY}" \
  -F "dataset_name=${DATASET_NAME}" \
  -F "file=@${CSV_PATH}")
export INGEST_JSON

DATASET_ID=$(python - <<'PY'
import json, os
payload = json.loads(os.environ["INGEST_JSON"])
print((payload.get("data") or {}).get("dataset", {}).get("dataset_id") or "")
PY
)
DATASET_VERSION_ID=$(python - <<'PY'
import json, os
payload = json.loads(os.environ["INGEST_JSON"])
print((payload.get("data") or {}).get("version", {}).get("version_id") or "")
PY
)

if [ -z "$DATASET_ID" ] || [ -z "$DATASET_VERSION_ID" ]; then
  echo "Failed to ingest dataset" >&2
  echo "$INGEST_JSON" >&2
  exit 1
fi

PIPELINE_JSON="$E2E_DIR/pipeline_${RUN_ID}.json"
cat > "$PIPELINE_JSON" <<JSON
{
  "nodes": [
    {
      "id": "in1",
      "type": "input",
      "metadata": {
        "datasetId": "${DATASET_ID}",
        "datasetName": "${DATASET_NAME}"
      }
    },
    {
      "id": "out1",
      "type": "output",
      "metadata": {
        "datasetName": "artifact_output",
        "outputName": "artifact_output"
      }
    }
  ],
  "edges": [
    {"from": "in1", "to": "out1"}
  ],
  "parameters": [],
  "settings": {"engine": "Batch"},
  "outputs": [
    {"name": "artifact_output", "type": "dataset"}
  ]
}
JSON

CREATE_PIPELINE_JSON=$(curl -sS -X POST "${BFF_URL}/api/v1/pipelines" \
  "${AUTH_HEADERS[@]}" \
  -H "Content-Type: application/json" \
  -d @- <<JSON
{
  "db_name": "${DB_NAME}",
  "name": "${PIPELINE_NAME}",
  "location": "e2e",
  "description": "artifact e2e",
  "branch": "${BRANCH}",
  "pipeline_type": "batch",
  "definition_json": $(cat "$PIPELINE_JSON")
}
JSON
)
export CREATE_PIPELINE_JSON

PIPELINE_ID=$(python - <<'PY'
import json, os
payload = json.loads(os.environ["CREATE_PIPELINE_JSON"])
print((payload.get("data") or {}).get("pipeline", {}).get("pipeline_id") or "")
PY
)

if [ -z "$PIPELINE_ID" ]; then
  echo "Failed to create pipeline" >&2
  echo "$CREATE_PIPELINE_JSON" >&2
  exit 1
fi

PREVIEW_JSON=$(curl -sS -X POST "${BFF_URL}/api/v1/pipelines/${PIPELINE_ID}/preview" \
  "${AUTH_HEADERS[@]}" \
  -H "Content-Type: application/json" \
  -d @- <<JSON
{
  "db_name": "${DB_NAME}",
  "node_id": "out1",
  "limit": 3
}
JSON
)
export PREVIEW_JSON

PREVIEW_JOB_ID=$(python - <<'PY'
import json, os
payload = json.loads(os.environ["PREVIEW_JSON"])
print((payload.get("data") or {}).get("job_id") or "")
PY
)

if [ -z "$PREVIEW_JOB_ID" ]; then
  echo "Failed to enqueue preview" >&2
  echo "$PREVIEW_JSON" >&2
  exit 1
fi

wait_run() {
  local job_id="$1"
  local attempts=0
  while [ $attempts -lt 120 ]; do
    RUNS_JSON=$(curl -sS "${BFF_URL}/api/v1/pipelines/${PIPELINE_ID}/runs" \
      "${AUTH_HEADERS[@]}" \
      -G --data-urlencode "limit=200")
    export RUNS_JSON
    STATUS=$(python - <<'PY'
import json, os
payload = json.loads(os.environ["RUNS_JSON"])
runs = (payload.get("data") or {}).get("runs") or []
job_id = os.environ["JOB_ID"]
match = next((item for item in runs if item.get("job_id") == job_id), {})
print(str(match.get("status") or "").upper())
PY
)
    if [ "$STATUS" = "SUCCESS" ] || [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "DEPLOYED" ]; then
      echo "$STATUS"
      return 0
    fi
    attempts=$((attempts + 1))
    sleep 2
  done
  echo "TIMEOUT" >&2
  return 1
}

JOB_ID="$PREVIEW_JOB_ID"
export JOB_ID
wait_run "$PREVIEW_JOB_ID" >/dev/null

PREVIEW_ARTIFACTS_JSON=$(curl -sS "${BFF_URL}/api/v1/pipelines/${PIPELINE_ID}/artifacts" \
  "${AUTH_HEADERS[@]}" \
  -G --data-urlencode "mode=preview" --data-urlencode "limit=1")
export PREVIEW_ARTIFACTS_JSON

PREVIEW_ARTIFACT_ID=$(python - <<'PY'
import json, os
payload = json.loads(os.environ["PREVIEW_ARTIFACTS_JSON"])
artifacts = (payload.get("data") or {}).get("artifacts") or []
print((artifacts[0] if artifacts else {}).get("artifact_id") or "")
PY
)

if [ -z "$PREVIEW_ARTIFACT_ID" ]; then
  echo "Preview artifact not found" >&2
  exit 1
fi

BUILD_JSON=$(curl -sS -X POST "${BFF_URL}/api/v1/pipelines/${PIPELINE_ID}/build" \
  "${AUTH_HEADERS[@]}" \
  -H "Content-Type: application/json" \
  -d @- <<JSON
{
  "db_name": "${DB_NAME}",
  "node_id": "out1"
}
JSON
)
export BUILD_JSON

BUILD_JOB_ID=$(python - <<'PY'
import json, os
payload = json.loads(os.environ["BUILD_JSON"])
print((payload.get("data") or {}).get("job_id") or "")
PY
)

if [ -z "$BUILD_JOB_ID" ]; then
  echo "Failed to enqueue build" >&2
  echo "$BUILD_JSON" >&2
  exit 1
fi

JOB_ID="$BUILD_JOB_ID"
export JOB_ID
wait_run "$BUILD_JOB_ID" >/dev/null

BUILD_ARTIFACTS_JSON=$(curl -sS "${BFF_URL}/api/v1/pipelines/${PIPELINE_ID}/artifacts" \
  "${AUTH_HEADERS[@]}" \
  -G --data-urlencode "mode=build" --data-urlencode "limit=1")
export BUILD_ARTIFACTS_JSON

BUILD_ARTIFACT_ID=$(python - <<'PY'
import json, os
payload = json.loads(os.environ["BUILD_ARTIFACTS_JSON"])
artifacts = (payload.get("data") or {}).get("artifacts") or []
print((artifacts[0] if artifacts else {}).get("artifact_id") or "")
PY
)

if [ -z "$BUILD_ARTIFACT_ID" ]; then
  echo "Build artifact not found" >&2
  exit 1
fi

DEPLOY_JSON=$(curl -sS -X POST "${BFF_URL}/api/v1/pipelines/${PIPELINE_ID}/deploy" \
  "${AUTH_HEADERS[@]}" \
  -H "Content-Type: application/json" \
  -d @- <<JSON
{
  "promote_build": true,
  "build_job_id": "${BUILD_JOB_ID}",
  "artifact_id": "${BUILD_ARTIFACT_ID}",
  "node_id": "out1",
  "output": {"db_name": "${DB_NAME}"}
}
JSON
)
export DEPLOY_JSON

DEPLOYED_COMMIT_ID=$(python - <<'PY'
import json, os
payload = json.loads(os.environ["DEPLOY_JSON"])
print((payload.get("data") or {}).get("deployed_commit_id") or "")
PY
)

if [ -z "$DEPLOYED_COMMIT_ID" ]; then
  echo "Failed to deploy" >&2
  echo "$DEPLOY_JSON" >&2
  exit 1
fi

SUMMARY_PATH="$E2E_DIR/summary_${RUN_ID}.txt"
cat > "$SUMMARY_PATH" <<TXT
run_id=${RUN_ID}
db_name=${DB_NAME}
branch=${BRANCH}
dataset_name=${DATASET_NAME}
dataset_id=${DATASET_ID}
dataset_version_id=${DATASET_VERSION_ID}
pipeline_id=${PIPELINE_ID}
preview_job_id=${PREVIEW_JOB_ID}
preview_artifact_id=${PREVIEW_ARTIFACT_ID}
build_job_id=${BUILD_JOB_ID}
build_artifact_id=${BUILD_ARTIFACT_ID}
deploy_commit_id=${DEPLOYED_COMMIT_ID}
TXT

echo "E2E summary saved to ${SUMMARY_PATH}"

echo "Optional verification: dataset_versions.promoted_from_artifact_id"
if command -v psql >/dev/null 2>&1 && [ -n "${POSTGRES_URL:-}" ]; then
  psql "$POSTGRES_URL" -c "SELECT version_id, promoted_from_artifact_id, created_at FROM spice_datasets.dataset_versions WHERE dataset_id='${DATASET_ID}' ORDER BY created_at DESC LIMIT 3;"
fi

echo "Optional verification: lineage_edges metadata"
if command -v psql >/dev/null 2>&1 && [ -n "${POSTGRES_URL:-}" ]; then
  psql "$POSTGRES_URL" -c "SELECT edge_id, edge_type, metadata->>'promoted_from_artifact_id' AS artifact_id, recorded_at FROM spice_lineage.lineage_edges WHERE metadata->>'promoted_from_artifact_id'='${BUILD_ARTIFACT_ID}' ORDER BY recorded_at DESC LIMIT 5;"
fi
