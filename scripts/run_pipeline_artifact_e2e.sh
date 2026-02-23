#!/usr/bin/env bash
set -euo pipefail

BFF_URL="${BFF_URL:-http://localhost:8002}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADMIN_TOKEN="${ADMIN_TOKEN:?ADMIN_TOKEN is required}"
DB_NAME="${DB_NAME:-demo_db}"
BRANCH="${BRANCH:-master}"
E2E_DIR="${E2E_DIR:-docs/platform_checklist/evidence/pipeline_artifact_e2e}"
CLEANUP_OBJECT_STORE="${CLEANUP_OBJECT_STORE:-true}"
PIPELINE_ID="${PIPELINE_ID:-}"
PIPELINE_RID="${PIPELINE_RID:-}"
PIPELINE_NODE_ID="${PIPELINE_NODE_ID:-}"

truthy() {
  local v
  v="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]' | xargs)"
  [[ "${v}" == "1" || "${v}" == "true" || "${v}" == "yes" || "${v}" == "y" ]]
}

cleanup() {
  if truthy "${CLEANUP_OBJECT_STORE}" && [[ -f "${SCRIPT_DIR}/dev_cleanup_object_store.py" ]]; then
    "${PYTHON_BIN}" "${SCRIPT_DIR}/dev_cleanup_object_store.py" --yes --db "${DB_NAME}" --quiet || true
  fi
}
trap cleanup EXIT

RUN_ID=$("${PYTHON_BIN}" - <<'PY'
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
  -H "X-Project: ${DB_NAME}"
)

if [ -z "${PIPELINE_RID}" ] && [ -n "${PIPELINE_ID}" ]; then
  PIPELINE_RID="ri.foundry.main.pipeline.${PIPELINE_ID}"
fi
if [ -z "${PIPELINE_RID}" ]; then
  echo "Missing PIPELINE_RID (or PIPELINE_ID). v2 orchestration build requires an existing pipeline RID." >&2
  exit 1
fi

CREATE_DATASET_JSON=$(curl -sS -X POST "${BFF_URL}/api/v2/datasets" \
  "${AUTH_HEADERS[@]}" \
  -H "Content-Type: application/json" \
  -d @- <<JSON
{
  "name": "${DATASET_NAME}",
  "parentFolderRid": "ri.foundry.main.folder.${DB_NAME}"
}
JSON
)
export CREATE_DATASET_JSON

DATASET_RID=$("${PYTHON_BIN}" - <<'PY'
import json, os
payload = json.loads(os.environ["CREATE_DATASET_JSON"])
print(str(payload.get("rid") or ""))
PY
)
DATASET_ID=$("${PYTHON_BIN}" - <<'PY'
import os
rid = str(os.environ.get("DATASET_RID") or "").strip()
prefixes = ("ri.foundry.main.dataset.", "ri.spice.main.dataset.")
for prefix in prefixes:
    if rid.startswith(prefix):
        print(rid[len(prefix):])
        break
else:
    print("")
PY
)

if [ -z "${DATASET_RID}" ] || [ -z "${DATASET_ID}" ]; then
  echo "Failed to create dataset via /api/v2/datasets" >&2
  echo "$CREATE_DATASET_JSON" >&2
  exit 1
fi

CREATE_TXN_JSON=$(curl -sS -X POST "${BFF_URL}/api/v2/datasets/${DATASET_RID}/transactions?branchName=${BRANCH}" \
  "${AUTH_HEADERS[@]}" \
  -H "Content-Type: application/json" \
  -d '{}')
export CREATE_TXN_JSON

TRANSACTION_RID=$("${PYTHON_BIN}" - <<'PY'
import json, os
payload = json.loads(os.environ["CREATE_TXN_JSON"])
print(str(payload.get("rid") or ""))
PY
)
if [ -z "${TRANSACTION_RID}" ]; then
  echo "Failed to create transaction" >&2
  echo "$CREATE_TXN_JSON" >&2
  exit 1
fi

UPLOAD_JSON=$(curl -sS -X POST "${BFF_URL}/api/v2/datasets/${DATASET_RID}/files/source.csv/upload?transactionRid=${TRANSACTION_RID}" \
  "${AUTH_HEADERS[@]}" \
  -H "Content-Type: text/csv" \
  --data-binary @"${CSV_PATH}")
export UPLOAD_JSON

COMMIT_JSON=$(curl -sS -X POST "${BFF_URL}/api/v2/datasets/${DATASET_RID}/transactions/${TRANSACTION_RID}/commit" \
  "${AUTH_HEADERS[@]}")
export COMMIT_JSON

COMMIT_STATUS=$("${PYTHON_BIN}" - <<'PY'
import json, os
payload = json.loads(os.environ["COMMIT_JSON"])
print(str(payload.get("status") or ""))
PY
)
if [ "${COMMIT_STATUS}" != "COMMITTED" ]; then
  echo "Failed to commit dataset transaction" >&2
  echo "$COMMIT_JSON" >&2
  exit 1
fi

READ_TABLE_CONTENT_PATH="${E2E_DIR}/dataset_preview_${RUN_ID}.csv"
curl -sS "${BFF_URL}/api/v2/datasets/${DATASET_RID}/readTable?format=CSV&branchName=${BRANCH}&rowLimit=3" \
  "${AUTH_HEADERS[@]}" > "${READ_TABLE_CONTENT_PATH}"

create_build() {
  local node_id="${1:-}"
  local limit="${2:-}"
  local payload
  if [ -n "${node_id}" ]; then
    payload=$(cat <<JSON
{
  "target": {"targetRids": ["${PIPELINE_RID}"]},
  "branchName": "${BRANCH}",
  "parameters": {"nodeId": "${node_id}", "limit": ${limit:-3}}
}
JSON
)
  else
    payload=$(cat <<JSON
{
  "target": {"targetRids": ["${PIPELINE_RID}"]},
  "branchName": "${BRANCH}"
}
JSON
)
  fi
  curl -sS -X POST "${BFF_URL}/api/v2/orchestration/builds/create" \
    "${AUTH_HEADERS[@]}" \
    -H "Content-Type: application/json" \
    -d "${payload}"
}

wait_build() {
  local build_rid="$1"
  local attempts=0
  while [ $attempts -lt 120 ]; do
    BUILD_STATUS_JSON=$(curl -sS "${BFF_URL}/api/v2/orchestration/builds/${build_rid}" "${AUTH_HEADERS[@]}")
    export BUILD_STATUS_JSON
    STATUS=$("${PYTHON_BIN}" - <<'PY'
import json, os
payload = json.loads(os.environ["BUILD_STATUS_JSON"])
print(str(payload.get("status") or "").upper())
PY
)
    if [ "${STATUS}" = "SUCCEEDED" ] || [ "${STATUS}" = "FAILED" ] || [ "${STATUS}" = "CANCELED" ] || [ "${STATUS}" = "CANCELLED" ]; then
      echo "${STATUS}"
      return 0
    fi
    attempts=$((attempts + 1))
    sleep 2
  done
  echo "TIMEOUT" >&2
  return 1
}

PREVIEW_BUILD_JSON="$(create_build "${PIPELINE_NODE_ID}" 3)"
export PREVIEW_BUILD_JSON
PREVIEW_BUILD_RID=$("${PYTHON_BIN}" - <<'PY'
import json, os
payload = json.loads(os.environ["PREVIEW_BUILD_JSON"])
print(str(payload.get("rid") or ""))
PY
)
if [ -z "${PREVIEW_BUILD_RID}" ]; then
  echo "Failed to enqueue preview build" >&2
  echo "${PREVIEW_BUILD_JSON}" >&2
  exit 1
fi
PREVIEW_STATUS="$(wait_build "${PREVIEW_BUILD_RID}")"
if [ "${PREVIEW_STATUS}" != "SUCCEEDED" ]; then
  echo "Preview build did not succeed: ${PREVIEW_STATUS}" >&2
  exit 1
fi

PREVIEW_JOBS_JSON=$(curl -sS "${BFF_URL}/api/v2/orchestration/builds/${PREVIEW_BUILD_RID}/jobs?pageSize=10" "${AUTH_HEADERS[@]}")
export PREVIEW_JOBS_JSON

BUILD_JSON="$(create_build "" "")"
export BUILD_JSON
BUILD_RID=$("${PYTHON_BIN}" - <<'PY'
import json, os
payload = json.loads(os.environ["BUILD_JSON"])
print(str(payload.get("rid") or ""))
PY
)
if [ -z "${BUILD_RID}" ]; then
  echo "Failed to enqueue build" >&2
  echo "${BUILD_JSON}" >&2
  exit 1
fi
BUILD_STATUS="$(wait_build "${BUILD_RID}")"
if [ "${BUILD_STATUS}" != "SUCCEEDED" ]; then
  echo "Build did not succeed: ${BUILD_STATUS}" >&2
  exit 1
fi

BUILD_JOBS_JSON=$(curl -sS "${BFF_URL}/api/v2/orchestration/builds/${BUILD_RID}/jobs?pageSize=10" "${AUTH_HEADERS[@]}")
export BUILD_JOBS_JSON

PREVIEW_JOB_ID=$("${PYTHON_BIN}" - <<'PY'
import json, os
payload = json.loads(os.environ["PREVIEW_JOBS_JSON"])
rows = payload.get("data") if isinstance(payload.get("data"), list) else []
first = rows[0] if rows else {}
rid = str(first.get("rid") or "")
prefix = "ri.foundry.main.job."
print(rid[len(prefix):] if rid.startswith(prefix) else "")
PY
)

BUILD_JOB_ID=$("${PYTHON_BIN}" - <<'PY'
import json, os
payload = json.loads(os.environ["BUILD_JOBS_JSON"])
rows = payload.get("data") if isinstance(payload.get("data"), list) else []
first = rows[0] if rows else {}
rid = str(first.get("rid") or "")
prefix = "ri.foundry.main.job."
print(rid[len(prefix):] if rid.startswith(prefix) else "")
PY
)

SUMMARY_PATH="$E2E_DIR/summary_${RUN_ID}.txt"
cat > "$SUMMARY_PATH" <<TXT
run_id=${RUN_ID}
db_name=${DB_NAME}
branch=${BRANCH}
dataset_name=${DATASET_NAME}
dataset_id=${DATASET_ID}
dataset_rid=${DATASET_RID}
transaction_rid=${TRANSACTION_RID}
pipeline_name=${PIPELINE_NAME}
pipeline_rid=${PIPELINE_RID}
preview_build_rid=${PREVIEW_BUILD_RID}
preview_status=${PREVIEW_STATUS}
preview_job_id=${PREVIEW_JOB_ID}
build_rid=${BUILD_RID}
build_status=${BUILD_STATUS}
build_job_id=${BUILD_JOB_ID}
dataset_preview_csv=${READ_TABLE_CONTENT_PATH}
TXT

echo "E2E summary saved to ${SUMMARY_PATH}"
echo "Preview and build completed via /api/v2/orchestration/builds/*"
