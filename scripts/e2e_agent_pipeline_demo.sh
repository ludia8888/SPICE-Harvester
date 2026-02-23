#!/usr/bin/env bash
set -euo pipefail

# End-to-end demo for the Pipeline Agent (single autonomous loop + MCP tools):
# - Create DB
# - Upload CSV datasets
# - Call POST /api/v1/agent/pipeline-runs with a natural-language goal
#
# Outputs are written under test_results/ (gitignored).
#
# Usage (default scenario: synthetic_3pl_topn):
#   ADMIN_TOKEN=... USER_JWT_HS256_SECRET=... docker compose -f docker-compose.full.yml up -d
#   ADMIN_TOKEN=... USER_JWT_HS256_SECRET=... ./scripts/e2e_agent_pipeline_demo.sh
#
# Scenarios:
#   SCENARIO=synthetic_3pl_topn (default)
#   SCENARIO=single_csv_null_check CSV_FILE=path/to/file.csv DATASET_NAME=my_ds

HOST="${HOST:-127.0.0.1}"
BFF_URL="${BFF_URL:-http://${HOST}:8002}"
API_BASE_URL="${API_BASE_URL:-${BFF_URL}/api/v1}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

DB_NAME="${DB_NAME:-demo_pipeline_agent}"
BRANCH="${BRANCH:-master}"
SCENARIO="${SCENARIO:-synthetic_3pl_topn}"

ADMIN_TOKEN="${ADMIN_TOKEN:-${BFF_ADMIN_TOKEN:-${BFF_WRITE_TOKEN:-${ADMIN_API_KEY:-}}}}"

# Delegated end-user auth for /api/v1/agent/* (required when USER_JWT_ENABLED=true).
USER_JWT="${USER_JWT:-}"
USER_JWT_HS256_SECRET="${USER_JWT_HS256_SECRET:-}"
USER_JWT_ISSUER="${USER_JWT_ISSUER:-}"
USER_JWT_AUDIENCE="${USER_JWT_AUDIENCE:-}"
USER_JWT_TTL_SECONDS="${USER_JWT_TTL_SECONDS:-3600}"
USER_ID="${USER_ID:-demo-user}"
USER_EMAIL="${USER_EMAIL:-demo@example.com}"
USER_TYPE="${USER_TYPE:-user}"
TENANT_ID="${TENANT_ID:-default}"
USER_ROLES="${USER_ROLES:-}"

# Scenario: single CSV
CSV_FILE="${CSV_FILE:-test_data/customers.csv}"
DATASET_NAME="${DATASET_NAME:-}"
DATASET_DESCRIPTION="${DATASET_DESCRIPTION:-pipeline agent demo dataset}"

# Scenario: synthetic 3PL (multi-join + groupBy + window)
SYNTHETIC_DIR="${SYNTHETIC_DIR:-test_data/spice_harvester_synthetic_3pl}"

require_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "Missing dependency: ${cmd}" >&2
    exit 1
  fi
}

require_cmd curl
require_cmd "${PYTHON_BIN}"

uuid() {
  if command -v uuidgen >/dev/null 2>&1; then
    uuidgen | tr '[:upper:]' '[:lower:]'
    return 0
  fi
  "${PYTHON_BIN}" - <<'PY'
import uuid
print(uuid.uuid4())
PY
}

RUN_ID="${RUN_ID:-$(uuid)}"
E2E_DIR="${E2E_DIR:-test_results/pipeline_agent_demo_${RUN_ID}}"
mkdir -p "${E2E_DIR}"

if [[ -z "${ADMIN_TOKEN}" ]]; then
  echo "Missing ADMIN_TOKEN (or BFF_ADMIN_TOKEN / BFF_WRITE_TOKEN / ADMIN_API_KEY)." >&2
  exit 1
fi

if [[ -z "${USER_JWT}" && -z "${USER_JWT_HS256_SECRET}" ]]; then
  echo "Warning: USER_JWT not set. If USER_JWT_ENABLED=true on the server, /api/v1/agent/* will require delegated auth." >&2
fi

if [[ -z "${USER_JWT}" && -n "${USER_JWT_HS256_SECRET}" ]]; then
  export USER_JWT_HS256_SECRET USER_JWT_TTL_SECONDS USER_ID USER_EMAIL USER_TYPE TENANT_ID USER_ROLES USER_JWT_ISSUER USER_JWT_AUDIENCE
  USER_JWT="$("${PYTHON_BIN}" - <<'PY'
import base64
import hashlib
import hmac
import json
import os
import time

def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

def sign_hs256(message: str, secret: str) -> str:
    digest = hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).digest()
    return b64url(digest)

now = int(time.time())
ttl = int(os.environ.get("USER_JWT_TTL_SECONDS", "3600") or "3600")
roles_raw = (os.environ.get("USER_ROLES") or "").strip()
roles = [r.strip() for r in roles_raw.split(",") if r.strip()] if roles_raw else []

claims = {
    "sub": (os.environ.get("USER_ID") or "demo-user").strip() or "demo-user",
    "email": (os.environ.get("USER_EMAIL") or "").strip() or None,
    "tenant_id": (os.environ.get("TENANT_ID") or "default").strip() or "default",
    "org_id": (os.environ.get("TENANT_ID") or "default").strip() or "default",
    "roles": roles,
    "typ": (os.environ.get("USER_TYPE") or "user").strip() or "user",
    "iat": now,
    "exp": now + max(60, ttl),
}
issuer = (os.environ.get("USER_JWT_ISSUER") or "").strip()
audience = (os.environ.get("USER_JWT_AUDIENCE") or "").strip()
if issuer:
    claims["iss"] = issuer
if audience:
    claims["aud"] = audience

header = {"alg": "HS256", "typ": "JWT"}
header_b64 = b64url(json.dumps(header, separators=(",", ":"), ensure_ascii=True).encode("utf-8"))
claims_b64 = b64url(json.dumps(claims, separators=(",", ":"), ensure_ascii=True).encode("utf-8"))
signing_input = f"{header_b64}.{claims_b64}"
secret = os.environ["USER_JWT_HS256_SECRET"]
sig_b64 = sign_hs256(signing_input, secret)
print(f"{signing_input}.{sig_b64}")
PY
)"
fi

AUTH_HEADERS_ADMIN=(-H "X-Admin-Token: ${ADMIN_TOKEN}" -H "X-DB-Name: ${DB_NAME}" -H "X-Project: ${DB_NAME}")
AUTH_HEADERS_AGENT=(
  -H "X-Admin-Token: ${ADMIN_TOKEN}"
  -H "Content-Type: application/json"
)
if [[ -n "${USER_JWT}" ]]; then
  AUTH_HEADERS_AGENT+=(-H "X-Delegated-Authorization: Bearer ${USER_JWT}")
fi

echo "E2E_DIR=${E2E_DIR}"
echo "BFF_URL=${BFF_URL}"
echo "DB_NAME=${DB_NAME} BRANCH=${BRANCH} SCENARIO=${SCENARIO}"

create_db() {
  local resp_file="${E2E_DIR}/00_create_db.response.json"
  echo "Creating DB: ${DB_NAME}"
  curl -sS -X POST "${API_BASE_URL}/databases" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: ${DB_NAME}-create" \
    "${AUTH_HEADERS_ADMIN[@]}" \
    -d "{\"name\":\"${DB_NAME}\",\"description\":\"pipeline agent demo\"}" | tee "${resp_file}" >/dev/null
}

upload_csv_dataset() {
  local file_path="$1"
  local dataset_name="$2"
  local resp_file="${E2E_DIR}/dataset_upload_${dataset_name}.response.json"
  local create_url="${BFF_URL}/api/v2/datasets"
  local idem_key="ingest-v2-${RUN_ID}-${dataset_name}"

  if [[ ! -f "${file_path}" ]]; then
    echo "CSV file not found: ${file_path}" >&2
    exit 1
  fi

  echo "Creating dataset (v2): ${dataset_name} (${file_path})"
  local create_resp
  create_resp="$(curl -sS -X POST "${create_url}" \
    "${AUTH_HEADERS_ADMIN[@]}" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: ${idem_key}-create" \
    -d @- <<JSON
{"name":"${dataset_name}","parentFolderRid":"ri.foundry.main.folder.${DB_NAME}"}
JSON
)"

  local dataset_rid
  dataset_rid="$("${PYTHON_BIN}" - <<'PY' "${create_resp}"
import json
import sys
payload = json.loads(sys.argv[1])
rid = str(payload.get("rid") or "").strip()
if not rid:
    raise SystemExit("Failed to create dataset via /api/v2/datasets")
print(rid)
PY
)"

  local txn_resp
  txn_resp="$(curl -sS -X POST "${BFF_URL}/api/v2/datasets/${dataset_rid}/transactions?branchName=${BRANCH}" \
    "${AUTH_HEADERS_ADMIN[@]}" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: ${idem_key}-txn" \
    -d '{}')"

  local transaction_rid
  transaction_rid="$("${PYTHON_BIN}" - <<'PY' "${txn_resp}"
import json
import sys
payload = json.loads(sys.argv[1])
rid = str(payload.get("rid") or "").strip()
if not rid:
    raise SystemExit("Failed to create transaction via /api/v2/datasets/{datasetRid}/transactions")
print(rid)
PY
)"

  local upload_resp
  upload_resp="$(curl -sS -X POST "${BFF_URL}/api/v2/datasets/${dataset_rid}/files/source.csv/upload?transactionRid=${transaction_rid}" \
    "${AUTH_HEADERS_ADMIN[@]}" \
    -H "Content-Type: text/csv" \
    --data-binary @"${file_path}")"

  local commit_resp
  commit_resp="$(curl -sS -X POST "${BFF_URL}/api/v2/datasets/${dataset_rid}/transactions/${transaction_rid}/commit" \
    "${AUTH_HEADERS_ADMIN[@]}" \
    -H "Idempotency-Key: ${idem_key}-commit")"

  printf '%s\n' "${create_resp}" > "${resp_file}"
  printf '%s\n' "${txn_resp}" >> "${resp_file}"
  printf '%s\n' "${upload_resp}" >> "${resp_file}"
  printf '%s\n' "${commit_resp}" >> "${resp_file}"

  "${PYTHON_BIN}" - <<'PY' "${dataset_rid}" "${commit_resp}"
import json
import sys

dataset_rid = str(sys.argv[1]).strip()
commit_payload = json.loads(sys.argv[2])
if str(commit_payload.get("status") or "").strip().upper() != "COMMITTED":
    raise SystemExit("Dataset transaction commit failed")
prefixes = ("ri.foundry.main.dataset.", "ri.spice.main.dataset.")
dataset_id = ""
for prefix in prefixes:
    if dataset_rid.startswith(prefix):
        dataset_id = dataset_rid[len(prefix):]
        break
if not dataset_id:
    raise SystemExit("Failed to parse dataset_id from dataset RID")
print(dataset_id)
PY
}

run_pipeline_agent() {
  local goal="$1"
  shift
  local dataset_ids_json="$1"

  local resp_file="${E2E_DIR}/10_pipeline_agent.response.json"
  local payload_file="${E2E_DIR}/10_pipeline_agent.request.json"

  cat > "${payload_file}" <<JSON
{
  "goal": ${goal},
  "data_scope": {"db_name": "${DB_NAME}", "branch": "${BRANCH}", "dataset_ids": ${dataset_ids_json}},
  "preview_limit": 200,
  "include_run_tables": false,
  "max_repairs": 2,
  "max_cleansing": 1,
  "max_transform": 1,
  "apply_specs": false,
  "auto_sync": false
}
JSON

  echo "Running Pipeline Agent..."
  curl -sS -X POST "${API_BASE_URL}/agent/pipeline-runs" \
    "${AUTH_HEADERS_AGENT[@]}" \
    -H "Idempotency-Key: pipeline-agent-${RUN_ID}" \
    --data-binary @"${payload_file}" | tee "${resp_file}" >/dev/null

  "${PYTHON_BIN}" - <<'PY' "${resp_file}"
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    payload = json.load(f)
data = payload.get("data") or {}
print("status=", data.get("status"))
print("plan_id=", data.get("plan_id"))
errs = data.get("validation_errors") or []
warns = data.get("validation_warnings") or []
print("validation_errors=", len(errs))
print("validation_warnings=", len(warns))
planner = data.get("planner") or {}
llm = data.get("llm") or {}
if llm:
    print("llm_model=", llm.get("model"))
    print("llm_cache_hit=", llm.get("cache_hit"))
    print("llm_latency_ms=", llm.get("latency_ms"))
if planner:
    print("planner_confidence=", planner.get("confidence"))
PY
}

create_db || true

dataset_ids=()
goal_json="\"null check 해줘\""

if [[ "${SCENARIO}" == "single_csv_null_check" ]]; then
  if [[ -z "${DATASET_NAME}" ]]; then
    DATASET_NAME="single_${RUN_ID}"
  fi
  dataset_ids+=("$(upload_csv_dataset "${CSV_FILE}" "${DATASET_NAME}")")
  goal_json="\"null check 해줘\""
elif [[ "${SCENARIO}" == "synthetic_3pl_topn" ]]; then
  dataset_ids+=("$(upload_csv_dataset "${SYNTHETIC_DIR}/orders.csv" "orders")")
  dataset_ids+=("$(upload_csv_dataset "${SYNTHETIC_DIR}/order_items.csv" "order_items")")
  dataset_ids+=("$(upload_csv_dataset "${SYNTHETIC_DIR}/skus.csv" "skus")")
  dataset_ids+=("$(upload_csv_dataset "${SYNTHETIC_DIR}/products.csv" "products")")
  goal_json="\"orders, order_items, skus, products를 조인해서 category별 매출(revenue=qty*unit_price) Top 10과 rank(row_number) 만들어줘\""
else
  echo "Unknown SCENARIO: ${SCENARIO}" >&2
  exit 1
fi

dataset_ids_json="$("${PYTHON_BIN}" - <<'PY' "${dataset_ids[@]}"
import json
import sys
print(json.dumps(list(sys.argv[1:]), ensure_ascii=True))
PY
)"

run_pipeline_agent "${goal_json}" "${dataset_ids_json}"

echo "Done. Artifacts written under: ${E2E_DIR}"
