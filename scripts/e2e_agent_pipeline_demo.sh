#!/usr/bin/env bash
set -euo pipefail

# End-to-end demo:
# - Raw CSV upload -> dataset
# - Agent Session -> attach dataset context (+ optional file upload)
# - Natural language -> plan compile -> (approval) -> job/run
# - CI result ingestion -> re-plan loop
#
# Output artifacts are written under test_results/ (gitignored).

HOST="${HOST:-127.0.0.1}"
BFF_URL="${BFF_URL:-http://${HOST}:8002}"
API_BASE_URL="${API_BASE_URL:-${BFF_URL}/api/v1}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

DB_NAME="${DB_NAME:-demo}"
BRANCH="${BRANCH:-main}"
ONTOLOGY_BRANCH="${ONTOLOGY_BRANCH:-}"
ONTOLOGY_BRANCH_FROM="${ONTOLOGY_BRANCH_FROM:-}"

CSV_FILE="${CSV_FILE:-test_data/customers.csv}"
DATASET_NAME="${DATASET_NAME:-agent_demo_$(date +%Y%m%d_%H%M%S)}"
DATASET_DESCRIPTION="${DATASET_DESCRIPTION:-agent pipeline demo dataset}"
DATASET_HAS_HEADER="${DATASET_HAS_HEADER:-true}"
DATASET_DELIMITER="${DATASET_DELIMITER:-}"
DATASET_INCLUDE_MODE="${DATASET_INCLUDE_MODE:-summary}" # full|summary|search

AUTO_APPROVE="${AUTO_APPROVE:-false}"   # true to auto-approve agent plans
DO_APPLY="${DO_APPLY:-true}"            # true to run the "apply changes" prompt
DO_CI_LOOP="${DO_CI_LOOP:-true}"        # true to ingest CI + replan once

RETRIES="${RETRIES:-120}"
SLEEP_SECONDS="${SLEEP_SECONDS:-2}"

# Optional: bootstrap ontology/object-type so Objectify can run end-to-end.
SETUP_OBJECTIFY="${SETUP_OBJECTIFY:-true}"
OBJECTIFY_CLASS_ID="${OBJECTIFY_CLASS_ID:-Customer}"

# Auth tokens
ADMIN_TOKEN="${ADMIN_TOKEN:-}"
if [[ -z "${ADMIN_TOKEN}" ]]; then
  ADMIN_TOKEN="${BFF_ADMIN_TOKEN:-}"
fi
if [[ -z "${ADMIN_TOKEN}" ]]; then
  ADMIN_TOKEN="${BFF_WRITE_TOKEN:-}"
fi
if [[ -z "${ADMIN_TOKEN}" ]]; then
  ADMIN_TOKEN="${ADMIN_API_KEY:-}"
fi
if [[ -z "${ADMIN_TOKEN}" ]]; then
  ADMIN_TOKEN="${ADMIN_TOKEN:-}"
fi

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

CI_PROVIDER="${CI_PROVIDER:-demo_ci}"
CI_STATUS="${CI_STATUS:-failure}" # queued|in_progress|success|failure|cancelled|skipped|unknown
CI_DETAILS_URL="${CI_DETAILS_URL:-}"
CI_SUMMARY="${CI_SUMMARY:-unit tests failed (demo)}"

require_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "❌ Missing dependency: ${cmd}" >&2
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

truthy() {
  local v
  v="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]' | xargs)"
  [[ "${v}" == "1" || "${v}" == "true" || "${v}" == "yes" || "${v}" == "y" ]]
}

RUN_ID="${RUN_ID:-$(uuid)}"
export RUN_ID
E2E_DIR="${E2E_DIR:-test_results/agent_pipeline_demo_${RUN_ID}}"
mkdir -p "${E2E_DIR}"

# Resolve ontology branch for Objectify bootstrap.
# Ontology writes to protected branches (e.g., main) require proposal workflows.
if [[ -z "${ONTOLOGY_BRANCH}" ]]; then
  ONTOLOGY_BRANCH="${BRANCH}"
  if truthy "${DO_APPLY}" && truthy "${SETUP_OBJECTIFY}"; then
    case "$(printf '%s' "${ONTOLOGY_BRANCH}" | tr '[:upper:]' '[:lower:]')" in
      main|master|production|prod)
        ONTOLOGY_BRANCH="ontology/${RUN_ID}"
        ;;
    esac
  fi
fi
if [[ -z "${ONTOLOGY_BRANCH_FROM}" ]]; then
  ONTOLOGY_BRANCH_FROM="${BRANCH}"
fi

# Export frequently-used values for the embedded Python snippets.
export DB_NAME BRANCH ONTOLOGY_BRANCH ONTOLOGY_BRANCH_FROM DATASET_INCLUDE_MODE SETUP_OBJECTIFY OBJECTIFY_CLASS_ID

AUTH_HEADERS=(-H "X-Admin-Token: ${ADMIN_TOKEN}" -H "X-DB-Name: ${DB_NAME}")

if [[ -z "${ADMIN_TOKEN}" ]]; then
  echo "❌ Missing admin token. Set ADMIN_TOKEN or BFF_ADMIN_TOKEN." >&2
  exit 1
fi

if [[ ! -f "${CSV_FILE}" ]]; then
  echo "❌ CSV_FILE not found: ${CSV_FILE}" >&2
  exit 1
fi

if [[ -z "${USER_JWT}" ]]; then
  if [[ -z "${USER_JWT_HS256_SECRET}" ]]; then
    echo "❌ Missing USER_JWT. This demo uses /agent-sessions which require end-user JWT." >&2
    echo "   Set USER_JWT (preferred) or USER_JWT_HS256_SECRET (dev HS256 signing) and ensure the server has USER_JWT_ENABLED=true." >&2
    exit 1
  fi

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
header_b64 = b64url(json.dumps(header, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
claims_b64 = b64url(json.dumps(claims, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
signing_input = f"{header_b64}.{claims_b64}"
secret = os.environ["USER_JWT_HS256_SECRET"]
sig_b64 = sign_hs256(signing_input, secret)
print(f"{signing_input}.{sig_b64}")
PY
)"
fi

AGENT_HEADERS=(
  -H "X-Admin-Token: ${ADMIN_TOKEN}"
  -H "X-Delegated-Authorization: Bearer ${USER_JWT}"
  -H "Content-Type: application/json"
)

echo "E2E_DIR=${E2E_DIR}"
echo "BFF_URL=${BFF_URL}"
echo "DB_NAME=${DB_NAME} BRANCH=${BRANCH} ONTOLOGY_BRANCH=${ONTOLOGY_BRANCH}"
echo "CSV_FILE=${CSV_FILE}"
echo "AUTO_APPROVE=${AUTO_APPROVE} DO_APPLY=${DO_APPLY} DO_CI_LOOP=${DO_CI_LOOP}"
echo "USER_JWT=(present, length=${#USER_JWT})"

upload_csv_dataset() {
  local ingest_idempotency_key="ingest-${RUN_ID}"
  local resp_file="${E2E_DIR}/01_dataset_upload.response.json"
  local url="${API_BASE_URL}/pipelines/datasets/csv-upload?db_name=${DB_NAME}&branch=${BRANCH}"

  echo "📦 Uploading CSV dataset -> ${url}"
  if [[ -n "${DATASET_DELIMITER}" ]]; then
    curl -sS -X POST "${url}" \
      "${AUTH_HEADERS[@]}" \
      -H "Idempotency-Key: ${ingest_idempotency_key}" \
      -F "file=@${CSV_FILE}" \
      -F "dataset_name=${DATASET_NAME}" \
      -F "description=${DATASET_DESCRIPTION}" \
      -F "delimiter=${DATASET_DELIMITER}" \
      -F "has_header=${DATASET_HAS_HEADER}" \
      >"${resp_file}"
  else
    curl -sS -X POST "${url}" \
      "${AUTH_HEADERS[@]}" \
      -H "Idempotency-Key: ${ingest_idempotency_key}" \
      -F "file=@${CSV_FILE}" \
      -F "dataset_name=${DATASET_NAME}" \
      -F "description=${DATASET_DESCRIPTION}" \
      -F "has_header=${DATASET_HAS_HEADER}" \
      >"${resp_file}"
  fi

  export INGEST_JSON_FILE="${resp_file}"
  DATASET_ID="$("${PYTHON_BIN}" - <<'PY' "${INGEST_JSON_FILE}"
import json, sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
data = payload.get("data") or {}
print((data.get("dataset") or {}).get("dataset_id") or "")
PY
)"
  INGEST_REQUEST_ID="$("${PYTHON_BIN}" - <<'PY' "${INGEST_JSON_FILE}"
import json, sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
data = payload.get("data") or {}
print(data.get("ingest_request_id") or "")
PY
)"
  DATASET_VERSION_ID="$("${PYTHON_BIN}" - <<'PY' "${INGEST_JSON_FILE}"
import json, sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
data = payload.get("data") or {}
version = data.get("version") or {}
if not isinstance(version, dict):
    version = {}
print(version.get("version_id") or version.get("versionId") or "")
PY
)"
  SCHEMA_STATUS="$("${PYTHON_BIN}" - <<'PY' "${INGEST_JSON_FILE}"
import json, sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
data = payload.get("data") or {}
print(str(data.get("schema_status") or ""))
PY
)"

  if [[ -z "${DATASET_ID}" || -z "${INGEST_REQUEST_ID}" || -z "${DATASET_VERSION_ID}" ]]; then
    echo "❌ Dataset ingest failed; response:" >&2
    cat "${resp_file}" >&2
    exit 1
  fi
  export DATASET_ID INGEST_REQUEST_ID DATASET_VERSION_ID SCHEMA_STATUS
  echo "✅ DATASET_ID=${DATASET_ID}"
  echo "✅ INGEST_REQUEST_ID=${INGEST_REQUEST_ID} dataset_version_id=${DATASET_VERSION_ID} schema_status=${SCHEMA_STATUS:-unknown}"
}

approve_schema_if_needed() {
  local resp_file="${E2E_DIR}/02_schema_approve.response.json"
  local url="${API_BASE_URL}/pipelines/datasets/ingest-requests/${INGEST_REQUEST_ID}/schema/approve"
  local status_norm
  status_norm="$(printf '%s' "${SCHEMA_STATUS:-}" | tr '[:upper:]' '[:lower:]' | xargs)"
  if [[ -z "${status_norm}" || "${status_norm}" == "pending" ]]; then
    echo "🧾 Approving schema -> ${url}"
    curl -sS -X POST "${url}" \
      "${AUTH_HEADERS[@]}" \
      -H "Content-Type: application/json" \
      -d '{}' \
      >"${resp_file}"
    echo "✅ Schema approved (best-effort)"
  else
    echo "ℹ️  Skipping schema approval (schema_status=${SCHEMA_STATUS})"
  fi
}

create_agent_session() {
  local req_file="${E2E_DIR}/03_session_create.request.json"
  local resp_file="${E2E_DIR}/03_session_create.response.json"
  local url="${API_BASE_URL}/agent-sessions"

  "${PYTHON_BIN}" - <<'PY' >"${req_file}"
import json
payload = {
  "metadata": {
    "purpose": "agent_pipeline_demo",
  }
}
print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
PY

  echo "🧠 Creating agent session -> ${url}"
  curl -sS -X POST "${url}" "${AGENT_HEADERS[@]}" --data-binary @"${req_file}" >"${resp_file}"

  export SESSION_JSON_FILE="${resp_file}"
  SESSION_ID="$("${PYTHON_BIN}" - <<'PY' "${SESSION_JSON_FILE}"
import json, sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
data = payload.get("data") or {}
session = data.get("session") or {}
print(session.get("session_id") or "")
PY
)"
  SESSION_TENANT_ID="$("${PYTHON_BIN}" - <<'PY' "${SESSION_JSON_FILE}"
import json, sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
data = payload.get("data") or {}
session = data.get("session") or {}
print(session.get("tenant_id") or "")
PY
)"
  if [[ -z "${SESSION_ID}" ]]; then
    echo "❌ Failed to create agent session; response:" >&2
    cat "${resp_file}" >&2
    exit 1
  fi
  if [[ -z "${SESSION_TENANT_ID}" ]]; then
    SESSION_TENANT_ID="${TENANT_ID}"
  fi
  export SESSION_ID SESSION_TENANT_ID
  echo "✅ SESSION_ID=${SESSION_ID} tenant_id=${SESSION_TENANT_ID}"
}

update_session_variables() {
  local req_file="${E2E_DIR}/04_session_variables.request.json"
  local resp_file="${E2E_DIR}/04_session_variables.response.json"
  local url="${API_BASE_URL}/agent-sessions/${SESSION_ID}/variables"
  "${PYTHON_BIN}" - <<'PY' >"${req_file}"
import json
import os
payload = {
  "variables": {
    "run_id": os.environ.get("RUN_ID") or "",
    "db_name": os.environ["DB_NAME"],
    "branch": os.environ["BRANCH"],
    "ontology_branch": os.environ.get("ONTOLOGY_BRANCH") or "",
    "dataset_id": os.environ["DATASET_ID"],
    "dataset_version_id": os.environ.get("DATASET_VERSION_ID") or "",
    "ingest_request_id": os.environ.get("INGEST_REQUEST_ID") or "",
  }
}
print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
PY
  echo "🧩 Setting session variables -> ${url}"
  curl -sS -X PUT "${url}" "${AGENT_HEADERS[@]}" --data-binary @"${req_file}" >"${resp_file}"
}

attach_dataset_context() {
  local req_file="${E2E_DIR}/05_attach_dataset_context.request.json"
  local resp_file="${E2E_DIR}/05_attach_dataset_context.response.json"
  local url="${API_BASE_URL}/agent-sessions/${SESSION_ID}/context/items"

  "${PYTHON_BIN}" - <<'PY' >"${req_file}"
import json
import os
payload = {
  "item_type": "dataset",
  "include_mode": os.environ.get("DATASET_INCLUDE_MODE", "summary"),
  "ref": {
    "db_name": os.environ["DB_NAME"],
    "branch": os.environ["BRANCH"],
    "dataset_id": os.environ["DATASET_ID"],
  },
}
print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
PY

  echo "📎 Attaching dataset context -> ${url}"
  curl -sS -X POST "${url}" "${AGENT_HEADERS[@]}" --data-binary @"${req_file}" >"${resp_file}"
}

upload_optional_context_file() {
  if [[ -z "${CONTEXT_FILE:-}" ]]; then
    return 0
  fi
  if [[ ! -f "${CONTEXT_FILE}" ]]; then
    echo "❌ CONTEXT_FILE not found: ${CONTEXT_FILE}" >&2
    exit 1
  fi
  local resp_file="${E2E_DIR}/06_context_file_upload.response.json"
  local url="${API_BASE_URL}/agent-sessions/${SESSION_ID}/context/file-upload?include_mode=full"
  echo "📄 Uploading CONTEXT_FILE -> ${url}"
  curl -sS -X POST "${url}" \
    -H "X-Admin-Token: ${ADMIN_TOKEN}" \
    -H "X-Delegated-Authorization: Bearer ${USER_JWT}" \
    -F "file=@${CONTEXT_FILE}" \
    >"${resp_file}"
}

wait_for_bff() {
  local url="${API_BASE_URL}/health"
  echo "⏳ Waiting for BFF health: ${url}"
  for ((i=1; i<=RETRIES; i++)); do
    local code
    code="$(curl -sS -o /dev/null -w "%{http_code}" -X GET "${url}" -H "X-Admin-Token: ${ADMIN_TOKEN}")" || true
    if [[ "${code}" == "200" ]]; then
      echo "✅ BFF healthy"
      return 0
    fi
    echo "… (${i}/${RETRIES}) bff_http=${code}"
    sleep "${SLEEP_SECONDS}"
  done
  echo "❌ Timed out waiting for BFF health" >&2
  return 1
}

ensure_database_exists() {
  local resp_file="${E2E_DIR}/00_database_get.response.json"
  local url="${API_BASE_URL}/databases/${DB_NAME}"
  local code
  code="$(curl -sS -o "${resp_file}" -w "%{http_code}" -X GET "${url}" -H "X-Admin-Token: ${ADMIN_TOKEN}")" || true
  if [[ "${code}" == "200" ]]; then
    echo "✅ Database exists: ${DB_NAME}"
    return 0
  fi

  local create_req="${E2E_DIR}/00_database_create.request.json"
  local create_resp="${E2E_DIR}/00_database_create.response.json"
  local create_url="${API_BASE_URL}/databases"
  "${PYTHON_BIN}" - <<'PY' >"${create_req}"
import json, os
payload = {"name": os.environ["DB_NAME"], "description": "agent_pipeline_demo database (e2e)"}
print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
PY

  echo "🗄️  Creating database -> ${create_url}"
  code="$(curl -sS -o "${create_resp}" -w "%{http_code}" -X POST "${create_url}" \
    -H "X-Admin-Token: ${ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    --data-binary @"${create_req}")" || true
  if [[ "${code}" != "201" && "${code}" != "202" && "${code}" != "409" ]]; then
    echo "❌ Database create failed (http=${code}); response:" >&2
    cat "${create_resp}" >&2
    return 1
  fi

  echo "⏳ Waiting for database to become available: ${DB_NAME}"
  for ((i=1; i<=RETRIES; i++)); do
    code="$(curl -sS -o "${resp_file}" -w "%{http_code}" -X GET "${url}" -H "X-Admin-Token: ${ADMIN_TOKEN}")" || true
    if [[ "${code}" == "200" ]]; then
      echo "✅ Database ready: ${DB_NAME}"
      return 0
    fi
    echo "… (${i}/${RETRIES}) db_http=${code}"
    sleep "${SLEEP_SECONDS}"
  done
  echo "❌ Timed out waiting for database ${DB_NAME}" >&2
  return 1
}

ensure_ontology_branch_exists() {
  if ! truthy "${DO_APPLY}"; then
    return 0
  fi
  if ! truthy "${SETUP_OBJECTIFY}"; then
    return 0
  fi

  local branch="${ONTOLOGY_BRANCH}"
  if [[ -z "${branch}" ]]; then
    return 0
  fi

  local safe_branch
  safe_branch="$(printf '%s' "${branch}" | tr '/\\' '__')"
  local create_url="${API_BASE_URL}/databases/${DB_NAME}/ontology/branches"
  local create_req="${E2E_DIR}/00_ontology_branch_create_${safe_branch}.request.json"
  local create_resp="${E2E_DIR}/00_ontology_branch_create_${safe_branch}.response.json"

  "${PYTHON_BIN}" - <<'PY' >"${create_req}"
import json, os
payload = {
  "branch_name": os.environ.get("ONTOLOGY_BRANCH") or "",
  "description": "agent_pipeline_demo ontology branch",
  "from_branch": os.environ.get("ONTOLOGY_BRANCH_FROM") or "main",
}
print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
PY

  echo "🌿 Ensuring ontology branch exists: ${branch} (from=${ONTOLOGY_BRANCH_FROM:-})"
  local code
  code="$(curl -sS -o "${create_resp}" -w "%{http_code}" -X POST "${create_url}" \
    -H "X-Admin-Token: ${ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    --data-binary @"${create_req}")" || true

  if [[ "${code}" != "201" && "${code}" != "200" && "${code}" != "409" ]]; then
    echo "❌ Ontology branch create failed (http=${code}); response:" >&2
    cat "${create_resp}" >&2
    return 1
  fi
  return 0
}

ensure_objectify_prereqs() {
  if ! truthy "${DO_APPLY}"; then
    return 0
  fi
  if ! truthy "${SETUP_OBJECTIFY}"; then
    echo "ℹ️  SETUP_OBJECTIFY=false; skipping ontology/object-type bootstrap"
    return 0
  fi

  ensure_ontology_branch_exists

  local class_id="${OBJECTIFY_CLASS_ID}"
  local get_url="${API_BASE_URL}/databases/${DB_NAME}/ontology/${class_id}?branch=${ONTOLOGY_BRANCH}"
  local get_resp="${E2E_DIR}/00_ontology_get_${class_id}.response.json"
  local code
  code="$(curl -sS -o "${get_resp}" -w "%{http_code}" -X GET "${get_url}" -H "X-Admin-Token: ${ADMIN_TOKEN}")" || true
  if [[ "${code}" != "200" ]]; then
    local create_url="${API_BASE_URL}/databases/${DB_NAME}/ontology?branch=${ONTOLOGY_BRANCH}"
    local create_req="${E2E_DIR}/00_ontology_create_${class_id}.request.json"
    local create_resp="${E2E_DIR}/00_ontology_create_${class_id}.response.json"
    "${PYTHON_BIN}" - <<'PY' >"${create_req}"
import json, os
cid = os.environ.get("OBJECTIFY_CLASS_ID") or "Customer"
payload = {
  "id": cid,
  "label": cid,
  "description": "Objectify target class for agent_pipeline_demo",
  "properties": [
    {"name": "customer_id", "type": "string", "label": "Customer ID", "required": True},
    {"name": "full_name", "type": "string", "label": "Full Name", "required": True, "titleKey": True},
    {"name": "email_address", "type": "string", "label": "Email"},
    {"name": "phone_number", "type": "string", "label": "Phone"},
    {"name": "birth_date", "type": "date", "label": "Birth Date"},
    {"name": "registration_date", "type": "date", "label": "Registration Date"},
    {"name": "city", "type": "string", "label": "City"},
    {"name": "country", "type": "string", "label": "Country"},
    {"name": "total_purchases", "type": "integer", "label": "Total Purchases"},
    {"name": "vip_status", "type": "boolean", "label": "VIP Status"},
  ],
  "relationships": [],
}
print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
PY
    echo "🧬 Creating ontology class '${class_id}' (branch=${ONTOLOGY_BRANCH}) -> ${create_url}"
    code="$(curl -sS -o "${create_resp}" -w "%{http_code}" -X POST "${create_url}" \
      -H "X-Admin-Token: ${ADMIN_TOKEN}" \
      -H "Content-Type: application/json" \
      --data-binary @"${create_req}")" || true
    if [[ "${code}" != "200" && "${code}" != "202" && "${code}" != "409" ]]; then
      echo "❌ Ontology create failed (http=${code}); response:" >&2
      cat "${create_resp}" >&2
      return 1
    fi

    echo "⏳ Waiting for ontology '${class_id}'..."
    local ontology_ready="false"
    for ((i=1; i<=RETRIES; i++)); do
      code="$(curl -sS -o "${get_resp}" -w "%{http_code}" -X GET "${get_url}" -H "X-Admin-Token: ${ADMIN_TOKEN}")" || true
      if [[ "${code}" == "200" ]]; then
        echo "✅ Ontology ready: ${class_id}"
        ontology_ready="true"
        break
      fi
      echo "… (${i}/${RETRIES}) ontology_http=${code}"
      sleep "${SLEEP_SECONDS}"
    done
    if [[ "${ontology_ready}" != "true" ]]; then
      echo "❌ Timed out waiting for ontology '${class_id}'" >&2
      return 1
    fi
  else
    echo "✅ Ontology exists: ${class_id}"
  fi

  local ot_get_url="${API_BASE_URL}/databases/${DB_NAME}/ontology/object-types/${class_id}?branch=${ONTOLOGY_BRANCH}"
  local ot_get_resp="${E2E_DIR}/00_object_type_get_${class_id}.response.json"
  code="$(curl -sS -o "${ot_get_resp}" -w "%{http_code}" -X GET "${ot_get_url}" -H "X-Admin-Token: ${ADMIN_TOKEN}")" || true
  if [[ "${code}" == "200" ]]; then
    echo "✅ Object type contract exists: ${class_id}"
    return 0
  fi

  local ot_create_url="${API_BASE_URL}/databases/${DB_NAME}/ontology/object-types?branch=${ONTOLOGY_BRANCH}"
  local ot_create_req="${E2E_DIR}/00_object_type_create_${class_id}.request.json"
  local ot_create_resp="${E2E_DIR}/00_object_type_create_${class_id}.response.json"
  "${PYTHON_BIN}" - <<'PY' >"${ot_create_req}"
import json, os
cid = os.environ.get("OBJECTIFY_CLASS_ID") or "Customer"
payload = {
  "class_id": cid,
  "backing_dataset_id": os.environ.get("DATASET_ID") or "",
  "dataset_version_id": os.environ.get("DATASET_VERSION_ID") or None,
  "pk_spec": {"primary_key": ["customer_id"], "title_key": ["full_name"]},
  "status": "ACTIVE",
  "metadata": {"purpose": "agent_pipeline_demo"},
}
print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
PY

  echo "🧾 Creating object type contract '${class_id}' (branch=${ONTOLOGY_BRANCH}) -> ${ot_create_url}"
  code="$(curl -sS -o "${ot_create_resp}" -w "%{http_code}" -X POST "${ot_create_url}" \
    -H "X-Admin-Token: ${ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    --data-binary @"${ot_create_req}")" || true
  if [[ "${code}" != "200" && "${code}" != "201" && "${code}" != "202" && "${code}" != "409" ]]; then
    echo "❌ Object type create failed (http=${code}); response:" >&2
    cat "${ot_create_resp}" >&2
    return 1
  fi

  echo "⏳ Waiting for object type contract '${class_id}'..."
  for ((i=1; i<=RETRIES; i++)); do
    code="$(curl -sS -o "${ot_get_resp}" -w "%{http_code}" -X GET "${ot_get_url}" -H "X-Admin-Token: ${ADMIN_TOKEN}")" || true
    if [[ "${code}" == "200" ]]; then
      echo "✅ Object type contract ready: ${class_id}"
      return 0
    fi
    echo "… (${i}/${RETRIES}) object_type_http=${code}"
    sleep "${SLEEP_SECONDS}"
  done
  echo "❌ Timed out waiting for object type contract '${class_id}'" >&2
  return 1
}

decide_approval_if_needed() {
  local approval_request_id="$1"
  if [[ -z "${approval_request_id}" ]]; then
    return 0
  fi

  echo "🛡️  Approval required: approval_request_id=${approval_request_id}"

  if truthy "${AUTO_APPROVE}"; then
    echo "✅ AUTO_APPROVE=true -> approving"
  else
    if [[ -t 0 ]]; then
      read -r -p "Approve this plan now? [y/N] " answer
      if ! truthy "${answer}"; then
        echo "ℹ️  Not approved. Re-run with AUTO_APPROVE=true to auto-approve."
        return 1
      fi
    else
      echo "ℹ️  Non-interactive shell. Re-run with AUTO_APPROVE=true to auto-approve."
      return 1
    fi
  fi

  local req_file="${E2E_DIR}/approval_${approval_request_id}.request.json"
  local resp_file="${E2E_DIR}/approval_${approval_request_id}.response.json"
  local url="${API_BASE_URL}/agent-sessions/${SESSION_ID}/approvals/${approval_request_id}"

  "${PYTHON_BIN}" - <<'PY' >"${req_file}"
import json
payload = {"decision": "APPROVE", "comment": "approved by e2e_agent_pipeline_demo.sh"}
print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
PY

  curl -sS -X POST "${url}" "${AGENT_HEADERS[@]}" --data-binary @"${req_file}" >"${resp_file}"
  RUN_ID_LATEST="$("${PYTHON_BIN}" - <<'PY' "${resp_file}"
import json, sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
data = payload.get("data") or {}
print(data.get("run_id") or "")
PY
)"
  echo "✅ Approval recorded; run_id=${RUN_ID_LATEST:-<unknown>}"
  return 0
}

wait_for_job() {
  local job_id="$1"
  local url="${API_BASE_URL}/agent-sessions/${SESSION_ID}/jobs/${job_id}"
  echo "⏳ Waiting for job completion: job_id=${job_id}"
  for ((i=1; i<=RETRIES; i++)); do
    local resp
    resp="$(curl -sS -X GET "${url}" "${AGENT_HEADERS[@]}")"
    export _JOB_POLL_JSON="${resp}"
    local status
    status="$("${PYTHON_BIN}" - <<'PY'
import json, os
payload = json.loads(os.environ["_JOB_POLL_JSON"])
job = ((payload.get("data") or {}).get("job") or {})
print(str(job.get("status") or ""))
PY
)"
    local run_id
    run_id="$("${PYTHON_BIN}" - <<'PY'
import json, os
payload = json.loads(os.environ["_JOB_POLL_JSON"])
job = ((payload.get("data") or {}).get("job") or {})
print(str(job.get("run_id") or ""))
PY
)"

    if [[ -n "${run_id}" ]]; then
      RUN_ID_LATEST="${run_id}"
    fi

    case "$(printf '%s' "${status}" | tr '[:upper:]' '[:lower:]')" in
      completed|failed|rejected|cancelled)
        echo "✅ Job terminal status: ${status} (run_id=${RUN_ID_LATEST:-})"
        return 0
        ;;
    esac
    echo "… (${i}/${RETRIES}) job_status=${status} run_id=${RUN_ID_LATEST:-}"
    sleep "${SLEEP_SECONDS}"
  done
  echo "❌ Timed out waiting for job ${job_id}" >&2
  return 1
}

send_agent_message() {
  local label="$1"
  local prompt="$2"

  local req_file="${E2E_DIR}/${label}.request.json"
  local resp_file="${E2E_DIR}/${label}.response.json"
  local url="${API_BASE_URL}/agent-sessions/${SESSION_ID}/messages"

  export _PROMPT="${prompt}"
  "${PYTHON_BIN}" - <<'PY' >"${req_file}"
import json, os
payload = {
  "content": os.environ["_PROMPT"],
  "data_scope": {"db_name": os.environ["DB_NAME"], "branch": os.environ["BRANCH"], "dataset_id": os.environ["DATASET_ID"]},
  "execute": True,
}
print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
PY

  echo "💬 ${label}: POST ${url}"
  curl -sS -X POST "${url}" "${AGENT_HEADERS[@]}" --data-binary @"${req_file}" >"${resp_file}"

  export _MSG_JSON_FILE="${resp_file}"
  JOB_ID_LATEST="$("${PYTHON_BIN}" - <<'PY' "${_MSG_JSON_FILE}"
import json, sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
data = payload.get("data") or {}
print(data.get("job_id") or "")
PY
)"
  PLAN_ID_LATEST="$("${PYTHON_BIN}" - <<'PY' "${_MSG_JSON_FILE}"
import json, sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
data = payload.get("data") or {}
print(data.get("plan_id") or "")
PY
)"
  APPROVAL_REQUEST_ID_LATEST="$("${PYTHON_BIN}" - <<'PY' "${_MSG_JSON_FILE}"
import json, sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
data = payload.get("data") or {}
print(data.get("approval_request_id") or "")
PY
)"
  RUN_ID_LATEST="$("${PYTHON_BIN}" - <<'PY' "${_MSG_JSON_FILE}"
import json, sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
data = payload.get("data") or {}
print(data.get("run_id") or "")
PY
)"
  STATUS_LATEST="$("${PYTHON_BIN}" - <<'PY' "${_MSG_JSON_FILE}"
import json, sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
print(str(payload.get("status") or ""))
PY
)"
  MESSAGE_LATEST="$("${PYTHON_BIN}" - <<'PY' "${_MSG_JSON_FILE}"
import json, sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
print(str(payload.get("message") or ""))
PY
)"

  QUESTIONS_COUNT_LATEST="$("${PYTHON_BIN}" - <<'PY' "${_MSG_JSON_FILE}"
import json, sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
data = payload.get("data") or {}
questions = data.get("questions") or []
print(len(questions) if isinstance(questions, list) else 0)
PY
)"
  VALIDATION_ERRORS_COUNT_LATEST="$("${PYTHON_BIN}" - <<'PY' "${_MSG_JSON_FILE}"
import json, sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
data = payload.get("data") or {}
errs = data.get("validation_errors") or []
print(len(errs) if isinstance(errs, list) else 0)
PY
)"

  if [[ "${STATUS_LATEST}" == "warning" ]]; then
    if [[ "${QUESTIONS_COUNT_LATEST}" != "0" || "${VALIDATION_ERRORS_COUNT_LATEST}" != "0" ]]; then
      echo "❌ Clarification required. See: ${resp_file}" >&2
      if [[ "${VALIDATION_ERRORS_COUNT_LATEST}" != "0" ]]; then
        LLM_DISABLED_LATEST="$("${PYTHON_BIN}" - <<'PY' "${_MSG_JSON_FILE}"
import json, sys
payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
errs = ((payload.get("data") or {}).get("validation_errors") or [])
msg = " ".join([str(e) for e in errs]) if isinstance(errs, list) else str(errs)
print("1" if "LLM is disabled" in msg else "0")
PY
)"
        if [[ "${LLM_DISABLED_LATEST}" == "1" ]]; then
          echo "   Hint: planner needs an LLM provider (e.g. set LLM_PROVIDER=openai_compat and OPENAI_API_KEY=<key>)." >&2
        fi
      fi
      return 2
    fi
    echo "WARN (${label}): ${MESSAGE_LATEST}" >&2
  fi

  if [[ "${STATUS_LATEST}" == "error" ]]; then
    echo "❌ Agent message failed (${label}): ${MESSAGE_LATEST}" >&2
    cat "${resp_file}" >&2
    return 1
  fi

  if [[ -z "${PLAN_ID_LATEST:-}" || -z "${JOB_ID_LATEST:-}" ]]; then
    echo "❌ Agent message did not return plan_id/job_id (${label}). See: ${resp_file}" >&2
    cat "${resp_file}" >&2
    return 1
  fi

  echo "✅ plan_id=${PLAN_ID_LATEST:-} job_id=${JOB_ID_LATEST:-} run_id=${RUN_ID_LATEST:-}"

  if [[ -n "${APPROVAL_REQUEST_ID_LATEST}" ]]; then
    decide_approval_if_needed "${APPROVAL_REQUEST_ID_LATEST}" || return 1
  fi
  if [[ -n "${JOB_ID_LATEST}" ]]; then
    wait_for_job "${JOB_ID_LATEST}"
  fi
}

ingest_ci_result() {
  local req_file="${E2E_DIR}/ci_ingest.request.json"
  local resp_file="${E2E_DIR}/ci_ingest.response.json"
  local url="${API_BASE_URL}/admin/ci/ci-results"

  export SESSION_TENANT_ID JOB_ID_LATEST PLAN_ID_LATEST RUN_ID_LATEST CI_PROVIDER CI_STATUS CI_DETAILS_URL CI_SUMMARY
  "${PYTHON_BIN}" - <<'PY' >"${req_file}"
import json, os
payload = {
  "tenant_id": os.environ.get("SESSION_TENANT_ID") or "default",
  "session_id": os.environ["SESSION_ID"],
  "job_id": os.environ.get("JOB_ID_LATEST") or None,
  "plan_id": os.environ.get("PLAN_ID_LATEST") or None,
  "run_id": os.environ.get("RUN_ID_LATEST") or None,
  "provider": (os.environ.get("CI_PROVIDER") or "").strip() or None,
  "status": (os.environ.get("CI_STATUS") or "unknown").strip() or "unknown",
  "details_url": (os.environ.get("CI_DETAILS_URL") or "").strip() or None,
  "summary": (os.environ.get("CI_SUMMARY") or "").strip() or None,
}
print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
PY

  echo "🧪 Ingesting CI result -> ${url}"
  curl -sS -X POST "${url}" \
    -H "X-Admin-Token: ${ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    --data-binary @"${req_file}" \
    >"${resp_file}"
}

###############################################################################
# Main flow
###############################################################################

wait_for_bff
ensure_database_exists
upload_csv_dataset
approve_schema_if_needed
ensure_objectify_prereqs
create_agent_session
update_session_variables
attach_dataset_context
upload_optional_context_file

RECOMMEND_PROMPT="${RECOMMEND_PROMPT:-첨부한 dataset을 분석해서 다음을 해줘:\n1) 데이터 정제/클렌징 규칙(캐스팅, trim, 결측, 중복 제거) 추천\n2) Objectify에 적합한 canonical schema/PK 후보 추천\n3) Pipeline Builder로 실행 가능한 단계별 플랜 생성(지금은 preview/simulate/read 위주로; write/apply는 하지 말 것)\n출력은 실행 계획(툴 호출)과 자연어 요약을 분리해서 제공해줘.}"
send_agent_message "07_message_recommend" "${RECOMMEND_PROMPT}"

if truthy "${DO_APPLY}"; then
  APPLY_PROMPT="${APPLY_PROMPT:-좋아. 방금 추천한 정제/통합을 실제로 적용해서 새 dataset version 또는 새 pipeline을 만들어줘. 반드시 simulate/preview로 검증하고, write 단계는 승인(approval)을 통해 진행해. 마지막에는 Objectify까지 가능한 상태가 되도록 mapping spec 생성/실행까지 포함해줘.}"
  send_agent_message "08_message_apply" "${APPLY_PROMPT}"
else
  echo "ℹ️  DO_APPLY=false; skipping apply stage"
fi

if truthy "${DO_CI_LOOP}"; then
  ingest_ci_result
  REPLAN_PROMPT="${REPLAN_PROMPT:-방금 CI failure 결과를 입력으로 받아 원인을 분석하고, 필요한 수정/재실행 플랜을 만들어줘. 안전하게 preview/simulate를 먼저 하고, 변경이 필요하면 승인 요청을 통해 진행해.}"
  send_agent_message "09_message_replan_from_ci" "${REPLAN_PROMPT}"
else
  echo "ℹ️  DO_CI_LOOP=false; skipping CI ingest/replan stage"
fi

echo
echo "Done."
echo "- Session events: ${API_BASE_URL}/agent-sessions/${SESSION_ID}/events"
echo "- Session tool calls: ${API_BASE_URL}/agent-sessions/${SESSION_ID}/tool-calls"
echo "- Session LLM calls: ${API_BASE_URL}/agent-sessions/${SESSION_ID}/llm-calls"
echo "- Agent run events (latest): ${API_BASE_URL}/agent/runs/${RUN_ID_LATEST:-<run_id>}/events"
echo "- Outputs saved under: ${E2E_DIR}"
