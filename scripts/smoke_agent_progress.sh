#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-127.0.0.1}"
BFF_URL="${BFF_URL:-http://${HOST}:8002}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
INSTANCE_COUNT="${INSTANCE_COUNT:-25}"
DB_NAME="${DB_NAME:-agent_progress_smoke_$(date +%Y%m%d%H%M%S)}"
BASE_CLASS_ID="${CLASS_ID:-SmokeItem_$(date +%Y%m%d%H%M%S)}"
PK_FIELDS="${PK_FIELDS:-}"
BRANCH="${BRANCH:-main}"
RETRIES="${RETRIES:-60}"
SLEEP_SECONDS="${SLEEP_SECONDS:-1}"
export DB_NAME INSTANCE_COUNT BRANCH
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLEANUP_OBJECT_STORE="${CLEANUP_OBJECT_STORE:-true}"

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
  echo "Missing admin token. Set ADMIN_TOKEN or BFF_ADMIN_TOKEN." >&2
  exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required" >&2
  exit 1
fi

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "Python not found: PYTHON_BIN=${PYTHON_BIN}" >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
truthy() {
  local v
  v="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]' | xargs)"
  [[ "${v}" == "1" || "${v}" == "true" || "${v}" == "yes" || "${v}" == "y" ]]
}

cleanup() {
  rm -rf "${tmp_dir}"
  if truthy "${CLEANUP_OBJECT_STORE}" && [[ -f "${SCRIPT_DIR}/dev_cleanup_object_store.py" ]]; then
    PYTHONPATH="${SCRIPT_DIR}/../backend:${PYTHONPATH:-}" \
      "${PYTHON_BIN}" "${SCRIPT_DIR}/dev_cleanup_object_store.py" --yes --db "${DB_NAME}" --quiet || true
  fi
}
trap cleanup EXIT

auth_headers=(-H "X-Admin-Token: ${ADMIN_TOKEN}" -H "Content-Type: application/json")

if [[ -z "${PK_FIELDS}" ]]; then
  base_lower="$(printf '%s' "${BASE_CLASS_ID}" | tr '[:upper:]' '[:lower:]')"
  PK_FIELD="${PK_FIELD:-${base_lower}_id}"
  PK_FIELDS="${PK_FIELD}"
fi

IFS=',' read -r -a pk_field_list <<< "${PK_FIELDS}"

wait_for_command() {
  local command_id="$1"
  local status=""
  for ((i=1; i<=RETRIES; i++)); do
    status="$(curl -sS -X GET "${BFF_URL}/api/v1/commands/${command_id}/status" "${auth_headers[@]}" \
      | jq -r '.status // .data.status // empty')"
    if [[ "${status}" == "COMPLETED" || "${status}" == "FAILED" || "${status}" == "CANCELLED" ]]; then
      echo "command ${command_id} status=${status}"
      return 0
    fi
    sleep "${SLEEP_SECONDS}"
  done
  echo "Timed out waiting for command ${command_id} (status=${status})" >&2
  return 1
}

wait_for_database() {
  for ((i=1; i<=RETRIES; i++)); do
    if curl -sS -X GET "${BFF_URL}/api/v1/databases/${DB_NAME}" "${auth_headers[@]}" \
      | jq -e '
          (.data.data.exists // .data.exists // .exists // false) == true
        ' >/dev/null 2>&1; then
      return 0
    fi
    sleep "${SLEEP_SECONDS}"
  done
  echo "Timed out waiting for database visibility: ${DB_NAME}" >&2
  return 1
}

echo "Creating database: ${DB_NAME}"
"${PYTHON_BIN}" - <<'PY' > "${tmp_dir}/db.json"
import json
import os
payload = {
    "name": os.environ["DB_NAME"],
    "description": "agent progress smoke",
}
print(json.dumps(payload))
PY

db_code="$(curl -sS -o "${tmp_dir}/db_resp.json" -w "%{http_code}" \
  -X POST "${BFF_URL}/api/v1/databases" \
  "${auth_headers[@]}" \
  --data-binary @"${tmp_dir}/db.json")"

if [[ "${db_code}" == "202" ]]; then
  db_command_id="$(jq -r '.data.command_id // .data.commandId // .command_id // .commandId // empty' \
    "${tmp_dir}/db_resp.json")"
  if [[ -n "${db_command_id}" ]]; then
    wait_for_command "${db_command_id}"
  fi
elif [[ "${db_code}" != "200" && "${db_code}" != "201" && "${db_code}" != "409" ]]; then
  echo "Database create failed (status=${db_code}):" >&2
  cat "${tmp_dir}/db_resp.json" >&2
  exit 1
fi

wait_for_database

for pk_field_raw in "${pk_field_list[@]}"; do
  pk_field="$(printf '%s' "${pk_field_raw}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  if [[ -z "${pk_field}" ]]; then
    continue
  fi

  pk_field_lower="$(printf '%s' "${pk_field}" | tr '[:upper:]' '[:lower:]')"
  if [[ "${pk_field_lower}" != "${pk_field}" ]]; then
    echo "ℹ️  Normalizing PK_FIELD '${pk_field}' -> '${pk_field_lower}'"
    pk_field="${pk_field_lower}"
  fi

  if [[ "${pk_field}" != *_id ]]; then
    echo "⚠️  Skipping PK_FIELD '${pk_field}' (must end with _id for class_id-derived PK rule)" >&2
    continue
  fi

  class_base="${pk_field%_id}"
  if [[ -z "${class_base}" ]]; then
    echo "⚠️  Skipping PK_FIELD '${pk_field}' (empty class base)" >&2
    continue
  fi
  if ! [[ "${class_base}" =~ ^[a-zA-Z][a-zA-Z0-9_:-]*$ ]]; then
    echo "⚠️  Skipping PK_FIELD '${pk_field}' (class_id base '${class_base}' is invalid)" >&2
    continue
  fi

  CLASS_ID="${class_base}"
  CLASS_ID_LOWER="$(printf '%s' "${CLASS_ID}" | tr '[:upper:]' '[:lower:]')"
  PK_FIELD="${pk_field}"
  export CLASS_ID CLASS_ID_LOWER PK_FIELD

  echo "Scenario: PK_FIELD=${PK_FIELD} CLASS_ID=${CLASS_ID} (expected_key=${CLASS_ID_LOWER}_id)"
  echo "Creating ontology: ${CLASS_ID}"
  "${PYTHON_BIN}" - <<'PY' > "${tmp_dir}/class.json"
import json
import os
class_id = os.environ["CLASS_ID"]
pk_field = os.environ["PK_FIELD"]
payload = {
    "id": class_id,
    "label": class_id,
    "description": "agent progress smoke class",
    "properties": [
        {
            "name": pk_field,
            "type": "xsd:string",
            "label": pk_field,
            "required": True,
            "primaryKey": True,
            "titleKey": True,
        },
        {
            "name": "name",
            "type": "xsd:string",
            "label": "name",
            "required": False,
        },
    ],
    "relationships": [],
}
print(json.dumps(payload))
PY

  class_code="$(curl -sS -o "${tmp_dir}/class_resp.json" -w "%{http_code}" \
    -X POST "${BFF_URL}/api/v1/databases/${DB_NAME}/ontology?branch=${BRANCH}" \
    "${auth_headers[@]}" \
    --data-binary @"${tmp_dir}/class.json")"

  if [[ "${class_code}" == "202" ]]; then
    class_command_id="$(jq -r '.data.command_id // .data.commandId // .command_id // .commandId // empty' \
      "${tmp_dir}/class_resp.json")"
    if [[ -n "${class_command_id}" ]]; then
      wait_for_command "${class_command_id}"
    fi
  elif [[ "${class_code}" != "200" && "${class_code}" != "201" && "${class_code}" != "409" ]]; then
    echo "Ontology create failed (status=${class_code}):" >&2
    cat "${tmp_dir}/class_resp.json" >&2
    exit 1
  fi

  echo "Submitting async bulk-create command..."
  "${PYTHON_BIN}" - <<'PY' > "${tmp_dir}/bulk_create.json"
import json
import os
count = int(os.environ["INSTANCE_COUNT"])
pk_field = os.environ["PK_FIELD"]
instances = [
    {pk_field: f"item_{idx:04d}", "name": f"Item {idx}"}
    for idx in range(1, count + 1)
]
payload = {
    "instances": instances,
    "metadata": {"source": "agent_progress_smoke"},
}
print(json.dumps(payload))
PY

  bulk_code="$(curl -sS -o "${tmp_dir}/bulk_create_resp.json" -w "%{http_code}" \
    -X POST "${BFF_URL}/api/v1/databases/${DB_NAME}/instances/${CLASS_ID}/bulk-create?branch=${BRANCH}" \
    "${auth_headers[@]}" \
    --data-binary @"${tmp_dir}/bulk_create.json")"

  if [[ "${bulk_code}" != "200" && "${bulk_code}" != "202" ]]; then
    echo "Bulk-create submit failed (status=${bulk_code}):" >&2
    cat "${tmp_dir}/bulk_create_resp.json" >&2
    exit 1
  fi

  command_id="$(jq -r '.command_id // .commandId // .data.command_id // .data.commandId // empty' \
    "${tmp_dir}/bulk_create_resp.json")"
  if [[ -z "${command_id}" ]]; then
    echo "Missing command_id in bulk-create response:" >&2
    cat "${tmp_dir}/bulk_create_resp.json" >&2
    exit 1
  fi

  wait_for_command "${command_id}"
  final_status="$(curl -sS -X GET "${BFF_URL}/api/v1/commands/${command_id}/status" "${auth_headers[@]}" \
    | jq -r '.status // .data.status // empty')"
  if [[ "${final_status}" != "COMPLETED" ]]; then
    echo "Bulk-create command did not complete successfully (status=${final_status})" >&2
    exit 1
  fi
done

echo "Smoke complete"
