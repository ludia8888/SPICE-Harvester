#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-127.0.0.1}"
BFF_URL="${BFF_URL:-http://${HOST}:8002}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
INSTANCE_COUNT="${INSTANCE_COUNT:-25}"
DB_NAME="${DB_NAME:-agent_progress_smoke}"
BASE_CLASS_ID="${CLASS_ID:-SmokeItem_$(date +%Y%m%d%H%M%S)}"
PK_FIELDS="${PK_FIELDS:-}"
BRANCH="${BRANCH:-main}"
RETRIES="${RETRIES:-60}"
SLEEP_SECONDS="${SLEEP_SECONDS:-1}"
export DB_NAME INSTANCE_COUNT BRANCH

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
cleanup() {
  rm -rf "${tmp_dir}"
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

  echo "Starting agent run for bulk-create..."
  "${PYTHON_BIN}" - <<'PY' > "${tmp_dir}/run.json"
import json
import os
count = int(os.environ["INSTANCE_COUNT"])
db_name = os.environ["DB_NAME"]
class_id = os.environ["CLASS_ID"]
pk_field = os.environ["PK_FIELD"]
instances = [
    {pk_field: f"item_{idx:04d}", "name": f"Item {idx}"}
    for idx in range(1, count + 1)
]
payload = {
    "goal": "agent progress smoke",
    "steps": [
        {
            "service": "bff",
            "method": "POST",
            "path": f"/api/v1/databases/{db_name}/instances/{class_id}/bulk-create",
            "body": {
                "instances": instances,
                "metadata": {"source": "agent_progress_smoke"},
            },
        }
    ],
    "context": {"risk_level": "write"},
}
print(json.dumps(payload))
PY

  run_resp="$(curl -sS -X POST "${BFF_URL}/api/v1/agent/runs" \
    "${auth_headers[@]}" \
    --data-binary @"${tmp_dir}/run.json")"
  run_id="$(echo "${run_resp}" | jq -r '.data.run_id // empty')"

  if [[ -z "${run_id}" ]]; then
    echo "Failed to parse run_id from: ${run_resp}" >&2
    exit 1
  fi

  echo "RUN_ID=${run_id}"

  progress_updates=0
  last_progress=""
  final_status=""
  command_id=""

  for ((i=1; i<=RETRIES; i++)); do
    run_status="$(curl -sS -X GET "${BFF_URL}/api/v1/agent/runs/${run_id}" "${auth_headers[@]}")"
    final_status="$(echo "${run_status}" | jq -r '.data.status // empty')"
    progress="$(echo "${run_status}" | jq -r '.data.progress.progress.percentage // empty')"
    progress_message="$(echo "${run_status}" | jq -r '.data.progress.progress.message // empty')"
    command_id="$(echo "${run_status}" | jq -r '.data.progress.command_id // empty')"

    if [[ -n "${progress}" && "${progress}" != "${last_progress}" ]]; then
      progress_updates=$((progress_updates + 1))
      last_progress="${progress}"
      if [[ -n "${progress_message}" ]]; then
        echo "progress: ${progress}% (${progress_message})"
      else
        echo "progress: ${progress}%"
      fi
    fi

    if [[ "${final_status}" == "completed" || "${final_status}" == "failed" ]]; then
      echo "agent run status=${final_status}"
      break
    fi
    sleep "${SLEEP_SECONDS}"
  done

  if [[ "${final_status}" != "completed" ]]; then
    echo "Agent run did not complete (status=${final_status})" >&2
    exit 1
  fi

  if [[ "${progress_updates}" -lt 1 ]]; then
    echo "No progress updates observed from agent run" >&2
    exit 1
  fi

  if [[ -n "${command_id}" ]]; then
    wait_for_command "${command_id}"
  fi
done

echo "Smoke complete"
