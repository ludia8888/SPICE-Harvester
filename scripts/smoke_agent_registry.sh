#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-127.0.0.1}"
BFF_URL="${BFF_URL:-http://${HOST}:8002}"
ADMIN_TOKEN="${ADMIN_TOKEN:-${BFF_ADMIN_TOKEN:-test-token}}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-spice-foundry-postgres}"
PSQL_USER="${PSQL_USER:-spiceadmin}"
PSQL_DB="${PSQL_DB:-spicedb}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
RETRIES="${RETRIES:-20}"
SLEEP_SECONDS="${SLEEP_SECONDS:-1}"
SKIP_DB_CHECK="${SKIP_DB_CHECK:-}"

if ! command -v curl >/dev/null 2>&1; then
  echo "❌ curl is required" >&2
  exit 1
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "❌ Python not found: PYTHON_BIN=$PYTHON_BIN" >&2
  exit 1
fi

plan_id="$("$PYTHON_BIN" - <<'PY'
import uuid
print(uuid.uuid4())
PY
)"
tool_id="system.health.smoke.$("$PYTHON_BIN" - <<'PY'
import uuid
print(uuid.uuid4().hex[:8])
PY
)"

echo "PLAN_ID=$plan_id"
echo "TOOL_ID=$tool_id"

echo "🔧 Upserting allowlist policy..."
curl -sS -X POST "${BFF_URL}/api/v1/admin/agent-tools" \
  -H "X-Admin-Token: ${ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{
    \"tool_id\": \"${tool_id}\",
    \"method\": \"GET\",
    \"path\": \"/api/v1/health\",
    \"risk_level\": \"read\",
    \"requires_approval\": false,
    \"requires_idempotency_key\": false,
    \"status\": \"ACTIVE\"
  }" >/dev/null

echo "✅ Allowlist policy upserted"

echo "🔍 Validating plan..."
curl -sS -X POST "${BFF_URL}/api/v1/agent-plans/validate" \
  -H "Content-Type: application/json" \
  -d "{
    \"plan_id\": \"${plan_id}\",
    \"goal\": \"smoke\",
    \"risk_level\": \"read\",
    \"requires_approval\": false,
    \"steps\": [
      {
        \"step_id\": \"step_0\",
        \"tool_id\": \"${tool_id}\",
        \"method\": \"GET\"
      }
    ]
  }" >/dev/null

echo "✅ Plan validated"

echo "📝 Recording approval..."
curl -sS -X POST "${BFF_URL}/api/v1/agent-plans/${plan_id}/approvals" \
  -H "X-Admin-Token: ${ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"decision":"APPROVE","comment":"smoke"}' >/dev/null

echo "🚀 Starting agent run..."
run_resp="$(curl -sS -X POST "${BFF_URL}/api/v1/agent/runs" \
  -H "X-Admin-Token: ${ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{
    \"goal\": \"smoke\",
    \"steps\": [
      {
        \"tool_id\": \"${tool_id}\",
        \"service\": \"bff\",
        \"method\": \"GET\",
        \"path\": \"/api/v1/health\"
      }
    ],
    \"context\": {\"plan_id\": \"${plan_id}\", \"risk_level\": \"read\"}
  }")"

run_id="$("$PYTHON_BIN" - <<'PY' "$run_resp"
import json
import sys
payload = json.loads(sys.argv[1])
print((payload.get("data") or {}).get("run_id") or "")
PY
)"

if [[ -z "$run_id" ]]; then
  echo "❌ Failed to parse run_id from: $run_resp" >&2
  exit 1
fi

echo "RUN_ID=$run_id"

echo "⏳ Waiting for run completion..."
for ((i=1; i<=RETRIES; i++)); do
  status_resp="$(curl -sS -X GET "${BFF_URL}/api/v1/agent/runs/${run_id}" \
    -H "X-Admin-Token: ${ADMIN_TOKEN}")"
  status="$("$PYTHON_BIN" - <<'PY' "$status_resp"
import json
import sys
payload = json.loads(sys.argv[1])
print((payload.get("data") or {}).get("status") or "")
PY
)"
  if [[ "$status" == "completed" || "$status" == "failed" ]]; then
    echo "✅ Run status: $status"
    break
  fi
  echo "… still running (${i}/${RETRIES})"
  sleep "$SLEEP_SECONDS"
done

if [[ -z "${SKIP_DB_CHECK}" ]]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "❌ docker is required for DB checks (set SKIP_DB_CHECK=1 to skip)" >&2
    exit 1
  fi
  echo "🔎 Verifying DB records..."
  docker exec "${POSTGRES_CONTAINER}" psql -U "${PSQL_USER}" -d "${PSQL_DB}" \
    -c "select run_id, plan_id, status from spice_agent.agent_runs where run_id = '${run_id}';"
  docker exec "${POSTGRES_CONTAINER}" psql -U "${PSQL_USER}" -d "${PSQL_DB}" \
    -c "select run_id, step_id, tool_id, status from spice_agent.agent_steps where run_id = '${run_id}';"
  docker exec "${POSTGRES_CONTAINER}" psql -U "${PSQL_USER}" -d "${PSQL_DB}" \
    -c "select plan_id, decision, approved_by from spice_agent.agent_approvals where plan_id = '${plan_id}';"
  echo "✅ DB checks passed"
else
  echo "ℹ️  SKIP_DB_CHECK set; skipping Postgres verification"
fi
