#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-127.0.0.1}"
BFF_URL="${BFF_URL:-http://${HOST}:8002}"
ADMIN_TOKEN="${ADMIN_TOKEN:-${BFF_ADMIN_TOKEN:-test-token}}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-spice-harvester-postgres}"
PSQL_USER="${PSQL_USER:-spiceadmin}"
PSQL_DB="${PSQL_DB:-spicedb}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
RETRIES="${RETRIES:-20}"
SLEEP_SECONDS="${SLEEP_SECONDS:-1}"
SKIP_DB_CHECK="${SKIP_DB_CHECK:-}"

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

if ! command -v curl >/dev/null 2>&1; then
  echo "❌ curl is required" >&2
  exit 1
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "❌ Python not found: PYTHON_BIN=$PYTHON_BIN" >&2
  exit 1
fi

tool_id="system.health.smoke.$("$PYTHON_BIN" - <<'PY'
import uuid
print(uuid.uuid4().hex[:8])
PY
)"

echo "TOOL_ID=$tool_id"

if [[ -z "${USER_JWT}" && -n "${USER_JWT_HS256_SECRET}" ]]; then
  export USER_JWT_HS256_SECRET USER_JWT_TTL_SECONDS USER_ID USER_EMAIL USER_TYPE TENANT_ID USER_ROLES USER_JWT_ISSUER USER_JWT_AUDIENCE
  USER_JWT="$("$PYTHON_BIN" - <<'PY'
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

AGENT_HEADERS=(
  -H "X-Admin-Token: ${ADMIN_TOKEN}"
  -H "Content-Type: application/json"
)
if [[ -n "${USER_JWT}" ]]; then
  AGENT_HEADERS+=(-H "X-Delegated-Authorization: Bearer ${USER_JWT}")
fi

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

echo "🚀 Starting agent run..."
run_resp="$(curl -sS -X POST "${BFF_URL}/api/v1/agent/runs" \
  "${AGENT_HEADERS[@]}" \
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
    \"context\": {\"risk_level\": \"read\"}
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
    "${AGENT_HEADERS[@]}")"
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
