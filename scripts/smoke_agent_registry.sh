#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-127.0.0.1}"
BFF_URL="${BFF_URL:-http://${HOST}:8002}"
ADMIN_TOKEN="${ADMIN_TOKEN:-${BFF_ADMIN_TOKEN:-${BFF_WRITE_TOKEN:-${ADMIN_API_KEY:-}}}}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
DB_NAME="${DB_NAME:-agent_registry_smoke_$(date +%Y%m%d%H%M%S)}"
BRANCH="${BRANCH:-main}"
RETRIES="${RETRIES:-30}"
SLEEP_SECONDS="${SLEEP_SECONDS:-1}"
STRICT_STATUS="${STRICT_STATUS:-}"

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

if [[ -z "${ADMIN_TOKEN}" ]]; then
  echo "Missing admin token. Set ADMIN_TOKEN or BFF_ADMIN_TOKEN." >&2
  exit 1
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

auth_headers=(-H "X-Admin-Token: ${ADMIN_TOKEN}" -H "Content-Type: application/json")
if [[ -n "${USER_JWT}" ]]; then
  auth_headers+=(-H "X-Delegated-Authorization: Bearer ${USER_JWT}")
fi

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

tmp_dir="$(mktemp -d)"
cleanup() {
  rm -rf "${tmp_dir}"
}
trap cleanup EXIT

echo "Ensuring database exists: ${DB_NAME}"
db_code="$(curl -sS -o "${tmp_dir}/db_resp.json" -w "%{http_code}" \
  -X POST "${BFF_URL}/api/v1/databases" \
  "${auth_headers[@]}" \
  -d "{\"name\":\"${DB_NAME}\",\"description\":\"agent registry smoke\"}")"

if [[ "${db_code}" == "202" ]]; then
  db_command_id="$(jq -r '.data.command_id // .data.commandId // .command_id // .commandId // empty' "${tmp_dir}/db_resp.json")"
  if [[ -n "${db_command_id}" ]]; then
    wait_for_command "${db_command_id}"
  fi
elif [[ "${db_code}" != "200" && "${db_code}" != "201" && "${db_code}" != "409" ]]; then
  echo "Database create failed (status=${db_code}):" >&2
  cat "${tmp_dir}/db_resp.json" >&2
  exit 1
fi

cat > "${tmp_dir}/pipeline_run.json" <<JSON
{
  "goal": "agent registry smoke",
  "data_scope": {
    "db_name": "${DB_NAME}",
    "branch": "${BRANCH}",
    "dataset_ids": []
  },
  "preview_limit": 5,
  "include_run_tables": false,
  "max_repairs": 0,
  "max_cleansing": 0,
  "max_transform": 0,
  "auto_sync": false
}
JSON

echo "Posting /api/v1/agent/pipeline-runs..."
run_code="$(curl -sS -o "${tmp_dir}/pipeline_run_resp.json" -w "%{http_code}" \
  -X POST "${BFF_URL}/api/v1/agent/pipeline-runs" \
  "${auth_headers[@]}" \
  --data-binary @"${tmp_dir}/pipeline_run.json")"

if [[ "${run_code}" == "404" ]]; then
  echo "Contract drift: /api/v1/agent/pipeline-runs returned 404" >&2
  cat "${tmp_dir}/pipeline_run_resp.json" >&2
  exit 1
fi

if [[ -n "${STRICT_STATUS}" && "${run_code}" != "${STRICT_STATUS}" ]]; then
  echo "Expected STRICT_STATUS=${STRICT_STATUS}, got ${run_code}" >&2
  cat "${tmp_dir}/pipeline_run_resp.json" >&2
  exit 1
fi

case "${run_code}" in
  200|400|401|403|422|429|503)
    ;;
  *)
    echo "Unexpected status from /api/v1/agent/pipeline-runs: ${run_code}" >&2
    cat "${tmp_dir}/pipeline_run_resp.json" >&2
    exit 1
    ;;
esac

api_status="$(jq -r '.status // empty' "${tmp_dir}/pipeline_run_resp.json")"
agent_status="$(jq -r '.data.status // empty' "${tmp_dir}/pipeline_run_resp.json")"
message="$(jq -r '.message // empty' "${tmp_dir}/pipeline_run_resp.json")"

echo "pipeline-runs status_code=${run_code} api_status=${api_status:-n/a} agent_status=${agent_status:-n/a}"
if [[ -n "${message}" && "${message}" != "null" ]]; then
  echo "message=${message}"
fi

echo "Smoke complete"
