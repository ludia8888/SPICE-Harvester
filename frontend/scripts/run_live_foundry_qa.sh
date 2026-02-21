#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FRONTEND_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${FRONTEND_DIR}/.." && pwd)"
BACKEND_DIR="${REPO_ROOT}/backend"

BFF_HOST="${BFF_HOST:-127.0.0.1}"
BFF_PORT="${BFF_PORT:-18012}"
BFF_URL="http://${BFF_HOST}:${BFF_PORT}"
PLAYWRIGHT_PROJECT="${PLAYWRIGHT_PROJECT:-chromium}"
PLAYWRIGHT_SPEC="${PLAYWRIGHT_SPEC:-tests/e2e/live_foundry_qa.spec.ts}"
QA_LOG_FILE="${QA_LOG_FILE:-${REPO_ROOT}/.logs/live_foundry_qa_bff.log}"
E2E_REPEAT="${E2E_REPEAT:-1}"

mkdir -p "$(dirname "${QA_LOG_FILE}")"

require_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "required command not found: ${cmd}" >&2
    exit 1
  fi
}

wait_for_http() {
  local url="$1"
  local attempts="${2:-90}"
  local delay_seconds="${3:-1}"

  for ((i = 1; i <= attempts; i++)); do
    local status
    status="$(curl -sS -o /dev/null -w '%{http_code}' "${url}" || true)"
    if [[ "${status}" == "200" || "${status}" == "401" || "${status}" == "403" ]]; then
      return 0
    fi
    sleep "${delay_seconds}"
  done
  return 1
}

require_cmd curl
require_cmd python
require_cmd pnpm

if ! [[ "${E2E_REPEAT}" =~ ^[0-9]+$ ]] || [[ "${E2E_REPEAT}" -lt 1 ]]; then
  echo "invalid E2E_REPEAT: ${E2E_REPEAT} (must be integer >= 1)" >&2
  exit 1
fi

STARTED_LOCAL_BFF=0
BFF_PID=""

cleanup() {
  if [[ "${STARTED_LOCAL_BFF}" -eq 1 && -n "${BFF_PID}" ]]; then
    kill "${BFF_PID}" >/dev/null 2>&1 || true
    wait "${BFF_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if ! wait_for_http "${BFF_URL}/health" 1 0; then
  echo "live QA: starting local BFF at ${BFF_URL}"
  (
    cd "${BACKEND_DIR}"
    DOCKER_CONTAINER=false \
    ADMIN_TOKEN="${ADMIN_TOKEN:-change_me}" \
    BFF_ADMIN_TOKEN="${BFF_ADMIN_TOKEN:-${ADMIN_TOKEN:-change_me}}" \
    BFF_WRITE_TOKEN="${BFF_WRITE_TOKEN:-${BFF_ADMIN_TOKEN:-change_me}}" \
    OMS_ADMIN_TOKEN="${OMS_ADMIN_TOKEN:-${BFF_ADMIN_TOKEN:-change_me}}" \
    LAKEFS_API_URL="${LAKEFS_API_URL:-http://127.0.0.1:48080}" \
    LAKEFS_S3_ENDPOINT_URL="${LAKEFS_S3_ENDPOINT_URL:-http://127.0.0.1:48080}" \
    LAKEFS_ACCESS_KEY_ID="${LAKEFS_ACCESS_KEY_ID:-spice-lakefs-admin}" \
    LAKEFS_SECRET_ACCESS_KEY="${LAKEFS_SECRET_ACCESS_KEY:-spice-lakefs-admin-secret}" \
    LAKEFS_RAW_REPOSITORY="${LAKEFS_RAW_REPOSITORY:-raw-datasets}" \
    LAKEFS_ARTIFACTS_REPOSITORY="${LAKEFS_ARTIFACTS_REPOSITORY:-pipeline-artifacts}" \
    python -m uvicorn bff.main:app --host "${BFF_HOST}" --port "${BFF_PORT}" \
      >"${QA_LOG_FILE}" 2>&1
  ) &
  BFF_PID="$!"
  STARTED_LOCAL_BFF=1

  if ! wait_for_http "${BFF_URL}/health" 120 1; then
    echo "live QA: BFF failed to become healthy (${BFF_URL}/health)" >&2
    tail -n 120 "${QA_LOG_FILE}" >&2 || true
    exit 1
  fi
fi

echo "live QA: running Playwright (${PLAYWRIGHT_PROJECT}) with BFF ${BFF_URL}"
cd "${FRONTEND_DIR}"
for ((run = 1; run <= E2E_REPEAT; run++)); do
  echo "live QA: run ${run}/${E2E_REPEAT}"
  E2E_VITE_PROXY_TARGET="${BFF_URL}" \
    pnpm exec playwright test "${PLAYWRIGHT_SPEC}" --project="${PLAYWRIGHT_PROJECT}"
done

echo "live QA: completed successfully"
