#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-127.0.0.1}"
BFF_URL="${BFF_URL:-http://${HOST}:8002/api/v1/health}"
LAKEFS_URL="${LAKEFS_URL:-http://${HOST}:48080/healthcheck}"
RETRIES="${RETRIES:-60}"
SLEEP_SECONDS="${SLEEP_SECONDS:-2}"

wait_for_url() {
  local name="$1"
  local url="$2"
  local attempt=1

  while (( attempt <= RETRIES )); do
    if curl -fsS --connect-timeout 2 --max-time 5 -o /dev/null "$url"; then
      echo "OK ${name} reachable: ${url}"
      return 0
    fi
    echo "WAIT ${name} not ready yet (attempt ${attempt}/${RETRIES})"
    sleep "$SLEEP_SECONDS"
    attempt=$((attempt + 1))
  done

  echo "FAIL ${name} did not become reachable within timeout: ${url}" >&2
  return 1
}

wait_for_url "BFF" "$BFF_URL"
wait_for_url "lakeFS" "$LAKEFS_URL"
