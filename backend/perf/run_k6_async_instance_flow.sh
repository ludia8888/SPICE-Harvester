#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
K6_SCRIPT_PATH="${K6_SCRIPT_PATH:-$ROOT_DIR/backend/perf/k6/async_instance_flow.js}"

K6_IMAGE="${K6_IMAGE:-grafana/k6:0.49.0}"
K6_DOCKER_USER="${K6_DOCKER_USER:-0:0}"

# When running k6 inside Docker, localhost is the k6 container.
# - macOS/Windows: use host.docker.internal
# - Linux: prefer --network host or set K6_BASE_URL=http://172.17.0.1:8002/api/v1
K6_BASE_URL="${K6_BASE_URL:-http://host.docker.internal:8002/api/v1}"
K6_ADMIN_TOKEN="${K6_ADMIN_TOKEN:-${ADMIN_TOKEN:-change_me}}"

K6_VUS="${K6_VUS:-5}"
K6_ITERATIONS="${K6_ITERATIONS:-20}"
K6_MAX_DURATION="${K6_MAX_DURATION:-10m}"
K6_THINK_TIME_SECONDS="${K6_THINK_TIME_SECONDS:-0.1}"
K6_COMMAND_TIMEOUT_MS="${K6_COMMAND_TIMEOUT_MS:-240000}"
K6_COMMAND_POLL_INTERVAL_MS="${K6_COMMAND_POLL_INTERVAL_MS:-500}"
K6_SETUP_TIMEOUT="${K6_SETUP_TIMEOUT:-10m}"

exec docker run --rm \
  -i \
  --user "$K6_DOCKER_USER" \
  -e K6_BASE_URL="$K6_BASE_URL" \
  -e K6_ADMIN_TOKEN="$K6_ADMIN_TOKEN" \
  -e K6_VUS="$K6_VUS" \
  -e K6_ITERATIONS="$K6_ITERATIONS" \
  -e K6_MAX_DURATION="$K6_MAX_DURATION" \
  -e K6_SETUP_TIMEOUT="$K6_SETUP_TIMEOUT" \
  -e K6_THINK_TIME_SECONDS="$K6_THINK_TIME_SECONDS" \
  -e K6_COMMAND_TIMEOUT_MS="$K6_COMMAND_TIMEOUT_MS" \
  -e K6_COMMAND_POLL_INTERVAL_MS="$K6_COMMAND_POLL_INTERVAL_MS" \
  "$K6_IMAGE" run - < "$K6_SCRIPT_PATH"
