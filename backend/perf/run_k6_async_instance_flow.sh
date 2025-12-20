#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

K6_IMAGE="${K6_IMAGE:-grafana/k6:0.49.0}"

# When running k6 inside Docker, localhost is the k6 container.
# - macOS/Windows: use host.docker.internal
# - Linux: prefer --network host or set K6_BASE_URL=http://172.17.0.1:8002/api/v1
K6_BASE_URL="${K6_BASE_URL:-http://host.docker.internal:8002/api/v1}"
K6_ADMIN_TOKEN="${K6_ADMIN_TOKEN:-${ADMIN_TOKEN:-change_me}}"

K6_VUS="${K6_VUS:-5}"
K6_ITERATIONS="${K6_ITERATIONS:-20}"
K6_MAX_DURATION="${K6_MAX_DURATION:-10m}"
K6_THINK_TIME_SECONDS="${K6_THINK_TIME_SECONDS:-0.1}"

exec docker run --rm \
  -e K6_BASE_URL="$K6_BASE_URL" \
  -e K6_ADMIN_TOKEN="$K6_ADMIN_TOKEN" \
  -e K6_VUS="$K6_VUS" \
  -e K6_ITERATIONS="$K6_ITERATIONS" \
  -e K6_MAX_DURATION="$K6_MAX_DURATION" \
  -e K6_THINK_TIME_SECONDS="$K6_THINK_TIME_SECONDS" \
  -v "$ROOT_DIR:/work" \
  -w /work \
  "$K6_IMAGE" run backend/perf/k6/async_instance_flow.js

