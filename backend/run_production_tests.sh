#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"

MODE="full"
WAIT_FOR_SERVICES="true"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-90}"
RUN_OMS_SMOKE="true"
RUN_CORE="true"
RUN_CHAOS_LITE="false"
RUN_CHAOS_OUT_OF_ORDER="false"
RUN_CHAOS_SOAK="false"
SOAK_SECONDS="${SOAK_SECONDS:-300}"
SOAK_SEED="${SOAK_SEED:-0}"

trim_trailing_slash() {
  local value="${1:-}"
  echo "${value%/}"
}

OMS_URL="$(trim_trailing_slash "${OMS_BASE_URL:-http://localhost:8000}")"
BFF_URL="$(trim_trailing_slash "${BFF_BASE_URL:-http://localhost:8002}")"
FUNNEL_URL="$(trim_trailing_slash "${FUNNEL_BASE_URL:-http://localhost:8003}")"
MINIO_URL="$(trim_trailing_slash "${MINIO_ENDPOINT_URL:-http://localhost:9000}")"

usage() {
  cat <<'USAGE'
Usage: ./run_production_tests.sh [options]

Options:
  --quick            Run only Postgres correctness layer tests
  --full             Run full suite (default)
  --chaos-lite       Run docker-compose chaos-lite scenarios (stops/restarts infra; destructive)
  --chaos-out-of-order  Run out-of-order seq delivery scenario (Kafka injection; destructive)
  --chaos-soak       Run soak test with random failures (long-running; destructive)
  --soak-seconds     Soak duration (default: 300; or env SOAK_SECONDS)
  --soak-seed        Soak RNG seed (default: 0=now; or env SOAK_SEED)
  --no-wait          Do not wait for HTTP health endpoints
  --timeout-seconds  Health wait timeout (default: 90)
  --no-oms-smoke     Skip OMS live smoke test (creates/deletes real DBs)
  --skip-core        Skip full-stack core flow tests
  -h, --help         Show help

Notes:
- Tests live under `backend/tests/` (run from `backend/`).
- Full suite expects local ports from docker compose:
  OMS 8000, BFF 8002, Funnel 8003, MinIO 9000, Elasticsearch 9200, TerminusDB 6363, Kafka 9092, Postgres 5433/5432.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --quick)
      MODE="quick"
      RUN_OMS_SMOKE="false"
      RUN_CORE="false"
      shift
      ;;
    --full)
      MODE="full"
      shift
      ;;
    --chaos-lite)
      RUN_CHAOS_LITE="true"
      shift
      ;;
    --chaos-out-of-order)
      RUN_CHAOS_OUT_OF_ORDER="true"
      shift
      ;;
    --chaos-soak)
      RUN_CHAOS_SOAK="true"
      shift
      ;;
    --soak-seconds)
      SOAK_SECONDS="${2:-}"
      shift 2
      ;;
    --soak-seed)
      SOAK_SEED="${2:-}"
      shift 2
      ;;
    --no-wait)
      WAIT_FOR_SERVICES="false"
      shift
      ;;
    --timeout-seconds)
      TIMEOUT_SECONDS="${2:-}"
      shift 2
      ;;
    --no-oms-smoke)
      RUN_OMS_SMOKE="false"
      shift
      ;;
    --skip-core)
      RUN_CORE="false"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "❌ Python not found: PYTHON_BIN=$PYTHON_BIN" >&2
  exit 1
fi

if ! "$PYTHON_BIN" -m pytest --version >/dev/null 2>&1; then
  echo "❌ pytest is not available in this Python environment." >&2
  echo "   Install test deps (example):" >&2
  echo "   cd backend && pip install -e ./shared[test] && pip install -r ./tests/requirements.txt" >&2
  exit 1
fi

wait_for_url() {
  local name="$1"
  local url="$2"
  local timeout_s="$3"

  local deadline
  deadline="$(( $(date +%s) + timeout_s ))"

  while [[ "$(date +%s)" -lt "$deadline" ]]; do
    if curl -fsS --max-time 2 "$url" >/dev/null 2>&1; then
      echo "✅ $name healthy: $url"
      return 0
    fi
    sleep 1
  done

  echo "❌ Timed out waiting for $name: $url (timeout=${timeout_s}s)" >&2
  return 1
}

if [[ "$WAIT_FOR_SERVICES" == "true" && "$MODE" == "full" ]]; then
  echo "⏳ Waiting for services (timeout=${TIMEOUT_SECONDS}s)..."
  wait_for_url "MinIO" "${MINIO_URL}/minio/health/live" "$TIMEOUT_SECONDS"
  wait_for_url "OMS" "${OMS_URL}/health" "$TIMEOUT_SECONDS"
  wait_for_url "BFF" "${BFF_URL}/health" "$TIMEOUT_SECONDS"
  wait_for_url "Funnel" "${FUNNEL_URL}/health" "$TIMEOUT_SECONDS"
fi

echo "🧪 Running tests (mode=$MODE)..."

# 1) Postgres-backed correctness layer (idempotency + ordering + seq allocator)
"$PYTHON_BIN" -m pytest tests/test_sequence_allocator.py -q
"$PYTHON_BIN" -m pytest tests/test_idempotency_chaos.py -q

if [[ "$MODE" == "full" ]]; then
  # 2) TerminusDB primitives used by OMS (branch/version control)
  "$PYTHON_BIN" -m pytest tests/test_terminus_version_control.py -q

  # 3) OMS live smoke via HTTP (destructive; opt-out via --no-oms-smoke)
  if [[ "$RUN_OMS_SMOKE" == "true" ]]; then
    RUN_LIVE_OMS_SMOKE=true "$PYTHON_BIN" -m pytest tests/test_oms_smoke.py -q
  fi

  # 4) Full stack core flow (OMS/BFF/Funnel + Kafka + ES + Terminus)
  if [[ "$RUN_CORE" == "true" ]]; then
    "$PYTHON_BIN" -m pytest tests/test_core_functionality.py -q
  fi

  # 5) Chaos-lite (no mocks; controls docker-compose)
  if [[ "$RUN_CHAOS_LITE" == "true" ]]; then
    if ! command -v docker >/dev/null 2>&1; then
      echo "❌ docker is required for --chaos-lite" >&2
      exit 1
    fi
    "$PYTHON_BIN" tests/chaos_lite.py
  fi

  # 6) Extra chaos: out-of-order seq delivery (Kafka injection)
  if [[ "$RUN_CHAOS_OUT_OF_ORDER" == "true" ]]; then
    if ! command -v docker >/dev/null 2>&1; then
      echo "❌ docker is required for --chaos-out-of-order" >&2
      exit 1
    fi
    "$PYTHON_BIN" tests/chaos_lite.py --skip-lite --out-of-order
  fi

  # 7) Extra chaos: soak/random failures
  if [[ "$RUN_CHAOS_SOAK" == "true" ]]; then
    if ! command -v docker >/dev/null 2>&1; then
      echo "❌ docker is required for --chaos-soak" >&2
      exit 1
    fi
    "$PYTHON_BIN" tests/chaos_lite.py --skip-lite --soak --soak-seconds "$SOAK_SECONDS" --soak-seed "$SOAK_SEED"
  fi
fi

echo "✅ Production test suite complete."
