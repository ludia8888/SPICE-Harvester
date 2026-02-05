#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PYTHON_BIN_ENV="${PYTHON_BIN:-}"
PYTHON_BIN="${PYTHON_BIN_ENV:-python3}"

MODE="full"
WAIT_FOR_SERVICES="true"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-90}"
RUN_OMS_SMOKE="true"
RUN_CORE="true"
RUN_CHAOS_LITE="false"
RUN_CHAOS_OUT_OF_ORDER="false"
RUN_CHAOS_SOAK="false"
CLEAN_SEARCH="${CLEAN_SEARCH:-true}"
SOAK_SECONDS="${SOAK_SECONDS:-300}"
SOAK_SEED="${SOAK_SEED:-0}"

trim_trailing_slash() {
  local value="${1:-}"
  echo "${value%/}"
}

OMS_URL="$(trim_trailing_slash "${OMS_BASE_URL:-http://127.0.0.1:8000}")"
BFF_URL="$(trim_trailing_slash "${BFF_BASE_URL:-http://127.0.0.1:8002}")"
FUNNEL_URL="$(trim_trailing_slash "${FUNNEL_BASE_URL:-http://127.0.0.1:8003}")"
AGENT_URL="$(trim_trailing_slash "${AGENT_BASE_URL:-http://127.0.0.1:8004}")"

load_dotenv_defaults() {
  local dotenv_path="$1"
  [[ -f "$dotenv_path" ]] || return 0

  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line#"${line%%[![:space:]]*}"}" # ltrim
    line="${line%"${line##*[![:space:]]}"}" # rtrim

    [[ -z "$line" ]] && continue
    [[ "$line" == \#* ]] && continue

    line="${line#export }"

    if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]]; then
      local key="${BASH_REMATCH[1]}"
      local value="${BASH_REMATCH[2]}"

      if [[ "$value" =~ ^\"(.*)\"$ ]]; then
        value="${BASH_REMATCH[1]}"
      elif [[ "$value" =~ ^\'(.*)\'$ ]]; then
        value="${BASH_REMATCH[1]}"
      fi

      if [[ -z "${!key:-}" ]]; then
        export "$key=$value"
      fi
    fi
  done <"$dotenv_path"
}

# Load repo root `.env` (port overrides, etc) but don't override explicit env vars.
load_dotenv_defaults "$REPO_ROOT/.env"

# Compose-aligned defaults (local host ports may be overridden by `.env`)
POSTGRES_PORT_HOST="${POSTGRES_PORT_HOST:-5433}"
REDIS_PORT_HOST="${REDIS_PORT_HOST:-6379}"
MINIO_PORT_HOST="${MINIO_PORT_HOST:-9000}"
ELASTICSEARCH_PORT_HOST="${ELASTICSEARCH_PORT_HOST:-9200}"
KAFKA_PORT_HOST="${KAFKA_PORT_HOST:-39092}"
LAKEFS_PORT_HOST="${LAKEFS_PORT_HOST:-48080}"
AGENT_PORT_HOST="${AGENT_PORT_HOST:-8004}"
INGEST_RECONCILER_PORT_HOST="${INGEST_RECONCILER_PORT_HOST:-8012}"

REDIS_PASSWORD="${REDIS_PASSWORD:-spicepass123}"
export POSTGRES_URL="${POSTGRES_URL:-postgresql://spiceadmin:spicepass123@127.0.0.1:${POSTGRES_PORT_HOST}/spicedb}"
export REDIS_URL="${REDIS_URL:-redis://:${REDIS_PASSWORD}@127.0.0.1:${REDIS_PORT_HOST}/0}"
export MINIO_ENDPOINT_URL="${MINIO_ENDPOINT_URL:-http://127.0.0.1:${MINIO_PORT_HOST}}"
export ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-http://127.0.0.1:${ELASTICSEARCH_PORT_HOST}}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:${KAFKA_PORT_HOST}}"

MINIO_URL="$(trim_trailing_slash "${MINIO_ENDPOINT_URL}")"
LAKEFS_URL="$(trim_trailing_slash "${LAKEFS_URL:-http://127.0.0.1:${LAKEFS_PORT_HOST}}")"
INGEST_RECONCILER_URL="$(trim_trailing_slash "${INGEST_RECONCILER_BASE_URL:-http://127.0.0.1:${INGEST_RECONCILER_PORT_HOST}}")"

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
  --no-clean-search  Skip Elasticsearch test index cleanup (keeps e2e_/test_/smoke_ indices)
  --skip-core        Skip full-stack core flow tests
  -h, --help         Show help

Notes:
- Tests live under `backend/tests/` (run from `backend/`).
- Full suite expects local ports from docker compose:
  OMS 8000, BFF 8002, Funnel 8003, TerminusDB 6363.
  Infra ports default to: Postgres 5433, MinIO 9000, Elasticsearch 9200, Kafka 39092.
  If you use repo root `.env` port overrides (recommended), this script will auto-detect them.
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
    --no-clean-search)
      CLEAN_SEARCH="false"
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
  if [[ -z "$PYTHON_BIN_ENV" ]]; then
    for candidate in python3.13 python3.12 python3.11 python3.10 python3; do
      if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -m pytest --version >/dev/null 2>&1; then
        PYTHON_BIN="$candidate"
        echo "ℹ️  Selected PYTHON_BIN=$PYTHON_BIN (pytest detected)"
        break
      fi
    done
  fi
fi

if ! "$PYTHON_BIN" -m pytest --version >/dev/null 2>&1; then
  echo "❌ pytest is not available in this Python environment (PYTHON_BIN=$PYTHON_BIN)." >&2
  echo "   Install test deps (example):" >&2
  echo "   cd backend && pip install -e ./shared[test] && pip install -r ./tests/requirements.txt" >&2
  exit 1
fi

python_missing_test_deps() {
  local py="$1"
  local missing
  if ! missing="$(
    "$py" - <<'PY'
import importlib
import sys

required = [
    "asyncpg",
    "redis",
    "aiohttp",
    "confluent_kafka",
]

missing = []
for name in required:
    try:
        importlib.import_module(name)
    except Exception:
        missing.append(name)

if missing:
    print(" ".join(missing))
    sys.exit(1)
PY
  )"; then
    echo "$missing"
    return 1
  fi
  return 0
}

# Ensure we run the suite in a Python environment that includes the required deps.
# If the user didn't explicitly pin PYTHON_BIN, try to auto-select a usable Python.
if ! missing_deps="$(python_missing_test_deps "$PYTHON_BIN")"; then
  if [[ -z "$PYTHON_BIN_ENV" ]]; then
    for candidate in python3.13 python3.12 python3.11 python3.10 python3; do
      if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -m pytest --version >/dev/null 2>&1; then
        if python_missing_test_deps "$candidate" >/dev/null 2>&1; then
          PYTHON_BIN="$candidate"
          echo "ℹ️  Selected PYTHON_BIN=$PYTHON_BIN (pytest + required deps detected)"
          break
        fi
      fi
    done
  fi
fi

if ! missing_deps="$(python_missing_test_deps "$PYTHON_BIN")"; then
  echo "❌ Missing required test dependencies in PYTHON_BIN=$PYTHON_BIN: $missing_deps" >&2
  echo "   Install test deps into that environment (example):" >&2
  echo "   cd backend && $PYTHON_BIN -m pip install -e ./shared[test] && $PYTHON_BIN -m pip install -r ./tests/requirements.txt" >&2
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

wait_for_elasticsearch_ready() {
  local base_url="$1"
  local timeout_s="$2"

  local deadline
  deadline="$(( $(date +%s) + timeout_s ))"

  while [[ "$(date +%s)" -lt "$deadline" ]]; do
    local status
    if status="$(
      curl -fsS --max-time 2 "${base_url}/_cluster/health" 2>/dev/null | "$PYTHON_BIN" -c '
import json
import sys

try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)

status = str(d.get("status") or "").strip().lower()

# Single-node clusters are commonly YELLOW due to replicas; accept anything except RED.
# NOTE: Fresh clusters can legitimately have 0 primary shards (no indices yet).
if status and status != "red":
    print(status)
    sys.exit(0)
sys.exit(1)
'
    )"; then
      echo "✅ Elasticsearch healthy: ${base_url}/_cluster/health (status=${status})"
      return 0
    fi
    sleep 1
  done

  echo "❌ Timed out waiting for Elasticsearch readiness: ${base_url}/_cluster/health (timeout=${timeout_s}s)" >&2
  return 1
}

wait_for_port() {
  local name="$1"
  local host="$2"
  local port="$3"
  local timeout_s="$4"

  local deadline
  deadline="$(( $(date +%s) + timeout_s ))"

  while [[ "$(date +%s)" -lt "$deadline" ]]; do
    if "$PYTHON_BIN" - <<PY >/dev/null 2>&1
import socket, sys
host = "$host"
port = int("$port")
try:
    with socket.create_connection((host, port), timeout=1):
        pass
except Exception:
    sys.exit(1)
sys.exit(0)
PY
    then
      echo "✅ $name reachable: $host:$port"
      return 0
    fi
    sleep 2
  done

  echo "❌ Timed out waiting for $name: $host:$port (timeout=${timeout_s}s)" >&2
  return 1
}

check_redis_requirepass() {
  local host="localhost"
  local port="${REDIS_PORT_HOST}"
  local password="${REDIS_PASSWORD}"

  REDIS_CHECK_HOST="$host" REDIS_CHECK_PORT="$port" REDIS_CHECK_PASSWORD="$password" "$PYTHON_BIN" - <<'PY'
import os
import socket
import sys

host = os.environ["REDIS_CHECK_HOST"]
port = int(os.environ["REDIS_CHECK_PORT"])
password = os.environ["REDIS_CHECK_PASSWORD"]


def recv_line(sock: socket.socket) -> bytes:
    sock.settimeout(2.0)
    return sock.recv(4096)


def send(sock: socket.socket, payload: bytes) -> bytes:
    sock.sendall(payload)
    return recv_line(sock)


try:
    with socket.create_connection((host, port), timeout=2.0) as sock:
        # 1) Unauthed PING must NOT succeed.
        resp = send(sock, b"*1\r\n$4\r\nPING\r\n")
        if resp.startswith(b"+PONG"):
            print(
                f"Redis at {host}:{port} accepted unauthenticated PING (requirepass is NOT enforced).",
                file=sys.stderr,
            )
            sys.exit(2)
        if b"NOAUTH" not in resp and b"WRONGPASS" not in resp and not resp.startswith(b"-ERR"):
            print(f"Unexpected Redis response to unauthenticated PING: {resp!r}", file=sys.stderr)
            sys.exit(2)

        # 2) AUTH must succeed.
        auth = f"*2\r\n$4\r\nAUTH\r\n${len(password)}\r\n{password}\r\n".encode("utf-8")
        auth_resp = send(sock, auth)
        if not auth_resp.startswith(b"+OK"):
            print(f"Redis AUTH failed at {host}:{port}: {auth_resp!r}", file=sys.stderr)
            sys.exit(2)

        # 3) Authed PING must succeed.
        pong = send(sock, b"*1\r\n$4\r\nPING\r\n")
        if not pong.startswith(b"+PONG"):
            print(f"Redis authed PING failed at {host}:{port}: {pong!r}", file=sys.stderr)
            sys.exit(2)
except OSError as exc:
    print(f"Redis not reachable at {host}:{port}: {exc}", file=sys.stderr)
    sys.exit(2)
PY
}

cleanup_elasticsearch_test_indices() {
  local base_url="$1"

  if [[ -z "$base_url" ]]; then
    echo "⚠️  Skipping Elasticsearch cleanup (ELASTICSEARCH_URL is empty)"
    return 0
  fi

  echo "🧹 Cleaning Elasticsearch test indices (e2e_*, test_*, smoke_*)..."

  # Best-effort cleanup: do not fail the gate if indices don't exist.
  # NOTE: ES defaults `action.destructive_requires_name=true` (wildcard DELETE is forbidden),
  # so we enumerate and delete explicit index names.
  local indices
  if ! indices="$(curl -fsS --max-time 10 "${base_url}/_cat/indices?h=index" 2>/dev/null)"; then
    echo "⚠️  Elasticsearch cleanup skipped (failed to list indices)" >&2
    return 0
  fi

  local deleted=0
  while IFS= read -r index_name; do
    [[ -z "$index_name" ]] && continue
    if [[ "$index_name" == e2e_* || "$index_name" == test_* || "$index_name" == smoke_* ]]; then
      if curl -fsS --max-time 10 -X DELETE "${base_url}/${index_name}" >/dev/null 2>&1; then
        deleted=$((deleted + 1))
      else
        echo "⚠️  Failed to delete ES index: ${index_name} (continuing)" >&2
      fi
    fi
  done <<<"$indices"

  echo "🧹 Elasticsearch cleanup deleted ${deleted} indices"
}

ensure_kafka_topic_partitions() {
  local topic="$1"
  local min_partitions="$2"

  KAFKA_ENSURE_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-}"
  if [[ -z "$KAFKA_ENSURE_BOOTSTRAP" ]]; then
    echo "⚠️  Skipping Kafka topic check (KAFKA_BOOTSTRAP_SERVERS is empty)"
    return 0
  fi

  echo "🔧 Ensuring Kafka topic partitions: ${topic} (min=${min_partitions})"
  KAFKA_ENSURE_TOPIC="$topic" KAFKA_ENSURE_MIN_PARTITIONS="$min_partitions" KAFKA_ENSURE_BOOTSTRAP="$KAFKA_ENSURE_BOOTSTRAP" "$PYTHON_BIN" - <<'PY'
import os
import sys
import time

from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic

bootstrap = os.environ["KAFKA_ENSURE_BOOTSTRAP"]
topic = os.environ["KAFKA_ENSURE_TOPIC"]
min_partitions = int(os.environ["KAFKA_ENSURE_MIN_PARTITIONS"])

admin = AdminClient({"bootstrap.servers": bootstrap})


def _wait(futures, *, timeout_s: float = 20.0) -> None:
    deadline = time.monotonic() + timeout_s
    for name, fut in futures.items():
        remaining = max(0.1, deadline - time.monotonic())
        fut.result(timeout=remaining)


md = admin.list_topics(timeout=10.0)
meta = md.topics.get(topic)

if meta is None or meta.error is not None:
    fs = admin.create_topics([NewTopic(topic, num_partitions=min_partitions, replication_factor=1)])
    _wait(fs)
    print(f"✅ Created topic {topic} partitions={min_partitions}")
    sys.exit(0)

current = len(meta.partitions or {})
if current >= min_partitions:
    print(f"✅ Topic {topic} partitions={current} (ok)")
    sys.exit(0)

fs = admin.create_partitions([NewPartitions(topic, min_partitions)])
_wait(fs)
print(f"✅ Increased topic partitions {topic}: {current} -> {min_partitions}")
PY
}

if [[ "$WAIT_FOR_SERVICES" == "true" && "$MODE" == "full" ]]; then
  echo "⏳ Waiting for services (timeout=${TIMEOUT_SECONDS}s)..."
  wait_for_url "MinIO" "${MINIO_URL}/minio/health/live" "$TIMEOUT_SECONDS"
  wait_for_url "lakeFS" "${LAKEFS_URL}/healthcheck" "$TIMEOUT_SECONDS"
  wait_for_elasticsearch_ready "${ELASTICSEARCH_URL}" "$TIMEOUT_SECONDS"
  wait_for_url "OMS" "${OMS_URL}/health" "$TIMEOUT_SECONDS"
  wait_for_url "BFF" "${BFF_URL}/api/v1/health" "$TIMEOUT_SECONDS"
  wait_for_url "Funnel" "${FUNNEL_URL}/health" "$TIMEOUT_SECONDS"
  wait_for_url "Agent" "${AGENT_URL}/health" "$TIMEOUT_SECONDS"
  wait_for_url "Ingest Reconciler" "${INGEST_RECONCILER_URL}/health" "$TIMEOUT_SECONDS"

  kafka_bootstrap="${KAFKA_BOOTSTRAP_SERVERS%%,*}"
  kafka_bootstrap="${kafka_bootstrap#*://}"
  kafka_host="${kafka_bootstrap%%:*}"
  kafka_port="${kafka_bootstrap##*:}"
  if [[ "$kafka_host" == "$kafka_bootstrap" || -z "$kafka_host" ]]; then
    kafka_host="localhost"
  fi
  if [[ "$kafka_port" == "$kafka_bootstrap" || -z "$kafka_port" ]]; then
    kafka_port="$KAFKA_PORT_HOST"
  fi
  wait_for_port "Kafka" "$kafka_host" "$kafka_port" "$TIMEOUT_SECONDS"

  echo "🔒 Verifying Redis auth (requirepass)..."
  check_redis_requirepass

  if [[ "$CLEAN_SEARCH" == "true" ]]; then
    cleanup_elasticsearch_test_indices "${ELASTICSEARCH_URL}"
    wait_for_elasticsearch_ready "${ELASTICSEARCH_URL}" "$TIMEOUT_SECONDS"
  fi

  # Enterprise gate hardening: reduce head-of-line blocking across unrelated DB runs
  # by ensuring sufficient Kafka partitioning for key-based ordering topics.
  ensure_kafka_topic_partitions "${OBJECTIFY_JOBS_TOPIC:-objectify-jobs}" 12
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

  # 4b) Additional E2E + contract smoke tests (no mocks)
  "$PYTHON_BIN" -m pytest tests/test_openapi_contract_smoke.py -q
  "$PYTHON_BIN" -m pytest tests/test_auth_hardening_e2e.py -q
  "$PYTHON_BIN" -m pytest tests/test_websocket_auth_e2e.py -q
  RUN_LIVE_BRANCH_VIRTUALIZATION=true "$PYTHON_BIN" -m pytest tests/test_branch_virtualization_e2e.py -q
  "$PYTHON_BIN" -m pytest tests/test_command_status_ttl_e2e.py -q
  "$PYTHON_BIN" -m pytest tests/test_worker_lease_safety_e2e.py -q
  "$PYTHON_BIN" -m pytest tests/test_event_store_tls_guard.py -q
  "$PYTHON_BIN" -m pytest tests/test_critical_fixes_e2e.py -q
  "$PYTHON_BIN" -m pytest tests/test_consistency_e2e_smoke.py -q
  RUN_PIPELINE_OBJECTIFY_E2E=true "$PYTHON_BIN" -m pytest tests/test_pipeline_objectify_es_e2e.py -q
  RUN_PIPELINE_TRANSFORM_E2E=true "$PYTHON_BIN" -m pytest tests/test_pipeline_transform_cleansing_e2e.py -q
  "$PYTHON_BIN" -m pytest tests/test_financial_investigation_workflow_e2e.py -q

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
