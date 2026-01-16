#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

PYTHON_BIN="${PYTHON_BIN:-python3.11}"
NODE_BIN="${NODE_BIN:-node}"
NPM_BIN="${NPM_BIN:-npm}"
COMPOSE="${COMPOSE:-docker compose}"

ensure_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "❌ Missing command: $cmd" >&2
    exit 1
  fi
}

ensure_cmd "$PYTHON_BIN"
ensure_cmd "$NODE_BIN"
ensure_cmd "$NPM_BIN"
ensure_cmd docker

echo "🔧 Using PYTHON_BIN=$PYTHON_BIN"
echo "🔧 Using NODE_BIN=$NODE_BIN"
echo "🔧 Using NPM_BIN=$NPM_BIN"

echo "📦 Installing backend test dependencies (shared[test])..."
"$PYTHON_BIN" -m pip install --upgrade pip
"$PYTHON_BIN" -m pip install -e 'backend/shared[test]'

echo "🧪 Backend unit tests..."
PYTHON="$PYTHON_BIN" make backend-unit

echo "📊 Backend coverage (>=90%) + diff-cover (100%)..."
PYTHON="$PYTHON_BIN" make backend-coverage

BASE_REF="main"
if [[ "${GITHUB_EVENT_NAME:-}" == "pull_request" && -n "${GITHUB_BASE_REF:-}" ]]; then
  BASE_REF="${GITHUB_BASE_REF}"
fi
git fetch origin "${BASE_REF}" --depth=1 || true
diff-cover "coverage.xml" --compare-branch "origin/${BASE_REF}" --fail-under=100

echo "🧾 Backend methods check..."
"$PYTHON_BIN" scripts/generate_backend_methods.py --check

echo "🧾 API reference check..."
"$PYTHON_BIN" scripts/generate_api_reference.py --check

echo "🧾 Architecture reference check..."
"$PYTHON_BIN" scripts/generate_architecture_reference.py --check

echo "🎨 Frontend lint + build..."
cd frontend
"$NPM_BIN" ci
"$NPM_BIN" run lint
"$NPM_BIN" run build

echo "🧪 Frontend tests + coverage (>=90%)..."
"$NPM_BIN" run test
cd "$REPO_ROOT"

echo "🐳 Backend stack tests..."
"$COMPOSE" -f docker-compose.full.yml up -d --build
cleanup_stack() {
  COMPOSE_BIN="$COMPOSE" GC_MODE="${GC_MODE:-safe}" ./scripts/ops/compose_down_clean.sh -f docker-compose.full.yml
}
trap cleanup_stack EXIT

PYTHON_BIN="$PYTHON_BIN" ./backend/run_production_tests.sh --full

echo "✅ Local CI gates completed successfully."
