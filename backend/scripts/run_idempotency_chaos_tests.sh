#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

export POSTGRES_URL="${POSTGRES_URL:-postgresql://spiceadmin:spicepass123@localhost:5433/spicedb}"

python -m pytest \
  -c "${ROOT_DIR}/backend/tests/pytest.ini" \
  -m chaos \
  "${ROOT_DIR}/backend/tests"

