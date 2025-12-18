#!/usr/bin/env bash
set -euo pipefail

# Backward-compat wrapper.
# The canonical runner lives at `backend/run_production_tests.sh`.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/../run_production_tests.sh" "$@"
