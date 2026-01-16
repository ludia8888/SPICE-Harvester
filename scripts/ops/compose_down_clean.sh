#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

COMPOSE_BIN="${COMPOSE_BIN:-docker compose}"
GC_MODE="${GC_MODE:-safe}" # safe|aggressive
WITH_VOLUMES="${WITH_VOLUMES:-false}"
PRUNE_VOLUMES="${PRUNE_VOLUMES:-false}"
BUILDER_UNTIL="${BUILDER_UNTIL:-24h}"

COMPOSE_FILES=()

usage() {
  cat <<'USAGE'
Usage: ./scripts/ops/compose_down_clean.sh [-f <compose.yml> ...] [--with-volumes] [--aggressive] [--prune-volumes]

Stops a compose stack and immediately runs docker GC to prevent disk/cache growth.

Defaults:
  - compose files: docker-compose.full.yml (if present), else backend/docker-compose.yml
  - down flags: --remove-orphans --rmi local
  - GC mode: safe (builder cache older than 24h)

Notes:
  - --with-volumes passes --volumes to `docker compose down` (destroys stack data).
  - Global unused volume pruning requires --prune-volumes (and CONFIRM=YES).
USAGE
}

truthy() {
  local v
  v="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]' | xargs)"
  [[ "${v}" == "1" || "${v}" == "true" || "${v}" == "yes" || "${v}" == "y" || "${v}" == "on" ]]
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -f|--file)
      COMPOSE_FILES+=("${2:-}")
      shift 2
      ;;
    --with-volumes)
      WITH_VOLUMES="true"
      shift
      ;;
    --aggressive)
      GC_MODE="aggressive"
      shift
      ;;
    --prune-volumes)
      PRUNE_VOLUMES="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "Unknown argument: $1"
      ;;
  esac
done

REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ ${#COMPOSE_FILES[@]} -eq 0 ]]; then
  if [[ -f "${REPO_ROOT}/docker-compose.full.yml" ]]; then
    COMPOSE_FILES+=("${REPO_ROOT}/docker-compose.full.yml")
  elif [[ -f "${REPO_ROOT}/backend/docker-compose.yml" ]]; then
    COMPOSE_FILES+=("${REPO_ROOT}/backend/docker-compose.yml")
  else
    die "No default compose file found (expected docker-compose.full.yml or backend/docker-compose.yml)"
  fi
fi

COMPOSE_CMD=()
IFS=' ' read -r -a COMPOSE_CMD <<<"${COMPOSE_BIN}"

DOWN_CMD=("${COMPOSE_CMD[@]}")
for f in "${COMPOSE_FILES[@]}"; do
  [[ -n "${f}" ]] || die "Empty compose file path"
  DOWN_CMD+=(-f "${f}")
done
DOWN_CMD+=(down --remove-orphans --rmi local)
if truthy "${WITH_VOLUMES}"; then
  DOWN_CMD+=(--volumes)
fi

log "compose down: ${DOWN_CMD[*]}"
"${DOWN_CMD[@]}" || true

log "docker gc: mode=${GC_MODE} builder_until=${BUILDER_UNTIL} with_volumes=${WITH_VOLUMES}"
GC_CMD=("${SCRIPT_DIR}/docker_gc.sh")
if [[ "${GC_MODE}" == "aggressive" ]]; then
  GC_CMD+=(--aggressive)
fi
if truthy "${PRUNE_VOLUMES}"; then
  GC_CMD+=(--with-volumes)
fi
GC_CMD+=(--builder-until "${BUILDER_UNTIL}")

"${GC_CMD[@]}" || true
