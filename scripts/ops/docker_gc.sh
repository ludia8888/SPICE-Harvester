#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

MODE="safe" # safe|aggressive
WITH_VOLUMES="false"
BUILDER_UNTIL="24h"

usage() {
  cat <<'USAGE'
Usage: ./scripts/ops/docker_gc.sh [--aggressive] [--with-volumes] [--builder-until <duration>]

Purpose:
  Keep local Docker disk usage bounded after repeated compose up/down + builds.

Defaults (safe):
  - container/network prune
  - image prune (dangling only)
  - builder prune (unused, older than 24h)
  - volumes are NOT pruned unless explicitly requested

Flags:
  --aggressive          Also prune all unused images + all unused builder cache
  --with-volumes        Prune unused volumes (destructive; requires CONFIRM=YES)
  --builder-until DUR   Builder cache age filter (default: 24h). Use "0" to disable the filter.
USAGE
}

truthy() {
  local v
  v="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]' | xargs)"
  [[ "${v}" == "1" || "${v}" == "true" || "${v}" == "yes" || "${v}" == "y" || "${v}" == "on" ]]
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --aggressive)
      MODE="aggressive"
      shift
      ;;
    --with-volumes)
      WITH_VOLUMES="true"
      shift
      ;;
    --builder-until)
      BUILDER_UNTIL="${2:-}"
      shift 2
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

require_cmd docker

log "docker system df (before)"
docker system df || true

log "pruning stopped containers"
docker container prune -f >/dev/null 2>&1 || true

log "pruning unused networks"
docker network prune -f >/dev/null 2>&1 || true

if [[ "${MODE}" == "aggressive" ]]; then
  log "pruning unused images (all)"
  docker image prune -af >/dev/null 2>&1 || true
else
  log "pruning dangling images"
  docker image prune -f >/dev/null 2>&1 || true
fi

BUILDER_ARGS=(-f -a)
if [[ "${MODE}" != "aggressive" ]] && [[ -n "${BUILDER_UNTIL}" ]] && [[ "${BUILDER_UNTIL}" != "0" ]]; then
  BUILDER_ARGS+=(--filter "until=${BUILDER_UNTIL}")
fi

log "pruning builder cache (unused) ${MODE}"
docker builder prune "${BUILDER_ARGS[@]}" >/dev/null 2>&1 || true

if truthy "${WITH_VOLUMES}"; then
  confirm="$(printf '%s' "${CONFIRM:-}" | tr '[:lower:]' '[:upper:]' | xargs)"
  if [[ "${confirm}" != "YES" ]]; then
    die "docker volume prune is destructive; re-run with CONFIRM=YES"
  fi
  log "pruning unused volumes (--with-volumes)"
  docker volume prune -f >/dev/null 2>&1 || true
fi

log "docker system df (after)"
docker system df || true
