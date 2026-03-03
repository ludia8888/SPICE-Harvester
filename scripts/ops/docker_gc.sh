#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

MODE="safe" # safe|aggressive
WITH_VOLUMES="false"
BUILDER_UNTIL="24h"
DOCKER_DF_TIMEOUT="${DOCKER_DF_TIMEOUT:-10}"

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

Env:
  DOCKER_DF_TIMEOUT     Timeout seconds for `docker system df` snapshots (default: 10)
USAGE
}

truthy() {
  local v
  v="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]' | xargs)"
  [[ "${v}" == "1" || "${v}" == "true" || "${v}" == "yes" || "${v}" == "y" || "${v}" == "on" ]]
}

run_with_timeout() {
  local timeout_s="$1"
  shift

  "$@" &
  local cmd_pid=$!
  local elapsed=0

  while kill -0 "${cmd_pid}" >/dev/null 2>&1; do
    if (( elapsed >= timeout_s )); then
      kill -TERM "${cmd_pid}" >/dev/null 2>&1 || true
      sleep 1
      kill -KILL "${cmd_pid}" >/dev/null 2>&1 || true
      wait "${cmd_pid}" >/dev/null 2>&1 || true
      return 124
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done

  wait "${cmd_pid}"
}

log_docker_system_df() {
  local phase="$1"
  log "docker system df (${phase})"
  if ! run_with_timeout "${DOCKER_DF_TIMEOUT}" docker system df; then
    log "docker system df (${phase}) timed out after ${DOCKER_DF_TIMEOUT}s; skipping"
  fi
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

log_docker_system_df "before"

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

log_docker_system_df "after"
