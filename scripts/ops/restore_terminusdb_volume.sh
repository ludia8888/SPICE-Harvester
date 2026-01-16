#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/_common.sh"

usage() {
  cat <<'EOF' >&2
Usage:
  CONFIRM=YES ./scripts/ops/restore_terminusdb_volume.sh <archive.tar.gz>

Env:
  TERMINUSDB_CONTAINER  Default: spice_terminusdb
  TERMINUSDB_VOLUME     Auto-detected from container mount; override if needed
  AUTO_STOP=true        If true, stop/start the TerminusDB container automatically
EOF
}

archive="${1:-${ARCHIVE:-}}"
if [[ -z "${archive}" ]]; then
  usage
  exit 2
fi
if [[ ! -f "${archive}" ]]; then
  die "Archive not found: ${archive}"
fi
if [[ "${CONFIRM:-}" != "YES" ]]; then
  die "Refusing to restore without explicit confirmation. Re-run with CONFIRM=YES."
fi

TERMINUSDB_CONTAINER="${TERMINUSDB_CONTAINER:-}"
TERMINUSDB_VOLUME="${TERMINUSDB_VOLUME:-}"
AUTO_STOP="${AUTO_STOP:-false}"

require_cmd docker

if [[ -z "${TERMINUSDB_CONTAINER}" ]]; then
  TERMINUSDB_CONTAINER="$(first_container spice_terminusdb)" \
    || die "TerminusDB container not found. Set TERMINUSDB_CONTAINER."
fi

running="false"
if docker ps --format '{{.Names}}' | grep -Fxq "${TERMINUSDB_CONTAINER}"; then
  running="true"
fi

if [[ "${running}" == "true" ]]; then
  if [[ "${AUTO_STOP}" == "true" ]]; then
    log "Stopping TerminusDB container ${TERMINUSDB_CONTAINER}"
    docker stop "${TERMINUSDB_CONTAINER}" >/dev/null
  else
    die "TerminusDB container is running. Stop it first, or re-run with AUTO_STOP=true."
  fi
fi

if [[ -z "${TERMINUSDB_VOLUME}" ]]; then
  TERMINUSDB_VOLUME="$(container_named_volume_for_mount "${TERMINUSDB_CONTAINER}" "/app/terminusdb/storage")"
fi
if [[ -z "${TERMINUSDB_VOLUME}" ]]; then
  die "Failed to resolve TerminusDB volume. Set TERMINUSDB_VOLUME."
fi

log "Restoring TerminusDB volume (${TERMINUSDB_VOLUME}) from ${archive}"
docker run --rm \
  -v "${TERMINUSDB_VOLUME}:/data" \
  -v "$(cd "$(dirname "${archive}")" && pwd):/backup:ro" \
  alpine:3.19 \
  sh -c "rm -rf /data/* && tar -xzf \"/backup/$(basename "${archive}")\" -C /data"

if [[ "${AUTO_STOP}" == "true" ]]; then
  log "Starting TerminusDB container ${TERMINUSDB_CONTAINER}"
  docker start "${TERMINUSDB_CONTAINER}" >/dev/null
fi

log "Restore complete."

