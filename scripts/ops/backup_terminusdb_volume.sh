#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/_common.sh"

OUT_DIR="${OUT_DIR:-./backups/terminusdb}"
RETENTION_DAYS="${RETENTION_DAYS:-14}"

TERMINUSDB_CONTAINER="${TERMINUSDB_CONTAINER:-}"
TERMINUSDB_VOLUME="${TERMINUSDB_VOLUME:-}"

require_cmd docker
mkdir -p "${OUT_DIR}"

if [[ -z "${TERMINUSDB_CONTAINER}" ]]; then
  TERMINUSDB_CONTAINER="$(first_running_container spice_terminusdb)" \
    || die "TerminusDB container not running. Set TERMINUSDB_CONTAINER or start the stack."
fi

if [[ -z "${TERMINUSDB_VOLUME}" ]]; then
  TERMINUSDB_VOLUME="$(container_named_volume_for_mount "${TERMINUSDB_CONTAINER}" "/app/terminusdb/storage")"
fi
if [[ -z "${TERMINUSDB_VOLUME}" ]]; then
  die "Failed to resolve TerminusDB volume. Set TERMINUSDB_VOLUME."
fi

timestamp="$(date +"%Y%m%d_%H%M%S")"
archive="${OUT_DIR}/terminusdb_${timestamp}.tar.gz"

log "TerminusDB volume backup (${TERMINUSDB_VOLUME}) -> ${archive}"
docker run --rm \
  -v "${TERMINUSDB_VOLUME}:/data:ro" \
  -v "${OUT_DIR}:/backup" \
  alpine:3.19 \
  sh -c "tar -czf \"/backup/$(basename "${archive}")\" -C /data ."

if [[ ! -s "${archive}" ]]; then
  die "Backup failed: output file is empty (${archive})"
fi

log "Backup complete: ${archive}"
prune_mtime_days "${OUT_DIR}" "${RETENTION_DAYS}" "*.tar.gz"

