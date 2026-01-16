#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/_common.sh"

OUT_DIR="${OUT_DIR:-./backups/postgres}"
RETENTION_DAYS="${RETENTION_DAYS:-14}"

POSTGRES_URL="${POSTGRES_URL:-}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-}"
POSTGRES_USER="${POSTGRES_USER:-spiceadmin}"
POSTGRES_DB="${POSTGRES_DB:-spicedb}"

mkdir -p "${OUT_DIR}"
timestamp="$(date +"%Y%m%d_%H%M%S")"
out_file="${OUT_DIR}/postgres_${POSTGRES_DB}_${timestamp}.dump"

if [[ -n "${POSTGRES_URL}" ]]; then
  require_cmd pg_dump
  log "Postgres backup via POSTGRES_URL -> ${out_file}"
  pg_dump --format=custom --no-owner --no-acl "${POSTGRES_URL}" >"${out_file}"
else
  require_cmd docker
  if [[ -z "${POSTGRES_CONTAINER}" ]]; then
    POSTGRES_CONTAINER="$(first_running_container spice_postgres spice-harvester-postgres)" \
      || die "Postgres container not running. Set POSTGRES_URL or POSTGRES_CONTAINER."
  fi
  log "Postgres backup from container ${POSTGRES_CONTAINER} (${POSTGRES_DB}) -> ${out_file}"
  docker exec "${POSTGRES_CONTAINER}" pg_dump -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -Fc --no-owner --no-acl >"${out_file}"
fi

if [[ ! -s "${out_file}" ]]; then
  die "Backup failed: output file is empty (${out_file})"
fi

log "Backup complete: ${out_file}"
prune_mtime_days "${OUT_DIR}" "${RETENTION_DAYS}" "*.dump"

