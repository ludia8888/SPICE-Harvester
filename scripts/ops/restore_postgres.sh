#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/_common.sh"

usage() {
  cat <<'EOF' >&2
Usage:
  CONFIRM=YES ./scripts/ops/restore_postgres.sh <dump_file>

Env:
  POSTGRES_URL           Restore via host pg_restore (expects pg_restore installed)
  POSTGRES_CONTAINER     Restore via docker exec (default: auto-detect)
  POSTGRES_USER          (docker mode) default: spiceadmin
  POSTGRES_DB            (docker mode) default: spicedb
  CLEAN=true|false       When true, pass --clean --if-exists (destructive)
EOF
}

dump_file="${1:-${DUMP_FILE:-}}"
if [[ -z "${dump_file}" ]]; then
  usage
  exit 2
fi
if [[ ! -f "${dump_file}" ]]; then
  die "Dump file not found: ${dump_file}"
fi
if [[ "${CONFIRM:-}" != "YES" ]]; then
  die "Refusing to restore without explicit confirmation. Re-run with CONFIRM=YES."
fi

POSTGRES_URL="${POSTGRES_URL:-}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-}"
POSTGRES_USER="${POSTGRES_USER:-spiceadmin}"
POSTGRES_DB="${POSTGRES_DB:-spicedb}"
CLEAN="${CLEAN:-false}"

restore_args=(--no-owner --no-acl)
if [[ "${CLEAN}" == "true" ]]; then
  restore_args+=(--clean --if-exists)
fi

if [[ -n "${POSTGRES_URL}" ]]; then
  require_cmd pg_restore
  log "Postgres restore via POSTGRES_URL from ${dump_file}"
  pg_restore "${restore_args[@]}" --dbname="${POSTGRES_URL}" "${dump_file}"
else
  require_cmd docker
  if [[ -z "${POSTGRES_CONTAINER}" ]]; then
    POSTGRES_CONTAINER="$(first_running_container spice_postgres spice-harvester-postgres)" \
      || die "Postgres container not running. Set POSTGRES_URL or POSTGRES_CONTAINER."
  fi
  log "Postgres restore into container ${POSTGRES_CONTAINER} (${POSTGRES_DB}) from ${dump_file}"
  cat "${dump_file}" | docker exec -i "${POSTGRES_CONTAINER}" pg_restore -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" "${restore_args[@]}"
fi

log "Restore complete."

