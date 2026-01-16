#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/_common.sh"

COMPONENTS="${COMPONENTS:-postgres,minio,terminusdb}"

log "Starting stack backup (components=${COMPONENTS})"

IFS=',' read -r -a parts <<<"${COMPONENTS}"
for part in "${parts[@]}"; do
  part="$(echo "${part}" | xargs)"
  [[ -z "${part}" ]] && continue
  case "${part}" in
    postgres)
      "${SCRIPT_DIR}/backup_postgres.sh"
      ;;
    minio)
      "${SCRIPT_DIR}/backup_minio.sh"
      ;;
    terminusdb)
      "${SCRIPT_DIR}/backup_terminusdb_volume.sh"
      ;;
    *)
      die "Unknown component: ${part} (expected: postgres|minio|terminusdb)"
      ;;
  esac
done

log "Stack backup finished."

