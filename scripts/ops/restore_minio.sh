#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/_common.sh"

usage() {
  cat <<'EOF' >&2
Usage:
  CONFIRM=YES ./scripts/ops/restore_minio.sh <backup_dir>

Env:
  MINIO_CONTAINER          Default: auto-detect (spice_minio/spice-harvester-minio)
  MINIO_ACCESS_KEY         Default: minioadmin
  MINIO_SECRET_KEY         Default: minioadmin123
  REMOVE_EXTRANEOUS=true   When true, pass --remove to mc mirror (destructive)
EOF
}

backup_dir="${1:-${BACKUP_DIR:-}}"
if [[ -z "${backup_dir}" ]]; then
  usage
  exit 2
fi
if [[ ! -d "${backup_dir}" ]]; then
  die "Backup dir not found: ${backup_dir}"
fi
if [[ "${CONFIRM:-}" != "YES" ]]; then
  die "Refusing to restore without explicit confirmation. Re-run with CONFIRM=YES."
fi

MINIO_CONTAINER="${MINIO_CONTAINER:-}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin123}"
REMOVE_EXTRANEOUS="${REMOVE_EXTRANEOUS:-false}"

require_cmd docker

if [[ -z "${MINIO_CONTAINER}" ]]; then
  MINIO_CONTAINER="$(first_running_container spice_minio spice-harvester-minio)" \
    || die "MinIO container not running. Set MINIO_CONTAINER."
fi

network="$(container_first_network "${MINIO_CONTAINER}")"
if [[ -z "${network}" ]]; then
  die "Failed to resolve docker network for ${MINIO_CONTAINER}"
fi

mirror_args=(--overwrite)
if [[ "${REMOVE_EXTRANEOUS}" == "true" ]]; then
  mirror_args+=(--remove)
fi

log "MinIO restore into ${MINIO_CONTAINER} (network=${network}) from ${backup_dir}"

docker run --rm \
  --network "${network}" \
  -v "${backup_dir}:/backup:ro" \
  minio/mc:latest \
  /bin/sh -c "
    set -euo pipefail
    mc alias set dst \"http://${MINIO_CONTAINER}:9000\" \"${MINIO_ACCESS_KEY}\" \"${MINIO_SECRET_KEY}\" >/dev/null
    for d in /backup/*; do
      [ -d \"\${d}\" ] || continue
      b=\"\$(basename \"\${d}\")\"
      echo \"restore: \${b}\"
      mc mb -p \"dst/\${b}\" >/dev/null 2>&1 || true
      mc mirror ${mirror_args[*]} \"\${d}\" \"dst/\${b}\" >/dev/null
    done
  "

log "Restore complete."

