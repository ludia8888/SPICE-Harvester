#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/_common.sh"

BACKUP_ROOT="${BACKUP_ROOT:-./backups/minio}"
RETENTION_DAYS="${RETENTION_DAYS:-14}"

MINIO_CONTAINER="${MINIO_CONTAINER:-}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin123}"

EVENT_STORE_BUCKET="${EVENT_STORE_BUCKET:-spice-event-store}"
INSTANCE_BUCKET="${INSTANCE_BUCKET:-instance-events}"
DATASET_ARTIFACTS_BUCKET="${DATASET_ARTIFACTS_BUCKET:-dataset-artifacts}"
AGENT_EVENT_STORE_BUCKET="${AGENT_EVENT_STORE_BUCKET:-spice-agent-store}"

BUCKETS="${BUCKETS:-lakefs,${EVENT_STORE_BUCKET},${INSTANCE_BUCKET},${DATASET_ARTIFACTS_BUCKET},${AGENT_EVENT_STORE_BUCKET}}"
SKIP_MISSING="${SKIP_MISSING:-true}"

require_cmd docker

if [[ -z "${MINIO_CONTAINER}" ]]; then
  MINIO_CONTAINER="$(first_running_container spice_minio spice-harvester-minio)" \
    || die "MinIO container not running. Set MINIO_CONTAINER."
fi

network="$(container_first_network "${MINIO_CONTAINER}")"
if [[ -z "${network}" ]]; then
  die "Failed to resolve docker network for ${MINIO_CONTAINER}"
fi

timestamp="$(date +"%Y%m%d_%H%M%S")"
run_dir="${BACKUP_ROOT}/${timestamp}"
mkdir -p "${run_dir}"

log "MinIO backup from ${MINIO_CONTAINER} (network=${network}) -> ${run_dir}"
log "Buckets: ${BUCKETS}"

docker run --rm \
  --network "${network}" \
  -v "${run_dir}:/backup" \
  minio/mc:latest \
  /bin/sh -c "
    set -euo pipefail
    mc alias set src \"http://${MINIO_CONTAINER}:9000\" \"${MINIO_ACCESS_KEY}\" \"${MINIO_SECRET_KEY}\" >/dev/null
    IFS=',' read -r -a buckets <<'BUCKETS_EOF'
${BUCKETS}
BUCKETS_EOF
    for b in \"\${buckets[@]}\"; do
      b=\"\$(echo \"\${b}\" | xargs)\"
      [ -z \"\${b}\" ] && continue
      if mc ls \"src/\${b}\" >/dev/null 2>&1; then
        echo \"mirror: \${b}\"
        mc mirror --overwrite \"src/\${b}\" \"/backup/\${b}\" >/dev/null
      else
        if [ \"${SKIP_MISSING}\" = \"true\" ]; then
          echo \"skip missing bucket: \${b}\" >&2
          continue
        fi
        echo \"missing bucket: \${b}\" >&2
        exit 1
      fi
    done
  "

log "Backup complete: ${run_dir}"
prune_mtime_days "${BACKUP_ROOT}" "${RETENTION_DAYS}"

