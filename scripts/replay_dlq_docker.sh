#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CONTAINER="${DLQ_REPLAY_CONTAINER:-spice_connector_sync_worker}"
REMOTE_PATH="${DLQ_REPLAY_REMOTE_PATH:-/tmp/replay_dlq.py}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"

if ! docker ps --format '{{.Names}}' | grep -qx "${CONTAINER}"; then
  echo "Container not running: ${CONTAINER}" >&2
  echo "Set DLQ_REPLAY_CONTAINER to an active Python worker container." >&2
  exit 1
fi

if [[ ! -f "${SCRIPT_DIR}/replay_dlq.py" ]]; then
  echo "Missing ${SCRIPT_DIR}/replay_dlq.py" >&2
  exit 1
fi

cleanup() {
  docker exec "${CONTAINER}" rm -f "${REMOTE_PATH}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker cp "${SCRIPT_DIR}/replay_dlq.py" "${CONTAINER}:${REMOTE_PATH}"

docker exec \
  -e KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS}" \
  -e KAFKA_HOST="${KAFKA_HOST:-kafka}" \
  -e KAFKA_PORT_HOST="${KAFKA_PORT_HOST:-29092}" \
  "${CONTAINER}" \
  python "${REMOTE_PATH}" "$@"
