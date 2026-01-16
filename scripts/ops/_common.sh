#!/usr/bin/env bash
set -euo pipefail

log() {
  local msg="$*"
  printf '[%s] %s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "${msg}"
}

die() {
  local msg="$*"
  printf 'ERROR: %s\n' "${msg}" >&2
  exit 1
}

require_cmd() {
  local cmd="$1"
  command -v "${cmd}" >/dev/null 2>&1 || die "Missing dependency: ${cmd}"
}

first_running_container() {
  require_cmd docker
  local name
  for name in "$@"; do
    if docker ps --format '{{.Names}}' | grep -Fxq "${name}"; then
      echo "${name}"
      return 0
    fi
  done
  return 1
}

first_container() {
  require_cmd docker
  local name
  for name in "$@"; do
    if docker ps -a --format '{{.Names}}' | grep -Fxq "${name}"; then
      echo "${name}"
      return 0
    fi
  done
  return 1
}

container_first_network() {
  require_cmd docker
  local container="$1"
  docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{println $k}}{{end}}' "${container}" | head -n 1
}

container_named_volume_for_mount() {
  require_cmd docker
  local container="$1"
  local destination="$2"
  docker inspect -f '{{range .Mounts}}{{if and (eq .Type "volume") (eq .Destination "'"${destination}"'")}}{{.Name}}{{end}}{{end}}' "${container}"
}

prune_mtime_days() {
  local root="$1"
  local days="$2"
  local pattern="${3:-*}"
  if [[ -z "${root}" ]] || [[ ! -d "${root}" ]]; then
    return 0
  fi
  if [[ -z "${days}" ]] || ! [[ "${days}" =~ ^[0-9]+$ ]]; then
    return 0
  fi
  if [[ "${days}" -le 0 ]]; then
    return 0
  fi
  find "${root}" -mindepth 1 -maxdepth 1 -name "${pattern}" -mtime +"${days}" -print0 | xargs -0 rm -rf || true
}

