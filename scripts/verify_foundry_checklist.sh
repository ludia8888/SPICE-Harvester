#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
PYTHON_BIN="${PYTHON_BIN:-python3.11}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  PYTHON_BIN="python3"
fi
EVIDENCE_DIR="$ROOT_DIR/docs/foundry_checklist/evidence"
E2E_DIR="$EVIDENCE_DIR/e2e_results"
PERF_DIR="$EVIDENCE_DIR/perf"

mkdir -p "$EVIDENCE_DIR" "$E2E_DIR" "$PERF_DIR"

export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-foundry-checklist}"
export SPICE_NETWORK_NAME="${SPICE_NETWORK_NAME:-${COMPOSE_PROJECT_NAME}_network}"

# Prefer repo-provided .env for port overrides and tokens. If missing, set safe defaults.
if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
  set +a
else
  export POSTGRES_PORT_HOST="${POSTGRES_PORT_HOST:-55433}"
  export REDIS_PORT_HOST="${REDIS_PORT_HOST:-6380}"
  export ELASTICSEARCH_PORT_HOST="${ELASTICSEARCH_PORT_HOST:-19200}"
  export ELASTICSEARCH_TRANSPORT_PORT_HOST="${ELASTICSEARCH_TRANSPORT_PORT_HOST:-19300}"
  export MINIO_PORT_HOST="${MINIO_PORT_HOST:-9002}"
  export MINIO_CONSOLE_PORT_HOST="${MINIO_CONSOLE_PORT_HOST:-9003}"
  export KAFKA_PORT_HOST="${KAFKA_PORT_HOST:-39092}"
  export KAFKA_UI_PORT_HOST="${KAFKA_UI_PORT_HOST:-18082}"
  export ZOOKEEPER_PORT_HOST="${ZOOKEEPER_PORT_HOST:-2182}"
  export LAKEFS_PORT_HOST="${LAKEFS_PORT_HOST:-48080}"
  export LAKEFS_INSTALLATION_ACCESS_KEY_ID="${LAKEFS_INSTALLATION_ACCESS_KEY_ID:-spice-lakefs-admin}"
  export LAKEFS_INSTALLATION_SECRET_ACCESS_KEY="${LAKEFS_INSTALLATION_SECRET_ACCESS_KEY:-spice-lakefs-admin-secret}"
  export ADMIN_TOKEN="${ADMIN_TOKEN:-test-token}"
  export BFF_ADMIN_TOKEN="${BFF_ADMIN_TOKEN:-test-token}"
fi

log_section() {
  local title="$1"
  echo ""
  echo "## ${title}"
}

log_section "Docker compose cleanup" >"$EVIDENCE_DIR/compose_cleanup.log"
"$PYTHON_BIN" - <<'PY' 2>&1 | tee -a "$EVIDENCE_DIR/compose_cleanup.log"
from __future__ import annotations

from pathlib import Path
import re

paths = [
    Path("backend/docker-compose.yml"),
    Path("docker-compose.databases.yml"),
    Path("docker-compose.kafka.yml"),
    Path("docker-compose.terminusdb.yml"),
]

names: list[str] = []
for path in paths:
    if not path.exists():
        continue
    text = path.read_text(encoding="utf-8")
    for match in re.finditer(r"^\s*container_name:\s*([^\s#]+)\s*$", text, flags=re.M):
        names.append(match.group(1))

deduped: list[str] = []
for name in names:
    if name not in deduped:
        deduped.append(name)

print("container_names:")
for name in deduped:
    print(name)
PY

while IFS= read -r container; do
  [[ -z "$container" ]] && continue
  [[ "$container" == "container_names:" ]] && continue
  docker rm -f "$container" >/dev/null 2>&1 || true
done < <("$PYTHON_BIN" - <<'PY'
from pathlib import Path
import re

paths = [
    Path("backend/docker-compose.yml"),
    Path("docker-compose.databases.yml"),
    Path("docker-compose.kafka.yml"),
    Path("docker-compose.terminusdb.yml"),
]

names = []
for path in paths:
    if not path.exists():
        continue
    text = path.read_text(encoding="utf-8")
    names.extend(re.findall(r"^\s*container_name:\s*([^\s#]+)\s*$", text, flags=re.M))

seen = set()
for name in names:
    if name in seen:
        continue
    seen.add(name)
    print(name)
PY
)

docker network rm "${SPICE_NETWORK_NAME:-spice_network}" >/dev/null 2>&1 || true

log_section "Docker volume cleanup" >>"$EVIDENCE_DIR/compose_cleanup.log"
while IFS= read -r volume; do
  [[ -z "$volume" ]] && continue
  docker volume rm -f "$volume" >/dev/null 2>&1 || true
done < <("$PYTHON_BIN" - <<'PY'
import os
import subprocess

prefix = f"{os.environ.get('COMPOSE_PROJECT_NAME', 'foundry-checklist')}_"
out = subprocess.check_output(["docker", "volume", "ls", "--format", "{{.Name}}"], text=True)
for line in out.splitlines():
    name = line.strip()
    if name.startswith(prefix):
        print(name)
PY
)

log_section "Port preflight (docker + host, auto-override)" >"$EVIDENCE_DIR/port_preflight.log"
PORT_EXPORTS="$(
  "$PYTHON_BIN" - <<'PY' 2>&1 | tee -a "$EVIDENCE_DIR/port_preflight.log"
from __future__ import annotations

import contextlib
import os
import re
import socket
import subprocess


def _docker_used_host_ports() -> set[int]:
    used: set[int] = set()
    try:
        out = subprocess.check_output(["docker", "ps", "--format", "{{.Ports}}"], text=True, stderr=subprocess.STDOUT)
    except Exception:
        return used
    for line in out.splitlines():
        # Examples:
        # 0.0.0.0:6380->6379/tcp, :::6380->6379/tcp
        # 0.0.0.0:19200-19201->9200-9201/tcp
        for match in re.finditer(r":(\d+)(?:-(\d+))?->", line):
            start = int(match.group(1))
            end = int(match.group(2) or start)
            for port in range(start, end + 1):
                used.add(port)
    return used


DOCKER_USED_PORTS = _docker_used_host_ports()


def _port_is_free(port: int) -> bool:
    if port <= 0:
        return False
    if port in DOCKER_USED_PORTS:
        return False

    # Best-effort host bind check. On some Docker-for-mac backends, docker port usage
    # may not show as a local listener; we therefore treat docker-ps as authoritative.
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def _pick_port(key: str, desired: int, *, candidates: list[int]) -> int:
    if _port_is_free(desired):
        print(f"# {key}: desired {desired} is free")
        return desired

    for port in candidates:
        if _port_is_free(port):
            print(f"# {key}: desired {desired} is taken; using fallback {port}")
            return port

    # Last resort: let OS choose an ephemeral port (also avoid docker-used ports).
    for _ in range(20):
        with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            sock.bind(("127.0.0.1", 0))
            port = int(sock.getsockname()[1])
        if port not in DOCKER_USED_PORTS:
            print(f"# {key}: desired {desired} is taken; using ephemeral {port}")
            return port
    raise SystemExit(f"Could not find a free host port for {key} after multiple attempts")


ports: dict[str, tuple[int, list[int]]] = {
    # Keep fallbacks deterministic-ish: try {x+10000, x+20000, x+30000} before ephemeral.
    "POSTGRES_PORT_HOST": (int(os.getenv("POSTGRES_PORT_HOST", "55433")), []),
    "REDIS_PORT_HOST": (int(os.getenv("REDIS_PORT_HOST", "6380")), []),
    "ELASTICSEARCH_PORT_HOST": (int(os.getenv("ELASTICSEARCH_PORT_HOST", "19200")), []),
    "ELASTICSEARCH_TRANSPORT_PORT_HOST": (int(os.getenv("ELASTICSEARCH_TRANSPORT_PORT_HOST", "19300")), []),
    "MINIO_PORT_HOST": (int(os.getenv("MINIO_PORT_HOST", "9002")), []),
    "MINIO_CONSOLE_PORT_HOST": (int(os.getenv("MINIO_CONSOLE_PORT_HOST", "9003")), []),
    "KAFKA_PORT_HOST": (int(os.getenv("KAFKA_PORT_HOST", "39092")), []),
    "KAFKA_UI_PORT_HOST": (int(os.getenv("KAFKA_UI_PORT_HOST", "18082")), []),
    "ZOOKEEPER_PORT_HOST": (int(os.getenv("ZOOKEEPER_PORT_HOST", "2182")), []),
    "LAKEFS_PORT_HOST": (int(os.getenv("LAKEFS_PORT_HOST", "48080")), []),
}

print("# docker_used_host_ports_count=", len(DOCKER_USED_PORTS))

for key, (desired, explicit_candidates) in ports.items():
    base = desired
    candidates = list(explicit_candidates)
    if not candidates:
        for offset in (10000, 20000, 30000):
            candidate = base + offset
            if 1 <= candidate <= 65535:
                candidates.append(candidate)

    chosen = _pick_port(key, desired, candidates=candidates)
    print(f"export {key}=\"{chosen}\"")
PY
)"
eval "$PORT_EXPORTS"

log_section "Repo root scan" >"$EVIDENCE_DIR/baseline_repo_scan.log"
{
  date -u
  echo ""
  echo "pwd:"
  pwd
  echo ""
  echo "ls:"
  ls
  echo ""
  echo "git toplevel:"
  git rev-parse --show-toplevel 2>/dev/null || true
  echo ""
  echo "compose files:"
  ls docker-compose*.yml backend/docker-compose.yml 2>/dev/null || true
} >>"$EVIDENCE_DIR/baseline_repo_scan.log"

log_section "Checklist parse" >"$EVIDENCE_DIR/checklist_parse.log"
"$PYTHON_BIN" - <<'PY' >>"$EVIDENCE_DIR/checklist_parse.log"
from pathlib import Path

path = Path("docs/PipelineBuilder_checklist.md")
if not path.exists():
    raise SystemExit("Missing docs/PipelineBuilder_checklist.md")

text = path.read_text(encoding="utf-8")
lines = text.splitlines()
checkboxes = sum(("☐" in line) or ("☑" in line) for line in lines)
print(f"path={path.resolve()}")
print(f"total_checkboxes={checkboxes}")
print("")
print("## First 40 lines")
for line in lines[:40]:
    print(line)
PY

log_section "Docker compose up" >"$EVIDENCE_DIR/compose.log"
{
  date -u
  echo ""
  docker compose -f "$ROOT_DIR/docker-compose.full.yml" up -d --build
  echo ""
  docker compose -f "$ROOT_DIR/docker-compose.full.yml" ps
} 2>&1 | tee -a "$EVIDENCE_DIR/compose.log"

log_section "Docker compose wait" >"$EVIDENCE_DIR/compose_wait.log"
"$PYTHON_BIN" - <<'PY' 2>&1 | tee -a "$EVIDENCE_DIR/compose_wait.log"
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class ContainerCheck:
    name: str
    require_healthy: bool = True


def _run(cmd: list[str]) -> str:
    return subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT).strip()


def _inspect(name: str) -> dict:
    raw = _run(["docker", "inspect", name])
    payload = json.loads(raw)
    if not payload:
        raise RuntimeError(f"docker inspect returned empty payload for {name}")
    return payload[0]


def _state(name: str) -> tuple[str, str | None, int | None]:
    info = _inspect(name)
    state = info.get("State") or {}
    status = str(state.get("Status") or "")
    health = (state.get("Health") or {}).get("Status")
    exit_code = state.get("ExitCode")
    return status, str(health) if health is not None else None, int(exit_code) if exit_code is not None else None


def _env_var(name: str, key: str) -> str | None:
    info = _inspect(name)
    env = info.get("Config", {}).get("Env") or []
    prefix = f"{key}="
    for item in env:
        if item.startswith(prefix):
            return item[len(prefix) :]
    return None


def _published_host_ports(name: str, container_port: str) -> list[str]:
    info = _inspect(name)
    ports = (info.get("NetworkSettings") or {}).get("Ports") or {}
    bindings = ports.get(container_port) or []
    host_ports: list[str] = []
    for binding in bindings:
        host_port = binding.get("HostPort")
        if host_port:
            host_ports.append(str(host_port))
    return host_ports


def _tcp_check(host: str, port: int, timeout_seconds: float = 1.0) -> None:
    with socket.create_connection((host, port), timeout=timeout_seconds):
        return


checks = [
    ContainerCheck("spice-foundry-postgres"),
    ContainerCheck("spice-foundry-redis"),
    ContainerCheck("spice-foundry-elasticsearch"),
    ContainerCheck("spice-foundry-minio"),
    ContainerCheck("spice-foundry-lakefs"),
    ContainerCheck("spice_terminusdb"),
    ContainerCheck("spice-foundry-zookeeper"),
    ContainerCheck("spice-foundry-kafka"),
    ContainerCheck("spice_oms"),
    ContainerCheck("spice_bff"),
    ContainerCheck("spice_funnel"),
]

timeout_seconds = int(os.getenv("COMPOSE_WAIT_TIMEOUT_SECONDS", "240"))
deadline = time.monotonic() + timeout_seconds

print("wait_timeout_seconds=", timeout_seconds)
print("required_containers=", ", ".join(c.name for c in checks))
print("")

kafka_host_port = int(os.getenv("KAFKA_PORT_HOST", "39092"))
print("expected_kafka_bootstrap=", f"127.0.0.1:{kafka_host_port}")
postgres_host_port = int(os.getenv("POSTGRES_PORT_HOST", "55433"))
redis_host_port = int(os.getenv("REDIS_PORT_HOST", "6380"))
minio_host_port = int(os.getenv("MINIO_PORT_HOST", "9002"))
terminus_host_port = int(os.getenv("TERMINUS_PORT_HOST", "6363"))
lakefs_host_port = int(os.getenv("LAKEFS_PORT_HOST", "48080"))
print("expected_postgres_tcp=", f"127.0.0.1:{postgres_host_port}")
print("expected_redis_tcp=", f"127.0.0.1:{redis_host_port}")
print("expected_minio_tcp=", f"127.0.0.1:{minio_host_port}")
print("expected_terminus_tcp=", f"127.0.0.1:{terminus_host_port}")
print("expected_lakefs_tcp=", f"127.0.0.1:{lakefs_host_port}")

# Validate kafka advertised listeners matches the host bootstrap we rely on for host-run tests.
try:
    advertised = _env_var("spice-foundry-kafka", "KAFKA_ADVERTISED_LISTENERS")
except subprocess.CalledProcessError as e:
    print("ERROR: failed to inspect kafka container. docker output:")
    print(e.output)
    raise

if advertised:
    expected = f"PLAINTEXT_HOST://127.0.0.1:{kafka_host_port}"
    if expected not in advertised:
        raise SystemExit(
            f"KAFKA_ADVERTISED_LISTENERS mismatch: expected to contain {expected!r} but got {advertised!r}"
        )
    print("kafka_advertised_listeners_ok=", advertised)
else:
    print("WARNING: KAFKA_ADVERTISED_LISTENERS not found in kafka container env")

published_ports = _published_host_ports("spice-foundry-kafka", "9092/tcp")
if not published_ports:
    raise SystemExit("Kafka port 9092/tcp is not published to the host (no bindings found in docker inspect)")
if str(kafka_host_port) not in published_ports:
    raise SystemExit(
        f"Kafka host port mapping mismatch: expected 9092/tcp to include host port {kafka_host_port}, got {published_ports}"
    )
print("kafka_host_port_mapping_ok=", published_ports)

print("")

while True:
    pending: list[str] = []
    for check in checks:
        try:
            status, health, exit_code = _state(check.name)
        except subprocess.CalledProcessError:
            pending.append(f"{check.name}:missing")
            continue

        if status == "exited":
            # Fail fast: do not "wait and pray". Print logs for diagnosis.
            print(f"ERROR: container exited: {check.name} exit_code={exit_code}")
            try:
                logs = _run(["docker", "logs", "--tail", "200", check.name])
                print(f"--- docker logs tail ({check.name}) ---")
                print(logs)
            except Exception as e:
                print(f"failed to fetch logs for {check.name}: {e}")
            raise SystemExit(1)

        if check.require_healthy and health is not None and health != "healthy":
            pending.append(f"{check.name}:{health}")
            continue
        if status != "running":
            pending.append(f"{check.name}:{status}")
            continue

    # Extra host-side sanity: kafka bootstrap port must accept TCP.
    try:
        _tcp_check("127.0.0.1", kafka_host_port, timeout_seconds=1.0)
    except Exception as e:
        pending.append(f"kafka_bootstrap_tcp:127.0.0.1:{kafka_host_port} ({e.__class__.__name__})")

    for label, port in (
        ("postgres_tcp", postgres_host_port),
        ("redis_tcp", redis_host_port),
        ("minio_tcp", minio_host_port),
        ("terminus_tcp", terminus_host_port),
        ("lakefs_tcp", lakefs_host_port),
    ):
        try:
            _tcp_check("127.0.0.1", port, timeout_seconds=1.0)
        except Exception as e:
            pending.append(f"{label}:127.0.0.1:{port} ({e.__class__.__name__})")

    if not pending:
        print("ALL_READY")
        sys.exit(0)

    if time.monotonic() >= deadline:
        print("TIMEOUT waiting for readiness. pending:")
        for item in pending:
            print(" -", item)
        raise SystemExit(1)

    print("waiting... pending:", ", ".join(pending))
    time.sleep(2.0)
PY

log_section "lakeFS bootstrap (repos + DB credentials)" >"$EVIDENCE_DIR/lakefs_bootstrap.log"
LAKEFS_BOOTSTRAP_CREDS_FILE="$(mktemp)"
export LAKEFS_BOOTSTRAP_CREDS_FILE
trap 'rm -f "$LAKEFS_BOOTSTRAP_CREDS_FILE"' EXIT

"$PYTHON_BIN" - <<'PY' 2>&1 | tee -a "$EVIDENCE_DIR/lakefs_bootstrap.log"
from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.request


def _http_json(url: str, *, method: str = "GET", headers: dict[str, str] | None = None, payload: dict | None = None) -> tuple[int, dict]:
    data = None
    req_headers = dict(headers or {})
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        req_headers.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, data=data, method=method, headers=req_headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8")
            body = json.loads(raw) if raw else {}
            return resp.status, body
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8") if e.fp else ""
        try:
            body = json.loads(raw) if raw else {}
        except Exception:
            body = {"raw": raw}
        return int(e.code or 0), body


lakefs_port = int(os.getenv("LAKEFS_PORT_HOST", "48080"))
lakefs_url = f"http://127.0.0.1:{lakefs_port}"
bff_url = (os.getenv("BFF_BASE_URL") or "http://127.0.0.1:8002").rstrip("/")

admin_token = (os.getenv("ADMIN_TOKEN") or "").strip()
if not admin_token:
    raise SystemExit("ADMIN_TOKEN must be set for bootstrap")

install_user = (os.getenv("LAKEFS_INSTALLATION_USER_NAME") or "spice-admin").strip() or "spice-admin"
install_key = (os.getenv("LAKEFS_INSTALLATION_ACCESS_KEY_ID") or "spice-lakefs-admin").strip()
install_secret = (os.getenv("LAKEFS_INSTALLATION_SECRET_ACCESS_KEY") or "spice-lakefs-admin-secret").strip()

basic = base64.b64encode(f"{install_key}:{install_secret}".encode("utf-8")).decode("ascii")
auth_header = {"Authorization": f"Basic {basic}"}

print("lakefs_url=", lakefs_url)
print("bff_url=", bff_url)
print("lakefs_setup_user=", install_user)

# lakeFS readiness (explicit: do not 'wait and pray')
deadline = time.monotonic() + 60
while time.monotonic() < deadline:
    try:
        with urllib.request.urlopen(f"{lakefs_url}/healthcheck", timeout=3) as resp:
            if resp.status == 200:
                break
    except Exception:
        time.sleep(1)
else:
    raise SystemExit("lakeFS healthcheck did not become ready within 60s")

def _extract_setup_credentials(raw: str) -> tuple[str, str] | None:
    access = None
    secret = None
    for line in raw.splitlines():
        match = re.search(r"\baccess_key_id:\s*([^\s]+)\s*$", line)
        if match:
            access = match.group(1).strip()
        match = re.search(r"\bsecret_access_key:\s*([^\s]+)\s*$", line)
        if match:
            secret = match.group(1).strip()
    if access and secret:
        return access, secret
    return None


def _lakefs_basic_headers(access_key: str, secret_key: str) -> dict[str, str]:
    basic = base64.b64encode(f"{access_key}:{secret_key}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {basic}"}


def _run(cmd: list[str]) -> str:
    return subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT).strip()


def _ensure_lakefs_setup() -> tuple[str, str]:
    # Prefer configured installation credentials if they already work.
    status_code, _ = _http_json(f"{lakefs_url}/api/v1/repositories", headers=_lakefs_basic_headers(install_key, install_secret))
    if status_code == 200:
        return install_key, install_secret

    # Otherwise, bootstrap using `lakefs setup` inside the running container and use returned credentials.
    # Note: this is deterministic for CI because the verifier always starts from clean docker volumes.
    out = _run(
        [
            "docker",
            "exec",
            "spice-foundry-lakefs",
            "lakefs",
            "setup",
            "--no-check",
            "--user-name",
            install_user,
        ]
    )
    creds = _extract_setup_credentials(out)
    if not creds:
        raise SystemExit(f"lakeFS setup did not return credentials. output:\n{out}")
    access_key_id, secret_access_key = creds

    # Verify the returned credentials actually work.
    status_code, body = _http_json(f"{lakefs_url}/api/v1/repositories", headers=_lakefs_basic_headers(access_key_id, secret_access_key))
    if status_code != 200:
        raise SystemExit(f"lakeFS credentials from setup are invalid (http={status_code}): {body}")
    return access_key_id, secret_access_key


effective_key, effective_secret = _ensure_lakefs_setup()
auth_header = _lakefs_basic_headers(effective_key, effective_secret)
print("lakefs_auth_access_key_id=", effective_key)

creds_file = os.getenv("LAKEFS_BOOTSTRAP_CREDS_FILE")
if creds_file:
    with open(creds_file, "w", encoding="utf-8") as handle:
        json.dump({"access_key_id": effective_key, "secret_access_key": effective_secret}, handle)
    try:
        os.chmod(creds_file, 0o600)
    except Exception:
        pass

# lakeFS repo sanity (create if missing)
status_code, repos_payload = _http_json(f"{lakefs_url}/api/v1/repositories", headers=auth_header)
if status_code != 200:
    raise SystemExit(f"lakeFS list repositories failed (http={status_code}): {repos_payload}")

repo_names: set[str] = set()
if isinstance(repos_payload, dict):
    items = repos_payload.get("results") or repos_payload.get("repositories") or repos_payload.get("repos") or repos_payload.get("data")
    if isinstance(items, list):
        for item in items:
            if isinstance(item, dict) and isinstance(item.get("id"), str):
                repo_names.add(item["id"])
            elif isinstance(item, dict) and isinstance(item.get("name"), str):
                repo_names.add(item["name"])
            elif isinstance(item, str):
                repo_names.add(item)
elif isinstance(repos_payload, list):
    for item in repos_payload:
        if isinstance(item, dict) and isinstance(item.get("id"), str):
            repo_names.add(item["id"])
        elif isinstance(item, dict) and isinstance(item.get("name"), str):
            repo_names.add(item["name"])

required_repos = {"pipeline-artifacts", "raw-datasets"}
missing = sorted(required_repos - repo_names)
if missing:
    for name in missing:
        status_code, payload = _http_json(
            f"{lakefs_url}/api/v1/repositories",
            method="POST",
            headers=auth_header,
            payload={
                "name": name,
                "storage_namespace": f"s3://lakefs/{name}",
                "default_branch": "main",
            },
        )
        if status_code not in (201, 409):
            raise SystemExit(f"lakeFS create repository failed (name={name} http={status_code}): {payload}")
    status_code, repos_payload = _http_json(f"{lakefs_url}/api/v1/repositories", headers=auth_header)
    if status_code != 200:
        raise SystemExit(f"lakeFS list repositories failed after create (http={status_code}): {repos_payload}")
print("lakefs_repos_ok=", sorted(required_repos))

# Seed Postgres-managed lakeFS credentials via BFF admin API
principals = [
    ("service", "bff"),
    ("service", "pipeline-worker"),
    ("service", "pipeline-scheduler"),
]

for principal_type, principal_id in principals:
    status_code, payload = _http_json(
        f"{bff_url}/api/v1/admin/lakefs/credentials",
        method="POST",
        headers={"X-Admin-Token": admin_token},
        payload={
            "principal_type": principal_type,
            "principal_id": principal_id,
            "access_key_id": effective_key,
            "secret_access_key": effective_secret,
        },
    )
    if status_code != 200:
        raise SystemExit(f"BFF upsert lakeFS credentials failed for {principal_id} (http={status_code}): {payload}")

status_code, payload = _http_json(
    f"{bff_url}/api/v1/admin/lakefs/credentials",
    method="GET",
    headers={"X-Admin-Token": admin_token},
)
if status_code != 200:
    raise SystemExit(f"BFF list lakeFS credentials failed (http={status_code}): {payload}")
print("bff_lakefs_credentials_seeded_count=", ((payload.get("data") or {}).get("count")))
PY

export LAKEFS_INSTALLATION_ACCESS_KEY_ID="$("$PYTHON_BIN" - <<'PY'
import json
import os

path = os.environ.get("LAKEFS_BOOTSTRAP_CREDS_FILE")
if not path:
    raise SystemExit("Missing LAKEFS_BOOTSTRAP_CREDS_FILE")
with open(path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)
print(payload["access_key_id"])
PY
)"
export LAKEFS_INSTALLATION_SECRET_ACCESS_KEY="$("$PYTHON_BIN" - <<'PY'
import json
import os

path = os.environ.get("LAKEFS_BOOTSTRAP_CREDS_FILE")
if not path:
    raise SystemExit("Missing LAKEFS_BOOTSTRAP_CREDS_FILE")
with open(path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)
print(payload["secret_access_key"])
PY
)"

log_section "Worker readiness (post-lakeFS bootstrap)" >"$EVIDENCE_DIR/worker_wait.log"
"$PYTHON_BIN" -u - <<'PY' 2>&1 | tee -a "$EVIDENCE_DIR/worker_wait.log"
from __future__ import annotations

import json
import subprocess
import time


def _run(cmd: list[str]) -> str:
    return subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT).strip()


def _inspect(name: str) -> dict:
    raw = _run(["docker", "inspect", name])
    payload = json.loads(raw)
    if not payload:
        raise RuntimeError(f"docker inspect returned empty payload for {name}")
    return payload[0]


def _state(name: str) -> tuple[str, str | None, int | None]:
    info = _inspect(name)
    state = info.get("State") or {}
    status = str(state.get("Status") or "")
    health = (state.get("Health") or {}).get("Status")
    exit_code = state.get("ExitCode")
    return status, str(health) if health is not None else None, int(exit_code) if exit_code is not None else None


def _restart(name: str) -> None:
    subprocess.run(["docker", "restart", name], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)


def _log_tail(name: str, lines: int = 200) -> None:
    try:
        logs = _run(["docker", "logs", "--tail", str(lines), name])
    except Exception as exc:
        print(f"failed to fetch logs for {name}: {exc}")
        return
    print(f"--- docker logs tail ({name}) ---")
    print(logs)


targets = ["spice_pipeline_worker", "spice_pipeline_scheduler"]
print("targets=", ", ".join(targets))
for target in targets:
    _restart(target)

deadline = time.monotonic() + 120
while True:
    pending: list[str] = []
    for target in targets:
        status, health, exit_code = _state(target)
        if status == "exited":
            print(f"ERROR: container exited after lakeFS bootstrap: {target} exit_code={exit_code}")
            _log_tail(target)
            raise SystemExit(1)
        if status == "restarting":
            pending.append(f"{target}:restarting")
            continue
        if status != "running":
            pending.append(f"{target}:{status}")
            continue
        if health is not None and health != "healthy":
            pending.append(f"{target}:health={health}")
            continue

    if not pending:
        print("WORKERS_READY")
        raise SystemExit(0)

    if time.monotonic() >= deadline:
        print("TIMEOUT waiting for workers readiness. pending:")
        for item in pending:
            print(" -", item)
        for target in targets:
            _log_tail(target)
        raise SystemExit(1)

    print("waiting... pending:", ", ".join(pending))
    time.sleep(2.0)
PY

log_section "Backend tests" >"$EVIDENCE_DIR/backend_test.log"
VENV_DIR="$ROOT_DIR/.venv_foundry_checklist"
"$PYTHON_BIN" -c "import sys; print(sys.version)" >>"$EVIDENCE_DIR/backend_test.log"
rm -rf "$VENV_DIR"
"$PYTHON_BIN" -m venv "$VENV_DIR"
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"
python -m pip install -U pip setuptools wheel >/dev/null

pushd "$ROOT_DIR/backend/tests" >/dev/null
python -m pip install -r requirements.txt >/dev/null
python -m pip install pyyaml >/dev/null
popd >/dev/null

export PYTHONPATH="$ROOT_DIR/backend${PYTHONPATH:+:$PYTHONPATH}"
export ADMIN_TOKEN="${ADMIN_TOKEN:-test-token}"
export BFF_ADMIN_TOKEN="${BFF_ADMIN_TOKEN:-test-token}"
export RUN_LIVE_BRANCH_VIRTUALIZATION="${RUN_LIVE_BRANCH_VIRTUALIZATION:-true}"
export RUN_LIVE_OMS_SMOKE="${RUN_LIVE_OMS_SMOKE:-true}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-127.0.0.1:${KAFKA_PORT_HOST:-39092}}"
export POSTGRES_URL="${POSTGRES_URL:-postgresql://${POSTGRES_USER:-spiceadmin}:${POSTGRES_PASSWORD:-spicepass123}@127.0.0.1:${POSTGRES_PORT_HOST:-55433}/${POSTGRES_DB:-spicedb}}"
export REDIS_URL="${REDIS_URL:-redis://:${REDIS_PASSWORD:-spicepass123}@127.0.0.1:${REDIS_PORT_HOST:-6380}}"
export MINIO_ENDPOINT_URL="${MINIO_ENDPOINT_URL:-http://127.0.0.1:${MINIO_PORT_HOST:-9002}}"
export MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
export MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin123}"
export ELASTICSEARCH_HOST="${ELASTICSEARCH_HOST:-127.0.0.1}"
export ELASTICSEARCH_PORT="${ELASTICSEARCH_PORT:-${ELASTICSEARCH_PORT_HOST:-9200}}"
export ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}}"

pytest -q backend/tests backend/bff/tests backend/funnel/tests 2>&1 | tee -a "$EVIDENCE_DIR/backend_test.log"

deactivate

log_section "Frontend unit tests" >"$EVIDENCE_DIR/frontend_test.log"
pushd "$ROOT_DIR/frontend" >/dev/null
npm ci 2>&1 | tee -a "$EVIDENCE_DIR/frontend_test.log"
npm run test:unit 2>&1 | tee -a "$EVIDENCE_DIR/frontend_test.log"
popd >/dev/null

log_section "Frontend e2e tests" >"$EVIDENCE_DIR/e2e_test.log"
rm -rf "$E2E_DIR" && mkdir -p "$E2E_DIR"
pushd "$ROOT_DIR/frontend" >/dev/null
export PLAYWRIGHT_OUTPUT_DIR="../docs/foundry_checklist/evidence/e2e_results"
export PLAYWRIGHT_SCREENSHOT="on"
export PLAYWRIGHT_TRACE="on"
export PLAYWRIGHT_BFF_BASE_URL="http://127.0.0.1:8002"
# Ensure the Playwright dev server restarts each run (no stale proxy/env).
export CI=1
npm run test:e2e 2>&1 | tee -a "$EVIDENCE_DIR/e2e_test.log"
popd >/dev/null

log_section "Matrix gate" >"$EVIDENCE_DIR/matrix_gate.log"
"$VENV_DIR/bin/python" - <<'PY' 2>&1 | tee -a "$EVIDENCE_DIR/matrix_gate.log"
from pathlib import Path

import yaml

matrix_path = Path("docs/foundry_checklist/FOUNDARY_CHECKLIST_MATRIX.yml")
if not matrix_path.exists():
    raise SystemExit("Missing checklist matrix: docs/foundry_checklist/FOUNDARY_CHECKLIST_MATRIX.yml")

entries = yaml.safe_load(matrix_path.read_text(encoding="utf-8"))
if not isinstance(entries, list):
    raise SystemExit("Matrix must be a list of entries")

failures = []
for entry in entries:
    if entry.get("priority") in {"P0", "P1"} and entry.get("status") != "PASS":
        failures.append(entry.get("id"))

if failures:
    print("P0/P1 items not PASS:", ", ".join(failures))
    raise SystemExit(1)

print("All P0/P1 items are PASS.")
PY

echo ""
echo "Verification completed successfully."
