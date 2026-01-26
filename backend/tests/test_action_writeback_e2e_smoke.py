"""
E2E smoke test: BFF → OMS → EventStore → action-worker → lakeFS → ES overlay.

This is a destructive, live-stack test and is opt-in via env:

  RUN_LIVE_ACTION_WRITEBACK_SMOKE=true \
    PYTHONPATH=backend python -m pytest -q -c backend/pytest.ini \
      backend/tests/test_action_writeback_e2e_smoke.py
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import platform
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

import aiohttp
import pytest

from shared.config.search_config import get_instances_index_name
from shared.services.storage.event_store import event_store
from shared.utils.writeback_lifecycle import overlay_doc_id
from tests.utils.auth import bff_auth_headers


if os.getenv("RUN_LIVE_ACTION_WRITEBACK_SMOKE", "").strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip(
        "RUN_LIVE_ACTION_WRITEBACK_SMOKE must be enabled for this test run. "
        "Set RUN_LIVE_ACTION_WRITEBACK_SMOKE=true.",
        allow_module_level=True,
    )


BFF_URL = (os.getenv("BFF_BASE_URL") or os.getenv("BFF_URL") or "http://127.0.0.1:8002").rstrip("/")
OMS_URL = (os.getenv("OMS_BASE_URL") or os.getenv("OMS_URL") or "http://127.0.0.1:8000").rstrip("/")


def _load_repo_dotenv() -> Dict[str, str]:
    try:
        repo_root = Path(__file__).resolve().parents[2]
    except Exception:  # pragma: no cover
        return {}

    env_path = repo_root / ".env"
    if not env_path.exists():
        return {}

    values: Dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            values[key] = value
    return values


def _truthy(value: Optional[str]) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _port_from_env(
    *,
    env: Dict[str, str],
    key: str,
    fallback: int,
) -> int:
    raw = (os.getenv(key) or "").strip() or (env.get(key) or "").strip()
    if not raw:
        return int(fallback)
    try:
        return int(raw)
    except ValueError:
        return int(fallback)


def _require_env(
    *,
    env: Dict[str, str],
    key: str,
    allow_empty: bool = False,
) -> str:
    value = (os.getenv(key) or "").strip() or (env.get(key) or "").strip()
    if allow_empty:
        return value
    if not value:
        raise AssertionError(f"Missing required env var: {key} (set it or add to repo .env)")
    return value


def _base_headers(*, db_name: Optional[str], actor_id: str) -> Dict[str, str]:
    headers = dict(bff_auth_headers())
    headers.update(
        {
            "X-User-Type": "user",
            "X-User-ID": actor_id,
            "X-User-Name": actor_id,
        }
    )
    if db_name:
        headers["X-DB-Name"] = db_name
    return headers


async def _wait_for_command_completed(
    session: aiohttp.ClientSession,
    *,
    command_id: str,
    timeout_seconds: int = 240,
    poll_interval_seconds: float = 1.0,
) -> Dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last: Optional[Dict[str, Any]] = None

    while time.monotonic() < deadline:
        async with session.get(f"{BFF_URL}/api/v1/commands/{command_id}/status") as resp:
            if resp.status != 200:
                last = {"status": resp.status, "body": await resp.text()}
                await asyncio.sleep(poll_interval_seconds)
                continue
            last = await resp.json()

        status_value = str(last.get("status") or "").upper()
        if status_value in {"COMPLETED", "FAILED", "CANCELLED"}:
            if status_value != "COMPLETED":
                raise AssertionError(f"Command {command_id} ended in {status_value}: {last}")
            await asyncio.sleep(0.5)
            return last

        await asyncio.sleep(poll_interval_seconds)

    raise AssertionError(f"Timed out waiting for command completion (command_id={command_id}, last={last})")


def _extract_command_id(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    for key in ("command_id", "commandId", "id"):
        value = data.get(key) or payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


async def _get_branch_head_commit(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    branch: str = "main",
    headers: Optional[Dict[str, str]] = None,
) -> str:
    async with session.get(
        f"{OMS_URL}/api/v1/version/{db_name}/head",
        params={"branch": branch},
        headers=headers,
    ) as resp:
        if resp.status != 200:
            raise AssertionError(f"Failed to get ontology head commit (http={resp.status}): {await resp.text()}")
        payload = await resp.json()

    data = payload.get("data") if isinstance(payload, dict) else None
    head_commit_id = (data or {}).get("head_commit_id") if isinstance(data, dict) else None
    if isinstance(head_commit_id, str) and head_commit_id.strip():
        return head_commit_id.strip()

    raise AssertionError(f"Unexpected head commit payload shape: {payload}")


async def _record_deployed_commit(
    *,
    db_name: str,
    target_branch: str,
    ontology_commit_id: str,
) -> None:
    # Import lazily so test can set POSTGRES_* env before module init.
    from oms.database.postgres import db as postgres_db
    from oms.services.ontology_deployment_registry_v2 import OntologyDeploymentRegistryV2

    await postgres_db.connect()
    try:
        registry = OntologyDeploymentRegistryV2()
        await registry.record_deployment(
            db_name=db_name,
            target_branch=target_branch,
            ontology_commit_id=ontology_commit_id,
            proposal_id=None,
            status="succeeded",
            deployed_by="smoke",
            metadata={"source": "test_action_writeback_e2e_smoke"},
        )
    finally:
        await postgres_db.disconnect()


async def _wait_for_action_log(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    action_log_id: str,
    headers: Optional[Dict[str, str]] = None,
    timeout_seconds: int = 240,
    poll_interval_seconds: float = 1.0,
) -> Dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last: Optional[Dict[str, Any]] = None
    while time.monotonic() < deadline:
        async with session.get(
            f"{BFF_URL}/api/v1/databases/{db_name}/actions/logs/{action_log_id}",
            headers=headers,
        ) as resp:
            if resp.status != 200:
                await asyncio.sleep(poll_interval_seconds)
                continue
            last = await resp.json()
        rec = (last or {}).get("data") if isinstance(last, dict) else None
        status_value = str((rec or {}).get("status") or "").upper()
        if status_value in {"SUCCEEDED", "FAILED"}:
            return last or {}
        await asyncio.sleep(poll_interval_seconds)
    raise AssertionError(f"Timed out waiting for ActionLog completion (action_log_id={action_log_id}, last={last})")


def _pick_first_lifecycle_id(action_log_payload: Dict[str, Any]) -> str:
    rec = action_log_payload.get("data") if isinstance(action_log_payload.get("data"), dict) else {}
    result = rec.get("result") if isinstance(rec.get("result"), dict) else {}
    applied = result.get("applied_changes")
    if isinstance(applied, list):
        for item in applied:
            if not isinstance(item, dict):
                continue
            lifecycle_id = str(item.get("lifecycle_id") or "").strip()
            if lifecycle_id:
                return lifecycle_id
    return "lc-0"


async def _wait_for_es_overlay_doc(
    *,
    es_base_url: str,
    index_name: str,
    doc_id: str,
    timeout_seconds: int = 120,
    poll_interval_seconds: float = 1.0,
) -> Dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_status = None
    last_body = None
    async with aiohttp.ClientSession() as session:
        while time.monotonic() < deadline:
            async with session.get(f"{es_base_url}/{index_name}/_doc/{doc_id}") as resp:
                last_status = resp.status
                last_body = await resp.text()
                if resp.status == 200:
                    try:
                        payload = json.loads(last_body) if last_body else None
                    except Exception:
                        payload = None
                    if isinstance(payload, dict) and isinstance(payload.get("_source"), dict):
                        return payload["_source"]
            await asyncio.sleep(poll_interval_seconds)

    raise AssertionError(
        f"Timed out waiting for ES overlay doc (index={index_name}, doc_id={doc_id}, last_status={last_status}, last_body={last_body})"
    )


async def _start_action_worker(
    *,
    env: Dict[str, str],
    backend_dir: Path,
) -> asyncio.subprocess.Process:
    proc_env = dict(os.environ)
    proc_env.update(env)
    proc_env["PYTHONPATH"] = str(backend_dir)
    proc_env["DOCKER_CONTAINER"] = "false"
    return await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "action_worker.main",
        env=proc_env,
    )


async def _run_subprocess(
    *args: str,
    cwd: Optional[Path] = None,
    env: Optional[Dict[str, str]] = None,
    timeout_seconds: float = 300.0,
) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *args,
        cwd=str(cwd) if cwd else None,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout_seconds)
    except asyncio.TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        raise
    return proc.returncode or 0, (stdout or b"").decode("utf-8", errors="replace"), (stderr or b"").decode(
        "utf-8", errors="replace"
    )


async def _docker_container_running(container_name: str) -> bool:
    code, out, _ = await _run_subprocess(
        "docker",
        "inspect",
        "-f",
        "{{.State.Running}}",
        container_name,
        timeout_seconds=15.0,
    )
    return code == 0 and out.strip().lower() == "true"


def _smoke_worker_mode() -> str:
    mode = str(os.getenv("ACTION_WRITEBACK_SMOKE_WORKER_MODE") or "").strip().lower()
    if mode:
        if mode not in {"local", "docker"}:
            raise AssertionError("ACTION_WRITEBACK_SMOKE_WORKER_MODE must be one of: local, docker")
        return mode
    # macOS confluent_kafka (librdkafka) can segfault; default to Linux Docker worker.
    return "docker" if platform.system().lower() == "darwin" else "local"


async def _ensure_action_worker_docker_running(*, repo_root: Path, build: bool) -> bool:
    """
    Ensure action-worker is running in Docker and return whether the test started it.

    If it's already running, we avoid changing env/config to prevent unintended recreates.
    """

    container_name = "spice_action_worker"
    if await _docker_container_running(container_name):
        return False

    compose_file = repo_root / "docker-compose.full.yml"
    if not compose_file.exists():
        raise AssertionError(f"Missing docker-compose.full.yml at {compose_file}")

    args = ["docker", "compose", "-f", str(compose_file), "up", "-d"]
    if build:
        args.append("--build")
    args.append("action-worker")
    code, _, err = await _run_subprocess(*args, cwd=repo_root, env=os.environ.copy(), timeout_seconds=900.0)
    if code != 0:
        raise AssertionError(f"Failed to start action-worker via docker compose (exit={code}): {err}")

    # Give the consumer a moment to subscribe before we submit actions.
    await asyncio.sleep(2.0)
    return True


async def _stop_action_worker_docker(*, repo_root: Path) -> None:
    compose_file = repo_root / "docker-compose.full.yml"
    if not compose_file.exists():
        return
    await _run_subprocess(
        "docker",
        "compose",
        "-f",
        str(compose_file),
        "stop",
        "action-worker",
        cwd=repo_root,
        env=os.environ.copy(),
        timeout_seconds=120.0,
    )


async def _stop_process(proc: asyncio.subprocess.Process, *, timeout_seconds: float = 10.0) -> None:
    if proc.returncode is not None:
        return
    try:
        proc.terminate()
    except ProcessLookupError:
        return

    try:
        await asyncio.wait_for(proc.wait(), timeout=timeout_seconds)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            return
        await proc.wait()


def _es_base_url_from_dotenv(env: Dict[str, str]) -> str:
    port = _port_from_env(env=env, key="ELASTICSEARCH_PORT_HOST", fallback=9200)
    host = (os.getenv("ELASTICSEARCH_HOST") or "127.0.0.1").strip() or "127.0.0.1"
    return f"http://{host}:{port}".rstrip("/")


def _action_worker_env_from_dotenv(dotenv: Dict[str, str]) -> Dict[str, str]:
    postgres_port = _port_from_env(env=dotenv, key="POSTGRES_PORT_HOST", fallback=5433)
    minio_port = _port_from_env(env=dotenv, key="MINIO_PORT_HOST", fallback=9000)
    kafka_port = _port_from_env(env=dotenv, key="KAFKA_PORT_HOST", fallback=9092)
    es_port = _port_from_env(env=dotenv, key="ELASTICSEARCH_PORT_HOST", fallback=9200)
    lakefs_port = _port_from_env(env=dotenv, key="LAKEFS_PORT_HOST", fallback=48080)

    lakefs_access = _require_env(env=dotenv, key="LAKEFS_ACCESS_KEY_ID")
    lakefs_secret = _require_env(env=dotenv, key="LAKEFS_SECRET_ACCESS_KEY")

    return {
        "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "127.0.0.1"),
        "POSTGRES_PORT": str(postgres_port),
        "POSTGRES_USER": os.getenv("POSTGRES_USER", "spiceadmin"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "spicepass123"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB", "spicedb"),
        "MINIO_ENDPOINT_URL": f"http://127.0.0.1:{minio_port}",
        "MINIO_ACCESS_KEY": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        "MINIO_SECRET_KEY": os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        "KAFKA_HOST": os.getenv("KAFKA_HOST", "127.0.0.1"),
        "KAFKA_PORT": str(kafka_port),
        "ELASTICSEARCH_HOST": os.getenv("ELASTICSEARCH_HOST", "127.0.0.1"),
        "ELASTICSEARCH_PORT": str(es_port),
        "LAKEFS_API_URL": f"http://127.0.0.1:{lakefs_port}",
        "LAKEFS_S3_ENDPOINT_URL": f"http://127.0.0.1:{lakefs_port}",
        "LAKEFS_ACCESS_KEY_ID": lakefs_access,
        "LAKEFS_SECRET_ACCESS_KEY": lakefs_secret,
    }


def _apply_postgres_env_for_registry(dotenv: Dict[str, str]) -> None:
    # Must happen before importing oms.database.postgres so ServiceConfig gets correct URL.
    os.environ.setdefault("POSTGRES_HOST", os.getenv("POSTGRES_HOST") or "127.0.0.1")
    os.environ.setdefault("POSTGRES_PORT", os.getenv("POSTGRES_PORT") or str(_port_from_env(env=dotenv, key="POSTGRES_PORT_HOST", fallback=5433)))
    os.environ.setdefault("POSTGRES_USER", os.getenv("POSTGRES_USER") or "spiceadmin")
    os.environ.setdefault("POSTGRES_PASSWORD", os.getenv("POSTGRES_PASSWORD") or "spicepass123")
    os.environ.setdefault("POSTGRES_DB", os.getenv("POSTGRES_DB") or "spicedb")


def _apply_event_store_env(dotenv: Dict[str, str]) -> None:
    minio_port = _port_from_env(env=dotenv, key="MINIO_PORT_HOST", fallback=9000)
    os.environ.setdefault("MINIO_ENDPOINT_URL", f"http://127.0.0.1:{minio_port}")
    os.environ.setdefault("MINIO_ACCESS_KEY", os.getenv("MINIO_ACCESS_KEY") or "minioadmin")
    os.environ.setdefault("MINIO_SECRET_KEY", os.getenv("MINIO_SECRET_KEY") or "minioadmin123")


def _apply_lakefs_env(dotenv: Dict[str, str]) -> None:
    lakefs_port = _port_from_env(env=dotenv, key="LAKEFS_PORT_HOST", fallback=48080)
    lakefs_access = _require_env(env=dotenv, key="LAKEFS_ACCESS_KEY_ID")
    lakefs_secret = _require_env(env=dotenv, key="LAKEFS_SECRET_ACCESS_KEY")
    os.environ.setdefault("LAKEFS_API_URL", f"http://127.0.0.1:{lakefs_port}")
    os.environ.setdefault("LAKEFS_S3_ENDPOINT_URL", f"http://127.0.0.1:{lakefs_port}")
    os.environ.setdefault("LAKEFS_ACCESS_KEY_ID", lakefs_access)
    os.environ.setdefault("LAKEFS_SECRET_ACCESS_KEY", lakefs_secret)


def _pick_first_lifecycle_id_any(action_log_payload: Dict[str, Any]) -> str:
    rec = action_log_payload.get("data") if isinstance(action_log_payload.get("data"), dict) else {}
    result = rec.get("result") if isinstance(rec.get("result"), dict) else {}

    for field in ("applied_changes", "attempted_changes"):
        items = result.get(field)
        if isinstance(items, list):
            for item in items:
                if not isinstance(item, dict):
                    continue
                lifecycle_id = str(item.get("lifecycle_id") or "").strip()
                if lifecycle_id:
                    return lifecycle_id

    items = result.get("conflicts")
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            lifecycle_id = str(item.get("lifecycle_id") or "").strip()
            if lifecycle_id:
                return lifecycle_id

    return "lc-0"


async def _update_instance_ingest(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    base_branch: str,
    class_id: str,
    instance_id: str,
    expected_seq: Optional[int],
    patch: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
) -> None:
    if expected_seq is None:
        await event_store.connect()
        aggregate_id = f"{db_name}:{base_branch}:{class_id}:{instance_id}"
        expected_seq = await event_store.get_aggregate_version("Instance", aggregate_id)

    resp = await session.put(
        f"{BFF_URL}/api/v1/databases/{db_name}/instances/{class_id}/{instance_id}/update",
        headers=headers,
        params={"branch": base_branch, "expected_seq": int(expected_seq)},
        json={
            "data": patch,
            "metadata": {"kind": "ingest", "source": "test_action_writeback_e2e_smoke"},
        },
    )
    payload = await resp.json()
    assert resp.status == 202, payload
    cmd = _extract_command_id(payload) or str(payload.get("command_id") or "").strip()
    assert cmd, payload
    await _wait_for_command_completed(session, command_id=cmd)


async def _assert_action_applied_event_in_event_store(action_applied_event_id: str) -> None:
    await event_store.connect()
    key = await event_store.get_event_object_key(event_id=str(action_applied_event_id))
    assert key, f"Missing by-event-id index for ActionApplied event_id={action_applied_event_id}"
    envelope = await event_store.read_event_by_key(key=key)
    assert envelope.event_type == "ACTION_APPLIED"
    assert isinstance(envelope.metadata, dict)
    assert envelope.metadata.get("kind") == "domain"


@pytest.mark.integration
@pytest.mark.requires_infra
@pytest.mark.workflow
@pytest.mark.asyncio
async def test_action_writeback_e2e_smoke() -> None:
    dotenv = _load_repo_dotenv()
    backend_dir = Path(__file__).resolve().parents[1]
    actor_id = f"smoke_user_{uuid.uuid4().hex[:8]}"
    db_name = f"e2e_writeback_{uuid.uuid4().hex[:10]}"
    base_branch = f"smoke-{uuid.uuid4().hex[:8]}"

    class_id = "Ticket"
    instance_id = f"ticket_{uuid.uuid4().hex[:8]}"
    action_type_id = f"approve_ticket_{uuid.uuid4().hex[:8]}"

    es_base_url = _es_base_url_from_dotenv(dotenv)

    # Ensure Postgres env is configured for the deployment registry helper.
    _apply_postgres_env_for_registry(dotenv)
    _apply_event_store_env(dotenv)

    headers = _base_headers(db_name=None, actor_id=actor_id)

    action_worker_proc: Optional[asyncio.subprocess.Process] = None
    started_action_worker_docker = False
    created_db = False

    async with aiohttp.ClientSession(headers=headers) as session:
        try:
            # 1) Create DB (BFF -> OMS -> EventStore)
            resp = await session.post(
                f"{BFF_URL}/api/v1/databases",
                json={"name": db_name, "description": "Action writeback E2E smoke"},
            )
            payload = await resp.json()
            assert resp.status in {201, 202}, payload
            created_db = True

            if resp.status == 202:
                cmd = _extract_command_id(payload)
                assert cmd, payload
                await _wait_for_command_completed(session, command_id=cmd)

            db_headers = _base_headers(db_name=db_name, actor_id=actor_id)

            # 2) Create a writable base branch (avoid protected main branch).
            resp = await session.post(
                f"{BFF_URL}/api/v1/databases/{db_name}/ontology/branches",
                headers=db_headers,
                json={"branch_name": base_branch, "from_branch": "main"},
            )
            branch_payload = await resp.json()
            assert resp.status in {200, 201}, branch_payload

            # 3) Create base ontology class (Ticket)
            ontology_payload = {
                "id": class_id,
                "label": {"en": "Ticket", "ko": "티켓"},
                "description": {"en": "Ticket class (writeback smoke)", "ko": "티켓 (writeback smoke)"},
                "properties": [
                    {
                        "name": "ticket_id",
                        "type": "xsd:string",
                        "label": {"en": "Ticket ID", "ko": "티켓 ID"},
                        "required": True,
                        "primaryKey": True,
                    },
                    {
                        "name": "name",
                        "type": "xsd:string",
                        "label": {"en": "Name", "ko": "이름"},
                        "required": True,
                        "titleKey": True,
                    },
                    {
                        "name": "status",
                        "type": "xsd:string",
                        "label": {"en": "Status", "ko": "상태"},
                        "required": True,
                    },
                    {
                        "name": "approved_by",
                        "type": "xsd:string",
                        "label": {"en": "Approved By", "ko": "승인자"},
                        "required": False,
                    },
                ],
                "relationships": [],
                "metadata": {"source": "test_action_writeback_e2e_smoke"},
            }
            resp = await session.post(
                f"{BFF_URL}/api/v1/databases/{db_name}/ontology",
                headers=db_headers,
                params={"branch": base_branch},
                json=ontology_payload,
            )
            ontology_resp = await resp.json()
            assert resp.status in {200, 202}, ontology_resp
            if resp.status == 202:
                cmd = _extract_command_id(ontology_resp)
                assert cmd, ontology_resp
                await _wait_for_command_completed(session, command_id=cmd)

            # 4) Create a base instance (ingest-only metadata to survive WRITEBACK_ENFORCE).
            resp = await session.post(
                f"{BFF_URL}/api/v1/databases/{db_name}/instances/{class_id}/create",
                headers=db_headers,
                params={"branch": base_branch},
                json={
                    "data": {
                        "ticket_id": instance_id,
                        "name": "Writeback Smoke Ticket",
                        "status": "OPEN",
                    },
                    "metadata": {"kind": "ingest", "source": "test_action_writeback_e2e_smoke"},
                },
            )
            instance_cmd = await resp.json()
            assert resp.status == 202, instance_cmd
            cmd = instance_cmd.get("command_id")
            assert isinstance(cmd, str) and cmd.strip(), instance_cmd
            await _wait_for_command_completed(session, command_id=cmd)

            # 5) Create ActionType ontology resource (writeback-enabled).
            action_type_payload = {
                "id": action_type_id,
                "label": {"en": "Approve Ticket", "ko": "티켓 승인"},
                "description": {"en": "Approve a ticket (smoke)", "ko": "티켓 승인 (smoke)"},
                "spec": {
                    "input_schema": {
                        "fields": [
                            {"name": "ticket", "type": "object_ref", "required": True, "object_type": class_id}
                        ]
                    },
                    "permission_policy": {
                        "effect": "ALLOW",
                        "principals": ["role:Owner", "role:Editor", "role:DomainModeler"],
                    },
                    "writeback_target": {
                        "repo": "ontology-writeback",
                        "branch": "writeback-{db_name}",
                    },
                    "conflict_policy": "FAIL",
                    "implementation": {
                        "type": "template_v1",
                        "targets": [
                            {
                                "target": {"from": "input.ticket"},
                                "changes": {
                                    "set": {
                                        "status": "APPROVED",
                                        "approved_by": {"$ref": "user.id"},
                                    }
                                },
                            }
                        ],
                    },
                },
                "metadata": {"source": "test_action_writeback_e2e_smoke"},
            }
            resp = await session.post(
                f"{BFF_URL}/api/v1/databases/{db_name}/ontology/action-types",
                headers=db_headers,
                params={"branch": base_branch, "expected_head_commit": ""},
                json=action_type_payload,
            )
            created_action_type = await resp.json()
            assert resp.status == 201, created_action_type

            # 6) Record deployment for current base-branch head (actions execute only on deployed commits).
            head_commit = await _get_branch_head_commit(session, db_name=db_name, branch=base_branch, headers=db_headers)
            await _record_deployed_commit(db_name=db_name, target_branch=base_branch, ontology_commit_id=head_commit)

            # 7) Run action-worker (Docker by default on macOS to avoid confluent_kafka segfaults).
            worker_mode = _smoke_worker_mode()
            if worker_mode == "docker":
                build_worker = _truthy(os.getenv("ACTION_WRITEBACK_SMOKE_DOCKER_BUILD", "true"))
                started_action_worker_docker = await _ensure_action_worker_docker_running(
                    repo_root=backend_dir.parent,
                    build=build_worker,
                )
            else:
                action_worker_env = _action_worker_env_from_dotenv(dotenv)
                action_worker_env["ACTION_WORKER_GROUP"] = f"action-worker-smoke-{uuid.uuid4().hex[:8]}"
                action_worker_proc = await _start_action_worker(env=action_worker_env, backend_dir=backend_dir)
                await asyncio.sleep(2.0)

            # 8) Submit action (BFF -> OMS -> EventStore -> Kafka)
            resp = await session.post(
                f"{BFF_URL}/api/v1/databases/{db_name}/actions/{action_type_id}/submit",
                headers=db_headers,
                params={"base_branch": base_branch},
                json={
                    "input": {"ticket": {"class_id": class_id, "instance_id": instance_id}},
                    "correlation_id": f"smoke-{uuid.uuid4()}",
                    "metadata": {"source": "test_action_writeback_e2e_smoke"},
                },
            )
            submit_payload = await resp.json()
            assert resp.status == 202, submit_payload
            action_log_id = str(submit_payload.get("action_log_id") or "").strip()
            assert action_log_id, submit_payload

            # 9) Wait for ActionLog SUCCEEDED (action-worker -> lakeFS commit + ActionApplied emission).
            action_log = await _wait_for_action_log(
                session,
                db_name=db_name,
                action_log_id=action_log_id,
                headers=db_headers,
            )
            rec = action_log.get("data") if isinstance(action_log.get("data"), dict) else {}
            assert str(rec.get("status") or "").upper() == "SUCCEEDED", action_log

            # 10) Verify ActionApplied exists in EventStore (SSoT)
            action_applied_event_id = str(rec.get("action_applied_event_id") or "").strip()
            assert action_applied_event_id, action_log
            await _assert_action_applied_event_in_event_store(action_applied_event_id)

            # 11) Verify overlay doc exists in ES (projection-worker -> overlay)
            writeback_target = rec.get("writeback_target") if isinstance(rec.get("writeback_target"), dict) else {}
            overlay_branch = str(writeback_target.get("branch") or "").strip()
            assert overlay_branch, action_log

            lifecycle_id = _pick_first_lifecycle_id(action_log)
            doc_id = overlay_doc_id(instance_id=instance_id, lifecycle_id=lifecycle_id)
            overlay_index = get_instances_index_name(db_name, branch=overlay_branch)

            doc = await _wait_for_es_overlay_doc(
                es_base_url=es_base_url,
                index_name=overlay_index,
                doc_id=doc_id,
            )
            assert str(doc.get("action_log_id") or "").strip() == action_log_id
            assert str(doc.get("patchset_commit_id") or "").strip() == str(rec.get("writeback_commit_id") or "").strip()
        finally:
            if action_worker_proc is not None:
                await _stop_process(action_worker_proc)

            if started_action_worker_docker and not _truthy(os.getenv("ACTION_WRITEBACK_SMOKE_KEEP_WORKER")):
                await _stop_action_worker_docker(repo_root=backend_dir.parent)

            if created_db and _truthy(os.getenv("ACTION_WRITEBACK_SMOKE_CLEANUP", "true")):
                db_headers = _base_headers(db_name=db_name, actor_id=actor_id)
                resp = await session.delete(f"{BFF_URL}/api/v1/databases/{db_name}", headers=db_headers)
                if resp.status == 202:
                    payload = await resp.json()
                    cmd = _extract_command_id(payload)
                    if cmd:
                        with contextlib.suppress(Exception):
                            await _wait_for_command_completed(session, command_id=cmd, timeout_seconds=240)


@pytest.mark.integration
@pytest.mark.requires_infra
@pytest.mark.workflow
@pytest.mark.asyncio
async def test_action_writeback_e2e_verification_suite() -> None:
    """
    Verification suite for Action writeback behavior (ACTION_WRITEBACK_DESIGN.md):
    - conflict_policy branches (FAIL / WRITEBACK_WINS / BASE_WINS)
    - submission_criteria false rejection
    - permission denied rejection
    - writeback queue + materialized snapshot (lakeFS)
    - BFF read-path overlay usage (ES overlay doc exists + BFF returns overlay doc)
    """

    dotenv = _load_repo_dotenv()
    backend_dir = Path(__file__).resolve().parents[1]
    repo_root = backend_dir.parent

    owner_id = f"smoke_owner_{uuid.uuid4().hex[:8]}"
    unauthorized_id = f"smoke_unauth_{uuid.uuid4().hex[:8]}"
    db_name = f"e2e_writeback_verify_{uuid.uuid4().hex[:10]}"
    base_branch = f"verify-{uuid.uuid4().hex[:8]}"

    class_id = "Ticket"

    action_fail = f"approve_ticket_fail_{uuid.uuid4().hex[:6]}"
    action_merge = f"approve_ticket_merge_{uuid.uuid4().hex[:6]}"
    action_skip = f"approve_ticket_skip_{uuid.uuid4().hex[:6]}"
    action_criteria = f"approve_ticket_criteria_{uuid.uuid4().hex[:6]}"

    es_base_url = _es_base_url_from_dotenv(dotenv)

    # Ensure local helpers can talk to Postgres/MinIO/lakeFS directly.
    _apply_postgres_env_for_registry(dotenv)
    _apply_event_store_env(dotenv)
    _apply_lakefs_env(dotenv)

    action_worker_proc: Optional[asyncio.subprocess.Process] = None
    started_action_worker_docker = False
    created_db = False
    old_materializer_base_branch = os.environ.get("WRITEBACK_MATERIALIZER_BASE_BRANCH")

    headers = _base_headers(db_name=None, actor_id=owner_id)

    async with aiohttp.ClientSession(headers=headers) as session:
        try:
            # Ensure deterministic conflict windows (stop action-worker before enqueuing commands).
            if _smoke_worker_mode() == "docker":
                await _stop_action_worker_docker(repo_root=repo_root)

            # 1) Create DB
            resp = await session.post(
                f"{BFF_URL}/api/v1/databases",
                json={"name": db_name, "description": "Action writeback verification suite"},
            )
            payload = await resp.json()
            assert resp.status in {201, 202}, payload
            created_db = True
            if resp.status == 202:
                cmd = _extract_command_id(payload)
                assert cmd, payload
                await _wait_for_command_completed(session, command_id=cmd)

            db_headers = _base_headers(db_name=db_name, actor_id=owner_id)

            # 2) Create a writable base branch
            resp = await session.post(
                f"{BFF_URL}/api/v1/databases/{db_name}/ontology/branches",
                headers=db_headers,
                json={"branch_name": base_branch, "from_branch": "main"},
            )
            branch_payload = await resp.json()
            assert resp.status in {200, 201}, branch_payload

            # 3) Create base ontology class (Ticket)
            ontology_payload = {
                "id": class_id,
                "label": {"en": "Ticket", "ko": "티켓"},
                "description": {"en": "Ticket class (writeback verify)", "ko": "티켓 (writeback verify)"},
                "properties": [
                    {
                        "name": "ticket_id",
                        "type": "xsd:string",
                        "label": {"en": "Ticket ID", "ko": "티켓 ID"},
                        "required": True,
                        "primaryKey": True,
                    },
                    {
                        "name": "name",
                        "type": "xsd:string",
                        "label": {"en": "Name", "ko": "이름"},
                        "required": True,
                        "titleKey": True,
                    },
                    {
                        "name": "status",
                        "type": "xsd:string",
                        "label": {"en": "Status", "ko": "상태"},
                        "required": True,
                    },
                    {
                        "name": "approved_by",
                        "type": "xsd:string",
                        "label": {"en": "Approved By", "ko": "승인자"},
                        "required": False,
                    },
                ],
                "relationships": [],
                "metadata": {"source": "test_action_writeback_e2e_verification_suite"},
            }
            resp = await session.post(
                f"{BFF_URL}/api/v1/databases/{db_name}/ontology",
                headers=db_headers,
                params={"branch": base_branch},
                json=ontology_payload,
            )
            ontology_resp = await resp.json()
            assert resp.status in {200, 202}, ontology_resp
            if resp.status == 202:
                cmd = _extract_command_id(ontology_resp)
                assert cmd, ontology_resp
                await _wait_for_command_completed(session, command_id=cmd)

            # 4) Create action types (three conflict policies + one criteria-gated action).
            def _action_type_payload(*, action_id: str, conflict_policy: str, submission_criteria: Optional[str]) -> Dict[str, Any]:
                spec: Dict[str, Any] = {
                    "input_schema": {
                        "fields": [{"name": "ticket", "type": "object_ref", "required": True, "object_type": class_id}]
                    },
                    "permission_policy": {
                        "effect": "ALLOW",
                        "principals": ["role:Owner", "role:Editor", "role:DomainModeler"],
                    },
                    "writeback_target": {"repo": "ontology-writeback", "branch": "writeback-{db_name}"},
                    "conflict_policy": conflict_policy,
                    "implementation": {
                        "type": "template_v1",
                        "targets": [
                            {
                                "target": {"from": "input.ticket"},
                                "changes": {"set": {"status": "APPROVED", "approved_by": {"$ref": "user.id"}}},
                            }
                        ],
                    },
                }
                if submission_criteria is not None:
                    spec["submission_criteria"] = submission_criteria
                return {
                    "id": action_id,
                    "label": {"en": action_id, "ko": action_id},
                    "description": {"en": "writeback verify action", "ko": "writeback verify action"},
                    "spec": spec,
                    "metadata": {"source": "test_action_writeback_e2e_verification_suite"},
                }

            for action_payload in (
                _action_type_payload(action_id=action_fail, conflict_policy="FAIL", submission_criteria=None),
                _action_type_payload(action_id=action_merge, conflict_policy="WRITEBACK_WINS", submission_criteria=None),
                _action_type_payload(action_id=action_skip, conflict_policy="BASE_WINS", submission_criteria=None),
                _action_type_payload(
                    action_id=action_criteria,
                    conflict_policy="WRITEBACK_WINS",
                    submission_criteria="target.status == 'OPEN'",
                ),
            ):
                resp = await session.post(
                    f"{BFF_URL}/api/v1/databases/{db_name}/ontology/action-types",
                    headers=db_headers,
                    params={"branch": base_branch, "expected_head_commit": ""},
                    json=action_payload,
                )
                created_action_type = await resp.json()
                assert resp.status == 201, created_action_type

            # 5) Deploy the branch head so Actions are executable.
            head_commit = await _get_branch_head_commit(session, db_name=db_name, branch=base_branch, headers=db_headers)
            await _record_deployed_commit(db_name=db_name, target_branch=base_branch, ontology_commit_id=head_commit)

            # Helper: start/stop action-worker when needed.
            async def _start_worker() -> None:
                nonlocal action_worker_proc, started_action_worker_docker
                worker_mode = _smoke_worker_mode()
                if worker_mode == "docker":
                    build_worker = _truthy(os.getenv("ACTION_WRITEBACK_SMOKE_DOCKER_BUILD", "true"))
                    started_action_worker_docker = await _ensure_action_worker_docker_running(
                        repo_root=repo_root,
                        build=build_worker,
                    )
                    return

                action_worker_env = _action_worker_env_from_dotenv(dotenv)
                action_worker_env["ACTION_WORKER_GROUP"] = f"action-worker-verify-{uuid.uuid4().hex[:8]}"
                action_worker_proc = await _start_action_worker(env=action_worker_env, backend_dir=backend_dir)
                await asyncio.sleep(2.0)

            async def _stop_worker() -> None:
                nonlocal action_worker_proc
                worker_mode = _smoke_worker_mode()
                if worker_mode == "docker":
                    await _stop_action_worker_docker(repo_root=repo_root)
                    return
                if action_worker_proc is not None:
                    await _stop_process(action_worker_proc)
                    action_worker_proc = None

            # 6) Permission denied case (BFF gate): submit as user without db role.
            unauth_headers = _base_headers(db_name=db_name, actor_id=unauthorized_id)
            resp = await session.post(
                f"{BFF_URL}/api/v1/databases/{db_name}/actions/{action_merge}/submit",
                headers=unauth_headers,
                params={"base_branch": base_branch},
                json={
                    "input": {"ticket": {"class_id": class_id, "instance_id": f"ticket_perm_{uuid.uuid4().hex[:8]}"}},
                    "correlation_id": f"perm-{uuid.uuid4()}",
                    "metadata": {"source": "test_action_writeback_e2e_verification_suite"},
                },
            )
            assert resp.status == 403, await resp.text()

            # 7) submission_criteria=false case
            criteria_instance_id = f"ticket_criteria_{uuid.uuid4().hex[:8]}"
            resp = await session.post(
                f"{BFF_URL}/api/v1/databases/{db_name}/instances/{class_id}/create",
                headers=db_headers,
                params={"branch": base_branch},
                json={
                    "data": {"ticket_id": criteria_instance_id, "name": "Criteria Ticket", "status": "CLOSED"},
                    "metadata": {"kind": "ingest", "source": "test_action_writeback_e2e_verification_suite"},
                },
            )
            instance_cmd = await resp.json()
            assert resp.status == 202, instance_cmd
            cmd_id = str(instance_cmd.get("command_id") or "").strip()
            assert cmd_id, instance_cmd
            await _wait_for_command_completed(session, command_id=cmd_id)

            await _stop_worker()
            resp = await session.post(
                f"{BFF_URL}/api/v1/databases/{db_name}/actions/{action_criteria}/submit",
                headers=db_headers,
                params={"base_branch": base_branch},
                json={
                    "input": {"ticket": {"class_id": class_id, "instance_id": criteria_instance_id}},
                    "correlation_id": f"criteria-{uuid.uuid4()}",
                    "metadata": {"source": "test_action_writeback_e2e_verification_suite"},
                },
            )
            submit_payload = await resp.json()
            assert resp.status == 202, submit_payload
            criteria_action_log_id = str(submit_payload.get("action_log_id") or "").strip()
            assert criteria_action_log_id, submit_payload

            await _start_worker()
            criteria_log = await _wait_for_action_log(
                session,
                db_name=db_name,
                action_log_id=criteria_action_log_id,
                headers=db_headers,
            )
            criteria_rec = criteria_log.get("data") if isinstance(criteria_log.get("data"), dict) else {}
            assert str(criteria_rec.get("status") or "").upper() == "FAILED", criteria_log
            criteria_result = criteria_rec.get("result") if isinstance(criteria_rec.get("result"), dict) else {}
            assert str(criteria_result.get("error") or "").strip() == "submission_criteria_failed", criteria_log
            assert not str(criteria_rec.get("writeback_commit_id") or "").strip(), criteria_log
            assert not str(criteria_rec.get("action_applied_event_id") or "").strip(), criteria_log

            await _stop_worker()

            # 8) Conflict policy branches: FAIL / WRITEBACK_WINS (merge) / BASE_WINS (skip)
            from shared.config.app_config import AppConfig
            from shared.config.settings import get_settings
            from shared.services.storage.lakefs_storage_service import create_lakefs_storage_service
            from shared.utils.writeback_paths import (
                queue_entry_key,
                ref_key,
                snapshot_latest_pointer_key,
                snapshot_manifest_key,
                snapshot_object_key,
            )

            lakefs_branch = AppConfig.get_ontology_writeback_branch(db_name)
            settings = get_settings()
            lakefs_storage = create_lakefs_storage_service(settings)
            assert lakefs_storage is not None

            async def _run_conflict_case(
                *,
                action_type_id: str,
                expect_failed: bool,
                expect_queue: bool,
                expect_resolution: str,
            ) -> Dict[str, Any]:
                instance_id = f"ticket_conflict_{uuid.uuid4().hex[:8]}"
                # Create base instance at seq=1.
                resp = await session.post(
                    f"{BFF_URL}/api/v1/databases/{db_name}/instances/{class_id}/create",
                    headers=db_headers,
                    params={"branch": base_branch},
                    json={
                        "data": {"ticket_id": instance_id, "name": "Conflict Ticket", "status": "OPEN"},
                        "metadata": {"kind": "ingest", "source": "test_action_writeback_e2e_verification_suite"},
                    },
                )
                instance_cmd = await resp.json()
                assert resp.status == 202, instance_cmd
                cmd_id = str(instance_cmd.get("command_id") or "").strip()
                assert cmd_id, instance_cmd
                await _wait_for_command_completed(session, command_id=cmd_id)

                # Enqueue action (worker stopped so we can create a deterministic conflict window).
                await _stop_worker()
                resp = await session.post(
                    f"{BFF_URL}/api/v1/databases/{db_name}/actions/{action_type_id}/submit",
                    headers=db_headers,
                    params={"base_branch": base_branch},
                    json={
                        "input": {"ticket": {"class_id": class_id, "instance_id": instance_id}},
                        "correlation_id": f"conflict-{uuid.uuid4()}",
                        "metadata": {"source": "test_action_writeback_e2e_verification_suite"},
                    },
                )
                submit_payload = await resp.json()
                assert resp.status == 202, submit_payload
                action_log_id = str(submit_payload.get("action_log_id") or "").strip()
                assert action_log_id, submit_payload

                # Mutate base state on the touched field (status) so overlap is detected.
                await _update_instance_ingest(
                    session,
                    db_name=db_name,
                    base_branch=base_branch,
                    class_id=class_id,
                    instance_id=instance_id,
                    expected_seq=None,
                    patch={"status": "UPDATED_BY_INGEST"},
                    headers=db_headers,
                )

                # Now run the worker to process the queued ActionCommand.
                await _start_worker()
                log_payload = await _wait_for_action_log(
                    session,
                    db_name=db_name,
                    action_log_id=action_log_id,
                    headers=db_headers,
                )

                rec = log_payload.get("data") if isinstance(log_payload.get("data"), dict) else {}
                status_value = str(rec.get("status") or "").upper()
                if expect_failed:
                    assert status_value == "FAILED", log_payload
                else:
                    assert status_value == "SUCCEEDED", log_payload

                result = rec.get("result") if isinstance(rec.get("result"), dict) else {}
                conflicts = result.get("conflicts")
                assert isinstance(conflicts, list) and conflicts, log_payload
                assert str(conflicts[0].get("resolution") or "").upper() == str(expect_resolution).upper(), log_payload

                writeback_commit_id = str(rec.get("writeback_commit_id") or "").strip()
                writeback_target = rec.get("writeback_target") if isinstance(rec.get("writeback_target"), dict) else {}
                overlay_branch = str(writeback_target.get("branch") or "").strip()

                if expect_failed:
                    assert not writeback_commit_id, log_payload
                    return {
                        "action_log_id": action_log_id,
                        "instance_id": instance_id,
                        "lifecycle_id": _pick_first_lifecycle_id_any(log_payload),
                    }

                assert overlay_branch, log_payload
                assert writeback_commit_id, log_payload

                lifecycle_id = _pick_first_lifecycle_id_any(log_payload)
                if str(expect_resolution).upper() != "SKIPPED":
                    overlay_index = get_instances_index_name(db_name, branch=overlay_branch)
                    doc_id = overlay_doc_id(instance_id=instance_id, lifecycle_id=lifecycle_id)
                    overlay_doc = await _wait_for_es_overlay_doc(
                        es_base_url=es_base_url,
                        index_name=overlay_index,
                        doc_id=doc_id,
                    )
                    assert str(overlay_doc.get("patchset_commit_id") or "").strip() == writeback_commit_id

                action_applied_seq = rec.get("action_applied_seq")
                assert isinstance(action_applied_seq, int), log_payload

                queue_key = ref_key(
                    overlay_branch,
                    queue_entry_key(
                        object_type=class_id,
                        instance_id=instance_id,
                        lifecycle_id=lifecycle_id,
                        action_applied_seq=int(action_applied_seq),
                        action_log_id=action_log_id,
                    ),
                )
                if expect_queue:
                    queue_entry = await lakefs_storage.load_json(bucket="ontology-writeback", key=queue_key)
                    assert str(queue_entry.get("patchset_commit_id") or "").strip() == writeback_commit_id
                else:
                    with pytest.raises(FileNotFoundError):
                        await lakefs_storage.load_json(bucket="ontology-writeback", key=queue_key)

                return {
                    "action_log_id": action_log_id,
                    "instance_id": instance_id,
                    "lifecycle_id": lifecycle_id,
                    "overlay_branch": overlay_branch,
                    "writeback_commit_id": writeback_commit_id,
                    "action_applied_seq": int(action_applied_seq),
                }

            await _run_conflict_case(
                action_type_id=action_fail,
                expect_failed=True,
                expect_queue=False,
                expect_resolution="REJECTED",
            )
            await _run_conflict_case(
                action_type_id=action_skip,
                expect_failed=False,
                expect_queue=False,
                expect_resolution="SKIPPED",
            )
            merge_case = await _run_conflict_case(
                action_type_id=action_merge,
                expect_failed=False,
                expect_queue=True,
                expect_resolution="APPLIED",
            )

            # 9) BFF read-path overlay behavior check (merge case).
            resp = await session.get(
                f"{BFF_URL}/api/v1/databases/{db_name}/class/{class_id}/instance/{merge_case['instance_id']}",
                headers=db_headers,
                params={"base_branch": base_branch, "overlay_branch": merge_case["overlay_branch"]},
            )
            read_payload = await resp.json()
            assert resp.status == 200, read_payload
            data = read_payload.get("data") if isinstance(read_payload, dict) else None
            assert isinstance(data, dict), read_payload
            assert isinstance(data.get("data"), dict)
            assert data["data"].get("status") == "APPROVED"
            assert data["data"].get("approved_by") == owner_id

            # Best-effort: overlay metadata is expected to be present unless masked by access policies.
            if data.get("patchset_commit_id") is not None:
                assert str(data.get("patchset_commit_id") or "").strip() == merge_case["writeback_commit_id"]
            if data.get("action_log_id") is not None:
                assert str(data.get("action_log_id") or "").strip() == merge_case["action_log_id"]

            # 10) Materializer snapshot verification (merge case must be present in snapshot).
            os.environ["WRITEBACK_MATERIALIZER_BASE_BRANCH"] = base_branch
            from writeback_materializer_worker.main import WritebackMaterializerWorker

            materializer = WritebackMaterializerWorker()
            await materializer.initialize()
            try:
                await materializer.materialize_db(db_name=db_name)
            finally:
                await materializer.shutdown()

            latest_ptr = await lakefs_storage.load_json(
                bucket="ontology-writeback",
                key=ref_key(lakefs_branch, snapshot_latest_pointer_key()),
            )
            snapshot_id = str(latest_ptr.get("snapshot_id") or "").strip()
            assert snapshot_id, latest_ptr

            manifest = await lakefs_storage.load_json(
                bucket="ontology-writeback",
                key=ref_key(lakefs_branch, snapshot_manifest_key(snapshot_id)),
            )
            assert int(manifest.get("queue_high_watermark") or 0) >= int(merge_case["action_applied_seq"])

            snapshot_obj = await lakefs_storage.load_json(
                bucket="ontology-writeback",
                key=ref_key(
                    lakefs_branch,
                    snapshot_object_key(
                        snapshot_id=snapshot_id,
                        object_type=class_id,
                        instance_id=merge_case["instance_id"],
                        lifecycle_id=merge_case["lifecycle_id"],
                    ),
                ),
            )
            assert snapshot_obj.get("status") == "APPROVED"
            assert snapshot_obj.get("approved_by") == owner_id

        finally:
            await _stop_worker()

            if _smoke_worker_mode() == "docker" and started_action_worker_docker and not _truthy(
                os.getenv("ACTION_WRITEBACK_SMOKE_KEEP_WORKER")
            ):
                await _stop_action_worker_docker(repo_root=repo_root)

            if old_materializer_base_branch is None:
                os.environ.pop("WRITEBACK_MATERIALIZER_BASE_BRANCH", None)
            else:
                os.environ["WRITEBACK_MATERIALIZER_BASE_BRANCH"] = old_materializer_base_branch

            if created_db and _truthy(os.getenv("ACTION_WRITEBACK_SMOKE_CLEANUP", "true")):
                db_headers = _base_headers(db_name=db_name, actor_id=owner_id)
                resp = await session.delete(f"{BFF_URL}/api/v1/databases/{db_name}", headers=db_headers)
                if resp.status == 202:
                    payload = await resp.json()
                    cmd = _extract_command_id(payload)
                    if cmd:
                        with contextlib.suppress(Exception):
                            await _wait_for_command_completed(session, command_id=cmd, timeout_seconds=240)
