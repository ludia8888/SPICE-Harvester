"""
AccessPolicy + LinkIndexing E2E coverage (no mocks).

These tests drive real OMS/BFF endpoints and workers to validate
row/column access policies, link indexing, and object type migration gates.
"""

from __future__ import annotations

import asyncio
import csv
import io
import os
import time
import uuid
from typing import Any, Dict, Iterable, List, Optional, Tuple

import asyncpg
import aiohttp
import pytest
import pytest_asyncio

from shared.config.settings import ApplicationSettings
from shared.config.service_config import ServiceConfig
from shared.services.dataset_registry import DatasetRegistry
from shared.services.event_store import EventStore
from shared.services.lakefs_storage_service import create_lakefs_storage_service
from shared.utils.s3_uri import build_s3_uri
from tests.utils.auth import bff_auth_headers, oms_auth_headers


BFF_URL = (os.getenv("BFF_BASE_URL") or os.getenv("BFF_URL") or "http://localhost:8002").rstrip("/")
OMS_URL = (os.getenv("OMS_BASE_URL") or os.getenv("OMS_URL") or "http://localhost:8000").rstrip("/")
LAKEFS_REPO = (os.getenv("LAKEFS_RAW_REPOSITORY") or "raw-datasets").strip()
DEFAULT_TIMEOUT = float(os.getenv("E2E_TIMEOUT_SECONDS", "120") or 120)
_LAKEFS_READY: set[str] = set()


def _load_repo_dotenv() -> Dict[str, str]:
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[2]
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


def _ensure_lakefs_credentials() -> None:
    dotenv = _load_repo_dotenv()
    access = (os.getenv("LAKEFS_ACCESS_KEY_ID") or dotenv.get("LAKEFS_ACCESS_KEY_ID") or "").strip()
    secret = (os.getenv("LAKEFS_SECRET_ACCESS_KEY") or dotenv.get("LAKEFS_SECRET_ACCESS_KEY") or "").strip()
    if not access:
        access = (os.getenv("MINIO_ACCESS_KEY") or "minioadmin").strip()
    if not secret:
        secret = (os.getenv("MINIO_SECRET_KEY") or "minioadmin123").strip()
    os.environ.setdefault("LAKEFS_ACCESS_KEY_ID", access)
    os.environ.setdefault("LAKEFS_SECRET_ACCESS_KEY", secret)

    api_url = (os.getenv("LAKEFS_API_URL") or dotenv.get("LAKEFS_API_URL") or "").strip()
    if not api_url:
        port = (
            os.getenv("LAKEFS_API_PORT")
            or dotenv.get("LAKEFS_API_PORT")
            or dotenv.get("LAKEFS_PORT_HOST")
            or "8000"
        )
        api_url = f"http://127.0.0.1:{port}"
    os.environ.setdefault("LAKEFS_API_URL", api_url)
    os.environ.setdefault("LAKEFS_S3_ENDPOINT_URL", api_url)


def _lakefs_admin_credentials() -> Tuple[str, str]:
    dotenv = _load_repo_dotenv()
    access = (
        os.getenv("LAKEFS_INSTALLATION_ACCESS_KEY_ID")
        or dotenv.get("LAKEFS_INSTALLATION_ACCESS_KEY_ID")
        or os.getenv("LAKEFS_ACCESS_KEY_ID")
        or dotenv.get("LAKEFS_ACCESS_KEY_ID")
        or "spice-lakefs-admin"
    ).strip()
    secret = (
        os.getenv("LAKEFS_INSTALLATION_SECRET_ACCESS_KEY")
        or dotenv.get("LAKEFS_INSTALLATION_SECRET_ACCESS_KEY")
        or os.getenv("LAKEFS_SECRET_ACCESS_KEY")
        or dotenv.get("LAKEFS_SECRET_ACCESS_KEY")
        or "spice-lakefs-admin-secret"
    ).strip()
    return access, secret


async def _ensure_lakefs_repository(*, repository: str, branch: str) -> None:
    _ensure_lakefs_credentials()
    repo_key = f"{repository}:{branch}"
    if repo_key in _LAKEFS_READY:
        return

    base_url = (os.getenv("LAKEFS_API_URL") or "").strip().rstrip("/")
    access, secret = _lakefs_admin_credentials()
    auth = aiohttp.BasicAuth(access, secret)
    async with aiohttp.ClientSession(auth=auth) as session:
        async with session.get(f"{base_url}/api/v1/repositories/{repository}") as resp:
            if resp.status not in {200, 404}:
                raise AssertionError(await resp.text())
            if resp.status == 404:
                payload = {
                    "name": repository,
                    "storage_namespace": f"s3://lakefs/{repository}",
                    "default_branch": "main",
                }
                async with session.post(f"{base_url}/api/v1/repositories", json=payload) as create_resp:
                    if create_resp.status not in {201, 409}:
                        raise AssertionError(await create_resp.text())

        if branch and branch != "main":
            async with session.get(
                f"{base_url}/api/v1/repositories/{repository}/branches/{branch}"
            ) as resp:
                if resp.status == 404:
                    payload = {"name": branch, "source": "main"}
                    async with session.post(
                        f"{base_url}/api/v1/repositories/{repository}/branches", json=payload
                    ) as create_branch_resp:
                        if create_branch_resp.status not in {201, 409}:
                            raise AssertionError(await create_branch_resp.text())
                elif resp.status != 200:
                    raise AssertionError(await resp.text())

    _LAKEFS_READY.add(repo_key)


async def _release_stale_processing_events(*, max_age_seconds: int = 60) -> None:
    dsn = ServiceConfig.get_postgres_url()
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            """
            UPDATE spice_event_registry.processed_events
               SET status = 'skipped_stale',
                   processed_at = NOW(),
                   heartbeat_at = NOW(),
                   last_error = 'test_cleanup_stale_processing'
             WHERE status = 'processing'
               AND started_at < NOW() - ($1 * INTERVAL '1 second')
               AND aggregate_id LIKE 'e2e_%'
            """,
            int(max_age_seconds),
        )
    finally:
        await conn.close()


@pytest_asyncio.fixture(autouse=True)
async def _cleanup_stale_processing_events() -> None:
    await _release_stale_processing_events()


async def _wait_for_db_exists(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    expected: bool,
    timeout_seconds: float = DEFAULT_TIMEOUT,
    poll_interval_seconds: float = 2.0,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last: Optional[dict] = None
    while time.monotonic() < deadline:
        async with session.get(f"{OMS_URL}/api/v1/database/exists/{db_name}") as resp:
            if resp.status == 200:
                last = await resp.json()
                if (last.get("data") or {}).get("exists") is expected:
                    return
        await asyncio.sleep(poll_interval_seconds)
    raise AssertionError(f"Timed out waiting for db exists={expected} (last={last})")


async def _wait_for_command_completed(
    session: aiohttp.ClientSession,
    *,
    command_id: str,
    timeout_seconds: float = DEFAULT_TIMEOUT,
    poll_interval_seconds: float = 2.0,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last: Optional[dict] = None
    while time.monotonic() < deadline:
        async with session.get(f"{OMS_URL}/api/v1/commands/{command_id}/status") as resp:
            if resp.status != 200:
                last = {"status": resp.status, "body": await resp.text()}
                await asyncio.sleep(poll_interval_seconds)
                continue
            last = await resp.json()
        status_value = str(last.get("status") or "").upper()
        if status_value in {"COMPLETED", "FAILED", "CANCELLED"}:
            if status_value != "COMPLETED":
                raise AssertionError(f"Command {command_id} ended in {status_value}: {last}")
            return
        await asyncio.sleep(poll_interval_seconds)
    raise AssertionError(f"Timed out waiting for command completion (command_id={command_id}, last={last})")


async def _wait_for_ontology_present(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    ontology_id: str,
    branch: str = "main",
    timeout_seconds: float = DEFAULT_TIMEOUT,
    poll_interval_seconds: float = 1.0,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last: Optional[dict] = None
    while time.monotonic() < deadline:
        async with session.get(
            f"{OMS_URL}/api/v1/database/{db_name}/ontology",
            params={"branch": branch},
        ) as resp:
            if resp.status != 200:
                await asyncio.sleep(poll_interval_seconds)
                continue
            last = await resp.json()
            ontologies = (last.get("data") or {}).get("ontologies") or []
            if any(o.get("id") == ontology_id for o in ontologies):
                return
        await asyncio.sleep(poll_interval_seconds)
    raise AssertionError(f"Timed out waiting for ontology {ontology_id} (last={last})")


async def _get_head_commit(session: aiohttp.ClientSession, *, db_name: str, branch: str = "main") -> str:
    async with session.get(
        f"{OMS_URL}/api/v1/version/{db_name}/head",
        params={"branch": branch},
    ) as resp:
        assert resp.status == 200, await resp.text()
        payload = await resp.json()
        head_commit = ((payload.get("data") or {}).get("head_commit_id") or "").strip()
        assert head_commit
        return head_commit


async def _create_db(session: aiohttp.ClientSession, *, db_name: str) -> None:
    async with session.post(
        f"{OMS_URL}/api/v1/database/create",
        json={"name": db_name, "description": "e2e"},
    ) as resp:
        assert resp.status == 202, await resp.text()
        payload = await resp.json()
        command_id = (payload.get("data") or {}).get("command_id") or payload.get("command_id")
        if command_id:
            await _wait_for_command_completed(session, command_id=str(command_id))
    await _wait_for_db_exists(session, db_name=db_name, expected=True)


async def _create_ontology(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    ontology: Dict[str, Any],
    branch: str = "main",
) -> None:
    async with session.post(
        f"{OMS_URL}/api/v1/database/{db_name}/ontology",
        params={"branch": branch},
        json=ontology,
    ) as resp:
        assert resp.status == 202, await resp.text()
        payload = await resp.json()
        command_id = (payload.get("data") or {}).get("command_id") or payload.get("command_id")
        if command_id:
            await _wait_for_command_completed(session, command_id=str(command_id))
    await _wait_for_ontology_present(
        session,
        db_name=db_name,
        ontology_id=str(ontology.get("id")),
        branch=branch,
    )


async def _create_branch(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    branch: str,
    from_branch: str = "main",
) -> None:
    async with session.post(
        f"{OMS_URL}/api/v1/branch/{db_name}/create",
        json={"branch_name": branch, "from_branch": from_branch},
    ) as resp:
        assert resp.status in {200, 201, 409}, await resp.text()


async def _checkout_branch(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    branch: str,
) -> None:
    async with session.post(
        f"{OMS_URL}/api/v1/branch/{db_name}/checkout",
        json={"target": branch, "target_type": "branch"},
    ) as resp:
        assert resp.status == 200, await resp.text()


async def _wait_for_instance_count(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    class_id: str,
    expected_count: int,
    timeout_seconds: float = DEFAULT_TIMEOUT,
    poll_interval_seconds: float = 1.5,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last: Optional[dict] = None
    while time.monotonic() < deadline:
        async with session.get(f"{OMS_URL}/api/v1/instance/{db_name}/class/{class_id}/count") as resp:
            if resp.status != 200:
                await asyncio.sleep(poll_interval_seconds)
                continue
            last = await resp.json()
            if int(last.get("count") or 0) >= expected_count:
                return
        await asyncio.sleep(poll_interval_seconds)
    raise AssertionError(
        f"Timed out waiting for instance count >= {expected_count} for {class_id} (last={last})"
    )


async def _create_instance(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    class_id: str,
    payload: Dict[str, Any],
    branch: str = "main",
) -> None:
    async with session.post(
        f"{OMS_URL}/api/v1/instances/{db_name}/async/{class_id}/create",
        params={"branch": branch},
        json={"data": payload},
    ) as resp:
        assert resp.status == 202, await resp.text()
        data = await resp.json()
        command_id = data.get("command_id") or (data.get("data") or {}).get("command_id")
        if command_id:
            await _wait_for_command_completed(session, command_id=str(command_id))


def _render_csv(headers: Iterable[str], rows: List[Dict[str, Any]]) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    header_list = list(headers)
    writer.writerow(header_list)
    for row in rows:
        writer.writerow([row.get(col) for col in header_list])
    return output.getvalue()


async def _create_dataset_with_artifact(
    dataset_registry: DatasetRegistry,
    *,
    db_name: str,
    name: str,
    columns: List[Dict[str, Any]],
    rows: List[Dict[str, Any]],
    branch: str = "main",
) -> Tuple[Any, Any]:
    _ensure_lakefs_credentials()
    await _ensure_lakefs_repository(repository=LAKEFS_REPO, branch=branch)
    dataset = await dataset_registry.create_dataset(
        db_name=db_name,
        name=name,
        description=f"{name} dataset",
        source_type="manual",
        source_ref=None,
        schema_json={"columns": columns},
        branch=branch,
    )

    storage = create_lakefs_storage_service(ApplicationSettings())
    if storage is None:
        raise AssertionError("LakeFS storage service unavailable")

    headers = [str(col.get("name") or "") for col in columns if col.get("name")]
    csv_payload = _render_csv(headers, rows).encode("utf-8")
    prefix = f"e2e/{dataset.dataset_id}"
    object_key = f"{branch}/{prefix}/data.csv"
    await storage.save_bytes(LAKEFS_REPO, object_key, csv_payload, content_type="text/csv")
    artifact_key = build_s3_uri(LAKEFS_REPO, object_key)

    sample_json = {"columns": columns, "rows": rows}
    version = await dataset_registry.add_version(
        dataset_id=dataset.dataset_id,
        lakefs_commit_id=branch,
        artifact_key=artifact_key,
        row_count=len(rows),
        sample_json=sample_json,
        schema_json={"columns": columns},
    )
    return dataset, version


async def _wait_for_link_index_status(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    link_type_id: str,
    branch: str = "main",
    expected_status: str = "PASS",
    timeout_seconds: float = DEFAULT_TIMEOUT,
    poll_interval_seconds: float = 2.0,
) -> Dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last: Optional[dict] = None
    expected_status = expected_status.upper()
    while time.monotonic() < deadline:
        async with session.get(
            f"{BFF_URL}/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}",
            params={"branch": branch},
        ) as resp:
            if resp.status != 200:
                await asyncio.sleep(poll_interval_seconds)
                continue
            last = await resp.json()
            relationship_spec = ((last.get("data") or {}).get("relationship_spec") or {}) if isinstance(last, dict) else {}
            status_value = str(relationship_spec.get("last_index_status") or "").upper()
            if status_value == expected_status:
                return relationship_spec
        await asyncio.sleep(poll_interval_seconds)
    raise AssertionError(f"Timed out waiting for link index status {expected_status} (last={last})")


async def _wait_for_relationship(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    class_id: str,
    pk_field: str,
    pk_value: str,
    predicate: str,
    branch: str = "main",
    timeout_seconds: float = DEFAULT_TIMEOUT,
    poll_interval_seconds: float = 2.0,
) -> Dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last: Optional[dict] = None
    while time.monotonic() < deadline:
        async with session.post(
            f"{OMS_URL}/api/v1/database/{db_name}/ontology/query",
            params={"branch": branch},
            json={
                "class_id": class_id,
                "filters": [{"field": pk_field, "operator": "eq", "value": pk_value}],
                "limit": 5,
            },
        ) as resp:
            if resp.status != 200:
                await asyncio.sleep(poll_interval_seconds)
                continue
            last = await resp.json()
            data = last.get("data") if isinstance(last, dict) else None
            if isinstance(data, list):
                for doc in data:
                    if isinstance(doc, dict) and predicate in doc:
                        return doc
        await asyncio.sleep(poll_interval_seconds)
    raise AssertionError(f"Timed out waiting for relationship {predicate} on {class_id} (last={last})")


async def _wait_for_instance_edits(
    dataset_registry: DatasetRegistry,
    *,
    db_name: str,
    class_id: str,
    min_count: int = 1,
    timeout_seconds: float = 30,
    poll_interval_seconds: float = 1.0,
) -> int:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        count = await dataset_registry.count_instance_edits(db_name=db_name, class_id=class_id)
        if count >= min_count:
            return count
        await asyncio.sleep(poll_interval_seconds)
    raise AssertionError("Timed out waiting for instance edit records")


async def _get_write_side_last_sequence(*, aggregate_type: str, aggregate_id: str) -> Optional[int]:
    import asyncpg
    import re
    from urllib.parse import urlparse

    schema = os.getenv("EVENT_STORE_SEQUENCE_SCHEMA", "spice_event_registry")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        return None

    prefix = (os.getenv("EVENT_STORE_SEQUENCE_HANDLER_PREFIX", "write_side") or "write_side").strip()
    handler = f"{prefix}:{aggregate_type}"

    candidates = []
    if (os.getenv("POSTGRES_URL") or "").strip():
        candidates.append(os.getenv("POSTGRES_URL"))
    candidates.extend(
        [
            "postgresql://spiceadmin:spicepass123@localhost:55433/spicedb",
            "postgresql://spiceadmin:spicepass123@localhost:5432/spicedb",
        ]
    )

    for dsn in candidates:
        try:
            parsed = urlparse(dsn)
            conn = await asyncpg.connect(
                host=parsed.hostname or "localhost",
                port=parsed.port or 5432,
                user=parsed.username or "spiceadmin",
                password=parsed.password or "spicepass123",
                database=(parsed.path or "/spicedb").lstrip("/") or "spicedb",
            )
        except Exception:
            continue
        try:
            row = await conn.fetchrow(
                f"""
                SELECT last_sequence
                FROM {schema}.sequence_registry
                WHERE handler = $1 AND aggregate_id = $2
                """,
                handler,
                aggregate_id,
            )
            if row:
                return int(row["last_sequence"])
        except asyncpg.UndefinedTableError:
            return None
        except asyncpg.InvalidSchemaNameError:
            return None
        finally:
            await conn.close()
    return None


async def _delete_db_best_effort(session: aiohttp.ClientSession, *, db_name: str) -> None:
    expected_seq = await _get_write_side_last_sequence(aggregate_type="Database", aggregate_id=db_name)
    if expected_seq is None:
        async with session.delete(
            f"{OMS_URL}/api/v1/database/{db_name}",
            params={"expected_seq": 0},
        ):
            return
    else:
        async with session.delete(
            f"{OMS_URL}/api/v1/database/{db_name}",
            params={"expected_seq": expected_seq},
        ) as resp:
            if resp.status not in {200, 202, 404}:
                raise AssertionError(await resp.text())
        await _wait_for_db_exists(session, db_name=db_name, expected=False)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_access_policy_filters_and_masks_query_results() -> None:
    suffix = uuid.uuid4().hex[:12]
    db_name = f"e2e_access_{suffix}"
    class_id = "PolicyClass"
    branch = "main"

    async with aiohttp.ClientSession(headers=oms_auth_headers()) as oms_session, aiohttp.ClientSession(
        headers=bff_auth_headers()
    ) as bff_session:
        try:
            await _create_db(oms_session, db_name=db_name)

            await _create_ontology(
                oms_session,
                db_name=db_name,
                branch=branch,
                ontology={
                    "id": class_id,
                    "label": "PolicyClass",
                    "description": "Access policy test class",
                    "properties": [
                        {
                            "name": "policyclass_id",
                            "type": "string",
                            "label": "ID",
                            "required": True,
                            "titleKey": True,
                        },
                        {"name": "email", "type": "string", "label": "Email", "required": True},
                        {"name": "status", "type": "string", "label": "Status", "required": True},
                    ],
                    "relationships": [],
                },
            )

            await _create_instance(
                oms_session,
                db_name=db_name,
                class_id=class_id,
                payload={"policyclass_id": "a1", "email": "allow@example.com", "status": "active"},
                branch=branch,
            )
            await _create_instance(
                oms_session,
                db_name=db_name,
                class_id=class_id,
                payload={"policyclass_id": "b1", "email": "deny@example.com", "status": "inactive"},
                branch=branch,
            )
            await _wait_for_instance_count(
                oms_session,
                db_name=db_name,
                class_id=class_id,
                expected_count=2,
            )

            async with bff_session.post(
                f"{BFF_URL}/api/v1/access-policies",
                json={
                    "db_name": db_name,
                    "scope": "data_access",
                    "subject_type": "object_type",
                    "subject_id": class_id,
                    "policy": {
                        "row_filters": [{"field": "status", "op": "eq", "value": "active"}],
                        "filter_operator": "and",
                        "filter_mode": "allow",
                        "mask_columns": ["email"],
                        "mask_value": None,
                    },
                    "status": "ACTIVE",
                },
                headers={"X-DB-Name": db_name},
            ) as resp:
                assert resp.status == 200, await resp.text()

            async with bff_session.post(
                f"{BFF_URL}/api/v1/databases/{db_name}/query/raw",
                json={"type": "select", "class_id": class_id, "limit": 20},
                headers={"X-DB-Name": db_name},
            ) as resp:
                assert resp.status == 200, await resp.text()
                payload = await resp.json()
                data = payload.get("data") if isinstance(payload, dict) else None
                assert isinstance(data, list)
                assert len(data) == 1
                row = data[0]
                assert row.get("status") == "active"
                assert row.get("email") is None
        finally:
            await _delete_db_best_effort(oms_session, db_name=db_name)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_link_indexing_updates_relationships_and_status() -> None:
    suffix = uuid.uuid4().hex[:12]
    db_name = f"e2e_link_{suffix}"
    branch = f"e2e_{suffix}"
    source_class = "Source"
    target_class = "Target"
    predicate = "related_to"

    dataset_registry = DatasetRegistry()
    await dataset_registry.initialize()

    async with aiohttp.ClientSession(headers=oms_auth_headers()) as oms_session, aiohttp.ClientSession(
        headers=bff_auth_headers()
    ) as bff_session:
        try:
            await _create_db(oms_session, db_name=db_name)
            await _create_branch(oms_session, db_name=db_name, branch=branch)
            await _checkout_branch(oms_session, db_name=db_name, branch=branch)

            await _create_ontology(
                oms_session,
                db_name=db_name,
                branch=branch,
                ontology={
                    "id": target_class,
                    "label": "Target",
                    "description": "Target class",
                    "properties": [
                        {
                            "name": "target_id",
                            "type": "string",
                            "label": "Target ID",
                            "required": True,
                            "titleKey": True,
                        },
                        {"name": "label", "type": "string", "label": "Label", "required": False},
                    ],
                    "relationships": [],
                },
            )

            await _create_ontology(
                oms_session,
                db_name=db_name,
                branch=branch,
                ontology={
                    "id": source_class,
                    "label": "Source",
                    "description": "Source class",
                    "properties": [
                        {
                            "name": "source_id",
                            "type": "string",
                            "label": "Source ID",
                            "required": True,
                            "titleKey": True,
                        },
                        {"name": "name", "type": "string", "label": "Name", "required": False},
                    ],
                    "relationships": [
                        {
                            "predicate": predicate,
                            "target": target_class,
                            "label": "Related To",
                            "cardinality": "n:m",
                        }
                    ],
                },
            )

            source_columns = [{"name": "source_id", "type": "xsd:string"}]
            target_columns = [{"name": "target_id", "type": "xsd:string"}]
            join_columns = [
                {"name": "source_id", "type": "xsd:string"},
                {"name": "target_id", "type": "xsd:string"},
            ]
            source_dataset, source_version = await _create_dataset_with_artifact(
                dataset_registry,
                db_name=db_name,
                name="source_ds",
                columns=source_columns,
                rows=[{"source_id": "s1"}],
                branch=branch,
            )
            target_dataset, target_version = await _create_dataset_with_artifact(
                dataset_registry,
                db_name=db_name,
                name="target_ds",
                columns=target_columns,
                rows=[{"target_id": "t1"}],
                branch=branch,
            )
            join_dataset, join_version = await _create_dataset_with_artifact(
                dataset_registry,
                db_name=db_name,
                name="join_ds",
                columns=join_columns,
                rows=[{"source_id": "s1", "target_id": "t1"}],
                branch=branch,
            )

            head_commit = await _get_head_commit(oms_session, db_name=db_name, branch=branch)
            async with bff_session.post(
                f"{BFF_URL}/api/v1/databases/{db_name}/ontology/object-types",
                params={"expected_head_commit": head_commit, "branch": branch},
                json={
                    "class_id": source_class,
                    "backing_dataset_id": source_dataset.dataset_id,
                    "dataset_version_id": source_version.version_id,
                    "pk_spec": {"primary_key": ["source_id"], "title_key": ["source_id"]},
                    "status": "ACTIVE",
                },
                headers={"X-DB-Name": db_name},
            ) as resp:
                assert resp.status == 201, await resp.text()

            head_commit = await _get_head_commit(oms_session, db_name=db_name, branch=branch)
            async with bff_session.post(
                f"{BFF_URL}/api/v1/databases/{db_name}/ontology/object-types",
                params={"expected_head_commit": head_commit, "branch": branch},
                json={
                    "class_id": target_class,
                    "backing_dataset_id": target_dataset.dataset_id,
                    "dataset_version_id": target_version.version_id,
                    "pk_spec": {"primary_key": ["target_id"], "title_key": ["target_id"]},
                    "status": "ACTIVE",
                },
                headers={"X-DB-Name": db_name},
            ) as resp:
                assert resp.status == 201, await resp.text()

            await _create_instance(
                oms_session,
                db_name=db_name,
                class_id=source_class,
                payload={"source_id": "s1", "name": "Source One"},
                branch=branch,
            )
            await _create_instance(
                oms_session,
                db_name=db_name,
                class_id=target_class,
                payload={"target_id": "t1", "label": "Target One"},
                branch=branch,
            )

            head_commit = await _get_head_commit(oms_session, db_name=db_name, branch=branch)
            link_type_id = f"link_{suffix}"
            async with bff_session.post(
                f"{BFF_URL}/api/v1/databases/{db_name}/ontology/link-types",
                params={"expected_head_commit": head_commit, "branch": branch},
                json={
                    "id": link_type_id,
                    "label": "Related To",
                    "from": source_class,
                    "to": target_class,
                    "predicate": predicate,
                    "cardinality": "n:m",
                    "relationship_spec": {
                        "type": "join_table",
                        "join_dataset_id": join_dataset.dataset_id,
                        "join_dataset_version_id": join_version.version_id,
                        "source_key_column": "source_id",
                        "target_key_column": "target_id",
                        "auto_sync": True,
                    },
                    "trigger_index": True,
                },
                headers={"X-DB-Name": db_name},
            ) as resp:
                assert resp.status == 201, await resp.text()

            await _wait_for_link_index_status(
                bff_session,
                db_name=db_name,
                link_type_id=link_type_id,
                branch=branch,
                expected_status="PASS",
            )
            await _checkout_branch(oms_session, db_name=db_name, branch=branch)
            doc = await _wait_for_relationship(
                oms_session,
                db_name=db_name,
                class_id=source_class,
                pk_field="source_id",
                pk_value="s1",
                predicate=predicate,
                branch=branch,
            )
            related_value = doc.get(predicate)
            if isinstance(related_value, list):
                assert f"{target_class}/t1" in related_value
            else:
                assert related_value == f"{target_class}/t1"
        finally:
            await dataset_registry.close()
            await _delete_db_best_effort(oms_session, db_name=db_name)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_object_type_migration_requires_edit_reset() -> None:
    suffix = uuid.uuid4().hex[:12]
    db_name = f"e2e_migration_{suffix}"
    branch = f"e2e_{suffix}"
    class_id = "Customer"

    dataset_registry = DatasetRegistry()
    await dataset_registry.initialize()

    async with aiohttp.ClientSession(headers=oms_auth_headers()) as oms_session, aiohttp.ClientSession(
        headers=bff_auth_headers()
    ) as bff_session:
        try:
            await _create_db(oms_session, db_name=db_name)
            await _create_branch(oms_session, db_name=db_name, branch=branch)
            await _checkout_branch(oms_session, db_name=db_name, branch=branch)

            await _create_ontology(
                oms_session,
                db_name=db_name,
                branch=branch,
                ontology={
                    "id": class_id,
                    "label": "Customer",
                    "description": "Customer class",
                    "properties": [
                        {
                            "name": "customer_id",
                            "type": "string",
                            "label": "Customer ID",
                            "required": True,
                            "titleKey": True,
                        },
                        {"name": "name", "type": "string", "label": "Name", "required": False},
                    ],
                    "relationships": [],
                },
            )

            dataset, version = await _create_dataset_with_artifact(
                dataset_registry,
                db_name=db_name,
                name="customer_ds",
                columns=[{"name": "customer_id", "type": "xsd:string"}, {"name": "name", "type": "xsd:string"}],
                rows=[{"customer_id": "c1", "name": "Alice"}],
                branch=branch,
            )

            head_commit = await _get_head_commit(oms_session, db_name=db_name, branch=branch)
            async with bff_session.post(
                f"{BFF_URL}/api/v1/databases/{db_name}/ontology/object-types",
                params={"expected_head_commit": head_commit, "branch": branch},
                json={
                    "class_id": class_id,
                    "backing_dataset_id": dataset.dataset_id,
                    "dataset_version_id": version.version_id,
                    "pk_spec": {"primary_key": ["customer_id"], "title_key": ["customer_id"]},
                    "status": "ACTIVE",
                },
                headers={"X-DB-Name": db_name},
            ) as resp:
                assert resp.status == 201, await resp.text()

            await _create_instance(
                oms_session,
                db_name=db_name,
                class_id=class_id,
                payload={"customer_id": "c1", "name": "Alice"},
                branch=branch,
            )
            await _wait_for_instance_edits(dataset_registry, db_name=db_name, class_id=class_id, min_count=1)

            head_commit = await _get_head_commit(oms_session, db_name=db_name, branch=branch)
            async with bff_session.put(
                f"{BFF_URL}/api/v1/databases/{db_name}/ontology/object-types/{class_id}",
                params={"expected_head_commit": head_commit, "branch": branch},
                json={
                    "pk_spec": {"primary_key": ["customer_id", "name"], "title_key": ["name"]},
                    "migration": {"approved": True},
                },
                headers={"X-DB-Name": db_name},
            ) as resp:
                assert resp.status == 409, await resp.text()
                payload = await resp.json()
                detail = payload.get("detail") if isinstance(payload, dict) else None
                assert isinstance(detail, dict)
                assert detail.get("code") == "OBJECT_TYPE_EDIT_RESET_REQUIRED"

            head_commit = await _get_head_commit(oms_session, db_name=db_name, branch=branch)
            async with bff_session.put(
                f"{BFF_URL}/api/v1/databases/{db_name}/ontology/object-types/{class_id}",
                params={"expected_head_commit": head_commit, "branch": branch},
                json={
                    "pk_spec": {"primary_key": ["customer_id", "name"], "title_key": ["name"]},
                    "migration": {"approved": True, "reset_edits": True, "note": "e2e reset"},
                },
                headers={"X-DB-Name": db_name},
            ) as resp:
                assert resp.status == 200, await resp.text()
                payload = await resp.json()
                data = payload.get("data") if isinstance(payload, dict) else None
                object_type = (data or {}).get("object_type") if isinstance(data, dict) else None
                assert isinstance(object_type, dict)
                spec = object_type.get("spec") if isinstance(object_type.get("spec"), dict) else {}
                pk_spec = spec.get("pk_spec") if isinstance(spec.get("pk_spec"), dict) else {}
                assert set(pk_spec.get("primary_key") or []) == {"customer_id", "name"}

            cleared = await dataset_registry.count_instance_edits(db_name=db_name, class_id=class_id)
            assert cleared == 0
        finally:
            await dataset_registry.close()
            await _delete_db_best_effort(oms_session, db_name=db_name)
