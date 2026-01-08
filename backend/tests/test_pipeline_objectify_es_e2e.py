from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Any, Optional

import httpx
import pytest

from shared.config.search_config import get_instances_index_name
from shared.services.dataset_registry import DatasetRegistry
from shared.utils.s3_uri import parse_s3_uri


BFF_URL = (os.getenv("BFF_BASE_URL") or os.getenv("BFF_URL") or "http://localhost:8002").rstrip("/")
OMS_URL = (os.getenv("OMS_BASE_URL") or os.getenv("OMS_URL") or "http://localhost:8000").rstrip("/")
_ES_PORT = (
    os.getenv("ELASTICSEARCH_PORT")
    or os.getenv("ELASTICSEARCH_PORT_HOST")
    or "9201"
)
ELASTICSEARCH_URL = os.getenv(
    "ELASTICSEARCH_URL",
    f"http://{os.getenv('ELASTICSEARCH_HOST', 'localhost')}:{_ES_PORT}",
)
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN") or os.getenv("BFF_ADMIN_TOKEN") or "test-token"
HTTPX_TIMEOUT = float(os.getenv("PIPELINE_HTTP_TIMEOUT", "180") or 180)
RUN_TIMEOUT_SECONDS = int(os.getenv("PIPELINE_RUN_TIMEOUT_SECONDS", "420") or 420)
ES_TIMEOUT_SECONDS = int(os.getenv("PIPELINE_ES_TIMEOUT_SECONDS", "240") or 240)

if os.getenv("RUN_PIPELINE_OBJECTIFY_E2E", "").strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip(
        "RUN_PIPELINE_OBJECTIFY_E2E must be enabled for this test run. Set RUN_PIPELINE_OBJECTIFY_E2E=true.",
        allow_module_level=True,
    )


def _postgres_url_candidates() -> list[str]:
    env_url = (os.getenv("POSTGRES_URL") or "").strip()
    if env_url:
        return [env_url]
    return [
        "postgresql://spiceadmin:spicepass123@localhost:55433/spicedb",
        "postgresql://spiceadmin:spicepass123@localhost:5432/spicedb",
    ]


async def _grant_db_role(
    *,
    db_name: str,
    principal_id: str,
    role: str = "Owner",
    principal_type: str = "user",
) -> None:
    import asyncpg

    from shared.security.database_access import ensure_database_access_table

    last_error: Optional[Exception] = None
    for dsn in _postgres_url_candidates():
        try:
            conn = await asyncpg.connect(dsn)
        except Exception as exc:
            last_error = exc
            continue
        try:
            await ensure_database_access_table(conn)
            await conn.execute(
                """
                INSERT INTO database_access (
                    db_name, principal_type, principal_id, principal_name, role, created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
                ON CONFLICT (db_name, principal_type, principal_id)
                DO UPDATE SET
                    principal_name = EXCLUDED.principal_name,
                    role = EXCLUDED.role,
                    updated_at = NOW()
                """,
                db_name,
                principal_type,
                principal_id,
                principal_id,
                role,
            )
            return
        except Exception as exc:
            last_error = exc
        finally:
            await conn.close()
    if last_error:
        raise RuntimeError("Failed to grant database role for test user") from last_error


async def _post_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    json_payload: dict,
    retries: int = 5,
    retry_sleep: float = 3.0,
) -> httpx.Response:
    last_response: Optional[httpx.Response] = None
    for attempt in range(retries):
        try:
            response = await client.post(url, json=json_payload)
        except httpx.HTTPError:
            if attempt + 1 >= retries:
                raise
        else:
            if response.status_code < 500:
                return response
            last_response = response
        if attempt + 1 < retries:
            await asyncio.sleep(retry_sleep)
    if last_response is not None:
        return last_response
    raise RuntimeError("request failed without response")


async def _wait_for_command(
    client: httpx.AsyncClient,
    command_id: str,
    *,
    timeout_seconds: int = 90,
    db_name: Optional[str] = None,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Optional[str] = None
    while time.monotonic() < deadline:
        try:
            resp = await client.get(f"{BFF_URL}/api/v1/commands/{command_id}/status")
        except httpx.HTTPError as exc:
            last_error = str(exc)
            await asyncio.sleep(0.5)
            continue
        if resp.status_code == 200:
            payload = resp.json()
            status = str(payload.get("status") or (payload.get("data") or {}).get("status") or "").upper()
            if status in {"COMPLETED", "SUCCESS", "SUCCEEDED", "DONE"}:
                return
            if status in {"FAILED", "ERROR"}:
                if db_name:
                    try:
                        db_resp = await client.get(f"{BFF_URL}/api/v1/databases/{db_name}")
                    except httpx.HTTPError as exc:
                        last_error = str(exc)
                    else:
                        if db_resp.status_code == 200:
                            return
                raise AssertionError(f"Command {command_id} failed: {payload}")
        if db_name:
            try:
                db_resp = await client.get(f"{BFF_URL}/api/v1/databases/{db_name}")
            except httpx.HTTPError as exc:
                last_error = str(exc)
            else:
                if db_resp.status_code == 200:
                    return
        await asyncio.sleep(0.5)
    suffix = f" last_error={last_error}" if last_error else ""
    raise AssertionError(f"Timed out waiting for command {command_id}{suffix}")


async def _create_db_with_retry(
    client: httpx.AsyncClient,
    *,
    db_name: str,
    description: str,
) -> None:
    response = await _post_with_retry(
        client,
        f"{BFF_URL}/api/v1/databases",
        json_payload={"name": db_name, "description": description},
    )
    if response.status_code == 409:
        await _wait_for_command(client, "conflict", db_name=db_name)
        return
    response.raise_for_status()
    command_id = str(((response.json().get("data") or {}) or {}).get("command_id") or "")
    assert command_id
    await _wait_for_command(client, command_id, db_name=db_name)


async def _wait_for_ontology(
    client: httpx.AsyncClient,
    *,
    db_name: str,
    class_id: str,
    branch: str = "main",
    timeout_seconds: int = 90,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Optional[str] = None
    while time.monotonic() < deadline:
        try:
            resp = await client.get(
                f"{OMS_URL}/api/v1/database/{db_name}/ontology/{class_id}",
                params={"branch": branch},
            )
        except httpx.HTTPError as exc:
            last_error = str(exc)
            await asyncio.sleep(1.0)
            continue
        if resp.status_code == 200:
            return
        last_error = f"status={resp.status_code}"
        await asyncio.sleep(1.0)
    suffix = f" last_error={last_error}" if last_error else ""
    raise AssertionError(f"Timed out waiting for ontology {class_id}{suffix}")


async def _wait_for_run_terminal(
    client: httpx.AsyncClient,
    *,
    pipeline_id: str,
    job_id: str,
    timeout_seconds: int = RUN_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_payload: Optional[dict[str, Any]] = None
    last_error: Optional[str] = None
    while time.monotonic() < deadline:
        try:
            resp = await client.get(f"{BFF_URL}/api/v1/pipelines/{pipeline_id}/runs", params={"limit": 200})
        except httpx.HTTPError as exc:
            last_error = str(exc)
            await asyncio.sleep(1.0)
            continue
        if resp.status_code != 200:
            last_error = f"status={resp.status_code}"
            await asyncio.sleep(1.0)
            continue
        payload = resp.json()
        last_payload = payload
        runs = (payload.get("data") or {}).get("runs") or []
        run = next((item for item in runs if item.get("job_id") == job_id), None)
        if run:
            status = str(run.get("status") or "").upper()
            if status in {"SUCCESS", "FAILED", "DEPLOYED", "IGNORED"}:
                return run
        await asyncio.sleep(1.0)
    suffix = f" last_error={last_error}" if last_error else ""
    raise AssertionError(f"Timed out waiting for run job_id={job_id} (last={last_payload}){suffix}")


async def _wait_for_artifact(
    client: httpx.AsyncClient,
    *,
    pipeline_id: str,
    job_id: str,
    timeout_seconds: int = 120,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_payload: Optional[dict[str, Any]] = None
    last_error: Optional[str] = None
    while time.monotonic() < deadline:
        try:
            resp = await client.get(
                f"{BFF_URL}/api/v1/pipelines/{pipeline_id}/artifacts",
                params={"limit": 200, "mode": "build"},
            )
        except httpx.HTTPError as exc:
            last_error = str(exc)
            await asyncio.sleep(1.0)
            continue
        if resp.status_code != 200:
            last_error = f"status={resp.status_code}"
            await asyncio.sleep(1.0)
            continue
        payload = resp.json()
        last_payload = payload
        artifacts = (payload.get("data") or {}).get("artifacts") or []
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                continue
            if artifact.get("job_id") != job_id:
                continue
            if str(artifact.get("status") or "").upper() == "SUCCESS":
                return artifact
        await asyncio.sleep(1.0)
    suffix = f" last_error={last_error}" if last_error else ""
    raise AssertionError(f"Timed out waiting for artifact job_id={job_id} (last={last_payload}){suffix}")


async def _wait_for_es_doc(
    client: httpx.AsyncClient,
    *,
    index_name: str,
    doc_id: str,
    timeout_seconds: int = ES_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_error: Optional[str] = None
    while time.monotonic() < deadline:
        try:
            resp = await client.get(f"{ELASTICSEARCH_URL}/{index_name}/_doc/{doc_id}")
        except httpx.HTTPError as exc:
            last_error = str(exc)
            await asyncio.sleep(1.0)
            continue
        if resp.status_code == 200:
            payload = resp.json()
            if payload.get("found") is True or payload.get("_source"):
                return payload
        else:
            last_error = f"status={resp.status_code}"
        await asyncio.sleep(1.0)
    suffix = f" last_error={last_error}" if last_error else ""
    raise AssertionError(f"Timed out waiting for ES doc {index_name}/{doc_id}{suffix}")


def _commit_id_from_artifact(artifact_key: str) -> str:
    parsed = parse_s3_uri(artifact_key)
    if not parsed:
        raise AssertionError(f"invalid artifact_key: {artifact_key}")
    _, key = parsed
    commit_id, _, _ = key.partition("/")
    if not commit_id:
        raise AssertionError(f"artifact_key missing commit id: {artifact_key}")
    return commit_id


async def _get_head_commit(client: httpx.AsyncClient, *, db_name: str, branch: str = "main") -> str:
    resp = await client.get(f"{OMS_URL}/api/v1/version/{db_name}/head", params={"branch": branch})
    resp.raise_for_status()
    data = resp.json().get("data") or {}
    head_commit = str(
        data.get("head_commit_id")
        or data.get("commit")
        or data.get("head_commit")
        or ""
    ).strip()
    assert head_commit
    return head_commit


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pipeline_objectify_es_projection() -> None:
    """
    Full flow: raw ingest -> pipeline build -> objectify -> ES projection.
    """
    suffix = uuid.uuid4().hex[:10]
    db_name = f"e2e_pipe_{suffix}"
    raw_dataset_name = "products_raw"
    clean_dataset_name = "products_clean"
    class_id = "Product"
    ontology_branch = "e2e"
    headers = {
        "X-Admin-Token": ADMIN_TOKEN,
        "X-DB-Name": db_name,
        "X-User-ID": "pipeline-e2e",
        "X-User-Type": "user",
    }

    schema_columns = [
        {"name": "product_id", "type": "xsd:string"},
        {"name": "name", "type": "xsd:string"},
    ]
    sample_rows = [
        {"product_id": "p1", "name": "Alpha"},
        {"product_id": "p2", "name": "Beta"},
    ]
    sample_json = {"rows": sample_rows}
    schema_json = {"columns": schema_columns}

    async with httpx.AsyncClient(headers=headers, timeout=HTTPX_TIMEOUT) as client:
        await _create_db_with_retry(client, db_name=db_name, description="pipeline objectify e2e")
        await _grant_db_role(
            db_name=db_name,
            principal_id=headers["X-User-ID"],
            principal_type=headers["X-User-Type"],
        )

        ontology_payload = {
            "id": class_id,
            "label": "Product",
            "description": "Product class for pipeline objectify e2e",
            "properties": [
                {
                    "name": "product_id",
                    "type": "xsd:string",
                    "label": "Product ID",
                    "required": True,
                    "primaryKey": True,
                },
                {
                    "name": "name",
                    "type": "xsd:string",
                    "label": "Name",
                    "required": True,
                    "titleKey": True,
                },
            ],
            "relationships": [],
            "metadata": {"source": "pipeline_objectify_e2e"},
        }
        branch_resp = await client.post(
            f"{OMS_URL}/api/v1/branch/{db_name}/create",
            json={"branch_name": ontology_branch, "from_branch": "main"},
        )
        if branch_resp.status_code not in {200, 201}:
            branch_resp.raise_for_status()

        ontology_resp = await client.post(
            f"{BFF_URL}/api/v1/databases/{db_name}/ontology",
            params={"branch": ontology_branch},
            json=ontology_payload,
        )
        ontology_resp.raise_for_status()
        await _wait_for_ontology(client, db_name=db_name, class_id=class_id, branch=ontology_branch)

        create_raw = await _post_with_retry(
            client,
            f"{BFF_URL}/api/v1/pipelines/datasets",
            json_payload={
                "db_name": db_name,
                "name": raw_dataset_name,
                "description": "raw input",
                "branch": "main",
                "source_type": "manual",
                "schema_json": schema_json,
            },
        )
        create_raw.raise_for_status()
        raw_dataset = (create_raw.json().get("data") or {}).get("dataset") or {}
        raw_dataset_id = str(raw_dataset.get("dataset_id") or "")
        assert raw_dataset_id

        raw_version = await _post_with_retry(
            client,
            f"{BFF_URL}/api/v1/pipelines/datasets/{raw_dataset_id}/versions",
            json_payload={"sample_json": sample_json, "schema_json": schema_json},
        )
        raw_version.raise_for_status()

        definition_json: dict[str, Any] = {
            "nodes": [
                {
                    "id": "in1",
                    "type": "input",
                    "metadata": {"datasetId": raw_dataset_id, "datasetName": raw_dataset_name},
                },
                {
                    "id": "out1",
                    "type": "output",
                    "metadata": {"datasetName": clean_dataset_name, "outputFormat": "json"},
                },
            ],
            "edges": [{"from": "in1", "to": "out1"}],
            "parameters": [],
            "schemaContract": [
                {"column": "product_id", "type": "xsd:string", "required": True},
                {"column": "name", "type": "xsd:string", "required": True},
            ],
            "settings": {"engine": "Batch"},
        }

        create_pipeline = await client.post(
            f"{BFF_URL}/api/v1/pipelines",
            json={
                "db_name": db_name,
                "name": "raw_to_clean",
                "location": "e2e",
                "description": "raw to clean",
                "branch": "main",
                "pipeline_type": "batch",
                "definition_json": definition_json,
            },
        )
        create_pipeline.raise_for_status()
        pipeline = (create_pipeline.json().get("data") or {}).get("pipeline") or {}
        pipeline_id = str(pipeline.get("pipeline_id") or "")
        assert pipeline_id

        build_resp = await _post_with_retry(
            client,
            f"{BFF_URL}/api/v1/pipelines/{pipeline_id}/build",
            json_payload={"db_name": db_name, "node_id": "out1", "limit": 10},
        )
        build_resp.raise_for_status()
        build_job_id = str(((build_resp.json().get("data") or {}) or {}).get("job_id") or "")
        assert build_job_id

        run = await _wait_for_run_terminal(client, pipeline_id=pipeline_id, job_id=build_job_id)
        assert str(run.get("status") or "").upper() == "SUCCESS"

        artifact = await _wait_for_artifact(client, pipeline_id=pipeline_id, job_id=build_job_id)
        artifact_id = str(artifact.get("artifact_id") or "")
        assert artifact_id
        outputs = artifact.get("outputs") or []
        assert any(
            isinstance(item, dict) and item.get("dataset_name") == clean_dataset_name for item in outputs
        )

        dataset_registry = DatasetRegistry()
        await dataset_registry.initialize()
        try:
            output = next(
                (item for item in outputs if isinstance(item, dict) and item.get("dataset_name") == clean_dataset_name),
                None,
            )
            assert output
            artifact_commit_key = str(output.get("artifact_commit_key") or output.get("artifact_key") or "")
            assert artifact_commit_key
            commit_id = _commit_id_from_artifact(artifact_commit_key)
            schema_columns = output.get("columns") if isinstance(output.get("columns"), list) else schema_columns
            sample_rows = output.get("rows") if isinstance(output.get("rows"), list) else sample_rows

            clean_dataset = await dataset_registry.get_dataset_by_name(
                db_name=db_name, name=clean_dataset_name, branch="main"
            )
            if not clean_dataset:
                clean_dataset = await dataset_registry.create_dataset(
                    db_name=db_name,
                    name=clean_dataset_name,
                    description="clean output",
                    source_type="pipeline",
                    source_ref=str(pipeline_id),
                    schema_json={"columns": schema_columns},
                    branch="main",
                )
            clean_dataset_id = str(clean_dataset.dataset_id)
            clean_version = await dataset_registry.add_version(
                dataset_id=clean_dataset_id,
                lakefs_commit_id=commit_id,
                artifact_key=artifact_commit_key,
                row_count=output.get("row_count"),
                sample_json={"columns": schema_columns, "rows": sample_rows},
                schema_json={"columns": schema_columns},
                promoted_from_artifact_id=artifact_id,
            )
            clean_version_id = str(clean_version.version_id)
        finally:
            await dataset_registry.close()

        head_commit = await _get_head_commit(client, db_name=db_name, branch=ontology_branch)
        object_type_payload = {
            "class_id": class_id,
            "backing_dataset_id": clean_dataset_id,
            "pk_spec": {"primary_key": ["product_id"], "title_key": ["name"]},
        }
        object_type_resp = await client.post(
            f"{BFF_URL}/api/v1/databases/{db_name}/ontology/object-types",
            params={"branch": ontology_branch, "expected_head_commit": head_commit},
            json=object_type_payload,
        )
        if object_type_resp.status_code >= 400:
            raise AssertionError(
                f"object_type_contract failed: {object_type_resp.status_code} {object_type_resp.text}"
            )

        mapping_payload = {
            "dataset_id": clean_dataset_id,
            "artifact_output_name": clean_dataset_name,
            "target_class_id": class_id,
            "options": {"ontology_branch": ontology_branch},
            "mappings": [
                {"source_field": "product_id", "target_field": "product_id"},
                {"source_field": "name", "target_field": "name"},
            ],
        }
        mapping_resp = await client.post(f"{BFF_URL}/api/v1/objectify/mapping-specs", json=mapping_payload)
        mapping_resp.raise_for_status()
        mapping_spec = (mapping_resp.json().get("data") or {}).get("mapping_spec") or {}
        mapping_spec_id = str(mapping_spec.get("mapping_spec_id") or "")
        assert mapping_spec_id

        run_objectify = await client.post(
            f"{BFF_URL}/api/v1/objectify/datasets/{clean_dataset_id}/run",
            json={"dataset_version_id": clean_version_id},
        )
        run_objectify.raise_for_status()
        objectify_job_id = str(((run_objectify.json().get("data") or {}) or {}).get("job_id") or "")
        assert objectify_job_id

        index_name = get_instances_index_name(db_name, branch=ontology_branch)
        doc = await _wait_for_es_doc(client, index_name=index_name, doc_id="p1")
        source = doc.get("_source") or {}
        assert source.get("instance_id") == "p1"
        assert source.get("class_id") == class_id
