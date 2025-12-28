from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from typing import Any, Optional, Tuple

import httpx
import pytest

from shared.models.pipeline_job import PipelineJob
from shared.services.pipeline_job_queue import PipelineJobQueue


BFF_URL = (os.getenv("BFF_BASE_URL") or "http://localhost:8002").rstrip("/")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN") or os.getenv("BFF_ADMIN_TOKEN") or "test-token"


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("s3://"):
        raise AssertionError(f"not an s3 uri: {uri}")
    remainder = uri[len("s3://") :]
    bucket, _, key = remainder.partition("/")
    return bucket, key


def _lakefs_s3_client():
    import boto3
    from botocore.config import Config

    port = int(os.getenv("LAKEFS_PORT_HOST") or "48080")
    endpoint = f"http://127.0.0.1:{port}"
    access = (os.getenv("LAKEFS_INSTALLATION_ACCESS_KEY_ID") or "spice-lakefs-admin").strip()
    secret = (os.getenv("LAKEFS_INSTALLATION_SECRET_ACCESS_KEY") or "spice-lakefs-admin-secret").strip()
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name="us-east-1",
        verify=False,
        config=Config(s3={"addressing_style": "path"}),
    )


def _list_relative_object_keys(bucket: str, *, commit_id: str, artifact_prefix: str) -> set[str]:
    """
    List the object keys for a dataset artifact under a specific lakeFS ref (commit id),
    but return paths relative to the ref prefix so keys can be compared across commits.
    """
    client = _lakefs_s3_client()
    normalized_prefix = artifact_prefix.strip("/").rstrip("/")
    prefix = f"{commit_id}/{normalized_prefix}/"
    keys: set[str] = set()
    token: Optional[str] = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        for item in resp.get("Contents") or []:
            key = str(item.get("Key") or "")
            if not key or key.endswith("/"):
                continue
            base = os.path.basename(key)
            if base.startswith("_") or base.startswith("."):
                continue
            if not os.path.splitext(base)[1]:
                continue
            if not key.startswith(f"{commit_id}/"):
                continue
            keys.add(key[len(commit_id) + 1 :])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
            if not token:
                break
        else:
            break
    return keys


def _load_rows_from_artifact(bucket: str, *, commit_id: str, artifact_prefix: str) -> list[dict[str, Any]]:
    client = _lakefs_s3_client()
    normalized_prefix = artifact_prefix.strip("/").rstrip("/")
    prefix = f"{commit_id}/{normalized_prefix}/"
    rows: list[dict[str, Any]] = []
    token: Optional[str] = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        for item in resp.get("Contents") or []:
            key = str(item.get("Key") or "")
            if not key or key.endswith("/"):
                continue
            base = os.path.basename(key)
            if base.startswith("_") or base.startswith("."):
                continue
            if not base.endswith(".json"):
                continue
            obj = client.get_object(Bucket=bucket, Key=key)
            body = obj["Body"].read().decode("utf-8")
            for line in body.splitlines():
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
            if not token:
                break
        else:
            break
    return rows


async def _wait_for_command(client: httpx.AsyncClient, command_id: str, *, timeout_seconds: int = 90) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        resp = await client.get(f"{BFF_URL}/api/v1/commands/{command_id}/status")
        if resp.status_code == 200:
            payload = resp.json()
            status = str(payload.get("status") or (payload.get("data") or {}).get("status") or "").upper()
            if status in {"COMPLETED", "SUCCESS", "SUCCEEDED", "DONE"}:
                return
            if status in {"FAILED", "ERROR"}:
                raise AssertionError(f"Command {command_id} failed: {payload}")
        await asyncio.sleep(0.5)
    raise AssertionError(f"Timed out waiting for command {command_id}")


async def _wait_for_run_terminal(
    client: httpx.AsyncClient,
    *,
    pipeline_id: str,
    job_id: str,
    timeout_seconds: int = 180,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_payload: Optional[dict[str, Any]] = None
    while time.monotonic() < deadline:
        resp = await client.get(f"{BFF_URL}/api/v1/pipelines/{pipeline_id}/runs", params={"limit": 200})
        resp.raise_for_status()
        payload = resp.json()
        last_payload = payload
        runs = (payload.get("data") or {}).get("runs") or []
        run = next((item for item in runs if item.get("job_id") == job_id), None)
        if run:
            status = str(run.get("status") or "").upper()
            if status in {"SUCCESS", "FAILED", "DEPLOYED", "IGNORED"}:
                return run
        await asyncio.sleep(1.0)
    raise AssertionError(f"Timed out waiting for run job_id={job_id} (last={last_payload})")


def _artifact_for_output(run: dict[str, Any], *, node_id: str) -> str:
    output_json = run.get("output_json") or {}
    if not isinstance(output_json, dict):
        raise AssertionError(f"run output_json missing/invalid: {run}")
    outputs = output_json.get("outputs") or []
    if not isinstance(outputs, list):
        raise AssertionError(f"run outputs missing/invalid: {run}")
    match = next((item for item in outputs if str(item.get("node_id") or "") == node_id), None)
    if not match:
        raise AssertionError(f"output node_id {node_id} not found in run outputs: {outputs}")
    artifact_key = str(match.get("artifact_key") or "")
    assert artifact_key.startswith("s3://")
    return artifact_key


def _commit_and_prefix_from_artifact(artifact_key: str) -> tuple[str, str, str]:
    bucket, key = _parse_s3_uri(artifact_key)
    commit_id, _, prefix = key.partition("/")
    if not commit_id or not prefix:
        raise AssertionError(f"artifact_key does not contain commit_id + prefix: {artifact_key}")
    return bucket, commit_id, prefix


@pytest.mark.integration
@pytest.mark.asyncio
async def test_snapshot_overwrites_outputs_across_runs() -> None:
    """
    Checklist CL-015:
    - Snapshot builds overwrite outputs (no append duplicates across runs).

    Proof:
    - Two deploy runs against the same latest input version must not retain prior output part objects
      (relative keys differ across commits due to overwrite semantics).
    """
    headers = {"X-Admin-Token": ADMIN_TOKEN}
    suffix = uuid.uuid4().hex[:8]
    db_name = f"e2e_snap_{suffix}"

    async with httpx.AsyncClient(headers=headers, timeout=30.0) as client:
        create_db = await client.post(f"{BFF_URL}/api/v1/databases", json={"name": db_name, "description": "snap"})
        create_db.raise_for_status()
        command_id = str(((create_db.json().get("data") or {}) or {}).get("command_id") or "")
        assert command_id
        await _wait_for_command(client, command_id)

        create_dataset = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets",
            json={
                "db_name": db_name,
                "name": "in_ds",
                "description": "snap",
                "branch": "main",
                "source_type": "manual",
                "schema_json": {"columns": [{"name": "id", "type": "xsd:integer"}]},
            },
        )
        create_dataset.raise_for_status()
        dataset = (create_dataset.json().get("data") or {}).get("dataset") or {}
        dataset_id = str(dataset.get("dataset_id") or "")
        assert dataset_id

        create_version = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets/{dataset_id}/versions",
            json={
                "sample_json": {"rows": [{"id": 1}, {"id": 2}, {"id": 3}]},
                "schema_json": {"columns": [{"name": "id", "type": "xsd:integer"}]},
            },
        )
        create_version.raise_for_status()

        definition_json: dict[str, Any] = {
            "nodes": [
                {"id": "in1", "type": "input", "metadata": {"datasetId": dataset_id, "datasetName": "in_ds"}},
                {"id": "out1", "type": "output", "metadata": {"datasetName": "out_ds"}},
            ],
            "edges": [{"from": "in1", "to": "out1"}],
            "parameters": [],
            "settings": {"engine": "Batch"},
        }

        create_pipeline = await client.post(
            f"{BFF_URL}/api/v1/pipelines",
            json={
                "db_name": db_name,
                "name": "snap pipeline",
                "location": "e2e",
                "description": "snap",
                "branch": "main",
                "pipeline_type": "batch",
                "definition_json": definition_json,
            },
        )
        create_pipeline.raise_for_status()
        pipeline = (create_pipeline.json().get("data") or {}).get("pipeline") or {}
        pipeline_id = str(pipeline.get("pipeline_id") or "")
        assert pipeline_id

        queue = PipelineJobQueue()
        job_id_1 = f"deploy-snap-{uuid.uuid4().hex}"
        await queue.publish(
            PipelineJob(
                job_id=job_id_1,
                pipeline_id=pipeline_id,
                db_name=db_name,
                pipeline_type="batch",
                definition_json=definition_json,
                node_id="out1",
                output_dataset_name="out_ds",
                mode="deploy",
                branch="main",
            )
        )

        run1 = await _wait_for_run_terminal(client, pipeline_id=pipeline_id, job_id=job_id_1)
        assert str(run1.get("status") or "").upper() == "DEPLOYED"
        artifact1 = _artifact_for_output(run1, node_id="out1")
        bucket1, commit1, prefix1 = _commit_and_prefix_from_artifact(artifact1)
        keys1 = _list_relative_object_keys(bucket1, commit_id=commit1, artifact_prefix=prefix1)
        assert keys1

        job_id_2 = f"deploy-snap-{uuid.uuid4().hex}"
        await queue.publish(
            PipelineJob(
                job_id=job_id_2,
                pipeline_id=pipeline_id,
                db_name=db_name,
                pipeline_type="batch",
                definition_json=definition_json,
                node_id="out1",
                output_dataset_name="out_ds",
                mode="deploy",
                branch="main",
            )
        )

        run2 = await _wait_for_run_terminal(client, pipeline_id=pipeline_id, job_id=job_id_2)
        assert str(run2.get("status") or "").upper() == "DEPLOYED"
        artifact2 = _artifact_for_output(run2, node_id="out1")
        bucket2, commit2, prefix2 = _commit_and_prefix_from_artifact(artifact2)
        assert bucket2 == bucket1
        assert prefix2 == prefix1
        keys2 = _list_relative_object_keys(bucket2, commit_id=commit2, artifact_prefix=prefix2)
        assert keys2

        # Snapshot overwrite: previous part files should not remain visible after a new run.
        assert keys1.isdisjoint(keys2), f"expected overwrite semantics, got overlap={keys1 & keys2}"
        rows2 = _load_rows_from_artifact(bucket2, commit_id=commit2, artifact_prefix=prefix2)
        assert len(rows2) == 3


@pytest.mark.integration
@pytest.mark.asyncio
async def test_incremental_appends_outputs_and_preserves_previous_parts() -> None:
    """
    Checklist CL-016:
    - Incremental builds process only new data and append to existing output parts.

    Proof:
    - After the first run, capture the set of output part objects (relative keys).
    - After appending new input rows and running again, the new commit must contain all previous part keys
      plus at least one new part key (append semantics).
    """
    headers = {"X-Admin-Token": ADMIN_TOKEN}
    suffix = uuid.uuid4().hex[:8]
    db_name = f"e2e_inc_{suffix}"

    async with httpx.AsyncClient(headers=headers, timeout=30.0) as client:
        create_db = await client.post(f"{BFF_URL}/api/v1/databases", json={"name": db_name, "description": "inc"})
        create_db.raise_for_status()
        command_id = str(((create_db.json().get("data") or {}) or {}).get("command_id") or "")
        assert command_id
        await _wait_for_command(client, command_id)

        schema_json = {"columns": [{"name": "id", "type": "xsd:integer"}, {"name": "ts", "type": "xsd:integer"}]}
        create_dataset = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets",
            json={
                "db_name": db_name,
                "name": "in_ds",
                "description": "inc",
                "branch": "main",
                "source_type": "manual",
                "schema_json": schema_json,
            },
        )
        create_dataset.raise_for_status()
        dataset = (create_dataset.json().get("data") or {}).get("dataset") or {}
        dataset_id = str(dataset.get("dataset_id") or "")
        assert dataset_id

        create_version_1 = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets/{dataset_id}/versions",
            json={
                "sample_json": {"rows": [{"id": 1, "ts": 1}, {"id": 2, "ts": 2}, {"id": 3, "ts": 3}]},
                "schema_json": schema_json,
            },
        )
        create_version_1.raise_for_status()

        definition_json: dict[str, Any] = {
            "nodes": [
                {"id": "in1", "type": "input", "metadata": {"datasetId": dataset_id, "datasetName": "in_ds"}},
                {"id": "out1", "type": "output", "metadata": {"datasetName": "out_ds"}},
            ],
            "edges": [{"from": "in1", "to": "out1"}],
            "parameters": [],
            "settings": {"engine": "Incremental", "watermarkColumn": "ts"},
        }

        create_pipeline = await client.post(
            f"{BFF_URL}/api/v1/pipelines",
            json={
                "db_name": db_name,
                "name": "inc pipeline",
                "location": "e2e",
                "description": "inc",
                "branch": "main",
                "pipeline_type": "incremental",
                "definition_json": definition_json,
            },
        )
        create_pipeline.raise_for_status()
        pipeline = (create_pipeline.json().get("data") or {}).get("pipeline") or {}
        pipeline_id = str(pipeline.get("pipeline_id") or "")
        assert pipeline_id

        queue = PipelineJobQueue()
        job_id_1 = f"deploy-inc-{uuid.uuid4().hex}"
        await queue.publish(
            PipelineJob(
                job_id=job_id_1,
                pipeline_id=pipeline_id,
                db_name=db_name,
                pipeline_type="incremental",
                definition_json=definition_json,
                node_id="out1",
                output_dataset_name="out_ds",
                mode="deploy",
                branch="main",
            )
        )

        run1 = await _wait_for_run_terminal(client, pipeline_id=pipeline_id, job_id=job_id_1)
        assert str(run1.get("status") or "").upper() == "DEPLOYED"
        artifact1 = _artifact_for_output(run1, node_id="out1")
        bucket1, commit1, prefix1 = _commit_and_prefix_from_artifact(artifact1)
        keys1 = _list_relative_object_keys(bucket1, commit_id=commit1, artifact_prefix=prefix1)
        assert keys1

        create_version_2 = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets/{dataset_id}/versions",
            json={
                "sample_json": {
                    "rows": [
                        {"id": 1, "ts": 1},
                        {"id": 2, "ts": 2},
                        {"id": 3, "ts": 3},
                        {"id": 4, "ts": 4},
                        {"id": 5, "ts": 5},
                    ]
                },
                "schema_json": schema_json,
            },
        )
        create_version_2.raise_for_status()

        job_id_2 = f"deploy-inc-{uuid.uuid4().hex}"
        await queue.publish(
            PipelineJob(
                job_id=job_id_2,
                pipeline_id=pipeline_id,
                db_name=db_name,
                pipeline_type="incremental",
                definition_json=definition_json,
                node_id="out1",
                output_dataset_name="out_ds",
                mode="deploy",
                branch="main",
            )
        )

        run2 = await _wait_for_run_terminal(client, pipeline_id=pipeline_id, job_id=job_id_2)
        assert str(run2.get("status") or "").upper() == "DEPLOYED"
        artifact2 = _artifact_for_output(run2, node_id="out1")
        bucket2, commit2, prefix2 = _commit_and_prefix_from_artifact(artifact2)
        assert bucket2 == bucket1
        assert prefix2 == prefix1

        keys2 = _list_relative_object_keys(bucket2, commit_id=commit2, artifact_prefix=prefix2)
        assert keys2

        assert keys1.issubset(keys2), "incremental runs must preserve prior output parts"
        assert keys2 - keys1, "expected at least one new output part to be appended"
        rows2 = _load_rows_from_artifact(bucket2, commit_id=commit2, artifact_prefix=prefix2)
        ids = [int(row.get("id")) for row in rows2 if isinstance(row, dict) and row.get("id") is not None]
        assert sorted(ids) == [1, 2, 3, 4, 5], f"expected non-duplicated incremental output, got ids={ids} rows={rows2}"
