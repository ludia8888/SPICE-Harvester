from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Any, Optional

import httpx
import pytest

from shared.services.dataset_registry import DatasetRegistry


BFF_URL = (os.getenv("BFF_BASE_URL") or "http://localhost:8002").rstrip("/")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN") or os.getenv("BFF_ADMIN_TOKEN") or "test-token"


async def _wait_for_command(client: httpx.AsyncClient, command_id: str, *, timeout_seconds: int = 120) -> None:
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
    timeout_seconds: int = 240,
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_streaming_build_deploy_promotes_all_outputs() -> None:
    """
    Checklist CL-017:
    - Streaming pipelines behave as a job group: build and deploy publish all outputs together.

    Proof:
    - Build succeeds even if node_id is provided (API ignores node targeting for streaming).
    - Deploy promotion without node_id publishes all build outputs, with a single merge commit id.
    """
    headers = {"X-Admin-Token": ADMIN_TOKEN}
    suffix = uuid.uuid4().hex[:8]
    db_name = f"e2e_stream_{suffix}"

    async with httpx.AsyncClient(headers=headers, timeout=60.0) as client:
        create_db = await client.post(f"{BFF_URL}/api/v1/databases", json={"name": db_name, "description": "stream"})
        create_db.raise_for_status()
        command_id = str(((create_db.json().get("data") or {}) or {}).get("command_id") or "")
        assert command_id
        await _wait_for_command(client, command_id)

        create_dataset = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets",
            json={
                "db_name": db_name,
                "name": "in_ds",
                "description": "stream input",
                "branch": "main",
                "source_type": "manual",
                "schema_json": {
                    "columns": [
                        {"name": "id", "type": "xsd:integer"},
                        {"name": "ts", "type": "xsd:integer"},
                    ]
                },
            },
        )
        create_dataset.raise_for_status()
        dataset = (create_dataset.json().get("data") or {}).get("dataset") or {}
        dataset_id = str(dataset.get("dataset_id") or "")
        assert dataset_id

        create_version = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets/{dataset_id}/versions",
            json={
                "sample_json": {"rows": [{"id": 1, "ts": 1}, {"id": 2, "ts": 2}]},
                "schema_json": {
                    "columns": [
                        {"name": "id", "type": "xsd:integer"},
                        {"name": "ts", "type": "xsd:integer"},
                    ]
                },
            },
        )
        create_version.raise_for_status()

        definition_json: dict[str, Any] = {
            "nodes": [
                {"id": "in1", "type": "input", "metadata": {"datasetId": dataset_id, "datasetName": "in_ds"}},
                {"id": "out1", "type": "output", "metadata": {"datasetName": "out_a"}},
                {"id": "out2", "type": "output", "metadata": {"datasetName": "out_b"}},
            ],
            "edges": [
                {"from": "in1", "to": "out1"},
                {"from": "in1", "to": "out2"},
            ],
            "parameters": [],
            "schemaContract": [
                {"column": "id", "type": "xsd:integer", "required": True},
                {"column": "ts", "type": "xsd:integer", "required": True},
            ],
            "settings": {"engine": "Streaming", "watermarkColumn": "ts"},
        }

        create_pipeline = await client.post(
            f"{BFF_URL}/api/v1/pipelines",
            json={
                "db_name": db_name,
                "name": "stream pipeline",
                "location": "e2e",
                "description": "stream",
                "branch": "main",
                "pipeline_type": "streaming",
                "definition_json": definition_json,
            },
        )
        create_pipeline.raise_for_status()
        pipeline = (create_pipeline.json().get("data") or {}).get("pipeline") or {}
        pipeline_id = str(pipeline.get("pipeline_id") or "")
        assert pipeline_id

        build_resp = await client.post(
            f"{BFF_URL}/api/v1/pipelines/{pipeline_id}/build",
            json={"db_name": db_name, "node_id": "out1", "limit": 5},
        )
        build_resp.raise_for_status()
        build_job_id = str(((build_resp.json().get("data") or {}) or {}).get("job_id") or "")
        assert build_job_id
        build_run = await _wait_for_run_terminal(client, pipeline_id=pipeline_id, job_id=build_job_id)
        assert str(build_run.get("status") or "").upper() == "SUCCESS"

        deploy_resp = await client.post(
            f"{BFF_URL}/api/v1/pipelines/{pipeline_id}/deploy",
            json={"promote_build": True, "build_job_id": build_job_id},
        )
        deploy_resp.raise_for_status()
        deploy_payload = deploy_resp.json()
        deployed_commit_id = str(((deploy_payload.get("data") or {}) or {}).get("deployed_commit_id") or "")
        outputs = ((deploy_payload.get("data") or {}) or {}).get("outputs") or []
        assert deployed_commit_id
        assert isinstance(outputs, list)
        assert {item.get("dataset_name") for item in outputs if isinstance(item, dict)} == {"out_a", "out_b"}
        assert {item.get("merge_commit_id") for item in outputs if isinstance(item, dict)} == {deployed_commit_id}

    dataset_registry = DatasetRegistry()
    await dataset_registry.initialize()
    try:
        out_a = await dataset_registry.get_dataset_by_name(db_name=db_name, name="out_a", branch="main")
        out_b = await dataset_registry.get_dataset_by_name(db_name=db_name, name="out_b", branch="main")
        assert out_a and out_b
        ver_a = await dataset_registry.get_latest_version(dataset_id=out_a.dataset_id)
        ver_b = await dataset_registry.get_latest_version(dataset_id=out_b.dataset_id)
        assert ver_a and ver_b
        assert ver_a.lakefs_commit_id == deployed_commit_id
        assert ver_b.lakefs_commit_id == deployed_commit_id
    finally:
        await dataset_registry.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_streaming_build_fails_as_job_group_on_contract_mismatch() -> None:
    """
    Checklist CL-017:
    - Streaming pipelines run as a unit: if one output violates schema contract, the build fails and no output is published.
    """
    headers = {"X-Admin-Token": ADMIN_TOKEN}
    suffix = uuid.uuid4().hex[:8]
    db_name = f"e2e_stream_fail_{suffix}"

    async with httpx.AsyncClient(headers=headers, timeout=60.0) as client:
        create_db = await client.post(f"{BFF_URL}/api/v1/databases", json={"name": db_name, "description": "stream"})
        create_db.raise_for_status()
        command_id = str(((create_db.json().get("data") or {}) or {}).get("command_id") or "")
        assert command_id
        await _wait_for_command(client, command_id)

        create_dataset = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets",
            json={
                "db_name": db_name,
                "name": "in_ds",
                "description": "stream input",
                "branch": "main",
                "source_type": "manual",
                "schema_json": {
                    "columns": [
                        {"name": "id", "type": "xsd:integer"},
                        {"name": "ts", "type": "xsd:integer"},
                    ]
                },
            },
        )
        create_dataset.raise_for_status()
        dataset = (create_dataset.json().get("data") or {}).get("dataset") or {}
        dataset_id = str(dataset.get("dataset_id") or "")
        assert dataset_id

        create_version = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets/{dataset_id}/versions",
            json={
                "sample_json": {"rows": [{"id": 1, "ts": 1}, {"id": 2, "ts": 2}]},
                "schema_json": {
                    "columns": [
                        {"name": "id", "type": "xsd:integer"},
                        {"name": "ts", "type": "xsd:integer"},
                    ]
                },
            },
        )
        create_version.raise_for_status()

        definition_json: dict[str, Any] = {
            "nodes": [
                {"id": "in1", "type": "input", "metadata": {"datasetId": dataset_id, "datasetName": "in_ds"}},
                {"id": "sel", "type": "transform", "metadata": {"operation": "select", "columns": ["id"]}},
                {"id": "out_ok", "type": "output", "metadata": {"datasetName": "out_ok"}},
                {"id": "out_bad", "type": "output", "metadata": {"datasetName": "out_bad"}},
            ],
            "edges": [
                {"from": "in1", "to": "out_ok"},
                {"from": "in1", "to": "sel"},
                {"from": "sel", "to": "out_bad"},
            ],
            "parameters": [],
            "schemaContract": [{"column": "ts", "type": "xsd:integer", "required": True}],
            "settings": {"engine": "Streaming", "watermarkColumn": "ts"},
        }

        create_pipeline = await client.post(
            f"{BFF_URL}/api/v1/pipelines",
            json={
                "db_name": db_name,
                "name": "stream pipeline bad",
                "location": "e2e",
                "description": "stream",
                "branch": "main",
                "pipeline_type": "streaming",
                "definition_json": definition_json,
            },
        )
        create_pipeline.raise_for_status()
        pipeline = (create_pipeline.json().get("data") or {}).get("pipeline") or {}
        pipeline_id = str(pipeline.get("pipeline_id") or "")
        assert pipeline_id

        build_resp = await client.post(
            f"{BFF_URL}/api/v1/pipelines/{pipeline_id}/build",
            json={"db_name": db_name, "node_id": "out_ok", "limit": 5},
        )
        build_resp.raise_for_status()
        build_job_id = str(((build_resp.json().get("data") or {}) or {}).get("job_id") or "")
        assert build_job_id
        build_run = await _wait_for_run_terminal(client, pipeline_id=pipeline_id, job_id=build_job_id)
        assert str(build_run.get("status") or "").upper() == "FAILED"

        deploy_resp = await client.post(
            f"{BFF_URL}/api/v1/pipelines/{pipeline_id}/deploy",
            json={"promote_build": True, "build_job_id": build_job_id},
        )
        assert deploy_resp.status_code == 409
        payload = deploy_resp.json()
        assert payload.get("detail", {}).get("code") == "BUILD_NOT_SUCCESS"

    dataset_registry = DatasetRegistry()
    await dataset_registry.initialize()
    try:
        out_ok = await dataset_registry.get_dataset_by_name(db_name=db_name, name="out_ok", branch="main")
        out_bad = await dataset_registry.get_dataset_by_name(db_name=db_name, name="out_bad", branch="main")
        assert out_ok is None
        assert out_bad is None
    finally:
        await dataset_registry.close()

