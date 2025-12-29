from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Any, Optional

import httpx
import pytest


BFF_URL = (os.getenv("BFF_BASE_URL") or "http://localhost:8002").rstrip("/")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN") or os.getenv("BFF_ADMIN_TOKEN") or "test-token"


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
    timeout_seconds: int = 120,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_payload: Optional[dict[str, Any]] = None
    while time.monotonic() < deadline:
        resp = await client.get(f"{BFF_URL}/api/v1/pipelines/{pipeline_id}/runs", params={"limit": 50})
        resp.raise_for_status()
        payload = resp.json()
        last_payload = payload
        runs = (payload.get("data") or {}).get("runs") or []
        run = next((item for item in runs if item.get("job_id") == job_id), None)
        if run:
            status = str(run.get("status") or "").upper()
            if status in {"SUCCESS", "FAILED", "DEPLOYED"}:
                return run
        await asyncio.sleep(1.0)
    raise AssertionError(f"Timed out waiting for run job_id={job_id} (last={last_payload})")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_preview_rejects_type_mismatch_in_compute_expression() -> None:
    """
    Checklist CL-006:
    - Type-mismatched expressions must not silently produce NULLs.

    Repro:
    - name is declared xsd:string and contains non-numeric values.
    - compute uses `bad = name + 1` which would require an implicit cast.
    - In permissive mode Spark produces NULLs without error; we require a FAILED preview with errors.
    """

    headers = {"X-Admin-Token": ADMIN_TOKEN}
    async with httpx.AsyncClient(headers=headers, timeout=30.0) as client:
        suffix = uuid.uuid4().hex[:8]
        db_name = f"e2e_tm_{suffix}"
        headers = {"X-Admin-Token": ADMIN_TOKEN, "X-DB-Name": db_name}
        client.headers.update(headers)

        create_db = await client.post(f"{BFF_URL}/api/v1/databases", json={"name": db_name, "description": "tm"})
        create_db.raise_for_status()
        command_id = str(((create_db.json().get("data") or {}) or {}).get("command_id") or "")
        assert command_id
        await _wait_for_command(client, command_id)

        create_dataset = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets",
            json={
                "db_name": db_name,
                "name": "tm_ds",
                "description": "tm",
                "branch": "main",
                "source_type": "manual",
                "schema_json": {
                    "columns": [
                        {"name": "id", "type": "xsd:integer"},
                        {"name": "name", "type": "xsd:string"},
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
                "sample_json": {"rows": [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]},
                "schema_json": {
                    "columns": [
                        {"name": "id", "type": "xsd:integer"},
                        {"name": "name", "type": "xsd:string"},
                    ]
                },
            },
        )
        create_version.raise_for_status()

        definition_json: dict[str, Any] = {
            "nodes": [
                {"id": "in1", "type": "input", "metadata": {"datasetId": dataset_id, "datasetName": "tm_ds"}},
                {
                    "id": "t1",
                    "type": "transform",
                    "metadata": {"operation": "compute", "expression": "bad = name + 1"},
                },
                {"id": "out1", "type": "output", "metadata": {"datasetName": "out_ds"}},
            ],
            "edges": [
                {"from": "in1", "to": "t1"},
                {"from": "t1", "to": "out1"},
            ],
            "parameters": [],
        }

        create_pipeline = await client.post(
            f"{BFF_URL}/api/v1/pipelines",
            json={
                "db_name": db_name,
                "name": "tm pipeline",
                "location": "e2e",
                "description": "tm",
                "branch": "main",
                "pipeline_type": "batch",
                "definition_json": definition_json,
            },
        )
        create_pipeline.raise_for_status()
        pipeline = (create_pipeline.json().get("data") or {}).get("pipeline") or {}
        pipeline_id = str(pipeline.get("pipeline_id") or "")
        assert pipeline_id

        preview = await client.post(
            f"{BFF_URL}/api/v1/pipelines/{pipeline_id}/preview",
            json={
                "db_name": db_name,
                "definition_json": definition_json,
                "node_id": "out1",
                "branch": "main",
                "limit": 50,
            },
        )
        preview.raise_for_status()
        job_id = str((preview.json().get("data") or {}).get("job_id") or "")
        assert job_id

        run = await _wait_for_run_terminal(client, pipeline_id=pipeline_id, job_id=job_id)
        assert str(run.get("mode") or "").lower() == "preview"

        fetched = await client.get(
            f"{BFF_URL}/api/v1/pipelines/{pipeline_id}",
            params={"preview_node_id": "out1"},
        )
        fetched.raise_for_status()
        pipeline_payload = ((fetched.json().get("data") or {}) or {}).get("pipeline") or {}
        sample = pipeline_payload.get("last_preview_sample")
        status_value = str(pipeline_payload.get("last_preview_status") or "").upper()

        errors = []
        if isinstance(sample, dict):
            sample_errors = sample.get("errors")
            if isinstance(sample_errors, list):
                errors = [str(item) for item in sample_errors if str(item).strip()]

        assert status_value == "FAILED", f"expected FAILED preview, got status={status_value} sample={sample}"
        assert errors, f"expected preview errors for type mismatch, got sample={sample}"
