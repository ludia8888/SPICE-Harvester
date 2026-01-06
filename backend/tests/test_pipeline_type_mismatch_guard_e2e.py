from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Any, Optional

import httpx
import pytest


BFF_URL = (os.getenv("BFF_BASE_URL") or "http://localhost:8002").rstrip("/")
OMS_URL = (os.getenv("OMS_BASE_URL") or "http://localhost:8000").rstrip("/")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN") or os.getenv("BFF_ADMIN_TOKEN") or "test-token"
HTTPX_TIMEOUT = float(os.getenv("PIPELINE_HTTP_TIMEOUT", "120") or 120)
RUN_TIMEOUT_SECONDS = int(os.getenv("PIPELINE_RUN_TIMEOUT_SECONDS", "300") or 300)
COMMAND_TIMEOUT_SECONDS = int(os.getenv("PIPELINE_COMMAND_TIMEOUT_SECONDS", "120") or 120)


async def _wait_for_command(
    client: httpx.AsyncClient,
    command_id: str,
    *,
    timeout_seconds: int = COMMAND_TIMEOUT_SECONDS,
    db_name: Optional[str] = None,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        resp = await client.get(f"{BFF_URL}/api/v1/commands/{command_id}/status")
        if resp.status_code == 200:
            payload = resp.json()
            status = str(payload.get("status") or (payload.get("data") or {}).get("status") or "").upper()
            if status in {"COMPLETED", "SUCCESS", "SUCCEEDED", "DONE"}:
                return
            if status in {"FAILED", "ERROR"}:
                if db_name:
                    try:
                        exists_resp = await client.get(f"{OMS_URL}/api/v1/database/exists/{db_name}")
                        if exists_resp.status_code == 200:
                            exists_payload = exists_resp.json()
                            exists = (exists_payload.get("data") or {}).get("exists")
                            if exists is True:
                                return
                    except httpx.HTTPError:
                        pass
                raise AssertionError(f"Command {command_id} failed: {payload}")
        if db_name:
            try:
                exists_resp = await client.get(f"{OMS_URL}/api/v1/database/exists/{db_name}")
                if exists_resp.status_code == 200:
                    exists_payload = exists_resp.json()
                    exists = (exists_payload.get("data") or {}).get("exists")
                    if exists is True:
                        return
            except httpx.HTTPError:
                pass
        await asyncio.sleep(0.5)
    raise AssertionError(f"Timed out waiting for command {command_id}")


async def _wait_for_run_terminal(
    client: httpx.AsyncClient,
    *,
    pipeline_id: str,
    job_id: str,
    timeout_seconds: int = RUN_TIMEOUT_SECONDS,
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


async def _post_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    json_payload: dict,
    retries: int = 3,
    retry_sleep: float = 2.0,
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
    async with httpx.AsyncClient(headers=headers, timeout=HTTPX_TIMEOUT) as client:
        suffix = uuid.uuid4().hex[:12]
        db_name = f"e2e_tm_{suffix}"
        headers = {"X-Admin-Token": ADMIN_TOKEN, "X-DB-Name": db_name}
        client.headers.update(headers)

        await _create_db_with_retry(client, db_name=db_name, description="tm")

        create_dataset = await _post_with_retry(
            client,
            f"{BFF_URL}/api/v1/pipelines/datasets",
            json_payload={
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

        create_version = await _post_with_retry(
            client,
            f"{BFF_URL}/api/v1/pipelines/datasets/{dataset_id}/versions",
            json_payload={
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

        create_pipeline = await _post_with_retry(
            client,
            f"{BFF_URL}/api/v1/pipelines",
            json_payload={
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

        preview = await _post_with_retry(
            client,
            f"{BFF_URL}/api/v1/pipelines/{pipeline_id}/preview",
            json_payload={
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
