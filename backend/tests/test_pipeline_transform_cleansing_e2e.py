from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from typing import Any, Optional

import httpx
import pytest

from shared.utils.s3_uri import parse_s3_uri


BFF_URL = (os.getenv("BFF_BASE_URL") or "http://localhost:8002").rstrip("/")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN") or os.getenv("BFF_ADMIN_TOKEN") or "test-token"
HTTPX_TIMEOUT = float(os.getenv("PIPELINE_HTTP_TIMEOUT", "180") or 180)
RUN_TIMEOUT_SECONDS = int(os.getenv("PIPELINE_RUN_TIMEOUT_SECONDS", "420") or 420)

if os.getenv("RUN_PIPELINE_TRANSFORM_E2E", "").strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip(
        "RUN_PIPELINE_TRANSFORM_E2E must be enabled for this test run. Set RUN_PIPELINE_TRANSFORM_E2E=true.",
        allow_module_level=True,
    )


def _lakefs_s3_client():
    import boto3
    from botocore.config import Config

    port = int(os.getenv("LAKEFS_PORT_HOST") or os.getenv("LAKEFS_API_PORT") or "48080")
    endpoint = f"http://127.0.0.1:{port}"
    access = (
        os.getenv("LAKEFS_INSTALLATION_ACCESS_KEY_ID")
        or os.getenv("LAKEFS_ACCESS_KEY_ID")
        or "spice-lakefs-admin"
    ).strip()
    secret = (
        os.getenv("LAKEFS_INSTALLATION_SECRET_ACCESS_KEY")
        or os.getenv("LAKEFS_SECRET_ACCESS_KEY")
        or "spice-lakefs-admin-secret"
    ).strip()
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name="us-east-1",
        verify=False,
        config=Config(s3={"addressing_style": "path"}),
    )


def _artifact_commit_and_prefix(artifact_key: str) -> tuple[str, str, str]:
    parsed = parse_s3_uri(artifact_key)
    if not parsed:
        raise AssertionError(f"invalid artifact key: {artifact_key}")
    bucket, key = parsed
    commit_id, _, prefix = key.partition("/")
    if not commit_id or not prefix:
        raise AssertionError(f"artifact key missing commit/prefix: {artifact_key}")
    return bucket, commit_id, prefix


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


async def _post_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    json_payload: dict,
    retries: int = 5,
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


async def _wait_for_run_errors(
    client: httpx.AsyncClient,
    *,
    pipeline_id: str,
    job_id: str,
    timeout_seconds: int = 60,
) -> list[Any]:
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
            output_json = run.get("output_json") or {}
            if isinstance(output_json, dict):
                errors = output_json.get("errors") or []
                if errors:
                    return errors
        await asyncio.sleep(1.0)
    suffix = f" last_error={last_error}" if last_error else ""
    raise AssertionError(f"Timed out waiting for run errors job_id={job_id} (last={last_payload}){suffix}")


def _select_output(artifact: dict[str, Any], dataset_name: str) -> dict[str, Any]:
    outputs = artifact.get("outputs") or []
    for output in outputs:
        if isinstance(output, dict) and output.get("dataset_name") == dataset_name:
            return output
    raise AssertionError(f"output not found: {dataset_name} in {outputs}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pipeline_transform_cleansing_and_validation_e2e() -> None:
    """
    Validate transform/cleansing ops + schema/type/bad-record handling via real pipeline builds.
    """
    suffix = uuid.uuid4().hex[:10]
    db_name = f"e2e_transform_{suffix}"
    orders_dataset_name = "orders_raw"
    customers_dataset_name = "customers_raw"
    clean_output_name = "orders_clean"
    bad_output_name = "orders_bad"
    schema_output_name = "orders_schema_bad"

    headers = {
        "X-Admin-Token": ADMIN_TOKEN,
        "X-DB-Name": db_name,
        "X-User-ID": "pipeline-transform-e2e",
        "X-User-Type": "user",
    }

    orders_schema = {
        "columns": [
            {"name": "order_id", "type": "xsd:string"},
            {"name": "customer_id", "type": "xsd:string"},
            {"name": "amount_raw", "type": "xsd:string"},
            {"name": "priority", "type": "xsd:string"},
            {"name": "status", "type": "xsd:string"},
        ]
    }
    orders_rows = [
        {"order_id": "o1", "customer_id": "c1", "amount_raw": "10.5", "priority": "1", "status": "active"},
        {"order_id": "o1", "customer_id": "c1", "amount_raw": "10.5", "priority": "1", "status": "active"},
        {"order_id": "o2", "customer_id": "c2", "amount_raw": "15", "priority": "2", "status": "active"},
        {"order_id": "o3", "customer_id": "c3", "amount_raw": "bad", "priority": "2", "status": "active"},
        {"order_id": "o4", "customer_id": "c4", "amount_raw": "25", "priority": "3", "status": "canceled"},
    ]

    customers_schema = {
        "columns": [
            {"name": "customer_id", "type": "xsd:string"},
            {"name": "name", "type": "xsd:string"},
        ]
    }
    customers_rows = [
        {"customer_id": "c1", "name": "  ALICE "},
        {"customer_id": "c2", "name": None},
        {"customer_id": "c3", "name": "Chris"},
        {"customer_id": "c4", "name": "Dana"},
    ]

    async with httpx.AsyncClient(headers=headers, timeout=HTTPX_TIMEOUT) as client:
        await _create_db_with_retry(client, db_name=db_name, description="pipeline transform e2e")

        create_orders = await _post_with_retry(
            client,
            f"{BFF_URL}/api/v1/pipelines/datasets",
            json_payload={
                "db_name": db_name,
                "name": orders_dataset_name,
                "description": "orders raw",
                "branch": "main",
                "source_type": "manual",
                "schema_json": orders_schema,
            },
        )
        create_orders.raise_for_status()
        orders_dataset = (create_orders.json().get("data") or {}).get("dataset") or {}
        orders_dataset_id = str(orders_dataset.get("dataset_id") or "")
        assert orders_dataset_id

        create_customers = await _post_with_retry(
            client,
            f"{BFF_URL}/api/v1/pipelines/datasets",
            json_payload={
                "db_name": db_name,
                "name": customers_dataset_name,
                "description": "customers raw",
                "branch": "main",
                "source_type": "manual",
                "schema_json": customers_schema,
            },
        )
        create_customers.raise_for_status()
        customers_dataset = (create_customers.json().get("data") or {}).get("dataset") or {}
        customers_dataset_id = str(customers_dataset.get("dataset_id") or "")
        assert customers_dataset_id

        orders_version = await _post_with_retry(
            client,
            f"{BFF_URL}/api/v1/pipelines/datasets/{orders_dataset_id}/versions",
            json_payload={"sample_json": {"rows": orders_rows}, "schema_json": orders_schema},
        )
        orders_version.raise_for_status()

        customers_version = await _post_with_retry(
            client,
            f"{BFF_URL}/api/v1/pipelines/datasets/{customers_dataset_id}/versions",
            json_payload={"sample_json": {"rows": customers_rows}, "schema_json": customers_schema},
        )
        customers_version.raise_for_status()

        cleanse_definition: dict[str, Any] = {
            "nodes": [
                {
                    "id": "orders_in",
                    "type": "input",
                    "metadata": {"datasetId": orders_dataset_id, "datasetName": orders_dataset_name},
                },
                {
                    "id": "customers_in",
                    "type": "input",
                    "metadata": {"datasetId": customers_dataset_id, "datasetName": customers_dataset_name},
                },
                {
                    "id": "filter_valid",
                    "type": "transform",
                    "metadata": {
                        "operation": "filter",
                        "expression": "status != 'canceled' AND amount_raw rlike '^[0-9]+(\\\\.[0-9]+)?$'",
                    },
                },
                {"id": "dedupe_orders", "type": "transform", "metadata": {"operation": "dedupe", "columns": ["order_id"]}},
                {
                    "id": "cast_priority",
                    "type": "transform",
                    "metadata": {"operation": "cast", "casts": [{"column": "priority", "type": "int"}]},
                },
                {
                    "id": "rename_priority",
                    "type": "transform",
                    "metadata": {"operation": "rename", "rename": {"priority": "priority_level"}},
                },
                {
                    "id": "rename_amount",
                    "type": "transform",
                    "metadata": {"operation": "rename", "rename": {"amount_raw": "amount"}},
                },
                {
                    "id": "cast_amount",
                    "type": "transform",
                    "metadata": {"operation": "cast", "casts": [{"column": "amount", "type": "double"}]},
                },
                {
                    "id": "join_customers",
                    "type": "transform",
                    "metadata": {
                        "operation": "join",
                        "joinType": "left",
                        "leftKey": "customer_id",
                        "rightKey": "customer_id",
                    },
                },
                {
                    "id": "compute_name",
                    "type": "transform",
                    "metadata": {
                        "operation": "compute",
                        "expression": "customer_name = coalesce(trim(name), 'unknown')",
                    },
                },
                {
                    "id": "normalize_name",
                    "type": "transform",
                    "metadata": {"operation": "compute", "expression": "customer_name_norm = lower(customer_name)"},
                },
                {
                    "id": "select_cols",
                    "type": "transform",
                    "metadata": {
                        "operation": "select",
                        "columns": [
                            "order_id",
                            "customer_id",
                            "amount",
                            "priority_level",
                            "customer_name",
                            "customer_name_norm",
                        ],
                    },
                },
                {
                    "id": "out1",
                    "type": "output",
                    "metadata": {"datasetName": clean_output_name, "outputFormat": "json"},
                },
            ],
            "edges": [
                {"from": "orders_in", "to": "filter_valid"},
                {"from": "filter_valid", "to": "dedupe_orders"},
                {"from": "dedupe_orders", "to": "cast_priority"},
                {"from": "cast_priority", "to": "rename_priority"},
                {"from": "rename_priority", "to": "rename_amount"},
                {"from": "rename_amount", "to": "cast_amount"},
                {"from": "cast_amount", "to": "join_customers"},
                {"from": "customers_in", "to": "join_customers"},
                {"from": "join_customers", "to": "compute_name"},
                {"from": "compute_name", "to": "normalize_name"},
                {"from": "normalize_name", "to": "select_cols"},
                {"from": "select_cols", "to": "out1"},
            ],
            "parameters": [],
            "settings": {"engine": "Batch"},
        }

        cleanse_pipeline = await client.post(
            f"{BFF_URL}/api/v1/pipelines",
            json={
                "db_name": db_name,
                "name": f"cleanse-{suffix}",
                "location": "e2e",
                "description": "transform + cleansing pipeline",
                "branch": "main",
                "pipeline_type": "batch",
                "definition_json": cleanse_definition,
            },
        )
        cleanse_pipeline.raise_for_status()
        cleanse_pipeline_id = str(((cleanse_pipeline.json().get("data") or {}) or {}).get("pipeline", {}).get("pipeline_id") or "")
        assert cleanse_pipeline_id

        build_clean = await _post_with_retry(
            client,
            f"{BFF_URL}/api/v1/pipelines/{cleanse_pipeline_id}/build",
            json_payload={"db_name": db_name, "node_id": "out1", "limit": 50},
        )
        build_clean.raise_for_status()
        clean_job_id = str(((build_clean.json().get("data") or {}) or {}).get("job_id") or "")
        assert clean_job_id

        clean_run = await _wait_for_run_terminal(client, pipeline_id=cleanse_pipeline_id, job_id=clean_job_id)
        assert str(clean_run.get("status") or "").upper() == "SUCCESS"

        clean_artifact = await _wait_for_artifact(client, pipeline_id=cleanse_pipeline_id, job_id=clean_job_id)
        clean_output = _select_output(clean_artifact, clean_output_name)
        artifact_key = str(clean_output.get("artifact_commit_key") or clean_output.get("artifact_key") or "")
        assert artifact_key
        bucket, commit_id, prefix = _artifact_commit_and_prefix(artifact_key)
        rows = _load_rows_from_artifact(bucket, commit_id=commit_id, artifact_prefix=prefix)
        by_order = {row.get("order_id"): row for row in rows if isinstance(row, dict)}

        assert set(by_order.keys()) == {"o1", "o2"}
        o1 = by_order["o1"]
        o2 = by_order["o2"]
        assert float(o1.get("amount")) == pytest.approx(10.5)
        assert int(o1.get("priority_level")) == 1
        assert o1.get("customer_name") == "ALICE"
        assert o1.get("customer_name_norm") == "alice"
        assert float(o2.get("amount")) == pytest.approx(15.0)
        assert int(o2.get("priority_level")) == 2
        assert o2.get("customer_name") == "unknown"
        assert o2.get("customer_name_norm") == "unknown"

        bad_definition: dict[str, Any] = {
            "nodes": [
                {
                    "id": "orders_in",
                    "type": "input",
                    "metadata": {"datasetId": orders_dataset_id, "datasetName": orders_dataset_name},
                },
                {
                    "id": "compute_amount",
                    "type": "transform",
                    "metadata": {
                        "operation": "compute",
                        "expression": "amount = try_cast(amount_raw as double)",
                    },
                },
                {
                    "id": "out1",
                    "type": "output",
                    "metadata": {"datasetName": bad_output_name, "outputFormat": "json"},
                },
            ],
            "edges": [
                {"from": "orders_in", "to": "compute_amount"},
                {"from": "compute_amount", "to": "out1"},
            ],
            "parameters": [],
            "expectations": [{"rule": "not_null", "column": "amount"}],
            "settings": {"engine": "Batch"},
        }

        bad_pipeline = await client.post(
            f"{BFF_URL}/api/v1/pipelines",
            json={
                "db_name": db_name,
                "name": f"bad-records-{suffix}",
                "location": "e2e",
                "description": "bad record expectation failure",
                "branch": "main",
                "pipeline_type": "batch",
                "definition_json": bad_definition,
            },
        )
        bad_pipeline.raise_for_status()
        bad_pipeline_id = str(((bad_pipeline.json().get("data") or {}) or {}).get("pipeline", {}).get("pipeline_id") or "")
        assert bad_pipeline_id

        build_bad = await _post_with_retry(
            client,
            f"{BFF_URL}/api/v1/pipelines/{bad_pipeline_id}/build",
            json_payload={"db_name": db_name, "node_id": "out1", "limit": 50},
        )
        build_bad.raise_for_status()
        bad_job_id = str(((build_bad.json().get("data") or {}) or {}).get("job_id") or "")
        assert bad_job_id

        bad_run = await _wait_for_run_terminal(client, pipeline_id=bad_pipeline_id, job_id=bad_job_id)
        assert str(bad_run.get("status") or "").upper() == "FAILED"
        bad_errors = (bad_run.get("output_json") or {}).get("errors") or []
        if not bad_errors:
            bad_errors = await _wait_for_run_errors(client, pipeline_id=bad_pipeline_id, job_id=bad_job_id)
        assert any("not_null failed: amount" in str(item) for item in bad_errors), bad_errors

        schema_definition: dict[str, Any] = {
            "nodes": [
                {
                    "id": "orders_in",
                    "type": "input",
                    "metadata": {"datasetId": orders_dataset_id, "datasetName": orders_dataset_name},
                },
                {
                    "id": "out1",
                    "type": "output",
                    "metadata": {"datasetName": schema_output_name, "outputFormat": "json"},
                },
            ],
            "edges": [{"from": "orders_in", "to": "out1"}],
            "parameters": [],
            "schemaContract": [
                {"column": "missing_col", "type": "xsd:string", "required": True},
                {"column": "amount_raw", "type": "xsd:integer", "required": True},
            ],
            "settings": {"engine": "Batch"},
        }

        schema_pipeline = await client.post(
            f"{BFF_URL}/api/v1/pipelines",
            json={
                "db_name": db_name,
                "name": f"schema-mismatch-{suffix}",
                "location": "e2e",
                "description": "schema contract mismatch",
                "branch": "main",
                "pipeline_type": "batch",
                "definition_json": schema_definition,
            },
        )
        schema_pipeline.raise_for_status()
        schema_pipeline_id = str(((schema_pipeline.json().get("data") or {}) or {}).get("pipeline", {}).get("pipeline_id") or "")
        assert schema_pipeline_id

        build_schema = await _post_with_retry(
            client,
            f"{BFF_URL}/api/v1/pipelines/{schema_pipeline_id}/build",
            json_payload={"db_name": db_name, "node_id": "out1", "limit": 10},
        )
        if build_schema.status_code == 409:
            detail = build_schema.json().get("detail") if isinstance(build_schema.json(), dict) else {}
            assert detail.get("code") == "PIPELINE_PREFLIGHT_FAILED"
            issues = (detail.get("preflight") or {}).get("issues") or []
            kinds = {str(item.get("kind") or "") for item in issues if isinstance(item, dict)}
            assert "schema_contract_missing_column" in kinds, issues
            assert "schema_contract_type_mismatch" in kinds, issues
            return
        build_schema.raise_for_status()
        schema_job_id = str(((build_schema.json().get("data") or {}) or {}).get("job_id") or "")
        assert schema_job_id

        schema_run = await _wait_for_run_terminal(client, pipeline_id=schema_pipeline_id, job_id=schema_job_id)
        assert str(schema_run.get("status") or "").upper() == "FAILED"
        schema_errors = (schema_run.get("output_json") or {}).get("errors") or []
        if not schema_errors:
            schema_errors = await _wait_for_run_errors(client, pipeline_id=schema_pipeline_id, job_id=schema_job_id)
        assert any("schema contract missing column: missing_col" in str(item) for item in schema_errors), schema_errors
