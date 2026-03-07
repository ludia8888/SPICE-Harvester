from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from mcp_servers.feature_helpers import (
    build_objectify_job_status,
    build_objectify_run_body,
    build_pipeline_execution_payload,
    extract_pipeline_job_id,
    tool_error_from_upstream_response,
    unwrap_api_payload,
)


def test_build_objectify_run_body_keeps_supported_fields() -> None:
    payload = build_objectify_run_body(
        {
            "mapping_spec_id": "map-1",
            "target_class_id": "Customer",
            "dataset_version_id": "ver-1",
            "artifact_id": "artifact-1",
            "artifact_output_name": "customers",
            "max_rows": "100",
            "batch_size": 200,
            "allow_partial": True,
            "options": {"ontology_branch": "feature-1"},
        }
    )

    assert payload == {
        "mapping_spec_id": "map-1",
        "target_class_id": "Customer",
        "dataset_version_id": "ver-1",
        "artifact_id": "artifact-1",
        "artifact_output_name": "customers",
        "max_rows": 100,
        "batch_size": 200,
        "allow_partial": True,
        "options": {"ontology_branch": "feature-1"},
    }


def test_build_pipeline_execution_payload_builds_v2_shape() -> None:
    payload = build_pipeline_execution_payload(
        pipeline_id="29db2af7-84ef-4567-bebb-b457e4696d7a",
        mode="preview",
        branch="feature-1",
        limit=250,
        node_id="node-1",
    )

    assert payload == {
        "target": {
            "targetRids": ["ri.foundry.main.pipeline.29db2af7-84ef-4567-bebb-b457e4696d7a"],
        },
        "mode": "preview",
        "parameters": {
            "limit": 250,
            "nodeId": "node-1",
        },
        "branchName": "feature-1",
    }


def test_extract_pipeline_job_id_reads_rid_and_data_fallback() -> None:
    assert extract_pipeline_job_id({"rid": "build://build-job-123"}) == "build-job-123"
    assert extract_pipeline_job_id({"rid": "ri.foundry.main.build.build-job-789"}) == "build-job-789"
    assert extract_pipeline_job_id({"data": {"job_id": "job-456"}}) == "job-456"
    assert extract_pipeline_job_id({"data": {"buildRid": "ri.foundry.main.build.build-job-999"}}) == "build-job-999"
    assert extract_pipeline_job_id({"status": "accepted"}) is None


def test_unwrap_api_payload_prefers_data_object() -> None:
    assert unwrap_api_payload({"data": {"job_id": "job-1"}}) == {"job_id": "job-1"}
    assert unwrap_api_payload({"status": "success"}) == {"status": "success"}
    assert unwrap_api_payload("not-a-dict") == {}


def test_tool_error_from_upstream_response_preserves_context_and_response() -> None:
    payload = tool_error_from_upstream_response(
        {"error": "missing", "status_code": 404, "response": {"detail": "not found"}},
        default_message="fallback",
        context={"tool": "test"},
    )

    assert payload["status"] == "error"
    assert payload["error"] == "missing"
    assert payload["http_status"] == 404
    assert payload["context"]["tool"] == "test"
    assert payload["upstream_response"] == {"detail": "not found"}


def test_build_objectify_job_status_serializes_report_and_timestamps() -> None:
    now = datetime.now(timezone.utc)
    job = SimpleNamespace(
        job_id="job-123",
        status="COMPLETED",
        dataset_id="ds-1",
        target_class_id="Customer",
        created_at=now,
        updated_at=now,
        completed_at=now,
        error=None,
        report={"rows_processed": 10, "rows_failed": 1, "instances_created": 9},
    )

    payload = build_objectify_job_status(job, wait_elapsed_seconds=3.5)

    assert payload == {
        "status": "success",
        "job_id": "job-123",
        "job_status": "COMPLETED",
        "dataset_id": "ds-1",
        "target_class_id": "Customer",
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
        "completed_at": now.isoformat(),
        "error": None,
        "rows_processed": 10,
        "rows_failed": 1,
        "instances_created": 9,
        "wait_elapsed_seconds": 3.5,
    }
