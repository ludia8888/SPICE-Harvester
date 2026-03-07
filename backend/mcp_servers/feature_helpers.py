from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from shared.errors.error_types import ErrorCategory, ErrorCode
from shared.foundry.payload_ids import extract_pipeline_job_id as extract_pipeline_job_id_from_payload
from shared.foundry.rids import build_rid
from mcp_servers.pipeline_mcp_errors import tool_error


def build_objectify_run_body(arguments: Dict[str, Any]) -> Dict[str, Any]:
    body: Dict[str, Any] = {}

    for source_key in ("mapping_spec_id", "target_class_id", "dataset_version_id", "artifact_id", "artifact_output_name"):
        value = str(arguments.get(source_key) or "").strip()
        if value:
            body[source_key] = value

    for int_key in ("max_rows", "batch_size"):
        value = arguments.get(int_key)
        if value is not None:
            body[int_key] = int(value)

    if "allow_partial" in arguments:
        body["allow_partial"] = bool(arguments.get("allow_partial"))

    options = arguments.get("options")
    if isinstance(options, dict) and options:
        body["options"] = options

    return body


def unwrap_api_payload(response: Any) -> Dict[str, Any]:
    if not isinstance(response, dict):
        return {}
    data = response.get("data")
    if isinstance(data, dict):
        return data
    return response


def tool_error_from_upstream_response(
    response: Dict[str, Any],
    *,
    default_message: str,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    status_code = int(response.get("status_code") or 502)
    message = str(response.get("error") or default_message)
    if status_code == 404:
        code = ErrorCode.RESOURCE_NOT_FOUND
        category = ErrorCategory.RESOURCE
    elif status_code == 409:
        code = ErrorCode.CONFLICT
        category = ErrorCategory.CONFLICT
    elif status_code == 403:
        code = ErrorCode.PERMISSION_DENIED
        category = ErrorCategory.PERMISSION
    elif status_code == 401:
        code = ErrorCode.AUTH_INVALID
        category = ErrorCategory.AUTH
    elif status_code >= 500:
        code = ErrorCode.UPSTREAM_ERROR
        category = ErrorCategory.UPSTREAM
    else:
        code = ErrorCode.REQUEST_VALIDATION_FAILED
        category = ErrorCategory.INPUT
    payload = tool_error(
        message,
        status_code=status_code,
        code=code,
        category=category,
        context=context,
    )
    upstream_response = response.get("response")
    if upstream_response is not None:
        payload["upstream_response"] = upstream_response
    return payload


def build_objectify_job_status(
    job: Any,
    *,
    status: str = "success",
    wait_elapsed_seconds: Optional[float] = None,
) -> Dict[str, Any]:
    report = getattr(job, "report", None) or {}
    payload = {
        "status": status,
        "job_id": getattr(job, "job_id", None),
        "job_status": getattr(job, "status", None),
        "dataset_id": getattr(job, "dataset_id", None),
        "target_class_id": getattr(job, "target_class_id", None),
        "created_at": _to_iso(getattr(job, "created_at", None)),
        "updated_at": _to_iso(getattr(job, "updated_at", None)),
        "completed_at": _to_iso(getattr(job, "completed_at", None)),
        "error": getattr(job, "error", None),
        "rows_processed": report.get("rows_processed"),
        "rows_failed": report.get("rows_failed"),
        "instances_created": report.get("instances_created"),
    }
    if wait_elapsed_seconds is not None:
        payload["wait_elapsed_seconds"] = wait_elapsed_seconds
    return payload


def build_pipeline_execution_payload(
    *,
    pipeline_id: str,
    mode: str,
    branch: Optional[str] = None,
    limit: Optional[int] = None,
    node_id: Optional[str] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "target": {"targetRids": [build_rid("pipeline", pipeline_id)]},
        "mode": str(mode or "").strip(),
    }

    parameters: Dict[str, Any] = {}
    if limit is not None:
        parameters["limit"] = int(limit)
    if node_id:
        parameters["nodeId"] = str(node_id).strip()
    if parameters:
        payload["parameters"] = parameters

    branch_name = str(branch or "").strip()
    if branch_name:
        payload["branchName"] = branch_name

    return payload


def extract_pipeline_job_id(response: Dict[str, Any]) -> Optional[str]:
    return extract_pipeline_job_id_from_payload(response, nested_fields=("data",))


def _to_iso(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return value.isoformat()
    return None
