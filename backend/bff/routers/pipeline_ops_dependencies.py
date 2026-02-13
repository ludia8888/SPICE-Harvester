"""Pipeline Builder dependency helpers.

Normalize/validate dependency payloads.
Extracted from `bff.routers.pipeline_ops`.
"""


from typing import Any, Optional
from uuid import UUID

from fastapi import status
from shared.errors.error_types import ErrorCode, classified_http_exception

from shared.services.pipeline.pipeline_dependency_utils import normalize_dependency_entries
from shared.services.registries.pipeline_registry import PipelineRegistry

_DEPENDENCY_STATUS_ALLOWLIST = {"DEPLOYED", "SUCCESS"}


def _normalize_dependencies_payload(raw: Any) -> list[dict[str, str]]:
    try:
        entries, _ = normalize_dependency_entries(raw, strict=True)
    except ValueError as exc:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(exc), code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc

    normalized: dict[str, str] = {}
    for item in entries:
        pipeline_id = item["pipeline_id"]
        status_value = item["status"]
        try:
            UUID(pipeline_id)
        except Exception:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                f"dependency pipeline_id must be UUID: {pipeline_id}",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )
        if status_value not in _DEPENDENCY_STATUS_ALLOWLIST:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                f"dependency status must be one of {sorted(_DEPENDENCY_STATUS_ALLOWLIST)}",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )
        normalized[pipeline_id] = status_value
    return [{"pipeline_id": pipeline_id, "status": status_value} for pipeline_id, status_value in normalized.items()]


async def _validate_dependency_targets(
    pipeline_registry: PipelineRegistry,
    *,
    db_name: str,
    pipeline_id: Optional[str],
    dependencies: list[dict[str, str]],
) -> None:
    for dep in dependencies:
        dep_id = str(dep.get("pipeline_id") or "").strip()
        if not dep_id:
            continue
        if pipeline_id and dep_id == str(pipeline_id):
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "pipeline cannot depend on itself", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        record = await pipeline_registry.get_pipeline(pipeline_id=dep_id)
        if not record:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, f"dependency pipeline not found: {dep_id}", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        if str(record.db_name) != str(db_name):
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                f"dependency pipeline must be in same db (expected {db_name} got {record.db_name})",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )


def _format_dependencies_for_api(dependencies: list[dict[str, str]]) -> list[dict[str, str]]:
    output: list[dict[str, str]] = []
    for dep in dependencies or []:
        if not isinstance(dep, dict):
            continue
        pipeline_id = str(dep.get("pipeline_id") or "").strip()
        if not pipeline_id:
            continue
        status_value = str(dep.get("status") or "DEPLOYED").strip().upper()
        output.append({"pipelineId": pipeline_id, "status": status_value})
    return output

