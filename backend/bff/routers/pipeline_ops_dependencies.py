"""Pipeline Builder dependency helpers.

Normalize/validate dependency payloads.
Extracted from `bff.routers.pipeline_ops`.
"""

from __future__ import annotations

from typing import Any, Optional
from uuid import UUID

from fastapi import HTTPException, status

from shared.services.pipeline.pipeline_dependency_utils import normalize_dependency_entries
from shared.services.registries.pipeline_registry import PipelineRegistry

_DEPENDENCY_STATUS_ALLOWLIST = {"DEPLOYED", "SUCCESS"}


def _normalize_dependencies_payload(raw: Any) -> list[dict[str, str]]:
    try:
        entries, _ = normalize_dependency_entries(raw, strict=True)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    normalized: dict[str, str] = {}
    for item in entries:
        pipeline_id = item["pipeline_id"]
        status_value = item["status"]
        try:
            UUID(pipeline_id)
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"dependency pipeline_id must be UUID: {pipeline_id}",
            )
        if status_value not in _DEPENDENCY_STATUS_ALLOWLIST:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"dependency status must be one of {sorted(_DEPENDENCY_STATUS_ALLOWLIST)}",
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
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="pipeline cannot depend on itself")
        record = await pipeline_registry.get_pipeline(pipeline_id=dep_id)
        if not record:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"dependency pipeline not found: {dep_id}")
        if str(record.db_name) != str(db_name):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"dependency pipeline must be in same db (expected {db_name} got {record.db_name})",
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

