"""Shared helpers for pipeline dataset uploads (BFF).

This module keeps upload routers/services thin by centralizing common HTTP-level steps:
- Enforce idempotency key
- Validate database scope and permissions
- Resolve LakeFS client/storage service for the acting user
- Provide a consistent response payload builder
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from fastapi import Request
from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.services.http_idempotency import require_idempotency_key
from shared.security.auth_utils import enforce_db_scope
from shared.security.input_sanitizer import validate_db_name
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.utils.path_utils import safe_lakefs_ref


@dataclass(frozen=True, slots=True)
class _DatasetUploadContext:
    db_name: str
    dataset_branch: str
    actor_user_id: Optional[str]
    idempotency_key: str
    lakefs_client: Any
    lakefs_storage_service: Any


async def _prepare_dataset_upload_context(
    *,
    request: Optional[Request],
    db_name: str,
    branch: Optional[str],
    pipeline_registry: PipelineRegistry,
) -> _DatasetUploadContext:
    if request is None:
        raise classified_http_exception(400, "Request context is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
    idempotency_key = require_idempotency_key(request)

    db_name = validate_db_name(db_name)
    try:
        enforce_db_scope(request.headers, db_name=db_name)
    except ValueError as exc:
        raise classified_http_exception(403, str(exc), code=ErrorCode.PERMISSION_DENIED) from exc

    actor_user_id = (request.headers.get("X-User-ID") or "").strip() or None
    lakefs_storage_service = await pipeline_registry.get_lakefs_storage(user_id=actor_user_id)
    lakefs_client = await pipeline_registry.get_lakefs_client(user_id=actor_user_id)

    dataset_branch = safe_lakefs_ref((branch or "main").strip() or "main")

    return _DatasetUploadContext(
        db_name=db_name,
        dataset_branch=dataset_branch,
        actor_user_id=actor_user_id,
        idempotency_key=idempotency_key,
        lakefs_client=lakefs_client,
        lakefs_storage_service=lakefs_storage_service,
    )


def _build_dataset_upload_response(
    *,
    result: Any,
    preview: Dict[str, Any],
    source: Any,
    funnel_analysis: Any,
    schema_json: Dict[str, Any],
) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "dataset": result.dataset.__dict__,
        "version": result.version.__dict__,
        "preview": preview,
        "source": source,
        "funnel_analysis": funnel_analysis,
        "ingest_request_id": result.ingest_request.ingest_request_id,
        "schema_status": getattr(result.ingest_request, "schema_status", "PENDING"),
        "schema_suggestion": schema_json,
    }
    objectify_job_id = getattr(result, "objectify_job_id", None)
    if objectify_job_id:
        data["objectify_job_id"] = objectify_job_id
    return data

