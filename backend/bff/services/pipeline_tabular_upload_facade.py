from __future__ import annotations

from typing import Any, Dict, Optional

import bff.routers.pipeline_datasets_ops as ops
from bff.services.pipeline_dataset_upload_context import _DatasetUploadContext, _build_dataset_upload_response
from bff.services.pipeline_dataset_upload_service import TabularDatasetUploadInput, upload_tabular_dataset
from shared.models.requests import ApiResponse


async def finalize_tabular_upload(
    *,
    ctx: _DatasetUploadContext,
    dataset_name: str,
    description: Optional[str],
    source_type: str,
    source_ref: str,
    request_fingerprint_payload: Dict[str, Any],
    schema_json: Dict[str, Any],
    sample_json: Dict[str, Any],
    row_count: Optional[int],
    source_metadata: Optional[Dict[str, Any]],
    artifact_fileobj: Any,
    artifact_basename: str,
    artifact_content_type: str,
    content_sha256: str,
    commit_message: str,
    commit_metadata_extra: Dict[str, Any],
    lineage_label: str,
    dataset_registry: Any,
    objectify_registry: Any,
    objectify_job_queue: Any,
    lineage_store: Any,
    preview_payload: Dict[str, Any],
    funnel_analysis: Any,
    success_message: str,
) -> Dict[str, Any]:
    result = await upload_tabular_dataset(
        inputs=TabularDatasetUploadInput(
            db_name=ctx.db_name,
            dataset_branch=ctx.dataset_branch,
            dataset_name=dataset_name,
            description=description.strip() if description else None,
            source_type=source_type,
            source_ref=source_ref,
            idempotency_key=ctx.idempotency_key,
            actor_user_id=ctx.actor_user_id,
            request_fingerprint_payload=request_fingerprint_payload,
            schema_json=schema_json,
            sample_json=sample_json,
            row_count=row_count,
            source_metadata=source_metadata,
            artifact_fileobj=artifact_fileobj,
            artifact_basename=artifact_basename,
            artifact_content_type=artifact_content_type,
            content_sha256=content_sha256,
            commit_message=commit_message,
            commit_metadata_extra=commit_metadata_extra,
            lineage_label=lineage_label,
        ),
        lakefs_client=ctx.lakefs_client,
        lakefs_storage_service=ctx.lakefs_storage_service,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        objectify_job_queue=objectify_job_queue,
        lineage_store=lineage_store,
        build_dataset_event_payload=ops.build_dataset_event_payload,
        flush_dataset_ingest_outbox=ops.flush_dataset_ingest_outbox,
    )

    data = _build_dataset_upload_response(
        result=result,
        preview=preview_payload,
        source=source_metadata,
        funnel_analysis=funnel_analysis,
        schema_json=schema_json,
    )

    return ApiResponse.success(
        message=success_message,
        data=data,
    ).to_dict()

