"""Pipeline dataset media upload domain logic (BFF).

Extracted from `bff.routers.pipeline_datasets_uploads_media` to keep routers thin.
"""

import hashlib
import logging
from typing import Any, Optional
from uuid import uuid4

from fastapi import HTTPException, status

import bff.routers.pipeline_datasets_ops as ops
from bff.services.pipeline_dataset_upload_context import _prepare_dataset_upload_context
from bff.services.dataset_ingest_outbox_builder import DatasetIngestOutboxBuilder
from bff.services.dataset_ingest_failures import mark_ingest_failed
from shared.models.requests import ApiResponse
from shared.utils.path_utils import safe_path_segment
from shared.utils.s3_uri import build_s3_uri

logger = logging.getLogger(__name__)


async def upload_media_dataset(
    *,
    db_name: str,
    branch: Optional[str],
    files: list[Any],
    dataset_name: Optional[str],
    description: Optional[str],
    request: Any,
    pipeline_registry: Any,
    dataset_registry: Any,
    objectify_registry: Any,
    objectify_job_queue: Any,
    lineage_store: Any,
    flush_dataset_ingest_outbox: Any,
    build_dataset_event_payload: Any,
) -> dict[str, Any]:
    ingest_request = None
    ingest_transaction = None
    try:
        ctx = await _prepare_dataset_upload_context(
            request=request,
            db_name=db_name,
            branch=branch,
            pipeline_registry=pipeline_registry,
        )
        actor_user_id = ctx.actor_user_id
        lakefs_storage_service = ctx.lakefs_storage_service
        lakefs_client = ctx.lakefs_client
        idempotency_key = ctx.idempotency_key
        db_name = ctx.db_name
        dataset_branch = ctx.dataset_branch

        resolved_name = (dataset_name or "").strip()
        if not resolved_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="dataset_name is required")

        if not files:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="files are required")

        uploaded: list[dict[str, Any]] = []
        for file in files:
            if not file:
                continue
            filename = (getattr(file, "filename", None) or "").strip()
            if not filename:
                filename = f"upload-{uuid4().hex}"
            content = await file.read()
            if not content:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Empty file: {filename}")
            content_hash = hashlib.sha256(content).hexdigest()
            uploaded.append(
                {
                    "filename": filename,
                    "content_type": (getattr(file, "content_type", None) or "application/octet-stream").strip()
                    or "application/octet-stream",
                    "size_bytes": len(content),
                    "content": content,
                    "content_sha256": content_hash,
                }
            )

        if not uploaded:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No valid files uploaded")

        schema_columns = [
            {"name": "s3_uri", "type": "xsd:string"},
            {"name": "filename", "type": "xsd:string"},
            {"name": "content_type", "type": "xsd:string"},
            {"name": "size_bytes", "type": "xsd:integer"},
        ]
        row_count = len(uploaded)
        ingest_sample_json = {"columns": schema_columns, "rows": []}

        dataset = await dataset_registry.get_dataset_by_name(
            db_name=db_name,
            name=resolved_name,
            branch=dataset_branch,
        )
        created_dataset = False
        if not dataset:
            dataset = await dataset_registry.create_dataset(
                db_name=db_name,
                name=resolved_name,
                description=(description or "").strip() or None,
                source_type="media",
                source_ref=None,
                schema_json={"columns": schema_columns},
                branch=dataset_branch,
            )
            created_dataset = True

        safe_name = (dataset.name or "media").replace(" ", "_")
        prefix = f"datasets-media/{db_name}/{dataset.dataset_id}/{safe_path_segment(safe_name)}"
        request_fingerprint = ops._build_ingest_request_fingerprint(
            {
                "db_name": db_name,
                "branch": dataset_branch,
                "dataset_id": dataset.dataset_id,
                "dataset_name": resolved_name,
                "source_type": "media_upload",
                "files": [
                    {
                        "filename": item["filename"],
                        "size_bytes": item["size_bytes"],
                        "content_sha256": item["content_sha256"],
                    }
                    for item in uploaded
                ],
            }
        )
        ingest_request, _ = await dataset_registry.create_ingest_request(
            dataset_id=dataset.dataset_id,
            db_name=db_name,
            branch=dataset_branch,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
            schema_json={"columns": schema_columns},
            sample_json=ingest_sample_json,
            row_count=row_count,
            source_metadata={"source_type": "media_upload", "file_count": row_count},
        )
        ingest_transaction = await ops._ensure_ingest_transaction(
            dataset_registry,
            ingest_request_id=ingest_request.ingest_request_id,
        )
        if ingest_request.dataset_id != dataset.dataset_id:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency key already used for a different dataset",
            )
        if ingest_request.request_fingerprint and ingest_request.request_fingerprint != request_fingerprint:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency key reuse detected with different payload",
            )
        if ingest_request.status == "FAILED":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=ingest_request.error or "Previous ingest failed",
            )

        if ingest_request.status == "PUBLISHED":
            existing_version = await dataset_registry.get_version_by_ingest_request(
                ingest_request_id=ingest_request.ingest_request_id
            )
            if existing_version:
                await flush_dataset_ingest_outbox(
                    dataset_registry=dataset_registry,
                    lineage_store=lineage_store,
                )
                return ApiResponse.success(
                    message="Media dataset created",
                    data={
                        "dataset": dataset.__dict__,
                        "version": existing_version.__dict__,
                        "preview": {"columns": schema_columns, "rows": []},
                        "artifact_key": existing_version.artifact_key,
                    },
                ).to_dict()

        repo = ops._resolve_lakefs_raw_repository()
        await ops._ensure_lakefs_branch_exists(
            lakefs_client=lakefs_client,
            repository=repo,
            branch=dataset_branch,
            source_branch="main",
        )
        commit_id = ingest_request.lakefs_commit_id
        artifact_key = ingest_request.artifact_key
        staging_prefix = ops._ingest_staging_prefix(prefix, ingest_request.ingest_request_id)
        if not commit_id or not artifact_key:
            fallback_object_key = None
            fallback_checksum = None
            for index, item in enumerate(uploaded):
                filename = str(item.get("filename") or f"upload-{index}")
                content_type = str(item.get("content_type") or "application/octet-stream")
                blob = item.get("content") or b""
                object_key = f"{staging_prefix}/{index:04d}_{filename}"
                if index == 0:
                    fallback_object_key = object_key
                    fallback_checksum = str(item.get("content_sha256") or "").strip() or None
                await lakefs_storage_service.save_bytes(
                    repo,
                    f"{dataset_branch}/{object_key}",
                    blob,
                    content_type=content_type,
                    metadata=ops._sanitize_s3_metadata(
                        {
                            "db_name": db_name,
                            "dataset_id": dataset.dataset_id,
                            "dataset_name": dataset.name,
                            "source": "media_upload",
                            "content_type": content_type,
                            "ingest_request_id": ingest_request.ingest_request_id,
                        }
                    ),
                )

            commit_id = await ops._commit_lakefs_with_predicate_fallback(
                lakefs_client=lakefs_client,
                lakefs_storage_service=lakefs_storage_service,
                repository=repo,
                branch=dataset_branch,
                message=f"Media dataset upload {db_name}/{resolved_name}",
                metadata={
                    "dataset_id": dataset.dataset_id,
                    "db_name": db_name,
                    "dataset_name": resolved_name,
                    "source_type": "media_upload",
                    "file_count": len(uploaded),
                    "ingest_request_id": ingest_request.ingest_request_id,
                    "transaction_id": ingest_transaction.transaction_id if ingest_transaction else None,
                },
                object_key=fallback_object_key or staging_prefix,
                expected_checksum=fallback_checksum,
            )

            artifact_key = build_s3_uri(repo, f"{commit_id}/{staging_prefix}")
            ingest_request = await dataset_registry.mark_ingest_committed(
                ingest_request_id=ingest_request.ingest_request_id,
                lakefs_commit_id=commit_id,
                artifact_key=artifact_key,
            )
            if ingest_transaction:
                await dataset_registry.mark_ingest_transaction_committed(
                    ingest_request_id=ingest_request.ingest_request_id,
                    lakefs_commit_id=commit_id,
                    artifact_key=artifact_key,
                )
        sample_rows: list[dict[str, Any]] = []
        for index, item in enumerate(uploaded):
            filename = str(item.get("filename") or f"upload-{index}")
            content_type = str(item.get("content_type") or "application/octet-stream")
            size_bytes = int(item.get("size_bytes") or 0)
            object_key = f"{staging_prefix}/{index:04d}_{filename}"
            sample_rows.append(
                {
                    "s3_uri": build_s3_uri(repo, f"{commit_id}/{object_key}"),
                    "filename": filename,
                    "content_type": content_type,
                    "size_bytes": size_bytes,
                }
            )

        await dataset_registry.update_ingest_request_payload(
            ingest_request_id=ingest_request.ingest_request_id,
            sample_json={"columns": schema_columns, "rows": sample_rows},
            row_count=row_count,
        )

        outbox_entries: list[dict[str, Any]] = []
        transaction_id = ingest_transaction.transaction_id if ingest_transaction else None
        outbox_builder = DatasetIngestOutboxBuilder(
            build_dataset_event_payload=build_dataset_event_payload,
            lineage_store=lineage_store,
        )
        if created_dataset:
            outbox_entries.append(
                outbox_builder.dataset_created(
                    dataset_id=dataset.dataset_id,
                    db_name=db_name,
                    name=resolved_name,
                    actor=actor_user_id,
                    transaction_id=transaction_id,
                )
            )
        version_event_id = ingest_request.ingest_request_id
        outbox_entries.append(
            outbox_builder.version_created(
                event_id=str(version_event_id),
                dataset_id=dataset.dataset_id,
                db_name=db_name,
                name=resolved_name,
                actor=actor_user_id,
                command_type="INGEST_MEDIA_SET",
                lakefs_commit_id=commit_id,
                artifact_key=artifact_key,
                transaction_id=transaction_id,
                extra_data={"source_type": "media"},
            )
        )
        lineage_entry = outbox_builder.artifact_stored_lineage(
            version_event_id=str(version_event_id),
            artifact_key=artifact_key,
            db_name=db_name,
            from_label="media_upload",
            edge_metadata={
                "db_name": db_name,
                "dataset_id": dataset.dataset_id,
                "dataset_name": resolved_name,
                "source": "media_upload",
                "file_count": row_count,
                "transaction_id": transaction_id,
            },
        )
        if lineage_entry:
            outbox_entries.append(lineage_entry)

        try:
            version = await dataset_registry.publish_ingest_request(
                ingest_request_id=ingest_request.ingest_request_id,
                dataset_id=dataset.dataset_id,
                lakefs_commit_id=commit_id or "",
                artifact_key=artifact_key,
                row_count=row_count,
                sample_json={
                    "columns": schema_columns,
                    "rows": sample_rows[: min(50, len(sample_rows))],
                    "row_count": row_count,
                    "sample_row_count": min(50, len(sample_rows)),
                },
                schema_json={"columns": schema_columns},
                outbox_entries=outbox_entries,
            )
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

        objectify_job_id = await ops._maybe_enqueue_objectify_job(
            dataset=dataset,
            version=version,
            objectify_registry=objectify_registry,
            job_queue=objectify_job_queue,
            dataset_registry=dataset_registry,
            actor_user_id=actor_user_id,
        )

        await flush_dataset_ingest_outbox(
            dataset_registry=dataset_registry,
            lineage_store=lineage_store,
        )

        return ApiResponse.success(
            message="Media dataset created",
            data={
                "dataset": dataset.__dict__,
                "version": version.__dict__,
                "objectify_job_id": objectify_job_id,
                "preview": {
                    "columns": ops._columns_from_schema(schema_columns),
                    "rows": sample_rows[: min(6, len(sample_rows))],
                },
                "artifact_key": artifact_key,
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        await mark_ingest_failed(
            dataset_registry=dataset_registry,
            ingest_request=ingest_request,
            error=str(exc),
            stage="media",
        )
        logger.error("Failed to upload media dataset: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
