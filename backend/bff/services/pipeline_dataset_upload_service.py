"""
Pipeline dataset upload domain logic (BFF).

Extracted from `bff.routers.pipeline_datasets_uploads` to keep routers thin and
deduplicate the common ingest workflow (Template Method + Strategy).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

from fastapi import HTTPException
from shared.errors.error_types import ErrorCode, classified_http_exception

import bff.routers.pipeline_datasets_ops as ops
from bff.services.dataset_ingest_commit_service import (
    ensure_lakefs_commit_artifact,
    persist_ingest_commit_state,
)
from bff.services.dataset_ingest_idempotency import resolve_existing_version_or_raise
from bff.services.dataset_ingest_outbox_builder import DatasetIngestOutboxBuilder
from bff.services.dataset_ingest_outbox_flusher import maybe_flush_dataset_ingest_outbox_inline
from bff.services.dataset_ingest_failures import mark_ingest_failed
from shared.observability.tracing import trace_external_call

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TabularDatasetUploadInput:
    db_name: str
    dataset_branch: str
    dataset_name: str
    description: Optional[str]
    source_type: str
    source_ref: str
    idempotency_key: str
    actor_user_id: Optional[str]
    request_fingerprint_payload: Dict[str, Any]
    schema_json: Dict[str, Any]
    sample_json: Dict[str, Any]
    row_count: Optional[int]
    source_metadata: Optional[Dict[str, Any]]
    artifact_fileobj: Any
    artifact_basename: str
    artifact_content_type: str
    content_sha256: str
    commit_message: str
    commit_metadata_extra: Dict[str, Any]
    lineage_label: str


@dataclass(frozen=True)
class TabularDatasetUploadResult:
    dataset: Any
    version: Any
    ingest_request: Any
    objectify_job_id: Optional[str]
    reused_existing_version: bool


async def _save_artifact(
    *,
    lakefs_storage_service: Any,
    repo: str,
    key: str,
    fileobj: Any,
    content_type: str,
    metadata: Dict[str, Any],
    checksum: Optional[str],
) -> None:
    try:
        fileobj.seek(0)
    except Exception:
        pass

    save_fileobj = getattr(lakefs_storage_service, "save_fileobj", None)
    if callable(save_fileobj):
        await save_fileobj(
            repo,
            key,
            fileobj,
            content_type=content_type,
            metadata=metadata,
            checksum=checksum,
        )
        return

    save_bytes = getattr(lakefs_storage_service, "save_bytes", None)
    if not callable(save_bytes):
        raise RuntimeError("lakeFS storage service missing save_fileobj/save_bytes")

    content = await asyncio.to_thread(fileobj.read)
    await save_bytes(
        repo,
        key,
        content,
        content_type=content_type,
        metadata=metadata,
    )


@trace_external_call("bff.pipeline_dataset_upload.upload_tabular_dataset")
async def upload_tabular_dataset(
    *,
    inputs: TabularDatasetUploadInput,
    lakefs_client: Any,
    lakefs_storage_service: Any,
    dataset_registry: Any,
    objectify_registry: Any,
    objectify_job_queue: Any,
    lineage_store: Any,
    build_dataset_event_payload: Any,
    flush_dataset_ingest_outbox: Any,
) -> TabularDatasetUploadResult:
    ingest_request = None
    try:
        dataset = await dataset_registry.get_dataset_by_name(
            db_name=inputs.db_name,
            name=inputs.dataset_name,
            branch=inputs.dataset_branch,
        )
        created_dataset = False
        if not dataset:
            dataset = await dataset_registry.create_dataset(
                db_name=inputs.db_name,
                name=inputs.dataset_name,
                description=inputs.description,
                source_type=inputs.source_type,
                source_ref=inputs.source_ref,
                schema_json={},
                branch=inputs.dataset_branch,
            )
            created_dataset = True

        fingerprint_payload = dict(inputs.request_fingerprint_payload or {})
        fingerprint_payload["dataset_id"] = dataset.dataset_id
        request_fingerprint = ops._build_ingest_request_fingerprint(fingerprint_payload)
        ingest_request, _ = await dataset_registry.create_ingest_request(
            dataset_id=dataset.dataset_id,
            db_name=inputs.db_name,
            branch=inputs.dataset_branch,
            idempotency_key=inputs.idempotency_key,
            request_fingerprint=request_fingerprint,
            schema_json=inputs.schema_json,
            sample_json=inputs.sample_json,
            row_count=inputs.row_count,
            source_metadata=inputs.source_metadata,
        )
        ingest_transaction = await ops._ensure_ingest_transaction(
            dataset_registry,
            ingest_request_id=ingest_request.ingest_request_id,
        )
        existing_version = await resolve_existing_version_or_raise(
            dataset_registry=dataset_registry,
            ingest_request=ingest_request,
            expected_dataset_id=str(dataset.dataset_id),
            request_fingerprint=str(request_fingerprint),
        )
        if existing_version:
            await maybe_flush_dataset_ingest_outbox_inline(
                dataset_registry=dataset_registry,
                lineage_store=lineage_store,
                flush_dataset_ingest_outbox=flush_dataset_ingest_outbox,
            )
            return TabularDatasetUploadResult(
                dataset=dataset,
                version=existing_version,
                ingest_request=ingest_request,
                objectify_job_id=None,
                reused_existing_version=True,
            )

        commit_id = ingest_request.lakefs_commit_id
        artifact_key = ingest_request.artifact_key
        if not commit_id or not artifact_key:
            repo = ops._resolve_lakefs_raw_repository()
            prefix = ops._dataset_artifact_prefix(
                db_name=inputs.db_name,
                dataset_id=dataset.dataset_id,
                dataset_name=dataset.name,
            )
            staging_prefix = ops._ingest_staging_prefix(prefix, ingest_request.ingest_request_id)
            object_key = f"{staging_prefix}/{inputs.artifact_basename}"
            await _save_artifact(
                lakefs_storage_service=lakefs_storage_service,
                repo=repo,
                key=f"{inputs.dataset_branch}/{object_key}",
                fileobj=inputs.artifact_fileobj,
                content_type=inputs.artifact_content_type,
                metadata=ops._sanitize_s3_metadata(
                    {
                        "db_name": inputs.db_name,
                        "dataset_name": inputs.dataset_name,
                        "source": inputs.source_type,
                        "ingest_request_id": ingest_request.ingest_request_id,
                    }
                ),
                checksum=inputs.content_sha256,
            )
            transaction_id = ingest_transaction.transaction_id if ingest_transaction else None
            commit_state = await ensure_lakefs_commit_artifact(
                ingest_request=ingest_request,
                initial_commit_id=commit_id,
                initial_artifact_key=artifact_key,
                lakefs_client=lakefs_client,
                lakefs_storage_service=lakefs_storage_service,
                repository=repo,
                branch=inputs.dataset_branch,
                commit_message=inputs.commit_message,
                commit_metadata={
                    "dataset_id": dataset.dataset_id,
                    "db_name": inputs.db_name,
                    "dataset_name": inputs.dataset_name,
                    "source_type": inputs.source_type,
                    "filename": inputs.source_ref,
                    "ingest_request_id": ingest_request.ingest_request_id,
                    "transaction_id": transaction_id,
                    "content_sha256": inputs.content_sha256,
                    **(inputs.commit_metadata_extra or {}),
                },
                object_key=object_key,
                expected_checksum=inputs.content_sha256,
            )
            commit_id = commit_state.commit_id
            artifact_key = commit_state.artifact_key
            ingest_request = await persist_ingest_commit_state(
                dataset_registry=dataset_registry,
                ingest_request=ingest_request,
                ingest_transaction=ingest_transaction,
                commit_id=commit_id,
                artifact_key=artifact_key,
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
                    db_name=inputs.db_name,
                    name=inputs.dataset_name,
                    actor=inputs.actor_user_id,
                    transaction_id=transaction_id,
                )
            )
        version_event_id = ingest_request.ingest_request_id
        outbox_entries.append(
            outbox_builder.version_created(
                event_id=str(version_event_id),
                dataset_id=dataset.dataset_id,
                db_name=inputs.db_name,
                name=inputs.dataset_name,
                actor=inputs.actor_user_id,
                command_type="INGEST_DATASET_SNAPSHOT",
                lakefs_commit_id=commit_id,
                artifact_key=artifact_key,
                transaction_id=transaction_id,
            )
        )
        lineage_entry = outbox_builder.artifact_stored_lineage(
            version_event_id=str(version_event_id),
            artifact_key=artifact_key,
            db_name=inputs.db_name,
            from_label=inputs.lineage_label,
            edge_metadata={
                "db_name": inputs.db_name,
                "dataset_id": dataset.dataset_id,
                "dataset_name": inputs.dataset_name,
                "source": inputs.source_type,
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
                row_count=inputs.row_count,
                sample_json=inputs.sample_json,
                schema_json=inputs.schema_json,
                apply_schema=False,
                outbox_entries=outbox_entries,
            )
        except ValueError as exc:
            raise classified_http_exception(400, str(exc), code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc

        objectify_job_id = await ops._maybe_enqueue_objectify_job(
            dataset=dataset,
            version=version,
            objectify_registry=objectify_registry,
            job_queue=objectify_job_queue,
            dataset_registry=dataset_registry,
            actor_user_id=inputs.actor_user_id,
        )
        await maybe_flush_dataset_ingest_outbox_inline(
            dataset_registry=dataset_registry,
            lineage_store=lineage_store,
            flush_dataset_ingest_outbox=flush_dataset_ingest_outbox,
        )
        return TabularDatasetUploadResult(
            dataset=dataset,
            version=version,
            ingest_request=ingest_request,
            objectify_job_id=objectify_job_id,
            reused_existing_version=False,
        )
    except HTTPException:
        raise
    except Exception as exc:
        await mark_ingest_failed(
            dataset_registry=dataset_registry,
            ingest_request=ingest_request,
            error=str(exc),
            stage=inputs.source_type,
        )
        raise
