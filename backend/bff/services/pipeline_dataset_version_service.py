"""Pipeline dataset version domain logic (BFF).

Extracted from `bff.routers.pipeline_datasets_versions` to keep routers thin.
"""

import logging
from typing import Any, Dict
from uuid import uuid4

from fastapi import HTTPException, Request, status

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
from shared.models.requests import ApiResponse
from shared.security.auth_utils import enforce_db_scope
from shared.security.input_sanitizer import sanitize_input
from shared.utils.path_utils import safe_lakefs_ref
from shared.utils.s3_uri import build_s3_uri
from shared.observability.tracing import trace_external_call

logger = logging.getLogger(__name__)


@trace_external_call("bff.pipeline_dataset_version.create_dataset_version")
async def create_dataset_version(
    *,
    dataset_id: str,
    payload: Dict[str, Any],
    request: Request,
    pipeline_registry: Any,
    dataset_registry: Any,
    objectify_registry: Any,
    objectify_job_queue: Any,
    lineage_store: Any,
    flush_dataset_ingest_outbox: Any,
    build_dataset_event_payload: Any,
) -> Dict[str, Any]:
    ingest_request = None
    ingest_transaction = None
    try:
        actor_user_id = (request.headers.get("X-User-ID") or "").strip() or None
        lakefs_storage_service = await pipeline_registry.get_lakefs_storage(user_id=actor_user_id)
        lakefs_client = await pipeline_registry.get_lakefs_client(user_id=actor_user_id)
        idempotency_key = (request.headers.get("Idempotency-Key") or "").strip()
        if not idempotency_key:
            idempotency_key = f"manual-{uuid4().hex}"

        sanitized = sanitize_input(payload)
        sample_json = sanitized.get("sample_json") if isinstance(sanitized.get("sample_json"), dict) else {}
        schema_json = sanitized.get("schema_json") if isinstance(sanitized.get("schema_json"), dict) else None
        row_count = None
        if sanitized.get("row_count") is not None:
            try:
                row_count = int(sanitized.get("row_count"))
            except Exception as exc:
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "row_count must be integer", code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc
        if row_count is None and isinstance(sample_json, dict):
            rows = sample_json.get("rows")
            if isinstance(rows, list):
                row_count = len(rows)

        artifact_key = str(sanitized.get("artifact_key") or "").strip() or None
        lakefs_commit_id = str(
            sanitized.get("lakefs_commit_id") or sanitized.get("lakefsCommitId") or sanitized.get("commit_id") or ""
        ).strip() or None

        dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
        if not dataset:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Dataset not found", code=ErrorCode.RESOURCE_NOT_FOUND)
        try:
            enforce_db_scope(request.headers, db_name=dataset.db_name)
        except ValueError as exc:
            raise classified_http_exception(status.HTTP_403_FORBIDDEN, str(exc), code=ErrorCode.PERMISSION_DENIED) from exc

        dataset_branch = safe_lakefs_ref(getattr(dataset, "branch", None) or "main")
        if not hasattr(dataset_registry, "create_ingest_request"):
            if not artifact_key:
                repo = ops._resolve_lakefs_raw_repository()
                await ops._ensure_lakefs_branch_exists(
                    lakefs_client=lakefs_client,
                    repository=repo,
                    branch=dataset_branch,
                    source_branch="main",
                )
                prefix = ops._dataset_artifact_prefix(
                    db_name=dataset.db_name,
                    dataset_id=dataset.dataset_id,
                    dataset_name=dataset.name,
                )
                object_key = f"{prefix}/data.json"
                checksum = await lakefs_storage_service.save_json(
                    repo,
                    f"{dataset_branch}/{object_key}",
                    sample_json,
                    metadata=ops._sanitize_s3_metadata(
                        {
                            "db_name": dataset.db_name,
                            "dataset_id": dataset.dataset_id,
                            "dataset_name": dataset.name,
                            "source": "manual",
                        }
                    ),
                )
                lakefs_commit_id = await ops._commit_lakefs_with_predicate_fallback(
                    lakefs_client=lakefs_client,
                    lakefs_storage_service=lakefs_storage_service,
                    repository=repo,
                    branch=dataset_branch,
                    message=f"Manual dataset version {dataset.db_name}/{dataset.name}",
                    metadata={
                        "dataset_id": dataset.dataset_id,
                        "db_name": dataset.db_name,
                        "dataset_name": dataset.name,
                        "source_type": str(getattr(dataset, "source_type", None) or "manual"),
                    },
                    object_key=object_key,
                    expected_checksum=checksum,
                )
                artifact_key = build_s3_uri(repo, f"{lakefs_commit_id}/{object_key}")
            version = await dataset_registry.add_version(
                dataset_id=dataset.dataset_id,
                lakefs_commit_id=lakefs_commit_id or "",
                artifact_key=artifact_key,
                row_count=row_count,
                sample_json=sample_json,
                schema_json=schema_json,
            )
            return ApiResponse.success(
                message="Dataset version created",
                data={"version": version.__dict__},
            ).to_dict()

        request_fingerprint = ops._build_ingest_request_fingerprint(
            {
                "dataset_id": dataset_id,
                "db_name": dataset.db_name,
                "branch": dataset_branch,
                "source_type": str(getattr(dataset, "source_type", None) or "manual"),
                "artifact_key": artifact_key,
                "lakefs_commit_id": lakefs_commit_id,
                "sample_json": sample_json,
                "row_count": row_count,
            }
        )
        ingest_request, _ = await dataset_registry.create_ingest_request(
            dataset_id=dataset_id,
            db_name=dataset.db_name,
            branch=dataset_branch,
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
            schema_json=schema_json,
            sample_json=sample_json,
            row_count=row_count,
        )
        ingest_transaction = await ops._ensure_ingest_transaction(
            dataset_registry,
            ingest_request_id=ingest_request.ingest_request_id,
        )
        existing_version = await resolve_existing_version_or_raise(
            dataset_registry=dataset_registry,
            ingest_request=ingest_request,
            expected_dataset_id=str(dataset_id),
            request_fingerprint=str(request_fingerprint),
        )
        if existing_version:
            await maybe_flush_dataset_ingest_outbox_inline(
                dataset_registry=dataset_registry,
                lineage_store=lineage_store,
                flush_dataset_ingest_outbox=flush_dataset_ingest_outbox,
            )
            return ApiResponse.success(
                message="Dataset version created",
                data={"version": existing_version.__dict__},
            ).to_dict()

        if artifact_key:
            ref = ops._extract_lakefs_ref_from_artifact_key(artifact_key)
            if ref == dataset_branch:
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "artifact_key must reference an immutable lakeFS commit id (not a branch ref)", code=ErrorCode.REQUEST_VALIDATION_FAILED)
            if lakefs_commit_id and lakefs_commit_id != ref:
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "lakefs_commit_id does not match artifact_key ref", code=ErrorCode.REQUEST_VALIDATION_FAILED)
            lakefs_commit_id = lakefs_commit_id or ref
        else:
            repo = ops._resolve_lakefs_raw_repository()
            prefix = ops._dataset_artifact_prefix(
                db_name=dataset.db_name,
                dataset_id=dataset.dataset_id,
                dataset_name=dataset.name,
            )
            staging_prefix = ops._ingest_staging_prefix(prefix, ingest_request.ingest_request_id)
            object_key = f"{staging_prefix}/data.json"
            checksum = await lakefs_storage_service.save_json(
                repo,
                f"{dataset_branch}/{object_key}",
                sample_json,
                metadata=ops._sanitize_s3_metadata(
                    {
                        "db_name": dataset.db_name,
                        "dataset_id": dataset.dataset_id,
                        "dataset_name": dataset.name,
                        "source": "manual",
                        "ingest_request_id": ingest_request.ingest_request_id,
                    }
                ),
            )
            commit_state = await ensure_lakefs_commit_artifact(
                ingest_request=ingest_request,
                initial_commit_id=lakefs_commit_id,
                initial_artifact_key=artifact_key,
                lakefs_client=lakefs_client,
                lakefs_storage_service=lakefs_storage_service,
                repository=repo,
                branch=dataset_branch,
                commit_message=f"Manual dataset version {dataset.db_name}/{dataset.name}",
                commit_metadata={
                    "dataset_id": dataset.dataset_id,
                    "db_name": dataset.db_name,
                    "dataset_name": dataset.name,
                    "source_type": str(getattr(dataset, "source_type", None) or "manual"),
                    "ingest_request_id": ingest_request.ingest_request_id,
                    "transaction_id": ingest_transaction.transaction_id if ingest_transaction else None,
                },
                object_key=object_key,
                expected_checksum=checksum,
            )
            lakefs_commit_id = commit_state.commit_id
            artifact_key = commit_state.artifact_key

        if not lakefs_commit_id:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "lakefs_commit_id is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        ingest_request = await persist_ingest_commit_state(
            dataset_registry=dataset_registry,
            ingest_request=ingest_request,
            ingest_transaction=ingest_transaction,
            commit_id=lakefs_commit_id,
            artifact_key=artifact_key or "",
            force=True,
        )
        version_event_id = ingest_request.ingest_request_id
        transaction_id = ingest_transaction.transaction_id if ingest_transaction else None
        outbox_builder = DatasetIngestOutboxBuilder(
            build_dataset_event_payload=build_dataset_event_payload,
            lineage_store=lineage_store,
        )
        outbox_entries = [
            outbox_builder.version_created(
                event_id=str(version_event_id),
                dataset_id=dataset_id,
                db_name=str(getattr(dataset, "db_name", "") or ""),
                name=str(getattr(dataset, "name", "") or ""),
                actor=actor_user_id,
                command_type="INGEST_DATASET_SNAPSHOT",
                lakefs_commit_id=lakefs_commit_id,
                artifact_key=artifact_key,
                transaction_id=transaction_id,
            )
        ]
        try:
            version = await dataset_registry.publish_ingest_request(
                ingest_request_id=ingest_request.ingest_request_id,
                dataset_id=dataset_id,
                lakefs_commit_id=lakefs_commit_id,
                artifact_key=artifact_key,
                row_count=row_count,
                sample_json=sample_json,
                schema_json=schema_json,
                outbox_entries=outbox_entries,
            )
        except ValueError as exc:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(exc), code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc

        objectify_job_id = await ops._maybe_enqueue_objectify_job(
            dataset=dataset,
            version=version,
            objectify_registry=objectify_registry,
            job_queue=objectify_job_queue,
            dataset_registry=dataset_registry,
            actor_user_id=actor_user_id,
        )

        await maybe_flush_dataset_ingest_outbox_inline(
            dataset_registry=dataset_registry,
            lineage_store=lineage_store,
            flush_dataset_ingest_outbox=flush_dataset_ingest_outbox,
        )

        return ApiResponse.success(
            message="Dataset version created",
            data={"version": version.__dict__, "objectify_job_id": objectify_job_id},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        await mark_ingest_failed(
            dataset_registry=dataset_registry,
            ingest_request=ingest_request,
            error=str(exc),
            stage="manual",
        )
        logger.error("Failed to create dataset version: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc
