"""Pipeline dataset read endpoints (BFF).

Composed by `bff.routers.pipeline_datasets` via router composition (Composite pattern).
"""

import base64
import logging
import mimetypes
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from bff.routers.pipeline_datasets_ops import (
    _resolve_lakefs_raw_repository,
    _select_sample_row,
)

from bff.routers.pipeline_deps import get_dataset_registry, get_pipeline_registry
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.models.requests import ApiResponse
from shared.observability.tracing import trace_endpoint
from shared.security.auth_utils import enforce_db_scope
from shared.security.input_sanitizer import validate_db_name
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry
from shared.utils.s3_uri import parse_s3_uri

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Pipeline Builder"])

@router.get("/datasets", response_model=ApiResponse)
@trace_endpoint("list_datasets")
async def list_datasets(
    db_name: str,
    branch: Optional[str] = Query(default=None),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        datasets = await dataset_registry.list_datasets(db_name=db_name, branch=branch)
        return ApiResponse.success(
            message="Datasets fetched",
            data={"datasets": datasets, "count": len(datasets)},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list datasets: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


@router.get("/branches", response_model=ApiResponse, include_in_schema=False)
@trace_endpoint("list_pipeline_branches")
async def list_pipeline_branches(
    db_name: str = Query(...),
    request: Request = None,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
):
    db_name = validate_db_name(db_name)
    repo = _resolve_lakefs_raw_repository()
    try:
        actor_user_id = (request.headers.get("X-User-ID") or "").strip() or None
        lakefs_client = await pipeline_registry.get_lakefs_client(user_id=actor_user_id)
        raw = await lakefs_client.list_branches(repository=repo)
        branch_names = [br.get("name") if isinstance(br, dict) else str(br) for br in raw]
    except Exception:
        branch_names = ["main"]
    return ApiResponse.success(data={"branches": branch_names}).to_dict()


@router.post("/branches", response_model=ApiResponse, include_in_schema=False)
@trace_endpoint("create_pipeline_branch")
async def create_pipeline_branch(
    request: Request,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
):
    body = await request.json()
    pipeline_id = str(body.get("pipeline_id") or "").strip()
    new_branch = str(body.get("branch") or "").strip()
    if not pipeline_id or not new_branch:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "pipeline_id and branch are required",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )
    try:
        actor_user_id = (request.headers.get("X-User-ID") or "").strip() or None
        created = await pipeline_registry.create_branch(
            pipeline_id=pipeline_id,
            new_branch=new_branch,
            user_id=actor_user_id,
        )
        return ApiResponse.success(
            message="Branch created",
            data={"pipeline": created._asdict() if hasattr(created, '_asdict') else created.__dict__},
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create branch: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


@router.delete("/datasets/{dataset_id}", response_model=ApiResponse)
@trace_endpoint("delete_dataset")
async def delete_dataset(
    dataset_id: str,
    request: Request = None,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
        if not dataset:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Dataset not found", code=ErrorCode.RESOURCE_NOT_FOUND)
        try:
            enforce_db_scope(request.headers, db_name=dataset.db_name)
        except ValueError as exc:
            raise classified_http_exception(status.HTTP_403_FORBIDDEN, str(exc), code=ErrorCode.PERMISSION_DENIED)

        deleted = await dataset_registry.delete_dataset(dataset_id=dataset_id)
        if not deleted:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Dataset not found", code=ErrorCode.RESOURCE_NOT_FOUND)

        return ApiResponse.success(message="Dataset deleted", data={"dataset_id": dataset_id}).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete dataset: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


@router.get("/datasets/{dataset_id}/raw-file", response_model=ApiResponse)
@trace_endpoint("get_dataset_raw_file")
async def get_dataset_raw_file(
    dataset_id: str,
    file_name: Optional[str] = Query(default=None, description="Optional filename for media datasets"),
    file_index: Optional[int] = Query(default=None, ge=0, description="Optional index for media datasets"),
    request: Request = None,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
        if not dataset:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Dataset not found", code=ErrorCode.RESOURCE_NOT_FOUND)
        try:
            enforce_db_scope(request.headers, db_name=dataset.db_name)
        except ValueError as exc:
            raise classified_http_exception(status.HTTP_403_FORBIDDEN, str(exc), code=ErrorCode.PERMISSION_DENIED)

        version = await dataset_registry.get_latest_version(dataset_id=dataset_id)
        if not version or not version.artifact_key:
            # Best-effort self-heal for cases where ingest reached RAW_COMMITTED but
            # version publication lagged (or worker restarted between phases).
            try:
                await dataset_registry.reconcile_ingest_state(
                    stale_after_seconds=60,
                    limit=50,
                    use_lock=False,
                )
                version = await dataset_registry.get_latest_version(dataset_id=dataset_id)
            except Exception as reconcile_exc:
                logger.warning("Dataset ingest reconcile before raw-file lookup failed: %s", reconcile_exc)
        if not version or not version.artifact_key:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Dataset version not found", code=ErrorCode.RESOURCE_NOT_FOUND)

        actor_user_id = (request.headers.get("X-User-ID") or "").strip() if request else ""
        lakefs_storage_service = await pipeline_registry.get_lakefs_storage(user_id=actor_user_id or None)

        sample_json = version.sample_json if isinstance(version.sample_json, dict) else {}
        dataset_source = str(dataset.source_type or "").lower()
        row = None
        if dataset_source == "media":
            row = _select_sample_row(sample_json, filename=file_name, file_index=file_index)
        target_uri = str(row.get("s3_uri") or "").strip() if row else ""
        content_type = str(row.get("content_type") or "").strip() if row else ""
        size_bytes = row.get("size_bytes") if row else None
        if size_bytes is not None:
            try:
                size_bytes = int(size_bytes)
            except Exception:
                logging.getLogger(__name__).warning("Exception fallback at bff/routers/pipeline_datasets_catalog.py:90", exc_info=True)
                size_bytes = None

        if not target_uri:
            if str(dataset.source_type or "").lower() == "media":
                raise classified_http_exception(
                    status.HTTP_409_CONFLICT,
                    "Media dataset requires a file selection",
                    code=ErrorCode.CONFLICT,
                )
            target_uri = str(version.artifact_key or "").strip()

        parsed_target = parse_s3_uri(target_uri)
        if parsed_target:
            bucket, key = parsed_target
        else:
            if row and row.get("s3_uri"):
                raise classified_http_exception(
                    status.HTTP_400_BAD_REQUEST,
                    "artifact_key must be an s3:// URI",
                    code=ErrorCode.REQUEST_VALIDATION_FAILED,
                )
            bucket = _resolve_lakefs_raw_repository()
            key = f"{(dataset.branch or 'main').strip() or 'main'}/{target_uri.lstrip('/')}"
        if row and row.get("s3_uri"):
            parsed_prefix = parse_s3_uri(str(version.artifact_key or "").strip())
            if not parsed_prefix:
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "artifact_key must be an s3:// URI", code=ErrorCode.REQUEST_VALIDATION_FAILED)
            prefix_bucket, prefix_key = parsed_prefix
            normalized_prefix = prefix_key.rstrip("/")
            if bucket != prefix_bucket or not key.startswith(f"{normalized_prefix}/"):
                raise classified_http_exception(
                    status.HTTP_400_BAD_REQUEST,
                    "Requested file is outside dataset artifact prefix",
                    code=ErrorCode.REQUEST_VALIDATION_FAILED,
                )

        filename = str(row.get("filename") or "").strip() if row else ""
        if not filename:
            filename = key.split("/")[-1] or dataset.name
        if not content_type:
            guessed_type, _ = mimetypes.guess_type(filename)
            content_type = guessed_type or "application/octet-stream"

        blob = await lakefs_storage_service.load_bytes(bucket, key)
        if size_bytes is None:
            size_bytes = len(blob)

        try:
            content = blob.decode("utf-8")
            encoding = "utf-8"
        except UnicodeDecodeError:
            content = base64.b64encode(blob).decode("ascii")
            encoding = "base64"

        version_created_at = None
        if getattr(version, "created_at", None) is not None:
            created_at = getattr(version, "created_at")
            version_created_at = created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at)

        return ApiResponse.success(
            message="Dataset raw file",
            data={
                "file": {
                    "dataset_id": dataset.dataset_id,
                    "version_id": version.version_id,
                    "lakefs_commit_id": version.lakefs_commit_id,
                    "version_created_at": version_created_at,
                    "filename": filename,
                    "content_type": content_type,
                    "size_bytes": size_bytes,
                    "artifact_key": version.artifact_key,
                    "s3_uri": target_uri,
                    "encoding": encoding,
                    "content": content,
                }
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to load dataset raw file: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)
