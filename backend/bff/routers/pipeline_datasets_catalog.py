"""Pipeline dataset read endpoints (BFF).

Composed by `bff.routers.pipeline_datasets` via router composition (Composite pattern).
"""

import base64
import logging
import mimetypes
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

import bff.routers.pipeline_datasets_ops as ops
from bff.routers.pipeline_datasets_ops import (
    _select_sample_row,
)

from bff.routers.pipeline_deps import get_dataset_registry, get_pipeline_registry
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
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


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
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
        try:
            enforce_db_scope(request.headers, db_name=dataset.db_name)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))

        version = await dataset_registry.get_latest_version(dataset_id=dataset_id)
        if not version or not version.artifact_key:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset version not found")

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
                size_bytes = None

        if not target_uri:
            if str(dataset.source_type or "").lower() == "media":
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Media dataset requires a file selection",
                )
            target_uri = str(version.artifact_key or "").strip()

        parsed_target = parse_s3_uri(target_uri)
        if not parsed_target:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="artifact_key must be an s3:// URI")
        bucket, key = parsed_target
        if row and row.get("s3_uri"):
            parsed_prefix = parse_s3_uri(str(version.artifact_key or "").strip())
            if not parsed_prefix:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="artifact_key must be an s3:// URI")
            prefix_bucket, prefix_key = parsed_prefix
            normalized_prefix = prefix_key.rstrip("/")
            if bucket != prefix_bucket or not key.startswith(f"{normalized_prefix}/"):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Requested file is outside dataset artifact prefix",
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

        return ApiResponse.success(
            message="Dataset raw file",
            data={
                "file": {
                    "dataset_id": dataset.dataset_id,
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
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
