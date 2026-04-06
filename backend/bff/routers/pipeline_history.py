"""
Pipeline run/artifact endpoints (BFF).

Composed by `bff.routers.pipeline` via router composition (Composite pattern).
"""

import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.routers.pipeline_deps import get_pipeline_registry
from bff.routers.pipeline_shared import _ensure_pipeline_permission
from shared.models.responses import ApiResponse
from shared.observability.tracing import trace_endpoint
from shared.services.registries.pipeline_registry import PipelineRegistry

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Pipeline Builder"])


@router.get("/{pipeline_id}/runs", response_model=ApiResponse)
@trace_endpoint("list_pipeline_runs")
async def list_pipeline_runs(
    pipeline_id: str,
    limit: int = Query(default=50, ge=1, le=200),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="read",
        )
        runs = None
        for attempt in (1, 2):
            try:
                runs = await pipeline_registry.list_runs(pipeline_id=pipeline_id, limit=limit)
                break
            except (TimeoutError, asyncio.TimeoutError):
                logger.warning(
                    "Pipeline runs query timed out (pipeline_id=%s, attempt=%s/%s, limit=%s)",
                    pipeline_id,
                    attempt,
                    2,
                    limit,
                    exc_info=True,
                )
                if attempt == 2:
                    raise
                await asyncio.sleep(0.2)
        if runs is None:
            raise classified_http_exception(
                status.HTTP_504_GATEWAY_TIMEOUT,
                "Pipeline runs temporarily unavailable",
                code=ErrorCode.DB_TIMEOUT,
            )
        return ApiResponse.success(
            message="Pipeline runs fetched",
            data={"runs": runs, "count": len(runs)},
        ).to_dict()
    except HTTPException:
        raise
    except (TimeoutError, asyncio.TimeoutError) as exc:
        logger.warning(
            "Failed to list pipeline runs due to timeout (pipeline_id=%s, limit=%s)",
            pipeline_id,
            limit,
            exc_info=True,
        )
        raise classified_http_exception(
            status.HTTP_504_GATEWAY_TIMEOUT,
            "Pipeline runs temporarily unavailable",
            code=ErrorCode.DB_TIMEOUT,
        ) from exc
    except Exception as e:
        logger.error("Failed to list pipeline runs (pipeline_id=%s, limit=%s): %s", pipeline_id, limit, e, exc_info=True)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR) from e


@router.get("/{pipeline_id}/artifacts", response_model=ApiResponse)
@trace_endpoint("list_pipeline_artifacts")
async def list_pipeline_artifacts(
    pipeline_id: str,
    mode: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="read",
        )
        artifacts = await pipeline_registry.list_artifacts(
            pipeline_id=pipeline_id,
            limit=limit,
            mode=mode,
        )
        payload = [artifact.__dict__ for artifact in artifacts]
        return ApiResponse.success(message="Pipeline artifacts", data={"artifacts": payload}).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list pipeline artifacts: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


@router.get("/{pipeline_id}/artifacts/{artifact_id}", response_model=ApiResponse)
@trace_endpoint("get_pipeline_artifact")
async def get_pipeline_artifact(
    pipeline_id: str,
    artifact_id: str,
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        await _ensure_pipeline_permission(
            pipeline_registry,
            pipeline_id=pipeline_id,
            request=request,
            required_role="read",
        )
        artifact = await pipeline_registry.get_artifact(artifact_id=artifact_id)
        if not artifact or artifact.pipeline_id != pipeline_id:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Artifact not found", code=ErrorCode.RESOURCE_NOT_FOUND)
        return ApiResponse.success(message="Pipeline artifact", data={"artifact": artifact.__dict__}).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch pipeline artifact: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)
