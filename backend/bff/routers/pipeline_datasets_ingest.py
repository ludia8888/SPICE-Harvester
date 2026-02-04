"""Pipeline dataset ingest request endpoints (BFF).

Composed by `bff.routers.pipeline_datasets` via router composition (Composite pattern).
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status

import bff.routers.pipeline_datasets_ops as ops
from bff.routers.pipeline_datasets_ops import (
    _compute_funnel_analysis_from_sample,
)

from bff.routers.pipeline_deps import get_dataset_registry
from bff.schemas.pipeline_datasets import FunnelAnalysisApiResponse
from shared.models.requests import ApiResponse
from shared.observability.tracing import trace_endpoint
from shared.security.auth_utils import enforce_db_scope
from shared.security.input_sanitizer import sanitize_input
from shared.services.registries.dataset_registry import DatasetRegistry

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Pipeline Builder"])

@router.get("/datasets/ingest-requests/{ingest_request_id}", response_model=FunnelAnalysisApiResponse)
@trace_endpoint("get_dataset_ingest_request")
async def get_dataset_ingest_request(
    ingest_request_id: str,
    request: Request = None,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        ingest_request = await dataset_registry.get_ingest_request(ingest_request_id=ingest_request_id)
        if not ingest_request:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ingest request not found")
        try:
            enforce_db_scope(request.headers, db_name=ingest_request.db_name)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
        dataset = await dataset_registry.get_dataset(dataset_id=ingest_request.dataset_id)
        funnel_analysis = await _compute_funnel_analysis_from_sample(ingest_request.sample_json)
        return ApiResponse.success(
            message="Ingest request fetched",
            data={
                "dataset": dataset.__dict__ if dataset else None,
                "ingest_request": ingest_request.__dict__,
                "funnel_analysis": funnel_analysis,
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get ingest request: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/datasets/ingest-requests/{ingest_request_id}/schema/approve", response_model=ApiResponse)
@trace_endpoint("approve_dataset_schema")
async def approve_dataset_schema(
    ingest_request_id: str,
    payload: Optional[Dict[str, Any]] = None,
    request: Request = None,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        ingest_request = await dataset_registry.get_ingest_request(ingest_request_id=ingest_request_id)
        if not ingest_request:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ingest request not found")
        try:
            enforce_db_scope(request.headers, db_name=ingest_request.db_name)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
        sanitized = sanitize_input(payload or {})
        schema_json = sanitized.get("schema_json")
        actor_user_id = (request.headers.get("X-User-ID") or "").strip() if request else ""
        actor_user_id = actor_user_id or None
        dataset, updated_request = await dataset_registry.approve_ingest_schema(
            ingest_request_id=ingest_request_id,
            schema_json=schema_json if isinstance(schema_json, dict) else None,
            approved_by=actor_user_id,
        )
        return ApiResponse.success(
            message="Schema approved",
            data={"dataset": dataset.__dict__, "ingest_request": updated_request.__dict__},
        ).to_dict()
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to approve dataset schema: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
