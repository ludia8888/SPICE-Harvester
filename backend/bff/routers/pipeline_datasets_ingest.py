"""Pipeline dataset ingest request endpoints (BFF).

Composed by `bff.routers.pipeline_datasets` via router composition (Composite pattern).
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status

from bff.routers.pipeline_datasets_ops import (
    _compute_tabular_analysis_from_sample,
)

from bff.routers.pipeline_deps import get_dataset_registry
from shared.errors.error_types import ErrorCode, classified_http_exception
from bff.schemas.pipeline_datasets import TabularAnalysisApiResponse
from shared.models.responses import ApiResponse
from shared.observability.tracing import trace_endpoint
from shared.security.auth_utils import enforce_db_scope
from shared.security.input_sanitizer import sanitize_input
from shared.services.registries.dataset_registry import DatasetRegistry

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Pipeline Builder"])

@router.get("/datasets/ingest-requests/{ingest_request_id}", response_model=TabularAnalysisApiResponse, deprecated=True, include_in_schema=False)
@trace_endpoint("get_dataset_ingest_request")
async def get_dataset_ingest_request(
    ingest_request_id: str,
    request: Request = None,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        ingest_request = await dataset_registry.get_ingest_request(ingest_request_id=ingest_request_id)
        if not ingest_request:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Ingest request not found", code=ErrorCode.RESOURCE_NOT_FOUND)
        try:
            enforce_db_scope(request.headers, db_name=ingest_request.db_name)
        except ValueError as exc:
            raise classified_http_exception(status.HTTP_403_FORBIDDEN, str(exc), code=ErrorCode.PERMISSION_DENIED)
        dataset = await dataset_registry.get_dataset(dataset_id=ingest_request.dataset_id)
        tabular_analysis = await _compute_tabular_analysis_from_sample(ingest_request.sample_json)
        return ApiResponse.success(
            message="Ingest request fetched",
            data={
                "dataset": dataset.__dict__ if dataset else None,
                "ingest_request": ingest_request.__dict__,
                "tabular_analysis": tabular_analysis,
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get ingest request: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)


@router.post("/datasets/ingest-requests/{ingest_request_id}/schema/approve", response_model=ApiResponse, deprecated=True, include_in_schema=False)
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
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "Ingest request not found", code=ErrorCode.RESOURCE_NOT_FOUND)
        try:
            enforce_db_scope(request.headers, db_name=ingest_request.db_name)
        except ValueError as exc:
            raise classified_http_exception(status.HTTP_403_FORBIDDEN, str(exc), code=ErrorCode.PERMISSION_DENIED)
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
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(exc), code=ErrorCode.REQUEST_VALIDATION_FAILED)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to approve dataset schema: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)
