"""
Pipeline simulation endpoint (BFF).

Composed by `bff.routers.pipeline` via router composition (Composite pattern).
"""

import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.routers.pipeline_deps import get_dataset_registry, get_pipeline_registry
from bff.routers.pipeline_ops import (
    _augment_definition_with_canonical_contract,
    _augment_definition_with_casts,
    _run_pipeline_preflight,
    _validate_pipeline_definition,
)
from shared.models.requests import ApiResponse
from shared.observability.tracing import trace_endpoint
from shared.security.auth_utils import enforce_db_scope
from shared.security.input_sanitizer import sanitize_input, validate_db_name
from shared.services.pipeline.pipeline_executor import PipelineExecutor
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.pipeline_registry import PipelineRegistry

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Pipeline Builder"])


@router.post("/simulate-definition", response_model=ApiResponse)
@trace_endpoint("simulate_pipeline_definition")
async def simulate_pipeline_definition(
    payload: Dict[str, Any],
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
    request: Request = None,
) -> ApiResponse:
    try:
        sanitized = sanitize_input(payload)
        db_name = validate_db_name(str(sanitized.get("db_name") or ""))
        if request is None:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "request context is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        try:
            enforce_db_scope(request.headers, db_name=db_name)
        except ValueError as exc:
            raise classified_http_exception(status.HTTP_403_FORBIDDEN, str(exc), code=ErrorCode.PERMISSION_DENIED) from exc

        definition_json = sanitized.get("definition_json") if isinstance(sanitized.get("definition_json"), dict) else None
        if not isinstance(definition_json, dict) or not definition_json:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "definition_json is required", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        node_id = str(sanitized.get("node_id") or "").strip() or None
        branch = str(sanitized.get("branch") or "main").strip() or "main"
        limit = int(sanitized.get("limit") or 200)
        limit = max(1, min(limit, 200))

        definition_json = await _augment_definition_with_casts(
            definition_json=definition_json,
            db_name=db_name,
            branch=branch,
            dataset_registry=dataset_registry,
        )
        definition_json = _augment_definition_with_canonical_contract(
            definition_json=definition_json,
            branch=branch,
        )

        preview_definition = dict(definition_json)
        preview_meta = dict(preview_definition.get("__preview_meta__") or {})
        preview_meta.setdefault("branch", branch)
        preview_definition["__preview_meta__"] = preview_meta

        validation_errors = _validate_pipeline_definition(
            definition_json=preview_definition,
            require_output=(node_id is None),
        )
        preflight = await _run_pipeline_preflight(
            definition_json=preview_definition,
            db_name=db_name,
            branch=branch,
            dataset_registry=dataset_registry,
            pipeline_registry=pipeline_registry,
        )

        if validation_errors:
            return ApiResponse.warning(
                message="Pipeline definition invalid",
                data={
                    "db_name": db_name,
                    "branch": branch,
                    "node_id": node_id,
                    "limit": limit,
                    "preflight": preflight,
                    "errors": validation_errors,
                },
            ).to_dict()

        if preflight.get("has_blocking_errors"):
            return ApiResponse.warning(
                message="Pipeline preflight failed",
                data={
                    "db_name": db_name,
                    "branch": branch,
                    "node_id": node_id,
                    "limit": limit,
                    "preflight": preflight,
                },
            ).to_dict()

        executor = PipelineExecutor(dataset_registry)
        try:
            preview = await executor.preview(
                definition=preview_definition,
                db_name=db_name,
                node_id=node_id,
                limit=limit,
            )
        except ValueError as exc:
            return ApiResponse.warning(
                message="Pipeline definition simulation failed",
                data={
                    "db_name": db_name,
                    "branch": branch,
                    "node_id": node_id,
                    "limit": limit,
                    "preflight": preflight,
                    "error": str(exc),
                },
            ).to_dict()

        return ApiResponse.success(
            message="Pipeline definition simulated",
            data={
                "db_name": db_name,
                "branch": branch,
                "node_id": node_id,
                "limit": limit,
                "preflight": preflight,
                "preview": preview,
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to simulate pipeline definition: {e}")
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e), code=ErrorCode.INTERNAL_ERROR)
