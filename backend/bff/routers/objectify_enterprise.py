"""
Objectify enterprise helper endpoints (BFF).

Composed by `bff.routers.objectify` via router composition (Composite pattern).
"""

import logging
from typing import Any, Dict
from shared.observability.tracing import trace_endpoint

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.routers.objectify_deps import get_dataset_registry, _require_db_role
from bff.schemas.objectify_requests import DetectRelationshipsRequest
from shared.models.requests import ApiResponse
from shared.security.database_access import DATA_ENGINEER_ROLES
from shared.security.input_sanitizer import sanitize_input
from shared.services.registries.dataset_registry import DatasetRegistry

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Objectify"])


@router.post(
    "/databases/{db_name}/datasets/{dataset_id}/detect-relationships",
    summary="Detect FK relationships in a dataset",
    description="Analyzes dataset columns to detect potential foreign key relationships based on naming conventions and value overlap.",
)
@trace_endpoint("bff.objectify.detect_relationships")
async def detect_relationships(
    db_name: str,
    dataset_id: str,
    request: Request,
    body: DetectRelationshipsRequest = DetectRelationshipsRequest(),
    branch: str = Query(default="master"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> Dict[str, Any]:
    """Detect potential FK relationships in a dataset."""
    db_name = sanitize_input(db_name)
    dataset_id = sanitize_input(dataset_id)
    await _require_db_role(request, db_name=db_name, roles=DATA_ENGINEER_ROLES)

    try:
        from shared.services.pipeline.fk_pattern_detector import (
            FKDetectionConfig,
            ForeignKeyPatternDetector,
            TargetCandidate,
        )

        dataset = await dataset_registry.get_dataset(dataset_id=dataset_id)
        if not dataset:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, f"Dataset not found: {dataset_id}", code=ErrorCode.RESOURCE_NOT_FOUND)

        schema = dataset.schema_json or {}
        columns = schema.get("columns") or schema.get("fields") or []

        all_datasets = await dataset_registry.list_datasets(db_name=db_name, branch=branch, limit=200)
        target_candidates = []
        for ds in all_datasets:
            if ds.dataset_id == dataset_id:
                continue
            ds_schema = ds.schema_json or {}
            ds_columns = ds_schema.get("columns") or ds_schema.get("fields") or []
            pk_cols = [c.get("name") for c in ds_columns if c.get("name", "").lower() in ("id", "pk")]
            if not pk_cols and ds_columns:
                pk_cols = [ds_columns[0].get("name", "id")]
            target_candidates.append(
                TargetCandidate(
                    candidate_type="dataset",
                    candidate_id=ds.dataset_id,
                    candidate_name=ds.name or ds.dataset_id,
                    pk_columns=pk_cols,
                )
            )

        config = FKDetectionConfig(min_confidence=body.confidence_threshold)
        detector = ForeignKeyPatternDetector(config)
        patterns = detector.detect_patterns(
            source_dataset_id=dataset_id,
            source_schema=columns,
            target_candidates=target_candidates,
        )

        suggestions = [detector.suggest_link_type(pattern) for pattern in patterns]

        return ApiResponse.success(
            message=f"Detected {len(patterns)} potential FK relationships",
            data={
                "dataset_id": dataset_id,
                "db_name": db_name,
                "patterns_found": len(patterns),
                "patterns": [
                    {
                        "source_column": p.source_column,
                        "target_dataset_id": p.target_dataset_id,
                        "target_object_type": p.target_object_type,
                        "target_pk_field": p.target_pk_field,
                        "confidence": p.confidence,
                        "detection_method": p.detection_method,
                        "reasons": p.reasons,
                    }
                    for p in patterns
                ],
                "suggestions": suggestions,
            },
        ).to_dict()

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("detect_relationships failed: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc
