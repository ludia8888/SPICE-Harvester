"""Link type edit endpoints (BFF).

Composed by `bff.routers.link_types` via router composition (Composite pattern).
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from bff.routers.link_types_deps import get_dataset_registry
from bff.schemas.link_types_requests import LinkEditRequest
from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import sanitize_input, validate_branch_name, validate_db_name
from shared.services.registries.dataset_registry import DatasetRegistry

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Ontology Link Types"])


@router.get("/link-types/{link_type_id}/edits", response_model=ApiResponse)
async def list_link_edits(
    db_name: str,
    link_type_id: str,
    branch: str = Query("main", description="Target branch"),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        link_type_id = str(link_type_id or "").strip()
        if not link_type_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="link_type_id is required")
        branch = validate_branch_name(branch)

        relationship_spec = await dataset_registry.get_relationship_spec(link_type_id=link_type_id)
        if not relationship_spec:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Relationship spec not found")
        if relationship_spec.db_name != db_name:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Link type does not belong to requested database",
            )

        edits = await dataset_registry.list_link_edits(
            db_name=db_name,
            link_type_id=link_type_id,
            branch=branch,
            status="ACTIVE",
        )
        return ApiResponse.success(
            message="Link edits retrieved",
            data={"link_edits": [e.__dict__ for e in edits]},
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to list link edits: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.post("/link-types/{link_type_id}/edits", response_model=ApiResponse)
async def create_link_edit(
    db_name: str,
    link_type_id: str,
    body: LinkEditRequest,
    request: Request,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> ApiResponse:
    try:
        db_name = validate_db_name(db_name)
        link_type_id = str(link_type_id or "").strip()
        if not link_type_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="link_type_id is required")

        relationship_spec = await dataset_registry.get_relationship_spec(link_type_id=link_type_id)
        if not relationship_spec:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Relationship spec not found")
        if relationship_spec.db_name != db_name:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Link type does not belong to requested database",
            )

        spec = relationship_spec.spec or {}
        edits_enabled = bool(spec.get("edits_enabled") or spec.get("editsEnabled"))
        if not edits_enabled:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"code": "LINK_EDITS_DISABLED", "link_type_id": link_type_id},
            )

        payload = sanitize_input(body.model_dump())
        branch = validate_branch_name(payload.get("branch") or "main")
        edit_type = str(payload.get("edit_type") or "").strip().upper()
        if edit_type not in {"ADD", "REMOVE"}:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="edit_type must be ADD or REMOVE")

        source_instance_id = str(payload.get("source_instance_id") or "").strip()
        target_instance_id = str(payload.get("target_instance_id") or "").strip()
        if not source_instance_id or not target_instance_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="source_instance_id and target_instance_id are required",
            )

        _ = request  # reserved for future auth checks
        record = await dataset_registry.record_link_edit(
            db_name=db_name,
            link_type_id=link_type_id,
            branch=branch,
            source_object_type=relationship_spec.source_object_type,
            target_object_type=relationship_spec.target_object_type,
            predicate=relationship_spec.predicate,
            source_instance_id=source_instance_id,
            target_instance_id=target_instance_id,
            edit_type=edit_type,
            status="ACTIVE",
            metadata=payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {},
        )
        return ApiResponse.success(message="Link edit recorded", data={"link_edit": record.__dict__})
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to record link edit: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))

