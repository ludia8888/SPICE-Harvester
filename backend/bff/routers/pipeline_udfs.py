"""
Pipeline UDF API (BFF).

This module is a sub-router composed by `bff.routers.pipeline` (router composition / composite pattern).
"""

from typing import Optional

from fastapi import APIRouter, Depends, Path, Query
from pydantic import BaseModel

from bff.routers.pipeline_deps import get_pipeline_registry
from bff.services import pipeline_udf_service
from shared.models.requests import ApiResponse
from shared.observability.tracing import trace_endpoint
from shared.services.registries.pipeline_registry import PipelineRegistry

router = APIRouter(tags=["Pipeline Builder"])


class UdfCreateRequest(BaseModel):
    name: str
    code: str
    description: Optional[str] = None


class UdfVersionCreateRequest(BaseModel):
    code: str


@router.post("/udfs", response_model=ApiResponse)
@trace_endpoint("create_udf")
async def create_udf(
    body: UdfCreateRequest,
    db_name: str = Query(..., description="Database name"),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
) -> ApiResponse:
    return await pipeline_udf_service.create_udf(
        db_name=db_name,
        name=body.name,
        code=body.code,
        description=body.description,
        pipeline_registry=pipeline_registry,
    )


@router.get("/udfs", response_model=ApiResponse)
@trace_endpoint("list_udfs")
async def list_udfs(
    db_name: str = Query(..., description="Database name"),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
) -> ApiResponse:
    return await pipeline_udf_service.list_udfs(
        db_name=db_name,
        pipeline_registry=pipeline_registry,
    )


@router.get("/udfs/{udf_id}", response_model=ApiResponse)
@trace_endpoint("get_udf")
async def get_udf(
    udf_id: str = Path(..., description="UDF ID"),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
) -> ApiResponse:
    return await pipeline_udf_service.get_udf(
        udf_id=udf_id,
        pipeline_registry=pipeline_registry,
    )


@router.post("/udfs/{udf_id}/versions", response_model=ApiResponse)
@trace_endpoint("create_udf_version")
async def create_udf_version(
    body: UdfVersionCreateRequest,
    udf_id: str = Path(..., description="UDF ID"),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
) -> ApiResponse:
    return await pipeline_udf_service.create_udf_version(
        udf_id=udf_id,
        code=body.code,
        pipeline_registry=pipeline_registry,
    )


@router.get("/udfs/{udf_id}/versions/{version}", response_model=ApiResponse)
@trace_endpoint("get_udf_version")
async def get_udf_version(
    udf_id: str = Path(..., description="UDF ID"),
    version: int = Path(..., description="Version number"),
    pipeline_registry: PipelineRegistry = Depends(get_pipeline_registry),
) -> ApiResponse:
    return await pipeline_udf_service.get_udf_version(
        udf_id=udf_id,
        version=version,
        pipeline_registry=pipeline_registry,
    )
