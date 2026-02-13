"""Ontology CRUD endpoints (BFF).

Composed by `bff.routers.ontology` via router composition (Composite pattern).

Business logic lives in `bff.services.ontology_crud_service` (Facade).
"""

from __future__ import annotations
from shared.observability.tracing import trace_endpoint

from typing import Optional

from fastapi import APIRouter, Query, Request, status

from bff.dependencies import (
    JSONLDConverterDep,
    JSONToJSONLDConverter,
    LabelMapper,
    LabelMapperDep,
    OMSClientDep,
    TerminusService,
    TerminusServiceDep,
)
from bff.services import ontology_crud_service
from bff.services.oms_client import OMSClient
from shared.models.ontology import OntologyCreateRequestBFF, OntologyResponse, OntologyUpdateInput
from shared.models.responses import ApiResponse

router = APIRouter(tags=["Ontology Management"])


@router.post(
    "/ontology",
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_200_OK: {"model": ApiResponse, "description": "Direct mode (legacy)"},
        status.HTTP_202_ACCEPTED: {"model": ApiResponse, "description": "Event-sourcing mode (async)"},
        status.HTTP_409_CONFLICT: {"description": "Conflict (already exists / OCC)"},
    },
)
@trace_endpoint("bff.ontology.create_ontology")
async def create_ontology(
    db_name: str,
    ontology: OntologyCreateRequestBFF,
    branch: str = Query("main", description="Target branch (default: main)"),
    mapper: LabelMapper = LabelMapperDep,
    oms_client: OMSClient = OMSClientDep,
):
    return await ontology_crud_service.create_ontology(
        db_name=db_name,
        body=ontology,
        branch=branch,
        mapper=mapper,
        oms_client=oms_client,
    )


@router.get("/ontology/list")
@trace_endpoint("bff.ontology.list_ontologies")
async def list_ontologies(
    db_name: str,
    request: Request,
    branch: str = Query("main", description="Target branch (default: main)"),
    class_type: str = Query("sys:Class", description="클래스 타입"),
    limit: Optional[int] = Query(None, description="결과 개수 제한"),
    offset: int = Query(0, description="오프셋"),
    mapper: LabelMapper = LabelMapperDep,
    terminus: TerminusService = TerminusServiceDep,
):
    return await ontology_crud_service.list_ontologies(
        db_name=db_name,
        request=request,
        branch=branch,
        class_type=class_type,
        limit=limit,
        offset=offset,
        mapper=mapper,
        terminus=terminus,
    )


@router.get("/ontology/{class_label}", response_model=OntologyResponse)
@trace_endpoint("bff.ontology.get_ontology")
async def get_ontology(
    db_name: str,
    class_label: str,
    request: Request,
    branch: str = Query("main", description="Target branch (default: main)"),
    mapper: LabelMapper = LabelMapperDep,
    terminus: TerminusService = TerminusServiceDep,
):
    return await ontology_crud_service.get_ontology(
        db_name=db_name,
        class_label=class_label,
        request=request,
        branch=branch,
        mapper=mapper,
        terminus=terminus,
    )


@router.post("/ontology/validate", response_model=ApiResponse)
@trace_endpoint("bff.ontology.validate_ontology_create_bff")
async def validate_ontology_create_bff(
    db_name: str,
    ontology: OntologyCreateRequestBFF,
    request: Request,
    branch: str = Query("main", description="Target branch (default: main)"),
    oms_client: OMSClient = OMSClientDep,
):
    _ = request
    return await ontology_crud_service.validate_ontology_create(
        db_name=db_name,
        body=ontology,
        branch=branch,
        oms_client=oms_client,
    )


@router.post("/ontology/{class_label}/validate", response_model=ApiResponse)
@trace_endpoint("bff.ontology.validate_ontology_update_bff")
async def validate_ontology_update_bff(
    db_name: str,
    class_label: str,
    ontology: OntologyUpdateInput,
    request: Request,
    branch: str = Query("main", description="Target branch (default: main)"),
    mapper: LabelMapper = LabelMapperDep,
    oms_client: OMSClient = OMSClientDep,
):
    return await ontology_crud_service.validate_ontology_update(
        db_name=db_name,
        class_label=class_label,
        body=ontology,
        request=request,
        branch=branch,
        mapper=mapper,
        oms_client=oms_client,
    )


@router.put(
    "/ontology/{class_label}",
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_200_OK: {"model": ApiResponse, "description": "Direct mode (legacy)"},
        status.HTTP_202_ACCEPTED: {"model": ApiResponse, "description": "Event-sourcing mode (async)"},
        status.HTTP_409_CONFLICT: {"description": "OCC conflict"},
    },
)
@trace_endpoint("bff.ontology.update_ontology")
async def update_ontology(
    db_name: str,
    class_label: str,
    ontology: OntologyUpdateInput,
    request: Request,
    expected_seq: int = Query(..., ge=0, description="Expected current aggregate sequence (OCC)"),
    branch: str = Query("main", description="Target branch (default: main)"),
    mapper: LabelMapper = LabelMapperDep,
    terminus: TerminusService = TerminusServiceDep,
):
    return await ontology_crud_service.update_ontology(
        db_name=db_name,
        class_label=class_label,
        body=ontology,
        request=request,
        expected_seq=expected_seq,
        branch=branch,
        mapper=mapper,
        terminus=terminus,
    )


@router.delete(
    "/ontology/{class_label}",
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_200_OK: {"model": ApiResponse, "description": "Direct mode (legacy)"},
        status.HTTP_202_ACCEPTED: {"model": ApiResponse, "description": "Event-sourcing mode (async)"},
        status.HTTP_409_CONFLICT: {"description": "OCC conflict"},
    },
)
@trace_endpoint("bff.ontology.delete_ontology")
async def delete_ontology(
    db_name: str,
    class_label: str,
    request: Request,
    expected_seq: int = Query(..., ge=0, description="Expected current aggregate sequence (OCC)"),
    branch: str = Query("main", description="Target branch (default: main)"),
    mapper: LabelMapper = LabelMapperDep,
    terminus: TerminusService = TerminusServiceDep,
):
    return await ontology_crud_service.delete_ontology(
        db_name=db_name,
        class_label=class_label,
        request=request,
        expected_seq=expected_seq,
        branch=branch,
        mapper=mapper,
        terminus=terminus,
    )


@router.get("/ontology/{class_id}/schema")
@trace_endpoint("bff.ontology.get_ontology_schema")
async def get_ontology_schema(
    db_name: str,
    class_id: str,
    request: Request,
    format: str = Query("json", description="스키마 형식 (json, jsonld, owl)"),
    branch: str = Query("main", description="Target branch (default: main)"),
    mapper: LabelMapper = LabelMapperDep,
    terminus: TerminusService = TerminusServiceDep,
    jsonld_conv: JSONToJSONLDConverter = JSONLDConverterDep,
):
    return await ontology_crud_service.get_ontology_schema(
        db_name=db_name,
        class_id=class_id,
        request=request,
        format=format,
        branch=branch,
        mapper=mapper,
        terminus=terminus,
        jsonld_conv=jsonld_conv,
    )

