"""Ontology relationship endpoints (BFF).

Composed by `bff.routers.ontology` via router composition (Composite pattern).

This module defines HTTP routes only. Business logic lives in
`bff.services.ontology_relationships_service` (Facade).
"""

from __future__ import annotations
from shared.observability.tracing import trace_endpoint

from typing import Optional

from fastapi import APIRouter, Query, Request, status

from bff.dependencies import LabelMapper, LabelMapperDep, TerminusService, TerminusServiceDep
from bff.services import ontology_relationships_service
from shared.models.ontology import OntologyCreateRequestBFF
from shared.models.responses import ApiResponse

router = APIRouter(tags=["Ontology Management"])


@router.post(
    "/ontology-advanced",
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_200_OK: {"model": ApiResponse, "description": "Direct mode (legacy)"},
        status.HTTP_202_ACCEPTED: {"model": ApiResponse, "description": "Event-sourcing mode (async)"},
        status.HTTP_409_CONFLICT: {"description": "Conflict (already exists / OCC)"},
    },
)
@trace_endpoint("bff.ontology.create_ontology_with_relationship_validation")
async def create_ontology_with_relationship_validation(
    db_name: str,
    ontology: OntologyCreateRequestBFF,
    request: Request,
    branch: str = Query("main", description="Target branch (default: main)"),
    auto_generate_inverse: bool = Query(False, description="(Not implemented) Auto-generate inverse metadata"),
    validate_relationships: bool = Query(True, description="Validate relationships against current schema"),
    check_circular_references: bool = Query(True, description="Reject introducing critical schema cycles"),
    mapper: LabelMapper = LabelMapperDep,
    terminus: TerminusService = TerminusServiceDep,
):
    return await ontology_relationships_service.create_ontology_with_relationship_validation(
        db_name=db_name,
        ontology=ontology,
        request=request,
        branch=branch,
        auto_generate_inverse=auto_generate_inverse,
        validate_relationships=validate_relationships,
        check_circular_references=check_circular_references,
        mapper=mapper,
        terminus=terminus,
    )


@router.post("/validate-relationships")
@trace_endpoint("bff.ontology.validate_ontology_relationships_bff")
async def validate_ontology_relationships_bff(
    db_name: str,
    ontology: OntologyCreateRequestBFF,
    request: Request,
    branch: str = Query("main", description="Target branch (default: main)"),
    mapper: LabelMapper = LabelMapperDep,
    terminus: TerminusService = TerminusServiceDep,
):
    return await ontology_relationships_service.validate_ontology_relationships(
        db_name=db_name,
        ontology=ontology,
        request=request,
        branch=branch,
        mapper=mapper,
        terminus=terminus,
    )


@router.post("/check-circular-references")
@trace_endpoint("bff.ontology.check_circular_references_bff")
async def check_circular_references_bff(
    db_name: str,
    request: Request,
    ontology: Optional[OntologyCreateRequestBFF] = None,
    branch: str = Query("main", description="Target branch (default: main)"),
    mapper: LabelMapper = LabelMapperDep,
    terminus: TerminusService = TerminusServiceDep,
):
    return await ontology_relationships_service.check_circular_references(
        db_name=db_name,
        request=request,
        ontology=ontology,
        branch=branch,
        mapper=mapper,
        terminus=terminus,
    )


@router.get("/relationship-network/analyze")
@trace_endpoint("bff.ontology.analyze_relationship_network_bff")
async def analyze_relationship_network_bff(
    db_name: str,
    request: Request,
    terminus: TerminusService = TerminusServiceDep,
    mapper: LabelMapper = LabelMapperDep,
):
    _ = mapper  # reserved for future label-enrichment
    return await ontology_relationships_service.analyze_relationship_network(
        db_name=db_name,
        request=request,
        terminus=terminus,
    )


@router.get("/relationship-paths")
@trace_endpoint("bff.ontology.find_relationship_paths_bff")
async def find_relationship_paths_bff(
    request: Request,
    db_name: str,
    start_entity: str,
    end_entity: Optional[str] = Query(None, description="목표 엔티티 (없으면 모든 도달 가능한 엔티티)"),
    max_depth: int = Query(3, ge=1, le=5, description="최대 탐색 깊이"),
    path_type: str = Query("shortest", description="경로 타입 (shortest, all, weighted, semantic)"),
    terminus: TerminusService = TerminusServiceDep,
    mapper: LabelMapper = LabelMapperDep,
):
    return await ontology_relationships_service.find_relationship_paths(
        request=request,
        db_name=db_name,
        start_entity=start_entity,
        end_entity=end_entity,
        max_depth=max_depth,
        path_type=path_type,
        terminus=terminus,
        mapper=mapper,
    )

