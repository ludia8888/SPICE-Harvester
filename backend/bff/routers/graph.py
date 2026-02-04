"""
Graph Query Router (BFF).

Thin router: delegates graph query logic to `bff.services.graph_query_service`.
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel

from bff.routers.registry_deps import get_dataset_registry
from bff.services.graph_federation_provider import get_graph_federation_service
from bff.services.graph_query_service import (
    execute_graph_query as execute_graph_query_service,
    execute_multi_hop_query as execute_multi_hop_query_service,
    execute_simple_graph_query as execute_simple_graph_query_service,
    find_relationship_paths as find_relationship_paths_service,
    graph_service_health as graph_service_health_service,
)
from shared.dependencies.providers import LineageStoreDep
from shared.models.graph_query import GraphHop, GraphQueryRequest, GraphQueryResponse, SimpleGraphQueryRequest
from shared.security.input_sanitizer import validate_db_name
from shared.services.core.graph_federation_service_woql import GraphFederationServiceWOQL
from shared.services.registries.dataset_registry import DatasetRegistry

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["Graph"])


@router.post("/graph-query/{db_name}", response_model=GraphQueryResponse)
async def execute_graph_query(
    db_name: str,
    query: GraphQueryRequest,
    request: Request,
    lineage_store: LineageStoreDep,
    graph_service: GraphFederationServiceWOQL = Depends(get_graph_federation_service),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    base_branch: str = Query("main", description="Base branch (Terminus) (default: main)"),
    overlay_branch: Optional[str] = Query(default=None, description="ES overlay branch (writeback)"),
    branch: Optional[str] = Query(default=None, description="Deprecated alias for base_branch"),
):
    return await execute_graph_query_service(
        db_name=db_name,
        query=query,
        request=request,
        lineage_store=lineage_store,
        graph_service=graph_service,
        dataset_registry=dataset_registry,
        base_branch=base_branch,
        overlay_branch=overlay_branch,
        branch=branch,
    )


@router.post("/graph-query/{db_name}/simple", response_model=Dict[str, Any])
async def execute_simple_graph_query(
    db_name: str,
    query: SimpleGraphQueryRequest,
    request: Request,
    graph_service: GraphFederationServiceWOQL = Depends(get_graph_federation_service),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    base_branch: str = Query("main", description="Base branch (Terminus) (default: main)"),
    overlay_branch: Optional[str] = Query(default=None, description="ES overlay branch (writeback)"),
    branch: Optional[str] = Query(default=None, description="Deprecated alias for base_branch"),
):
    return await execute_simple_graph_query_service(
        db_name=db_name,
        query=query,
        request=request,
        graph_service=graph_service,
        dataset_registry=dataset_registry,
        base_branch=base_branch,
        overlay_branch=overlay_branch,
        branch=branch,
    )


@router.post("/graph-query/{db_name}/multi-hop", response_model=Dict[str, Any])
async def execute_multi_hop_query(
    db_name: str,
    query: Dict[str, Any],
    request: Request,
    graph_service: GraphFederationServiceWOQL = Depends(get_graph_federation_service),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    base_branch: str = Query("main", description="Base branch (Terminus) (default: main)"),
    overlay_branch: Optional[str] = Query(default=None, description="ES overlay branch (writeback)"),
    branch: Optional[str] = Query(default=None, description="Deprecated alias for base_branch"),
):
    return await execute_multi_hop_query_service(
        db_name=db_name,
        query=query,
        request=request,
        graph_service=graph_service,
        dataset_registry=dataset_registry,
        base_branch=base_branch,
        overlay_branch=overlay_branch,
        branch=branch,
    )


@router.get("/graph-query/{db_name}/paths")
async def find_relationship_paths(
    db_name: str,
    source_class: str,
    target_class: str,
    max_depth: int = 5,
    graph_service: GraphFederationServiceWOQL = Depends(get_graph_federation_service),
    branch: str = Query("main", description="Target branch (default: main)"),
):
    return await find_relationship_paths_service(
        db_name=db_name,
        source_class=source_class,
        target_class=target_class,
        max_depth=max_depth,
        graph_service=graph_service,
        branch=branch,
    )


@router.get("/graph-query/health")
async def graph_service_health(graph_service: GraphFederationServiceWOQL = Depends(get_graph_federation_service)):
    return await graph_service_health_service(graph_service=graph_service)


# ========== PROJECTION/VIEW ENDPOINTS (Materialized Views) ==========


class ProjectionRegistrationRequest(BaseModel):
    view_name: str
    start_class: str
    hops: List[GraphHop]
    filters: Optional[Dict[str, Any]] = None
    refresh_interval: int = 3600


class ProjectionQueryRequest(BaseModel):
    view_name: str
    filters: Optional[Dict[str, Any]] = None
    limit: int = 100


@router.post(
    "/projections/{db_name}/register",
    summary="🚧 (WIP) Register a projection (materialized view)",
    tags=["Projections (WIP)"],
    include_in_schema=False,
)
async def register_projection(
    db_name: str,
    request: ProjectionRegistrationRequest,
    graph_service: GraphFederationServiceWOQL = Depends(get_graph_federation_service),
):
    try:
        db_name = validate_db_name(db_name)
        logger.info("🎯 Registering projection %s for %s", request.view_name, db_name)
        return {
            "status": "pending",
            "message": f"Projection {request.view_name} registration queued",
            "view_name": request.view_name,
            "refresh_interval": request.refresh_interval,
        }
    except Exception as e:
        logger.error("Projection registration failed: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Registration failed: {str(e)}",
        ) from e


@router.post(
    "/projections/{db_name}/query",
    summary="🚧 (WIP) Query a projection (materialized view)",
    tags=["Projections (WIP)"],
    include_in_schema=False,
)
async def query_projection(
    db_name: str,
    request: ProjectionQueryRequest,
    graph_service: GraphFederationServiceWOQL = Depends(get_graph_federation_service),
):
    try:
        db_name = validate_db_name(db_name)
        logger.info("🎯 Querying projection %s from %s", request.view_name, db_name)
        return {
            "status": "fallback",
            "message": "Projection not yet materialized, executing real-time query",
            "data": [],
            "count": 0,
        }
    except Exception as e:
        logger.error("Projection query failed: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Query failed: {str(e)}",
        ) from e


@router.get(
    "/projections/{db_name}/list",
    summary="🚧 (WIP) List projections (materialized views)",
    tags=["Projections (WIP)"],
    include_in_schema=False,
)
async def list_projections(
    db_name: str,
    graph_service: GraphFederationServiceWOQL = Depends(get_graph_federation_service),
):
    try:
        db_name = validate_db_name(db_name)
        logger.info("🎯 Listing projections for %s", db_name)
        return {"projections": [], "count": 0}
    except Exception as e:
        logger.error("Failed to list projections: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"List failed: {str(e)}",
        ) from e
