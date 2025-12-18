"""
Graph Query Router
THINK ULTRA³ - Multi-hop graph queries with TerminusDB + ES federation

This router provides endpoints for executing graph traversal queries
that combine TerminusDB's graph authority with Elasticsearch document payloads.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from fastapi import APIRouter, Depends, HTTPException, Request, status, Query
from pydantic import BaseModel

from bff.dependencies import get_oms_client
from shared.security.input_sanitizer import validate_branch_name, validate_db_name
from shared.utils.language import get_accept_language
from shared.services.graph_federation_service_woql import GraphFederationServiceWOQL
from shared.config.settings import ApplicationSettings
from shared.config.service_config import ServiceConfig
from shared.dependencies.providers import LineageStoreDep
from shared.services.lineage_store import LineageStore
from shared.models.graph_query import (
    GraphEdge,
    GraphHop,
    GraphNode,
    GraphQueryRequest,
    GraphQueryResponse,
    SimpleGraphQueryRequest,
)
import os

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["Graph"])


# Import needed dependencies
from shared.services.async_terminus import AsyncTerminusService
from shared.models.config import ConnectionConfig

# Dependency for GraphFederationServiceWOQL (REAL WOQL)
_graph_service: Optional[GraphFederationServiceWOQL] = None


async def get_graph_federation_service() -> GraphFederationServiceWOQL:
    """Get or create GraphFederationServiceWOQL instance - REAL WOQL solution"""
    global _graph_service
    
    if _graph_service is None:
        # Initialize TerminusDB service
        terminus_url = ServiceConfig.get_terminus_url()
        
        connection_info = ConnectionConfig(
            server_url=terminus_url,
            user=os.getenv("TERMINUS_USER", "admin"),
            account=os.getenv("TERMINUS_ACCOUNT", "admin"),
            key=os.getenv("TERMINUS_KEY", "admin"),
        )
        
        terminus_service = AsyncTerminusService(connection_info)
        await terminus_service.connect()
        
        # Initialize REAL WOQL GraphFederationService
        es_host = ServiceConfig.get_elasticsearch_host()
        es_port = ServiceConfig.get_elasticsearch_port()
        es_username = (os.getenv("ELASTICSEARCH_USERNAME") or "").strip()
        es_password = os.getenv("ELASTICSEARCH_PASSWORD") or ""
        
        _graph_service = GraphFederationServiceWOQL(
            terminus_service=terminus_service,
            es_host=es_host,
            es_port=es_port,
            es_username=es_username,
            es_password=es_password
        )
        
        logger.info(f"✅ GraphFederationServiceWOQL (REAL WOQL) initialized (ES: {es_host}:{es_port})")
    
    return _graph_service


@router.post("/graph-query/{db_name}", response_model=GraphQueryResponse)
async def execute_graph_query(
    db_name: str,
    query: GraphQueryRequest,
    request: Request,
    lineage_store: LineageStoreDep,
    graph_service: GraphFederationServiceWOQL = Depends(get_graph_federation_service),
    branch: str = Query("main", description="Target branch (default: main)"),
):
    """
    Execute multi-hop graph query with ES federation
    
    This endpoint performs graph traversal through TerminusDB's graph structure
    while fetching full document payloads from Elasticsearch.
    
    Example:
    ```json
    {
        "start_class": "Product",
        "hops": [
            {"predicate": "owned_by", "target_class": "Client"}
        ],
        "filters": {"product_id": "PROD-001"},
        "limit": 10
    }
    ```
    
    This would find all Products with ID PROD-001 and traverse the "owned_by" 
    relationship to find their associated Clients.
    """
    try:
        # Validate database name
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)
        
        # Convert hops to tuple format expected by service
        hops_tuples = [(hop.predicate, hop.target_class) for hop in query.hops]
        
        logger.info(f"📊 Graph query on {db_name}: {query.start_class} -> {hops_tuples}")
        
        # Execute multi-hop query
        result = await graph_service.multi_hop_query(
            db_name=db_name,
            branch=branch,
            start_class=query.start_class,
            hops=hops_tuples,
            filters=query.filters,
            limit=query.limit,
            offset=query.offset,
            max_nodes=query.max_nodes,
            max_edges=query.max_edges,
            include_paths=query.include_paths,
            max_paths=query.max_paths,
            no_cycles=query.no_cycles,
            include_documents=query.include_documents,
            include_audit=query.include_audit,
        )
        
        raw_nodes = list(result.get("nodes", []) or [])

        terminus_latest: Dict[str, Dict[str, Any]] = {}
        es_latest: Dict[str, Dict[str, Any]] = {}
        terminus_artifact_by_node_id: Dict[str, str] = {}
        es_artifact_by_node_id: Dict[str, str] = {}

        if query.include_provenance and raw_nodes:
            try:
                # Best-effort provenance: last event -> Terminus doc / ES doc
                terminus_artifacts: List[str] = []
                es_artifacts: List[str] = []
                for node in raw_nodes:
                    terminus_id = str(node.get("terminus_id") or node.get("id") or "")
                    if terminus_id:
                        art = LineageStore.node_artifact("terminus", db_name, branch, terminus_id)
                        terminus_artifact_by_node_id[terminus_id] = art
                        terminus_artifacts.append(art)

                    es_ref = node.get("es_ref") or {}
                    index_name = str(es_ref.get("index") or "")
                    doc_id = str(es_ref.get("id") or "")
                    if index_name and doc_id:
                        art = LineageStore.node_artifact("es", index_name, doc_id)
                        es_artifact_by_node_id[f"{index_name}/{doc_id}"] = art
                        es_artifacts.append(art)

                terminus_latest = await lineage_store.get_latest_edges_to(
                    to_node_ids=list({*terminus_artifacts}),
                    edge_type="event_wrote_terminus_document",
                    db_name=db_name,
                )
                es_latest = await lineage_store.get_latest_edges_to(
                    to_node_ids=list({*es_artifacts}),
                    edge_type="event_materialized_es_document",
                    db_name=db_name,
                )
            except Exception as e:
                logger.debug(f"Provenance lookup failed (non-fatal): {e}")

        # Convert result to response model (include data_status + display for UX stability)
        nodes: List[GraphNode] = []
        for node in raw_nodes:
            terminus_id = str(node.get("terminus_id") or node.get("id") or "")
            es_doc_id = node.get("es_doc_id") or terminus_id or node.get("id")
            es_ref = node.get("es_ref") or {
                "index": f"{db_name}_instances",
                "id": str(es_doc_id),
            }
            node_index_status = dict(node.get("index_status") or {})

            provenance: Optional[Dict[str, Any]] = None
            if query.include_provenance and terminus_id:
                terminus_art = terminus_artifact_by_node_id.get(terminus_id) or LineageStore.node_artifact(
                    "terminus", db_name, terminus_id
                )
                es_key = f"{es_ref.get('index')}/{es_ref.get('id')}"
                es_art = es_artifact_by_node_id.get(es_key) or (
                    LineageStore.node_artifact("es", str(es_ref.get("index")), str(es_ref.get("id")))
                    if es_ref.get("index") and es_ref.get("id")
                    else None
                )

                terminus_prov = terminus_latest.get(terminus_art)
                es_prov = es_latest.get(es_art) if es_art else None
                provenance = {"terminus": terminus_prov, "es": es_prov}

                try:
                    t_at = (
                        datetime.fromisoformat(str(terminus_prov.get("occurred_at")))
                        if terminus_prov and terminus_prov.get("occurred_at")
                        else None
                    )
                    e_at = (
                        datetime.fromisoformat(str(es_prov.get("occurred_at")))
                        if es_prov and es_prov.get("occurred_at")
                        else None
                    )
                    if t_at and t_at.tzinfo is None:
                        t_at = t_at.replace(tzinfo=timezone.utc)
                    if e_at and e_at.tzinfo is None:
                        e_at = e_at.replace(tzinfo=timezone.utc)
                    if t_at and e_at:
                        node_index_status["graph_last_updated_at"] = t_at.isoformat()
                        node_index_status["projection_last_indexed_at"] = e_at.isoformat()
                        node_index_status["projection_lag_seconds"] = max(0.0, (t_at - e_at).total_seconds())
                except Exception:
                    pass

            nodes.append(
                GraphNode(
                    id=node["id"],
                    type=node["type"],
                    es_doc_id=str(es_doc_id),
                    db_name=str(node.get("db_name") or db_name),
                    es_ref=es_ref,
                    data_status=str(node.get("data_status") or ("FULL" if node.get("data") else "PARTIAL")),
                    display=node.get("display"),
                    data=node.get("data") if query.include_documents else None,
                    index_status=node_index_status or None,
                    provenance=provenance,
                )
            )
        
        edges: List[GraphEdge] = []
        for edge in result.get("edges", []) or []:
            edge_prov: Optional[Dict[str, Any]] = None
            if query.include_provenance:
                # Relationships come from the source node's Terminus document; attribute to its last write event.
                from_node = str(edge.get("from") or "")
                if from_node:
                    terminus_art = LineageStore.node_artifact("terminus", db_name, branch, from_node)
                    edge_prov = {"terminus": terminus_latest.get(terminus_art)}
            edges.append(
                GraphEdge(
                    from_node=edge["from"],
                    to_node=edge["to"],
                    predicate=edge["predicate"],
                    provenance=edge_prov,
                )
            )

        # Index summary (best-effort observability)
        full_docs = sum(1 for n in nodes if n.data_status == "FULL")
        index_ages = [
            float(n.index_status.get("index_age_seconds"))
            for n in nodes
            if isinstance(n.index_status, dict) and n.index_status.get("index_age_seconds") is not None
        ]
        index_summary = {
            "documents_found": full_docs,
            "documents_missing": max(0, len(nodes) - full_docs),
            "max_index_age_seconds": max(index_ages) if index_ages else None,
            "avg_index_age_seconds": (sum(index_ages) / len(index_ages)) if index_ages else None,
        }
        
        response = GraphQueryResponse(
            nodes=nodes,
            edges=edges,
            paths=result.get("paths") if query.include_paths else None,
            index_summary=index_summary,
            query={
                "start_class": query.start_class,
                "hops": [{"predicate": h[0], "target_class": h[1]} for h in hops_tuples],
                "filters": query.filters,
                "limit": query.limit,
                "offset": query.offset,
                "max_nodes": query.max_nodes,
                "max_edges": query.max_edges,
                "include_paths": query.include_paths,
                "max_paths": query.max_paths,
                "no_cycles": query.no_cycles,
                "include_provenance": query.include_provenance,
                "include_documents": query.include_documents,
                "include_audit": query.include_audit,
            },
            count=len(nodes),
            warnings=list(result.get("warnings") or []),
            page=result.get("page"),
        )
        
        logger.info(f"✅ Graph query complete: {len(nodes)} nodes, {len(edges)} edges")
        
        return response
        
    except ValueError as e:
        logger.error(f"Invalid query parameters: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid query: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Graph query failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Graph query failed: {str(e)}"
        )


@router.post("/graph-query/{db_name}/simple", response_model=Dict[str, Any])
async def execute_simple_graph_query(
    db_name: str,
    query: SimpleGraphQueryRequest,
    request: Request,
    graph_service: GraphFederationServiceWOQL = Depends(get_graph_federation_service),
    branch: str = Query("main", description="Target branch (default: main)"),
):
    """
    Execute simple single-class graph query
    
    This endpoint queries a single class and returns all instances
    with their ES document payloads.
    
    Example:
    ```json
    {
        "class_name": "Product",
        "filters": {"category": "electronics"},
        "limit": 50
    }
    ```
    """
    try:
        # Validate database name
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)
        
        logger.info(f"📊 Simple graph query on {db_name}: {query.class_name}")
        
        # Execute simple query
        result = await graph_service.simple_graph_query(
            db_name=db_name,
            branch=branch,
            class_name=query.class_name,
            filters=query.filters
        )
        
        logger.info(f"✅ Simple query complete: {result.get('count', 0)} documents")
        
        return result
        
    except ValueError as e:
        logger.error(f"Invalid query parameters: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid query: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Simple graph query failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Query failed: {str(e)}"
        )


@router.post("/graph-query/{db_name}/multi-hop", response_model=Dict[str, Any])
async def execute_multi_hop_query(
    db_name: str,
    query: Dict[str, Any],
    request: Request,
    graph_service: GraphFederationServiceWOQL = Depends(get_graph_federation_service),
    branch: str = Query("main", description="Target branch (default: main)"),
):
    """
    Execute multi-hop graph query with Federation
    
    Query format:
    {
        "start_class": "Product",
        "hops": [("owned_by", "Client")],
        "filters": {"category": "Software"},
        "include_documents": true
    }
    """
    try:
        # Validate database name
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)
        
        start_class = query.get("start_class")
        hops = query.get("hops", [])
        filters = query.get("filters", {})
        include_documents = query.get("include_documents", True)
        include_audit = query.get("include_audit", False)
        limit = query.get("limit", 100)
        
        if not start_class:
            raise ValueError("start_class is required")
        
        logger.info(f"🚀 Multi-hop query: {start_class} -> {hops}")
        
        # Convert hop list to proper format
        hop_tuples = [(h[0], h[1]) for h in hops] if hops else []
        
        # Execute multi-hop query
        result = await graph_service.multi_hop_query(
            db_name=db_name,
            branch=branch,
            start_class=start_class,
            hops=hop_tuples,
            filters=filters,
            limit=limit,
            include_documents=include_documents,
            include_audit=include_audit
        )
        
        return {
            "status": "success",
            "data": result
        }
        
    except ValueError as e:
        logger.error(f"Invalid multi-hop query: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid query: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Multi-hop query failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Query failed: {str(e)}"
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
    """
    Find all possible relationship paths between two classes
    
    This endpoint discovers all valid graph traversal paths from a source class
    to a target class within the specified maximum depth.
    
    Example:
    GET /graph-query/mydb/paths?source_class=SKU&target_class=Client&max_depth=3
    
    This would find paths like:
    - SKU -> belongs_to -> Product -> owned_by -> Client
    """
    try:
        # Validate database name
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)
        
        logger.info(f"🔍 Finding paths from {source_class} to {target_class} in {db_name}")
        
        # Use REAL WOQL to query schema relationships
        # THINK ULTRA³ - This is now using REAL schema discovery!
        from shared.services.async_terminus import AsyncTerminusService
        from shared.models.config import ConnectionConfig
        
        # Get TerminusDB connection details from environment
        terminus_url = ServiceConfig.get_terminus_url()
        es_host = ServiceConfig.get_elasticsearch_host()
        es_port = ServiceConfig.get_elasticsearch_port()
        
        # Initialize services for schema query
        connection_info = ConnectionConfig(
            server_url=terminus_url,
            user="admin",
            account="admin",
            key="admin"
        )
        
        terminus_service = AsyncTerminusService(connection_info)
        await terminus_service.connect()
        
        graph_service = GraphFederationServiceWOQL(
            terminus_service=terminus_service,
            es_host=es_host,
            es_port=es_port
        )
        
        # Find paths using REAL WOQL schema queries
        paths = await graph_service.find_relationship_paths(
            db_name=db_name,
            branch=branch,
            source_class=source_class,
            target_class=target_class,
            max_depth=max_depth,
        )
        
        logger.info(f"✅ Found {len(paths)} paths via REAL WOQL schema discovery")
        
        return {
            "source_class": source_class,
            "target_class": target_class,
            "paths": paths,
            "count": len(paths),
            "max_depth": max_depth
        }
        
    except Exception as e:
        logger.error(f"Path finding failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Path finding failed: {str(e)}"
        )


@router.get("/graph-query/health")
async def graph_service_health(
    graph_service: GraphFederationServiceWOQL = Depends(get_graph_federation_service)
):
    """
    Check health of graph federation service
    
    Verifies connectivity to both TerminusDB and Elasticsearch.
    """
    try:
        # Check TerminusDB using the terminus service
        terminus_healthy = await graph_service.terminus.ping()
        
        # Check Elasticsearch
        import aiohttp
        es_healthy = False
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(f"{graph_service.es_url}/_cluster/health", auth=graph_service.es_auth) as resp:
                    if resp.status == 200:
                        es_health = await resp.json()
                        es_healthy = es_health.get("status") in ["green", "yellow"]
            except Exception as e:
                logger.error(f"ES health check failed: {e}")
        
        return {
            "status": "healthy" if (terminus_healthy and es_healthy) else "degraded",
            "services": {
                "terminusdb": "healthy" if terminus_healthy else "unhealthy",
                "elasticsearch": "healthy" if es_healthy else "unhealthy"
            },
            "message": "Graph federation service operational" if (terminus_healthy and es_healthy) 
                      else "Some services are degraded"
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e)
        }


# ========== PROJECTION/VIEW ENDPOINTS (팔란티어 스타일) ==========

class ProjectionRegistrationRequest(BaseModel):
    """프로젝션 등록 요청"""
    view_name: str
    start_class: str
    hops: List[GraphHop]
    filters: Optional[Dict[str, Any]] = None
    refresh_interval: int = 3600  # 기본 1시간


class ProjectionQueryRequest(BaseModel):
    """프로젝션 조회 요청"""
    view_name: str
    filters: Optional[Dict[str, Any]] = None
    limit: int = 100


@router.post(
    "/projections/{db_name}/register",
    summary="🚧 (WIP) Register a projection (materialized view)",
    tags=["Projections (WIP)"],
)
async def register_projection(
    db_name: str,
    request: ProjectionRegistrationRequest,
    graph_service: GraphFederationServiceWOQL = Depends(get_graph_federation_service)
):
    """
    🚧 (WIP) 빈번한 멀티홉 쿼리를 “프로젝션(=materialized view)”으로 등록
    
    주의:
    - 현재 이 엔드포인트는 “겉 API만 존재”하며, 실제 materialize(저장/갱신/조회)는 구현되어 있지 않습니다.
    - 프론트엔드는 이 기능을 사용하지 마세요. (Swagger/OpenAPI에 보이더라도 WIP입니다.)
    - 현재 제품 UI는 `/graph-query/{db_name}`(실시간 federation) 기반으로 구현해야 합니다.
    
    Example:
    ```json
    {
        "view_name": "products_with_clients",
        "start_class": "Product",
        "hops": [
            {"predicate": "owned_by", "target_class": "Client"}
        ],
        "filters": {"category": "electronics"},
        "refresh_interval": 1800
    }
    ```
    """
    try:
        db_name = validate_db_name(db_name)
        
        # TODO: ProjectionManager를 통한 프로젝션 등록
        # 현재는 스켈레톤만 구현
        logger.info(f"🎯 Registering projection {request.view_name} for {db_name}")
        
        return {
            "status": "pending",
            "message": f"Projection {request.view_name} registration queued",
            "view_name": request.view_name,
            "refresh_interval": request.refresh_interval
        }
        
    except Exception as e:
        logger.error(f"Projection registration failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Registration failed: {str(e)}"
        )


@router.post(
    "/projections/{db_name}/query",
    summary="🚧 (WIP) Query a projection (materialized view)",
    tags=["Projections (WIP)"],
)
async def query_projection(
    db_name: str,
    request: ProjectionQueryRequest,
    graph_service: GraphFederationServiceWOQL = Depends(get_graph_federation_service)
):
    """
    🚧 (WIP) 프로젝션 뷰 조회 (캐시된 데이터)
    
    주의:
    - 현재는 프로젝션이 materialize 되지 않으므로, 의미있는 데이터를 반환하지 않습니다.
    - 프론트엔드는 이 엔드포인트를 사용하지 마세요.
    - 대신 `/graph-query/{db_name}`를 사용하세요.
    
    Example:
    ```json
    {
        "view_name": "products_with_clients",
        "filters": {"client_id": "CL-001"},
        "limit": 50
    }
    ```
    """
    try:
        db_name = validate_db_name(db_name)
        
        # TODO: ProjectionManager를 통한 캐시 조회
        # 현재는 실시간 WOQL 실행으로 폴백
        logger.info(f"🎯 Querying projection {request.view_name} from {db_name}")
        
        return {
            "status": "fallback",
            "message": "Projection not yet materialized, executing real-time query",
            "data": [],
            "count": 0
        }
        
    except Exception as e:
        logger.error(f"Projection query failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Query failed: {str(e)}"
        )


@router.get(
    "/projections/{db_name}/list",
    summary="🚧 (WIP) List projections (materialized views)",
    tags=["Projections (WIP)"],
)
async def list_projections(
    db_name: str,
    graph_service: GraphFederationServiceWOQL = Depends(get_graph_federation_service)
):
    """
    🚧 (WIP) 등록된 프로젝션 목록 조회

    주의:
    - 현재는 ProjectionManager가 없어서 항상 빈 리스트를 반환합니다.
    - 프론트엔드는 이 엔드포인트를 사용하지 마세요.
    """
    try:
        db_name = validate_db_name(db_name)
        
        # TODO: ProjectionManager에서 목록 조회
        logger.info(f"🎯 Listing projections for {db_name}")
        
        return {
            "projections": [],
            "count": 0
        }
        
    except Exception as e:
        logger.error(f"Failed to list projections: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"List failed: {str(e)}"
        )
