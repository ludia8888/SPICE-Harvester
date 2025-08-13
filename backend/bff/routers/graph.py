"""
Graph Query Router
THINK ULTRA³ - Multi-hop graph queries with TerminusDB + ES federation

This router provides endpoints for executing graph traversal queries
that combine TerminusDB's graph authority with Elasticsearch document payloads.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from bff.dependencies import get_oms_client
from shared.security.input_sanitizer import validate_db_name
from shared.utils.language import get_accept_language
from shared.services.graph_federation_service_woql import GraphFederationServiceWOQL
from shared.config.settings import ApplicationSettings
import os

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["Graph"])


# Request/Response Models
class GraphHop(BaseModel):
    """Represents a single hop in a graph traversal"""
    predicate: str  # Relationship name (e.g., "owned_by")
    target_class: str  # Target class name (e.g., "Client")


class GraphQueryRequest(BaseModel):
    """Request model for multi-hop graph queries"""
    start_class: str  # Starting class (e.g., "Product")
    hops: List[GraphHop]  # List of hops to traverse
    filters: Optional[Dict[str, Any]] = None  # Optional filters on start class
    limit: int = 100  # Max results


class GraphNode(BaseModel):
    """Graph node with ES document reference"""
    id: str
    type: str
    es_doc_id: str
    data: Optional[Dict[str, Any]] = None  # Document payload from ES


class GraphEdge(BaseModel):
    """Graph edge between nodes"""
    from_node: str
    to_node: str
    predicate: str


class GraphQueryResponse(BaseModel):
    """Response model for graph queries"""
    nodes: List[GraphNode]
    edges: List[GraphEdge]
    query: Dict[str, Any]
    count: int


class SimpleGraphQueryRequest(BaseModel):
    """Request for simple single-class queries"""
    class_name: str
    filters: Optional[Dict[str, Any]] = None
    limit: int = 100


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
        # Use Docker service names when running in container
        is_docker = os.getenv("DOCKER_CONTAINER", "false").lower() == "true"
        terminus_url = "http://terminusdb:6363" if is_docker else os.getenv("TERMINUS_SERVER_URL", "http://localhost:6363")
        
        connection_info = ConnectionConfig(
            server_url=terminus_url,
            user=os.getenv("TERMINUS_USER", "admin"),
            account=os.getenv("TERMINUS_ACCOUNT", "admin"),
            key=os.getenv("TERMINUS_KEY", "admin")
        )
        
        terminus_service = AsyncTerminusService(connection_info)
        await terminus_service.connect()
        
        # Initialize REAL WOQL GraphFederationService
        es_host = "elasticsearch" if is_docker else os.getenv("ELASTICSEARCH_HOST", "localhost")
        es_port = int(os.getenv("ELASTICSEARCH_PORT", "9200"))
        es_username = os.getenv("ELASTICSEARCH_USERNAME", "elastic")
        es_password = os.getenv("ELASTICSEARCH_PASSWORD", "spice123!")
        
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
    graph_service: GraphFederationServiceWOQL = Depends(get_graph_federation_service)
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
        
        # Convert hops to tuple format expected by service
        hops_tuples = [(hop.predicate, hop.target_class) for hop in query.hops]
        
        logger.info(f"📊 Graph query on {db_name}: {query.start_class} -> {hops_tuples}")
        
        # Execute multi-hop query
        result = await graph_service.multi_hop_query(
            db_name=db_name,
            start_class=query.start_class,
            hops=hops_tuples,
            filters=query.filters,
            limit=query.limit
        )
        
        # Convert result to response model
        nodes = [
            GraphNode(
                id=node["id"],
                type=node["type"],
                es_doc_id=node.get("es_doc_id", ""),
                data=node.get("data")
            )
            for node in result.get("nodes", [])
        ]
        
        edges = [
            GraphEdge(
                from_node=edge["from"],
                to_node=edge["to"],
                predicate=edge["predicate"]
            )
            for edge in result.get("edges", [])
        ]
        
        response = GraphQueryResponse(
            nodes=nodes,
            edges=edges,
            query={
                "start_class": query.start_class,
                "hops": [{"predicate": h[0], "target_class": h[1]} for h in hops_tuples],
                "filters": query.filters,
                "limit": query.limit
            },
            count=len(nodes)
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
    graph_service: GraphFederationServiceWOQL = Depends(get_graph_federation_service)
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
        
        logger.info(f"📊 Simple graph query on {db_name}: {query.class_name}")
        
        # Execute simple query
        result = await graph_service.simple_graph_query(
            db_name=db_name,
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


@router.get("/graph-query/{db_name}/paths")
async def find_relationship_paths(
    db_name: str,
    source_class: str,
    target_class: str,
    max_depth: int = 5,
    graph_service: GraphFederationServiceWOQL = Depends(get_graph_federation_service)
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
        
        logger.info(f"🔍 Finding paths from {source_class} to {target_class} in {db_name}")
        
        # Use REAL WOQL to query schema relationships
        # THINK ULTRA³ - This is now using REAL schema discovery!
        from shared.services.async_terminus import AsyncTerminusService
        from shared.models.config import ConnectionConfig
        
        # Get TerminusDB connection details from environment
        terminus_url = os.environ.get("TERMINUS_SERVER_URL", "http://localhost:6363")
        es_host = os.environ.get("ELASTICSEARCH_HOST", "localhost")
        es_port = int(os.environ.get("ELASTICSEARCH_PORT", "9201"))
        
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
            source_class=source_class,
            target_class=target_class,
            max_depth=max_depth
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