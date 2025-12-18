"""
Context7 Integration Router for BFF
Provides endpoints for Context7 MCP server integration
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from backend.mcp.mcp_client import get_context7_client, Context7Client

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/context7",
    tags=["context7"],
    responses={
        404: {"description": "Not found"},
        500: {"description": "Internal server error"}
    }
)


class SearchRequest(BaseModel):
    """Context7 search request"""
    query: str = Field(..., description="Search query string")
    limit: int = Field(10, ge=1, le=100, description="Maximum number of results")
    filters: Optional[Dict[str, Any]] = Field(None, description="Search filters")


class KnowledgeRequest(BaseModel):
    """Request to add knowledge to Context7"""
    title: str = Field(..., description="Knowledge title")
    content: str = Field(..., description="Knowledge content")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    tags: Optional[List[str]] = Field(None, description="Tags for categorization")


class EntityLinkRequest(BaseModel):
    """Request to create entity relationship"""
    source_id: str = Field(..., description="Source entity ID")
    target_id: str = Field(..., description="Target entity ID")
    relationship: str = Field(..., description="Relationship type")
    properties: Optional[Dict[str, Any]] = Field(None, description="Relationship properties")


class OntologyAnalysisRequest(BaseModel):
    """Request to analyze ontology with Context7"""
    ontology_id: str = Field(..., description="Ontology ID")
    db_name: str = Field(..., description="Database name")
    include_relationships: bool = Field(True, description="Include relationship analysis")
    include_suggestions: bool = Field(True, description="Include improvement suggestions")


@router.post("/search")
async def search_context7(
    request: SearchRequest,
    client: Context7Client = Depends(get_context7_client)
) -> Dict[str, Any]:
    """
    Search Context7 knowledge base
    
    Returns:
        Search results with relevance scores
    """
    try:
        results = await client.search(
            query=request.query,
            limit=request.limit
        )
        
        return {
            "query": request.query,
            "count": len(results),
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Context7 search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/context/{entity_id}")
async def get_entity_context(
    entity_id: str,
    client: Context7Client = Depends(get_context7_client)
) -> Dict[str, Any]:
    """
    Get context information for a specific entity
    
    Args:
        entity_id: Entity identifier
        
    Returns:
        Entity context including relationships and metadata
    """
    try:
        context = await client.get_context(entity_id)
        
        return {
            "entity_id": entity_id,
            "context": context
        }
        
    except Exception as e:
        logger.error(f"Failed to get context for entity {entity_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/knowledge")
async def add_knowledge(
    request: KnowledgeRequest,
    client: Context7Client = Depends(get_context7_client)
) -> Dict[str, Any]:
    """
    Add new knowledge to Context7
    
    Returns:
        Created knowledge entry details
    """
    try:
        # Prepare metadata with tags
        metadata = request.metadata or {}
        if request.tags:
            metadata["tags"] = request.tags
        
        result = await client.add_knowledge(
            title=request.title,
            content=request.content,
            metadata=metadata
        )
        
        return {
            "success": True,
            "knowledge_id": result.get("id"),
            "result": result
        }
        
    except Exception as e:
        logger.error(f"Failed to add knowledge: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/link")
async def create_entity_link(
    request: EntityLinkRequest,
    client: Context7Client = Depends(get_context7_client)
) -> Dict[str, Any]:
    """
    Create relationship between entities in Context7
    
    Returns:
        Link creation result
    """
    try:
        result = await client.link_entities(
            source_id=request.source_id,
            target_id=request.target_id,
            relationship=request.relationship
        )
        
        return {
            "success": True,
            "link": {
                "source": request.source_id,
                "target": request.target_id,
                "relationship": request.relationship
            },
            "result": result
        }
        
    except Exception as e:
        logger.error(f"Failed to create entity link: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze/ontology")
async def analyze_ontology(
    request: OntologyAnalysisRequest,
    client: Context7Client = Depends(get_context7_client)
) -> Dict[str, Any]:
    """
    Analyze ontology structure with Context7 AI
    
    Returns:
        Analysis results including suggestions and insights
    """
    try:
        # First get the ontology from our system
        # This would integrate with OMS client
        # For now, we'll create a mock structure
        
        ontology_data = {
            "id": request.ontology_id,
            "db_name": request.db_name,
            "include_relationships": request.include_relationships,
            "include_suggestions": request.include_suggestions
        }
        
        analysis = await client.analyze_ontology(ontology_data)
        
        return {
            "ontology_id": request.ontology_id,
            "analysis": analysis,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to analyze ontology: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/suggestions/{db_name}/{class_id}")
async def get_ontology_suggestions(
    db_name: str,
    class_id: str,
    client: Context7Client = Depends(get_context7_client)
) -> Dict[str, Any]:
    """
    Get AI-powered suggestions for ontology improvements
    
    Args:
        db_name: Database name
        class_id: Ontology class ID
        
    Returns:
        Improvement suggestions from Context7
    """
    try:
        # Query Context7 for suggestions based on ontology structure
        query = f"ontology improvements for {class_id} in {db_name}"
        suggestions = await client.search(query, limit=5)
        
        return {
            "db_name": db_name,
            "class_id": class_id,
            "suggestions": suggestions,
            "generated_at": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get suggestions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def check_context7_health(
    client: Context7Client = Depends(get_context7_client)
) -> Dict[str, Any]:
    """
    Check Context7 connection health
    
    Returns:
        Connection status and available capabilities
    """
    try:
        # Try to list available tools as health check
        from backend.mcp.mcp_client import get_mcp_manager
        mcp_manager = get_mcp_manager()
        
        tools = await mcp_manager.list_tools("context7")
        
        return {
            "status": "healthy",
            "connected": True,
            "available_tools": len(tools),
            "tools": [tool.get("name") for tool in tools]
        }
        
    except Exception as e:
        logger.error(f"Context7 health check failed: {e}")
        return {
            "status": "unhealthy",
            "connected": False,
            "error": str(e)
        }


# Import datetime for timestamp generation
from datetime import datetime, timezone
