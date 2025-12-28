"""
Query Router for CQRS Read Side
Palantir-style architecture: Query from Elasticsearch with TerminusDB graph federation

🔥 THINK ULTRA: This implements the correct query patterns:
1. Simple queries → Direct from Elasticsearch
2. Graph queries → TerminusDB for traversal, ES for documents
3. WOQL queries → TerminusDB analysis with optional ES enrichment
"""

import logging
from typing import Any, AsyncIterator, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query as QueryParam
from pydantic import BaseModel

from oms.dependencies import (
    AsyncTerminusService,
    TerminusServiceDep,
    ValidatedDatabaseName,
)
from elasticsearch import AsyncElasticsearch
from shared.config.settings import ApplicationSettings

logger = logging.getLogger(__name__)

router = APIRouter()


class SimpleQuery(BaseModel):
    """Simple SQL-like query"""
    query: str  # e.g., "SELECT * FROM Product WHERE price > 1000"


class WOQLQuery(BaseModel):
    """WOQL query for graph analysis"""
    type: str = "select"
    query: str  # WOQL/SPARQL query string


async def get_elasticsearch() -> AsyncIterator[AsyncElasticsearch]:
    """Get Elasticsearch client"""
    settings = ApplicationSettings()
    es_url = f"http://{settings.database.elasticsearch_host}:{settings.database.elasticsearch_port}"
    es_username = (settings.database.elasticsearch_username or "").strip()
    es_password = settings.database.elasticsearch_password or ""

    es_kwargs = {
        "hosts": [es_url],
        "verify_certs": False,
        "ssl_show_warn": False,
    }
    if es_username:
        es_kwargs["basic_auth"] = (es_username, es_password)

    client = AsyncElasticsearch(**es_kwargs)
    try:
        yield client
    finally:
        await client.close()


@router.post("/query/{db_name}")
async def execute_simple_query(
    db_name: str = Depends(ValidatedDatabaseName),
    query: SimpleQuery = ...,
    es_client: AsyncElasticsearch = Depends(get_elasticsearch),
) -> Dict[str, Any]:
    """
    Execute simple SQL-like query against Elasticsearch
    
    This is PATTERN 1: Direct ES queries for simple instance retrieval
    No TerminusDB involvement - pure document store query
    """
    try:
        # Parse the simple SQL-like query
        # For now, support basic SELECT * FROM Class WHERE field op value
        query_str = query.query.strip()
        
        # Extract class name (preserve original case)
        query_upper = query_str.upper()
        if "FROM" not in query_upper:
            raise ValueError("Query must contain FROM clause")
        
        # Find the position of FROM to extract class name with original case
        from_index = query_upper.index("FROM") + 4
        remaining = query_str[from_index:].strip()
        
        # Get class name (first word after FROM)
        class_name = remaining.split()[0] if remaining else None
        
        if not class_name:
            raise ValueError("No class name specified")
        
        # Build ES query
        # Note: class_id is mapped as text with keyword subfield
        es_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"class_id.keyword": class_name}}
                    ]
                }
            },
            "size": 100
        }
        
        # Add WHERE conditions if present
        if "WHERE" in query_upper:
            where_index = query_upper.index("WHERE") + 5
            where_part = query_str[where_index:].strip()
            
            # Simple parser for conditions like "price > 1000"
            if ">" in where_part:
                field, value = where_part.split(">")
                field = field.strip()
                value = float(value.strip())
                es_query["query"]["bool"]["must"].append({
                    "range": {f"data.{field}": {"gt": value}}
                })
            elif "<" in where_part:
                field, value = where_part.split("<")
                field = field.strip()
                value = float(value.strip())
                es_query["query"]["bool"]["must"].append({
                    "range": {f"data.{field}": {"lt": value}}
                })
            elif "=" in where_part:
                field, value = where_part.split("=")
                field = field.strip()
                value = value.strip().strip('"').strip("'")
                es_query["query"]["bool"]["must"].append({
                    "term": {f"data.{field}": value}
                })
        
        # Execute ES query
        index_name = f"{db_name.lower()}_instances"
        logger.info(f"Executing ES query on index {index_name}: {es_query}")
        result = await es_client.search(
            index=index_name,
            body=es_query,
            ignore_unavailable=True,
            allow_no_indices=True,
        )
        logger.info(f"ES query result: {result.get('hits', {}).get('total', {})}")
        
        # Extract documents
        hits = result.get("hits", {}).get("hits", [])
        documents = []
        for hit in hits:
            source = hit["_source"]
            # Return the data field which contains the actual instance data
            doc = source.get("data", {})
            doc["_instance_id"] = source.get("instance_id")
            doc["_class_id"] = source.get("class_id")
            documents.append(doc)
        
        return {
            "success": True,
            "data": documents,
            "count": len(documents),
            "query": query_str
        }
        
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await es_client.close()


@router.post("/query/{db_name}/woql")
async def execute_woql_query(
    db_name: str = Depends(ValidatedDatabaseName),
    query: WOQLQuery = ...,
    terminus: AsyncTerminusService = TerminusServiceDep,
    es_client: AsyncElasticsearch = Depends(get_elasticsearch),
) -> Dict[str, Any]:
    """
    Execute WOQL query for graph analysis
    
    This is PATTERN 3: Complex graph queries using TerminusDB
    Returns lightweight nodes with optional ES document enrichment
    """
    try:
        # Execute WOQL query via TerminusDB
        woql_result = await terminus.execute_woql(db_name, query.query)
        
        # Extract bindings
        bindings = woql_result.get("bindings", [])
        
        # Optionally enrich with ES documents
        # (For now, just return the graph results)
        
        return {
            "success": True,
            "data": {
                "bindings": bindings,
                "count": len(bindings)
            },
            "query_type": "woql"
        }
        
    except Exception as e:
        logger.error(f"WOQL query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await es_client.close()


@router.get("/instances/{db_name}/{class_id}")
async def list_instances(
    db_name: str = Depends(ValidatedDatabaseName),
    class_id: str = ...,
    limit: int = QueryParam(default=100, le=1000),
    offset: int = QueryParam(default=0, ge=0),
    es_client: AsyncElasticsearch = Depends(get_elasticsearch),
) -> Dict[str, Any]:
    """
    List instances of a class from Elasticsearch
    
    Direct ES query for listing instances - no graph traversal needed
    """
    try:
        index_name = f"{db_name.lower()}_instances"
        
        # Build ES query
        es_query = {
            "query": {
                "term": {"class_id": class_id}
            },
            "size": limit,
            "from": offset
        }
        
        # Execute query
        result = await es_client.search(
            index=index_name,
            body=es_query
        )
        
        # Extract instances
        hits = result.get("hits", {}).get("hits", [])
        instances = []
        for hit in hits:
            source = hit["_source"]
            instance = source.get("data", {})
            instance["_metadata"] = {
                "instance_id": source.get("instance_id"),
                "class_id": source.get("class_id"),
                "created_at": source.get("created_at"),
                "s3_uri": source.get("s3_uri")
            }
            instances.append(instance)
        
        return {
            "success": True,
            "data": instances,
            "total": result.get("hits", {}).get("total", {}).get("value", 0),
            "class_id": class_id,
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.error(f"Failed to list instances: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await es_client.close()
