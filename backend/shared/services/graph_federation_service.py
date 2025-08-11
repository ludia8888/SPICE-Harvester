"""
Graph Federation Service
THINK ULTRA³ - Graph Authority with ES Federation

This service combines:
1. TerminusDB graph traversal (authority)
2. Elasticsearch document retrieval (payload)
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
import aiohttp
import asyncio
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class GraphNode:
    """Represents a node in the graph with ES reference"""
    id: str
    type: str
    es_doc_id: str
    s3_uri: Optional[str] = None
    relationships: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.relationships is None:
            self.relationships = {}


class GraphFederationService:
    """
    Federates graph queries between TerminusDB and Elasticsearch
    
    Architecture:
    - TerminusDB: Graph structure, relationships (authority)
    - Elasticsearch: Document payload (projection)
    - S3: Original event data (source)
    """
    
    def __init__(
        self,
        terminus_service,
        es_host: str = "localhost",
        es_port: int = 9201
    ):
        self.terminus = terminus_service
        self.es_url = f"http://{es_host}:{es_port}"
        
    async def multi_hop_query(
        self,
        db_name: str,
        start_class: str,
        hops: List[Tuple[str, str]],  # [(predicate, target_class), ...]
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Execute multi-hop graph query with ES federation
        
        Args:
            db_name: Database name
            start_class: Starting class (e.g., "Product")
            hops: List of (predicate, target_class) tuples
                  e.g., [("owned_by", "Client"), ("managed_by", "Manager")]
            filters: Optional filters on start class
            limit: Maximum results
            
        Returns:
            Combined graph + document results
        """
        logger.info(f"🔥 Multi-hop query: {start_class} -> {hops}")
        
        # Step 1: Build WOQL query for graph traversal
        woql_query = self._build_multi_hop_woql(start_class, hops, filters)
        
        # Step 2: Execute graph query in TerminusDB
        graph_results = await self.terminus.execute_query(db_name, woql_query)
        
        if not graph_results:
            logger.info("No graph results found")
            return {"nodes": [], "edges": [], "documents": {}}
        
        # Step 3: Extract ES document IDs from graph results
        es_doc_ids_by_class = self._extract_es_doc_ids(graph_results, start_class, hops)
        
        # Step 4: Bulk fetch documents from Elasticsearch
        documents = await self._fetch_es_documents(db_name, es_doc_ids_by_class)
        
        # Step 5: Combine graph structure with document data
        result = self._combine_results(graph_results, documents, start_class, hops)
        
        logger.info(f"✅ Multi-hop complete: {len(result['nodes'])} nodes, {len(result['edges'])} edges")
        return result
    
    def _build_multi_hop_woql(
        self,
        start_class: str,
        hops: List[Tuple[str, str]],
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Build WOQL query for multi-hop traversal"""
        
        # Start with the base class
        conditions = []
        
        # Add type constraint for start node
        conditions.append({
            "@type": "woql:Triple",
            "woql:subject": {"@type": "woql:Node", "@value": f"v:{start_class}"},
            "woql:predicate": {"@type": "woql:Node", "@value": "@type"},
            "woql:object": {"@type": "woql:Node", "@value": start_class}
        })
        
        # Add ES doc ID extraction for start node
        conditions.append({
            "@type": "woql:Triple",
            "woql:subject": {"@type": "woql:Node", "@value": f"v:{start_class}"},
            "woql:predicate": {"@type": "woql:Node", "@value": "es_doc_id"},
            "woql:object": {"@type": "woql:Node", "@value": f"v:{start_class}_ES"}
        })
        
        # Build hop conditions
        prev_var = f"v:{start_class}"
        for i, (predicate, target_class) in enumerate(hops):
            target_var = f"v:{target_class}_{i}"
            
            # Add relationship triple
            conditions.append({
                "@type": "woql:Triple",
                "woql:subject": {"@type": "woql:Node", "@value": prev_var},
                "woql:predicate": {"@type": "woql:Node", "@value": predicate},
                "woql:object": {"@type": "woql:Node", "@value": target_var}
            })
            
            # Add type constraint for target
            conditions.append({
                "@type": "woql:Triple",
                "woql:subject": {"@type": "woql:Node", "@value": target_var},
                "woql:predicate": {"@type": "woql:Node", "@value": "@type"},
                "woql:object": {"@type": "woql:Node", "@value": target_class}
            })
            
            # Add ES doc ID extraction for target
            conditions.append({
                "@type": "woql:Triple",
                "woql:subject": {"@type": "woql:Node", "@value": target_var},
                "woql:predicate": {"@type": "woql:Node", "@value": "es_doc_id"},
                "woql:object": {"@type": "woql:Node", "@value": f"v:{target_class}_{i}_ES"}
            })
            
            prev_var = target_var
        
        # Apply filters if provided
        if filters:
            for key, value in filters.items():
                conditions.append({
                    "@type": "woql:Triple",
                    "woql:subject": {"@type": "woql:Node", "@value": f"v:{start_class}"},
                    "woql:predicate": {"@type": "woql:Node", "@value": key},
                    "woql:object": {"@type": "woql:DataValue", "@value": value}
                })
        
        # Wrap in And clause
        return {
            "@type": "woql:And",
            "woql:and": conditions
        }
    
    def _extract_es_doc_ids(
        self,
        graph_results: List[Dict[str, Any]],
        start_class: str,
        hops: List[Tuple[str, str]]
    ) -> Dict[str, List[str]]:
        """Extract ES document IDs grouped by class"""
        
        es_doc_ids = {}
        
        # Handle different result formats
        if isinstance(graph_results, dict) and "bindings" in graph_results:
            bindings = graph_results["bindings"]
        elif isinstance(graph_results, list):
            bindings = graph_results
        else:
            logger.warning(f"Unexpected graph result format: {type(graph_results)}")
            return es_doc_ids
        
        # Extract start class IDs
        start_es_key = f"{start_class}_ES"
        es_doc_ids[start_class] = []
        
        for binding in bindings:
            # Get start class ES ID
            if start_es_key in binding:
                es_id = binding[start_es_key]
                if isinstance(es_id, dict):
                    es_id = es_id.get("@value", es_id)
                if es_id and es_id not in es_doc_ids[start_class]:
                    es_doc_ids[start_class].append(es_id)
            
            # Get hop target IDs
            for i, (_, target_class) in enumerate(hops):
                target_es_key = f"{target_class}_{i}_ES"
                if target_es_key in binding:
                    if target_class not in es_doc_ids:
                        es_doc_ids[target_class] = []
                    
                    es_id = binding[target_es_key]
                    if isinstance(es_id, dict):
                        es_id = es_id.get("@value", es_id)
                    if es_id and es_id not in es_doc_ids[target_class]:
                        es_doc_ids[target_class].append(es_id)
        
        logger.info(f"Extracted ES IDs: {', '.join(f'{k}:{len(v)}' for k, v in es_doc_ids.items())}")
        return es_doc_ids
    
    async def _fetch_es_documents(
        self,
        db_name: str,
        es_doc_ids_by_class: Dict[str, List[str]]
    ) -> Dict[str, Dict[str, Any]]:
        """Fetch documents from Elasticsearch"""
        
        documents = {}
        index_name = f"instances_{db_name.replace('-', '_')}"
        
        async with aiohttp.ClientSession() as session:
            for class_name, doc_ids in es_doc_ids_by_class.items():
                if not doc_ids:
                    continue
                
                # Use ES _mget for bulk fetch
                mget_body = {
                    "ids": doc_ids
                }
                
                try:
                    async with session.post(
                        f"{self.es_url}/{index_name}/_mget",
                        json=mget_body
                    ) as resp:
                        if resp.status == 200:
                            result = await resp.json()
                            for doc in result.get("docs", []):
                                if doc.get("found"):
                                    doc_id = doc["_id"]
                                    documents[doc_id] = doc["_source"]
                                    logger.debug(f"Fetched {class_name} document: {doc_id}")
                        else:
                            logger.warning(f"Failed to fetch {class_name} documents: {resp.status}")
                except Exception as e:
                    logger.error(f"Error fetching {class_name} documents: {e}")
        
        logger.info(f"Fetched {len(documents)} documents from Elasticsearch")
        return documents
    
    def _combine_results(
        self,
        graph_results: Any,
        documents: Dict[str, Dict[str, Any]],
        start_class: str,
        hops: List[Tuple[str, str]]
    ) -> Dict[str, Any]:
        """Combine graph structure with document data"""
        
        nodes = []
        edges = []
        seen_nodes = set()
        
        # Parse bindings
        if isinstance(graph_results, dict) and "bindings" in graph_results:
            bindings = graph_results["bindings"]
        elif isinstance(graph_results, list):
            bindings = graph_results
        else:
            bindings = []
        
        for binding in bindings:
            # Process start node
            start_var = f"{start_class}"
            if start_var in binding:
                node_id = binding[start_var]
                if isinstance(node_id, dict):
                    node_id = node_id.get("@value", node_id)
                    
                if node_id and node_id not in seen_nodes:
                    seen_nodes.add(node_id)
                    
                    # Get ES document ID
                    es_id = binding.get(f"{start_class}_ES")
                    if isinstance(es_id, dict):
                        es_id = es_id.get("@value", es_id)
                    
                    # Combine graph node with ES document
                    node_data = {
                        "id": node_id,
                        "type": start_class,
                        "es_doc_id": es_id
                    }
                    
                    # Add document data if available
                    if es_id and es_id in documents:
                        node_data["data"] = documents[es_id]
                    
                    nodes.append(node_data)
            
            # Process hop nodes and edges
            prev_node = binding.get(start_var)
            if isinstance(prev_node, dict):
                prev_node = prev_node.get("@value", prev_node)
                
            for i, (predicate, target_class) in enumerate(hops):
                target_var = f"{target_class}_{i}"
                if target_var in binding:
                    target_node = binding[target_var]
                    if isinstance(target_node, dict):
                        target_node = target_node.get("@value", target_node)
                    
                    if target_node and target_node not in seen_nodes:
                        seen_nodes.add(target_node)
                        
                        # Get ES document ID
                        es_id = binding.get(f"{target_class}_{i}_ES")
                        if isinstance(es_id, dict):
                            es_id = es_id.get("@value", es_id)
                        
                        # Create node
                        node_data = {
                            "id": target_node,
                            "type": target_class,
                            "es_doc_id": es_id
                        }
                        
                        # Add document data
                        if es_id and es_id in documents:
                            node_data["data"] = documents[es_id]
                        
                        nodes.append(node_data)
                    
                    # Create edge
                    if prev_node and target_node:
                        edges.append({
                            "from": prev_node,
                            "to": target_node,
                            "predicate": predicate
                        })
                    
                    prev_node = target_node
        
        return {
            "nodes": nodes,
            "edges": edges,
            "documents": documents,
            "query": {
                "start_class": start_class,
                "hops": hops
            }
        }
    
    async def simple_graph_query(
        self,
        db_name: str,
        class_name: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Simple single-class query with ES federation
        """
        woql_query = {
            "@type": "woql:And",
            "woql:and": [
                {
                    "@type": "woql:Triple",
                    "woql:subject": {"@type": "woql:Node", "@value": "v:Node"},
                    "woql:predicate": {"@type": "woql:Node", "@value": "@type"},
                    "woql:object": {"@type": "woql:Node", "@value": class_name}
                },
                {
                    "@type": "woql:Triple",
                    "woql:subject": {"@type": "woql:Node", "@value": "v:Node"},
                    "woql:predicate": {"@type": "woql:Node", "@value": "es_doc_id"},
                    "woql:object": {"@type": "woql:Node", "@value": "v:ES"}
                }
            ]
        }
        
        # Add filters
        if filters:
            for key, value in filters.items():
                woql_query["woql:and"].append({
                    "@type": "woql:Triple",
                    "woql:subject": {"@type": "woql:Node", "@value": "v:Node"},
                    "woql:predicate": {"@type": "woql:Node", "@value": key},
                    "woql:object": {"@type": "woql:DataValue", "@value": value}
                })
        
        # Execute query
        graph_results = await self.terminus.execute_query(db_name, woql_query)
        
        # Extract ES IDs
        es_ids = []
        if isinstance(graph_results, dict) and "bindings" in graph_results:
            for binding in graph_results["bindings"]:
                if "ES" in binding:
                    es_id = binding["ES"]
                    if isinstance(es_id, dict):
                        es_id = es_id.get("@value", es_id)
                    if es_id:
                        es_ids.append(es_id)
        
        # Fetch from ES
        documents = await self._fetch_es_documents(db_name, {class_name: es_ids})
        
        return {
            "class": class_name,
            "count": len(documents),
            "documents": list(documents.values())
        }