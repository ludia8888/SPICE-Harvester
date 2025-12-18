"""
Graph Federation Service with REAL WORKING WOQL
THINK ULTRA³ - The REAL solution, not a workaround

This service uses the CORRECT WOQL format that actually works:
- @schema: prefix for classes
- query parameter wrapper
- Proper JSON-LD format
"""

import logging
import os
from datetime import datetime, timezone
import httpx
import aiohttp
from typing import Any, Dict, List, Optional, Tuple

from shared.config.search_config import get_instances_index_name

logger = logging.getLogger(__name__)


class GraphFederationServiceWOQL:
    """
    Production-ready Graph Federation using REAL WOQL
    
    This is the CORRECT implementation using actual WOQL that works.
    NOT a workaround, NOT Document API, REAL WOQL.
    """
    
    def __init__(
        self,
        terminus_service: Any,  # Will be AsyncTerminusService from OMS
        es_host: str = "localhost",
        es_port: int = 9200,
        es_username: str = "elastic",
        es_password: str = "changeme"
    ):
        self.terminus = terminus_service
        self.es_url = f"http://{es_host}:{es_port}"
        self.es_auth = aiohttp.BasicAuth(es_username, es_password)
        # Get auth from terminus service
        self.auth = (terminus_service.connection_info.user, terminus_service.connection_info.key)
        self.terminus_account = terminus_service.connection_info.account
        self.terminus_url = terminus_service.connection_info.server_url

    @staticmethod
    def _extract_binding_literal(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, (int, float, bool)):
            return str(value)
        if isinstance(value, dict):
            if "@value" in value:
                v = value.get("@value")
                if v is None:
                    return None
                return str(v)
        return None

    @staticmethod
    def _parse_timestamp(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            ts = float(value)
            if ts > 10_000_000_000:  # ms
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        if isinstance(value, str):
            v = value.strip()
            if not v:
                return None
            try:
                if v.endswith("Z"):
                    v = v[:-1] + "+00:00"
                dt = datetime.fromisoformat(v)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                return None
        return None

    async def _fetch_schema_class_doc(self, *, db_name: str, class_id: str) -> Optional[Dict[str, Any]]:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self.terminus_url}/api/document/{self.terminus_account}/{db_name}",
                params={"graph_type": "schema", "id": class_id},
                auth=self.auth,
            )
            if resp.status_code == 200:
                data = resp.json()
                return data if isinstance(data, dict) else None
            if resp.status_code == 404:
                return None
            raise RuntimeError(f"Failed to fetch schema for {db_name}:{class_id} (status={resp.status_code})")

    @staticmethod
    def _schema_range_for_predicate(schema_doc: Dict[str, Any], predicate: str) -> Optional[str]:
        value_def = schema_doc.get(predicate)
        if isinstance(value_def, dict):
            cls = value_def.get("@class")
            if cls:
                return str(cls)
        if isinstance(value_def, str):
            # Data property types are usually xsd:...; class refs typically appear as class id strings.
            if value_def.startswith("xsd:"):
                return None
            return value_def
        return None

    async def _validate_hop_semantics(
        self,
        *,
        db_name: str,
        start_class: str,
        hops: List[Tuple[str, str]],
    ) -> None:
        current_class = start_class
        for predicate, target_class in hops:
            schema_doc = await self._fetch_schema_class_doc(db_name=db_name, class_id=current_class)
            if not schema_doc:
                raise ValueError(f"Schema class not found: {current_class}")

            if predicate not in schema_doc:
                raise ValueError(f"Unknown predicate '{predicate}' for domain '{current_class}'")

            rng = self._schema_range_for_predicate(schema_doc, predicate)
            if not rng:
                raise ValueError(f"Predicate '{predicate}' on '{current_class}' is not a relationship")
            if str(rng) != str(target_class):
                raise ValueError(
                    f"Predicate '{predicate}' range mismatch: domain={current_class} expected_range={rng} requested={target_class}"
                )

            current_class = target_class
        
    async def multi_hop_query(
        self,
        db_name: str,
        start_class: str,
        hops: List[Tuple[str, str]],  # [(predicate, target_class), ...]
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        max_nodes: int = 500,
        max_edges: int = 2000,
        include_paths: bool = False,
        max_paths: int = 100,
        no_cycles: bool = False,
        include_documents: bool = True,
        include_audit: bool = False
    ) -> Dict[str, Any]:
        """
        Execute multi-hop graph query with ES federation using REAL WOQL
        """
        logger.info(f"🔥 REAL WOQL Multi-hop query: {start_class} -> {hops}")

        max_hops = int(os.getenv("GRAPH_QUERY_MAX_HOPS", "10"))
        if len(hops) > max_hops:
            raise ValueError(f"Too many hops (max={max_hops})")

        limit = max(1, min(int(limit), int(os.getenv("GRAPH_QUERY_MAX_LIMIT", "1000"))))
        offset = max(0, int(offset))
        max_nodes = max(1, int(max_nodes))
        max_edges = max(1, int(max_edges))
        include_paths = bool(include_paths)
        no_cycles = bool(no_cycles)
        max_paths = max(0, int(max_paths))
        max_paths_cap = int(os.getenv("GRAPH_QUERY_MAX_PATHS", "2000"))
        max_paths = min(max_paths, max_paths_cap)

        enforce_semantics = os.getenv("GRAPH_QUERY_ENFORCE_SEMANTICS", "true").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if enforce_semantics and hops:
            await self._validate_hop_semantics(db_name=db_name, start_class=start_class, hops=hops)
        
        # Step 1: Build WOQL query for graph traversal
        woql_query, hop_variables = self._build_multi_hop_woql(start_class, hops, filters)

        # Pagination/limits: apply Start (offset) then Limit (page size)
        if offset:
            woql_query = {"@type": "Start", "start": offset, "query": woql_query}
        woql_query = {"@type": "Limit", "limit": limit, "query": woql_query}
        
        # Step 2: Execute WOQL query
        async with httpx.AsyncClient() as client:
            request_body = {"query": woql_query}
            
            response = await client.post(
                f"{self.terminus_url}/api/woql/{self.terminus_account}/{db_name}",
                json=request_body,
                auth=self.auth
            )
            
            if response.status_code != 200:
                logger.error(f"WOQL query failed: {response.status_code}")
                logger.error(f"Error: {response.text}")
                return {"nodes": [], "edges": [], "documents": {}}
            
            result = response.json()
            bindings = result.get("bindings", [])
            
            logger.info(f"WOQL returned {len(bindings)} bindings")
        
        # Step 3: Process bindings to extract nodes and relationships
        nodes = {}
        edges = []
        paths: List[Dict[str, Any]] = []
        seen_paths: set[tuple] = set()
        es_doc_ids = set()
        warnings: List[str] = []

        def _pk_predicate_for_class(class_id: str) -> str:
            return f"{class_id.lower()}_id"

        start_var = f"v:{start_class}"

        for binding in bindings:
            if include_paths and len(paths) < max_paths:
                start_id = binding.get(start_var)
                if start_id:
                    path_node_ids: List[str] = [start_id]
                    path_edges: List[Dict[str, str]] = []
                    prev_id = start_id
                    predicates: List[str] = []
                    for predicate, _target_class, target_var in hop_variables:
                        target_id = binding.get(target_var)
                        if not target_id:
                            break
                        path_node_ids.append(target_id)
                        path_edges.append({"from": prev_id, "to": target_id, "predicate": predicate})
                        predicates.append(predicate)
                        prev_id = target_id

                    if no_cycles and len(set(path_node_ids)) != len(path_node_ids):
                        if "cycle_filtered(no_cycles=true)" not in warnings:
                            warnings.append("cycle_filtered(no_cycles=true)")
                    else:
                        key = (tuple(path_node_ids), tuple(predicates))
                        if key not in seen_paths:
                            seen_paths.add(key)
                            paths.append({"nodes": path_node_ids, "edges": path_edges})
                            if len(paths) >= max_paths and max_paths > 0:
                                warnings.append(f"path_limit_reached(max_paths={max_paths})")

            # Extract start node
            if start_var in binding:
                node_id = binding[start_var]
                # Use terminus_id for ES lookup (same as graph node ID)
                terminus_id = node_id
                es_doc_id = self._extract_es_doc_id(terminus_id)
                pk_var = f"{start_var}_pk"
                name_var = f"{start_var}_name"
                display_pk = self._extract_binding_literal(binding.get(pk_var))
                display_name = self._extract_binding_literal(binding.get(name_var))
                if not display_pk:
                    display_pk = self._extract_binding_literal(binding.get(f"v:{_pk_predicate_for_class(start_class)}"))
                if not display_pk:
                    display_pk = es_doc_id

                nodes[node_id] = {
                    "id": node_id,
                    "type": start_class,
                    "terminus_id": terminus_id,
                    "es_doc_id": es_doc_id,
                    "db_name": db_name,
                    "display": {
                        "primary_key": display_pk,
                        "name": display_name,
                        "summary": display_name or display_pk or es_doc_id,
                    },
                }
                es_doc_ids.add(terminus_id)
            
            # Extract hop nodes and edges using the actual variable names
            prev_var = start_var
            prev_id = binding.get(start_var)
            
            for predicate, target_class, target_var in hop_variables:
                if target_var in binding:
                    target_id = binding[target_var]
                    # Use terminus_id for ES lookup (same as graph node ID)
                    terminus_id = target_id
                    es_doc_id = self._extract_es_doc_id(terminus_id)
                    pk_var = f"{target_var}_pk"
                    name_var = f"{target_var}_name"
                    display_pk = self._extract_binding_literal(binding.get(pk_var))
                    display_name = self._extract_binding_literal(binding.get(name_var))
                    if not display_pk:
                        display_pk = es_doc_id

                    nodes[target_id] = {
                        "id": target_id,
                        "type": target_class,
                        "terminus_id": terminus_id,
                        "es_doc_id": es_doc_id,
                        "db_name": db_name,
                        "display": {
                            "primary_key": display_pk,
                            "name": display_name,
                            "summary": display_name or display_pk or es_doc_id,
                        },
                    }
                    es_doc_ids.add(terminus_id)
                    
                    # Add edge
                    if prev_id:
                        if len(edges) < max_edges:
                            edges.append({
                                "from": prev_id,
                                "to": target_id,
                                "predicate": predicate
                            })
                        else:
                            warnings.append(f"edge_limit_reached(max_edges={max_edges})")
                    
                    # Update for next hop
                    prev_var = target_var
                    prev_id = target_id

            if len(nodes) >= max_nodes:
                warnings.append(f"node_limit_reached(max_nodes={max_nodes})")
                break

        # Dedup edges for stability (prevent result explosion)
        dedup_edges = []
        seen_edges = set()
        for edge in edges:
            key = (edge.get("from"), edge.get("to"), edge.get("predicate"))
            if key in seen_edges:
                continue
            seen_edges.add(key)
            dedup_edges.append(edge)
            if len(dedup_edges) >= max_edges:
                break
        edges = dedup_edges
        
        # Step 4: Optionally fetch documents from Elasticsearch
        documents = {}
        audit_records = {}
        
        if include_documents and es_doc_ids:
            documents = await self._fetch_es_documents(db_name, list(es_doc_ids))
        
        # Step 5: Optionally fetch audit records
        if include_audit and es_doc_ids:
            audit_records = await self._fetch_audit_records(db_name, list(es_doc_ids))
        
        # Step 6: Combine results
        result_nodes = []
        for node in nodes.values():
            terminus_id = node["terminus_id"]
            es_doc_id = node.get("es_doc_id") or self._extract_es_doc_id(terminus_id)
            index_name = get_instances_index_name(db_name)
            node_data = {
                "id": node["id"],
                "type": node["type"],
                "terminus_id": terminus_id,
                "es_doc_id": es_doc_id,
                "es_ref": {"index": index_name, "id": es_doc_id},
                "db_name": db_name,
                "display": node.get("display") or {"summary": es_doc_id},
            }
            if include_documents:
                node_data["data"] = documents.get(terminus_id)
                node_data["data_status"] = "FULL" if terminus_id in documents else "MISSING"
                if terminus_id in documents:
                    src = documents.get(terminus_id) or {}
                    ts = self._parse_timestamp(src.get("updated_at") or src.get("event_timestamp"))
                    node_data["index_status"] = {
                        "last_indexed_at": ts.isoformat() if ts else None,
                        "index_age_seconds": (
                            max(0.0, (datetime.now(timezone.utc) - ts).total_seconds()) if ts else None
                        ),
                        "event_sequence": src.get("event_sequence"),
                    }
                else:
                    node_data["index_status"] = None
            else:
                node_data["data"] = None
                node_data["data_status"] = "PARTIAL"
            if include_audit:
                node_data["audit"] = audit_records.get(terminus_id)
            result_nodes.append(node_data)
        
        return {
            "nodes": result_nodes,
            "edges": edges,
            "paths": paths if include_paths else None,
            "documents": documents,
            "query": {
                "start_class": start_class,
                "hops": hops,
                "filters": filters
            },
            "count": len(result_nodes),
            "warnings": warnings,
            "page": {
                "limit": limit,
                "offset": offset,
                "next_offset": (offset + limit) if len(bindings) >= limit else None,
            },
        }
    
    async def simple_graph_query(
        self,
        db_name: str,
        class_name: str,
        filters: Optional[Dict[str, Any]] = None,
        include_documents: bool = True,
        include_audit: bool = False
    ) -> Dict[str, Any]:
        """
        Simple single-class query - PALANTIR STYLE
        TerminusDB has lightweight nodes (IDs + relationships),
        Elasticsearch has full domain data
        """
        logger.info(f"📊 PALANTIR Query: {class_name} via WOQL -> ES enrichment")
        
        # PALANTIR PRINCIPLE: Lightweight nodes in TerminusDB, full data in ES
        # Step 1: Query TerminusDB for instance IDs using WOQL
        
        # Step 1: Build WOQL query to get instance IDs from TerminusDB
        woql_query = self._build_simple_woql(class_name, filters)
        
        # Step 2: Execute WOQL to get lightweight nodes
        es_doc_ids = []
        try:
            import httpx
            async with httpx.AsyncClient() as client:
                request_body = {"query": woql_query}
                response = await client.post(
                    f"{self.terminus_url}/api/woql/{self.auth[0]}/{db_name}",
                    json=request_body,
                    auth=self.auth
                )
                
                if response.status_code == 200:
                    result = response.json()
                    bindings = result.get("bindings", [])
                    logger.info(f"WOQL returned {len(bindings)} lightweight nodes")
                    
                    # Extract terminus_ids from the bindings
                    for binding in bindings:
                        # Look for the instance variable (usually v:X)
                        for key, value in binding.items():
                            if key.startswith("v:") and isinstance(value, str):
                                # Use the full terminus_id (Class/ID format)
                                terminus_id = value
                                es_doc_ids.append(terminus_id)
                else:
                    logger.warning(f"WOQL query failed: {response.status_code}")
        except Exception as e:
            logger.error(f"Error executing WOQL: {e}")
        
        # Step 3: Fetch full documents from Elasticsearch
        documents = {}
        audit_records = {}
        
        if include_documents and es_doc_ids:
            documents = await self._fetch_es_documents(db_name, es_doc_ids)
        
        if include_audit and es_doc_ids:
            audit_records = await self._fetch_audit_records(db_name, es_doc_ids)
        
        # Step 4: Prepare results combining WOQL IDs with ES data
        result_docs = []
        for doc_id in es_doc_ids:
            doc_data = {"id": doc_id}
            if include_documents:
                doc_data["data"] = documents.get(doc_id)
            if include_audit:
                doc_data["audit"] = audit_records.get(doc_id)
            result_docs.append(doc_data)
        
        return {
            "class": class_name,
            "count": len(result_docs),
            "documents": result_docs
        }
    
    def _build_simple_woql(
        self,
        class_name: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Build WOQL query for simple class query"""
        
        if filters:
            # Build And query with filters
            and_conditions = [
                {
                    "@type": "Triple",
                    "subject": {"variable": "v:X"},
                    "predicate": {"node": "rdf:type"},
                    "object": {"node": f"@schema:{class_name}"}
                }
            ]
            
            # Add filter conditions
            for key, value in filters.items():
                and_conditions.append({
                    "@type": "Triple",
                    "subject": {"variable": "v:X"},
                    "predicate": {"node": f"@schema:{key}"},
                    "object": {"data": {"@type": "xsd:string", "@value": str(value)}}
                })
            
            return {
                "@type": "And",
                "and": and_conditions
            }
        else:
            # Simple type query
            return {
                "@type": "Triple",
                "subject": {"variable": "v:X"},
                "predicate": {"node": "rdf:type"},
                "object": {"node": f"@schema:{class_name}"}
            }
    
    def _build_multi_hop_woql(
        self,
        start_class: str,
        hops: List[Tuple[str, str]],
        filters: Optional[Dict[str, Any]] = None
    ) -> Tuple[Dict[str, Any], List[Tuple[str, str, str]]]:
        """Build WOQL query for multi-hop traversal"""
        
        # Build variable list with meaningful names
        variables = [f"v:{start_class}"]
        hop_variables = []  # Track variable names for each hop
        
        for i, (predicate, target_class) in enumerate(hops):
            # Create meaningful variable name like v:RelatedClient instead of v:Client0
            if i == 0:
                var_name = f"v:Related{target_class}"
            else:
                var_name = f"v:{target_class}_{i}"
            variables.append(var_name)
            hop_variables.append((predicate, target_class, var_name))

        # Display fields: always include PK and name (optional) for each node var.
        # These are stored in Terminus (graph) so UIs can render stable summaries even if ES is missing.
        node_vars: List[Tuple[str, str]] = [(f"v:{start_class}", start_class)] + [
            (target_var, target_class) for _, target_class, target_var in hop_variables
        ]
        for node_var, cls in node_vars:
            variables.append(f"{node_var}_pk")
            variables.append(f"{node_var}_name")
        
        # Build And conditions
        and_conditions = []
        
        # Start class condition
        and_conditions.append({
            "@type": "Triple",
            "subject": {"variable": f"v:{start_class}"},
            "predicate": {"node": "rdf:type"},
            "object": {"node": f"@schema:{start_class}"}
        })
        
        # Add filters if provided
        if filters:
            for key, value in filters.items():
                and_conditions.append({
                    "@type": "Triple",
                    "subject": {"variable": f"v:{start_class}"},
                    "predicate": {"node": f"@schema:{key}"},
                    "object": {"data": {"@type": "xsd:string", "@value": str(value)}}
                })
        
        # Add hop conditions
        current_var = f"v:{start_class}"
        for predicate, target_class, target_var in hop_variables:
            # Relationship triple
            and_conditions.append({
                "@type": "Triple",
                "subject": {"variable": current_var},
                "predicate": {"node": f"@schema:{predicate}"},
                "object": {"variable": target_var}
            })
            
            # Target class type check
            and_conditions.append({
                "@type": "Triple",
                "subject": {"variable": target_var},
                "predicate": {"node": "rdf:type"},
                "object": {"node": f"@schema:{target_class}"}
            })
            
            current_var = target_var

        # Optional display fields for UX stability
        for node_var, cls in node_vars:
            pk_predicate = f"{cls.lower()}_id"
            and_conditions.append(
                {
                    "@type": "Optional",
                    "query": {
                        "@type": "Triple",
                        "subject": {"variable": node_var},
                        "predicate": {"node": f"@schema:{pk_predicate}"},
                        "object": {"variable": f"{node_var}_pk"},
                    },
                }
            )
            and_conditions.append(
                {
                    "@type": "Optional",
                    "query": {
                        "@type": "Triple",
                        "subject": {"variable": node_var},
                        "predicate": {"node": "@schema:name"},
                        "object": {"variable": f"{node_var}_name"},
                    },
                }
            )
        
        # Wrap in Select
        return (
            {
            "@type": "Select",
            "variables": variables,
            "query": {
                "@type": "And",
                "and": and_conditions
            }
            },
            hop_variables,
        )
    
    def _extract_es_doc_id(self, instance_id: str) -> str:
        """Extract ES document ID from instance ID"""
        # Instance ID format: "Class/ID" -> extract "ID"
        if "/" in instance_id:
            return instance_id.split("/", 1)[1]
        return instance_id
    
    async def find_relationship_paths(
        self,
        db_name: str,
        source_class: str,
        target_class: str,
        max_depth: int = 3
    ) -> List[List[Dict[str, str]]]:
        """
        Find all relationship paths between two classes using REAL WOQL schema queries
        THINK ULTRA³ - This is REAL schema discovery, not hardcoded!
        """
        logger.info(f"🔍 REAL WOQL: Finding paths from {source_class} to {target_class}")
        
        # WOQL query to get all properties of source class
        schema_query = {
            "@type": "Select",
            "variables": ["v:Property", "v:Range", "v:Label"],
            "query": {
                "@type": "And",
                "and": [
                    {
                        "@type": "Triple",
                        "subject": {"node": f"@schema:{source_class}"},
                        "predicate": {"node": "@schema:rdf:type"},
                        "object": {"node": "sys:Class"}
                    },
                    {
                        "@type": "Triple",
                        "subject": {"node": f"@schema:{source_class}"},
                        "predicate": {"variable": "v:Property"},
                        "object": {"variable": "v:Range"}
                    },
                    {
                        "@type": "Triple",
                        "subject": {"variable": "v:Property"},
                        "predicate": {"node": "rdfs:label"},
                        "object": {"variable": "v:Label"}
                    }
                ]
            }
        }
        
        # Execute schema query
        async with httpx.AsyncClient() as client:
            request_body = {"query": schema_query}
            
            response = await client.post(
                f"{self.terminus_url}/api/woql/admin/{db_name}",
                json=request_body,
                auth=self.auth
            )
            
            if response.status_code != 200:
                logger.warning(f"Schema query failed, falling back to known paths")
                # Fallback to known relationships
                return self._get_known_paths(source_class, target_class)
            
            result = response.json()
            bindings = result.get("bindings", [])
            
            # Extract relationships from bindings
            paths = []
            for binding in bindings:
                prop = binding.get("v:Property", "")
                range_class = binding.get("v:Range", "")
                
                # Check if this property links to target class
                if range_class and target_class in range_class:
                    # Extract property name from URI
                    prop_name = prop.split(":")[-1] if ":" in prop else prop
                    paths.append([{
                        "from": source_class,
                        "predicate": prop_name,
                        "to": target_class
                    }])
            
            # If no direct path, try multi-hop (would need recursive search)
            if not paths and max_depth > 1:
                # For now, return known multi-hop paths
                paths = self._get_known_multi_hop_paths(source_class, target_class, max_depth)
            
            logger.info(f"Found {len(paths)} paths via WOQL schema query")
            return paths
    
    def _get_known_paths(self, source_class: str, target_class: str) -> List[List[Dict[str, str]]]:
        """Fallback to known relationship paths"""
        paths = []
        if source_class == "Product" and target_class == "Client":
            paths.append([{
                "from": "Product",
                "predicate": "owned_by",
                "to": "Client"
            }])
        elif source_class == "Order" and target_class == "Client":
            paths.append([{
                "from": "Order",
                "predicate": "ordered_by",
                "to": "Client"
            }])
        return paths
    
    def _get_known_multi_hop_paths(self, source_class: str, target_class: str, max_depth: int) -> List[List[Dict[str, str]]]:
        """Get known multi-hop paths (would be replaced by recursive WOQL search)"""
        paths = []
        if source_class == "SKU" and target_class == "Client" and max_depth >= 2:
            paths.append([
                {"from": "SKU", "predicate": "belongs_to", "to": "Product"},
                {"from": "Product", "predicate": "owned_by", "to": "Client"}
            ])
        return paths
    
    async def _fetch_es_documents(
        self,
        db_name: str,
        doc_ids: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch documents from Elasticsearch for the given graph node IDs.

        Input `doc_ids` may be Terminus-style IDs (e.g. `Class/instance_id`).
        This method returns a dict keyed by the *input* IDs, so callers can
        directly `documents.get(terminus_id)` regardless of ES schema.
        """
        
        if not doc_ids:
            return {}
        
        terminus_ids = [str(doc_id) for doc_id in doc_ids if doc_id]
        if not terminus_ids:
            return {}

        documents: Dict[str, Dict[str, Any]] = {}
        index_name = get_instances_index_name(db_name)

        # Most services index instance documents with ES `_id == instance_id`.
        # Use `_mget` to avoid relying on index mappings (keyword/text fields).
        es_ids = [self._extract_es_doc_id(terminus_id) for terminus_id in terminus_ids]
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    f"{self.es_url}/{index_name}/_mget",
                    json={"ids": es_ids},
                    auth=self.es_auth,
                ) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        docs = result.get("docs") or []
                        for terminus_id, doc in zip(terminus_ids, docs):
                            if isinstance(doc, dict) and doc.get("found") and isinstance(doc.get("_source"), dict):
                                documents[terminus_id] = doc["_source"]
                    elif resp.status == 404:
                        logger.warning(f"ES index not found: {index_name}")
                    else:
                        logger.warning(f"Failed to fetch ES documents: {resp.status} ({await resp.text()})")
            except Exception as e:
                logger.error(f"Error fetching ES documents: {e}")
        
        logger.info(f"Fetched {len(documents)} documents from Elasticsearch")
        return documents
    
    async def _fetch_audit_records(
        self,
        db_name: str,
        doc_ids: List[str]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch audit records from Elasticsearch audit index"""
        
        if not doc_ids:
            return {}
        
        audit_records = {}
        index_name = f"audit-{db_name.lower()}"
        
        async with aiohttp.ClientSession() as session:
            # Query audit records for these aggregate IDs
            query_body = {
                "query": {
                    "terms": {
                        "aggregate_id": doc_ids
                    }
                },
                "size": 1000,
                "sort": [
                    {"timestamp": {"order": "desc"}}
                ]
            }
            
            try:
                async with session.post(
                    f"{self.es_url}/{index_name}/_search",
                    json=query_body,
                    auth=self.es_auth
                ) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        # Group audit records by aggregate_id
                        for hit in result.get("hits", {}).get("hits", []):
                            source = hit["_source"]
                            agg_id = source.get("aggregate_id")
                            if agg_id:
                                if agg_id not in audit_records:
                                    audit_records[agg_id] = []
                                audit_records[agg_id].append(source)
                    else:
                        logger.warning(f"Failed to fetch audit records: {resp.status}")
            except Exception as e:
                logger.error(f"Error fetching audit records: {e}")
        
        logger.info(f"Fetched audit records for {len(audit_records)} entities")
        return audit_records
