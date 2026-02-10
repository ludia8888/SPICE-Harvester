"""
ES-Native Graph Federation Service — Search Arounds (Link Traversal).

Replaces GraphFederationServiceWOQL: performs multi-hop relationship
traversal entirely within Elasticsearch.  No TerminusDB dependency.

Algorithm (hop-by-hop):
    Hop 0: ES search(class_id=StartClass + filters)  →  start docs
    Hop 1: extract relationships.{predicate} targets  →  ES mget/search
    Hop N: repeat …                                   →  build graph

Reverse traversal:
    ES terms query on ``relationships.{predicate}`` to find docs that
    reference the current set of instance IDs.
"""

from __future__ import annotations

import logging
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from shared.config.search_config import get_instances_index_name
from shared.config.settings import get_settings
from shared.security.input_sanitizer import validate_branch_name
from shared.services.storage.elasticsearch_service import ElasticsearchService

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_MAX_FAN_OUT = 1000


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class GraphFederationServiceES:
    """ES-native graph traversal — Search Arounds without TerminusDB."""

    def __init__(
        self,
        es_service: ElasticsearchService,
        oms_base_url: Optional[str] = None,
    ):
        self._es = es_service
        self._oms_base_url = oms_base_url
        self._connected = False

    async def _ensure_connected(self) -> None:
        """Lazily connect the ES client on first use."""
        if not self._connected:
            if self._es._client is None:
                await self._es.connect()
            self._connected = True

    # ------------------------------------------------------------------
    # Public: multi-hop query (same signature as WOQL version)
    # ------------------------------------------------------------------

    async def multi_hop_query(
        self,
        *,
        db_name: str,
        start_class: str,
        hops: List[Any],
        base_branch: str = "main",
        overlay_branch: Optional[str] = None,
        terminus_branch: Optional[str] = None,  # ignored — backward compat
        strict_overlay: bool = False,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        max_nodes: int = 500,
        max_edges: int = 2000,
        include_paths: bool = False,
        max_paths: int = 100,
        no_cycles: bool = False,
        include_documents: bool = True,
        include_audit: bool = False,
    ) -> Dict[str, Any]:
        """Execute multi-hop graph query entirely within Elasticsearch."""
        await self._ensure_connected()
        base_branch = validate_branch_name(base_branch or "main")
        index_name = get_instances_index_name(db_name, branch=base_branch)

        graph_settings = get_settings().graph_query
        max_hops = int(graph_settings.max_hops)

        normalized = self._normalize_hops(hops)
        if len(normalized) > max_hops:
            raise ValueError(f"Too many hops ({len(normalized)} > max {max_hops})")

        limit = max(1, min(int(limit), int(graph_settings.max_limit)))
        offset = max(0, int(offset))
        max_nodes = max(1, int(max_nodes))
        max_edges = max(1, int(max_edges))

        # ---- Hop 0: start class -------------------------------------------
        start_docs, _total = await self._search_start_class(
            index_name=index_name,
            class_id=start_class,
            filters=filters,
            limit=limit,
            offset=offset,
        )
        if not start_docs:
            return self._empty_result()

        # Bookkeeping
        all_nodes: Dict[str, Dict[str, Any]] = {}   # key = "Class/instance_id"
        all_edges: List[Dict[str, str]] = []
        all_paths: List[List[str]] = []
        visited: Set[str] = set()

        # Register start nodes
        for doc in start_docs:
            nid = self._node_id(doc)
            all_nodes[nid] = self._make_node(doc)
            visited.add(nid)

        current_docs = start_docs
        # Track per-node paths for include_paths
        # node_paths: for each current doc, the list of node_ids traversed to reach it
        current_paths: Dict[str, List[str]] = {
            self._node_id(doc): [self._node_id(doc)] for doc in start_docs
        }

        # ---- Hop 1…N -------------------------------------------------------
        for hop_idx, (predicate, target_class, reverse) in enumerate(normalized):
            if not current_docs:
                break

            if reverse:
                next_docs, hop_edges = await self._hop_reverse(
                    index_name=index_name,
                    source_docs=current_docs,
                    predicate=predicate,
                    owner_class=target_class,
                    source_class=self._class_of(current_docs),
                    max_fan_out=_DEFAULT_MAX_FAN_OUT,
                )
            else:
                next_docs, hop_edges = await self._hop_forward(
                    index_name=index_name,
                    source_docs=current_docs,
                    predicate=predicate,
                    target_class=target_class,
                    max_fan_out=_DEFAULT_MAX_FAN_OUT,
                )

            # Apply cycle / limits
            new_paths: Dict[str, List[str]] = {}
            filtered_docs: List[Dict[str, Any]] = []
            for doc in next_docs:
                nid = self._node_id(doc)
                if no_cycles and nid in visited:
                    continue
                if len(all_nodes) >= max_nodes:
                    break
                all_nodes[nid] = self._make_node(doc)
                visited.add(nid)
                filtered_docs.append(doc)

                # Extend paths
                if include_paths:
                    # Find parent paths from edges
                    for edge in hop_edges:
                        if edge["to"] == nid:
                            parent_path = current_paths.get(edge["from"], [edge["from"]])
                            new_paths[nid] = parent_path + [nid]
                            break
                    if nid not in new_paths:
                        new_paths[nid] = [nid]

            for edge in hop_edges:
                if len(all_edges) >= max_edges:
                    break
                if edge["to"] in all_nodes and edge["from"] in all_nodes:
                    all_edges.append(edge)

            current_docs = filtered_docs
            current_paths = new_paths

        # ---- Build paths list -----------------------------------------------
        if include_paths:
            # Paths that reach the last hop
            for nid, path in current_paths.items():
                if len(all_paths) >= max_paths:
                    break
                all_paths.append(path)

        # ---- Build response (same shape as WOQL version) --------------------
        nodes_list = list(all_nodes.values())
        return {
            "nodes": nodes_list,
            "edges": all_edges,
            "paths": all_paths if include_paths else [],
            "documents": {n["id"]: n.get("data", {}) for n in nodes_list} if include_documents else {},
            "count": len(nodes_list),
            "warnings": [],
        }

    # ------------------------------------------------------------------
    # Public: simple (single-class) query
    # ------------------------------------------------------------------

    async def simple_graph_query(
        self,
        *,
        db_name: str,
        class_name: str,
        base_branch: str = "main",
        overlay_branch: Optional[str] = None,
        terminus_branch: Optional[str] = None,
        strict_overlay: bool = False,
        filters: Optional[Dict[str, Any]] = None,
        include_documents: bool = True,
        include_audit: bool = False,
    ) -> Dict[str, Any]:
        """Single-class ES search — no hops."""
        await self._ensure_connected()
        base_branch = validate_branch_name(base_branch or "main")
        index_name = get_instances_index_name(db_name, branch=base_branch)

        docs, total = await self._search_start_class(
            index_name=index_name,
            class_id=class_name,
            filters=filters,
            limit=100,
            offset=0,
        )

        nodes = [self._make_node(doc) for doc in docs]
        return {
            "nodes": nodes,
            "edges": [],
            "paths": [],
            "documents": {n["id"]: n.get("data", {}) for n in nodes} if include_documents else {},
            "count": total,
            "warnings": [],
        }

    # ------------------------------------------------------------------
    # Public: find relationship paths (schema-based BFS)
    # ------------------------------------------------------------------

    async def find_relationship_paths(
        self,
        *,
        db_name: str,
        source_class: str,
        target_class: str,
        branch: str = "main",
        max_depth: int = 3,
    ) -> List[List[Dict[str, str]]]:
        """Discover relationship paths between two classes by sampling ES docs."""
        await self._ensure_connected()
        branch = validate_branch_name(branch or "main")
        index_name = get_instances_index_name(db_name, branch=branch)

        # Build adjacency graph from ES doc samples
        adjacency = await self._discover_class_adjacency(index_name)

        # BFS from source_class to target_class
        paths: List[List[Dict[str, str]]] = []
        queue: deque[Tuple[str, List[Dict[str, str]]]] = deque()
        queue.append((source_class, []))

        visited_bfs: Set[str] = {source_class}

        while queue:
            current, path_so_far = queue.popleft()
            if len(path_so_far) >= max_depth:
                continue

            for predicate, neighbor in adjacency.get(current, []):
                hop_step = {"from": current, "predicate": predicate, "to": neighbor}
                new_path = path_so_far + [hop_step]

                if neighbor == target_class:
                    paths.append(new_path)
                    continue

                if neighbor not in visited_bfs:
                    visited_bfs.add(neighbor)
                    queue.append((neighbor, new_path))

        logger.info(
            "Found %d paths from %s to %s (max_depth=%d)",
            len(paths), source_class, target_class, max_depth,
        )
        return paths

    # ==================================================================
    # Internal: hop mechanics
    # ==================================================================

    async def _search_start_class(
        self,
        *,
        index_name: str,
        class_id: str,
        filters: Optional[Dict[str, Any]],
        limit: int,
        offset: int,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Hop 0 — search for start class instances with optional filters."""
        must_clauses: List[Dict[str, Any]] = [{"term": {"class_id": class_id}}]

        if filters:
            for key, value in filters.items():
                must_clauses.append({
                    "bool": {
                        "should": [
                            {"term": {f"data.{key}.keyword": str(value)}},
                            {
                                "nested": {
                                    "path": "properties",
                                    "query": {
                                        "bool": {
                                            "filter": [
                                                {"term": {"properties.name": key}},
                                                {"term": {"properties.value.keyword": str(value)}},
                                            ]
                                        }
                                    },
                                }
                            },
                        ],
                        "minimum_should_match": 1,
                    }
                })

        result = await self._es.search(
            index=index_name,
            query={"bool": {"filter": must_clauses}},
            size=limit,
            from_=offset,
            sort=[{"instance_id": {"order": "asc"}}],
        )
        return result.get("hits", []), result.get("total", 0)

    async def _hop_forward(
        self,
        *,
        index_name: str,
        source_docs: List[Dict[str, Any]],
        predicate: str,
        target_class: str,
        max_fan_out: int = _DEFAULT_MAX_FAN_OUT,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
        """Forward hop: extract relationships.{predicate} → fetch targets via mget."""
        target_ids: List[str] = []
        edges: List[Dict[str, str]] = []

        for doc in source_docs:
            src_nid = self._node_id(doc)
            refs = self._get_relationship_refs(doc, predicate)
            for ref in refs:
                ref_class, ref_id = self._parse_ref(ref)
                if ref_class != target_class:
                    continue
                target_ids.append(ref_id)
                edges.append({
                    "from": src_nid,
                    "to": f"{target_class}/{ref_id}",
                    "predicate": predicate,
                })

        # Deduplicate, cap fan-out
        seen: Set[str] = set()
        unique_ids: List[str] = []
        for tid in target_ids:
            if tid not in seen:
                seen.add(tid)
                unique_ids.append(tid)
                if len(unique_ids) >= max_fan_out:
                    break

        target_docs = await self._mget_instances(
            index_name=index_name,
            class_id=target_class,
            instance_ids=unique_ids,
        )
        return target_docs, edges

    async def _hop_reverse(
        self,
        *,
        index_name: str,
        source_docs: List[Dict[str, Any]],
        predicate: str,
        owner_class: str,
        source_class: str,
        max_fan_out: int = _DEFAULT_MAX_FAN_OUT,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
        """Reverse hop: find owner_class docs whose relationships.{predicate}
        contains references to source_docs."""
        # Build lookup refs: "SourceClass/instance_id"
        source_refs: List[str] = []
        source_nids: Set[str] = set()
        for doc in source_docs:
            nid = self._node_id(doc)
            source_nids.add(nid)
            source_refs.append(nid)  # "Class/instance_id"

        if not source_refs:
            return [], []

        # ES query: class_id=owner_class AND relationships.{predicate} IN source_refs
        # Try .keyword first (dynamic mapping auto-creates it for text fields)
        rel_field = f"relationships.{predicate}"
        query: Dict[str, Any] = {
            "bool": {
                "filter": [
                    {"term": {"class_id": owner_class}},
                    {
                        "bool": {
                            "should": [
                                {"terms": {f"{rel_field}.keyword": source_refs}},
                                {"terms": {rel_field: source_refs}},
                            ],
                            "minimum_should_match": 1,
                        }
                    },
                ]
            }
        }

        result = await self._es.search(
            index=index_name,
            query=query,
            size=min(max_fan_out, 10000),
        )
        owner_docs = result.get("hits", [])

        # Build edges by matching which source refs each owner doc contains
        edges: List[Dict[str, str]] = []
        for owner_doc in owner_docs:
            owner_nid = self._node_id(owner_doc)
            refs = self._get_relationship_refs(owner_doc, predicate)
            for ref in refs:
                if ref in source_nids:
                    edges.append({
                        "from": owner_nid,
                        "to": ref,
                        "predicate": predicate,
                    })

        return owner_docs, edges

    # ------------------------------------------------------------------
    # Internal: ES helpers
    # ------------------------------------------------------------------

    async def _mget_instances(
        self,
        *,
        index_name: str,
        class_id: str,
        instance_ids: List[str],
    ) -> List[Dict[str, Any]]:
        """Batch-fetch instance documents by ID (instance_id == ES _id)."""
        if not instance_ids:
            return []

        try:
            response = await self._es.client.mget(
                index=index_name,
                body={"ids": instance_ids},
            )
            payload = getattr(response, "body", response)
            docs: List[Dict[str, Any]] = []
            for doc in payload.get("docs", []):
                if isinstance(doc, dict) and doc.get("found") and isinstance(doc.get("_source"), dict):
                    docs.append(doc["_source"])
            return docs
        except Exception as exc:
            logger.warning("mget failed for %s/%s: %s", index_name, class_id, exc)
            return []

    async def _discover_class_adjacency(
        self,
        index_name: str,
    ) -> Dict[str, List[Tuple[str, str]]]:
        """Sample ES docs to discover class → [(predicate, target_class)] adjacency."""
        adjacency: Dict[str, List[Tuple[str, str]]] = {}

        try:
            # 1. Discover all class_ids
            class_result = await self._es.search(
                index=index_name,
                size=0,
                aggregations={
                    "classes": {"terms": {"field": "class_id", "size": 200}},
                },
            )
            buckets = class_result.get("aggregations", {}).get("classes", {}).get("buckets", [])
            class_ids = [b["key"] for b in buckets if "key" in b]

            # 2. For each class, sample docs to discover relationship predicates
            for cls in class_ids:
                sample = await self._es.search(
                    index=index_name,
                    query={"term": {"class_id": cls}},
                    size=5,
                    source_includes=["relationships"],
                )
                neighbors: List[Tuple[str, str]] = []
                seen_pairs: Set[Tuple[str, str]] = set()
                for doc in sample.get("hits", []):
                    rels = doc.get("relationships") or {}
                    for predicate, refs in rels.items():
                        if isinstance(refs, str):
                            refs = [refs]
                        if not isinstance(refs, list):
                            continue
                        for ref in refs:
                            target_class, _ = self._parse_ref(str(ref))
                            if target_class and (predicate, target_class) not in seen_pairs:
                                seen_pairs.add((predicate, target_class))
                                neighbors.append((predicate, target_class))
                adjacency[cls] = neighbors
        except Exception as exc:
            logger.warning("Class adjacency discovery failed: %s", exc)

        return adjacency

    # ------------------------------------------------------------------
    # Internal: node / ref utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_hops(hops: Any) -> List[Tuple[str, str, bool]]:
        """Normalize hop specs into (predicate, target_class, reverse) tuples."""
        output: List[Tuple[str, str, bool]] = []
        if not isinstance(hops, list):
            return output
        for hop in hops:
            predicate = None
            target_class = None
            reverse = False
            if isinstance(hop, (tuple, list)) and len(hop) >= 2:
                predicate = hop[0]
                target_class = hop[1]
                if len(hop) >= 3:
                    reverse = bool(hop[2])
            elif isinstance(hop, dict):
                predicate = hop.get("predicate")
                target_class = hop.get("target_class") or hop.get("targetClass")
                reverse = bool(hop.get("reverse", False))
            if not predicate or not target_class:
                continue
            output.append((str(predicate), str(target_class), reverse))
        return output

    @staticmethod
    def _node_id(doc: Dict[str, Any]) -> str:
        """Build canonical node ID: 'Class/instance_id'."""
        class_id = doc.get("class_id", "")
        instance_id = doc.get("instance_id", "")
        return f"{class_id}/{instance_id}"

    @staticmethod
    def _make_node(doc: Dict[str, Any]) -> Dict[str, Any]:
        """Build a graph node dict from an ES document."""
        class_id = doc.get("class_id", "")
        instance_id = doc.get("instance_id", "")
        node_id = f"{class_id}/{instance_id}"
        return {
            "id": node_id,
            "type": class_id,
            "instance_id": instance_id,
            "data": doc,
            "data_status": "FULL",
        }

    @staticmethod
    def _class_of(docs: List[Dict[str, Any]]) -> str:
        """Return the class_id of the first doc (all should be same class)."""
        if docs:
            return str(docs[0].get("class_id", ""))
        return ""

    @staticmethod
    def _get_relationship_refs(doc: Dict[str, Any], predicate: str) -> List[str]:
        """Extract relationship references for a given predicate from a doc."""
        rels = doc.get("relationships") or {}
        refs = rels.get(predicate)
        if refs is None:
            return []
        if isinstance(refs, str):
            return [refs]
        if isinstance(refs, list):
            return [str(r) for r in refs]
        return []

    @staticmethod
    def _parse_ref(ref: str) -> Tuple[str, str]:
        """Parse 'TargetClass/instance_id' → (target_class, instance_id)."""
        if "/" in ref:
            parts = ref.split("/", 1)
            return parts[0], parts[1]
        return "", ref

    @staticmethod
    def _empty_result() -> Dict[str, Any]:
        return {
            "nodes": [],
            "edges": [],
            "paths": [],
            "documents": {},
            "count": 0,
            "warnings": [],
        }
