"""
Graph query domain logic (BFF).

Extracted from `bff.routers.graph` to keep routers thin and to deduplicate
branch/overlay virtualization + access policy application across endpoints.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from elasticsearch import NotFoundError
from fastapi import HTTPException, Request, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.models.lineage_edge_types import (
    EDGE_EVENT_MATERIALIZED_ES_DOCUMENT,
    EDGE_EVENT_WROTE_GRAPH_DOCUMENT,
)
from shared.models.graph_query import (
    GraphEdge,
    GraphNode,
    GraphQueryRequest,
    GraphQueryResponse,
    SimpleGraphQueryRequest,
)
from shared.security.input_sanitizer import validate_branch_name, validate_db_name
from shared.services.core.graph_federation_service_es import GraphFederationServiceES
from shared.services.core.writeback_merge_service import WritebackMergeService
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.registries.lineage_store import LineageStore
from shared.services.storage.lakefs_storage_service import create_lakefs_storage_service
from shared.services.storage.storage_service import create_storage_service
from shared.services.core.runtime_status import availability_surface, build_runtime_issue, normalize_runtime_status
from shared.observability.tracing import trace_external_call
from shared.utils.access_policy import apply_access_policy

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GraphBranchContext:
    graph_branch: str
    read_model_base_branch: str
    read_model_overlay_branch: Optional[str]
    overlay_active: bool
    overlay_required: bool
    overlay_status: str
    writeback_enabled: bool
    branch_virtualization_active: bool


def _resolve_graph_branches(
    *,
    db_name: str,
    base_branch: str,
    overlay_branch: Optional[str],
    branch: Optional[str],
    include_documents: bool,
    classes_in_query: List[str],
) -> GraphBranchContext:
    resolved_graph_branch = validate_branch_name(branch or base_branch or "main")

    writeback_enabled = bool(
        AppConfig.WRITEBACK_READ_OVERLAY
        and any(AppConfig.is_writeback_enabled_object_type(c) for c in classes_in_query if c)
    )
    requested_overlay = str(overlay_branch).strip() if overlay_branch else None
    resolved_overlay_branch = None
    if requested_overlay:
        resolved_overlay_branch = validate_branch_name(requested_overlay)
    elif writeback_enabled:
        resolved_overlay_branch = AppConfig.get_ontology_writeback_branch(db_name)

    virtualization_base_branch = "main"
    try:
        virtualization_base_branch = validate_branch_name(get_settings().branch_virtualization.base_branch)
    except Exception:
        logging.getLogger(__name__).warning("Exception fallback at bff/services/graph_query_service.py:76", exc_info=True)
        virtualization_base_branch = "main"

    read_model_base_branch = resolved_graph_branch
    read_model_overlay_branch = resolved_overlay_branch
    branch_virtualization_active = False
    if include_documents and not requested_overlay:
        if resolved_graph_branch != virtualization_base_branch or read_model_overlay_branch is None:
            branch_virtualization_active = True
            read_model_base_branch = virtualization_base_branch
            read_model_overlay_branch = resolved_graph_branch
    else:
        # Graph-only queries should keep the requested branch as read-model base.
        # Virtualized base indices can legitimately be absent for non-document queries.
        read_model_base_branch = resolved_graph_branch
        read_model_overlay_branch = resolved_overlay_branch

    overlay_active = bool(read_model_overlay_branch)
    overlay_required = bool(include_documents and read_model_overlay_branch)
    overlay_status = "ACTIVE" if overlay_active else "DISABLED"
    return GraphBranchContext(
        graph_branch=resolved_graph_branch,
        read_model_base_branch=read_model_base_branch,
        read_model_overlay_branch=read_model_overlay_branch,
        overlay_active=overlay_active,
        overlay_required=overlay_required,
        overlay_status=overlay_status,
        writeback_enabled=writeback_enabled,
        branch_virtualization_active=branch_virtualization_active,
    )


def _raise_overlay_degraded(*, ctx: GraphBranchContext) -> None:
    raise classified_http_exception(
        status.HTTP_503_SERVICE_UNAVAILABLE,
        "Overlay index unavailable; cannot serve authoritative view.",
        code=ErrorCode.UPSTREAM_UNAVAILABLE,
        extra={
            "error": "overlay_degraded",
            "base_branch": ctx.graph_branch,
            "overlay_branch": ctx.read_model_overlay_branch,
            "overlay_status": "DEGRADED",
            "writeback_enabled": ctx.writeback_enabled,
            "writeback_edits_present": None,
            "branch_virtualization_active": ctx.branch_virtualization_active,
            "read_model_base_branch": ctx.read_model_base_branch,
        },
    )


async def _merge_fallback_for_degraded(
    *,
    db_name: str,
    ctx: GraphBranchContext,
    documents: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Server-side merge fallback when ES overlay index is unavailable (DEGRADED).

    Instead of returning a 503 error, we take the base-only query results and
    apply pending writeback edits via WritebackMergeService.  This ensures
    users still see a best-effort merged view even when the overlay ES index is down.

    Returns (merged_documents, writeback_edits_present).
    """
    settings = get_settings()
    base_storage = create_storage_service(settings)
    lakefs_storage = create_lakefs_storage_service(settings)
    if not base_storage or not lakefs_storage:
        logger.warning("Cannot perform merge fallback: storage services unavailable")
        return documents, False

    merger = WritebackMergeService(base_storage=base_storage, lakefs_storage=lakefs_storage)
    writeback_repo = AppConfig.ONTOLOGY_WRITEBACK_REPO
    writeback_branch = ctx.read_model_overlay_branch or AppConfig.get_ontology_writeback_branch(db_name)
    base_branch = ctx.read_model_base_branch or "main"

    merged_docs: List[Dict[str, Any]] = []
    edits_found = False

    for doc in documents:
        if not isinstance(doc, dict):
            merged_docs.append(doc)
            continue

        class_id = str(doc.get("class_id") or doc.get("type") or "").strip()
        instance_id = str(doc.get("instance_id") or doc.get("id") or "").strip()
        if not class_id or not instance_id:
            merged_docs.append(doc)
            continue

        try:
            merged = await merger.merge_instance(
                db_name=db_name,
                base_branch=base_branch,
                overlay_branch=writeback_branch,
                class_id=class_id,
                instance_id=instance_id,
                writeback_repo=writeback_repo,
                writeback_branch=writeback_branch,
            )
            if merged.overlay_tombstone:
                # Instance was deleted via writeback — exclude from results
                continue
            if merged.writeback_edits_present:
                edits_found = True
                merged_doc = dict(doc)
                merged_data = merged.document.get("data")
                if isinstance(merged_data, dict):
                    merged_doc["data"] = merged_data
                merged_docs.append(merged_doc)
            else:
                merged_docs.append(doc)
        except FileNotFoundError:
            merged_docs.append(doc)
        except Exception as exc:
            logger.warning(
                "Merge fallback failed for %s/%s: %s; returning base doc",
                class_id,
                instance_id,
                exc,
            )
            merged_docs.append(doc)

    return merged_docs, edits_found


async def _load_access_policies(
    dataset_registry: DatasetRegistry,
    *,
    db_name: str,
    class_ids: List[str],
) -> Dict[str, Dict[str, Any]]:
    policies: Dict[str, Dict[str, Any]] = {}
    for class_id in sorted({c for c in class_ids if c}):
        record = await dataset_registry.get_access_policy(
            db_name=db_name,
            scope="data_access",
            subject_type="object_type",
            subject_id=class_id,
        )
        if record:
            policies[class_id] = record.policy
    return policies


def _apply_access_policies_to_nodes(
    nodes: List[Dict[str, Any]],
    *,
    policies: Dict[str, Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], set[str]]:
    filtered_nodes: List[Dict[str, Any]] = []
    allowed_ids: set[str] = set()
    for node in nodes:
        class_id = str(node.get("type") or "")
        policy = policies.get(class_id)
        if not policy:
            filtered_nodes.append(node)
            allowed_ids.add(str(node.get("id")))
            continue
        data = node.get("data")
        if data is None:
            filters = policy.get("row_filters") or policy.get("filters") or []
            if filters:
                continue
            filtered_nodes.append(node)
            allowed_ids.add(str(node.get("id")))
            continue
        filtered_rows, _ = apply_access_policy([data], policy=policy)
        if not filtered_rows:
            continue
        node["data"] = filtered_rows[0]
        filtered_nodes.append(node)
        allowed_ids.add(str(node.get("id")))
    return filtered_nodes, allowed_ids


def _apply_access_policies_to_documents(
    documents: List[Dict[str, Any]],
    *,
    policy: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not policy:
        return documents
    output: List[Dict[str, Any]] = []
    for doc in documents:
        if not isinstance(doc, dict):
            continue
        data = doc.get("data")
        if data is None:
            output.append(doc)
            continue
        filtered_rows, _ = apply_access_policy([data], policy=policy)
        if not filtered_rows:
            continue
        next_doc = dict(doc)
        next_doc["data"] = filtered_rows[0]
        output.append(next_doc)
    return output


def _collect_class_ids_from_hops(hops: Any) -> List[str]:
    class_ids: List[str] = []
    if not isinstance(hops, list):
        return class_ids
    for hop in hops:
        if isinstance(hop, (tuple, list)) and len(hop) >= 2:
            class_ids.append(str(hop[1] or "").strip())
            continue
        if isinstance(hop, dict):
            class_ids.append(str(hop.get("target_class") or hop.get("targetClass") or "").strip())
    return [c for c in class_ids if c]


def _normalize_paths_for_response(paths: Any) -> List[Dict[str, Any]]:
    """
    Normalize graph path payloads to a stable object shape.

    GraphFederationService may emit paths as `List[List[str]]` while API response
    schema expects `List[Dict[str, Any]]`.
    """
    if not isinstance(paths, list):
        return []

    normalized: List[Dict[str, Any]] = []
    for idx, path in enumerate(paths):
        if isinstance(path, dict):
            normalized.append(path)
            continue

        node_ids: List[str] = []
        if isinstance(path, (list, tuple)):
            for node_id in path:
                text = str(node_id or "").strip()
                if text:
                    node_ids.append(text)
        else:
            text = str(path or "").strip()
            if text:
                node_ids.append(text)

        normalized.append(
            {
                "path_id": idx,
                "nodes": node_ids,
                "hops": max(0, len(node_ids) - 1),
            }
        )
    return normalized


def _normalize_es_doc_id(raw_id: Any) -> str:
    """
    Normalize graph document identifiers to instance ID form for API compatibility.

    Internal graph IDs are often stored as `Class/instance_id`; response field
    `es_doc_id` is expected by clients/tests as the instance identifier.
    """
    text = str(raw_id or "").strip()
    if not text:
        return ""
    if "/" in text:
        _, instance_id = text.split("/", 1)
        return instance_id
    return text


@trace_external_call("bff.graph_query.execute_graph_query")
async def execute_graph_query(
    *,
    db_name: str,
    query: GraphQueryRequest,
    request: Request,
    lineage_store: LineageStore,
    graph_service: GraphFederationServiceES,
    dataset_registry: DatasetRegistry,
    base_branch: str = "main",
    overlay_branch: Optional[str] = None,
    branch: Optional[str] = None,
) -> GraphQueryResponse:
    """
    Execute multi-hop graph query with ES federation (graph + Elasticsearch).
    """
    try:
        db_name = validate_db_name(db_name)

        hops_tuples = [
            (hop.predicate, hop.target_class, bool(getattr(hop, "reverse", False))) for hop in (query.hops or [])
        ]
        classes_in_query = [str(query.start_class or "").strip()] + [
            str(hop.target_class or "").strip() for hop in (query.hops or [])
        ]
        ctx = _resolve_graph_branches(
            db_name=db_name,
            base_branch=base_branch,
            overlay_branch=overlay_branch,
            branch=branch,
            include_documents=bool(query.include_documents),
            classes_in_query=classes_in_query,
        )

        logger.info("📊 Graph query on %s: %s -> %s", db_name, query.start_class, hops_tuples)

        degraded_fallback = False
        try:
            result = await graph_service.multi_hop_query(
                db_name=db_name,
                base_branch=ctx.read_model_base_branch,
                overlay_branch=ctx.read_model_overlay_branch,
                graph_branch=ctx.graph_branch,
                strict_overlay=bool(ctx.overlay_required),
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
        except Exception as exc:
            if not ctx.overlay_required:
                raise exc
            # Retry without strict overlay — base-only query + server-side merge fallback
            logger.warning("Overlay query failed; retrying with base-only + merge fallback: %s", exc)
            try:
                result = await graph_service.multi_hop_query(
                    db_name=db_name,
                    base_branch=ctx.read_model_base_branch,
                    overlay_branch=None,
                    graph_branch=ctx.graph_branch,
                    strict_overlay=False,
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
                degraded_fallback = True
            except Exception as fallback_exc:
                if ctx.branch_virtualization_active and isinstance(fallback_exc, NotFoundError):
                    logger.warning(
                        "Virtualized base branch index missing (base=%s graph=%s); retrying requested branch",
                        ctx.read_model_base_branch,
                        ctx.graph_branch,
                    )
                    result = await graph_service.multi_hop_query(
                        db_name=db_name,
                        base_branch=ctx.graph_branch,
                        overlay_branch=None,
                        graph_branch=ctx.graph_branch,
                        strict_overlay=False,
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
                    degraded_fallback = True
                else:
                    logging.getLogger(__name__).warning("Exception fallback at bff/services/graph_query_service.py:367", exc_info=True)
                    _raise_overlay_degraded(ctx=ctx)

        raw_nodes = list(result.get("nodes", []) or [])
        policies = await _load_access_policies(
            dataset_registry,
            db_name=db_name,
            class_ids=[str(node.get("type") or "") for node in raw_nodes],
        )
        filtered_nodes, allowed_ids = _apply_access_policies_to_nodes(raw_nodes, policies=policies)

        # Apply server-side merge fallback for DEGRADED overlay
        effective_overlay_status = ctx.overlay_status
        if degraded_fallback and query.include_documents:
            try:
                merged_nodes, edits_found = await _merge_fallback_for_degraded(
                    db_name=db_name, ctx=ctx, documents=filtered_nodes,
                )
                filtered_nodes = merged_nodes
                effective_overlay_status = "DEGRADED"
                # Rebuild allowed_ids after merge (tombstoned nodes removed)
                allowed_ids = {str(n.get("id")) for n in filtered_nodes if isinstance(n, dict)}
                logger.info("DEGRADED merge fallback applied: %d nodes, edits_present=%s", len(filtered_nodes), edits_found)
            except Exception as merge_exc:
                logger.warning("DEGRADED merge fallback failed; returning base-only: %s", merge_exc)
                effective_overlay_status = "DEGRADED"

        raw_edges = list(result.get("edges", []) or [])
        if allowed_ids:
            raw_edges = [
                edge
                for edge in raw_edges
                if str(edge.get("from") or "") in allowed_ids and str(edge.get("to") or "") in allowed_ids
            ]
        else:
            raw_edges = []

        graph_latest: Dict[str, Dict[str, Any]] = {}
        es_latest: Dict[str, Dict[str, Any]] = {}
        graph_artifact_candidates_by_node_id: Dict[str, List[str]] = {}
        es_artifact_by_node_id: Dict[str, str] = {}

        def _graph_artifact_candidates(graph_node_id: str) -> List[str]:
            return [LineageStore.node_artifact("graph", db_name, ctx.graph_branch, graph_node_id)]

        if query.include_provenance and raw_nodes:
            try:
                graph_artifacts: List[str] = []
                es_artifacts: List[str] = []
                for node in raw_nodes:
                    graph_id = str(node.get("graph_id") or node.get("id") or "")
                    if graph_id:
                        artifact_candidates = _graph_artifact_candidates(graph_id)
                        graph_artifact_candidates_by_node_id[graph_id] = artifact_candidates
                        graph_artifacts.extend(artifact_candidates)

                    es_ref = node.get("es_ref") or {}
                    index_name = str(es_ref.get("index") or "")
                    doc_id = str(es_ref.get("id") or "")
                    if index_name and doc_id:
                        art = LineageStore.node_artifact("es", index_name, doc_id)
                        es_artifact_by_node_id[f"{index_name}/{doc_id}"] = art
                        es_artifacts.append(art)

                latest_by_type = await lineage_store.get_latest_edges_to(
                    to_node_ids=list({*graph_artifacts}),
                    edge_type=EDGE_EVENT_WROTE_GRAPH_DOCUMENT,
                    db_name=db_name,
                    branch=ctx.graph_branch,
                )
                for to_node_id, edge_payload in (latest_by_type or {}).items():
                    graph_latest.setdefault(str(to_node_id), edge_payload)

                es_latest = await lineage_store.get_latest_edges_to(
                    to_node_ids=list({*es_artifacts}),
                    edge_type=EDGE_EVENT_MATERIALIZED_ES_DOCUMENT,
                    db_name=db_name,
                    branch=ctx.graph_branch,
                )
            except Exception as exc:
                logger.debug("Provenance lookup failed (non-fatal): %s", exc)

        nodes: List[GraphNode] = []
        for node in filtered_nodes:
            graph_id = str(node.get("graph_id") or node.get("id") or "")
            node_id = str(node.get("id") or "")
            es_doc_id = (
                str(node.get("es_doc_id") or "").strip()
                or str(node.get("instance_id") or "").strip()
                or _normalize_es_doc_id(graph_id)
                or _normalize_es_doc_id(node_id)
            )
            es_ref = node.get("es_ref") or {
                "index": f"{db_name}_instances",
                "id": node_id or graph_id or str(es_doc_id),
            }
            node_index_status = dict(node.get("index_status") or {})

            provenance: Optional[Dict[str, Any]] = None
            if query.include_provenance and graph_id:
                graph_artifacts = graph_artifact_candidates_by_node_id.get(graph_id) or _graph_artifact_candidates(graph_id)
                es_key = f"{es_ref.get('index')}/{es_ref.get('id')}"
                es_art = es_artifact_by_node_id.get(es_key) or (
                    LineageStore.node_artifact("es", str(es_ref.get("index")), str(es_ref.get("id")))
                    if es_ref.get("index") and es_ref.get("id")
                    else None
                )

                graph_prov = next((graph_latest.get(artifact_id) for artifact_id in graph_artifacts if graph_latest.get(artifact_id)), None)
                es_prov = es_latest.get(es_art) if es_art else None
                provenance = {"graph": graph_prov, "es": es_prov}

                try:
                    t_at = (
                        datetime.fromisoformat(str(graph_prov.get("occurred_at")))
                        if graph_prov and graph_prov.get("occurred_at")
                        else None
                    )
                    e_at = (
                        datetime.fromisoformat(str(es_prov.get("occurred_at"))) if es_prov and es_prov.get("occurred_at") else None
                    )
                    if t_at and t_at.tzinfo is None:
                        t_at = t_at.replace(tzinfo=timezone.utc)
                    if e_at and e_at.tzinfo is None:
                        e_at = e_at.replace(tzinfo=timezone.utc)
                    if t_at and e_at:
                        node_index_status["graph_last_updated_at"] = t_at.isoformat()
                        node_index_status["projection_last_indexed_at"] = e_at.isoformat()
                        node_index_status["projection_lag_seconds"] = max(0.0, (t_at - e_at).total_seconds())
                except (TypeError, ValueError):
                    logging.getLogger(__name__).warning(
                        "Failed to compute projection lag from provenance timestamps; skipping lag enrichment",
                        exc_info=True,
                    )

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
        for edge in raw_edges:
            edge_prov: Optional[Dict[str, Any]] = None
            if query.include_provenance:
                from_node = str(edge.get("from") or "")
                if from_node:
                    graph_artifacts = graph_artifact_candidates_by_node_id.get(from_node) or _graph_artifact_candidates(from_node)
                    edge_prov = {
                        "graph": next(
                            (graph_latest.get(artifact_id) for artifact_id in graph_artifacts if graph_latest.get(artifact_id)),
                            None,
                        )
                    }
            edges.append(
                GraphEdge(
                    from_node=edge["from"],
                    to_node=edge["to"],
                    predicate=edge["predicate"],
                    provenance=edge_prov,
                )
            )

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

        response_warnings = list(result.get("warnings") or [])
        if degraded_fallback:
            response_warnings.append("overlay_degraded: ES overlay unavailable; results merged server-side from lakeFS writeback queue")

        response = GraphQueryResponse(
            base_branch=ctx.graph_branch,
            overlay_branch=ctx.read_model_overlay_branch,
            overlay_status=effective_overlay_status,
            writeback_enabled=ctx.writeback_enabled,
            nodes=nodes,
            edges=edges,
            paths=_normalize_paths_for_response(result.get("paths")) if query.include_paths else None,
            index_summary=index_summary,
            query={
                "start_class": query.start_class,
                "hops": [{"predicate": h[0], "target_class": h[1], "reverse": bool(h[2])} for h in hops_tuples],
                "filters": query.filters,
                "base_branch": ctx.graph_branch,
                "overlay_branch": ctx.read_model_overlay_branch,
                "graph_branch": ctx.graph_branch,
                "read_model_base_branch": ctx.read_model_base_branch,
                "read_model_overlay_branch": ctx.read_model_overlay_branch,
                "branch_virtualization_active": ctx.branch_virtualization_active,
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
            warnings=response_warnings,
            page=result.get("page"),
        )

        logger.info("✅ Graph query complete: %s nodes, %s edges", len(nodes), len(edges))
        return response

    except ValueError as exc:
        logger.error("Invalid query parameters: %s", exc)
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, f"Invalid query: {str(exc)}", code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc
    except HTTPException:
        raise
    except NotFoundError as exc:
        # Elasticsearch index missing typically means the DB/branch has not been materialized yet.
        # Treat this as a user-visible 404 instead of a 500.
        raise classified_http_exception(
            status.HTTP_404_NOT_FOUND,
            "Graph index not found",
            code=ErrorCode.ES_INDEX_NOT_FOUND,
            extra={"db_name": db_name},
        ) from exc
    except Exception as exc:
        logger.error("Graph query failed: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Graph query failed: {str(exc)}", code=ErrorCode.INTERNAL_ERROR) from exc


@trace_external_call("bff.graph_query.execute_simple_graph_query")
async def execute_simple_graph_query(
    *,
    db_name: str,
    query: SimpleGraphQueryRequest,
    request: Request,
    graph_service: GraphFederationServiceES,
    dataset_registry: DatasetRegistry,
    base_branch: str = "main",
    overlay_branch: Optional[str] = None,
    branch: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Execute simple single-class graph query.
    """
    try:
        db_name = validate_db_name(db_name)

        ctx = _resolve_graph_branches(
            db_name=db_name,
            base_branch=base_branch,
            overlay_branch=overlay_branch,
            branch=branch,
            include_documents=True,
            classes_in_query=[str(query.class_name or "").strip()],
        )

        logger.info("📊 Simple graph query on %s: %s", db_name, query.class_name)

        degraded_fallback = False
        try:
            result = await graph_service.simple_graph_query(
                db_name=db_name,
                base_branch=ctx.read_model_base_branch,
                overlay_branch=ctx.read_model_overlay_branch,
                graph_branch=ctx.graph_branch,
                strict_overlay=bool(ctx.overlay_required),
                class_name=query.class_name,
                filters=query.filters,
            )
        except Exception as exc:
            if not ctx.overlay_required:
                raise exc
            # Retry without strict overlay — base-only query + server-side merge fallback
            logger.warning("Simple query overlay failed; retrying with base-only + merge fallback: %s", exc)
            try:
                result = await graph_service.simple_graph_query(
                    db_name=db_name,
                    base_branch=ctx.read_model_base_branch,
                    overlay_branch=None,
                    graph_branch=ctx.graph_branch,
                    strict_overlay=False,
                    class_name=query.class_name,
                    filters=query.filters,
                )
                degraded_fallback = True
            except Exception as fallback_exc:
                if ctx.branch_virtualization_active and isinstance(fallback_exc, NotFoundError):
                    logger.warning(
                        "Virtualized base branch index missing (base=%s graph=%s); retrying requested branch",
                        ctx.read_model_base_branch,
                        ctx.graph_branch,
                    )
                    result = await graph_service.simple_graph_query(
                        db_name=db_name,
                        base_branch=ctx.graph_branch,
                        overlay_branch=None,
                        graph_branch=ctx.graph_branch,
                        strict_overlay=False,
                        class_name=query.class_name,
                        filters=query.filters,
                    )
                    degraded_fallback = True
                else:
                    logging.getLogger(__name__).warning("Exception fallback at bff/services/graph_query_service.py:645", exc_info=True)
                    _raise_overlay_degraded(ctx=ctx)

        policy = await dataset_registry.get_access_policy(
            db_name=db_name,
            scope="data_access",
            subject_type="object_type",
            subject_id=str(query.class_name),
        )
        documents = result.get("documents") or []
        if isinstance(documents, list):
            result["documents"] = _apply_access_policies_to_documents(
                documents,
                policy=policy.policy if policy else None,
            )
            result["count"] = len(result["documents"])

        # Apply server-side merge fallback for DEGRADED overlay
        effective_overlay_status = ctx.overlay_status
        writeback_edits_present: Optional[bool] = None
        if degraded_fallback:
            try:
                merged_docs, edits_found = await _merge_fallback_for_degraded(
                    db_name=db_name, ctx=ctx, documents=result.get("documents") or [],
                )
                result["documents"] = merged_docs
                result["count"] = len(merged_docs)
                writeback_edits_present = edits_found
                effective_overlay_status = "DEGRADED"
                logger.info("DEGRADED merge fallback (simple): %d docs, edits_present=%s", len(merged_docs), edits_found)
            except Exception as merge_exc:
                logger.warning("DEGRADED merge fallback failed (simple); returning base-only: %s", merge_exc)
                effective_overlay_status = "DEGRADED"

        result["base_branch"] = ctx.graph_branch
        result["overlay_branch"] = ctx.read_model_overlay_branch
        result["overlay_status"] = effective_overlay_status
        result["writeback_enabled"] = ctx.writeback_enabled
        result["writeback_edits_present"] = writeback_edits_present
        result["graph_branch"] = ctx.graph_branch
        result["read_model_base_branch"] = ctx.read_model_base_branch
        result["read_model_overlay_branch"] = ctx.read_model_overlay_branch
        result["branch_virtualization_active"] = ctx.branch_virtualization_active
        if degraded_fallback:
            warnings = list(result.get("warnings") or [])
            warnings.append("overlay_degraded: ES overlay unavailable; results merged server-side from lakeFS writeback queue")
            result["warnings"] = warnings

        return result

    except ValueError as exc:
        logger.error("Invalid query parameters: %s", exc)
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, f"Invalid query: {str(exc)}", code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc
    except HTTPException:
        raise
    except NotFoundError as exc:
        raise classified_http_exception(
            status.HTTP_404_NOT_FOUND,
            "Graph index not found",
            code=ErrorCode.ES_INDEX_NOT_FOUND,
            extra={"db_name": db_name},
        ) from exc
    except Exception as exc:
        logger.error("Simple graph query failed: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Query failed: {str(exc)}", code=ErrorCode.INTERNAL_ERROR) from exc


@trace_external_call("bff.graph_query.execute_multi_hop_query")
async def execute_multi_hop_query(
    *,
    db_name: str,
    query: Dict[str, Any],
    request: Request,
    graph_service: GraphFederationServiceES,
    dataset_registry: DatasetRegistry,
    base_branch: str = "main",
    overlay_branch: Optional[str] = None,
    branch: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Execute multi-hop graph query (compat dict payload).
    """
    try:
        db_name = validate_db_name(db_name)

        resolved_graph_branch = validate_branch_name(branch or base_branch or "main")

        start_class = query.get("start_class")
        hops = query.get("hops", [])
        filters = query.get("filters", {})
        include_documents = bool(query.get("include_documents", True))
        include_audit = bool(query.get("include_audit", False))
        limit = query.get("limit", 100)

        if not start_class:
            raise ValueError("start_class is required")

        classes_in_query = [str(start_class or "").strip()] + _collect_class_ids_from_hops(hops)
        ctx = _resolve_graph_branches(
            db_name=db_name,
            base_branch=resolved_graph_branch,
            overlay_branch=overlay_branch,
            branch=None,
            include_documents=include_documents,
            classes_in_query=classes_in_query,
        )

        logger.info("🚀 Multi-hop query: %s -> %s", start_class, hops)

        degraded_fallback = False
        try:
            result = await graph_service.multi_hop_query(
                db_name=db_name,
                base_branch=ctx.read_model_base_branch,
                overlay_branch=ctx.read_model_overlay_branch,
                graph_branch=ctx.graph_branch,
                strict_overlay=bool(ctx.overlay_required),
                start_class=start_class,
                hops=hops or [],
                filters=filters,
                limit=limit,
                include_documents=include_documents,
                include_audit=include_audit,
            )
        except Exception as exc:
            if not (ctx.overlay_required and include_documents):
                raise exc
            # Retry without strict overlay — base-only query + server-side merge fallback
            logger.warning("Multi-hop overlay failed; retrying with base-only + merge fallback: %s", exc)
            try:
                result = await graph_service.multi_hop_query(
                    db_name=db_name,
                    base_branch=ctx.read_model_base_branch,
                    overlay_branch=None,
                    graph_branch=ctx.graph_branch,
                    strict_overlay=False,
                    start_class=start_class,
                    hops=hops or [],
                    filters=filters,
                    limit=limit,
                    include_documents=include_documents,
                    include_audit=include_audit,
                )
                degraded_fallback = True
            except Exception as fallback_exc:
                if ctx.branch_virtualization_active and isinstance(fallback_exc, NotFoundError):
                    logger.warning(
                        "Virtualized base branch index missing (base=%s graph=%s); retrying requested branch",
                        ctx.read_model_base_branch,
                        ctx.graph_branch,
                    )
                    result = await graph_service.multi_hop_query(
                        db_name=db_name,
                        base_branch=ctx.graph_branch,
                        overlay_branch=None,
                        graph_branch=ctx.graph_branch,
                        strict_overlay=False,
                        start_class=start_class,
                        hops=hops or [],
                        filters=filters,
                        limit=limit,
                        include_documents=include_documents,
                        include_audit=include_audit,
                    )
                    degraded_fallback = True
                else:
                    logging.getLogger(__name__).warning("Exception fallback at bff/services/graph_query_service.py:782", exc_info=True)
                    _raise_overlay_degraded(ctx=ctx)

        raw_nodes = list(result.get("nodes", []) or [])
        policies = await _load_access_policies(
            dataset_registry,
            db_name=db_name,
            class_ids=[str(node.get("type") or "") for node in raw_nodes],
        )
        filtered_nodes, allowed_ids = _apply_access_policies_to_nodes(raw_nodes, policies=policies)

        # Apply server-side merge fallback for DEGRADED overlay
        effective_overlay_status = ctx.overlay_status
        writeback_edits_present: Optional[bool] = None
        if degraded_fallback and include_documents:
            try:
                merged_nodes, edits_found = await _merge_fallback_for_degraded(
                    db_name=db_name, ctx=ctx, documents=filtered_nodes,
                )
                filtered_nodes = merged_nodes
                writeback_edits_present = edits_found
                effective_overlay_status = "DEGRADED"
                # Rebuild allowed_ids after merge (tombstoned nodes removed)
                allowed_ids = {str(n.get("id")) for n in filtered_nodes if isinstance(n, dict)}
                logger.info("DEGRADED merge fallback (multi-hop compat): %d nodes, edits_present=%s", len(filtered_nodes), edits_found)
            except Exception as merge_exc:
                logger.warning("DEGRADED merge fallback failed (multi-hop compat); returning base-only: %s", merge_exc)
                effective_overlay_status = "DEGRADED"

        raw_edges = list(result.get("edges", []) or [])
        if allowed_ids:
            raw_edges = [
                edge
                for edge in raw_edges
                if str(edge.get("from") or "") in allowed_ids and str(edge.get("to") or "") in allowed_ids
            ]
        else:
            raw_edges = []

        result["nodes"] = filtered_nodes
        result["edges"] = raw_edges
        result["count"] = len(filtered_nodes)
        result["base_branch"] = ctx.graph_branch
        result["overlay_branch"] = ctx.read_model_overlay_branch
        result["overlay_status"] = effective_overlay_status
        result["writeback_enabled"] = ctx.writeback_enabled
        result["writeback_edits_present"] = writeback_edits_present
        result["graph_branch"] = ctx.graph_branch
        result["read_model_base_branch"] = ctx.read_model_base_branch
        result["read_model_overlay_branch"] = ctx.read_model_overlay_branch
        result["branch_virtualization_active"] = ctx.branch_virtualization_active
        if degraded_fallback:
            warnings = list(result.get("warnings") or [])
            warnings.append("overlay_degraded: ES overlay unavailable; results merged server-side from lakeFS writeback queue")
            result["warnings"] = warnings

        return {"status": "success", "data": result}

    except ValueError as exc:
        logger.error("Invalid multi-hop query: %s", exc)
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, f"Invalid query: {str(exc)}", code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Multi-hop query failed: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Query failed: {str(exc)}", code=ErrorCode.INTERNAL_ERROR) from exc


@trace_external_call("bff.graph_query.find_relationship_paths")
async def find_relationship_paths(
    *,
    db_name: str,
    source_class: str,
    target_class: str,
    max_depth: int,
    graph_service: GraphFederationServiceES,
    branch: str,
) -> Dict[str, Any]:
    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)

        logger.info("🔍 Finding paths from %s to %s in %s", source_class, target_class, db_name)

        paths = await graph_service.find_relationship_paths(
            db_name=db_name,
            branch=branch,
            source_class=source_class,
            target_class=target_class,
            max_depth=max_depth,
        )

        logger.info("✅ Found %s paths via schema discovery", len(paths))
        return {
            "source_class": source_class,
            "target_class": target_class,
            "paths": paths,
            "count": len(paths),
            "max_depth": max_depth,
        }

    except Exception as exc:
        logger.error("Path finding failed: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Path finding failed: {str(exc)}", code=ErrorCode.INTERNAL_ERROR) from exc


@trace_external_call("bff.graph_query.graph_service_health")
async def graph_service_health(*, graph_service: GraphFederationServiceES) -> Dict[str, Any]:
    """
    Check health of graph federation service (ES-only, no external graph DB dependency).

    Returns a best-effort diagnostic payload (does not raise on failure).
    """
    try:
        await graph_service._ensure_connected()
        health = await graph_service._es.get_cluster_health()
        es_status = health.get("status", "unknown")
        es_healthy = es_status in ("green", "yellow")
        runtime_status = normalize_runtime_status(
            {}
            if es_healthy
            else {
                "degraded": True,
                "issues": [
                    build_runtime_issue(
                        component="graph_federation",
                        dependency="elasticsearch",
                        message=f"Elasticsearch cluster degraded: {es_status}",
                        state="degraded",
                        classification="unavailable",
                        affected_features=("graph.query", "graph.paths", "graph.projections"),
                        affects_readiness=False,
                    )
                ],
            }
        )
        surface = availability_surface(
            service="graph_federation",
            container_ready=True,
            runtime_status=runtime_status,
            dependency_status_overrides={"elasticsearch": "ready" if es_healthy else "degraded"},
            status_reason_override=(
                "Graph federation service operational"
                if es_healthy
                else f"Elasticsearch cluster degraded: {es_status}"
            ),
            message=(
                "Graph federation service operational"
                if es_healthy
                else "Elasticsearch cluster degraded"
            ),
        )
        surface["services"] = {"elasticsearch": es_status}
        return surface
    except Exception as exc:
        logger.error("Health check failed: %s", exc)
        surface = availability_surface(
            service="graph_federation",
            container_ready=True,
            runtime_status=normalize_runtime_status(
                {
                    "issues": [
                        build_runtime_issue(
                            component="graph_federation",
                            dependency="elasticsearch",
                            message=str(exc),
                            state="hard_down",
                            classification="unavailable",
                            affected_features=("graph.query", "graph.paths", "graph.projections"),
                            affects_readiness=True,
                        )
                    ]
                }
            ),
            dependency_status_overrides={"elasticsearch": "hard_down"},
            status_reason_override=f"Elasticsearch unavailable: {exc}",
            message="Elasticsearch unavailable",
        )
        surface["services"] = {"elasticsearch": "unavailable"}
        surface["error"] = str(exc)
        return surface
