"""Context7 service (BFF).

Extracted from `bff.routers.context7` to keep routers thin and to centralize
error handling (Facade + Template Method).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Mapping, TypeVar

from fastapi import HTTPException
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.services.core.runtime_status import availability_surface, build_runtime_issue, normalize_runtime_status

from bff.schemas.context7_requests import EntityLinkRequest, KnowledgeRequest, OntologyAnalysisRequest, SearchRequest
from bff.services.oms_client import OMSClient
from shared.observability.tracing import trace_external_call

logger = logging.getLogger(__name__)

T = TypeVar("T")


async def _call_context7(*, action: str, func: Callable[[], Awaitable[T]]) -> T:
    try:
        return await func()
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("%s failed: %s", action, exc)
        raise classified_http_exception(500, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc


@trace_external_call("bff.context7.search_context7")
async def search_context7(*, request: SearchRequest, client: Any) -> Dict[str, Any]:
    results = await _call_context7(
        action="Context7 search",
        func=lambda: client.search(query=request.query, limit=request.limit, filters=request.filters),
    )
    return {"query": request.query, "count": len(results), "results": results}


@trace_external_call("bff.context7.get_entity_context")
async def get_entity_context(*, entity_id: str, client: Any) -> Dict[str, Any]:
    context = await _call_context7(action="Context7 get_context", func=lambda: client.get_context(entity_id))
    return {"entity_id": entity_id, "context": context}


@trace_external_call("bff.context7.add_knowledge")
async def add_knowledge(*, request: KnowledgeRequest, client: Any) -> Dict[str, Any]:
    metadata = request.metadata or {}
    if request.tags:
        metadata["tags"] = request.tags

    result = await _call_context7(
        action="Context7 add_knowledge",
        func=lambda: client.add_knowledge(title=request.title, content=request.content, metadata=metadata),
    )
    knowledge_id = result.get("id") if isinstance(result, dict) else None
    return {"success": True, "knowledge_id": knowledge_id, "result": result}


@trace_external_call("bff.context7.create_entity_link")
async def create_entity_link(*, request: EntityLinkRequest, client: Any) -> Dict[str, Any]:
    result = await _call_context7(
        action="Context7 link_entities",
        func=lambda: client.link_entities(
            source_id=request.source_id,
            target_id=request.target_id,
            relationship=request.relationship,
            properties=request.properties,
        ),
    )
    return {
        "success": True,
        "link": {"source": request.source_id, "target": request.target_id, "relationship": request.relationship},
        "result": result,
    }


@trace_external_call("bff.context7.analyze_ontology")
async def analyze_ontology(*, request: OntologyAnalysisRequest, client: Any, oms_client: OMSClient) -> Dict[str, Any]:
    payload = await _call_context7(
        action="OMS get ontology for analysis",
        func=lambda: oms_client.get(
            f"/api/v1/database/{request.db_name}/ontology/{request.ontology_id}",
            params={"branch": request.branch},
        ),
    )
    ontology_data = payload.get("data", payload) if isinstance(payload, dict) else payload

    analysis_request = {
        "db_name": request.db_name,
        "branch": request.branch,
        "ontology_id": request.ontology_id,
        "include_relationships": request.include_relationships,
        "include_suggestions": request.include_suggestions,
        "ontology": ontology_data,
    }

    analysis = await _call_context7(action="Context7 analyze_ontology", func=lambda: client.analyze_ontology(analysis_request))

    return {
        "ontology_id": request.ontology_id,
        "analysis": analysis,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@trace_external_call("bff.context7.get_ontology_suggestions")
async def get_ontology_suggestions(*, db_name: str, class_id: str, client: Any) -> Dict[str, Any]:
    query = f"ontology improvements for {class_id} in {db_name}"
    suggestions = await _call_context7(action="Context7 suggestions search", func=lambda: client.search(query, limit=5))
    return {
        "db_name": db_name,
        "class_id": class_id,
        "suggestions": suggestions,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@trace_external_call("bff.context7.check_context7_health")
async def check_context7_health(
    *,
    client: Any | None = None,
    client_resolver: Callable[[], Awaitable[Any]] | None = None,
    runtime_status: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    try:
        if client is None and client_resolver is not None:
            client = await client_resolver()
        _ = client
        from shared.services.mcp_client import get_mcp_manager

        mcp_manager = get_mcp_manager()
        tools = await mcp_manager.list_tools("context7")
        surface = availability_surface(
            service="context7",
            container_ready=True,
            runtime_status=normalize_runtime_status(runtime_status or {}),
            dependency_status_overrides={"context7": "ready"},
            status_reason_override="Context7 MCP tools available",
            message="Context7 MCP tools available",
        )
        surface["connected"] = True
        surface["available_tools"] = len(tools)
        surface["tools"] = [tool.get("name") for tool in tools if isinstance(tool, dict)]
        return surface
    except Exception as exc:
        logger.error("Context7 health check failed: %s", exc)
        issue = build_runtime_issue(
            component="context7",
            dependency="context7",
            message=str(exc),
            state="degraded",
            classification="unavailable",
            affected_features=[
                "context7.search",
                "context7.context",
                "context7.knowledge",
                "context7.ontology",
            ],
            affects_readiness=False,
        )
        normalized = normalize_runtime_status(
            {
                **dict(runtime_status or {}),
                "degraded": True,
                "issues": [*(list((runtime_status or {}).get("issues") or [])), issue],
            }
        )
        surface = availability_surface(
            service="context7",
            container_ready=True,
            runtime_status=normalized,
            dependency_status_overrides={"context7": "degraded"},
            status_reason_override=f"Context7 unavailable: {exc}",
            message="Context7 unavailable",
        )
        surface["connected"] = False
        surface["available_tools"] = 0
        surface["tools"] = []
        surface["error"] = str(exc)
        return surface
