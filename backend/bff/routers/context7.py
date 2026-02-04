"""Context7 integration endpoints (BFF).

Thin router delegating to `bff.services.context7_service` (Facade pattern).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, TYPE_CHECKING

from fastapi import APIRouter, Depends, HTTPException

from bff.dependencies import OMSClientDep
from bff.schemas.context7_requests import (
    EntityLinkRequest,
    KnowledgeRequest,
    OntologyAnalysisRequest,
    SearchRequest,
)
from bff.services import context7_service
from bff.services.oms_client import OMSClient

if TYPE_CHECKING:  # pragma: no cover
    from mcp_servers.mcp_client import Context7Client as Context7Client  # noqa: F401
else:
    Context7Client = Any  # type: ignore[misc,assignment]

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/context7",
    tags=["context7"],
    responses={404: {"description": "Not found"}, 500: {"description": "Internal server error"}},
)


def _context7_unavailable_exc() -> HTTPException:
    return HTTPException(
        status_code=503,
        detail={
            "error": "context7_unavailable",
            "message": "Context7 MCP client is unavailable in this environment.",
            "hint": "Install the MCP client dependencies and ensure the Context7 MCP server is configured.",
        },
    )


async def get_context7_client() -> Any:
    try:
        from mcp_servers.mcp_client import get_context7_client as _get_context7_client
    except Exception:
        raise _context7_unavailable_exc()
    try:
        return await _get_context7_client()
    except HTTPException:
        raise
    except Exception:
        raise _context7_unavailable_exc()


@router.post("/search")
async def search_context7(request: SearchRequest, client: Context7Client = Depends(get_context7_client)) -> Dict[str, Any]:
    return await context7_service.search_context7(request=request, client=client)


@router.get("/context/{entity_id}")
async def get_entity_context(entity_id: str, client: Context7Client = Depends(get_context7_client)) -> Dict[str, Any]:
    return await context7_service.get_entity_context(entity_id=entity_id, client=client)


@router.post("/knowledge")
async def add_knowledge(request: KnowledgeRequest, client: Context7Client = Depends(get_context7_client)) -> Dict[str, Any]:
    return await context7_service.add_knowledge(request=request, client=client)


@router.post("/link")
async def create_entity_link(request: EntityLinkRequest, client: Context7Client = Depends(get_context7_client)) -> Dict[str, Any]:
    return await context7_service.create_entity_link(request=request, client=client)


@router.post("/analyze/ontology")
async def analyze_ontology(
    request: OntologyAnalysisRequest,
    client: Context7Client = Depends(get_context7_client),
    oms_client: OMSClient = OMSClientDep,
) -> Dict[str, Any]:
    return await context7_service.analyze_ontology(request=request, client=client, oms_client=oms_client)


@router.get("/suggestions/{db_name}/{class_id}")
async def get_ontology_suggestions(
    db_name: str,
    class_id: str,
    client: Context7Client = Depends(get_context7_client),
) -> Dict[str, Any]:
    return await context7_service.get_ontology_suggestions(db_name=db_name, class_id=class_id, client=client)


@router.get("/health")
async def check_context7_health(client: Context7Client = Depends(get_context7_client)) -> Dict[str, Any]:
    return await context7_service.check_context7_health(client=client)
