from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from bff.routers.context7 import get_context7_client
from shared.models.responses import ApiResponse
from shared.security.input_sanitizer import sanitize_input
from shared.services.agent_policy_registry import AgentPolicyRegistry

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/document-bundles", tags=["Document Bundles"])


async def get_agent_policy_registry() -> AgentPolicyRegistry:
    from bff.main import get_agent_policy_registry as _get_agent_policy_registry

    return await _get_agent_policy_registry()


def _resolve_verified_principal(request: Request) -> tuple[str, str]:
    user = getattr(request.state, "user", None)
    if user is None or not getattr(user, "verified", False):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User JWT required")
    tenant_id = getattr(user, "tenant_id", None) or getattr(user, "org_id", None) or "default"
    user_id = str(getattr(user, "id", "") or "").strip() or "unknown"
    return str(tenant_id), user_id


def _enforce_bundle_access(*, tenant_policy: Any, bundle_id: str) -> None:
    data_policies = getattr(tenant_policy, "data_policies", None) if tenant_policy is not None else None
    if not isinstance(data_policies, dict):
        return
    allowed = data_policies.get("allowed_document_bundle_ids") or []
    allowed_ids = {str(v).strip() for v in (allowed or []) if str(v).strip()}
    if allowed_ids and str(bundle_id).strip() not in allowed_ids:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permission denied")


class DocumentBundleSearchRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=4000)
    limit: int = Field(default=10, ge=1, le=50)
    filters: Optional[Dict[str, Any]] = None


@router.post("/{bundle_id}/search", response_model=ApiResponse)
async def search_document_bundle(
    bundle_id: str,
    body: DocumentBundleSearchRequest,
    request: Request,
    policy_registry: AgentPolicyRegistry = Depends(get_agent_policy_registry),
    client: Any = Depends(get_context7_client),
) -> ApiResponse:
    tenant_id, _user_id = _resolve_verified_principal(request)
    policy = await policy_registry.get_tenant_policy(tenant_id=tenant_id)
    _enforce_bundle_access(tenant_policy=policy, bundle_id=bundle_id)

    payload = sanitize_input(body.model_dump(exclude_none=True))
    query = str(payload.get("query") or "").strip()
    if not query:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="query is required")

    try:
        limit = int(payload.get("limit") or 10)
    except Exception:
        limit = 10
    limit = max(1, min(50, limit))

    filters = payload.get("filters")
    filters_payload: Dict[str, Any] = dict(filters) if isinstance(filters, dict) else {}
    filters_payload.setdefault("bundle_id", str(bundle_id))

    try:
        raw_results = await client.search(query=query, limit=limit, filters=filters_payload)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Document bundle search failed")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

    results: list[dict[str, Any]] = []
    for idx, item in enumerate(raw_results or []):
        if isinstance(item, dict):
            obj = dict(item)
        else:
            obj = {"value": item}

        doc_id = str(
            obj.get("id")
            or obj.get("entity_id")
            or obj.get("doc_id")
            or obj.get("document_id")
            or f"result_{idx}"
        ).strip()
        title = str(obj.get("title") or obj.get("name") or "").strip() or None
        score = obj.get("score") or obj.get("relevance") or obj.get("similarity")
        try:
            score = float(score) if score is not None else None
        except Exception:
            score = None

        snippet = obj.get("snippet")
        if snippet is None:
            snippet = obj.get("content") or obj.get("text") or ""
        if not isinstance(snippet, str):
            snippet = str(snippet)
        snippet = snippet.strip()
        if len(snippet) > 1200:
            snippet = snippet[:1200] + "…"

        citation_id = f"context7:{bundle_id}:{doc_id}"
        results.append(
            {
                "citation_id": citation_id,
                "bundle_id": str(bundle_id),
                "doc_id": doc_id,
                "title": title,
                "score": score,
                "snippet": snippet,
                "metadata": {k: v for k, v in obj.items() if k not in {"content", "text", "snippet"}},
            }
        )

    citations = [
        {
            "citation_id": r["citation_id"],
            "ref": {
                "provider": "context7",
                "bundle_id": r["bundle_id"],
                "doc_id": r["doc_id"],
                "title": r.get("title"),
            },
        }
        for r in results
    ]

    return ApiResponse.success(
        message="Document bundle search complete",
        data={
            "bundle_id": str(bundle_id),
            "query": query,
            "count": len(results),
            "results": results,
            "citations": citations,
        },
    )
