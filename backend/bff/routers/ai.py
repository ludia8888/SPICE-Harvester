"""
AI API (BFF).

Thin router that delegates domain logic to `bff.services.ai_service`.
"""

from fastapi import APIRouter, Depends, Query, Request
from shared.observability.tracing import trace_endpoint

from bff.dependencies import (
    FoundryQueryService,
    LabelMapper,
    get_foundry_query_service,
    get_label_mapper,
    get_oms_client,
)
from bff.routers.registry_deps import get_agent_session_registry, get_dataset_registry
from bff.services import ai_service
from bff.services.oms_client import OMSClient
from shared.dependencies.providers import AuditLogStoreDep, LLMGatewayDep, LineageStoreDep, RedisServiceDep
from shared.middleware.rate_limiter import RateLimitPresets, rate_limit
from shared.models.ai import AIIntentRequest, AIIntentResponse, AIQueryRequest, AIQueryResponse
from shared.services.registries.agent_session_registry import AgentSessionRegistry
from shared.services.registries.dataset_registry import DatasetRegistry

router = APIRouter(prefix="/ai", tags=["AI"])


@router.post("/intent", response_model=AIIntentResponse)
@rate_limit(**RateLimitPresets.STRICT)
@trace_endpoint("bff.ai.ai_intent")
async def ai_intent(
    body: AIIntentRequest,
    request: Request,
    *,
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
) -> AIIntentResponse:
    return await ai_service.ai_intent(
        body=body,
        request=request,
        llm=llm,
        redis_service=redis_service,
        audit_store=audit_store,
        sessions=sessions,
        dataset_registry=dataset_registry,
    )


@router.post("/translate/query-plan/{db_name}", deprecated=True)
@rate_limit(**RateLimitPresets.STRICT)
@trace_endpoint("bff.ai.translate_query_plan")
async def translate_query_plan(
    db_name: str,
    body: AIQueryRequest,
    request: Request,
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    oms: OMSClient = Depends(get_oms_client),
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
):
    """Deprecated: use ``POST /ai/query/{db_name}?dry_run=true`` instead."""
    return await ai_service.translate_query_plan(
        db_name=db_name,
        body=body,
        request=request,
        llm=llm,
        redis_service=redis_service,
        audit_store=audit_store,
        oms=oms,
        sessions=sessions,
        dataset_registry=dataset_registry,
    )


@router.post("/query/{db_name}", response_model=AIQueryResponse)
@rate_limit(**RateLimitPresets.STRICT)
@trace_endpoint("bff.ai.ai_query")
async def ai_query(
    db_name: str,
    body: AIQueryRequest,
    request: Request,
    *,
    dry_run: bool = Query(
        default=False,
        description="When true, return only the query plan without executing (same as translate/query-plan).",
    ),
    llm: LLMGatewayDep,
    redis_service: RedisServiceDep,
    audit_store: AuditLogStoreDep,
    lineage_store: LineageStoreDep,
    oms: OMSClient = Depends(get_oms_client),
    mapper: LabelMapper = Depends(get_label_mapper),
    query_service: FoundryQueryService = Depends(get_foundry_query_service),
    sessions: AgentSessionRegistry = Depends(get_agent_session_registry),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
):
    if dry_run:
        return await ai_service.translate_query_plan(
            db_name=db_name,
            body=body,
            request=request,
            llm=llm,
            redis_service=redis_service,
            audit_store=audit_store,
            oms=oms,
            sessions=sessions,
            dataset_registry=dataset_registry,
        )
    return await ai_service.ai_query(
        db_name=db_name,
        body=body,
        request=request,
        llm=llm,
        redis_service=redis_service,
        audit_store=audit_store,
        lineage_store=lineage_store,
        oms=oms,
        mapper=mapper,
        query_service=query_service,
        sessions=sessions,
        dataset_registry=dataset_registry,
    )
