"""
Objectify relationship reconciliation endpoint.

Automatically detects FK-based relationships between ontology classes and
populates ``relationships`` fields on ES instances.
"""


import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Query

from bff.dependencies import get_elasticsearch_service, get_oms_client
from bff.routers.registry_deps import get_objectify_registry
from bff.services import relationship_reconciler_service
from bff.services.oms_client import OMSClient
from shared.observability.tracing import trace_endpoint
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.storage.elasticsearch_service import ElasticsearchService

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post(
    "/databases/{db_name}/reconcile-relationships",
    summary="Reconcile FK-based relationships",
    description=(
        "Scans ES instances and OMS ontology definitions to automatically "
        "populate `relationships` fields based on detected foreign-key references."
    ),
)
@trace_endpoint("bff.objectify.reconcile_relationships")
async def reconcile_relationships(
    db_name: str,
    branch: str = Query("main", description="Ontology branch"),
    oms_client: OMSClient = Depends(get_oms_client),
    es_service: ElasticsearchService = Depends(get_elasticsearch_service),
    objectify_registry: ObjectifyRegistry = Depends(get_objectify_registry),
) -> Dict[str, Any]:
    return await relationship_reconciler_service.reconcile_relationships(
        db_name=db_name,
        branch=branch,
        oms_client=oms_client,
        es_service=es_service,
        objectify_registry=objectify_registry,
    )
