"""
Graph federation dependency provider (BFF).

Provides ``GraphFederationServiceES`` via DI container — no TerminusDB dependency.
"""

import logging

from shared.config.settings import ApplicationSettings, get_settings
from shared.dependencies.container import ServiceContainer, get_container
from shared.services.core.graph_federation_service_es import GraphFederationServiceES
from shared.services.storage.elasticsearch_service import (
    ElasticsearchService,
    create_elasticsearch_service,
)

logger = logging.getLogger(__name__)


def _build_graph_federation_service(settings: ApplicationSettings) -> GraphFederationServiceES:
    es_service = create_elasticsearch_service(settings)
    oms_base_url = getattr(settings, "oms_base_url", None) or getattr(
        getattr(settings, "database", None), "oms_base_url", None
    )
    return GraphFederationServiceES(
        es_service=es_service,
        oms_base_url=oms_base_url,
    )


async def _get_from_container(container: ServiceContainer) -> GraphFederationServiceES:
    if not container.has(GraphFederationServiceES):
        container.register_singleton(GraphFederationServiceES, _build_graph_federation_service)
        logger.info("GraphFederationServiceES registered in DI container")
    return await container.get(GraphFederationServiceES)


async def get_graph_federation_service() -> GraphFederationServiceES:
    """
    FastAPI dependency to get a GraphFederationServiceES instance.

    - Primary: container-backed singleton (preferred for runtime).
    - Fallback: ephemeral instance when container is not initialized (tests / scripts).
    """
    try:
        container = await get_container()
    except RuntimeError:
        return _build_graph_federation_service(get_settings())
    return await _get_from_container(container)
