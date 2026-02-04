"""
Graph federation dependency provider (BFF).

Replaces the module-level singleton in `bff.routers.graph` with container-backed
service composition while remaining test-friendly (works even when the global
container is not initialized).
"""

import logging

from shared.config.settings import ApplicationSettings, get_settings
from shared.dependencies.container import ServiceContainer, get_container
from shared.models.config import ConnectionConfig
from shared.services.core.async_terminus import AsyncTerminusService
from shared.services.core.graph_federation_service_woql import GraphFederationServiceWOQL

logger = logging.getLogger(__name__)


def _build_graph_federation_service(settings: ApplicationSettings) -> GraphFederationServiceWOQL:
    db = settings.database
    connection_info = ConnectionConfig(
        server_url=db.terminus_url,
        user=db.terminus_user,
        key=db.terminus_password,
        account=db.terminus_account,
        timeout=int(db.terminus_timeout),
        retries=int(db.terminus_retry_attempts),
        retry_delay=float(db.terminus_retry_delay),
        ssl_verify=bool(db.terminus_ssl_verify),
    )
    terminus_service = AsyncTerminusService(connection_info)
    return GraphFederationServiceWOQL(
        terminus_service=terminus_service,
        es_host=db.elasticsearch_host,
        es_port=db.elasticsearch_port,
        es_username=(db.elasticsearch_username or "").strip(),
        es_password=db.elasticsearch_password or "",
    )


async def _get_from_container(container: ServiceContainer) -> GraphFederationServiceWOQL:
    if not container.has(GraphFederationServiceWOQL):
        container.register_singleton(GraphFederationServiceWOQL, _build_graph_federation_service)
        logger.info("GraphFederationServiceWOQL registered in DI container")
    return await container.get(GraphFederationServiceWOQL)


async def get_graph_federation_service() -> GraphFederationServiceWOQL:
    """
    FastAPI dependency to get a GraphFederationServiceWOQL instance.

    - Primary: container-backed singleton (preferred for runtime).
    - Fallback: ephemeral instance when container is not initialized (tests / scripts).
    """
    try:
        container = await get_container()
    except RuntimeError:
        return _build_graph_federation_service(get_settings())
    return await _get_from_container(container)

