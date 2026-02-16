from __future__ import annotations

import logging
from typing import Any, Dict

from shared.config.settings import ApplicationSettings
from shared.middleware.rate_limiter import RateLimiter
from shared.services.storage.elasticsearch_service import ElasticsearchService


async def initialize_rate_limiter_service(*, services: Dict[str, Any], logger: logging.Logger) -> None:
    """Shared initialization routine for rate limiting."""
    try:
        logger.info("Initializing rate limiter...")
        rate_limiter = RateLimiter()
        await rate_limiter.initialize()
        services["rate_limiter"] = rate_limiter
        logger.info("Rate limiter initialized successfully")
    except Exception as exc:
        logger.error("Failed to initialize rate limiter: %s", exc)


async def initialize_elasticsearch_service(
    *,
    services: Dict[str, Any],
    settings: ApplicationSettings,
    logger: logging.Logger,
) -> None:
    """Shared initialization routine for Elasticsearch."""
    try:
        elasticsearch_service = ElasticsearchService(
            host=settings.database.elasticsearch_host,
            port=settings.database.elasticsearch_port,
            username=settings.database.elasticsearch_username,
            password=settings.database.elasticsearch_password,
        )
        await elasticsearch_service.connect()
        services["elasticsearch_service"] = elasticsearch_service
        logger.info("Elasticsearch initialized successfully")
    except Exception as exc:
        logger.error("Failed to initialize Elasticsearch: %s", exc)
        # Elasticsearch is optional in current runtime profiles.
        services["elasticsearch_service"] = None

