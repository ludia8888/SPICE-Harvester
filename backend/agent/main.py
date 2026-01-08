"""
Agent Service - LangGraph-based orchestration runtime.

Port: 8004
"""

from dotenv import load_dotenv

load_dotenv()

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from agent.routers.agent import router as agent_router
from shared.middleware.rate_limiter import RateLimiter
from shared.services.audit_log_store import AuditLogStore
from shared.services.event_store import event_store
from shared.services.service_factory import AGENT_SERVICE_INFO, create_fastapi_service, run_service
from shared.utils.app_logger import get_logger

logger = get_logger(__name__)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize dependencies."""
    require_event_store = _env_bool("AGENT_REQUIRE_EVENT_STORE", True)
    try:
        await event_store.connect()
        app.state.event_store = event_store
        logger.info("Event store connected for agent service")
    except Exception as exc:
        if require_event_store:
            raise
        logger.warning("Event store unavailable; agent service running in degraded mode: %s", exc)
        app.state.event_store = event_store

    audit_store = None
    try:
        audit_store = AuditLogStore()
        await audit_store.initialize()
        logger.info("AuditLogStore connected for agent service")
    except Exception as exc:
        logger.warning("AuditLogStore unavailable for agent service: %s", exc)
    app.state.audit_store = audit_store

    app.state.agent_tasks = {}

    try:
        rate_limiter = RateLimiter()
        await rate_limiter.initialize()
        app.state.rate_limiter = rate_limiter
        logger.info("Rate limiter initialized for agent service")
    except Exception as exc:
        logger.warning("Rate limiter unavailable for agent service: %s", exc)

    yield

    if getattr(app.state, "rate_limiter", None):
        await app.state.rate_limiter.close()
    if getattr(app.state, "audit_store", None):
        await app.state.audit_store.close()


app = create_fastapi_service(
    service_info=AGENT_SERVICE_INFO,
    custom_lifespan=lifespan,
    include_health_check=True,
    include_logging_middleware=True,
)

app.include_router(agent_router, prefix="/api/v1")


if __name__ == "__main__":
    run_service(app, AGENT_SERVICE_INFO, "agent.main:app")
