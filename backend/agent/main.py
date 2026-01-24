"""
Agent Service - LangGraph-based orchestration runtime.

Port: 8004
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from agent.routers.agent import router as agent_router
from shared.config.settings import get_settings
from shared.middleware.rate_limiter import RateLimiter
from shared.services.audit_log_store import AuditLogStore
from shared.services.agent_registry import AgentRegistry
from shared.services.agent_session_registry import AgentSessionRegistry
from shared.services.event_store import event_store
from shared.services.service_factory import create_fastapi_service, get_agent_service_info, run_service
from shared.utils.app_logger import get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize dependencies."""
    require_event_store = get_settings().agent.require_event_store
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

    agent_registry = None
    try:
        agent_registry = AgentRegistry()
        await agent_registry.initialize()
        logger.info("AgentRegistry connected for agent service")
    except Exception as exc:
        logger.warning("AgentRegistry unavailable for agent service: %s", exc)
    app.state.agent_registry = agent_registry

    session_registry = None
    try:
        session_registry = AgentSessionRegistry()
        await session_registry.initialize()
        logger.info("AgentSessionRegistry connected for agent service")
    except Exception as exc:
        logger.warning("AgentSessionRegistry unavailable for agent service: %s", exc)
    app.state.agent_session_registry = session_registry

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
    if getattr(app.state, "agent_registry", None):
        await app.state.agent_registry.close()
    if getattr(app.state, "agent_session_registry", None):
        await app.state.agent_session_registry.close()


service_info = get_agent_service_info()
app = create_fastapi_service(
    service_info=service_info,
    custom_lifespan=lifespan,
    include_health_check=True,
    include_logging_middleware=True,
)

app.include_router(agent_router, prefix="/api/v1")


if __name__ == "__main__":
    run_service(app, service_info, "agent.main:app")
