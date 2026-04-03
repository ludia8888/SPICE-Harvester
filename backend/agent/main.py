"""
Agent Service - deterministic tool runner (single sequential loop).

Port: 8004
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from agent.middleware.auth import ensure_agent_auth_configured, install_agent_auth_middleware
from agent.routers.agent import router as agent_router
from shared.config.settings import get_settings
from shared.middleware.rate_limiter import RateLimiter
from shared.security.startup_guard import ensure_startup_security
from shared.services.core.audit_log_store import AuditLogStore
from shared.services.registries.agent_registry import AgentRegistry
from shared.services.registries.agent_session_registry import AgentSessionRegistry
from shared.services.storage.event_store import event_store
from shared.services.core.service_factory import create_fastapi_service, get_agent_service_info, run_service
from shared.services.core.runtime_status import ensure_runtime_status, record_runtime_issue
from shared.utils.app_logger import get_logger

logger = get_logger(__name__)


def _ensure_runtime_status(app: FastAPI) -> dict[str, object]:
    return ensure_runtime_status(app)


def _mark_runtime_issue(app: FastAPI, *, message: str, ready: bool) -> None:
    record_runtime_issue(
        app,
        component=message,
        dependency=message,
        message=message,
        state="degraded" if ready else "hard_down",
        classification="unavailable",
        affected_features=("agent_runtime",),
        affects_readiness=not ready,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize dependencies."""
    ensure_startup_security("agent")
    ensure_agent_auth_configured()
    _ensure_runtime_status(app)
    require_event_store = get_settings().agent.require_event_store
    try:
        await event_store.connect()
        app.state.event_store = event_store
        logger.info("Event store connected for agent service")
    except Exception as exc:
        if require_event_store:
            raise
        logger.warning("Event store unavailable; agent service running in degraded mode: %s", exc)
        app.state.event_store = None
        _mark_runtime_issue(app, message="event_store_unavailable", ready=False)

    audit_store = None
    try:
        audit_store = AuditLogStore()
        await audit_store.initialize()
        logger.info("AuditLogStore connected for agent service")
    except Exception as exc:
        logger.warning("AuditLogStore unavailable for agent service: %s", exc)
        _mark_runtime_issue(app, message="audit_store_unavailable", ready=True)
    app.state.audit_store = audit_store

    agent_registry = None
    try:
        agent_registry = AgentRegistry()
        await agent_registry.initialize()
        logger.info("AgentRegistry connected for agent service")
    except Exception as exc:
        logger.warning("AgentRegistry unavailable for agent service: %s", exc)
        _mark_runtime_issue(app, message="agent_registry_unavailable", ready=False)
    app.state.agent_registry = agent_registry

    session_registry = None
    try:
        session_registry = AgentSessionRegistry()
        await session_registry.initialize()
        logger.info("AgentSessionRegistry connected for agent service")
    except Exception as exc:
        logger.warning("AgentSessionRegistry unavailable for agent service: %s", exc)
        _mark_runtime_issue(app, message="agent_session_registry_unavailable", ready=False)
    app.state.agent_session_registry = session_registry

    app.state.agent_tasks = {}

    try:
        rate_limiter = RateLimiter()
        await rate_limiter.initialize()
        app.state.rate_limiter = rate_limiter
        logger.info("Rate limiter initialized for agent service")
    except Exception as exc:
        logger.warning("Rate limiter unavailable for agent service: %s", exc)
        _mark_runtime_issue(app, message="rate_limiter_unavailable", ready=True)

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
    openapi_url=None,
    docs_url=None,
    redoc_url=None,
)
install_agent_auth_middleware(app)

app.include_router(agent_router, prefix="/api/v1")


if __name__ == "__main__":
    run_service(app, service_info, "agent.main:app")
