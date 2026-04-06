from __future__ import annotations

import pytest

from shared.services.registries.agent_function_registry import AgentFunctionRegistry
from shared.services.registries.agent_model_registry import AgentModelRegistry
from shared.services.registries.agent_policy_registry import AgentPolicyRegistry
from shared.services.registries.agent_registry import AgentRegistry
from shared.services.registries.agent_session_registry import AgentSessionRegistry
from shared.services.registries.agent_tool_registry import AgentToolRegistry
from shared.services.registries.postgres_schema_registry import MissingSchemaObjectsError


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Conn:
    def __init__(
        self,
        *,
        existing_schemas: set[str] | None = None,
        existing_relations: set[str] | None = None,
    ) -> None:
        self._existing_schemas = set(existing_schemas or set())
        self._existing_relations = set(existing_relations or set())
        self.executed: list[str] = []

    async def fetchval(self, sql: str, *args):
        self.executed.append(sql)
        if sql.startswith("SELECT pg_try_advisory_lock"):
            return True
        if "information_schema.schemata" in sql:
            return args[0] in self._existing_schemas
        if sql == "SELECT to_regclass($1) IS NOT NULL":
            return args[0] in self._existing_relations
        return None

    async def execute(self, sql: str, *_args):
        self.executed.append(sql)
        if sql == "CREATE SCHEMA IF NOT EXISTS spice_agent":
            self._existing_schemas.add("spice_agent")
        if "CREATE TABLE IF NOT EXISTS spice_agent." in sql:
            name = sql.split("CREATE TABLE IF NOT EXISTS spice_agent.", 1)[1].split(" ", 1)[0].split("(", 1)[0]
            self._existing_relations.add(f"spice_agent.{name.strip()}")
        return "OK"


class _Pool:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn

    def acquire(self):
        return _AcquireCtx(self._conn)


@pytest.mark.parametrize(
    ("registry_cls", "expected"),
    [
        (AgentModelRegistry, ("agent_models",)),
        (AgentPolicyRegistry, ("agent_tenant_policies",)),
        (AgentToolRegistry, ("agent_tool_policies",)),
        (AgentFunctionRegistry, ("agent_functions",)),
        (
            AgentRegistry,
            (
                "agent_runs",
                "agent_steps",
                "agent_approvals",
                "agent_approval_requests",
                "agent_tool_idempotency",
            ),
        ),
        (
            AgentSessionRegistry,
            (
                "agent_sessions",
                "agent_session_messages",
                "agent_session_jobs",
                "agent_session_context_items",
                "agent_session_events",
                "agent_session_tool_calls",
                "agent_session_llm_calls",
                "agent_session_ci_results",
            ),
        ),
    ],
)
def test_agent_registry_required_tables_declared(registry_cls, expected) -> None:
    registry = registry_cls(dsn="postgres://unused")

    assert registry._required_tables() == expected  # noqa: SLF001


@pytest.mark.asyncio
async def test_agent_registry_ensure_schema_skips_bootstrap_when_objects_exist() -> None:
    registry = AgentRegistry(
        dsn="postgres://unused",
        allow_runtime_ddl_bootstrap=False,
    )
    conn = _Conn(
        existing_schemas={"spice_agent"},
        existing_relations={
            "spice_agent.agent_runs",
            "spice_agent.agent_steps",
            "spice_agent.agent_approvals",
            "spice_agent.agent_approval_requests",
            "spice_agent.agent_tool_idempotency",
        },
    )
    registry._pool = _Pool(conn)

    await registry.ensure_schema()

    assert "CREATE SCHEMA IF NOT EXISTS spice_agent" not in conn.executed


@pytest.mark.asyncio
async def test_agent_session_registry_raises_when_objects_missing_and_bootstrap_disabled() -> None:
    registry = AgentSessionRegistry(
        dsn="postgres://unused",
        allow_runtime_ddl_bootstrap=False,
    )
    registry._pool = _Pool(_Conn())

    with pytest.raises(MissingSchemaObjectsError, match="missing required schema objects"):
        await registry.ensure_schema()


@pytest.mark.asyncio
async def test_agent_registry_bootstrap_executes_representative_schema_sql() -> None:
    registry = AgentRegistry(
        dsn="postgres://unused",
        allow_runtime_ddl_bootstrap=True,
    )
    conn = _Conn()
    registry._pool = _Pool(conn)

    await registry.ensure_schema()

    assert any("CREATE TABLE IF NOT EXISTS spice_agent.agent_runs" in sql for sql in conn.executed)
    assert any("CREATE TABLE IF NOT EXISTS spice_agent.agent_approval_requests" in sql for sql in conn.executed)
    assert any("idx_agent_tool_idempotency_tool_id" in sql for sql in conn.executed)


@pytest.mark.asyncio
async def test_agent_session_registry_bootstrap_executes_representative_schema_sql() -> None:
    registry = AgentSessionRegistry(
        dsn="postgres://unused",
        allow_runtime_ddl_bootstrap=True,
    )
    conn = _Conn()
    registry._pool = _Pool(conn)

    await registry.ensure_schema()

    assert any("CREATE TABLE IF NOT EXISTS spice_agent.agent_sessions" in sql for sql in conn.executed)
    assert any("CREATE TABLE IF NOT EXISTS spice_agent.agent_session_tool_calls" in sql for sql in conn.executed)
    assert any("idx_agent_session_llm_calls_model" in sql for sql in conn.executed)


@pytest.mark.asyncio
async def test_agent_catalog_registries_bootstrap_executes_representative_schema_sql() -> None:
    registries = [
        AgentFunctionRegistry(dsn="postgres://unused", allow_runtime_ddl_bootstrap=True),
        AgentModelRegistry(dsn="postgres://unused", allow_runtime_ddl_bootstrap=True),
        AgentPolicyRegistry(dsn="postgres://unused", allow_runtime_ddl_bootstrap=True),
        AgentToolRegistry(dsn="postgres://unused", allow_runtime_ddl_bootstrap=True),
    ]

    for registry in registries:
        conn = _Conn()
        registry._pool = _Pool(conn)
        await registry.ensure_schema()
        if isinstance(registry, AgentFunctionRegistry):
            assert any("CREATE TABLE IF NOT EXISTS spice_agent.agent_functions" in sql for sql in conn.executed)
            assert any("idx_agent_functions_function_id" in sql for sql in conn.executed)
        if isinstance(registry, AgentModelRegistry):
            assert any("CREATE TABLE IF NOT EXISTS spice_agent.agent_models" in sql for sql in conn.executed)
            assert any("idx_agent_models_provider" in sql for sql in conn.executed)
        if isinstance(registry, AgentPolicyRegistry):
            assert any("CREATE TABLE IF NOT EXISTS spice_agent.agent_tenant_policies" in sql for sql in conn.executed)
            assert any("idx_agent_tenant_policies_updated_at" in sql for sql in conn.executed)
        if isinstance(registry, AgentToolRegistry):
            assert any("CREATE TABLE IF NOT EXISTS spice_agent.agent_tool_policies" in sql for sql in conn.executed)
            assert any("idx_agent_tool_policies_type" in sql for sql in conn.executed)
