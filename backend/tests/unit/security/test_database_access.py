import asyncpg
import pytest

from shared.security.database_access import (
    DatabaseAccessInspection,
    DatabaseAccessRegistryUnavailableError,
    DatabaseAccessState,
    ensure_database_access_table,
    enforce_database_role,
    fetch_database_access_entries,
    get_database_access_role,
    has_database_access_config,
    inspect_database_access,
    list_database_names,
    upsert_database_access_entry,
)


class _FakeConn:
    def __init__(
        self,
        *,
        fetchrow_exception=None,
        fetchrow_result=None,
        fetch_exception=None,
        fetch_result=None,
        execute_exceptions=None,
        existing_relations: set[str] | None = None,
    ):
        self._fetchrow_exception = fetchrow_exception
        self._fetchrow_result = fetchrow_result
        self._fetch_exception = fetch_exception
        self._fetch_result = fetch_result or []
        self._execute_exceptions = list(execute_exceptions or [])
        self._existing_relations = set(existing_relations or set())
        self.closed = False
        self.executed: list[str] = []

    async def fetchrow(self, *args, **kwargs):
        if self._fetchrow_exception is not None:
            raise self._fetchrow_exception
        return self._fetchrow_result

    async def fetch(self, *args, **kwargs):
        if self._fetch_exception is not None:
            raise self._fetch_exception
        return self._fetch_result

    async def fetchval(self, sql, *args, **kwargs):
        _ = kwargs
        if sql == "SELECT to_regclass($1) IS NOT NULL":
            return args[0] in self._existing_relations
        return None

    async def execute(self, *args, **kwargs):
        sql = args[0]
        self.executed.append(sql)
        _ = kwargs
        if self._execute_exceptions:
            exc = self._execute_exceptions.pop(0)
            if exc is not None:
                raise exc
        if "CREATE TABLE IF NOT EXISTS database_access" in sql:
            self._existing_relations.add("database_access")
        return None

    async def close(self):
        self.closed = True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_database_access_role_raises_unavailable_when_table_missing(monkeypatch):
    conn = _FakeConn(fetchrow_exception=asyncpg.UndefinedTableError("missing table"))

    async def _connect(_dsn):
        return conn

    import shared.security.database_access as module

    monkeypatch.setattr(module.asyncpg, "connect", _connect)

    with pytest.raises(DatabaseAccessRegistryUnavailableError):
        await get_database_access_role(db_name="demo", principal_type="user", principal_id="u1")
    assert conn.closed is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_inspect_database_access_reports_configured_role(monkeypatch):
    conn = _FakeConn(fetchrow_result={"configured": True, "role": "Owner"})

    async def _connect(_dsn):
        return conn

    import shared.security.database_access as module

    monkeypatch.setattr(module.asyncpg, "connect", _connect)

    inspection = await inspect_database_access(db_name="demo", principal_type="user", principal_id="u1")
    assert inspection == DatabaseAccessInspection(state=DatabaseAccessState.CONFIGURED, role="Owner")
    assert conn.closed is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_database_access_role_returns_none_when_registry_unavailable(monkeypatch):
    async def _connect(_dsn):
        raise ConnectionRefusedError("registry down")

    import shared.security.database_access as module

    monkeypatch.setattr(module.asyncpg, "connect", _connect)

    role = await get_database_access_role(db_name="demo", principal_type="user", principal_id="u1")
    assert role is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_inspect_database_access_reports_unavailable_when_registry_unreachable(monkeypatch):
    async def _connect(_dsn):
        raise ConnectionRefusedError("registry down")

    import shared.security.database_access as module

    monkeypatch.setattr(module.asyncpg, "connect", _connect)

    inspection = await inspect_database_access(db_name="demo")
    assert inspection == DatabaseAccessInspection(state=DatabaseAccessState.UNAVAILABLE)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_has_database_access_config_raises_unavailable_when_table_missing(monkeypatch):
    conn = _FakeConn(fetchrow_exception=asyncpg.UndefinedTableError("missing table"))

    async def _connect(_dsn):
        return conn

    import shared.security.database_access as module

    monkeypatch.setattr(module.asyncpg, "connect", _connect)

    with pytest.raises(DatabaseAccessRegistryUnavailableError):
        await has_database_access_config(db_name="demo")
    assert conn.closed is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_has_database_access_config_returns_false_when_registry_unavailable(monkeypatch):
    async def _connect(_dsn):
        raise ConnectionRefusedError("registry down")

    import shared.security.database_access as module

    monkeypatch.setattr(module.asyncpg, "connect", _connect)

    configured = await has_database_access_config(db_name="demo")
    assert configured is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_database_role_allows_when_unconfigured_and_flag_unset(monkeypatch):
    async def _unconfigured(**_kwargs):
        return DatabaseAccessInspection(state=DatabaseAccessState.UNCONFIGURED)

    import shared.security.database_access as module

    monkeypatch.setattr(module, "inspect_database_access", _unconfigured)
    monkeypatch.delenv("BFF_REQUIRE_DB_ACCESS", raising=False)

    await enforce_database_role(
        headers={"X-User-Type": "user", "X-User-ID": "u1"},
        db_name="demo",
        required_roles=("Owner",),
        allow_if_unconfigured=True,
        require_env_key="BFF_REQUIRE_DB_ACCESS",
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_database_role_denies_when_flag_true_and_no_role(monkeypatch):
    async def _configured_without_role(**_kwargs):
        return DatabaseAccessInspection(state=DatabaseAccessState.CONFIGURED)

    import shared.security.database_access as module

    monkeypatch.setattr(module, "inspect_database_access", _configured_without_role)
    monkeypatch.setenv("BFF_REQUIRE_DB_ACCESS", "true")

    with pytest.raises(ValueError, match="Permission denied"):
        await enforce_database_role(
            headers={"X-User-Type": "user", "X-User-ID": "u1"},
            db_name="demo",
            required_roles=("Owner",),
            allow_if_unconfigured=True,
            require_env_key="BFF_REQUIRE_DB_ACCESS",
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_database_role_raises_when_registry_unavailable_and_not_allowed(monkeypatch):
    async def _unavailable(**_kwargs):
        return DatabaseAccessInspection(state=DatabaseAccessState.UNAVAILABLE)

    import shared.security.database_access as module

    monkeypatch.setattr(module, "inspect_database_access", _unavailable)
    monkeypatch.delenv("BFF_REQUIRE_DB_ACCESS", raising=False)

    with pytest.raises(DatabaseAccessRegistryUnavailableError, match="Database access registry unavailable"):
        await enforce_database_role(
            headers={"X-User-Type": "user", "X-User-ID": "u1"},
            db_name="demo",
            required_roles=("Owner",),
            allow_if_unconfigured=True,
            require_env_key="BFF_REQUIRE_DB_ACCESS",
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_database_role_allows_when_registry_unavailable_and_explicitly_enabled(monkeypatch):
    async def _unavailable(**_kwargs):
        return DatabaseAccessInspection(state=DatabaseAccessState.UNAVAILABLE)

    import shared.security.database_access as module

    monkeypatch.setattr(module, "inspect_database_access", _unavailable)
    monkeypatch.delenv("BFF_REQUIRE_DB_ACCESS", raising=False)

    await enforce_database_role(
        headers={"X-User-Type": "user", "X-User-ID": "u1"},
        db_name="demo",
        required_roles=("Owner",),
        allow_if_unconfigured=True,
        allow_if_registry_unavailable=True,
        require_env_key="BFF_REQUIRE_DB_ACCESS",
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_database_access_entries_raises_typed_error_when_registry_unavailable(monkeypatch):
    async def _connect(_dsn):
        raise ConnectionRefusedError("registry down")

    import shared.security.database_access as module

    monkeypatch.setattr(module.asyncpg, "connect", _connect)

    with pytest.raises(DatabaseAccessRegistryUnavailableError):
        await fetch_database_access_entries(db_names=["demo"])


@pytest.mark.unit
@pytest.mark.asyncio
async def test_list_database_names_raises_typed_error_when_registry_unavailable(monkeypatch):
    async def _connect(_dsn):
        raise ConnectionRefusedError("registry down")

    import shared.security.database_access as module

    monkeypatch.setattr(module.asyncpg, "connect", _connect)

    with pytest.raises(DatabaseAccessRegistryUnavailableError):
        await list_database_names()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_upsert_database_access_entry_raises_typed_error_when_registry_unavailable(monkeypatch):
    async def _connect(_dsn):
        raise ConnectionRefusedError("registry down")

    import shared.security.database_access as module

    monkeypatch.setattr(module.asyncpg, "connect", _connect)

    with pytest.raises(DatabaseAccessRegistryUnavailableError):
        await upsert_database_access_entry(
            db_name="demo",
            principal_type="user",
            principal_id="u1",
            principal_name="User 1",
            role="Owner",
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_inspect_database_access_raises_when_table_missing_and_bootstrap_disabled(monkeypatch):
    conn = _FakeConn(fetchrow_exception=asyncpg.UndefinedTableError("missing table"))

    async def _connect(_dsn):
        return conn

    import shared.security.database_access as module

    monkeypatch.setattr(module.asyncpg, "connect", _connect)
    monkeypatch.setenv("ALLOW_RUNTIME_DDL_BOOTSTRAP", "false")

    with pytest.raises(DatabaseAccessRegistryUnavailableError, match="database_access schema is missing"):
        await inspect_database_access(db_name="demo")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ensure_database_access_table_raises_when_bootstrap_disabled(monkeypatch):
    conn = _FakeConn()

    monkeypatch.setenv("ALLOW_RUNTIME_DDL_BOOTSTRAP", "false")

    with pytest.raises(DatabaseAccessRegistryUnavailableError, match="missing required schema objects"):
        await ensure_database_access_table(conn)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_upsert_database_access_entry_bootstraps_missing_table_in_dev(monkeypatch):
    conn = _FakeConn(
        execute_exceptions=[asyncpg.UndefinedTableError("missing table")],
    )

    async def _connect(_dsn):
        return conn

    import shared.security.database_access as module

    monkeypatch.setattr(module.asyncpg, "connect", _connect)
    monkeypatch.setenv("ALLOW_RUNTIME_DDL_BOOTSTRAP", "true")

    await upsert_database_access_entry(
        db_name="demo",
        principal_type="user",
        principal_id="u1",
        principal_name="User 1",
        role="Owner",
    )

    assert any("CREATE TABLE IF NOT EXISTS database_access" in sql for sql in conn.executed)
