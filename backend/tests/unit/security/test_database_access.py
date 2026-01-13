import asyncpg
import pytest

from shared.security.database_access import enforce_database_role, get_database_access_role, has_database_access_config


class _FakeConn:
    def __init__(self, *, fetchrow_exception=None, fetchrow_result=None):
        self._fetchrow_exception = fetchrow_exception
        self._fetchrow_result = fetchrow_result
        self.closed = False

    async def fetchrow(self, *args, **kwargs):
        if self._fetchrow_exception is not None:
            raise self._fetchrow_exception
        return self._fetchrow_result

    async def close(self):
        self.closed = True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_database_access_role_returns_none_when_table_missing(monkeypatch):
    conn = _FakeConn(fetchrow_exception=asyncpg.UndefinedTableError("missing table"))

    async def _connect(_dsn):
        return conn

    import shared.security.database_access as module

    monkeypatch.setattr(module.asyncpg, "connect", _connect)

    role = await get_database_access_role(db_name="demo", principal_type="user", principal_id="u1")
    assert role is None
    assert conn.closed is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_has_database_access_config_returns_false_when_table_missing(monkeypatch):
    conn = _FakeConn(fetchrow_exception=asyncpg.UndefinedTableError("missing table"))

    async def _connect(_dsn):
        return conn

    import shared.security.database_access as module

    monkeypatch.setattr(module.asyncpg, "connect", _connect)

    configured = await has_database_access_config(db_name="demo")
    assert configured is False
    assert conn.closed is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enforce_database_role_allows_when_unconfigured_and_flag_unset(monkeypatch):
    async def _no_role(**_kwargs):
        return None

    async def _no_config(**_kwargs):
        return False

    import shared.security.database_access as module

    monkeypatch.setattr(module, "get_database_access_role", _no_role)
    monkeypatch.setattr(module, "has_database_access_config", _no_config)
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
    async def _no_role(**_kwargs):
        return None

    import shared.security.database_access as module

    monkeypatch.setattr(module, "get_database_access_role", _no_role)
    monkeypatch.setenv("BFF_REQUIRE_DB_ACCESS", "true")

    with pytest.raises(ValueError, match="Permission denied"):
        await enforce_database_role(
            headers={"X-User-Type": "user", "X-User-ID": "u1"},
            db_name="demo",
            required_roles=("Owner",),
            allow_if_unconfigured=True,
            require_env_key="BFF_REQUIRE_DB_ACCESS",
        )

