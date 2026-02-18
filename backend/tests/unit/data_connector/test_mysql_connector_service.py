from __future__ import annotations

import sys

import pytest

from data_connector.adapters.factory import ConnectorAdapterFactory


class _FakeCursor:
    def execute(self, query: str) -> None:
        _ = query

    def fetchone(self):
        return {"File": "mysql-bin.000001", "Position": 123}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        _ = exc_type, exc, tb
        return False


class _FakeConnection:
    def cursor(self):
        return _FakeCursor()

    def close(self) -> None:
        return None


class _FakeDriver:
    class cursors:
        DictCursor = object

    def connect(self, **kwargs):
        _ = kwargs
        return _FakeConnection()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_mysql_peek_change_token_binlog_uses_shared_blocking_runner(monkeypatch: pytest.MonkeyPatch):
    factory = ConnectorAdapterFactory(google_sheets_service=object())
    service = factory.get_adapter("mysql")
    mysql_service_module = sys.modules["data_connector.mysql.service"]
    calls: dict[str, str] = {}

    async def _fake_run_blocking_query(fn, *, adapter_name: str, operation: str, timeout_seconds: int = 300):
        _ = timeout_seconds
        calls["adapter_name"] = adapter_name
        calls["operation"] = operation
        return fn()

    monkeypatch.setattr(mysql_service_module, "run_blocking_query", _fake_run_blocking_query)
    monkeypatch.setattr(service, "_require_driver", lambda: _FakeDriver())
    monkeypatch.setattr(
        service,
        "_connect_kwargs",
        lambda *, config, secrets: {"host": "localhost", "database": "demo", "user": "demo", "password": "secret"},
    )

    token = await service.peek_change_token(
        config={},
        secrets={},
        import_config={"cdcStrategy": "binlog"},
    )

    assert token == "mysql-bin.000001:123"
    assert calls == {
        "adapter_name": "MySQL",
        "operation": "peek_change_token binlog",
    }
