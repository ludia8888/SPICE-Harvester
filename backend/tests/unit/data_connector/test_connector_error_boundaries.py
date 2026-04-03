from __future__ import annotations

import pytest

from data_connector.adapters.factory import ConnectorAdapterFactory


class _BrokenGoogleSheetsService:
    async def _get_sheet_metadata(self, *args, **kwargs):  # noqa: ANN002, ANN003
        raise TypeError("unexpected metadata bug")


class _RaisingMySqlDriver:
    class cursors:
        DictCursor = object

    def connect(self, **kwargs):  # noqa: ANN003
        _ = kwargs
        raise TypeError("unexpected driver bug")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_google_sheets_connection_test_surfaces_internal_type_errors() -> None:
    factory = ConnectorAdapterFactory(google_sheets_service=_BrokenGoogleSheetsService())
    adapter = factory.get_adapter("google_sheets")

    with pytest.raises(TypeError, match="unexpected metadata bug"):
        await adapter.test_connection(
            config={"sheet_url": "https://docs.google.com/spreadsheets/d/sheet-1/edit"},
            secrets={"access_token": "token"},
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_mysql_connection_test_surfaces_internal_driver_type_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    factory = ConnectorAdapterFactory(google_sheets_service=object())
    adapter = factory.get_adapter("mysql")

    async def _run_blocking_query(fn, *, adapter_name: str, operation: str, timeout_seconds: int = 300):
        _ = adapter_name, operation, timeout_seconds
        return fn()

    monkeypatch.setattr("data_connector.mysql.service.run_blocking_query", _run_blocking_query)
    monkeypatch.setattr(adapter, "_require_driver", lambda: _RaisingMySqlDriver())
    monkeypatch.setattr(
        adapter,
        "_connect_kwargs",
        lambda *, config, secrets: {"host": "localhost", "database": "demo", "user": "demo", "password": "secret"},
    )

    with pytest.raises(TypeError, match="unexpected driver bug"):
        await adapter.test_connection(config={}, secrets={})
