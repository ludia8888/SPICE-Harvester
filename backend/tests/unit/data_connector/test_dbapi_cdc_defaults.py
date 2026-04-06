from __future__ import annotations

from typing import Any

import pytest

from data_connector.adapters.base import ConnectorExtractResult
from data_connector.adapters.factory import ConnectorAdapterFactory


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "adapter_name",
        "import_config",
        "sync_state",
        "row_data",
        "expected_query_fragment",
        "expected_params",
        "expected_next_state",
    ),
    [
        (
            "mysql",
            {"cdcQuery": "SELECT * FROM demo_events"},
            {"cdc_token": "mysql-bin.000001:5"},
            {"binlog_pos": "mysql-bin.000001:9"},
            "binlog_pos > %s",
            ["mysql-bin.000001:5"],
            {"cdc_token": "mysql-bin.000001:9", "cdc_strategy": "binlog"},
        ),
        (
            "sqlserver",
            {"cdcQuery": "SELECT * FROM cdc_rows"},
            {"cdc_token": 100},
            {"SYS_CHANGE_VERSION": 105},
            "SYS_CHANGE_VERSION > ?",
            [100],
            {"cdc_token": 105, "cdc_strategy": "change_tracking"},
        ),
        (
            "snowflake",
            {"cdcQuery": "SELECT * FROM stream_rows"},
            {"cdc_token": "row-1"},
            {"METADATA$ROW_ID": "row-3"},
            "METADATA$ROW_ID > %s",
            ["row-1"],
            {"cdc_token": "row-3", "cdc_strategy": "stream"},
        ),
        (
            "oracle",
            {"cdcQuery": "SELECT * FROM oracle_changes", "cdcTokenColumn": "SCN"},
            {"cdc_token": 7},
            {"SCN": 9},
            "SCN > :1",
            [7],
            {"cdc_token": 9},
        ),
    ],
)
async def test_dbapi_cdc_extract_uses_shared_defaults(
    adapter_name: str,
    import_config: dict[str, Any],
    sync_state: dict[str, Any],
    row_data: dict[str, Any],
    expected_query_fragment: str,
    expected_params: list[Any],
    expected_next_state: dict[str, Any],
) -> None:
    factory = ConnectorAdapterFactory(google_sheets_service=object())
    adapter = factory.get_adapter(adapter_name)
    captured: dict[str, Any] = {}

    async def _fake_fetch(*, query, config, secrets, params=None):
        _ = config, secrets
        captured["query"] = query
        captured["params"] = params
        return ConnectorExtractResult(
            columns=list(row_data.keys()),
            rows=[dict(row_data)],
            next_state={},
        )

    adapter._fetch = _fake_fetch  # type: ignore[attr-defined]

    result = await adapter.cdc_extract(
        config={},
        secrets={},
        import_config=import_config,
        sync_state=sync_state,
    )

    assert expected_query_fragment in str(captured["query"])
    assert captured["params"] == expected_params
    assert result.next_state == expected_next_state
