from __future__ import annotations

import pytest

from data_connector.adapters.factory import ConnectorAdapterFactory
from data_connector.adapters.base import ConnectorExtractResult


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("adapter_name", "expect_alias_fragment"),
    [
        ("postgresql", ") AS base ORDER BY updated_at ASC, id ASC"),
        ("mysql", ") AS base ORDER BY updated_at ASC, id ASC"),
        ("sqlserver", ") AS base ORDER BY updated_at ASC, id ASC"),
        ("oracle", ") base ORDER BY updated_at ASC, id ASC"),
        ("snowflake", ") AS base ORDER BY updated_at ASC, id ASC"),
    ],
)
async def test_incremental_extract_orders_initial_full_sync(
    adapter_name: str,
    expect_alias_fragment: str,
) -> None:
    factory = ConnectorAdapterFactory(google_sheets_service=object())
    adapter = factory.get_adapter(adapter_name)
    captured: dict[str, object] = {}

    async def _fake_fetch(*, query, config, secrets, params=None):
        captured["query"] = query
        captured["params"] = params
        _ = config, secrets
        return ConnectorExtractResult(columns=["updated_at", "id"], rows=[], next_state={})

    adapter._fetch = _fake_fetch  # type: ignore[attr-defined]

    await adapter.incremental_extract(
        config={},
        secrets={},
        import_config={
            "query": "SELECT * FROM demo_table",
            "watermarkColumn": "updated_at",
            "watermarkTieBreakerColumn": "id",
        },
        sync_state={},
    )

    assert expect_alias_fragment in str(captured["query"])
    assert captured["params"] in ([], None)
