from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from bff.dependencies import FoundryQueryService


@pytest.mark.asyncio
async def test_query_database_adapts_to_foundry_where_and_page_token() -> None:
    oms = AsyncMock()
    oms.search_objects_v2 = AsyncMock(
        return_value={
            "data": [{"customer_id": "c1", "status": "ACTIVE"}],
            "totalCount": "123",
            "nextPageToken": "MTI1",
        }
    )
    service = FoundryQueryService(oms)

    result = await service.query_database(
        "demo_db",
        {
            "class_id": "Customer",
            "filters": [{"field": "status", "operator": "eq", "value": "ACTIVE"}],
            "select": ["customer_id", "status"],
            "order_by": "customer_id",
            "order_direction": "asc",
            "limit": 25,
            "offset": 100,
        },
    )

    assert result["count"] == 123
    assert result["data"] == [{"customer_id": "c1", "status": "ACTIVE"}]
    assert result["nextPageToken"] == "MTI1"

    oms.search_objects_v2.assert_awaited_once()
    kwargs = oms.search_objects_v2.await_args.kwargs
    assert kwargs["db_name"] == "demo_db"
    assert kwargs["object_type"] == "Customer"
    assert kwargs["where"] == {"type": "eq", "field": "status", "value": "ACTIVE"}
    assert kwargs["page_size"] == 25
    assert kwargs["page_token"] == "MTAw"
    assert kwargs["select"] == ["customer_id", "status"]
    assert kwargs["order_by"] == "customer_id"
    assert kwargs["order_direction"] == "asc"
    assert kwargs["branch"] == "main"


@pytest.mark.asyncio
async def test_query_database_maps_not_in_and_not_null_filters() -> None:
    oms = AsyncMock()
    oms.search_objects_v2 = AsyncMock(
        return_value={
            "data": [],
            "totalCount": "0",
            "nextPageToken": None,
        }
    )
    service = FoundryQueryService(oms)

    await service.query_database(
        "demo_db",
        {
            "class_id": "Order",
            "filters": [
                {"field": "state", "operator": "not_in", "value": ["CANCELLED", "FAILED"]},
                {"field": "amount", "operator": "is_not_null"},
            ],
        },
    )

    where = oms.search_objects_v2.await_args.kwargs["where"]
    assert where["type"] == "and"
    assert len(where["value"]) == 2
    assert where["value"][0]["type"] == "not"
    assert where["value"][1] == {
        "type": "not",
        "value": {"type": "isNull", "field": "amount"},
    }


@pytest.mark.asyncio
async def test_query_database_rejects_invalid_order_direction() -> None:
    oms = AsyncMock()
    service = FoundryQueryService(oms)

    with pytest.raises(ValueError, match="order_direction must be 'asc' or 'desc'"):
        await service.query_database(
            "demo_db",
            {
                "class_id": "Customer",
                "order_by": "customer_id",
                "order_direction": "up",
            },
        )
