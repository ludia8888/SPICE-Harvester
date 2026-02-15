from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from oms.exceptions import DatabaseError
from oms.services.terminus.query import QueryService


def _make_service(mock_result):
    service = QueryService.__new__(QueryService)
    service.connection_info = SimpleNamespace(account="admin")
    service._branch_descriptor = lambda _branch: ""
    service._make_request = AsyncMock(return_value=mock_result)
    return service


@pytest.mark.asyncio
async def test_execute_query_rejects_legacy_woql_payload() -> None:
    service = _make_service([])

    with pytest.raises(DatabaseError, match="WOQL queries are no longer supported"):
        await service.execute_query("test_db", {"@type": "And", "query": []})


@pytest.mark.asyncio
async def test_execute_query_accepts_class_type_alias_and_applies_filters_and_select() -> None:
    service = _make_service(
        [
            {"@id": "Customer/c1", "@type": "Customer", "status": "ACTIVE", "name": "Alice", "tier": "gold"},
            {"@id": "Customer/c2", "@type": "Customer", "status": "INACTIVE", "name": "Bob", "tier": "silver"},
        ]
    )

    result = await service.execute_query(
        "test_db",
        {
            "class_type": "Customer",
            "filters": [{"field": "status", "operator": "eq", "value": "ACTIVE"}],
            "select": ["name", "status"],
            "limit": 50,
            "offset": 0,
        },
    )

    assert result["total"] == 1
    assert result["results"] == [
        {
            "@id": "Customer/c1",
            "@type": "Customer",
            "class_id": "Customer",
            "instance_id": "c1",
            "name": "Alice",
            "status": "ACTIVE",
        }
    ]

    call_kwargs = service._make_request.call_args.kwargs
    assert call_kwargs["params"]["type"] == "Customer"
    assert call_kwargs["params"]["graph_type"] == "instance"
