from __future__ import annotations

from fastapi import Response
from starlette.requests import Request

from bff.routers.query import execute_query
from shared.models.ontology import QueryInput


class _FakeMapper:
    async def convert_query_to_internal(self, db_name, query_dict, lang):  # noqa: ANN001, ANN201
        _ = db_name, lang
        return {"class_id": "Order", **query_dict}

    async def convert_to_display_batch(self, db_name, raw_results, lang):  # noqa: ANN001, ANN201
        _ = db_name, lang
        return list(raw_results)


class _FakeQueryService:
    async def query_database(self, db_name, internal_query):  # noqa: ANN001, ANN201
        _ = db_name, internal_query
        return {"data": [{"order_id": "o-1"}], "count": 1}


class _FakeDatasetRegistry:
    async def get_access_policy(self, **kwargs):  # noqa: ANN003, ANN201
        _ = kwargs
        return None


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/v1/databases/demo/query",
            "headers": [],
            "query_string": b"",
        }
    )


async def test_execute_query_sets_v1_deprecation_headers() -> None:
    response = Response()
    payload = await execute_query(
        db_name="demo",
        query=QueryInput(class_label="Order"),
        request=_request(),
        response=response,
        mapper=_FakeMapper(),
        query_service=_FakeQueryService(),
        dataset_registry=_FakeDatasetRegistry(),
    )

    assert payload["total"] == 1
    assert response.headers.get("Deprecation") == "true"
    assert response.headers.get("Sunset")
    link_header = str(response.headers.get("Link"))
    assert "successor-version" in link_header
    assert "deprecation" in link_header
