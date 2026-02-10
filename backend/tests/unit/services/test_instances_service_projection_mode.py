from __future__ import annotations

from typing import Any, Dict, Optional

import pytest
from fastapi import HTTPException

from bff.services.instances_service import (
    get_class_sample_values,
    get_instance_detail,
    list_class_instances,
)
from shared.config.search_config import get_instances_index_name


class _FakeDatasetRegistry:
    async def get_access_policy(  # noqa: D401
        self,
        *,
        db_name: str,  # noqa: ARG002
        scope: str,  # noqa: ARG002
        subject_type: str,  # noqa: ARG002
        subject_id: str,  # noqa: ARG002
    ) -> None:
        return None


class _FakeElasticsearchService:
    def __init__(
        self,
        *,
        search_result: Optional[Dict[str, Any]] = None,
        search_exception: Optional[Exception] = None,
        document_result: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.search_result = search_result if search_result is not None else {"total": 0, "hits": []}
        self.search_exception = search_exception
        self.document_result = document_result
        self.search_calls: list[Dict[str, Any]] = []
        self.get_document_calls: list[Dict[str, Any]] = []

    async def search(self, **kwargs: Any) -> Dict[str, Any]:
        self.search_calls.append(kwargs)
        if self.search_exception is not None:
            raise self.search_exception
        return self.search_result

    async def get_document(self, index: str, doc_id: str) -> Optional[Dict[str, Any]]:
        self.get_document_calls.append({"index": index, "doc_id": doc_id})
        return self.document_result


@pytest.mark.asyncio
async def test_list_instances_es_error_fails_closed_without_fallback() -> None:
    es = _FakeElasticsearchService(search_exception=TimeoutError("es-timeout"))

    with pytest.raises(HTTPException) as raised:
        await list_class_instances(
            db_name="demo_db",
            class_id="Order",
            request_headers={},
            base_branch="main",
            overlay_branch=None,
            branch=None,
            limit=25,
            offset=0,
            search=None,
            status_filter=None,
            action_type_id=None,
            submitted_by=None,
            elasticsearch_service=es,  # type: ignore[arg-type]
            dataset_registry=_FakeDatasetRegistry(),  # type: ignore[arg-type]
            action_logs=None,
        )

    assert raised.value.status_code == 503
    assert isinstance(raised.value.detail, dict)
    assert raised.value.detail.get("error") == "overlay_degraded"


@pytest.mark.asyncio
async def test_get_instance_es_error_fails_closed_without_fallback() -> None:
    es = _FakeElasticsearchService(search_exception=TimeoutError("es-timeout"))

    with pytest.raises(HTTPException) as raised:
        await get_instance_detail(
            db_name="demo_db",
            class_id="Order",
            instance_id="inst-1",
            request_headers={},
            base_branch="main",
            overlay_branch=None,
            branch=None,
            elasticsearch_service=es,  # type: ignore[arg-type]
            dataset_registry=_FakeDatasetRegistry(),  # type: ignore[arg-type]
            action_logs=None,
        )

    assert raised.value.status_code == 503
    assert isinstance(raised.value.detail, dict)
    assert raised.value.detail.get("error") == "overlay_degraded"


@pytest.mark.asyncio
async def test_get_instance_missing_doc_returns_404_without_fallback() -> None:
    es = _FakeElasticsearchService(search_result={"total": 0, "hits": []})

    with pytest.raises(HTTPException) as raised:
        await get_instance_detail(
            db_name="demo_db",
            class_id="Order",
            instance_id="inst-1",
            request_headers={},
            base_branch="main",
            overlay_branch=None,
            branch=None,
            elasticsearch_service=es,  # type: ignore[arg-type]
            dataset_registry=_FakeDatasetRegistry(),  # type: ignore[arg-type]
            action_logs=None,
        )

    assert raised.value.status_code == 404


@pytest.mark.asyncio
async def test_sample_values_reads_from_es() -> None:
    es = _FakeElasticsearchService(
        search_result={
            "total": 2,
            "hits": [
                {"class_id": "Order", "instance_id": "inst-1", "name": "alpha"},
                {"class_id": "Order", "instance_id": "inst-2", "name": "beta"},
            ],
        }
    )

    result = await get_class_sample_values(
        db_name="demo_db",
        class_id="Order",
        property_name="name",
        base_branch="main",
        branch=None,
        limit=10,
        elasticsearch_service=es,  # type: ignore[arg-type]
        dataset_registry=_FakeDatasetRegistry(),  # type: ignore[arg-type]
    )

    assert result["class_id"] == "Order"
    assert result["property"] == "name"
    assert result["values"] == ["alpha", "beta"]
    assert len(es.search_calls) == 1
    assert es.search_calls[0]["index"] == get_instances_index_name("demo_db", branch="main")


@pytest.mark.asyncio
async def test_sample_values_es_error_fails_closed() -> None:
    es = _FakeElasticsearchService(search_exception=TimeoutError("es-timeout"))

    with pytest.raises(HTTPException) as raised:
        await get_class_sample_values(
            db_name="demo_db",
            class_id="Order",
            property_name=None,
            base_branch="main",
            branch=None,
            limit=10,
            elasticsearch_service=es,  # type: ignore[arg-type]
            dataset_registry=_FakeDatasetRegistry(),  # type: ignore[arg-type]
        )

    assert raised.value.status_code == 503
    assert isinstance(raised.value.detail, dict)
    assert raised.value.detail.get("error") == "overlay_degraded"
