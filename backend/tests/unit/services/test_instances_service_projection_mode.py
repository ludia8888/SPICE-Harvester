from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, Optional

import pytest
from fastapi import HTTPException

import bff.services.instances_service as instances_service_module
from bff.services.instances_service import (
    get_class_sample_values,
    get_instance_detail,
    list_class_instances,
)
from shared.config.search_config import get_instances_index_name


class _FakeDatasetRegistry:
    def __init__(self, policy=None) -> None:
        self._policy = policy

    async def get_access_policy(  # noqa: D401
        self,
        *,
        db_name: str,  # noqa: ARG002
        scope: str,  # noqa: ARG002
        subject_type: str,  # noqa: ARG002
        subject_id: str,  # noqa: ARG002
    ):
        return self._policy


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


class _PerIndexElasticsearchService(_FakeElasticsearchService):
    def __init__(self, results_by_index: Dict[str, Dict[str, Any]]) -> None:
        super().__init__()
        self._results_by_index = results_by_index

    async def search(self, **kwargs: Any) -> Dict[str, Any]:
        self.search_calls.append(kwargs)
        return self._results_by_index[kwargs["index"]]


class _ActionLogs:
    async def list_logs(self, **kwargs):  # noqa: ANN201
        _ = kwargs
        return []

    async def count_logs(self, **kwargs):  # noqa: ANN201
        _ = kwargs
        return 7


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


@pytest.mark.asyncio
async def test_action_log_listing_uses_exact_total_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _allow(**kwargs):  # noqa: ANN001
        _ = kwargs
        return None

    monkeypatch.setattr(instances_service_module, "_require_action_log_role", _allow)

    result = await list_class_instances(
        db_name="demo_db",
        class_id="ActionLog",
        request_headers={},
        base_branch="main",
        overlay_branch=None,
        branch=None,
        limit=2,
        offset=5,
        search=None,
        status_filter=None,
        action_type_id=None,
        submitted_by=None,
        elasticsearch_service=_FakeElasticsearchService(),  # type: ignore[arg-type]
        dataset_registry=_FakeDatasetRegistry(),  # type: ignore[arg-type]
        action_logs=_ActionLogs(),  # type: ignore[arg-type]
    )

    assert result["total"] == 7


@pytest.mark.asyncio
async def test_overlay_pagination_happens_after_merge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_index = get_instances_index_name("demo_db", branch="main")
    overlay_index = get_instances_index_name("demo_db", branch="writeback/main")
    es = _PerIndexElasticsearchService(
        {
            base_index: {
                "total": 2,
                "hits": [
                    {"class_id": "Order", "instance_id": "base-1", "event_timestamp": "2026-01-01T00:00:02Z"},
                    {"class_id": "Order", "instance_id": "base-2", "event_timestamp": "2026-01-01T00:00:01Z"},
                ],
            },
            overlay_index: {
                "total": 1,
                "hits": [
                    {"class_id": "Order", "instance_id": "overlay-1", "event_timestamp": "2026-01-01T00:00:03Z"},
                ],
            },
        }
    )

    result = await list_class_instances(
        db_name="demo_db",
        class_id="Order",
        request_headers={},
        base_branch="main",
        overlay_branch="writeback/main",
        branch=None,
        limit=1,
        offset=1,
        search=None,
        status_filter=None,
        action_type_id=None,
        submitted_by=None,
        elasticsearch_service=es,  # type: ignore[arg-type]
        dataset_registry=_FakeDatasetRegistry(),  # type: ignore[arg-type]
        action_logs=None,
    )

    assert result["total"] == 3
    assert [row["instance_id"] for row in result["instances"]] == ["base-1"]
    assert len(es.search_calls) == 2
    assert all(call["index"] in {base_index, overlay_index} for call in es.search_calls)


@pytest.mark.asyncio
async def test_access_policy_paginates_after_filtering(monkeypatch: pytest.MonkeyPatch) -> None:
    es = _FakeElasticsearchService(
        search_result={
            "total": 3,
            "hits": [
                {"class_id": "Order", "instance_id": "hidden-1", "event_timestamp": "2026-01-01T00:00:03Z"},
                {"class_id": "Order", "instance_id": "hidden-2", "event_timestamp": "2026-01-01T00:00:02Z"},
                {"class_id": "Order", "instance_id": "visible-1", "event_timestamp": "2026-01-01T00:00:01Z"},
            ],
        }
    )

    def _fake_apply_access_policy(instances, *, policy):  # noqa: ANN001
        _ = policy
        return [row for row in instances if row.get("instance_id") == "visible-1"], {}

    monkeypatch.setattr(instances_service_module, "apply_access_policy", _fake_apply_access_policy)

    result = await list_class_instances(
        db_name="demo_db",
        class_id="Order",
        request_headers={},
        base_branch="main",
        overlay_branch=None,
        branch=None,
        limit=1,
        offset=0,
        search=None,
        status_filter=None,
        action_type_id=None,
        submitted_by=None,
        elasticsearch_service=es,  # type: ignore[arg-type]
        dataset_registry=_FakeDatasetRegistry(policy=SimpleNamespace(policy={"kind": "stub"})),  # type: ignore[arg-type]
        action_logs=None,
    )

    assert result["total"] == 1
    assert [row["instance_id"] for row in result["instances"]] == ["visible-1"]


@pytest.mark.asyncio
async def test_access_policy_still_applies_offset_limit_when_no_rows_filtered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    es = _FakeElasticsearchService(
        search_result={
            "total": 3,
            "hits": [
                {"class_id": "Order", "instance_id": "row-1", "event_timestamp": "2026-01-01T00:00:03Z"},
                {"class_id": "Order", "instance_id": "row-2", "event_timestamp": "2026-01-01T00:00:02Z"},
                {"class_id": "Order", "instance_id": "row-3", "event_timestamp": "2026-01-01T00:00:01Z"},
            ],
        }
    )

    def _fake_apply_access_policy(instances, *, policy):  # noqa: ANN001
        _ = policy
        return instances, {}

    monkeypatch.setattr(instances_service_module, "apply_access_policy", _fake_apply_access_policy)

    result = await list_class_instances(
        db_name="demo_db",
        class_id="Order",
        request_headers={},
        base_branch="main",
        overlay_branch=None,
        branch=None,
        limit=1,
        offset=1,
        search=None,
        status_filter=None,
        action_type_id=None,
        submitted_by=None,
        elasticsearch_service=es,  # type: ignore[arg-type]
        dataset_registry=_FakeDatasetRegistry(policy=SimpleNamespace(policy={"kind": "stub"})),  # type: ignore[arg-type]
        action_logs=None,
    )

    assert result["total"] == 3
    assert [row["instance_id"] for row in result["instances"]] == ["row-2"]


@pytest.mark.asyncio
async def test_overlay_and_access_policy_paginate_after_logical_merge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_index = get_instances_index_name("demo_db", branch="main")
    overlay_index = get_instances_index_name("demo_db", branch="writeback/main")
    es = _PerIndexElasticsearchService(
        {
            base_index: {
                "total": 3,
                "hits": [
                    {"class_id": "Order", "instance_id": "base-1", "event_timestamp": "2026-01-01T00:00:03Z"},
                    {"class_id": "Order", "instance_id": "base-2", "event_timestamp": "2026-01-01T00:00:02Z"},
                    {"class_id": "Order", "instance_id": "base-3", "event_timestamp": "2026-01-01T00:00:01Z"},
                ],
            },
            overlay_index: {
                "total": 2,
                "hits": [
                    {"class_id": "Order", "instance_id": "base-2", "event_timestamp": "2026-01-01T00:00:04Z"},
                    {"class_id": "Order", "instance_id": "overlay-1", "event_timestamp": "2026-01-01T00:00:05Z"},
                ],
            },
        }
    )

    def _fake_apply_access_policy(instances, *, policy):  # noqa: ANN001
        _ = policy
        return [row for row in instances if row.get("instance_id") != "overlay-1"], {}

    monkeypatch.setattr(instances_service_module, "apply_access_policy", _fake_apply_access_policy)

    result = await list_class_instances(
        db_name="demo_db",
        class_id="Order",
        request_headers={},
        base_branch="main",
        overlay_branch="writeback/main",
        branch=None,
        limit=2,
        offset=1,
        search=None,
        status_filter=None,
        action_type_id=None,
        submitted_by=None,
        elasticsearch_service=es,  # type: ignore[arg-type]
        dataset_registry=_FakeDatasetRegistry(policy=SimpleNamespace(policy={"kind": "stub"})),  # type: ignore[arg-type]
        action_logs=None,
    )

    assert result["total"] == 3
    assert [row["instance_id"] for row in result["instances"]] == ["base-1", "base-3"]
    assert len(es.search_calls) == 2
    assert all(call["index"] in {base_index, overlay_index} for call in es.search_calls)


@pytest.mark.asyncio
async def test_overlay_detail_hidden_by_policy_does_not_fall_back_to_base(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    es = _FakeElasticsearchService(
        search_result={
            "total": 1,
            "hits": [
                {
                    "class_id": "Order",
                    "instance_id": "inst-1",
                    "lifecycle_id": "lc-0",
                    "event_timestamp": "2026-01-01T00:00:00Z",
                    "data": {"order_id": "inst-1"},
                }
            ],
        },
        document_result={
            "class_id": "Order",
            "instance_id": "inst-1",
            "lifecycle_id": "lc-0",
            "event_timestamp": "2026-01-01T00:00:01Z",
            "data": {"order_id": "inst-1"},
        },
    )

    def _fake_apply_access_policy(instances, *, policy):  # noqa: ANN001
        _ = policy
        if instances and instances[0].get("event_timestamp") == "2026-01-01T00:00:01Z":
            return [], {}
        return instances, {}

    monkeypatch.setattr(instances_service_module, "apply_access_policy", _fake_apply_access_policy)

    with pytest.raises(HTTPException) as raised:
        await get_instance_detail(
            db_name="demo_db",
            class_id="Order",
            instance_id="inst-1",
            request_headers={},
            base_branch="main",
            overlay_branch="writeback/main",
            branch=None,
            elasticsearch_service=es,  # type: ignore[arg-type]
            dataset_registry=_FakeDatasetRegistry(policy=SimpleNamespace(policy={"kind": "stub"})),  # type: ignore[arg-type]
            action_logs=None,
        )

    assert raised.value.status_code == 404
