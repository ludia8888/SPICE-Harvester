"""Tests for relationship_reconciler_service."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Union
from unittest.mock import AsyncMock

import pytest

from bff.services.relationship_reconciler_service import (
    _bulk_update_relationships,
    _collect_relationships,
    _detect_fk_field,
    _scan_and_group_fk,
    reconcile_relationships,
)


class _FakeElasticsearchService:
    """Minimal fake for reconciler tests."""

    def __init__(self) -> None:
        self.updates: List[Dict[str, Any]] = []
        self._search_responses: List[Dict[str, Any]] = []
        self._refreshed: List[str] = []
        self._client = object()  # Pretend already connected

    async def connect(self) -> None:
        pass

    def add_search_response(self, resp: Dict[str, Any]) -> None:
        self._search_responses.append(resp)

    async def search(
        self,
        index: str,
        query: Optional[Dict[str, Any]] = None,
        size: int = 10,
        from_: int = 0,
        sort: Optional[List[Dict[str, Any]]] = None,
        source_includes: Optional[List[str]] = None,
        source_excludes: Optional[List[str]] = None,
        aggregations: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if self._search_responses:
            return self._search_responses.pop(0)
        return {"total": 0, "hits": []}

    async def update_document(
        self,
        index: str,
        doc_id: str,
        doc: Optional[Dict[str, Any]] = None,
        script: Optional[Dict[str, Any]] = None,
        upsert: Optional[Dict[str, Any]] = None,
        refresh: Union[bool, str] = False,
    ) -> bool:
        self.updates.append({"index": index, "doc_id": doc_id, "doc": doc})
        return True

    async def refresh_index(self, index: str) -> bool:
        self._refreshed.append(index)
        return True


def test_collect_relationships_extracts_from_class_defs() -> None:
    class_defs = {
        "Customer": {
            "id": "Customer",
            "relationships": [
                {"predicate": "orders", "target_class": "Order", "cardinality": "1:n"},
            ],
        },
        "Order": {
            "id": "Order",
            "relationships": [
                {"predicate": "payments", "target_class": "Payment", "cardinality": "1:n"},
            ],
        },
        "Payment": {"id": "Payment"},
    }

    rels = _collect_relationships(class_defs)
    assert len(rels) == 2
    predicates = {r["predicate"] for r in rels}
    assert predicates == {"orders", "payments"}


def test_collect_relationships_deduplicates() -> None:
    class_defs = {
        "Customer": {
            "relationships": [
                {"predicate": "orders", "target_class": "Order"},
                {"predicate": "orders", "target_class": "Order"},  # duplicate
            ],
        },
    }
    rels = _collect_relationships(class_defs)
    assert len(rels) == 1


@pytest.mark.asyncio
async def test_scan_and_group_fk() -> None:
    fake_es = _FakeElasticsearchService()
    fake_es.add_search_response({
        "total": 3,
        "hits": [
            {"_id": "ORD001", "_source": {"instance_id": "ORD001", "data": {"customer_id": "CUST001"}}},
            {"_id": "ORD002", "_source": {"instance_id": "ORD002", "data": {"customer_id": "CUST001"}}},
            {"_id": "ORD003", "_source": {"instance_id": "ORD003", "data": {"customer_id": "CUST002"}}},
        ],
    })
    # Empty second page to terminate
    fake_es.add_search_response({"total": 0, "hits": []})

    groups = await _scan_and_group_fk(
        index_name="test_instances",
        target_class="Order",
        fk_field="customer_id",
        es_service=fake_es,  # type: ignore[arg-type]
    )

    assert "CUST001" in groups
    assert set(groups["CUST001"]) == {"ORD001", "ORD002"}
    assert groups["CUST002"] == ["ORD003"]


@pytest.mark.asyncio
async def test_bulk_update_relationships() -> None:
    fake_es = _FakeElasticsearchService()
    fk_groups = {
        "CUST001": ["ORD001", "ORD002"],
        "CUST002": ["ORD003"],
    }

    updated = await _bulk_update_relationships(
        index_name="test_instances",
        source_class="Customer",
        predicate="orders",
        target_class="Order",
        fk_groups=fk_groups,
        es_service=fake_es,  # type: ignore[arg-type]
    )

    assert updated == 2
    assert len(fake_es.updates) == 2

    # Check CUST001 update
    cust001_update = next(u for u in fake_es.updates if u["doc_id"] == "CUST001")
    assert "Order/ORD001" in cust001_update["doc"]["relationships"]["orders"]
    assert "Order/ORD002" in cust001_update["doc"]["relationships"]["orders"]

    # Check CUST002 update
    cust002_update = next(u for u in fake_es.updates if u["doc_id"] == "CUST002")
    assert cust002_update["doc"]["relationships"]["orders"] == ["Order/ORD003"]


@pytest.mark.asyncio
async def test_detect_fk_field_via_pattern() -> None:
    fake_es = _FakeElasticsearchService()
    # Sample instance for Order class
    fake_es.add_search_response({
        "total": 1,
        "hits": [
            {"_id": "ORD001", "_source": {"instance_id": "ORD001", "data": {"customer_id": "CUST001", "amount": "100"}}},
        ],
    })

    result = await _detect_fk_field(
        source_class="Customer",
        target_class="Order",
        index_name="test_instances",
        es_service=fake_es,  # type: ignore[arg-type]
        objectify_registry=None,
        db_name="test_db",
    )

    assert result == "customer_id"


@pytest.mark.asyncio
async def test_reconcile_returns_no_classes_when_oms_empty() -> None:
    oms_client = AsyncMock()
    oms_client.list_ontologies.return_value = {"data": {"ontologies": []}}

    fake_es = _FakeElasticsearchService()

    result = await reconcile_relationships(
        db_name="test_db",
        branch="main",
        oms_client=oms_client,
        es_service=fake_es,  # type: ignore[arg-type]
    )

    assert result["status"] == "no_classes"
