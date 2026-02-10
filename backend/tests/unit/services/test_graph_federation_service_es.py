"""Tests for GraphFederationServiceES — ES-native Search Arounds."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from shared.services.core.graph_federation_service_es import GraphFederationServiceES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_es_service() -> AsyncMock:
    es = AsyncMock()
    es.client = AsyncMock()
    return es


def _make_doc(class_id: str, instance_id: str, relationships: dict = None) -> dict:
    return {
        "class_id": class_id,
        "instance_id": instance_id,
        "relationships": relationships or {},
        "data": {"some_field": "value"},
    }


def _mget_response(docs):
    """Build an ES _mget response from a list of source dicts."""
    return {
        "docs": [
            {"found": True, "_source": doc} for doc in docs
        ]
    }


# ---------------------------------------------------------------------------
# _normalize_hops
# ---------------------------------------------------------------------------


def test_normalize_hops_dict():
    result = GraphFederationServiceES._normalize_hops([
        {"predicate": "orders", "target_class": "Order"},
        {"predicate": "payments", "target_class": "Payment", "reverse": True},
    ])
    assert result == [("orders", "Order", False), ("payments", "Payment", True)]


def test_normalize_hops_tuple():
    result = GraphFederationServiceES._normalize_hops([
        ("orders", "Order"),
        ("payments", "Payment", True),
    ])
    assert result == [("orders", "Order", False), ("payments", "Payment", True)]


def test_normalize_hops_empty():
    assert GraphFederationServiceES._normalize_hops([]) == []
    assert GraphFederationServiceES._normalize_hops(None) == []


# ---------------------------------------------------------------------------
# _node_id / _make_node / _parse_ref
# ---------------------------------------------------------------------------


def test_node_id():
    doc = _make_doc("Customer", "cust_1")
    assert GraphFederationServiceES._node_id(doc) == "Customer/cust_1"


def test_parse_ref():
    assert GraphFederationServiceES._parse_ref("Order/order_1") == ("Order", "order_1")
    assert GraphFederationServiceES._parse_ref("noclass") == ("", "noclass")


def test_get_relationship_refs():
    doc = _make_doc("Customer", "c1", {"orders": ["Order/o1", "Order/o2"], "account": "Account/a1"})
    assert GraphFederationServiceES._get_relationship_refs(doc, "orders") == ["Order/o1", "Order/o2"]
    assert GraphFederationServiceES._get_relationship_refs(doc, "account") == ["Account/a1"]
    assert GraphFederationServiceES._get_relationship_refs(doc, "missing") == []


# ---------------------------------------------------------------------------
# Forward single hop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_forward_single_hop():
    """Customer -> orders -> Order"""
    es = _make_es_service()

    customer = _make_doc("Customer", "cust_1", {"orders": ["Order/o1", "Order/o2"]})
    order1 = _make_doc("Order", "o1", {})
    order2 = _make_doc("Order", "o2", {})

    es.search = AsyncMock(return_value={"hits": [customer], "total": 1})
    es.client.mget = AsyncMock(return_value=_mget_response([order1, order2]))

    svc = GraphFederationServiceES(es_service=es)

    with patch("shared.services.core.graph_federation_service_es.get_settings") as mock_settings:
        gq = MagicMock()
        gq.max_hops = 5
        gq.max_limit = 1000
        gq.max_paths = 100
        mock_settings.return_value.graph_query = gq

        result = await svc.multi_hop_query(
            db_name="test_db",
            start_class="Customer",
            hops=[{"predicate": "orders", "target_class": "Order"}],
            limit=10,
        )

    assert result["count"] == 3  # 1 customer + 2 orders
    node_ids = {n["id"] for n in result["nodes"]}
    assert "Customer/cust_1" in node_ids
    assert "Order/o1" in node_ids
    assert "Order/o2" in node_ids
    assert len(result["edges"]) == 2


# ---------------------------------------------------------------------------
# Reverse single hop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reverse_single_hop():
    """Order -> (reverse) orders -> Customer"""
    es = _make_es_service()

    order = _make_doc("Order", "o1", {})
    customer = _make_doc("Customer", "cust_1", {"orders": ["Order/o1"]})

    # Hop 0: search Order
    # Hop 1 (reverse): search Customer where relationships.orders contains "Order/o1"
    es.search = AsyncMock(side_effect=[
        {"hits": [order], "total": 1},
        {"hits": [customer], "total": 1},
    ])

    svc = GraphFederationServiceES(es_service=es)

    with patch("shared.services.core.graph_federation_service_es.get_settings") as mock_settings:
        gq = MagicMock()
        gq.max_hops = 5
        gq.max_limit = 1000
        gq.max_paths = 100
        mock_settings.return_value.graph_query = gq

        result = await svc.multi_hop_query(
            db_name="test_db",
            start_class="Order",
            hops=[{"predicate": "orders", "target_class": "Customer", "reverse": True}],
            limit=10,
        )

    assert result["count"] == 2
    node_ids = {n["id"] for n in result["nodes"]}
    assert "Order/o1" in node_ids
    assert "Customer/cust_1" in node_ids


# ---------------------------------------------------------------------------
# Multi-hop 3 layers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multi_hop_3_layers():
    """Customer -> orders -> Order -> payments -> Payment"""
    es = _make_es_service()

    customer = _make_doc("Customer", "c1", {"orders": ["Order/o1"]})
    order = _make_doc("Order", "o1", {"payments": ["Payment/p1", "Payment/p2"]})
    pay1 = _make_doc("Payment", "p1", {})
    pay2 = _make_doc("Payment", "p2", {})

    es.search = AsyncMock(return_value={"hits": [customer], "total": 1})
    es.client.mget = AsyncMock(side_effect=[
        _mget_response([order]),     # hop 1: orders
        _mget_response([pay1, pay2]),  # hop 2: payments
    ])

    svc = GraphFederationServiceES(es_service=es)

    with patch("shared.services.core.graph_federation_service_es.get_settings") as mock_settings:
        gq = MagicMock()
        gq.max_hops = 5
        gq.max_limit = 1000
        gq.max_paths = 100
        mock_settings.return_value.graph_query = gq

        result = await svc.multi_hop_query(
            db_name="test_db",
            start_class="Customer",
            hops=[
                {"predicate": "orders", "target_class": "Order"},
                {"predicate": "payments", "target_class": "Payment"},
            ],
            limit=10,
        )

    assert result["count"] == 4  # c1 + o1 + p1 + p2
    assert len(result["edges"]) == 3  # c1->o1, o1->p1, o1->p2


# ---------------------------------------------------------------------------
# Fan-out cap
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fan_out_cap():
    """Verify max_fan_out caps the number of targets fetched."""
    es = _make_es_service()

    # Customer with 50 orders
    order_refs = [f"Order/o{i}" for i in range(50)]
    customer = _make_doc("Customer", "c1", {"orders": order_refs})

    es.search = AsyncMock(return_value={"hits": [customer], "total": 1})

    # mget should receive at most max_fan_out IDs
    fetched_orders = [_make_doc("Order", f"o{i}", {}) for i in range(10)]
    es.client.mget = AsyncMock(return_value=_mget_response(fetched_orders))

    svc = GraphFederationServiceES(es_service=es)

    with patch("shared.services.core.graph_federation_service_es._DEFAULT_MAX_FAN_OUT", 10):
        with patch("shared.services.core.graph_federation_service_es.get_settings") as mock_settings:
            gq = MagicMock()
            gq.max_hops = 5
            gq.max_limit = 1000
            gq.max_paths = 100
            mock_settings.return_value.graph_query = gq

            result = await svc.multi_hop_query(
                db_name="test_db",
                start_class="Customer",
                hops=[{"predicate": "orders", "target_class": "Order"}],
                limit=10,
            )

    # mget called with at most 10 IDs (not 50)
    mget_call = es.client.mget.call_args
    ids_sent = mget_call.kwargs.get("body", {}).get("ids", [])
    assert len(ids_sent) <= 10


# ---------------------------------------------------------------------------
# No cycles
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_cycles():
    """A -> B -> A should be filtered when no_cycles=True."""
    es = _make_es_service()

    a = _make_doc("A", "a1", {"link": ["B/b1"]})
    b = _make_doc("B", "b1", {"link": ["A/a1"]})

    es.search = AsyncMock(return_value={"hits": [a], "total": 1})
    es.client.mget = AsyncMock(side_effect=[
        _mget_response([b]),  # hop 1: A->B
        _mget_response([a]),  # hop 2: B->A (should be filtered)
    ])

    svc = GraphFederationServiceES(es_service=es)

    with patch("shared.services.core.graph_federation_service_es.get_settings") as mock_settings:
        gq = MagicMock()
        gq.max_hops = 5
        gq.max_limit = 1000
        gq.max_paths = 100
        mock_settings.return_value.graph_query = gq

        result = await svc.multi_hop_query(
            db_name="test_db",
            start_class="A",
            hops=[
                {"predicate": "link", "target_class": "B"},
                {"predicate": "link", "target_class": "A"},
            ],
            no_cycles=True,
            limit=10,
        )

    # A/a1 should appear only once, not re-added in hop 2
    node_ids = [n["id"] for n in result["nodes"]]
    assert node_ids.count("A/a1") == 1
    assert "B/b1" in node_ids


# ---------------------------------------------------------------------------
# Empty relationships
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_relationships():
    """Docs with no relationships yield no targets."""
    es = _make_es_service()

    customer = _make_doc("Customer", "c1", {})  # no relationships
    es.search = AsyncMock(return_value={"hits": [customer], "total": 1})
    es.client.mget = AsyncMock(return_value=_mget_response([]))

    svc = GraphFederationServiceES(es_service=es)

    with patch("shared.services.core.graph_federation_service_es.get_settings") as mock_settings:
        gq = MagicMock()
        gq.max_hops = 5
        gq.max_limit = 1000
        gq.max_paths = 100
        mock_settings.return_value.graph_query = gq

        result = await svc.multi_hop_query(
            db_name="test_db",
            start_class="Customer",
            hops=[{"predicate": "orders", "target_class": "Order"}],
            limit=10,
        )

    assert result["count"] == 1  # only the customer
    assert len(result["edges"]) == 0


# ---------------------------------------------------------------------------
# Simple graph query (single class)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_simple_graph_query():
    es = _make_es_service()
    docs = [_make_doc("Order", f"o{i}", {}) for i in range(3)]
    es.search = AsyncMock(return_value={"hits": docs, "total": 3})

    svc = GraphFederationServiceES(es_service=es)

    result = await svc.simple_graph_query(
        db_name="test_db",
        class_name="Order",
    )

    assert result["count"] == 3
    assert len(result["nodes"]) == 3
    assert len(result["edges"]) == 0


# ---------------------------------------------------------------------------
# find_relationship_paths via ES sampling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_find_paths_es_sampling():
    """Discover Customer -> Order -> Payment path via ES doc sampling."""
    es = _make_es_service()

    # Step 1: class aggregation
    es.search = AsyncMock(side_effect=[
        # aggregation query
        {
            "hits": [], "total": 0,
            "aggregations": {
                "classes": {
                    "buckets": [
                        {"key": "Customer"}, {"key": "Order"}, {"key": "Payment"},
                    ]
                }
            },
        },
        # Customer sample
        {"hits": [_make_doc("Customer", "c1", {"orders": ["Order/o1"]})], "total": 1},
        # Order sample
        {"hits": [_make_doc("Order", "o1", {"payments": ["Payment/p1"]})], "total": 1},
        # Payment sample
        {"hits": [_make_doc("Payment", "p1", {})], "total": 1},
    ])

    svc = GraphFederationServiceES(es_service=es)

    paths = await svc.find_relationship_paths(
        db_name="test_db",
        source_class="Customer",
        target_class="Payment",
        max_depth=3,
    )

    assert len(paths) == 1
    assert paths[0] == [
        {"from": "Customer", "predicate": "orders", "to": "Order"},
        {"from": "Order", "predicate": "payments", "to": "Payment"},
    ]


# ---------------------------------------------------------------------------
# Empty start class
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_start_returns_empty():
    es = _make_es_service()
    es.search = AsyncMock(return_value={"hits": [], "total": 0})

    svc = GraphFederationServiceES(es_service=es)

    with patch("shared.services.core.graph_federation_service_es.get_settings") as mock_settings:
        gq = MagicMock()
        gq.max_hops = 5
        gq.max_limit = 1000
        gq.max_paths = 100
        mock_settings.return_value.graph_query = gq

        result = await svc.multi_hop_query(
            db_name="test_db",
            start_class="Customer",
            hops=[{"predicate": "orders", "target_class": "Order"}],
            limit=10,
        )

    assert result["count"] == 0
    assert result["nodes"] == []
    assert result["edges"] == []
