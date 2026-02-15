"""Tests for shared relationship extractor."""

from __future__ import annotations

import pytest

from shared.services.core.relationship_extractor import extract_relationships


class TestExtractRelationshipsWithRelMap:
    """Test extraction using pre-parsed rel_map (objectify_worker fast path)."""

    def test_single_relationship(self) -> None:
        rel_map = {
            "placed_by": {"predicate": "placed_by", "target_class": "Customer", "cardinality": "1:1"},
        }
        payload = {
            "order_id": "ORD-001",
            "placed_by": "Customer/cust_abc",
            "total": 100.0,
        }
        result = extract_relationships(payload, rel_map=rel_map)
        assert result == {"placed_by": "Customer/cust_abc"}

    def test_many_relationship(self) -> None:
        rel_map = {
            "has_items": {"predicate": "has_items", "target_class": "OrderItem", "cardinality": "1:n"},
        }
        payload = {
            "order_id": "ORD-001",
            "has_items": ["OrderItem/item_1", "OrderItem/item_2"],
        }
        result = extract_relationships(payload, rel_map=rel_map)
        assert result == {"has_items": ["OrderItem/item_1", "OrderItem/item_2"]}

    def test_deduplicates_many(self) -> None:
        rel_map = {
            "tags": {"predicate": "tags", "cardinality": "1:n"},
        }
        payload = {
            "tags": ["Tag/a", "Tag/b", "Tag/a"],
        }
        result = extract_relationships(payload, rel_map=rel_map)
        assert result == {"tags": ["Tag/a", "Tag/b"]}

    def test_missing_field_skipped(self) -> None:
        rel_map = {
            "linked_to": {"predicate": "linked_to", "cardinality": "1:1"},
        }
        payload = {"order_id": "ORD-001"}
        result = extract_relationships(payload, rel_map=rel_map)
        assert result == {}

    def test_empty_rel_map(self) -> None:
        payload = {"order_id": "ORD-001", "placed_by": "Customer/cust_abc"}
        result = extract_relationships(payload, rel_map={}, allow_pattern_fallback=False)
        assert result == {}


class TestExtractRelationshipsWithOntologyData:
    """Test extraction using raw ontology data (instance_worker path)."""

    def test_oms_dict_format(self) -> None:
        ontology = {
            "relationships": [
                {"predicate": "placed_by", "target_class": "Customer", "cardinality": "1:1"},
            ],
        }
        payload = {"placed_by": "Customer/cust_1"}
        result = extract_relationships(payload, ontology_data=ontology, allow_pattern_fallback=False)
        assert result == {"placed_by": "Customer/cust_1"}

    def test_legacy_graph_schema_format(self) -> None:
        ontology = {
            "placed_by": {"@class": "Customer", "@type": "Optional"},
            "items": {"@class": "OrderItem", "@type": "Set"},
        }
        payload = {
            "placed_by": "Customer/cust_1",
            "items": ["OrderItem/item_1", "OrderItem/item_2"],
        }
        result = extract_relationships(payload, ontology_data=ontology, allow_pattern_fallback=False)
        assert result["placed_by"] == "Customer/cust_1"
        assert result["items"] == ["OrderItem/item_1", "OrderItem/item_2"]


class TestPatternFallback:
    """Test pattern-based relationship detection."""

    def test_detects_by_pattern(self) -> None:
        payload = {
            "order_id": "ORD-001",
            "placed_by": "Customer/cust_abc",
            "shipped_to": "Address/addr_1",
            "name": "Test Order",
        }
        result = extract_relationships(payload, allow_pattern_fallback=True)
        assert "placed_by" in result
        assert "shipped_to" in result
        assert "name" not in result
        assert "order_id" not in result

    def test_pattern_fallback_disabled(self) -> None:
        payload = {"placed_by": "Customer/cust_abc"}
        result = extract_relationships(payload, allow_pattern_fallback=False)
        assert result == {}

    def test_pattern_list_refs(self) -> None:
        payload = {
            "linked_to": ["Order/ord_1", "Order/ord_2"],
        }
        result = extract_relationships(payload, allow_pattern_fallback=True)
        assert result == {"linked_to": ["Order/ord_1", "Order/ord_2"]}


class TestEdgeCases:
    """Test error handling and edge cases."""

    def test_invalid_ref_format(self) -> None:
        rel_map = {"placed_by": {"predicate": "placed_by", "cardinality": "1:1"}}
        payload = {"placed_by": "not_a_valid_ref"}
        with pytest.raises(ValueError, match="Invalid relationship ref"):
            extract_relationships(payload, rel_map=rel_map, allow_pattern_fallback=False)

    def test_non_string_ref(self) -> None:
        rel_map = {"placed_by": {"predicate": "placed_by", "cardinality": "1:1"}}
        payload = {"placed_by": 12345}
        with pytest.raises(ValueError, match="must be a string"):
            extract_relationships(payload, rel_map=rel_map, allow_pattern_fallback=False)

    def test_single_cardinality_with_multiple_values(self) -> None:
        rel_map = {"placed_by": {"predicate": "placed_by", "cardinality": "1:1"}}
        payload = {"placed_by": ["Customer/a", "Customer/b"]}
        with pytest.raises(ValueError, match="expects a single ref"):
            extract_relationships(payload, rel_map=rel_map, allow_pattern_fallback=False)

    def test_single_cardinality_with_empty_list(self) -> None:
        rel_map = {"placed_by": {"predicate": "placed_by", "cardinality": "1:1"}}
        payload = {"placed_by": []}
        result = extract_relationships(payload, rel_map=rel_map, allow_pattern_fallback=False)
        assert result == {}

    def test_none_inputs(self) -> None:
        result = extract_relationships({}, rel_map=None, ontology_data=None, allow_pattern_fallback=False)
        assert result == {}
