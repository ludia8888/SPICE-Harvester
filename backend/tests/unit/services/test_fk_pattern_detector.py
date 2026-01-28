"""
Unit tests for FK Pattern Detector service.
"""
from __future__ import annotations

import pytest
from shared.services.pipeline.fk_pattern_detector import (
    ForeignKeyPatternDetector,
    FKDetectionConfig,
    ForeignKeyPattern,
    TargetCandidate,
)


class TestFKDetectionConfig:
    def test_default_config(self) -> None:
        config = FKDetectionConfig()
        assert config.value_overlap_threshold == 0.7
        assert config.sample_size == 1000
        assert config.min_confidence == 0.5
        assert "_id$" in config.naming_rules

    def test_from_dict(self) -> None:
        config = FKDetectionConfig.from_dict({
            "value_overlap_threshold": 0.8,
            "min_confidence": 0.6,
            "naming_rules": {"_fk$": 0.99},
        })
        assert config.value_overlap_threshold == 0.8
        assert config.min_confidence == 0.6
        assert "_fk$" in config.naming_rules


class TestForeignKeyPatternDetector:
    def test_detect_by_naming_id_suffix(self) -> None:
        """_id suffix columns should be detected as FK candidates"""
        detector = ForeignKeyPatternDetector()
        schema = [
            {"name": "id", "type": "xsd:integer"},
            {"name": "customer_id", "type": "xsd:integer"},
            {"name": "name", "type": "xsd:string"},
        ]

        patterns = detector.detect_patterns(
            source_dataset_id="ds_orders",
            source_schema=schema,
        )

        assert len(patterns) >= 1
        customer_pattern = next((p for p in patterns if p.source_column == "customer_id"), None)
        assert customer_pattern is not None
        assert customer_pattern.confidence > 0.5
        assert customer_pattern.detection_method == "naming_convention"

    def test_detect_by_naming_fk_suffix(self) -> None:
        """_fk suffix columns should be detected with high confidence"""
        detector = ForeignKeyPatternDetector()
        schema = [
            {"name": "product_fk", "type": "xsd:string"},
        ]

        patterns = detector.detect_patterns(
            source_dataset_id="ds_line_items",
            source_schema=schema,
        )

        assert len(patterns) == 1
        assert patterns[0].source_column == "product_fk"
        assert patterns[0].confidence >= 0.7  # _fk has high base confidence

    def test_exclude_timestamp_columns(self) -> None:
        """Timestamp columns should be excluded from FK detection"""
        detector = ForeignKeyPatternDetector()
        schema = [
            {"name": "created_at", "type": "xsd:dateTime"},
            {"name": "updated_at", "type": "xsd:dateTime"},
            {"name": "customer_id", "type": "xsd:integer"},
        ]

        patterns = detector.detect_patterns(
            source_dataset_id="ds_orders",
            source_schema=schema,
        )

        # Only customer_id should be detected, not timestamps
        columns = [p.source_column for p in patterns]
        assert "created_at" not in columns
        assert "updated_at" not in columns
        assert "customer_id" in columns

    def test_exclude_boolean_prefixes(self) -> None:
        """Boolean-like columns should be excluded"""
        detector = ForeignKeyPatternDetector()
        schema = [
            {"name": "is_active", "type": "xsd:boolean"},
            {"name": "has_discount", "type": "xsd:boolean"},
            {"name": "customer_id", "type": "xsd:integer"},
        ]

        patterns = detector.detect_patterns(
            source_dataset_id="ds_orders",
            source_schema=schema,
        )

        columns = [p.source_column for p in patterns]
        assert "is_active" not in columns
        assert "has_discount" not in columns

    def test_match_with_target_candidates(self) -> None:
        """FK should match with target candidates when name matches"""
        detector = ForeignKeyPatternDetector()
        schema = [
            {"name": "customer_id", "type": "xsd:integer"},
        ]
        target_candidates = [
            TargetCandidate(
                candidate_type="dataset",
                candidate_id="ds_customers",
                candidate_name="customers",
                pk_columns=["id"],
            ),
        ]

        patterns = detector.detect_patterns(
            source_dataset_id="ds_orders",
            source_schema=schema,
            target_candidates=target_candidates,
        )

        assert len(patterns) == 1
        assert patterns[0].target_dataset_id == "ds_customers"
        assert patterns[0].target_pk_field == "id"
        assert patterns[0].confidence > 0.7  # Boost for target match

    def test_match_plural_target_name(self) -> None:
        """FK should match with pluralized target names"""
        detector = ForeignKeyPatternDetector()
        schema = [
            {"name": "product_id", "type": "xsd:integer"},
        ]
        target_candidates = [
            TargetCandidate(
                candidate_type="dataset",
                candidate_id="ds_products",
                candidate_name="products",  # Plural
                pk_columns=["id"],
            ),
        ]

        patterns = detector.detect_patterns(
            source_dataset_id="ds_orders",
            source_schema=schema,
            target_candidates=target_candidates,
        )

        assert len(patterns) == 1
        assert patterns[0].target_dataset_id == "ds_products"

    def test_min_confidence_filter(self) -> None:
        """Patterns below min_confidence should be filtered out"""
        config = FKDetectionConfig(min_confidence=0.99)
        detector = ForeignKeyPatternDetector(config)
        schema = [
            {"name": "customer_id", "type": "xsd:integer"},
        ]

        patterns = detector.detect_patterns(
            source_dataset_id="ds_orders",
            source_schema=schema,
        )

        # customer_id without target match has lower confidence
        assert len(patterns) == 0

    def test_suggest_link_type(self) -> None:
        """suggest_link_type should generate valid link_type structure"""
        detector = ForeignKeyPatternDetector()
        pattern = ForeignKeyPattern(
            source_dataset_id="ds_orders",
            source_column="customer_id",
            target_dataset_id="ds_customers",
            target_object_type="Customer",
            target_pk_field="id",
            confidence=0.9,
            detection_method="naming_convention",
            reasons=["Column name matches FK pattern"],
        )

        suggestion = detector.suggest_link_type(pattern, source_object_type="Order")

        assert suggestion["suggested_link_type"]["predicate"] == "hasCustomer"
        assert suggestion["suggested_link_type"]["source_class"] == "Order"
        assert suggestion["suggested_link_type"]["target_class"] == "Customer"
        assert suggestion["suggested_link_type"]["cardinality"] == "n:1"
        assert suggestion["relationship_spec"]["spec_type"] == "foreign_key"

    def test_generate_predicate_from_column(self) -> None:
        """Predicate generation should follow naming convention"""
        detector = ForeignKeyPatternDetector()

        assert detector._generate_predicate("customer_id") == "hasCustomer"
        assert detector._generate_predicate("order_fk") == "hasOrder"
        assert detector._generate_predicate("product_key") == "hasProduct"
        assert detector._generate_predicate("user") == "hasUser"

    def test_incompatible_types_excluded(self) -> None:
        """Non-FK-compatible types should be excluded"""
        detector = ForeignKeyPatternDetector()
        schema = [
            {"name": "customer_id", "type": "xsd:boolean"},  # Boolean can't be FK
            {"name": "order_id", "type": "xsd:integer"},     # Integer can be FK
        ]

        patterns = detector.detect_patterns(
            source_dataset_id="ds_line_items",
            source_schema=schema,
        )

        columns = [p.source_column for p in patterns]
        assert "customer_id" not in columns  # Boolean excluded
        assert "order_id" in columns

    def test_empty_schema(self) -> None:
        """Empty schema should return no patterns"""
        detector = ForeignKeyPatternDetector()
        patterns = detector.detect_patterns(
            source_dataset_id="ds_empty",
            source_schema=[],
        )
        assert patterns == []

    def test_patterns_sorted_by_confidence(self) -> None:
        """Patterns should be sorted by confidence descending"""
        detector = ForeignKeyPatternDetector()
        schema = [
            {"name": "customer_id", "type": "xsd:integer"},
            {"name": "product_fk", "type": "xsd:integer"},  # _fk has higher confidence
        ]

        patterns = detector.detect_patterns(
            source_dataset_id="ds_orders",
            source_schema=schema,
        )

        if len(patterns) >= 2:
            assert patterns[0].confidence >= patterns[1].confidence
