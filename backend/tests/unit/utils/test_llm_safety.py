"""
Tests for shared/utils/llm_safety.py

Covers:
- PII masking
- Raw Data-Centric Agent support functions (2026-01)
"""
from __future__ import annotations

import pytest

from shared.utils.llm_safety import (
    build_column_semantic_observations,
    build_relationship_observations,
    detect_value_pattern,
    extract_column_value_patterns,
    mask_pii,
    mask_pii_text,
)


# ==============================================================================
# Basic PII Masking Tests
# ==============================================================================


@pytest.mark.unit
def test_mask_pii_text_masks_email():
    text = "Contact: alice@example.com or bob@test.org"
    masked = mask_pii_text(text)
    assert "alice@example.com" not in masked
    assert "bob@test.org" not in masked
    assert "<email:" in masked


@pytest.mark.unit
def test_mask_pii_text_preserves_uuid():
    """UUIDs should NOT be masked (they're identifiers, not PII)."""
    text = "ID: 12345678-1234-1234-1234-123456789abc"
    masked = mask_pii_text(text)
    assert "12345678-1234-1234-1234-123456789abc" in masked


@pytest.mark.unit
def test_mask_pii_dict():
    data = {
        "user": {"email": "test@example.com", "phone": "123-456-7890"},
        "uuid": "12345678-1234-1234-1234-123456789abc",
    }
    masked = mask_pii(data)
    assert "<email:" in masked["user"]["email"]
    assert "12345678-1234-1234-1234-123456789abc" in masked["uuid"]


# ==============================================================================
# Raw Data-Centric: Value Pattern Detection
# ==============================================================================


@pytest.mark.unit
class TestDetectValuePattern:
    def test_detects_uuid(self):
        assert detect_value_pattern("12345678-1234-1234-1234-123456789abc") == "uuid"
        assert detect_value_pattern("AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE") == "uuid"

    def test_detects_sequential_id(self):
        assert detect_value_pattern("USR-001") == "sequential_id"
        assert detect_value_pattern("INV00123") == "sequential_id"
        assert detect_value_pattern("ORD-99999") == "sequential_id"

    def test_detects_iso_date(self):
        assert detect_value_pattern("2024-01-15") == "iso_date"
        assert detect_value_pattern("2024-01-15T14:30:00") == "iso_datetime"

    def test_detects_ambiguous_dates(self):
        assert detect_value_pattern("01/15/2024") == "us_date_format"
        assert detect_value_pattern("15.01.2024") == "eu_date_format"

    def test_detects_email(self):
        assert detect_value_pattern("alice@example.com") == "email"

    def test_detects_boolean_string(self):
        assert detect_value_pattern("true") == "boolean_string"
        assert detect_value_pattern("FALSE") == "boolean_string"
        assert detect_value_pattern("yes") == "boolean_string"

    def test_detects_numeric_strings(self):
        # Note: "12345" matches sequential_id pattern (0-5 letters + 3+ digits)
        # Pure integers need context or format that doesn't match sequential_id
        assert detect_value_pattern("123.45") == "decimal_string"
        assert detect_value_pattern("-99.9") == "decimal_string"
        assert detect_value_pattern("99") == "integer_string"  # Too short for sequential_id

    def test_detects_native_types(self):
        assert detect_value_pattern(None) == "null"
        assert detect_value_pattern(True) == "boolean"
        assert detect_value_pattern(42) == "integer"
        assert detect_value_pattern(3.14) == "decimal"

    def test_detects_url(self):
        assert detect_value_pattern("https://example.com/path") == "url"
        assert detect_value_pattern("http://api.test.org") == "url"

    def test_detects_short_codes(self):
        assert detect_value_pattern("USA") == "code_short"
        assert detect_value_pattern("KR") == "code_short"

    def test_detects_identifier_like(self):
        assert detect_value_pattern("STATUS_ACTIVE") == "identifier_like"
        assert detect_value_pattern("ENUM_VALUE") == "identifier_like"

    def test_free_text_fallback(self):
        assert detect_value_pattern("Hello World") == "free_text"
        assert detect_value_pattern("Some random text here") == "free_text"


# ==============================================================================
# Raw Data-Centric: Column Pattern Extraction
# ==============================================================================


@pytest.mark.unit
class TestExtractColumnValuePatterns:
    def test_uuid_column(self):
        values = [
            "12345678-1234-1234-1234-123456789abc",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "11111111-2222-3333-4444-555555555555",
        ]
        result = extract_column_value_patterns(values)

        assert result["dominant_pattern"] == "uuid"
        assert result["is_homogeneous"] is True
        assert "uuid" in result["pattern_distribution"]
        assert any("UUID" in obs for obs in result["observations"])

    def test_sequential_id_column(self):
        values = ["USR-001", "USR-002", "USR-003", None, "USR-005"]
        result = extract_column_value_patterns(values)

        assert result["dominant_pattern"] == "sequential_id"
        assert result["null_ratio"] > 0

    def test_mixed_patterns_column(self):
        values = ["uuid-like", "12345", "hello", None, "2024-01-01"]
        result = extract_column_value_patterns(values)

        assert result["is_homogeneous"] is False
        # Should have multiple patterns detected

    def test_empty_values(self):
        result = extract_column_value_patterns([])

        assert result["total_analyzed"] == 0
        assert result["dominant_pattern"] is None

    def test_provides_examples(self):
        values = ["A", "B", "C"]
        result = extract_column_value_patterns(values)

        # Should have examples in pattern distribution
        for pattern_data in result["pattern_distribution"].values():
            assert "examples" in pattern_data


# ==============================================================================
# Raw Data-Centric: Column Semantic Observations
# ==============================================================================


@pytest.mark.unit
class TestBuildColumnSemanticObservations:
    def test_id_column_with_uuid_values(self):
        values = [
            "12345678-1234-1234-1234-123456789abc",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        ]
        result = build_column_semantic_observations("user_id", values)

        assert "likely_identifier" in result["semantic_hints"]
        assert any("UUID" in obs.upper() or "uuid" in obs for obs in result["combined_observations"])
        # Cross-reference should confirm
        assert any("✓" in obs for obs in result["cross_observations"])

    def test_created_at_column(self):
        values = ["2024-01-15T14:30:00", "2024-01-16T09:00:00"]
        result = build_column_semantic_observations("created_at", values)

        assert "likely_temporal" in result["semantic_hints"]

    def test_foreign_key_pattern(self):
        values = ["1", "2", "3"]
        result = build_column_semantic_observations("order_id", values)

        assert "possible_foreign_key" in result["semantic_hints"]
        assert any("FK" in obs or "reference" in obs for obs in result["name_observations"])

    def test_sensitive_column_warning(self):
        values = ["alice@example.com", "bob@test.com"]
        result = build_column_semantic_observations("email", values)

        assert "possibly_sensitive" in result["semantic_hints"]

    def test_status_column_categorical(self):
        values = ["ACTIVE", "INACTIVE", "PENDING"]
        result = build_column_semantic_observations("status", values)

        assert "likely_categorical" in result["semantic_hints"]

    def test_boolean_flag_column(self):
        values = ["true", "false", "true"]
        result = build_column_semantic_observations("is_active", values)

        assert "likely_boolean" in result["semantic_hints"]


# ==============================================================================
# Raw Data-Centric: Relationship Observations
# ==============================================================================


@pytest.mark.unit
class TestBuildRelationshipObservations:
    def test_high_overlap_relationship(self):
        left_values = ["A", "B", "C", "D"]
        right_values = ["A", "B", "C", "D", "E"]

        result = build_relationship_observations(
            "left_col", "right_col", left_values, right_values
        )

        assert result["overlap_metrics"]["intersection_count"] == 4
        assert result["overlap_metrics"]["left_containment"] >= 0.8

    def test_no_overlap_relationship(self):
        left_values = ["A", "B", "C"]
        right_values = ["X", "Y", "Z"]

        result = build_relationship_observations(
            "left_col", "right_col", left_values, right_values
        )

        assert result["overlap_metrics"]["intersection_count"] == 0
        assert any("NO VALUE OVERLAP" in obs for obs in result["observations"])

    def test_one_to_many_hint(self):
        # Left is unique (parent), right has duplicates (child)
        left_values = ["1", "2", "3", "4", "5"]
        right_values = ["1", "1", "2", "2", "3", "3"]

        result = build_relationship_observations(
            "parent_id", "child_fk", left_values, right_values
        )

        assert result["uniqueness_metrics"]["left_unique_ratio"] >= 0.9
        assert result["uniqueness_metrics"]["right_unique_ratio"] < 0.7

    def test_null_handling(self):
        left_values = ["A", None, "B", None]
        right_values = ["A", "B", "C"]

        result = build_relationship_observations(
            "left_col", "right_col", left_values, right_values
        )

        assert result["null_metrics"]["left_null_ratio"] == 0.5
        assert result["null_metrics"]["right_null_ratio"] == 0.0

    def test_provides_agent_instruction(self):
        result = build_relationship_observations(
            "col_a", "col_b", ["X"], ["X"]
        )

        assert "agent_instruction" in result
        assert "domain knowledge" in result["agent_instruction"]

    def test_non_matching_examples(self):
        left_values = ["A", "B", "UNIQUE_LEFT"]
        right_values = ["A", "B", "UNIQUE_RIGHT"]

        result = build_relationship_observations(
            "left_col", "right_col", left_values, right_values
        )

        # Note: normalize() lowercases values for comparison
        assert "unique_left" in result["non_matching_examples"]["left_only"]
        assert "unique_right" in result["non_matching_examples"]["right_only"]
