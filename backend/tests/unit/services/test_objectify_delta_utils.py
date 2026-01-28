"""
Unit tests for Objectify Delta Utils.
"""
from __future__ import annotations

import pytest
from datetime import datetime
from shared.services.pipeline.objectify_delta_utils import (
    ObjectifyDeltaComputer,
    DeltaResult,
    WatermarkState,
    create_delta_computer_for_mapping_spec,
)


class TestDeltaResult:
    def test_default_stats(self) -> None:
        """DeltaResult should compute stats on creation"""
        result = DeltaResult(
            added_rows=[{"id": 1}, {"id": 2}],
            modified_rows=[{"id": 3}],
            deleted_keys=["4"],
        )

        assert result.stats["added_count"] == 2
        assert result.stats["modified_count"] == 1
        assert result.stats["deleted_count"] == 1
        assert result.stats["total_changes"] == 4

    def test_has_changes_true(self) -> None:
        """has_changes should be True when there are changes"""
        result = DeltaResult(added_rows=[{"id": 1}])
        assert result.has_changes is True

    def test_has_changes_false(self) -> None:
        """has_changes should be False when empty"""
        result = DeltaResult()
        assert result.has_changes is False


class TestWatermarkState:
    def test_to_dict(self) -> None:
        """to_dict should serialize watermark state"""
        state = WatermarkState(
            mapping_spec_id="ms_123",
            dataset_branch="main",
            watermark_column="updated_at",
            watermark_value="2024-01-01T00:00:00Z",
            lakefs_commit_id="abc123",
        )

        d = state.to_dict()

        assert d["mapping_spec_id"] == "ms_123"
        assert d["watermark_column"] == "updated_at"
        assert d["watermark_value"] == "2024-01-01T00:00:00Z"

    def test_from_dict(self) -> None:
        """from_dict should deserialize watermark state"""
        data = {
            "mapping_spec_id": "ms_123",
            "dataset_branch": "main",
            "watermark_column": "updated_at",
            "watermark_value": "2024-01-01T00:00:00Z",
            "lakefs_commit_id": "abc123",
            "last_processed_at": "2024-01-01T00:00:00+00:00",
        }

        state = WatermarkState.from_dict(data)

        assert state.mapping_spec_id == "ms_123"
        assert state.watermark_column == "updated_at"
        assert isinstance(state.last_processed_at, datetime)


class TestObjectifyDeltaComputer:
    def test_compute_row_key_single_pk(self) -> None:
        """compute_row_key should create key from single PK"""
        computer = ObjectifyDeltaComputer(pk_columns=["id"])

        key = computer.compute_row_key({"id": 123, "name": "test"})

        assert key == "123"

    def test_compute_row_key_composite_pk(self) -> None:
        """compute_row_key should create key from composite PK"""
        computer = ObjectifyDeltaComputer(pk_columns=["tenant_id", "user_id"])

        key = computer.compute_row_key({"tenant_id": "t1", "user_id": "u1", "name": "test"})

        assert key == "t1|u1"

    def test_compute_row_hash(self) -> None:
        """compute_row_hash should be stable"""
        computer = ObjectifyDeltaComputer(pk_columns=["id"])

        hash1 = computer.compute_row_hash({"id": 1, "name": "test"})
        hash2 = computer.compute_row_hash({"id": 1, "name": "test"})
        hash3 = computer.compute_row_hash({"id": 1, "name": "different"})

        assert hash1 == hash2  # Same content = same hash
        assert hash1 != hash3  # Different content = different hash

    def test_compare_watermarks_datetime(self) -> None:
        """_compare_watermarks should handle datetime"""
        computer = ObjectifyDeltaComputer(pk_columns=["id"])

        dt1 = datetime(2024, 1, 1, 0, 0, 0)
        dt2 = datetime(2024, 1, 2, 0, 0, 0)

        assert computer._compare_watermarks(dt2, dt1) > 0  # dt2 > dt1
        assert computer._compare_watermarks(dt1, dt2) < 0  # dt1 < dt2
        assert computer._compare_watermarks(dt1, dt1) == 0  # Equal

    def test_compare_watermarks_numeric(self) -> None:
        """_compare_watermarks should handle numeric values"""
        computer = ObjectifyDeltaComputer(pk_columns=["id"])

        assert computer._compare_watermarks(10, 5) > 0
        assert computer._compare_watermarks(5, 10) < 0
        assert computer._compare_watermarks(5, 5) == 0

    def test_compare_watermarks_string(self) -> None:
        """_compare_watermarks should handle string values"""
        computer = ObjectifyDeltaComputer(pk_columns=["id"])

        assert computer._compare_watermarks("b", "a") > 0
        assert computer._compare_watermarks("a", "b") < 0
        assert computer._compare_watermarks("a", "a") == 0

    def test_compare_watermarks_none_handling(self) -> None:
        """_compare_watermarks should handle None values"""
        computer = ObjectifyDeltaComputer(pk_columns=["id"])

        assert computer._compare_watermarks(None, None) == 0
        assert computer._compare_watermarks(None, 5) < 0
        assert computer._compare_watermarks(5, None) > 0

    def test_filter_rows_by_watermark(self) -> None:
        """filter_rows_by_watermark should filter correctly"""
        computer = ObjectifyDeltaComputer(pk_columns=["id"])

        rows = [
            {"id": 1, "updated_at": 5},
            {"id": 2, "updated_at": 10},
            {"id": 3, "updated_at": 15},
            {"id": 4, "updated_at": 20},
        ]

        filtered, max_wm = computer.filter_rows_by_watermark(
            rows=rows,
            watermark_column="updated_at",
            previous_watermark=10,
        )

        assert len(filtered) == 2  # Only rows with updated_at > 10
        assert filtered[0]["id"] == 3
        assert filtered[1]["id"] == 4
        assert max_wm == 20

    def test_filter_rows_by_watermark_empty(self) -> None:
        """filter_rows_by_watermark should handle no matches"""
        computer = ObjectifyDeltaComputer(pk_columns=["id"])

        rows = [
            {"id": 1, "updated_at": 5},
            {"id": 2, "updated_at": 10},
        ]

        filtered, max_wm = computer.filter_rows_by_watermark(
            rows=rows,
            watermark_column="updated_at",
            previous_watermark=100,  # All rows below this
        )

        assert len(filtered) == 0
        assert max_wm == 100  # Unchanged

    def test_filter_rows_by_watermark_null_values(self) -> None:
        """filter_rows_by_watermark should skip null watermark values"""
        computer = ObjectifyDeltaComputer(pk_columns=["id"])

        rows = [
            {"id": 1, "updated_at": None},
            {"id": 2, "updated_at": 10},
            {"id": 3, "updated_at": 15},
        ]

        filtered, max_wm = computer.filter_rows_by_watermark(
            rows=rows,
            watermark_column="updated_at",
            previous_watermark=5,
        )

        # Should skip null value row
        assert len(filtered) == 2
        ids = [r["id"] for r in filtered]
        assert 1 not in ids  # Null watermark excluded


class TestCreateDeltaComputerForMappingSpec:
    def test_creates_computer_with_pk_from_spec(self) -> None:
        """Should extract pk_columns from mapping_spec"""
        mapping_spec = {
            "pk_spec": {
                "columns": ["customer_id", "order_id"],
            },
            "batch_size": 5000,
        }

        computer = create_delta_computer_for_mapping_spec(mapping_spec)

        assert computer.pk_columns == ["customer_id", "order_id"]
        assert computer.batch_size == 5000

    def test_fallback_to_id_field(self) -> None:
        """Should fall back to 'id' field if no pk_spec"""
        mapping_spec = {
            "field_mappings": [
                {"source_field": "id"},
                {"source_field": "name"},
            ],
        }

        computer = create_delta_computer_for_mapping_spec(mapping_spec)

        assert computer.pk_columns == ["id"]

    def test_fallback_to_first_field(self) -> None:
        """Should fall back to first field if no id column"""
        mapping_spec = {
            "field_mappings": [
                {"source_field": "customer_code"},
                {"source_field": "name"},
            ],
        }

        computer = create_delta_computer_for_mapping_spec(mapping_spec)

        assert computer.pk_columns == ["customer_code"]
