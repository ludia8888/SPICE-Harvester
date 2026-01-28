"""
Unit tests for Optimistic Concurrency Control (OCC) patterns.
"""
from __future__ import annotations

import pytest
from shared.services.registries.objectify_registry import (
    OCCConflictError,
    ObjectifyJobRecord,
    OntologyMappingSpecRecord,
)


class TestOCCConflictError:
    def test_error_message(self) -> None:
        """OCCConflictError should include helpful details"""
        error = OCCConflictError(
            table="objectify_jobs",
            record_id="job-123",
            expected_version=1,
            actual_version=2,
        )

        assert error.table == "objectify_jobs"
        assert error.record_id == "job-123"
        assert error.expected_version == 1
        assert error.actual_version == 2
        assert "objectify_jobs" in str(error)
        assert "job-123" in str(error)
        assert "expected version 1" in str(error)
        assert "actual version 2" in str(error)

    def test_error_without_actual_version(self) -> None:
        """OCCConflictError should handle missing actual version"""
        error = OCCConflictError(
            table="pipelines",
            record_id="pipe-456",
            expected_version=5,
            actual_version=None,
        )

        assert error.actual_version is None
        assert "expected version 5" in str(error)


class TestObjectifyJobRecordOCC:
    def test_record_has_occ_version_default(self) -> None:
        """ObjectifyJobRecord should have occ_version field with default"""
        from datetime import datetime, timezone

        record = ObjectifyJobRecord(
            job_id="job-1",
            mapping_spec_id="spec-1",
            mapping_spec_version=1,
            dedupe_key=None,
            dataset_id="ds-1",
            dataset_version_id="v-1",
            artifact_id=None,
            artifact_output_name=None,
            dataset_branch="main",
            target_class_id="Product",
            status="PENDING",
            command_id=None,
            error=None,
            report={},
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            completed_at=None,
        )

        # Default occ_version should be 1
        assert record.occ_version == 1

    def test_record_with_explicit_occ_version(self) -> None:
        """ObjectifyJobRecord should accept explicit occ_version"""
        from datetime import datetime, timezone

        record = ObjectifyJobRecord(
            job_id="job-2",
            mapping_spec_id="spec-2",
            mapping_spec_version=1,
            dedupe_key=None,
            dataset_id="ds-2",
            dataset_version_id="v-2",
            artifact_id=None,
            artifact_output_name=None,
            dataset_branch="main",
            target_class_id="Order",
            status="RUNNING",
            command_id="cmd-1",
            error=None,
            report={"progress": 50},
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            completed_at=None,
            occ_version=5,
        )

        assert record.occ_version == 5


class TestOntologyMappingSpecRecordOCC:
    def test_record_has_occ_version_default(self) -> None:
        """OntologyMappingSpecRecord should have occ_version field"""
        from datetime import datetime, timezone

        record = OntologyMappingSpecRecord(
            mapping_spec_id="ms-1",
            dataset_id="ds-1",
            dataset_branch="main",
            artifact_output_name=None,
            schema_hash="abc123",
            backing_datasource_id=None,
            backing_datasource_version_id=None,
            target_class_id="Customer",
            mappings=[{"source_field": "id", "target_field": "customer_id"}],
            target_field_types={"customer_id": "xsd:integer"},
            status="ACTIVE",
            version=3,  # Business version
            auto_sync=True,
            options={},
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )

        # Default occ_version should be 1
        assert record.occ_version == 1
        # Business version is separate
        assert record.version == 3

    def test_record_with_explicit_occ_version(self) -> None:
        """OntologyMappingSpecRecord should accept explicit occ_version"""
        from datetime import datetime, timezone

        record = OntologyMappingSpecRecord(
            mapping_spec_id="ms-2",
            dataset_id="ds-2",
            dataset_branch="feature-x",
            artifact_output_name="output.csv",
            schema_hash="def456",
            backing_datasource_id=None,
            backing_datasource_version_id=None,
            target_class_id="Product",
            mappings=[],
            target_field_types={},
            status="INACTIVE",
            version=10,  # Business version
            auto_sync=False,
            options={"batch_size": 1000},
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            occ_version=7,  # OCC version
        )

        assert record.occ_version == 7
        assert record.version == 10  # Separate from occ_version


class TestOCCPatternIntegration:
    """Tests for OCC pattern usage scenarios"""

    def test_occ_conflict_is_runtime_error(self) -> None:
        """OCCConflictError should be a RuntimeError for easy catching"""
        error = OCCConflictError(
            table="test",
            record_id="123",
            expected_version=1,
            actual_version=2,
        )

        assert isinstance(error, RuntimeError)

    def test_occ_error_can_be_raised_and_caught(self) -> None:
        """OCCConflictError should be properly catchable"""
        with pytest.raises(OCCConflictError) as exc_info:
            raise OCCConflictError(
                table="objectify_jobs",
                record_id="job-abc",
                expected_version=3,
                actual_version=5,
            )

        error = exc_info.value
        assert error.expected_version == 3
        assert error.actual_version == 5
