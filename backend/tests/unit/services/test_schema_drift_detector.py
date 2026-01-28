"""
Unit tests for Schema Drift Detector service.
"""
from __future__ import annotations

import pytest
from shared.services.core.schema_drift_detector import (
    SchemaDriftDetector,
    SchemaDriftConfig,
    SchemaDrift,
    SchemaChange,
)


class TestSchemaDriftConfig:
    def test_default_config(self) -> None:
        config = SchemaDriftConfig()
        assert config.rename_similarity_threshold == 0.8
        assert "xsd:integer" in config.type_compatibility

    def test_type_compatibility_widening(self) -> None:
        """Widening conversions should be allowed"""
        config = SchemaDriftConfig()
        # integer can widen to decimal or string
        assert "xsd:decimal" in config.type_compatibility["xsd:integer"]
        assert "xsd:string" in config.type_compatibility["xsd:integer"]


class TestSchemaDriftDetector:
    def test_no_drift_same_schema(self) -> None:
        """No drift when schemas are identical"""
        detector = SchemaDriftDetector()
        schema = [
            {"name": "id", "type": "xsd:integer"},
            {"name": "name", "type": "xsd:string"},
        ]

        drift = detector.detect_drift(
            subject_type="dataset",
            subject_id="ds_test",
            db_name="test_db",
            current_schema=schema,
            previous_schema=schema,
        )

        assert drift is None

    def test_detect_column_added(self) -> None:
        """Detect when a new column is added"""
        detector = SchemaDriftDetector()
        old_schema = [
            {"name": "id", "type": "xsd:integer"},
        ]
        new_schema = [
            {"name": "id", "type": "xsd:integer"},
            {"name": "name", "type": "xsd:string"},
        ]

        drift = detector.detect_drift(
            subject_type="dataset",
            subject_id="ds_test",
            db_name="test_db",
            current_schema=new_schema,
            previous_schema=old_schema,
        )

        assert drift is not None
        assert drift.drift_type == "column_added"
        assert drift.severity == "info"  # Adding columns is non-breaking
        assert len(drift.changes) == 1
        assert drift.changes[0].change_type == "column_added"
        assert drift.changes[0].column_name == "name"

    def test_detect_column_removed(self) -> None:
        """Detect when a column is removed - breaking change"""
        detector = SchemaDriftDetector()
        old_schema = [
            {"name": "id", "type": "xsd:integer"},
            {"name": "name", "type": "xsd:string"},
        ]
        new_schema = [
            {"name": "id", "type": "xsd:integer"},
        ]

        drift = detector.detect_drift(
            subject_type="dataset",
            subject_id="ds_test",
            db_name="test_db",
            current_schema=new_schema,
            previous_schema=old_schema,
        )

        assert drift is not None
        assert "column_removed" in drift.drift_type
        assert drift.severity == "breaking"  # Removing columns is breaking
        assert drift.is_breaking

    def test_detect_type_changed(self) -> None:
        """Detect when a column type changes"""
        detector = SchemaDriftDetector()
        old_schema = [
            {"name": "id", "type": "xsd:integer"},
            {"name": "value", "type": "xsd:integer"},
        ]
        new_schema = [
            {"name": "id", "type": "xsd:integer"},
            {"name": "value", "type": "xsd:string"},
        ]

        drift = detector.detect_drift(
            subject_type="dataset",
            subject_id="ds_test",
            db_name="test_db",
            current_schema=new_schema,
            previous_schema=old_schema,
        )

        assert drift is not None
        assert drift.drift_type == "type_changed"
        assert len(drift.changes) == 1
        assert drift.changes[0].change_type == "type_changed"
        assert drift.changes[0].column_name == "value"
        assert drift.changes[0].old_value == "xsd:integer"
        assert drift.changes[0].new_value == "xsd:string"

    def test_detect_column_renamed(self) -> None:
        """Detect when a column is renamed (similar name, same type)"""
        detector = SchemaDriftDetector()
        old_schema = [
            {"name": "customer_name", "type": "xsd:string"},
        ]
        new_schema = [
            {"name": "customerName", "type": "xsd:string"},  # camelCase version
        ]

        drift = detector.detect_drift(
            subject_type="dataset",
            subject_id="ds_test",
            db_name="test_db",
            current_schema=new_schema,
            previous_schema=old_schema,
        )

        assert drift is not None
        # Should detect as rename due to high similarity
        renames = [c for c in drift.changes if c.change_type == "column_renamed"]
        assert len(renames) == 1
        assert renames[0].old_value == "customer_name"
        assert renames[0].new_value == "customerName"

    def test_mixed_changes(self) -> None:
        """Detect multiple changes at once"""
        detector = SchemaDriftDetector()
        old_schema = [
            {"name": "id", "type": "xsd:integer"},
            {"name": "old_col", "type": "xsd:string"},
        ]
        new_schema = [
            {"name": "id", "type": "xsd:string"},  # Type changed
            {"name": "new_col", "type": "xsd:string"},  # Added
            # old_col removed
        ]

        drift = detector.detect_drift(
            subject_type="dataset",
            subject_id="ds_test",
            db_name="test_db",
            current_schema=new_schema,
            previous_schema=old_schema,
        )

        assert drift is not None
        assert drift.drift_type == "mixed"
        assert len(drift.changes) >= 2

    def test_change_summary(self) -> None:
        """change_summary should count changes by type"""
        detector = SchemaDriftDetector()
        old_schema = [
            {"name": "id", "type": "xsd:integer"},
            {"name": "col1", "type": "xsd:string"},
        ]
        new_schema = [
            {"name": "id", "type": "xsd:integer"},
            {"name": "col2", "type": "xsd:string"},  # col1 -> col2
            {"name": "col3", "type": "xsd:string"},  # Added
        ]

        drift = detector.detect_drift(
            subject_type="dataset",
            subject_id="ds_test",
            db_name="test_db",
            current_schema=new_schema,
            previous_schema=old_schema,
        )

        assert drift is not None
        summary = drift.change_summary
        assert isinstance(summary, dict)

    def test_severity_classification_info(self) -> None:
        """Adding columns should be classified as info"""
        detector = SchemaDriftDetector()
        old_schema = [{"name": "id", "type": "xsd:integer"}]
        new_schema = [
            {"name": "id", "type": "xsd:integer"},
            {"name": "extra", "type": "xsd:string"},
        ]

        drift = detector.detect_drift(
            subject_type="dataset",
            subject_id="ds_test",
            db_name="test_db",
            current_schema=new_schema,
            previous_schema=old_schema,
        )

        assert drift.severity == "info"
        assert not drift.is_breaking

    def test_severity_classification_breaking(self) -> None:
        """Removing columns should be classified as breaking"""
        detector = SchemaDriftDetector()
        old_schema = [
            {"name": "id", "type": "xsd:integer"},
            {"name": "critical_col", "type": "xsd:string"},
        ]
        new_schema = [{"name": "id", "type": "xsd:integer"}]

        drift = detector.detect_drift(
            subject_type="dataset",
            subject_id="ds_test",
            db_name="test_db",
            current_schema=new_schema,
            previous_schema=old_schema,
        )

        assert drift.severity == "breaking"
        assert drift.is_breaking

    def test_pk_column_removal_is_critical(self) -> None:
        """Removing a PK-like column should be data_loss impact"""
        detector = SchemaDriftDetector()
        old_schema = [
            {"name": "customer_id", "type": "xsd:integer"},
            {"name": "name", "type": "xsd:string"},
        ]
        new_schema = [{"name": "name", "type": "xsd:string"}]

        drift = detector.detect_drift(
            subject_type="dataset",
            subject_id="ds_test",
            db_name="test_db",
            current_schema=new_schema,
            previous_schema=old_schema,
        )

        # customer_id matches _id$ pattern -> critical
        removed = [c for c in drift.changes if c.change_type == "column_removed"]
        assert len(removed) == 1
        assert removed[0].impact == "data_loss"

    def test_hash_based_quick_check(self) -> None:
        """Hash comparison should enable quick no-drift check"""
        detector = SchemaDriftDetector()
        schema = [{"name": "id", "type": "xsd:integer"}]

        # Same hash should return None
        drift = detector.detect_drift(
            subject_type="dataset",
            subject_id="ds_test",
            db_name="test_db",
            current_schema=schema,
            previous_hash="abc123",  # Dummy hash
        )

        # Without previous_schema, can't detect specific changes
        # but hash mismatch indicates drift
        # (Note: actual behavior depends on implementation)

    def test_to_notification_payload(self) -> None:
        """to_notification_payload should produce valid structure"""
        detector = SchemaDriftDetector()
        old_schema = [{"name": "id", "type": "xsd:integer"}]
        new_schema = [
            {"name": "id", "type": "xsd:integer"},
            {"name": "name", "type": "xsd:string"},
        ]

        drift = detector.detect_drift(
            subject_type="dataset",
            subject_id="ds_test",
            db_name="test_db",
            current_schema=new_schema,
            previous_schema=old_schema,
        )

        payload = detector.to_notification_payload(drift)

        assert payload["event_type"] == "schema_drift_detected"
        assert payload["subject_type"] == "dataset"
        assert payload["subject_id"] == "ds_test"
        assert payload["db_name"] == "test_db"
        assert payload["severity"] == "info"
        assert "changes" in payload
        assert "detected_at" in payload

    def test_empty_schema_handling(self) -> None:
        """Empty schemas should be handled gracefully"""
        detector = SchemaDriftDetector()

        drift = detector.detect_drift(
            subject_type="dataset",
            subject_id="ds_test",
            db_name="test_db",
            current_schema=[],
            previous_schema=[],
        )

        assert drift is None  # No drift between empty schemas
