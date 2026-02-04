"""
Enterprise-level idempotency tests for all 5 gap implementations.

This module verifies:
1. Pipeline API idempotency keys
2. PipelineJob dedupe_key auto-generation
3. Pipeline Registry OCC (Optimistic Concurrency Control)
4. Kafka dedup-id headers
5. Worker aggregate ordering
"""

from __future__ import annotations

import hashlib
import pytest
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

from pydantic import ValidationError


# =============================================================================
# Gap 1: PipelineJob dedupe_key Auto-Generation
# =============================================================================


class TestPipelineJobDedupeKey:
    """Test PipelineJob dedupe_key auto-generation (Gap 2)"""

    def test_dedupe_key_auto_generated(self) -> None:
        """PipelineJob should auto-generate dedupe_key when not provided"""
        from shared.models.pipeline_job import PipelineJob

        job = PipelineJob(
            job_id="job-1",
            pipeline_id="pipeline-1",
            db_name="test_db",
            output_dataset_name="output",
        )

        assert job.dedupe_key is not None
        assert len(job.dedupe_key) == 32

    def test_dedupe_key_preserved_when_provided(self) -> None:
        """PipelineJob should preserve dedupe_key if explicitly provided"""
        from shared.models.pipeline_job import PipelineJob

        custom_key = "my-custom-dedupe-key-12345678"
        job = PipelineJob(
            job_id="job-1",
            pipeline_id="pipeline-1",
            db_name="test_db",
            output_dataset_name="output",
            dedupe_key=custom_key,
        )

        assert job.dedupe_key == custom_key

    def test_dedupe_key_deterministic(self) -> None:
        """Same inputs should produce same dedupe_key"""
        from shared.models.pipeline_job import PipelineJob

        job1 = PipelineJob(
            job_id="job-1",
            pipeline_id="pipeline-1",
            db_name="test_db",
            output_dataset_name="output",
            mode="deploy",
            branch="main",
        )

        job2 = PipelineJob(
            job_id="job-2",  # Different job_id
            pipeline_id="pipeline-1",  # Same pipeline_id
            db_name="test_db",
            output_dataset_name="output",
            mode="deploy",
            branch="main",
        )

        # Same dedupe_key because composite key components are the same
        assert job1.dedupe_key == job2.dedupe_key

    def test_dedupe_key_includes_idempotency_key(self) -> None:
        """dedupe_key should incorporate client idempotency_key"""
        from shared.models.pipeline_job import PipelineJob

        job_with_idem = PipelineJob(
            job_id="job-1",
            pipeline_id="pipeline-1",
            db_name="test_db",
            output_dataset_name="output",
            idempotency_key="client-idem-key",
        )

        job_without_idem = PipelineJob(
            job_id="job-2",
            pipeline_id="pipeline-1",
            db_name="test_db",
            output_dataset_name="output",
        )

        assert job_with_idem.dedupe_key != job_without_idem.dedupe_key

    def test_dedupe_key_includes_definition_hash(self) -> None:
        """dedupe_key should incorporate definition_hash"""
        from shared.models.pipeline_job import PipelineJob

        job_with_hash = PipelineJob(
            job_id="job-1",
            pipeline_id="pipeline-1",
            db_name="test_db",
            output_dataset_name="output",
            definition_hash="abc123",
        )

        job_without_hash = PipelineJob(
            job_id="job-2",
            pipeline_id="pipeline-1",
            db_name="test_db",
            output_dataset_name="output",
        )

        assert job_with_hash.dedupe_key != job_without_hash.dedupe_key

    def test_dedupe_key_includes_node_id(self) -> None:
        """dedupe_key should incorporate node_id"""
        from shared.models.pipeline_job import PipelineJob

        job_with_node = PipelineJob(
            job_id="job-1",
            pipeline_id="pipeline-1",
            db_name="test_db",
            output_dataset_name="output",
            node_id="node-1",
        )

        job_without_node = PipelineJob(
            job_id="job-2",
            pipeline_id="pipeline-1",
            db_name="test_db",
            output_dataset_name="output",
        )

        assert job_with_node.dedupe_key != job_without_node.dedupe_key

    def test_dedupe_key_different_for_different_modes(self) -> None:
        """dedupe_key should differ for different modes"""
        from shared.models.pipeline_job import PipelineJob

        job_deploy = PipelineJob(
            job_id="job-1",
            pipeline_id="pipeline-1",
            db_name="test_db",
            output_dataset_name="output",
            mode="deploy",
        )

        job_preview = PipelineJob(
            job_id="job-2",
            pipeline_id="pipeline-1",
            db_name="test_db",
            output_dataset_name="output",
            mode="preview",
        )

        assert job_deploy.dedupe_key != job_preview.dedupe_key

    def test_build_dedupe_key_static_method(self) -> None:
        """build_dedupe_key static method should work correctly"""
        from shared.models.pipeline_job import PipelineJob

        key = PipelineJob.build_dedupe_key(
            pipeline_id="pipeline-1",
            mode="deploy",
            branch="main",
        )

        assert key is not None
        assert len(key) == 32

    def test_build_dedupe_key_matches_instance(self) -> None:
        """Static build_dedupe_key should match instance dedupe_key"""
        from shared.models.pipeline_job import PipelineJob

        job = PipelineJob(
            job_id="job-1",
            pipeline_id="pipeline-1",
            db_name="test_db",
            output_dataset_name="output",
            mode="deploy",
            branch="feature",
            definition_hash="hash123",
        )

        static_key = PipelineJob.build_dedupe_key(
            pipeline_id="pipeline-1",
            mode="deploy",
            branch="feature",
            definition_hash="hash123",
        )

        assert job.dedupe_key == static_key


# =============================================================================
# Gap 2: Kafka dedup-id Headers
# =============================================================================


class TestKafkaHeadersWithDedup:
    """Test kafka_headers_with_dedup function (Gap 5)"""

    def test_dedup_header_with_explicit_dedup_id(self) -> None:
        """Should include explicit dedup_id in headers"""
        from shared.observability.context_propagation import kafka_headers_with_dedup

        headers = kafka_headers_with_dedup(
            None,
            dedup_id="explicit-dedup-id",
        )

        dedup_headers = [h for h in headers if h[0] == "dedup-id"]
        assert len(dedup_headers) == 1
        assert dedup_headers[0][1] == b"explicit-dedup-id"

    def test_dedup_header_computed_from_event_id(self) -> None:
        """Should compute dedup_id from event_id and aggregate_id"""
        from shared.observability.context_propagation import kafka_headers_with_dedup

        headers = kafka_headers_with_dedup(
            None,
            event_id="event-123",
            aggregate_id="agg-456",
        )

        dedup_headers = [h for h in headers if h[0] == "dedup-id"]
        assert len(dedup_headers) == 1
        assert dedup_headers[0][1] == b"agg-456:event-123"

    def test_dedup_header_with_global_aggregate(self) -> None:
        """Should use 'global' when aggregate_id is None"""
        from shared.observability.context_propagation import kafka_headers_with_dedup

        headers = kafka_headers_with_dedup(
            None,
            event_id="event-123",
            aggregate_id=None,
        )

        dedup_headers = [h for h in headers if h[0] == "dedup-id"]
        assert len(dedup_headers) == 1
        assert dedup_headers[0][1] == b"global:event-123"

    def test_dedup_header_extracted_from_payload(self) -> None:
        """Should extract event_id from payload if not provided"""
        from shared.observability.context_propagation import kafka_headers_with_dedup

        payload = {
            "job_id": "job-from-payload",
            "dataset_id": "ds-from-payload",
        }

        headers = kafka_headers_with_dedup(payload)

        dedup_headers = [h for h in headers if h[0] == "dedup-id"]
        assert len(dedup_headers) == 1
        assert dedup_headers[0][1] == b"ds-from-payload:job-from-payload"

    def test_dedup_header_with_pipeline_id_fallback(self) -> None:
        """Should use pipeline_id as aggregate_id fallback"""
        from shared.observability.context_propagation import kafka_headers_with_dedup

        payload = {
            "event_id": "event-1",
            "pipeline_id": "pipeline-1",
        }

        headers = kafka_headers_with_dedup(payload)

        dedup_headers = [h for h in headers if h[0] == "dedup-id"]
        assert len(dedup_headers) == 1
        assert dedup_headers[0][1] == b"pipeline-1:event-1"

    def test_no_dedup_header_when_no_id(self) -> None:
        """Should not add dedup-id when no identifiers available"""
        from shared.observability.context_propagation import kafka_headers_with_dedup

        headers = kafka_headers_with_dedup({"random": "data"})

        dedup_headers = [h for h in headers if h[0] == "dedup-id"]
        assert len(dedup_headers) == 0

    def test_preserves_trace_headers(self) -> None:
        """Should preserve trace context headers alongside dedup-id"""
        from shared.observability.context_propagation import kafka_headers_with_dedup

        payload = {
            "traceparent": "00-trace-span-01",
            "job_id": "job-1",
        }

        headers = kafka_headers_with_dedup(payload)

        header_keys = [h[0] for h in headers]
        assert "traceparent" in header_keys
        assert "dedup-id" in header_keys


# =============================================================================
# Gap 3: Pipeline Registry OCC
# =============================================================================


class TestPipelineRegistryOCC:
    """Test Pipeline Registry Optimistic Concurrency Control (Gap 3)"""

    def test_occ_conflict_error_attributes(self) -> None:
        """PipelineOCCConflictError should have correct attributes"""
        from shared.services.registries.pipeline_registry import PipelineOCCConflictError

        error = PipelineOCCConflictError(
            pipeline_id="pipeline-1",
            expected_version=5,
            actual_version=6,
        )

        assert error.pipeline_id == "pipeline-1"
        assert error.expected_version == 5
        assert error.actual_version == 6
        assert "pipeline_id=pipeline-1" in str(error)
        assert "expected version 5" in str(error)
        assert "actual version 6" in str(error)

    def test_occ_conflict_error_without_actual_version(self) -> None:
        """PipelineOCCConflictError should work without actual_version"""
        from shared.services.registries.pipeline_registry import PipelineOCCConflictError

        error = PipelineOCCConflictError(
            pipeline_id="pipeline-1",
            expected_version=5,
        )

        assert error.actual_version is None
        assert "actual version" not in str(error)

    def test_pipeline_record_has_occ_version(self) -> None:
        """PipelineRecord should have occ_version field"""
        from shared.services.registries.pipeline_registry import PipelineRecord
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        record = PipelineRecord(
            pipeline_id="pipeline-1",
            db_name="test_db",
            name="Test Pipeline",
            description="Test description",
            pipeline_type="batch",
            location="internal",
            status="DRAFT",
            branch="main",
            lakefs_repository=None,
            proposal_status=None,
            proposal_title=None,
            proposal_description=None,
            proposal_submitted_at=None,
            proposal_reviewed_at=None,
            proposal_review_comment=None,
            proposal_bundle={},
            last_preview_status=None,
            last_preview_at=None,
            last_preview_rows=None,
            last_preview_job_id=None,
            last_preview_node_id=None,
            last_preview_sample={},
            last_preview_nodes={},
            last_build_status=None,
            last_build_at=None,
            last_build_output={},
            deployed_at=None,
            deployed_commit_id=None,
            schedule_interval_seconds=None,
            schedule_cron=None,
            last_scheduled_at=None,
            created_at=now,
            updated_at=now,
            occ_version=3,
        )

        assert record.occ_version == 3

    def test_pipeline_record_occ_version_default(self) -> None:
        """PipelineRecord occ_version should default to 1"""
        from shared.services.registries.pipeline_registry import PipelineRecord
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        record = PipelineRecord(
            pipeline_id="pipeline-1",
            db_name="test_db",
            name="Test Pipeline",
            description="Test description",
            pipeline_type="batch",
            location="internal",
            status="DRAFT",
            branch="main",
            lakefs_repository=None,
            proposal_status=None,
            proposal_title=None,
            proposal_description=None,
            proposal_submitted_at=None,
            proposal_reviewed_at=None,
            proposal_review_comment=None,
            proposal_bundle={},
            last_preview_status=None,
            last_preview_at=None,
            last_preview_rows=None,
            last_preview_job_id=None,
            last_preview_node_id=None,
            last_preview_sample={},
            last_preview_nodes={},
            last_build_status=None,
            last_build_at=None,
            last_build_output={},
            deployed_at=None,
            deployed_commit_id=None,
            schedule_interval_seconds=None,
            schedule_cron=None,
            last_scheduled_at=None,
            created_at=now,
            updated_at=now,
        )

        assert record.occ_version == 1


# =============================================================================
# Gap 4: Pipeline API Idempotency Keys
# =============================================================================


class TestPipelineAPIIdempotencyKeys:
    """Test Pipeline API idempotency key validation (Gap 1)"""

    def test_get_idempotency_key_from_primary_header(self) -> None:
        """_get_idempotency_key should extract from Idempotency-Key header"""
        from bff.routers.pipeline_shared import _get_idempotency_key

        mock_request = MagicMock()
        mock_request.headers.get = lambda key: "idem-key-123" if key == "Idempotency-Key" else None

        result = _get_idempotency_key(mock_request)
        assert result == "idem-key-123"

    def test_get_idempotency_key_from_fallback_header(self) -> None:
        """_get_idempotency_key should fallback to X-Idempotency-Key"""
        from bff.routers.pipeline_shared import _get_idempotency_key

        mock_request = MagicMock()

        def mock_get(key):
            if key == "X-Idempotency-Key":
                return "x-idem-key-456"
            return None

        mock_request.headers.get = mock_get

        result = _get_idempotency_key(mock_request)
        assert result == "x-idem-key-456"

    def test_get_idempotency_key_returns_none_when_missing(self) -> None:
        """_get_idempotency_key should return None when no header present"""
        from bff.routers.pipeline_shared import _get_idempotency_key

        mock_request = MagicMock()
        mock_request.headers.get = lambda key: None

        result = _get_idempotency_key(mock_request)
        assert result is None

    def test_get_idempotency_key_strips_whitespace(self) -> None:
        """_get_idempotency_key should strip whitespace from header value"""
        from bff.routers.pipeline_shared import _get_idempotency_key

        mock_request = MagicMock()
        mock_request.headers.get = lambda key: "  idem-key-789  " if key == "Idempotency-Key" else None

        result = _get_idempotency_key(mock_request)
        assert result == "idem-key-789"

    def test_get_idempotency_key_returns_none_for_empty_string(self) -> None:
        """_get_idempotency_key should return None for empty/whitespace-only"""
        from bff.routers.pipeline_shared import _get_idempotency_key

        mock_request = MagicMock()
        mock_request.headers.get = lambda key: "   " if key == "Idempotency-Key" else None

        result = _get_idempotency_key(mock_request)
        assert result is None

    def test_require_pipeline_idempotency_key_success(self) -> None:
        """_require_pipeline_idempotency_key should return key when present"""
        from bff.routers.pipeline_shared import _require_pipeline_idempotency_key

        mock_request = MagicMock()
        mock_request.headers.get = lambda key: "valid-key" if key == "Idempotency-Key" else None

        result = _require_pipeline_idempotency_key(mock_request, operation="test")
        assert result == "valid-key"

    def test_require_pipeline_idempotency_key_raises_when_missing(self) -> None:
        """_require_pipeline_idempotency_key should raise HTTPException when missing"""
        from bff.routers.pipeline_shared import _require_pipeline_idempotency_key
        from fastapi import HTTPException

        mock_request = MagicMock()
        mock_request.headers.get = lambda key: None

        with pytest.raises(HTTPException) as exc_info:
            _require_pipeline_idempotency_key(mock_request, operation="deploy")

        assert exc_info.value.status_code == 400
        assert "deploy" in exc_info.value.detail

    def test_require_pipeline_idempotency_key_includes_operation_in_error(self) -> None:
        """Error message should include the operation name"""
        from bff.routers.pipeline_shared import _require_pipeline_idempotency_key
        from fastapi import HTTPException

        mock_request = MagicMock()
        mock_request.headers.get = lambda key: None

        with pytest.raises(HTTPException) as exc_info:
            _require_pipeline_idempotency_key(mock_request, operation="custom-operation")

        assert "custom-operation" in exc_info.value.detail


# =============================================================================
# Gap 5: Worker Aggregate Ordering
# =============================================================================


class TestWorkerAggregateOrdering:
    """Test worker aggregate ordering with sequence_number (Gap 4)"""

    def test_pipeline_job_has_sequence_number(self) -> None:
        """PipelineJob should have sequence_number field"""
        from shared.models.pipeline_job import PipelineJob

        job = PipelineJob(
            job_id="job-1",
            pipeline_id="pipeline-1",
            db_name="test_db",
            output_dataset_name="output",
            sequence_number=42,
        )

        assert job.sequence_number == 42

    def test_pipeline_job_sequence_number_optional(self) -> None:
        """PipelineJob sequence_number should be optional"""
        from shared.models.pipeline_job import PipelineJob

        job = PipelineJob(
            job_id="job-1",
            pipeline_id="pipeline-1",
            db_name="test_db",
            output_dataset_name="output",
        )

        assert job.sequence_number is None


# =============================================================================
# Integration Tests
# =============================================================================


class TestIdempotencyIntegration:
    """Integration tests for idempotency features working together"""

    def test_pipeline_job_complete_idempotency_flow(self) -> None:
        """Complete idempotency flow from API to job creation"""
        from shared.models.pipeline_job import PipelineJob

        # Simulate API receiving idempotency key
        api_idempotency_key = "client-provided-idem-key"

        # Create job with idempotency key
        job = PipelineJob(
            job_id="job-123",
            pipeline_id="pipeline-456",
            db_name="test_db",
            output_dataset_name="output",
            mode="deploy",
            branch="main",
            idempotency_key=api_idempotency_key,
            sequence_number=1,
        )

        # Verify dedupe_key was computed
        assert job.dedupe_key is not None

        # Same job should produce same dedupe_key
        duplicate_job = PipelineJob(
            job_id="job-789",  # Different job_id
            pipeline_id="pipeline-456",  # Same pipeline
            db_name="test_db",
            output_dataset_name="output",
            mode="deploy",
            branch="main",
            idempotency_key=api_idempotency_key,  # Same idempotency key
            sequence_number=2,
        )

        assert job.dedupe_key == duplicate_job.dedupe_key

    def test_kafka_headers_include_job_dedupe(self) -> None:
        """Kafka headers should include job dedupe_key"""
        from shared.models.pipeline_job import PipelineJob
        from shared.observability.context_propagation import kafka_headers_with_dedup

        job = PipelineJob(
            job_id="job-1",
            pipeline_id="pipeline-1",
            db_name="test_db",
            output_dataset_name="output",
        )

        # Use dedupe_key for Kafka headers
        headers = kafka_headers_with_dedup(
            None,
            dedup_id=job.dedupe_key,
        )

        dedup_headers = [h for h in headers if h[0] == "dedup-id"]
        assert len(dedup_headers) == 1
        assert dedup_headers[0][1] == job.dedupe_key.encode("utf-8")
