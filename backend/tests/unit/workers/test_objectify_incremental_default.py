"""Tests for incremental mode default and auto-watermark detection."""
from __future__ import annotations

import pytest

from shared.models.objectify_job import ObjectifyJob


def _resolve_execution_mode(*, options: dict, job_mode: str | None) -> str:
    execution_mode = str(options.get("execution_mode") or job_mode or "full").strip().lower() or "full"
    if execution_mode not in {"full", "incremental", "delta"}:
        return "full"
    return execution_mode


def test_default_execution_mode_is_full() -> None:
    """Default execution mode should be 'full'."""
    job = ObjectifyJob(
        job_id="test-1",
        db_name="test_db",
        dataset_id="ds-1",
        dataset_version_id="v-1",
        mapping_spec_id="ms-1",
        mapping_spec_version=1,
        target_class_id="Customer",
    )
    assert job.execution_mode == "full"


def test_explicit_full_mode() -> None:
    job = ObjectifyJob(
        job_id="test-2",
        db_name="test_db",
        dataset_id="ds-1",
        dataset_version_id="v-1",
        mapping_spec_id="ms-1",
        mapping_spec_version=1,
        target_class_id="Customer",
        execution_mode="full",
    )
    assert job.execution_mode == "full"


def test_explicit_delta_mode() -> None:
    job = ObjectifyJob(
        job_id="test-3",
        db_name="test_db",
        dataset_id="ds-1",
        dataset_version_id="v-1",
        mapping_spec_id="ms-1",
        mapping_spec_version=1,
        target_class_id="Customer",
        execution_mode="delta",
        base_commit_id="commit_abc",
    )
    assert job.execution_mode == "delta"
    assert job.base_commit_id == "commit_abc"


def test_watermark_fields() -> None:
    job = ObjectifyJob(
        job_id="test-4",
        db_name="test_db",
        dataset_id="ds-1",
        dataset_version_id="v-1",
        mapping_spec_id="ms-1",
        mapping_spec_version=1,
        target_class_id="Customer",
        execution_mode="incremental",
        watermark_column="updated_at",
        previous_watermark="2026-01-01T00:00:00Z",
    )
    assert job.watermark_column == "updated_at"
    assert job.previous_watermark == "2026-01-01T00:00:00Z"


def test_execution_mode_prefers_options_over_job_mode() -> None:
    assert _resolve_execution_mode(options={"execution_mode": "delta"}, job_mode="full") == "delta"
    assert _resolve_execution_mode(options={"execution_mode": "incremental"}, job_mode="delta") == "incremental"


def test_execution_mode_falls_back_to_full_for_invalid_values() -> None:
    assert _resolve_execution_mode(options={"execution_mode": "streaming"}, job_mode="incremental") == "full"
    assert _resolve_execution_mode(options={}, job_mode=None) == "full"


def test_auto_detect_watermark_column() -> None:
    """Test the auto-detect helper function from objectify_worker."""
    # Import the worker helper
    from objectify_worker.main import _auto_detect_watermark_column

    # Should find updated_at
    result = _auto_detect_watermark_column(
        columns=["customer_id", "name", "updated_at", "email"],
    )
    assert result == "updated_at"

    # Should find created_at when updated_at is not present
    result = _auto_detect_watermark_column(
        columns=["id", "name", "created_at"],
    )
    assert result == "created_at"

    # Should find timestamp
    result = _auto_detect_watermark_column(
        columns=["id", "value", "timestamp"],
    )
    assert result == "timestamp"

    # Should return None when no candidate found
    result = _auto_detect_watermark_column(
        columns=["id", "name", "email"],
    )
    assert result is None

    # Should use options hint
    result = _auto_detect_watermark_column(
        columns=["id", "name"],
        options={"watermark_column_hint": "modified_date"},
    )
    assert result == "modified_date"
