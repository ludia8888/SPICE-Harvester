"""Tests for admin_reindex_instances_service."""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from bff.services.admin_reindex_instances_service import reindex_all_instances


class _FakeSpec:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class _FakeDataset:
    def __init__(self, *, db_name: str):
        self.db_name = db_name


@pytest.mark.asyncio
async def test_reindex_no_mapping_specs() -> None:
    registry = AsyncMock()
    registry.list_mapping_specs = AsyncMock(return_value=[])
    dataset_reg = AsyncMock()
    job_queue = AsyncMock()

    result = await reindex_all_instances(
        db_name="test_db",
        objectify_registry=registry,
        dataset_registry=dataset_reg,
        job_queue=job_queue,
    )

    assert result["status"] == "no_mapping_specs"


@pytest.mark.asyncio
async def test_reindex_submits_jobs() -> None:
    spec = _FakeSpec(
        mapping_spec_id="ms-1",
        version=2,
        target_class_id="Customer",
        dataset_id="ds-1",
        dataset_branch="main",
        db_name="test_db",
    )

    registry = AsyncMock()
    registry.list_mapping_specs = AsyncMock(return_value=[spec])

    dataset_reg = AsyncMock()
    dataset_reg.list_dataset_versions = AsyncMock(return_value=[
        {"version_id": "v-latest"},
    ])

    job_queue = AsyncMock()
    job_queue.enqueue = AsyncMock()

    result = await reindex_all_instances(
        db_name="test_db",
        objectify_registry=registry,
        dataset_registry=dataset_reg,
        job_queue=job_queue,
    )

    assert result["status"] == "submitted"
    assert result["submitted_jobs"] == 1
    assert len(result["jobs"]) == 1
    assert result["jobs"][0]["target_class_id"] == "Customer"
    job_queue.enqueue.assert_called_once()


@pytest.mark.asyncio
async def test_reindex_handles_missing_dataset_version() -> None:
    spec = _FakeSpec(
        mapping_spec_id="ms-1",
        version=1,
        target_class_id="Order",
        dataset_id="ds-1",
        dataset_branch="main",
        db_name="test_db",
    )

    registry = AsyncMock()
    registry.list_mapping_specs = AsyncMock(return_value=[spec])

    dataset_reg = AsyncMock()
    dataset_reg.list_dataset_versions = AsyncMock(return_value=[])

    job_queue = AsyncMock()

    result = await reindex_all_instances(
        db_name="test_db",
        objectify_registry=registry,
        dataset_registry=dataset_reg,
        job_queue=job_queue,
    )

    assert result["submitted_jobs"] == 0
    assert result["errors"] == 1
    assert "No dataset version" in result["error_details"][0]["error"]


@pytest.mark.asyncio
async def test_reindex_with_multiple_specs() -> None:
    specs = [
        _FakeSpec(mapping_spec_id="ms-1", version=1, target_class_id="Customer",
                  dataset_id="ds-1", dataset_branch="main", db_name="test_db"),
        _FakeSpec(mapping_spec_id="ms-2", version=3, target_class_id="Order",
                  dataset_id="ds-2", dataset_branch="main", db_name="test_db"),
    ]

    registry = AsyncMock()
    registry.list_mapping_specs = AsyncMock(return_value=specs)

    dataset_reg = AsyncMock()
    dataset_reg.list_dataset_versions = AsyncMock(return_value=[{"version_id": "v-1"}])

    job_queue = AsyncMock()
    job_queue.enqueue = AsyncMock()

    result = await reindex_all_instances(
        db_name="test_db",
        objectify_registry=registry,
        dataset_registry=dataset_reg,
        job_queue=job_queue,
    )

    assert result["submitted_jobs"] == 2
    assert job_queue.enqueue.call_count == 2


@pytest.mark.asyncio
async def test_reindex_filters_legacy_specs_by_dataset_db_name() -> None:
    spec = _FakeSpec(
        mapping_spec_id="ms-legacy",
        version=1,
        target_class_id="Order",
        dataset_id="ds-legacy",
        dataset_branch="main",
    )

    registry = AsyncMock()
    registry.list_mapping_specs = AsyncMock(return_value=[spec])

    dataset_reg = AsyncMock()
    dataset_reg.get_dataset = AsyncMock(return_value=_FakeDataset(db_name="other_db"))
    dataset_reg.list_dataset_versions = AsyncMock(return_value=[{"version_id": "v-1"}])

    job_queue = AsyncMock()
    job_queue.enqueue = AsyncMock()

    result = await reindex_all_instances(
        db_name="test_db",
        objectify_registry=registry,
        dataset_registry=dataset_reg,
        job_queue=job_queue,
    )

    assert result["status"] == "submitted"
    assert result["submitted_jobs"] == 0
    assert job_queue.enqueue.call_count == 0
