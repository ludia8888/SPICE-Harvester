"""Tests for instance index rebuild service."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from shared.services.core.instance_index_rebuild_service import (
    RebuildIndexRequest,
    rebuild_instance_index,
    _resolve_alias_targets,
    _reindex_from_source,
    _get_class_counts,
)


def _mock_es():
    """Create a mock ElasticsearchService with common methods."""
    es = AsyncMock()
    es.create_index = AsyncMock(return_value=True)
    es.delete_index = AsyncMock(return_value=True)
    es.index_exists = AsyncMock(return_value=False)
    es.update_aliases = AsyncMock(return_value=True)
    es.create_alias = AsyncMock(return_value=True)
    es.bulk_index = AsyncMock(return_value={"success": 100, "failed": 0})

    # Mock the underlying client for low-level operations
    client = AsyncMock()
    client.indices = AsyncMock()
    client.indices.exists_alias = AsyncMock(return_value=False)
    client.indices.get_alias = AsyncMock(return_value={})
    client.reindex = AsyncMock(return_value={"total": 100, "updated": 0, "created": 100})
    client.search = AsyncMock(return_value={
        "aggregations": {
            "classes": {
                "buckets": [
                    {"key": "Customer", "doc_count": 50},
                    {"key": "Order", "doc_count": 50},
                ]
            }
        }
    })
    es.client = client
    return es


@pytest.mark.asyncio
async def test_rebuild_success_with_existing_alias():
    """Rebuild with existing alias: reindex + swap."""
    es = _mock_es()
    # base_index is an alias pointing to an old versioned index
    es.client.indices.exists_alias = AsyncMock(return_value=True)
    es.client.indices.get_alias = AsyncMock(return_value={"demo_test_instances_v1000": {}})

    result = await rebuild_instance_index(
        request=RebuildIndexRequest(db_name="demo_test"),
        elasticsearch_service=es,
        task_id="test-task-1",
    )

    assert result.status == "COMPLETED"
    assert result.task_id == "test-task-1"
    assert result.total_instances_indexed == 100
    assert result.new_index.startswith("demo_test_instances_v")
    assert result.old_indices == ["demo_test_instances_v1000"]
    assert len(result.classes_processed) == 2
    # Verify create_index was called for the new index
    es.create_index.assert_called_once()
    # Verify reindex was called
    es.client.reindex.assert_called_once()
    # Verify alias swap
    es.update_aliases.assert_called_once()


@pytest.mark.asyncio
async def test_rebuild_success_no_existing_data():
    """Rebuild when no existing index exists — creates empty index with alias."""
    es = _mock_es()
    es.client.indices.exists_alias = AsyncMock(return_value=False)
    es.index_exists = AsyncMock(return_value=False)
    es.client.search = AsyncMock(return_value={"aggregations": {"classes": {"buckets": []}}})

    result = await rebuild_instance_index(
        request=RebuildIndexRequest(db_name="empty_db"),
        elasticsearch_service=es,
        task_id="test-task-2",
    )

    assert result.status == "COMPLETED"
    assert result.total_instances_indexed == 0
    assert result.classes_processed == []
    es.create_index.assert_called_once()
    es.client.reindex.assert_not_called()  # No source to reindex from


@pytest.mark.asyncio
async def test_rebuild_success_concrete_index():
    """Rebuild when base_index is a concrete index (not alias)."""
    es = _mock_es()
    es.client.indices.exists_alias = AsyncMock(return_value=False)
    es.index_exists = AsyncMock(return_value=True)

    result = await rebuild_instance_index(
        request=RebuildIndexRequest(db_name="concrete_db"),
        elasticsearch_service=es,
        task_id="test-task-3",
    )

    assert result.status == "COMPLETED"
    assert result.total_instances_indexed == 100
    es.client.reindex.assert_called_once()


@pytest.mark.asyncio
async def test_rebuild_alias_swap_failure():
    """Rebuild fails when alias swap fails — cleanup partial index."""
    es = _mock_es()
    es.client.indices.exists_alias = AsyncMock(return_value=False)
    es.index_exists = AsyncMock(return_value=False)
    # create_alias fails
    es.create_alias = AsyncMock(side_effect=RuntimeError("ES connection lost"))

    result = await rebuild_instance_index(
        request=RebuildIndexRequest(db_name="fail_db"),
        elasticsearch_service=es,
        task_id="test-task-4",
    )

    assert result.status == "FAILED"
    assert "ES connection lost" in result.error
    # Cleanup: delete_index should have been attempted on the new index
    es.delete_index.assert_called_once()


@pytest.mark.asyncio
async def test_resolve_alias_targets():
    """_resolve_alias_targets returns concrete indices behind alias."""
    es = _mock_es()
    es.client.indices.exists_alias = AsyncMock(return_value=True)
    es.client.indices.get_alias = AsyncMock(return_value={
        "demo_test_instances_main_v1": {},
        "demo_test_instances_main_v2": {},
    })

    targets = await _resolve_alias_targets(es, "demo_test_instances_main")
    assert set(targets) == {"demo_test_instances_main_v1", "demo_test_instances_main_v2"}


@pytest.mark.asyncio
async def test_resolve_alias_targets_no_alias():
    """_resolve_alias_targets returns empty list when alias doesn't exist."""
    es = _mock_es()
    es.client.indices.exists_alias = AsyncMock(return_value=False)

    targets = await _resolve_alias_targets(es, "nonexistent_alias")
    assert targets == []


@pytest.mark.asyncio
async def test_reindex_from_source():
    """_reindex_from_source calls ES reindex API."""
    es = _mock_es()
    es.client.reindex = AsyncMock(return_value={"total": 42, "created": 42})

    count = await _reindex_from_source(
        es, source_index="old_index", dest_index="new_index"
    )
    assert count == 42
    es.client.reindex.assert_called_once()


@pytest.mark.asyncio
async def test_get_class_counts():
    """_get_class_counts returns per-class document counts."""
    es = _mock_es()

    counts = await _get_class_counts(es, "test_index")
    assert counts == {"Customer": 50, "Order": 50}


@pytest.mark.asyncio
async def test_get_class_counts_empty_index():
    """_get_class_counts returns empty dict on empty index."""
    es = _mock_es()
    es.client.search = AsyncMock(return_value={
        "aggregations": {"classes": {"buckets": []}}
    })

    counts = await _get_class_counts(es, "empty_index")
    assert counts == {}
