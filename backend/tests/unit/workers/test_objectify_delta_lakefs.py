"""Tests for LakeFS diff-based delta computation and worker integration."""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from shared.services.pipeline.objectify_delta_utils import (
    DeltaResult,
    ObjectifyDeltaComputer,
    create_delta_computer_for_mapping_spec,
)


def test_delta_result_has_changes_true() -> None:
    result = DeltaResult(added_rows=[{"id": "1"}])
    assert result.has_changes is True


def test_delta_result_has_changes_false() -> None:
    result = DeltaResult()
    assert result.has_changes is False


def test_delta_result_stats_auto_computed() -> None:
    result = DeltaResult(
        added_rows=[{"id": "1"}, {"id": "2"}],
        modified_rows=[{"id": "3"}],
        deleted_keys=["4"],
    )
    assert result.stats["added_count"] == 2
    assert result.stats["modified_count"] == 1
    assert result.stats["deleted_count"] == 1
    assert result.stats["total_changes"] == 4


def test_compute_row_key() -> None:
    computer = ObjectifyDeltaComputer(pk_columns=["customer_id", "order_id"])
    key = computer.compute_row_key({"customer_id": "CUST001", "order_id": "ORD001", "amount": "100"})
    assert key == "CUST001|ORD001"


def test_compute_row_hash_deterministic() -> None:
    computer = ObjectifyDeltaComputer(pk_columns=["id"])
    row = {"id": "1", "name": "Alice", "age": "30"}
    h1 = computer.compute_row_hash(row)
    h2 = computer.compute_row_hash(row)
    assert h1 == h2


def test_compute_row_hash_differs_on_change() -> None:
    computer = ObjectifyDeltaComputer(pk_columns=["id"])
    h1 = computer.compute_row_hash({"id": "1", "name": "Alice"})
    h2 = computer.compute_row_hash({"id": "1", "name": "Bob"})
    assert h1 != h2


@pytest.mark.asyncio
async def test_compute_delta_from_lakefs_diff_added_file() -> None:
    computer = ObjectifyDeltaComputer(pk_columns=["id"])

    fake_lakefs = AsyncMock()
    fake_lakefs.diff = AsyncMock(return_value={
        "results": [
            {"type": "added", "path": "data/new_file.csv"},
        ]
    })

    async def fake_reader(repo, ref, path):
        return [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
        ]

    result = await computer.compute_delta_from_lakefs_diff(
        lakefs_client=fake_lakefs,
        repository="test_repo",
        base_ref="commit_a",
        target_ref="commit_b",
        path="data/",
        file_reader=fake_reader,
    )

    assert len(result.added_rows) == 2
    assert len(result.modified_rows) == 0
    assert len(result.deleted_keys) == 0


@pytest.mark.asyncio
async def test_compute_delta_from_lakefs_diff_removed_file() -> None:
    computer = ObjectifyDeltaComputer(pk_columns=["id"])

    fake_lakefs = AsyncMock()
    fake_lakefs.diff = AsyncMock(return_value={
        "results": [
            {"type": "removed", "path": "data/old_file.csv"},
        ]
    })

    async def fake_reader(repo, ref, path):
        return [
            {"id": "1", "name": "Alice"},
        ]

    result = await computer.compute_delta_from_lakefs_diff(
        lakefs_client=fake_lakefs,
        repository="test_repo",
        base_ref="commit_a",
        target_ref="commit_b",
        path="data/",
        file_reader=fake_reader,
    )

    assert len(result.added_rows) == 0
    assert len(result.deleted_keys) == 1
    assert result.deleted_keys[0] == "1"


@pytest.mark.asyncio
async def test_compute_delta_from_lakefs_diff_changed_file() -> None:
    computer = ObjectifyDeltaComputer(pk_columns=["id"])

    fake_lakefs = AsyncMock()
    fake_lakefs.diff = AsyncMock(return_value={
        "results": [
            {"type": "changed", "path": "data/customers.csv"},
        ]
    })

    call_count = {"n": 0}

    async def fake_reader(repo, ref, path):
        call_count["n"] += 1
        if call_count["n"] == 1:  # old version
            return [
                {"id": "1", "name": "Alice", "email": "alice@old.com"},
                {"id": "2", "name": "Bob", "email": "bob@example.com"},
            ]
        else:  # new version
            return [
                {"id": "1", "name": "Alice", "email": "alice@new.com"},  # modified
                {"id": "3", "name": "Charlie", "email": "charlie@example.com"},  # added
                # id=2 is deleted
            ]

    result = await computer.compute_delta_from_lakefs_diff(
        lakefs_client=fake_lakefs,
        repository="test_repo",
        base_ref="commit_a",
        target_ref="commit_b",
        path="data/",
        file_reader=fake_reader,
    )

    assert len(result.added_rows) == 1
    assert result.added_rows[0]["id"] == "3"
    assert len(result.modified_rows) == 1
    assert result.modified_rows[0]["email"] == "alice@new.com"
    assert len(result.deleted_keys) == 1
    assert result.deleted_keys[0] == "2"


@pytest.mark.asyncio
async def test_compute_delta_from_snapshots() -> None:
    computer = ObjectifyDeltaComputer(pk_columns=["id"])

    async def _iter(rows):
        for r in rows:
            yield r

    old_rows = [
        {"id": "1", "name": "Alice"},
        {"id": "2", "name": "Bob"},
    ]
    new_rows = [
        {"id": "1", "name": "Alice Updated"},
        {"id": "3", "name": "Charlie"},
    ]

    result = await computer.compute_delta_from_snapshots(
        old_rows_iterator=_iter(old_rows),
        new_rows_iterator=_iter(new_rows),
    )

    assert len(result.added_rows) == 1
    assert result.added_rows[0]["id"] == "3"
    assert len(result.modified_rows) == 1
    assert result.modified_rows[0]["name"] == "Alice Updated"
    assert len(result.deleted_keys) == 1
    assert result.deleted_keys[0] == "2"


def test_create_delta_computer_from_mapping_spec() -> None:
    spec = {
        "pk_spec": {"columns": ["customer_id", "order_id"]},
        "batch_size": 5000,
    }
    computer = create_delta_computer_for_mapping_spec(spec)
    assert computer.pk_columns == ["customer_id", "order_id"]
    assert computer.batch_size == 5000


def test_create_delta_computer_fallback_pk() -> None:
    spec = {
        "field_mappings": [{"source_field": "id"}, {"source_field": "name"}],
    }
    computer = create_delta_computer_for_mapping_spec(spec)
    assert computer.pk_columns == ["id"]
