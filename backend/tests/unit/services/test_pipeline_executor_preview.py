from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pytest

from shared.services.pipeline.pipeline_executor import PipelineExecutor


@dataclass(frozen=True)
class _Dataset:
    dataset_id: str
    db_name: str
    name: str
    branch: str
    schema_json: dict[str, Any]


@dataclass(frozen=True)
class _Version:
    dataset_id: str
    artifact_key: Optional[str]
    sample_json: dict[str, Any]


class _DatasetRegistry:
    def __init__(self) -> None:
        self.datasets_by_name: dict[tuple[str, str, str], _Dataset] = {}
        self.versions_by_dataset_id: dict[str, _Version] = {}

    async def get_dataset(self, *, dataset_id: str) -> Optional[_Dataset]:
        for dataset in self.datasets_by_name.values():
            if dataset.dataset_id == dataset_id:
                return dataset
        return None

    async def get_dataset_by_name(self, *, db_name: str, name: str, branch: str) -> Optional[_Dataset]:
        return self.datasets_by_name.get((db_name, name, branch))

    async def get_latest_version(self, *, dataset_id: str) -> Optional[_Version]:
        return self.versions_by_dataset_id.get(dataset_id)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_executor_preview_supports_node_level_preview_and_row_count() -> None:
    db_name = "demo"
    dataset_name = "ds1"
    dataset_id = "ds-demo-ds1-main"

    registry = _DatasetRegistry()
    registry.datasets_by_name[(db_name, dataset_name, "master")] = _Dataset(
        dataset_id=dataset_id,
        db_name=db_name,
        name=dataset_name,
        branch="master",
        schema_json={"columns": [{"name": "id", "type": "xsd:integer"}, {"name": "name", "type": "xsd:string"}]},
    )
    registry.versions_by_dataset_id[dataset_id] = _Version(
        dataset_id=dataset_id,
        artifact_key=None,
        sample_json={"rows": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]},
    )

    executor = PipelineExecutor(dataset_registry=registry)

    definition = {
        "nodes": [
            {"id": "in1", "type": "input", "metadata": {"datasetName": dataset_name}},
            {"id": "t1", "type": "transform", "metadata": {"operation": "select", "columns": ["id"]}},
            {"id": "out1", "type": "output"},
        ],
        "edges": [
            {"from": "in1", "to": "t1"},
            {"from": "t1", "to": "out1"},
        ],
        "parameters": [],
    }

    input_preview = await executor.preview(definition=definition, db_name=db_name, node_id="in1", limit=10)
    assert input_preview["row_count"] == 2
    assert {col["name"]: col["type"] for col in input_preview["columns"]} == {
        "id": "xsd:integer",
        "name": "xsd:string",
    }

    transform_preview = await executor.preview(definition=definition, db_name=db_name, node_id="t1", limit=10)
    assert transform_preview["row_count"] == 2
    assert {col["name"]: col["type"] for col in transform_preview["columns"]} == {"id": "xsd:integer"}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_executor_compute_structured_target_column_overwrites_existing() -> None:
    db_name = "demo"
    dataset_name = "ds1"
    dataset_id = "ds-demo-ds1-main"

    registry = _DatasetRegistry()
    registry.datasets_by_name[(db_name, dataset_name, "master")] = _Dataset(
        dataset_id=dataset_id,
        db_name=db_name,
        name=dataset_name,
        branch="master",
        schema_json={"columns": [{"name": "amount", "type": "xsd:integer"}]},
    )
    registry.versions_by_dataset_id[dataset_id] = _Version(
        dataset_id=dataset_id,
        artifact_key=None,
        sample_json={"rows": [{"amount": 1}, {"amount": 2}]},
    )

    executor = PipelineExecutor(dataset_registry=registry)

    definition = {
        "nodes": [
            {"id": "in1", "type": "input", "metadata": {"datasetName": dataset_name}},
            {
                "id": "t1",
                "type": "transform",
                "metadata": {"operation": "compute", "targetColumn": "amount", "formula": "amount + 1"},
            },
            {"id": "out1", "type": "output"},
        ],
        "edges": [{"from": "in1", "to": "t1"}, {"from": "t1", "to": "out1"}],
        "parameters": [],
    }

    preview = await executor.preview(definition=definition, db_name=db_name, node_id="t1", limit=10)
    amounts = [row.get("amount") for row in preview.get("rows") or []]
    assert amounts == [2, 3]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_executor_compute_equals_is_treated_as_comparison_when_lhs_exists() -> None:
    db_name = "demo"
    dataset_name = "ds1"
    dataset_id = "ds-demo-ds1-main"

    registry = _DatasetRegistry()
    registry.datasets_by_name[(db_name, dataset_name, "master")] = _Dataset(
        dataset_id=dataset_id,
        db_name=db_name,
        name=dataset_name,
        branch="master",
        schema_json={"columns": [{"name": "amount", "type": "xsd:integer"}]},
    )
    registry.versions_by_dataset_id[dataset_id] = _Version(
        dataset_id=dataset_id,
        artifact_key=None,
        sample_json={"rows": [{"amount": 1}, {"amount": 2}]},
    )

    executor = PipelineExecutor(dataset_registry=registry)

    definition = {
        "nodes": [
            {"id": "in1", "type": "input", "metadata": {"datasetName": dataset_name}},
            {"id": "t1", "type": "transform", "metadata": {"operation": "compute", "expression": "amount = 1"}},
            {"id": "out1", "type": "output"},
        ],
        "edges": [{"from": "in1", "to": "t1"}, {"from": "t1", "to": "out1"}],
        "parameters": [],
    }

    preview = await executor.preview(definition=definition, db_name=db_name, node_id="t1", limit=10)
    values = [row.get("computed") for row in preview.get("rows") or []]
    assert values == [True, False]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_executor_stream_join_dynamic_uses_backward_time_direction_by_default() -> None:
    db_name = "demo"
    left_name = "left_stream"
    right_name = "right_stream"
    left_id = "ds-demo-left-main"
    right_id = "ds-demo-right-main"

    registry = _DatasetRegistry()
    registry.datasets_by_name[(db_name, left_name, "master")] = _Dataset(
        dataset_id=left_id,
        db_name=db_name,
        name=left_name,
        branch="master",
        schema_json={"columns": [{"name": "id", "type": "xsd:integer"}, {"name": "ts_left", "type": "xsd:string"}]},
    )
    registry.datasets_by_name[(db_name, right_name, "master")] = _Dataset(
        dataset_id=right_id,
        db_name=db_name,
        name=right_name,
        branch="master",
        schema_json={"columns": [{"name": "id", "type": "xsd:integer"}, {"name": "ts_right", "type": "xsd:string"}]},
    )
    registry.versions_by_dataset_id[left_id] = _Version(
        dataset_id=left_id,
        artifact_key=None,
        sample_json={"rows": [{"id": 1, "ts_left": "2026-01-01T00:00:00Z"}]},
    )
    registry.versions_by_dataset_id[right_id] = _Version(
        dataset_id=right_id,
        artifact_key=None,
        sample_json={"rows": [{"id": 1, "ts_right": "2026-01-01T00:00:20Z"}]},
    )

    executor = PipelineExecutor(dataset_registry=registry)
    definition = {
        "nodes": [
            {"id": "left", "type": "input", "metadata": {"datasetName": left_name}},
            {"id": "right", "type": "input", "metadata": {"datasetName": right_name}},
            {
                "id": "sj1",
                "type": "transform",
                "metadata": {
                    "operation": "streamJoin",
                    "leftKeys": ["id"],
                    "rightKeys": ["id"],
                    "streamJoin": {
                        "strategy": "dynamic",
                        "leftEventTimeColumn": "ts_left",
                        "rightEventTimeColumn": "ts_right",
                        "allowedLatenessSeconds": 60,
                        "leftCacheExpirationSeconds": 300,
                        "rightCacheExpirationSeconds": 300,
                    },
                },
            },
            {"id": "out1", "type": "output"},
        ],
        "edges": [
            {"from": "left", "to": "sj1"},
            {"from": "right", "to": "sj1"},
            {"from": "sj1", "to": "out1"},
        ],
    }

    preview = await executor.preview(definition=definition, db_name=db_name, node_id="sj1", limit=10)
    assert preview["row_count"] == 2
    rows = preview.get("rows") or []
    assert any(row.get("id") == 1 and row.get("right_id") is None for row in rows)
    assert any(row.get("id") is None and row.get("right_id") == 1 for row in rows)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_executor_stream_join_dynamic_supports_forward_direction() -> None:
    db_name = "demo"
    left_name = "left_stream"
    right_name = "right_stream"
    left_id = "ds-demo-left-main"
    right_id = "ds-demo-right-main"

    registry = _DatasetRegistry()
    registry.datasets_by_name[(db_name, left_name, "master")] = _Dataset(
        dataset_id=left_id,
        db_name=db_name,
        name=left_name,
        branch="master",
        schema_json={"columns": [{"name": "id", "type": "xsd:integer"}, {"name": "ts_left", "type": "xsd:string"}]},
    )
    registry.datasets_by_name[(db_name, right_name, "master")] = _Dataset(
        dataset_id=right_id,
        db_name=db_name,
        name=right_name,
        branch="master",
        schema_json={"columns": [{"name": "id", "type": "xsd:integer"}, {"name": "ts_right", "type": "xsd:string"}]},
    )
    registry.versions_by_dataset_id[left_id] = _Version(
        dataset_id=left_id,
        artifact_key=None,
        sample_json={"rows": [{"id": 1, "ts_left": "2026-01-01T00:00:00Z"}]},
    )
    registry.versions_by_dataset_id[right_id] = _Version(
        dataset_id=right_id,
        artifact_key=None,
        sample_json={"rows": [{"id": 1, "ts_right": "2026-01-01T00:00:20Z"}]},
    )

    executor = PipelineExecutor(dataset_registry=registry)
    definition = {
        "nodes": [
            {"id": "left", "type": "input", "metadata": {"datasetName": left_name}},
            {"id": "right", "type": "input", "metadata": {"datasetName": right_name}},
            {
                "id": "sj1",
                "type": "transform",
                "metadata": {
                    "operation": "streamJoin",
                    "leftKeys": ["id"],
                    "rightKeys": ["id"],
                    "streamJoin": {
                        "strategy": "dynamic",
                        "timeDirection": "forward",
                        "leftEventTimeColumn": "ts_left",
                        "rightEventTimeColumn": "ts_right",
                        "allowedLatenessSeconds": 60,
                        "leftCacheExpirationSeconds": 300,
                        "rightCacheExpirationSeconds": 300,
                    },
                },
            },
            {"id": "out1", "type": "output"},
        ],
        "edges": [
            {"from": "left", "to": "sj1"},
            {"from": "right", "to": "sj1"},
            {"from": "sj1", "to": "out1"},
        ],
    }

    preview = await executor.preview(definition=definition, db_name=db_name, node_id="sj1", limit=10)
    assert preview["row_count"] == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_executor_stream_join_dynamic_applies_cache_expiration_window() -> None:
    db_name = "demo"
    left_name = "left_stream"
    right_name = "right_stream"
    left_id = "ds-demo-left-main"
    right_id = "ds-demo-right-main"

    registry = _DatasetRegistry()
    registry.datasets_by_name[(db_name, left_name, "master")] = _Dataset(
        dataset_id=left_id,
        db_name=db_name,
        name=left_name,
        branch="master",
        schema_json={"columns": [{"name": "id", "type": "xsd:integer"}, {"name": "ts_left", "type": "xsd:string"}]},
    )
    registry.datasets_by_name[(db_name, right_name, "master")] = _Dataset(
        dataset_id=right_id,
        db_name=db_name,
        name=right_name,
        branch="master",
        schema_json={"columns": [{"name": "id", "type": "xsd:integer"}, {"name": "ts_right", "type": "xsd:string"}]},
    )
    registry.versions_by_dataset_id[left_id] = _Version(
        dataset_id=left_id,
        artifact_key=None,
        sample_json={"rows": [{"id": 1, "ts_left": "2026-01-01T00:05:00Z"}]},
    )
    registry.versions_by_dataset_id[right_id] = _Version(
        dataset_id=right_id,
        artifact_key=None,
        sample_json={"rows": [{"id": 1, "ts_right": "2026-01-01T00:03:00Z"}]},
    )

    executor = PipelineExecutor(dataset_registry=registry)
    definition = {
        "nodes": [
            {"id": "left", "type": "input", "metadata": {"datasetName": left_name}},
            {"id": "right", "type": "input", "metadata": {"datasetName": right_name}},
            {
                "id": "sj1",
                "type": "transform",
                "metadata": {
                    "operation": "streamJoin",
                    "leftKeys": ["id"],
                    "rightKeys": ["id"],
                    "streamJoin": {
                        "strategy": "dynamic",
                        "leftEventTimeColumn": "ts_left",
                        "rightEventTimeColumn": "ts_right",
                        "allowedLatenessSeconds": 300,
                        "leftCacheExpirationSeconds": 60,
                        "rightCacheExpirationSeconds": 60,
                    },
                },
            },
            {"id": "out1", "type": "output"},
        ],
        "edges": [
            {"from": "left", "to": "sj1"},
            {"from": "right", "to": "sj1"},
            {"from": "sj1", "to": "out1"},
        ],
    }

    preview = await executor.preview(definition=definition, db_name=db_name, node_id="sj1", limit=10)
    assert preview["row_count"] == 2
    rows = preview.get("rows") or []
    assert any(row.get("id") == 1 and row.get("right_id") is None for row in rows)
    assert any(row.get("id") is None and row.get("right_id") == 1 for row in rows)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_executor_stream_join_dynamic_selects_single_best_match_per_left_row() -> None:
    db_name = "demo"
    left_name = "left_stream"
    right_name = "right_stream"
    left_id = "ds-demo-left-main"
    right_id = "ds-demo-right-main"

    registry = _DatasetRegistry()
    registry.datasets_by_name[(db_name, left_name, "master")] = _Dataset(
        dataset_id=left_id,
        db_name=db_name,
        name=left_name,
        branch="master",
        schema_json={"columns": [{"name": "id", "type": "xsd:integer"}, {"name": "ts_left", "type": "xsd:string"}]},
    )
    registry.datasets_by_name[(db_name, right_name, "master")] = _Dataset(
        dataset_id=right_id,
        db_name=db_name,
        name=right_name,
        branch="master",
        schema_json={
            "columns": [
                {"name": "id", "type": "xsd:integer"},
                {"name": "ts_right", "type": "xsd:string"},
                {"name": "payload", "type": "xsd:string"},
            ]
        },
    )
    registry.versions_by_dataset_id[left_id] = _Version(
        dataset_id=left_id,
        artifact_key=None,
        sample_json={"rows": [{"id": 1, "ts_left": "2026-01-01T00:00:30Z"}]},
    )
    registry.versions_by_dataset_id[right_id] = _Version(
        dataset_id=right_id,
        artifact_key=None,
        sample_json={
            "rows": [
                {"id": 1, "ts_right": "2026-01-01T00:00:10Z", "payload": "old"},
                {"id": 1, "ts_right": "2026-01-01T00:00:25Z", "payload": "latest"},
            ]
        },
    )

    executor = PipelineExecutor(dataset_registry=registry)
    definition = {
        "nodes": [
            {"id": "left", "type": "input", "metadata": {"datasetName": left_name}},
            {"id": "right", "type": "input", "metadata": {"datasetName": right_name}},
            {
                "id": "sj1",
                "type": "transform",
                "metadata": {
                    "operation": "streamJoin",
                    "joinType": "inner",
                    "leftKeys": ["id"],
                    "rightKeys": ["id"],
                    "streamJoin": {
                        "strategy": "dynamic",
                        "timeDirection": "backward",
                        "leftEventTimeColumn": "ts_left",
                        "rightEventTimeColumn": "ts_right",
                        "allowedLatenessSeconds": 60,
                        "leftCacheExpirationSeconds": 300,
                        "rightCacheExpirationSeconds": 300,
                    },
                },
            },
            {"id": "out1", "type": "output"},
        ],
        "edges": [
            {"from": "left", "to": "sj1"},
            {"from": "right", "to": "sj1"},
            {"from": "sj1", "to": "out1"},
        ],
    }

    preview = await executor.preview(definition=definition, db_name=db_name, node_id="sj1", limit=10)
    rows = preview.get("rows") or []
    matched_rows = [row for row in rows if row.get("ts_left") is not None and row.get("ts_right") is not None]
    assert len(matched_rows) == 1
    assert matched_rows[0].get("payload") == "latest"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_executor_stream_join_dynamic_emits_unmatched_rows_as_outer_join() -> None:
    db_name = "demo"
    left_name = "left_stream"
    right_name = "right_stream"
    left_id = "ds-demo-left-main"
    right_id = "ds-demo-right-main"

    registry = _DatasetRegistry()
    registry.datasets_by_name[(db_name, left_name, "master")] = _Dataset(
        dataset_id=left_id,
        db_name=db_name,
        name=left_name,
        branch="master",
        schema_json={"columns": [{"name": "id", "type": "xsd:integer"}, {"name": "ts_left", "type": "xsd:string"}]},
    )
    registry.datasets_by_name[(db_name, right_name, "master")] = _Dataset(
        dataset_id=right_id,
        db_name=db_name,
        name=right_name,
        branch="master",
        schema_json={"columns": [{"name": "id", "type": "xsd:integer"}, {"name": "ts_right", "type": "xsd:string"}]},
    )
    registry.versions_by_dataset_id[left_id] = _Version(
        dataset_id=left_id,
        artifact_key=None,
        sample_json={"rows": [{"id": 1, "ts_left": "2026-01-01T00:00:00Z"}]},
    )
    registry.versions_by_dataset_id[right_id] = _Version(
        dataset_id=right_id,
        artifact_key=None,
        sample_json={"rows": []},
    )

    executor = PipelineExecutor(dataset_registry=registry)
    definition = {
        "nodes": [
            {"id": "left", "type": "input", "metadata": {"datasetName": left_name}},
            {"id": "right", "type": "input", "metadata": {"datasetName": right_name}},
            {
                "id": "sj1",
                "type": "transform",
                "metadata": {
                    "operation": "streamJoin",
                    "leftKeys": ["id"],
                    "rightKeys": ["id"],
                    "streamJoin": {
                        "strategy": "dynamic",
                        "leftEventTimeColumn": "ts_left",
                        "rightEventTimeColumn": "ts_right",
                        "allowedLatenessSeconds": 60,
                        "leftCacheExpirationSeconds": 300,
                        "rightCacheExpirationSeconds": 300,
                    },
                },
            },
            {"id": "out1", "type": "output"},
        ],
        "edges": [
            {"from": "left", "to": "sj1"},
            {"from": "right", "to": "sj1"},
            {"from": "sj1", "to": "out1"},
        ],
    }

    preview = await executor.preview(definition=definition, db_name=db_name, node_id="sj1", limit=10)
    assert preview["row_count"] >= 1
    rows = preview.get("rows") or []
    assert any(row.get("id") == 1 and row.get("ts_right") is None for row in rows)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_executor_stream_join_left_lookup_forces_left_join_semantics() -> None:
    db_name = "demo"
    left_name = "left_stream"
    right_name = "right_lookup"
    left_id = "ds-demo-left-main"
    right_id = "ds-demo-right-main"

    registry = _DatasetRegistry()
    registry.datasets_by_name[(db_name, left_name, "master")] = _Dataset(
        dataset_id=left_id,
        db_name=db_name,
        name=left_name,
        branch="master",
        schema_json={"columns": [{"name": "id", "type": "xsd:integer"}, {"name": "value_left", "type": "xsd:string"}]},
    )
    registry.datasets_by_name[(db_name, right_name, "master")] = _Dataset(
        dataset_id=right_id,
        db_name=db_name,
        name=right_name,
        branch="master",
        schema_json={"columns": [{"name": "id", "type": "xsd:integer"}, {"name": "value_right", "type": "xsd:string"}]},
    )
    registry.versions_by_dataset_id[left_id] = _Version(
        dataset_id=left_id,
        artifact_key=None,
        sample_json={"rows": [{"id": 10, "value_left": "left-only"}]},
    )
    registry.versions_by_dataset_id[right_id] = _Version(
        dataset_id=right_id,
        artifact_key=None,
        sample_json={"rows": []},
    )

    executor = PipelineExecutor(dataset_registry=registry)
    definition = {
        "nodes": [
            {"id": "left", "type": "input", "metadata": {"datasetName": left_name}},
            {"id": "right", "type": "input", "metadata": {"datasetName": right_name}},
            {
                "id": "sj1",
                "type": "transform",
                "metadata": {
                    "operation": "streamJoin",
                    "joinType": "inner",
                    "leftKeys": ["id"],
                    "rightKeys": ["id"],
                    "streamJoin": {
                        "strategy": "left_lookup",
                    },
                },
            },
            {"id": "out1", "type": "output"},
        ],
        "edges": [
            {"from": "left", "to": "sj1"},
            {"from": "right", "to": "sj1"},
            {"from": "sj1", "to": "out1"},
        ],
    }

    preview = await executor.preview(definition=definition, db_name=db_name, node_id="sj1", limit=10)
    assert preview["row_count"] == 1
    row = (preview.get("rows") or [])[0]
    assert row.get("id") == 10
    assert row.get("value_right") is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_executor_stream_join_static_forces_left_join_semantics_even_when_full_requested() -> None:
    db_name = "demo"
    left_name = "left_stream"
    right_name = "right_lookup"
    left_id = "ds-demo-left-main"
    right_id = "ds-demo-right-main"

    registry = _DatasetRegistry()
    registry.datasets_by_name[(db_name, left_name, "master")] = _Dataset(
        dataset_id=left_id,
        db_name=db_name,
        name=left_name,
        branch="master",
        schema_json={"columns": [{"name": "id", "type": "xsd:integer"}, {"name": "value_left", "type": "xsd:string"}]},
    )
    registry.datasets_by_name[(db_name, right_name, "master")] = _Dataset(
        dataset_id=right_id,
        db_name=db_name,
        name=right_name,
        branch="master",
        schema_json={"columns": [{"name": "id", "type": "xsd:integer"}, {"name": "value_right", "type": "xsd:string"}]},
    )
    registry.versions_by_dataset_id[left_id] = _Version(
        dataset_id=left_id,
        artifact_key=None,
        sample_json={"rows": [{"id": 10, "value_left": "left-only"}]},
    )
    registry.versions_by_dataset_id[right_id] = _Version(
        dataset_id=right_id,
        artifact_key=None,
        sample_json={"rows": []},
    )

    executor = PipelineExecutor(dataset_registry=registry)
    definition = {
        "nodes": [
            {"id": "left", "type": "input", "metadata": {"datasetName": left_name}},
            {"id": "right", "type": "input", "metadata": {"datasetName": right_name}},
            {
                "id": "sj1",
                "type": "transform",
                "metadata": {
                    "operation": "streamJoin",
                    "joinType": "full",
                    "leftKeys": ["id"],
                    "rightKeys": ["id"],
                    "streamJoin": {"strategy": "static"},
                },
            },
            {"id": "out1", "type": "output"},
        ],
        "edges": [
            {"from": "left", "to": "sj1"},
            {"from": "right", "to": "sj1"},
            {"from": "sj1", "to": "out1"},
        ],
    }

    preview = await executor.preview(definition=definition, db_name=db_name, node_id="sj1", limit=10)
    assert preview["row_count"] == 1
    row = (preview.get("rows") or [])[0]
    assert row.get("id") == 10
    assert row.get("value_right") is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_executor_stream_join_left_lookup_picks_latest_right_row_per_key_without_event_time() -> None:
    db_name = "demo"
    left_name = "left_stream"
    right_name = "right_lookup"
    left_id = "ds-demo-left-main"
    right_id = "ds-demo-right-main"

    registry = _DatasetRegistry()
    registry.datasets_by_name[(db_name, left_name, "master")] = _Dataset(
        dataset_id=left_id,
        db_name=db_name,
        name=left_name,
        branch="master",
        schema_json={"columns": [{"name": "id", "type": "xsd:integer"}, {"name": "value_left", "type": "xsd:string"}]},
    )
    registry.datasets_by_name[(db_name, right_name, "master")] = _Dataset(
        dataset_id=right_id,
        db_name=db_name,
        name=right_name,
        branch="master",
        schema_json={"columns": [{"name": "id", "type": "xsd:integer"}, {"name": "value_right", "type": "xsd:string"}]},
    )
    registry.versions_by_dataset_id[left_id] = _Version(
        dataset_id=left_id,
        artifact_key=None,
        sample_json={"rows": [{"id": 10, "value_left": "left-only"}]},
    )
    registry.versions_by_dataset_id[right_id] = _Version(
        dataset_id=right_id,
        artifact_key=None,
        sample_json={
            "rows": [
                {"id": 10, "value_right": "right-old"},
                {"id": 10, "value_right": "right-latest"},
            ]
        },
    )

    executor = PipelineExecutor(dataset_registry=registry)
    definition = {
        "nodes": [
            {"id": "left", "type": "input", "metadata": {"datasetName": left_name}},
            {"id": "right", "type": "input", "metadata": {"datasetName": right_name}},
            {
                "id": "sj1",
                "type": "transform",
                "metadata": {
                    "operation": "streamJoin",
                    "joinType": "left",
                    "leftKeys": ["id"],
                    "rightKeys": ["id"],
                    "streamJoin": {"strategy": "left_lookup"},
                },
            },
            {"id": "out1", "type": "output"},
        ],
        "edges": [
            {"from": "left", "to": "sj1"},
            {"from": "right", "to": "sj1"},
            {"from": "sj1", "to": "out1"},
        ],
    }

    preview = await executor.preview(definition=definition, db_name=db_name, node_id="sj1", limit=10)
    assert preview["row_count"] == 1
    row = (preview.get("rows") or [])[0]
    assert row.get("id") == 10
    assert row.get("value_right") == "right-latest"
