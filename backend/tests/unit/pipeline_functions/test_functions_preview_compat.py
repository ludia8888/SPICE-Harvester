from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pytest

from shared.services.pipeline.pipeline_executor import PipelineExecutor
from shared.tools.foundry_functions_compat import (
    default_snapshot_path,
    filter_functions,
    load_foundry_functions_snapshot,
)


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


async def _build_executor() -> tuple[PipelineExecutor, str, str]:
    db_name = "demo"
    dataset_name = "ds_functions_preview"
    dataset_id = "ds-func-preview-master"

    registry = _DatasetRegistry()
    registry.datasets_by_name[(db_name, dataset_name, "master")] = _Dataset(
        dataset_id=dataset_id,
        db_name=db_name,
        name=dataset_name,
        branch="master",
        schema_json={
            "columns": [
                {"name": "group", "type": "xsd:string"},
                {"name": "amount", "type": "xsd:integer"},
            ]
        },
    )
    registry.versions_by_dataset_id[dataset_id] = _Version(
        dataset_id=dataset_id,
        artifact_key=None,
        sample_json={
            "rows": [
                {"group": "A", "amount": 1},
                {"group": "A", "amount": 3},
                {"group": "B", "amount": 10},
            ]
        },
    )
    return PipelineExecutor(dataset_registry=registry), db_name, dataset_name


async def _run_group_aggregate(op: str) -> dict[str, Any]:
    executor, db_name, dataset_name = await _build_executor()
    alias = f"{op}_amount"
    definition = {
        "nodes": [
            {"id": "in1", "type": "input", "metadata": {"datasetName": dataset_name}},
            {
                "id": "agg1",
                "type": "transform",
                "metadata": {
                    "operation": "groupBy",
                    "groupBy": ["group"],
                    "aggregates": [{"column": "amount", "op": op, "alias": alias}],
                },
            },
        ],
        "edges": [{"from": "in1", "to": "agg1"}],
        "parameters": [],
    }
    preview = await executor.preview(definition=definition, db_name=db_name, node_id="agg1", limit=200)
    rows = preview.get("rows") or []
    by_group = {str(row.get("group")): row.get(alias) for row in rows}
    return by_group


@pytest.mark.unit
@pytest.mark.asyncio
async def test_functions_preview_supported_matrix_contract() -> None:
    entries = load_foundry_functions_snapshot(default_snapshot_path())
    preview_supported = filter_functions(entries, engine="preview", status="supported")

    assert preview_supported, "snapshot must include at least one preview-supported function"

    checks = {
        "sum": {"A": 4, "B": 10},
        "count": {"A": 2, "B": 1},
        "avg": {"A": 2.0, "B": 10.0},
        "min": {"A": 1, "B": 10},
        "max": {"A": 3, "B": 10},
    }

    missing = [entry.name for entry in preview_supported if entry.name not in checks]
    assert not missing, f"missing preview compatibility assertions for: {missing}"

    for entry in preview_supported:
        actual = await _run_group_aggregate(entry.name)
        expected = checks[entry.name]
        assert actual == expected, f"preview compatibility mismatch for {entry.name}: {actual} != {expected}"
