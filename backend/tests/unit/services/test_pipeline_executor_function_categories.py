from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pytest

from shared.services.pipeline_executor import PipelineExecutor


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
async def test_function_categories_row_aggregation_generator_are_distinct_and_work() -> None:
    """
    Checklist CL-007:
    - Row-level (compute/add), aggregation (sum), generator (explode) are supported.
    """

    db_name = "demo"
    dataset_name = "ds_fn"
    dataset_id = "ds-demo-fn-main"

    registry = _DatasetRegistry()
    registry.datasets_by_name[(db_name, dataset_name, "main")] = _Dataset(
        dataset_id=dataset_id,
        db_name=db_name,
        name=dataset_name,
        branch="main",
        schema_json={
            "columns": [
                {"name": "group", "type": "xsd:string"},
                {"name": "amount", "type": "xsd:integer"},
                {"name": "items", "type": "xsd:string"},
            ]
        },
    )
    registry.versions_by_dataset_id[dataset_id] = _Version(
        dataset_id=dataset_id,
        artifact_key=None,
        sample_json={
            "rows": [
                {"group": "A", "amount": 1, "items": [1, 2]},
                {"group": "A", "amount": 3, "items": [3]},
                {"group": "B", "amount": 10, "items": []},
            ]
        },
    )

    executor = PipelineExecutor(dataset_registry=registry)

    definition = {
        "nodes": [
            {"id": "in1", "type": "input", "metadata": {"datasetName": dataset_name}},
            {
                "id": "row_add",
                "type": "transform",
                "metadata": {"operation": "compute", "expression": "amount_plus = amount + 1"},
            },
            {
                "id": "agg_sum",
                "type": "transform",
                "metadata": {
                    "operation": "groupBy",
                    "groupBy": ["group"],
                    "aggregates": [{"column": "amount", "op": "sum", "alias": "sum_amount"}],
                },
            },
            {
                "id": "gen_explode",
                "type": "transform",
                "metadata": {"operation": "explode", "columns": ["items"]},
            },
        ],
        "edges": [
            {"from": "in1", "to": "row_add"},
            {"from": "row_add", "to": "agg_sum"},
            {"from": "row_add", "to": "gen_explode"},
        ],
        "parameters": [],
    }

    row_level = await executor.preview(definition=definition, db_name=db_name, node_id="row_add", limit=200)
    assert any(row.get("amount_plus") == 2 for row in row_level.get("rows") or [])

    aggregated = await executor.preview(definition=definition, db_name=db_name, node_id="agg_sum", limit=200)
    rows = aggregated.get("rows") or []
    assert any(row.get("sum_amount") == 4 for row in rows)
    assert any(row.get("sum_amount") == 10 for row in rows)

    generated = await executor.preview(definition=definition, db_name=db_name, node_id="gen_explode", limit=200)
    gen_rows = generated.get("rows") or []
    assert generated.get("row_count") == 3
    values = set()
    for row in gen_rows:
        item = row.get("items")
        if isinstance(item, list):
            values.add(tuple(item))
        else:
            values.add(item)
    assert values == {1, 2, 3}
