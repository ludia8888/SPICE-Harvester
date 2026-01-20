from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pytest

from bff.services.pipeline_join_evaluator import evaluate_pipeline_joins


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
async def test_join_evaluator_reports_coverage_and_explosion() -> None:
    db_name = "demo"
    left_name = "left"
    right_name = "right"
    left_id = "ds-left"
    right_id = "ds-right"

    registry = _DatasetRegistry()
    registry.datasets_by_name[(db_name, left_name, "main")] = _Dataset(
        dataset_id=left_id,
        db_name=db_name,
        name=left_name,
        branch="main",
        schema_json={"columns": [{"name": "id", "type": "xsd:integer"}]},
    )
    registry.datasets_by_name[(db_name, right_name, "main")] = _Dataset(
        dataset_id=right_id,
        db_name=db_name,
        name=right_name,
        branch="main",
        schema_json={"columns": [{"name": "id", "type": "xsd:integer"}]},
    )
    registry.versions_by_dataset_id[left_id] = _Version(
        dataset_id=left_id,
        artifact_key=None,
        sample_json={"rows": [{"id": 1}, {"id": 2}, {"id": 3}]},
    )
    registry.versions_by_dataset_id[right_id] = _Version(
        dataset_id=right_id,
        artifact_key=None,
        sample_json={"rows": [{"id": 1}, {"id": 1}, {"id": 3}]},
    )

    definition = {
        "nodes": [
            {"id": "l1", "type": "input", "metadata": {"datasetName": left_name}},
            {"id": "r1", "type": "input", "metadata": {"datasetName": right_name}},
            {
                "id": "j1",
                "type": "transform",
                "metadata": {"operation": "join", "joinType": "inner", "leftKey": "id", "rightKey": "id"},
            },
            {"id": "out1", "type": "output"},
        ],
        "edges": [
            {"from": "l1", "to": "j1"},
            {"from": "r1", "to": "j1"},
            {"from": "j1", "to": "out1"},
        ],
    }

    evaluations, warnings = await evaluate_pipeline_joins(
        definition_json=definition,
        db_name=db_name,
        dataset_registry=registry,
    )

    assert warnings == []
    assert len(evaluations) == 1
    metrics = evaluations[0]
    assert metrics.left_row_count == 3
    assert metrics.right_row_count == 3
    assert metrics.output_row_count == 3
    assert metrics.left_coverage == pytest.approx(0.6667, rel=1e-3)
    assert metrics.right_coverage == pytest.approx(1.0, rel=1e-3)
    assert metrics.explosion_ratio == pytest.approx(1.0, rel=1e-3)
