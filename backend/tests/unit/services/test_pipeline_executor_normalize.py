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
async def test_normalize_transform_trims_and_nulls() -> None:
    db_name = "demo"
    dataset_name = "ds_norm"
    dataset_id = "ds-demo-norm-main"

    registry = _DatasetRegistry()
    registry.datasets_by_name[(db_name, dataset_name, "main")] = _Dataset(
        dataset_id=dataset_id,
        db_name=db_name,
        name=dataset_name,
        branch="main",
        schema_json={"columns": [{"name": "name", "type": "xsd:string"}, {"name": "code", "type": "xsd:string"}]},
    )
    registry.versions_by_dataset_id[dataset_id] = _Version(
        dataset_id=dataset_id,
        artifact_key=None,
        sample_json={
            "rows": [
                {"name": " Alice ", "code": ""},
                {"name": "Bob", "code": "   "},
            ]
        },
    )

    executor = PipelineExecutor(dataset_registry=registry)

    definition = {
        "nodes": [
            {"id": "in1", "type": "input", "metadata": {"datasetName": dataset_name}},
            {
                "id": "norm1",
                "type": "transform",
                "metadata": {
                    "operation": "normalize",
                    "columns": ["name", "code"],
                    "trim": True,
                    "emptyToNull": True,
                    "whitespaceToNull": True,
                },
            },
            {"id": "out1", "type": "output"},
        ],
        "edges": [
            {"from": "in1", "to": "norm1"},
            {"from": "norm1", "to": "out1"},
        ],
        "parameters": [],
    }

    preview = await executor.preview(definition=definition, db_name=db_name, node_id="norm1", limit=10)
    rows = preview.get("rows") or []
    assert rows[0]["name"] == "Alice"
    assert rows[0]["code"] is None
    assert rows[1]["name"] == "Bob"
    assert rows[1]["code"] is None
