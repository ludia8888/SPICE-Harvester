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


class _StorageService:
    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self._objects = dict(objects)

    async def load_bytes_lines(self, bucket: str, key: str, *, max_lines: int, max_bytes=None) -> bytes:  # noqa: ANN001
        # The executor only needs a small head sample for CSV parsing tests.
        data = self._objects.get((bucket, key), b"")
        lines = data.splitlines()
        trimmed = lines[: max_lines]
        return b"\n".join(trimmed) + (b"\n" if trimmed else b"")

    async def load_bytes(self, bucket: str, key: str) -> bytes:  # noqa: ANN001
        return self._objects.get((bucket, key), b"")

    async def list_objects(self, bucket: str, prefix: str):  # noqa: ANN001
        return []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_executor_parses_quoted_csv_headers_and_joins_correctly() -> None:
    db_name = "demo"

    left_name = "left"
    right_name = "right"
    left_id = "ds-left"
    right_id = "ds-right"

    bucket = "raw-datasets"
    left_key = "left.csv"
    right_key = "right.csv"

    left_csv = (
        '"customer_id","order_id"\n'
        '"c1","o1"\n'
        '"c2","o2"\n'
    ).encode("utf-8")
    right_csv = (
        '"customer_id","customer_unique_id"\n'
        '"c1","u1"\n'
        '"c3","u3"\n'
    ).encode("utf-8")

    registry = _DatasetRegistry()
    registry.datasets_by_name[(db_name, left_name, "main")] = _Dataset(
        dataset_id=left_id,
        db_name=db_name,
        name=left_name,
        branch="main",
        schema_json={"columns": [{"name": "customer_id", "type": "xsd:string"}, {"name": "order_id", "type": "xsd:string"}]},
    )
    registry.datasets_by_name[(db_name, right_name, "main")] = _Dataset(
        dataset_id=right_id,
        db_name=db_name,
        name=right_name,
        branch="main",
        schema_json={"columns": [{"name": "customer_id", "type": "xsd:string"}, {"name": "customer_unique_id", "type": "xsd:string"}]},
    )
    registry.versions_by_dataset_id[left_id] = _Version(
        dataset_id=left_id,
        artifact_key=f"s3://{bucket}/{left_key}",
        sample_json={},
    )
    registry.versions_by_dataset_id[right_id] = _Version(
        dataset_id=right_id,
        artifact_key=f"s3://{bucket}/{right_key}",
        sample_json={},
    )

    executor = PipelineExecutor(
        dataset_registry=registry,
        storage_service=_StorageService({(bucket, left_key): left_csv, (bucket, right_key): right_csv}),  # type: ignore[arg-type]
    )

    definition = {
        "__preview_meta__": {"sample_limit": 10},
        "nodes": [
            {"id": "in_left", "type": "input", "metadata": {"datasetName": left_name}},
            {"id": "in_right", "type": "input", "metadata": {"datasetName": right_name}},
            {
                "id": "j1",
                "type": "transform",
                "metadata": {"operation": "join", "joinType": "inner", "leftKeys": ["customer_id"], "rightKeys": ["customer_id"]},
            },
            {"id": "sel", "type": "transform", "metadata": {"operation": "select", "columns": ["order_id", "customer_unique_id"]}},
            {"id": "out", "type": "output"},
        ],
        "edges": [
            {"from": "in_left", "to": "j1"},
            {"from": "in_right", "to": "j1"},
            {"from": "j1", "to": "sel"},
            {"from": "sel", "to": "out"},
        ],
    }

    preview = await executor.preview(definition=definition, db_name=db_name, node_id="out", limit=20)
    assert preview["row_count"] == 1
    assert preview["rows"] == [{"order_id": "o1", "customer_unique_id": "u1"}]
