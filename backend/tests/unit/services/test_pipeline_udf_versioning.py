from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

import pytest

from shared.services.pipeline_executor import PipelineExecutor
from shared.services.pipeline_registry import PipelineRegistry


def _get_postgres_url_candidates() -> list[str]:
    env_url = (os.getenv("POSTGRES_URL") or "").strip()
    if env_url:
        return [env_url]
    return [
        "postgresql://spiceadmin:spicepass123@localhost:55433/spicedb",
        "postgresql://spiceadmin:spicepass123@localhost:5432/spicedb",
    ]


DEFAULT_POSTGRES_URL = _get_postgres_url_candidates()[0]
TEST_SCHEMA = os.getenv("PIPELINE_UDF_TEST_SCHEMA", "spice_pipelines_udf_test")


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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_udf_can_be_created_reused_and_version_upgraded() -> None:
    """
    Checklist CL-011:
    - Python UDF can be created and reused in a pipeline
    - UDF supports version upgrades
    """

    registry = PipelineRegistry(dsn=DEFAULT_POSTGRES_URL, schema=TEST_SCHEMA)
    await registry.connect()
    try:
        db_name = "demo"

        udf_v1 = """
def transform(row):
    value = int(row.get("id") or 0)
    return {**row, "id_plus": value + 1}
""".strip()

        udf_v2 = """
def transform(row):
    value = int(row.get("id") or 0)
    return {**row, "id_plus": value + 2}
""".strip()

        udf = await registry.create_udf(db_name=db_name, name="add_one", code=udf_v1, description="test udf")
        udf_id = udf.udf_id
        assert udf.latest_version == 1

        # Version upgrade creates v2, leaving v1 intact.
        new_version = await registry.create_udf_version(udf_id=udf_id, code=udf_v2)
        assert new_version.version == 2

        dataset_name = "ds_udf"
        dataset_id = "ds-demo-udf-main"
        ds_registry = _DatasetRegistry()
        ds_registry.datasets_by_name[(db_name, dataset_name, "main")] = _Dataset(
            dataset_id=dataset_id,
            db_name=db_name,
            name=dataset_name,
            branch="main",
            schema_json={"columns": [{"name": "id", "type": "xsd:integer"}]},
        )
        ds_registry.versions_by_dataset_id[dataset_id] = _Version(
            dataset_id=dataset_id,
            artifact_key=None,
            sample_json={"rows": [{"id": 1}, {"id": 2}]},
        )

        executor = PipelineExecutor(dataset_registry=ds_registry, pipeline_registry=registry)

        definition_v1 = {
            "nodes": [
                {"id": "in1", "type": "input", "metadata": {"datasetName": dataset_name}},
                {
                    "id": "t_udf",
                    "type": "transform",
                    "metadata": {"operation": "udf", "udfId": udf_id, "udfVersion": 1},
                },
                {"id": "out1", "type": "output"},
            ],
            "edges": [
                {"from": "in1", "to": "t_udf"},
                {"from": "t_udf", "to": "out1"},
            ],
        }

        preview_v1 = await executor.preview(definition=definition_v1, db_name=db_name, node_id="out1", limit=10)
        assert [row.get("id_plus") for row in preview_v1.get("rows") or []] == [2, 3]

        # Upgrade pipeline to use UDF v2 explicitly.
        definition_v2 = dict(definition_v1)
        definition_v2["nodes"] = [
            {"id": "in1", "type": "input", "metadata": {"datasetName": dataset_name}},
            {
                "id": "t_udf",
                "type": "transform",
                "metadata": {"operation": "udf", "udfId": udf_id, "udfVersion": 2},
            },
            {"id": "out1", "type": "output"},
        ]

        preview_v2 = await executor.preview(definition=definition_v2, db_name=db_name, node_id="out1", limit=10)
        assert [row.get("id_plus") for row in preview_v2.get("rows") or []] == [3, 4]
    finally:
        if registry._pool:
            async with registry._pool.acquire() as conn:
                await conn.execute(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE")
        await registry.close()

