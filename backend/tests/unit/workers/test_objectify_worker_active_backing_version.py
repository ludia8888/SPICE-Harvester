from __future__ import annotations

from types import SimpleNamespace

import pytest

from objectify_worker.main import ObjectifyWorker
from shared.models.objectify_job import ObjectifyJob


class _HttpClient:
    def __init__(self) -> None:
        self.put_calls: list[dict[str, object]] = []

    async def put(self, path: str, *, params: dict[str, object], json: dict[str, object]):
        self.put_calls.append({"path": path, "params": params, "json": json})
        return SimpleNamespace(status_code=200, text="")


class _Worker(ObjectifyWorker):
    def __init__(self, *, object_type_spec: dict[str, object]) -> None:
        super().__init__()
        self._object_type_spec = object_type_spec

    async def _fetch_object_type_contract(self, job):  # noqa: ANN003
        _ = job
        return {"data": {"spec": self._object_type_spec}}


def _job() -> ObjectifyJob:
    return ObjectifyJob(
        job_id="job-1",
        db_name="demo",
        dataset_id="dataset-1",
        dataset_version_id="version-1",
        dedupe_key="dedupe-1",
        dataset_branch="main",
        artifact_key="s3://bucket/orders.parquet",
        mapping_spec_id=None,
        target_class_id="Order",
        ontology_branch="main",
    )


@pytest.mark.asyncio
async def test_update_object_type_active_version_prefers_backing_ref_scoped_lookup() -> None:
    worker = _Worker(
        object_type_spec={
            "backing_source": {
                "ref": "backing-expected",
                "schema_hash": "schema-old",
            }
        }
    )
    worker.http = _HttpClient()

    class _Registry:
        def __init__(self) -> None:
            self.strict_calls: list[tuple[str, str]] = []
            self.ambiguous_calls: list[str] = []

        async def get_backing_datasource_version_for_dataset(self, *, backing_id: str, dataset_version_id: str):
            self.strict_calls.append((backing_id, dataset_version_id))
            return SimpleNamespace(
                version_id="backing-version-2",
                backing_id=backing_id,
                schema_hash="schema-new",
            )

        async def get_backing_datasource_version_by_dataset_version(self, *, dataset_version_id: str):
            self.ambiguous_calls.append(dataset_version_id)
            raise AssertionError("ambiguous dataset-version lookup should not be used when backing ref is present")

    worker.dataset_registry = _Registry()

    await worker._update_object_type_active_version(
        job=_job(),
        mapping_spec=SimpleNamespace(backing_datasource_version_id=None),
    )

    assert worker.dataset_registry.strict_calls == [("backing-expected", "version-1")]
    assert worker.dataset_registry.ambiguous_calls == []
    assert len(worker.http.put_calls) == 1
    payload = worker.http.put_calls[0]["json"]
    spec = payload["spec"]
    assert spec["backing_source"]["ref"] == "backing-expected"
    assert spec["backing_source"]["version_id"] == "backing-version-2"
    assert spec["backing_source"]["dataset_version_id"] == "version-1"
