from __future__ import annotations

import pytest

from objectify_worker.main import ObjectifyWorker
from shared.models.objectify_job import ObjectifyJob


class _OmsMappingWorker(ObjectifyWorker):
    def __init__(self, *, object_type_spec):
        super().__init__()
        self._object_type_spec = object_type_spec

    async def _fetch_object_type_contract(self, job):  # noqa: ANN003
        _ = job
        return {"data": {"spec": self._object_type_spec}}


def _build_job(*, dataset_id: str = "ds-1") -> ObjectifyJob:
    return ObjectifyJob(
        job_id="job-1",
        db_name="test_db",
        dataset_id=dataset_id,
        dataset_version_id="ver-1",
        dedupe_key="dedupe-1",
        dataset_branch="main",
        artifact_key="s3://bucket/ds.csv",
        mapping_spec_id=None,
        target_class_id="Order",
        ontology_branch="main",
    )


async def _fail_job(error: str):  # noqa: ANN001
    raise RuntimeError(error)


@pytest.mark.asyncio
async def test_resolve_mapping_spec_uses_backing_sources_when_backing_source_missing():
    worker = _OmsMappingWorker(
        object_type_spec={
            "status": "ACTIVE",
            "backing_sources": [
                {
                    "dataset_id": "ds-1",
                    "dataset_branch": "main",
                    "property_mappings": [
                        {"source_field": "order_id", "target_field": "order_id"},
                    ],
                }
            ],
        }
    )
    mapping_spec = await worker._resolve_mapping_spec_for_job(job=_build_job(), fail_job=_fail_job)
    assert mapping_spec.mapping_spec_id == "oms:Order"
    assert mapping_spec.dataset_id == "ds-1"
    assert mapping_spec.mappings == [{"source_field": "order_id", "target_field": "order_id"}]


@pytest.mark.asyncio
async def test_resolve_mapping_spec_rejects_job_dataset_mismatch_with_backing_sources():
    worker = _OmsMappingWorker(
        object_type_spec={
            "status": "ACTIVE",
            "backing_sources": [
                {
                    "dataset_id": "ds-contract",
                    "dataset_branch": "main",
                    "property_mappings": [
                        {"source_field": "order_id", "target_field": "order_id"},
                    ],
                }
            ],
        }
    )
    with pytest.raises(RuntimeError, match="oms_backing_dataset_mismatch"):
        await worker._resolve_mapping_spec_for_job(job=_build_job(dataset_id="ds-job"), fail_job=_fail_job)

