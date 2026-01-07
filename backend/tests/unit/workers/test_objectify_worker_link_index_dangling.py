from types import SimpleNamespace

import pytest

from objectify_worker.main import ObjectifyNonRetryableError, ObjectifyWorker
from shared.models.objectify_job import ObjectifyJob
from shared.services.sheet_import_service import FieldMapping


class _StubObjectifyRegistry:
    def __init__(self) -> None:
        self.last_status = None

    async def update_objectify_job_status(self, **kwargs):  # noqa: ANN003
        self.last_status = kwargs


class _StubDatasetRegistry:
    def __init__(self, *, edits=None) -> None:
        self.records = []
        self._edits = edits or []

    async def record_relationship_index_result(self, **kwargs):  # noqa: ANN003
        self.records.append(dict(kwargs))

    async def list_link_edits(self, **kwargs):  # noqa: ANN003
        return list(self._edits)


class _DummyWorker(ObjectifyWorker):
    def __init__(self, *, rows, target_ids, ids_by_class=None):
        super().__init__()
        self._rows = rows
        self._target_ids = target_ids
        self._ids_by_class = ids_by_class or {}
        self.objectify_registry = _StubObjectifyRegistry()
        self.dataset_registry = None

    async def _iter_dataset_batches(self, **kwargs):  # noqa: ANN003
        yield ["source_id", "target_id"], self._rows, 0

    async def _fetch_object_type_contract(self, job):  # noqa: ANN003
        return {
            "data": {
                "spec": {
                    "status": "ACTIVE",
                    "pk_spec": {"primary_key": ["source_id"], "title_key": ["source_id"]},
                }
            }
        }

    async def _fetch_ontology_version(self, job):  # noqa: ANN003
        return {}

    async def _bulk_update_instances(self, job, updates, *, ontology_version=None):  # noqa: ANN003
        return {"command_id": "cmd-1"}

    async def _iter_class_instance_ids(self, *, db_name, class_id, branch, limit=1000):  # noqa: ANN003
        _ = db_name, class_id, branch, limit
        ids = self._ids_by_class.get(class_id, self._target_ids)
        for value in ids:
            yield value

    async def _record_gate_result(self, **kwargs):  # noqa: ANN003
        return None


class _CaptureWorker(_DummyWorker):
    def __init__(self, *, rows, target_ids, ids_by_class=None):
        super().__init__(rows=rows, target_ids=target_ids, ids_by_class=ids_by_class)
        self.last_updates = None

    async def _bulk_update_instances(self, job, updates, *, ontology_version=None):  # noqa: ANN003
        _ = job, ontology_version
        self.last_updates = updates
        return {"command_id": "cmd-1"}


def _link_job(*, dangling_policy):
    return ObjectifyJob(
        job_id="job-1",
        db_name="test_db",
        dataset_id="ds-1",
        dataset_version_id="ver-1",
        artifact_output_name="join_table",
        dedupe_key="dedupe",
        dataset_branch="main",
        artifact_key="s3://bucket/key",
        mapping_spec_id="map-1",
        mapping_spec_version=1,
        target_class_id="Source",
        options={"dangling_policy": dangling_policy, "relationship_kind": "join_table"},
    )


def _object_backed_job(*, dangling_policy):
    return ObjectifyJob(
        job_id="job-1",
        db_name="test_db",
        dataset_id="ds-1",
        dataset_version_id="ver-1",
        artifact_output_name="relationship_ds",
        dedupe_key="dedupe",
        dataset_branch="main",
        artifact_key="s3://bucket/key",
        mapping_spec_id="map-1",
        mapping_spec_version=1,
        target_class_id="Source",
        options={"dangling_policy": dangling_policy, "relationship_kind": "object_backed"},
    )


def _fk_job(*, dangling_policy):
    return ObjectifyJob(
        job_id="job-1",
        db_name="test_db",
        dataset_id="ds-1",
        dataset_version_id="ver-1",
        artifact_output_name="source_ds",
        dedupe_key="dedupe",
        dataset_branch="main",
        artifact_key="s3://bucket/key",
        mapping_spec_id="map-1",
        mapping_spec_version=1,
        target_class_id="Source",
        options={"dangling_policy": dangling_policy, "relationship_kind": "foreign_key"},
    )


@pytest.mark.asyncio
async def test_link_index_fails_on_missing_target_when_policy_fail():
    worker = _DummyWorker(rows=[["s1", "missing"]], target_ids={"t1"})
    job = _link_job(dangling_policy="FAIL")
    mapping_spec = SimpleNamespace(mapping_spec_id="map-1", version=1, target_class_id="Source", options={})
    mappings = [
        FieldMapping(source_field="source_id", target_field="source_id"),
        FieldMapping(source_field="target_id", target_field="linked_to"),
    ]
    relationship_mappings = [FieldMapping(source_field="target_id", target_field="linked_to")]

    with pytest.raises(ObjectifyNonRetryableError):
        await worker._run_link_index_job(
            job=job,
            mapping_spec=mapping_spec,
            options=job.options,
            mappings=mappings,
            mapping_sources=["source_id", "target_id"],
            mapping_targets=["source_id", "linked_to"],
            sources_by_target={"source_id": ["source_id"], "linked_to": ["target_id"]},
            prop_map={"source_id": {"type": "xsd:string"}},
            rel_map={"linked_to": {"target": "Target", "cardinality": "1:n"}},
            relationship_mappings=relationship_mappings,
            stable_seed="seed",
            row_batch_size=10,
            max_rows=None,
        )

    assert worker.objectify_registry.last_status["status"] == "FAILED"


@pytest.mark.asyncio
async def test_link_index_warns_on_missing_target_when_policy_warn():
    worker = _DummyWorker(rows=[["s1", "missing"]], target_ids={"t1"})
    job = _link_job(dangling_policy="WARN")
    mapping_spec = SimpleNamespace(mapping_spec_id="map-1", version=1, target_class_id="Source", options={})
    mappings = [
        FieldMapping(source_field="source_id", target_field="source_id"),
        FieldMapping(source_field="target_id", target_field="linked_to"),
    ]
    relationship_mappings = [FieldMapping(source_field="target_id", target_field="linked_to")]

    await worker._run_link_index_job(
        job=job,
        mapping_spec=mapping_spec,
        options=job.options,
        mappings=mappings,
        mapping_sources=["source_id", "target_id"],
        mapping_targets=["source_id", "linked_to"],
        sources_by_target={"source_id": ["source_id"], "linked_to": ["target_id"]},
        prop_map={"source_id": {"type": "xsd:string"}},
        rel_map={"linked_to": {"target": "Target", "cardinality": "1:n"}},
        relationship_mappings=relationship_mappings,
        stable_seed="seed",
        row_batch_size=10,
        max_rows=None,
    )

    assert worker.objectify_registry.last_status["status"] == "SUBMITTED"
    stats = worker.objectify_registry.last_status["report"]["stats"]
    assert stats["dangling_missing_targets"] == 1


@pytest.mark.asyncio
async def test_link_index_creates_link_when_fk_matches_target():
    worker = _CaptureWorker(rows=[["s1", "t1"]], target_ids={"t1"})
    job = _fk_job(dangling_policy="FAIL")
    mapping_spec = SimpleNamespace(mapping_spec_id="map-1", version=1, target_class_id="Source", options={})
    mappings = [
        FieldMapping(source_field="source_id", target_field="source_id"),
        FieldMapping(source_field="target_id", target_field="linked_to"),
    ]
    relationship_mappings = [FieldMapping(source_field="target_id", target_field="linked_to")]

    await worker._run_link_index_job(
        job=job,
        mapping_spec=mapping_spec,
        options=job.options,
        mappings=mappings,
        mapping_sources=["source_id", "target_id"],
        mapping_targets=["source_id", "linked_to"],
        sources_by_target={"source_id": ["source_id"], "linked_to": ["target_id"]},
        prop_map={"source_id": {"type": "xsd:string"}},
        rel_map={"linked_to": {"target": "Target", "cardinality": "1:1"}},
        relationship_mappings=relationship_mappings,
        stable_seed="seed",
        row_batch_size=10,
        max_rows=None,
    )

    assert worker.last_updates == [
        {"instance_id": "s1", "data": {"linked_to": "Target/t1"}}
    ]


@pytest.mark.asyncio
async def test_link_index_dedupes_duplicate_pairs_for_join_table():
    worker = _CaptureWorker(
        rows=[["s1", "t1"], ["s1", "t1"]],
        target_ids={"t1"},
        ids_by_class={"Source": {"s1"}, "Target": {"t1"}},
    )
    job = _link_job(dangling_policy="FAIL")
    mapping_spec = SimpleNamespace(mapping_spec_id="map-1", version=1, target_class_id="Source", options={})
    mappings = [
        FieldMapping(source_field="source_id", target_field="source_id"),
        FieldMapping(source_field="target_id", target_field="linked_to"),
    ]
    relationship_mappings = [FieldMapping(source_field="target_id", target_field="linked_to")]

    await worker._run_link_index_job(
        job=job,
        mapping_spec=mapping_spec,
        options=job.options,
        mappings=mappings,
        mapping_sources=["source_id", "target_id"],
        mapping_targets=["source_id", "linked_to"],
        sources_by_target={"source_id": ["source_id"], "linked_to": ["target_id"]},
        prop_map={"source_id": {"type": "xsd:string"}},
        rel_map={"linked_to": {"target": "Target", "cardinality": "1:n"}},
        relationship_mappings=relationship_mappings,
        stable_seed="seed",
        row_batch_size=10,
        max_rows=None,
    )

    assert worker.last_updates == [
        {"instance_id": "s1", "data": {"linked_to": ["Target/t1"]}}
    ]


@pytest.mark.asyncio
async def test_object_backed_link_index_creates_link():
    worker = _CaptureWorker(
        rows=[["s1", "t1"]],
        target_ids={"t1"},
        ids_by_class={"Source": {"s1"}, "Target": {"t1"}},
    )
    job = _object_backed_job(dangling_policy="FAIL")
    mapping_spec = SimpleNamespace(mapping_spec_id="map-1", version=1, target_class_id="Source", options={})
    mappings = [
        FieldMapping(source_field="source_id", target_field="source_id"),
        FieldMapping(source_field="target_id", target_field="linked_to"),
    ]
    relationship_mappings = [FieldMapping(source_field="target_id", target_field="linked_to")]

    await worker._run_link_index_job(
        job=job,
        mapping_spec=mapping_spec,
        options=job.options,
        mappings=mappings,
        mapping_sources=["source_id", "target_id"],
        mapping_targets=["source_id", "linked_to"],
        sources_by_target={"source_id": ["source_id"], "linked_to": ["target_id"]},
        prop_map={"source_id": {"type": "xsd:string"}},
        rel_map={"linked_to": {"target": "Target", "cardinality": "1:n"}},
        relationship_mappings=relationship_mappings,
        stable_seed="seed",
        row_batch_size=10,
        max_rows=None,
    )

    assert worker.last_updates == [
        {"instance_id": "s1", "data": {"linked_to": ["Target/t1"]}}
    ]


@pytest.mark.asyncio
async def test_object_backed_full_sync_clears_links_when_no_rows():
    worker = _CaptureWorker(
        rows=[],
        target_ids={"t1"},
        ids_by_class={"Source": {"s1"}, "Target": {"t1"}},
    )
    job = _object_backed_job(dangling_policy="FAIL")
    mapping_spec = SimpleNamespace(mapping_spec_id="map-1", version=1, target_class_id="Source", options={})
    mappings = [
        FieldMapping(source_field="source_id", target_field="source_id"),
        FieldMapping(source_field="target_id", target_field="linked_to"),
    ]
    relationship_mappings = [FieldMapping(source_field="target_id", target_field="linked_to")]

    await worker._run_link_index_job(
        job=job,
        mapping_spec=mapping_spec,
        options=job.options,
        mappings=mappings,
        mapping_sources=["source_id", "target_id"],
        mapping_targets=["source_id", "linked_to"],
        sources_by_target={"source_id": ["source_id"], "linked_to": ["target_id"]},
        prop_map={"source_id": {"type": "xsd:string"}},
        rel_map={"linked_to": {"target": "Target", "cardinality": "1:n"}},
        relationship_mappings=relationship_mappings,
        stable_seed="seed",
        row_batch_size=10,
        max_rows=None,
    )

    assert worker.last_updates == [
        {"instance_id": "s1", "data": {"linked_to": []}}
    ]


@pytest.mark.asyncio
async def test_link_index_records_pass_result_with_lineage():
    registry = _StubDatasetRegistry()
    worker = _CaptureWorker(
        rows=[["s1", "t1"]],
        target_ids={"t1"},
        ids_by_class={"Source": {"s1"}, "Target": {"t1"}},
    )
    worker.dataset_registry = registry
    job = _link_job(dangling_policy="FAIL")
    job.options.update({"relationship_spec_id": "rel-1", "link_type_id": "link-1"})
    mapping_spec = SimpleNamespace(mapping_spec_id="map-1", version=1, target_class_id="Source", options={})
    mappings = [
        FieldMapping(source_field="source_id", target_field="source_id"),
        FieldMapping(source_field="target_id", target_field="linked_to"),
    ]
    relationship_mappings = [FieldMapping(source_field="target_id", target_field="linked_to")]

    await worker._run_link_index_job(
        job=job,
        mapping_spec=mapping_spec,
        options=job.options,
        mappings=mappings,
        mapping_sources=["source_id", "target_id"],
        mapping_targets=["source_id", "linked_to"],
        sources_by_target={"source_id": ["source_id"], "linked_to": ["target_id"]},
        prop_map={"source_id": {"type": "xsd:string"}},
        rel_map={"linked_to": {"target": "Target", "cardinality": "1:n"}},
        relationship_mappings=relationship_mappings,
        stable_seed="seed",
        row_batch_size=10,
        max_rows=None,
    )

    assert registry.records
    record = registry.records[0]
    assert record["relationship_spec_id"] == "rel-1"
    assert record["status"] == "PASS"
    assert record["lineage"]["dataset_version_id"] == "ver-1"
    assert record["stats"]["relationship_kind"] == "join_table"


@pytest.mark.asyncio
async def test_link_index_records_warn_when_dangling_policy_warn():
    registry = _StubDatasetRegistry()
    worker = _CaptureWorker(
        rows=[["s1", "missing"]],
        target_ids={"t1"},
        ids_by_class={"Source": {"s1"}, "Target": {"t1"}},
    )
    worker.dataset_registry = registry
    job = _link_job(dangling_policy="WARN")
    job.options.update({"relationship_spec_id": "rel-1", "link_type_id": "link-1"})
    mapping_spec = SimpleNamespace(mapping_spec_id="map-1", version=1, target_class_id="Source", options={})
    mappings = [
        FieldMapping(source_field="source_id", target_field="source_id"),
        FieldMapping(source_field="target_id", target_field="linked_to"),
    ]
    relationship_mappings = [FieldMapping(source_field="target_id", target_field="linked_to")]

    await worker._run_link_index_job(
        job=job,
        mapping_spec=mapping_spec,
        options=job.options,
        mappings=mappings,
        mapping_sources=["source_id", "target_id"],
        mapping_targets=["source_id", "linked_to"],
        sources_by_target={"source_id": ["source_id"], "linked_to": ["target_id"]},
        prop_map={"source_id": {"type": "xsd:string"}},
        rel_map={"linked_to": {"target": "Target", "cardinality": "1:n"}},
        relationship_mappings=relationship_mappings,
        stable_seed="seed",
        row_batch_size=10,
        max_rows=None,
    )

    assert registry.records
    record = registry.records[0]
    assert record["status"] == "WARN"
    assert any(err.get("code") == "RELATIONSHIP_TARGET_MISSING" for err in record["errors"])


@pytest.mark.asyncio
async def test_link_index_records_fail_when_dangling_policy_fail():
    registry = _StubDatasetRegistry()
    worker = _CaptureWorker(
        rows=[["s1", "missing"]],
        target_ids={"t1"},
        ids_by_class={"Source": {"s1"}, "Target": {"t1"}},
    )
    worker.dataset_registry = registry
    job = _link_job(dangling_policy="FAIL")
    job.options.update({"relationship_spec_id": "rel-1", "link_type_id": "link-1"})
    mapping_spec = SimpleNamespace(mapping_spec_id="map-1", version=1, target_class_id="Source", options={})
    mappings = [
        FieldMapping(source_field="source_id", target_field="source_id"),
        FieldMapping(source_field="target_id", target_field="linked_to"),
    ]
    relationship_mappings = [FieldMapping(source_field="target_id", target_field="linked_to")]

    with pytest.raises(ObjectifyNonRetryableError):
        await worker._run_link_index_job(
            job=job,
            mapping_spec=mapping_spec,
            options=job.options,
            mappings=mappings,
            mapping_sources=["source_id", "target_id"],
            mapping_targets=["source_id", "linked_to"],
            sources_by_target={"source_id": ["source_id"], "linked_to": ["target_id"]},
            prop_map={"source_id": {"type": "xsd:string"}},
            rel_map={"linked_to": {"target": "Target", "cardinality": "1:n"}},
            relationship_mappings=relationship_mappings,
            stable_seed="seed",
            row_batch_size=10,
            max_rows=None,
        )

    assert registry.records
    record = registry.records[0]
    assert record["status"] == "FAIL"


@pytest.mark.asyncio
async def test_link_edits_are_applied_to_updates():
    edits = [
        SimpleNamespace(
            source_instance_id="s1",
            target_instance_id="t2",
            predicate="linked_to",
            edit_type="ADD",
        ),
        SimpleNamespace(
            source_instance_id="s1",
            target_instance_id="t1",
            predicate="linked_to",
            edit_type="REMOVE",
        ),
    ]
    registry = _StubDatasetRegistry(edits=edits)
    worker = _CaptureWorker(
        rows=[["s1", "t1"]],
        target_ids={"t1", "t2"},
        ids_by_class={"Source": {"s1"}, "Target": {"t1", "t2"}},
    )
    worker.dataset_registry = registry
    job = _link_job(dangling_policy="FAIL")
    job.options.update({"link_type_id": "link-1"})
    mapping_spec = SimpleNamespace(mapping_spec_id="map-1", version=1, target_class_id="Source", options={})
    mappings = [
        FieldMapping(source_field="source_id", target_field="source_id"),
        FieldMapping(source_field="target_id", target_field="linked_to"),
    ]
    relationship_mappings = [FieldMapping(source_field="target_id", target_field="linked_to")]

    await worker._run_link_index_job(
        job=job,
        mapping_spec=mapping_spec,
        options=job.options,
        mappings=mappings,
        mapping_sources=["source_id", "target_id"],
        mapping_targets=["source_id", "linked_to"],
        sources_by_target={"source_id": ["source_id"], "linked_to": ["target_id"]},
        prop_map={"source_id": {"type": "xsd:string"}},
        rel_map={"linked_to": {"target": "Target", "cardinality": "1:n"}},
        relationship_mappings=relationship_mappings,
        stable_seed="seed",
        row_batch_size=10,
        max_rows=None,
    )

    assert worker.last_updates == [
        {"instance_id": "s1", "data": {"linked_to": ["Target/t2"]}}
    ]
