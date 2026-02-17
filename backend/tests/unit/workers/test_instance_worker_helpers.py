from __future__ import annotations

from types import SimpleNamespace

import pytest

from instance_worker.main import StrictInstanceWorker
from shared.models.event_envelope import EventEnvelope


@pytest.mark.asyncio
async def test_extract_payload_from_message_success() -> None:
    worker = StrictInstanceWorker()
    envelope = EventEnvelope(
        event_type="COMMAND",
        aggregate_type="instance",
        aggregate_id="agg-1",
        data={"aggregate_id": "agg-1", "payload": {"name": "x"}},
        metadata={"kind": "command"},
    )

    command = await worker.extract_payload_from_message(envelope.model_dump(mode="json"))
    assert command["aggregate_id"] == "agg-1"
    assert command["event_id"] == envelope.event_id


@pytest.mark.asyncio
async def test_extract_payload_from_message_rejects_non_command() -> None:
    worker = StrictInstanceWorker()
    envelope = EventEnvelope(
        event_type="DOMAIN",
        aggregate_type="instance",
        aggregate_id="agg-1",
        data={},
        metadata={"kind": "domain"},
    )

    with pytest.raises(ValueError):
        await worker.extract_payload_from_message(envelope.model_dump(mode="json"))


def test_primary_key_and_objectify_helpers() -> None:
    worker = StrictInstanceWorker()

    payload = {"customer_id": "c1"}
    assert worker.get_primary_key_value("Customer", payload) == "c1"

    payload = {"id": "x", "order_id": "o1"}
    assert worker.get_primary_key_value("Order", payload) == "o1"

    with pytest.raises(ValueError):
        worker.get_primary_key_value("Order", {"name": "x"}, allow_generate=False)

    assert worker._is_objectify_command({"metadata": {"mapping_spec_id": "m1"}}) is True
    assert worker._is_objectify_command({"metadata": {}}) is False


def test_retryable_error_detection() -> None:
    assert StrictInstanceWorker._is_retryable_error(Exception("ConnectionError: node unreachable")) is True
    assert StrictInstanceWorker._is_retryable_error(Exception("timeout waiting for response")) is True
    assert StrictInstanceWorker._is_retryable_error(Exception("409 conflict on version")) is True
    assert StrictInstanceWorker._is_retryable_error(Exception("invalid payload")) is False


@pytest.mark.asyncio
async def test_enqueue_link_reindex_uses_registry_job_enqueue() -> None:
    worker = StrictInstanceWorker()
    enqueued_jobs = []

    class _DatasetRegistryStub:
        async def get_relationship_spec(self, *, link_type_id: str):
            assert link_type_id == "link-1"
            return SimpleNamespace(
                relationship_spec_id="rel-1",
                mapping_spec_id="map-1",
                mapping_spec_version=3,
                dataset_id="dataset-1",
                dataset_version_id="version-1",
            )

        async def get_dataset(self, *, dataset_id: str):
            assert dataset_id == "dataset-1"
            return SimpleNamespace(dataset_id="dataset-1", branch="main", name="join_ds")

        async def get_version(self, *, version_id: str):
            assert version_id == "version-1"
            return SimpleNamespace(dataset_id="dataset-1", version_id="version-1", artifact_key="s3://bucket/key.parquet")

        async def get_latest_version(self, *, dataset_id: str):
            raise AssertionError("latest version fallback should not be used in this scenario")

    class _ObjectifyRegistryStub:
        async def get_mapping_spec(self, *, mapping_spec_id: str):
            assert mapping_spec_id == "map-1"
            return SimpleNamespace(
                version=3,
                target_class_id="Source",
                options={"mode": "link_index", "relationship_kind": "join_table"},
            )

        def build_dedupe_key(self, **kwargs):
            assert kwargs["dataset_id"] == "dataset-1"
            assert kwargs["mapping_spec_id"] == "map-1"
            return "dedupe-key"

        async def get_objectify_job_by_dedupe_key(self, *, dedupe_key: str):
            assert dedupe_key == "dedupe-key"
            return None

        async def enqueue_objectify_job(self, *, job):
            enqueued_jobs.append(job)

    worker.dataset_registry = _DatasetRegistryStub()
    worker.objectify_registry = _ObjectifyRegistryStub()

    await worker._enqueue_link_reindex(db_name="test_db", link_type_id="link-1")

    assert len(enqueued_jobs) == 1
    job = enqueued_jobs[0]
    assert job.execution_mode == "full"
    assert job.mapping_spec_id == "map-1"
    assert job.mapping_spec_version == 3
    assert job.options["mode"] == "link_index"
    assert job.options["relationship_spec_id"] == "rel-1"
    assert job.options["link_type_id"] == "link-1"


@pytest.mark.asyncio
async def test_enqueue_link_reindex_skips_when_dedupe_exists() -> None:
    worker = StrictInstanceWorker()
    enqueued_jobs = []

    class _DatasetRegistryStub:
        async def get_relationship_spec(self, *, link_type_id: str):
            return SimpleNamespace(
                relationship_spec_id="rel-1",
                mapping_spec_id="map-1",
                mapping_spec_version=1,
                dataset_id="dataset-1",
                dataset_version_id="version-1",
            )

        async def get_dataset(self, *, dataset_id: str):
            return SimpleNamespace(dataset_id="dataset-1", branch="main", name="join_ds")

        async def get_version(self, *, version_id: str):
            return SimpleNamespace(dataset_id="dataset-1", version_id="version-1", artifact_key="s3://bucket/key.parquet")

        async def get_latest_version(self, *, dataset_id: str):
            return None

    class _ObjectifyRegistryStub:
        async def get_mapping_spec(self, *, mapping_spec_id: str):
            return SimpleNamespace(version=1, target_class_id="Source", options={})

        def build_dedupe_key(self, **kwargs):
            return "dedupe-key"

        async def get_objectify_job_by_dedupe_key(self, *, dedupe_key: str):
            return SimpleNamespace(job_id="existing-job")

        async def enqueue_objectify_job(self, *, job):
            enqueued_jobs.append(job)

    worker.dataset_registry = _DatasetRegistryStub()
    worker.objectify_registry = _ObjectifyRegistryStub()

    await worker._enqueue_link_reindex(db_name="test_db", link_type_id="link-1")

    assert enqueued_jobs == []
