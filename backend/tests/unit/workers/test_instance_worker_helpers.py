from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import instance_worker.main as instance_worker_main
from instance_worker.main import StrictInstanceWorker, _InstanceCommandPayload
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


def test_es_outage_errors_expand_retry_budget() -> None:
    worker = StrictInstanceWorker()
    payload = _InstanceCommandPayload(command={"command_id": "cmd"}, envelope_metadata={})

    retries = worker._max_retries_for_error(
        Exception("ConnectionError: cannot connect to host elasticsearch:9200"),
        payload=payload,
        error="Connection error caused by: ConnectionError(Cannot connect to host elasticsearch:9200)",
        retryable=True,
    )
    backoff = worker._backoff_seconds_for_error(
        Exception("ConnectionError: cannot connect to host elasticsearch:9200"),
        payload=payload,
        error="Connection error caused by: ConnectionError(Cannot connect to host elasticsearch:9200)",
        attempt_count=6,
        retryable=True,
    )

    assert retries >= 20
    assert backoff <= 8


@pytest.mark.asyncio
async def test_process_create_instance_does_not_mark_failed_before_terminal_retry() -> None:
    worker = StrictInstanceWorker()
    status_events: list[str] = []

    async def _set_status(command_id: str, status: str, result=None):  # noqa: ANN001
        status_events.append(status)

    worker.set_command_status = _set_status  # type: ignore[assignment]
    worker._stamp_ontology_version = AsyncMock(return_value={"ref": "branch:main"})  # type: ignore[assignment]
    worker._apply_create_instance_side_effects = AsyncMock(side_effect=Exception("ConnectionError: es unavailable"))  # type: ignore[assignment]
    worker._record_instance_edit = AsyncMock()  # type: ignore[assignment]
    worker._apply_relationship_object_link_edits = AsyncMock()  # type: ignore[assignment]

    command = {
        "command_id": "cmd-create-1",
        "command_type": "CREATE_INSTANCE",
        "db_name": "qa_db",
        "class_id": "Order",
        "branch": "main",
        "payload": {
            "order_id": "order-1",
            "order_status": "PENDING",
        },
    }

    with pytest.raises(Exception, match="ConnectionError"):
        await worker.process_create_instance(command)

    assert status_events == ["processing"]


@pytest.mark.asyncio
async def test_create_side_effects_append_event_before_es_and_use_event_sequence() -> None:
    worker = StrictInstanceWorker()
    call_order: list[str] = []
    captured_build: dict[str, object] = {}

    class _FakeEventStore:
        async def append_event(self, envelope) -> None:  # noqa: ANN001
            call_order.append("event_store")
            envelope.sequence_number = 42

    class _FakeElasticsearchService:
        async def index_document(self, *, index: str, document, doc_id: str) -> None:  # noqa: ANN001
            call_order.append("elasticsearch")

    async def _fake_s3_call(*args, **kwargs):  # noqa: ANN002, ANN003
        return {}

    def _fake_build_es_document(**kwargs):  # noqa: ANN003
        captured_build.update(kwargs)
        return {"instance_id": kwargs["instance_id"]}

    worker.s3_client = SimpleNamespace(put_object=object())
    worker.elasticsearch_service = _FakeElasticsearchService()
    worker.event_store = _FakeEventStore()
    worker.observability = SimpleNamespace(record_link=AsyncMock(), audit_log=AsyncMock())
    worker._s3_call = _fake_s3_call  # type: ignore[assignment]
    worker.extract_relationships = AsyncMock(return_value={})  # type: ignore[assignment]
    worker._ensure_instances_index = AsyncMock(return_value="instances_demo_main")  # type: ignore[assignment]
    worker._build_es_document = _fake_build_es_document  # type: ignore[assignment]

    result = await worker._apply_create_instance_side_effects(
        command_id="cmd-1",
        db_name="demo",
        class_id="Account",
        branch="main",
        payload={"account_id": "acc-1", "name": "Alice"},
        instance_id="acc-1",
        command_log={"command_type": "CREATE_INSTANCE"},
        ontology_version={"ref": "branch:main"},
        created_by="tester",
    )

    assert call_order == ["event_store", "elasticsearch"]
    assert captured_build["event_sequence"] == 42
    assert result["domain_event_id"]


@pytest.mark.asyncio
async def test_create_side_effects_logs_write_path_contract(caplog: pytest.LogCaptureFixture) -> None:
    worker = StrictInstanceWorker()
    caplog.set_level(logging.INFO, logger=instance_worker_main.logger.name)

    class _FakeEventStore:
        async def append_event(self, envelope) -> None:  # noqa: ANN001
            envelope.sequence_number = 7

    class _FakeElasticsearchService:
        async def index_document(self, *, index: str, document, doc_id: str) -> None:  # noqa: ANN001
            _ = index, document, doc_id

    async def _fake_s3_call(*args, **kwargs):  # noqa: ANN002, ANN003
        return {}

    worker.s3_client = SimpleNamespace(put_object=object())
    worker.elasticsearch_service = _FakeElasticsearchService()
    worker.event_store = _FakeEventStore()
    worker.observability = SimpleNamespace(record_link=AsyncMock(), audit_log=AsyncMock())
    worker._s3_call = _fake_s3_call  # type: ignore[assignment]
    worker.extract_relationships = AsyncMock(return_value={})  # type: ignore[assignment]
    worker._ensure_instances_index = AsyncMock(return_value="instances_demo_main")  # type: ignore[assignment]
    worker._build_es_document = lambda **kwargs: {"instance_id": kwargs["instance_id"]}  # type: ignore[assignment]

    await worker._apply_create_instance_side_effects(
        command_id="cmd-2",
        db_name="demo",
        class_id="Account",
        branch="main",
        payload={"account_id": "acc-2", "name": "Bob"},
        instance_id="acc-2",
        command_log={"command_type": "CREATE_INSTANCE"},
        ontology_version={"ref": "branch:main"},
        created_by="tester",
    )

    assert "Instance worker write path contract" in caplog.text
    assert "instance_create_event" in caplog.text
    assert "instance_es_materialization" in caplog.text


@pytest.mark.asyncio
async def test_resolve_instance_payload_returns_none_for_deleted_event_history() -> None:
    worker = StrictInstanceWorker()
    worker.event_store = SimpleNamespace(
        get_events=AsyncMock(
            return_value=[
                SimpleNamespace(
                    event_type="INSTANCE_CREATED",
                    data={
                        "db_name": "demo",
                        "branch": "main",
                        "class_id": "Account",
                        "instance_id": "acc-1",
                        "name": "Alice",
                    },
                ),
                SimpleNamespace(
                    event_type="INSTANCE_DELETED",
                    data={
                        "db_name": "demo",
                        "branch": "main",
                        "class_id": "Account",
                        "instance_id": "acc-1",
                    },
                ),
            ]
        )
    )
    worker.s3_client = SimpleNamespace(list_objects_v2=object(), get_object=object())
    worker._s3_call = AsyncMock(side_effect=AssertionError("S3 fallback should not run after delete tombstone"))  # type: ignore[assignment]

    resolved = await worker._resolve_instance_payload(
        db_name="demo",
        branch="main",
        class_id="Account",
        instance_id="acc-1",
    )

    assert resolved is None


@pytest.mark.asyncio
async def test_shutdown_uses_disconnect_for_elasticsearch_service(monkeypatch) -> None:
    worker = StrictInstanceWorker()
    worker._close_consumer_runtime = AsyncMock()  # type: ignore[assignment]
    monkeypatch.setattr(instance_worker_main, "close_kafka_producer", AsyncMock())

    redis_client = SimpleNamespace(aclose=AsyncMock())
    es_service = SimpleNamespace(disconnect=AsyncMock())
    oms_http = SimpleNamespace(aclose=AsyncMock())
    processed_registry = SimpleNamespace(close=AsyncMock())

    worker.redis_client = redis_client
    worker.elasticsearch_service = es_service
    worker.oms_http = oms_http
    worker.processed_event_registry = processed_registry
    worker.dataset_registry = None
    worker.objectify_registry = None

    await worker.shutdown()

    worker._close_consumer_runtime.assert_awaited_once()  # type: ignore[attr-defined]
    instance_worker_main.close_kafka_producer.assert_awaited_once()  # type: ignore[attr-defined]
    redis_client.aclose.assert_awaited_once()
    es_service.disconnect.assert_awaited_once()
    oms_http.aclose.assert_awaited_once()
    processed_registry.close.assert_awaited_once()


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
