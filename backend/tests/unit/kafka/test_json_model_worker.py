from __future__ import annotations

from typing import Any, Optional

import pytest
from pydantic import BaseModel

from shared.services.kafka.processed_event_worker import CommandParseError, JsonModelKafkaWorker, RegistryKey


class _StubModel(BaseModel):
    job_id: str
    aggregate_id: str
    sequence_number: int | None = None


class _StubJsonModelWorker(JsonModelKafkaWorker[_StubModel, None]):
    json_model_cls = _StubModel
    registry_event_id_field = "job_id"
    registry_aggregate_id_field = "aggregate_id"
    registry_sequence_field = "sequence_number"
    json_model_metadata_fields = ("job_id", "aggregate_id")
    json_model_span_name = "stub.process"
    json_model_metric_event_name = "STUB_JOB"
    json_model_span_attribute_fields = (
        ("job_id", "stub.job_id"),
        ("aggregate_id", "stub.aggregate_id"),
    )

    def __init__(self) -> None:
        self.consumer = None
        self.consumer_ops = None
        self.processed = None
        self.handler = "stub_handler"
        self.max_retries = 3
        self.backoff_base = 1
        self.backoff_max = 10

    async def _process_payload(self, payload: _StubModel) -> None:  # type: ignore[override]
        _ = payload
        return None

    async def _send_to_dlq(  # type: ignore[override]
        self,
        *,
        msg: Any,
        payload: Optional[_StubModel] = None,
        raw_payload: Optional[str] = None,
        error: str,
        attempt_count: int,
        **_kwargs: Any,
    ) -> None:
        _ = msg, payload, raw_payload, error, attempt_count
        return None


class _Msg:
    def topic(self) -> str:
        return "stub-topic"

    def partition(self) -> int:
        return 1

    def offset(self) -> int:
        return 42


def test_json_model_worker_parses_and_builds_registry_key() -> None:
    worker = _StubJsonModelWorker()

    payload = worker._parse_payload(b'{"job_id":"job-1","aggregate_id":"agg-1","sequence_number":7}')
    registry_key = worker._registry_key(payload)

    assert payload.job_id == "job-1"
    assert registry_key == RegistryKey(event_id="job-1", aggregate_id="agg-1", sequence_number=7)


def test_json_model_worker_reports_validation_stage() -> None:
    worker = _StubJsonModelWorker()

    with pytest.raises(CommandParseError) as exc_info:
        worker._parse_payload(b'{"aggregate_id":"agg-1"}')

    assert exc_info.value.stage == "validate"
    assert exc_info.value.payload_obj == {"aggregate_id": "agg-1"}


def test_json_model_worker_derives_metadata_and_span_contract_from_declared_fields() -> None:
    worker = _StubJsonModelWorker()
    payload = worker._parse_payload(b'{"job_id":"job-1","aggregate_id":"agg-1","sequence_number":7}')
    registry_key = worker._registry_key(payload)
    attrs = worker._span_attributes(msg=_Msg(), payload=payload, registry_key=registry_key)

    assert worker._fallback_metadata(payload) == {"job_id": "job-1", "aggregate_id": "agg-1"}
    assert worker._span_name(payload=payload) == "stub.process"
    assert worker._metric_event_name(payload=payload) == "STUB_JOB"
    assert attrs["stub.job_id"] == "job-1"
    assert attrs["stub.aggregate_id"] == "agg-1"


def test_pipeline_worker_uses_shared_json_model_contract() -> None:
    from pipeline_worker.main import PipelineWorker

    worker = PipelineWorker()
    payload = worker._parse_payload(
        b'{"job_id":"job-1","pipeline_id":"pipe-1","db_name":"demo","output_dataset_name":"orders"}'
    )
    registry_key = worker._registry_key(payload)

    assert payload.job_id == "job-1"
    assert payload.pipeline_id == "pipe-1"
    assert registry_key == RegistryKey(event_id="job-1", aggregate_id="pipe-1", sequence_number=None)


def test_objectify_worker_uses_shared_json_model_contract() -> None:
    from objectify_worker.main import ObjectifyWorker

    worker = ObjectifyWorker()
    payload = worker._parse_payload(
        b'{"job_id":"job-1","db_name":"demo","dataset_id":"dataset-1","dataset_version_id":"ver-1","target_class_id":"Order"}'
    )
    registry_key = worker._registry_key(payload)

    assert payload.job_id == "job-1"
    assert payload.dataset_id == "dataset-1"
    assert registry_key == RegistryKey(event_id="job-1", aggregate_id="dataset-1", sequence_number=None)
