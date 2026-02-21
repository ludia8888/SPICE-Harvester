from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi import BackgroundTasks, HTTPException

from bff.schemas.admin_projection_requests import RecomputeProjectionRequest
from bff.services.admin_recompute_projection_service import (
    OntologiesProjectionStrategy,
    _load_projection_mapping,
    get_recompute_projection_result,
    recompute_projection_task,
    start_recompute_projection,
)
from shared.models.background_task import BackgroundTask, TaskStatus


class _TaskManagerStub:
    def __init__(self) -> None:
        self.calls = 0

    async def create_task(self, *args, **kwargs):  # noqa: ANN002, ANN003
        self.calls += 1
        self.last_args = args
        self.last_kwargs = kwargs
        return "task-1"


class _TaskStatusManagerStub:
    def __init__(self, task: BackgroundTask | None) -> None:
        self._task = task

    async def get_task_status(self, task_id: str) -> BackgroundTask | None:  # noqa: ARG002
        return self._task


def _request_payload(*, projection: str) -> RecomputeProjectionRequest:
    return RecomputeProjectionRequest(
        db_name="demo_db",
        projection=projection,
        branch="main",
        from_ts=datetime.now(timezone.utc),
        to_ts=None,
        promote=False,
    )


def _http_request_stub() -> SimpleNamespace:
    return SimpleNamespace(
        state=SimpleNamespace(admin_actor="admin-user"),
        client=SimpleNamespace(host="127.0.0.1"),
    )


@pytest.mark.asyncio
async def test_start_recompute_projection_blocks_instances_in_dataset_primary_mode(
) -> None:
    task_manager = _TaskManagerStub()
    with pytest.raises(HTTPException) as raised:
        await start_recompute_projection(
            http_request=_http_request_stub(),  # type: ignore[arg-type]
            request=_request_payload(projection="instances"),
            background_tasks=BackgroundTasks(),
            task_manager=task_manager,  # type: ignore[arg-type]
            redis_service=SimpleNamespace(),
            audit_store=SimpleNamespace(),
            lineage_store=SimpleNamespace(),
            elasticsearch_service=SimpleNamespace(),
        )

    assert raised.value.status_code == 400
    assert isinstance(raised.value.detail, dict)
    assert raised.value.detail.get("error") == "REQUEST_VALIDATION_FAILED"
    assert task_manager.calls == 0


@pytest.mark.asyncio
async def test_start_recompute_projection_allows_ontologies_in_dataset_primary_mode(
) -> None:
    task_manager = _TaskManagerStub()
    response = await start_recompute_projection(
        http_request=_http_request_stub(),  # type: ignore[arg-type]
        request=_request_payload(projection="ontologies"),
        background_tasks=BackgroundTasks(),
        task_manager=task_manager,  # type: ignore[arg-type]
        redis_service=SimpleNamespace(),
        audit_store=SimpleNamespace(),
        lineage_store=SimpleNamespace(),
        elasticsearch_service=SimpleNamespace(),
    )

    assert response.status == "accepted"
    assert task_manager.calls == 1


@pytest.mark.asyncio
async def test_recompute_projection_task_blocks_instances_in_dataset_primary_mode(
) -> None:
    with pytest.raises(RuntimeError, match="unsupported in dataset-primary mode"):
        await recompute_projection_task(
            task_id="task-1",
            request=_request_payload(projection="instances"),
            elasticsearch_service=SimpleNamespace(),
            redis_service=SimpleNamespace(),
            audit_store=SimpleNamespace(),
            lineage_store=SimpleNamespace(),
            requested_by=None,
            request_ip=None,
        )


@pytest.mark.asyncio
async def test_get_recompute_projection_result_returns_processing_for_running_task() -> None:
    now = datetime.now(timezone.utc)
    task = BackgroundTask(
        task_id="task-running",
        task_name="projection",
        task_type="projection_recompute",
        status=TaskStatus.PROCESSING,
        created_at=now,
        started_at=now,
        metadata={},
    )
    task_manager = _TaskStatusManagerStub(task)
    redis_stub = SimpleNamespace(get_json=None)

    result = await get_recompute_projection_result(
        task_id="task-running",
        task_manager=task_manager,  # type: ignore[arg-type]
        redis_service=redis_stub,  # type: ignore[arg-type]
    )

    assert result["task_id"] == "task-running"
    assert result["status"] == "processing"
    assert result["task_status"] == "PROCESSING"


class _EsStub:
    async def get_document(self, index: str, doc_id: str):  # noqa: ANN001
        return None


@pytest.mark.asyncio
async def test_ontologies_projection_strategy_normalizes_localized_fields_for_es() -> None:
    strategy = OntologiesProjectionStrategy()
    now = datetime.now(timezone.utc)
    envelope = SimpleNamespace(
        event_type="ONTOLOGY_CLASS_CREATED",
        event_id="evt-1",
    )
    data = {
        "class_id": "Customer",
        "label": {"ko": "고객", "en": "Customer"},
        "description": {"ko": "고객 개체", "en": "Customer entity"},
        "properties": [
            {
                "name": "customer_id",
                "label": {"ko": "고객 ID", "en": "Customer ID"},
                "description": {"ko": "식별자", "en": "Identifier"},
                "type": "string",
            }
        ],
        "relationships": [
            {
                "predicate": "PLACED",
                "target": "Order",
                "label": {"ko": "주문함", "en": "Placed"},
                "inverse_label": {"ko": "고객", "en": "Customer"},
            }
        ],
    }

    decision = await strategy.decide(
        envelope=envelope,
        data=data,
        db_name="demo_db",
        branch="main",
        new_index="demo_ontologies_v1",
        ontology_ref="main",
        ontology_commit="abc123",
        seq=7,
        event_ts=now,
        created_at_cache={},
        elasticsearch_service=_EsStub(),  # type: ignore[arg-type]
    )

    assert decision.action == "index"
    assert decision.document is not None
    assert decision.document["label"] == "고객"
    assert decision.document["description"] == "고객 개체"
    assert decision.document["label_i18n"] == {"ko": "고객", "en": "Customer"}
    assert decision.document["description_i18n"] == {"ko": "고객 개체", "en": "Customer entity"}
    first_property = decision.document["properties"][0]
    assert first_property["label"] == "고객 ID"
    assert first_property["label_i18n"] == {"ko": "고객 ID", "en": "Customer ID"}
    first_relationship = decision.document["relationships"][0]
    assert first_relationship["label"] == "주문함"
    assert first_relationship["label_i18n"] == {"ko": "주문함", "en": "Placed"}
    assert first_relationship["inverse_label"] == "고객"
    assert first_relationship["inverse_label_i18n"] == {"ko": "고객", "en": "Customer"}


def test_load_projection_mapping_falls_back_when_file_missing_for_ontologies(monkeypatch: pytest.MonkeyPatch) -> None:
    def _missing_file(*args, **kwargs):  # noqa: ANN002, ANN003
        raise FileNotFoundError("missing mapping file")

    monkeypatch.setattr("bff.services.admin_recompute_projection_service.open", _missing_file, raising=False)
    mapping = _load_projection_mapping(projection="ontologies")

    properties = mapping.get("mappings", {}).get("properties", {})
    assert properties.get("class_id", {}).get("type") == "keyword"
    assert properties.get("label_i18n", {}).get("type") == "object"


def test_load_projection_mapping_falls_back_when_file_missing_for_instances(monkeypatch: pytest.MonkeyPatch) -> None:
    def _missing_file(*args, **kwargs):  # noqa: ANN002, ANN003
        raise FileNotFoundError("missing mapping file")

    monkeypatch.setattr("bff.services.admin_recompute_projection_service.open", _missing_file, raising=False)
    mapping = _load_projection_mapping(projection="instances")

    properties = mapping.get("mappings", {}).get("properties", {})
    assert properties.get("instance_id", {}).get("type") == "keyword"
    assert properties.get("event_timestamp", {}).get("type") == "date"


class _ProgressTaskManagerStub:
    def __init__(self) -> None:
        self.updates: list[dict] = []

    async def update_progress(  # noqa: ANN002, ANN003
        self,
        task_id: str,
        current: int,
        total: int,
        message: str | None = None,
        metadata: dict | None = None,
    ) -> None:
        self.updates.append(
            {
                "task_id": task_id,
                "current": current,
                "total": total,
                "message": message,
                "metadata": metadata or {},
            }
        )


class _RedisResultStub:
    def __init__(self) -> None:
        self.saved: dict[str, dict] = {}

    async def set_json(self, key: str, value: dict, ttl: int) -> None:  # noqa: ARG002
        self.saved[key] = value


class _AuditStoreStub:
    async def log(self, **kwargs):  # noqa: ANN003
        return kwargs


class _LineageStoreStub:
    async def record_link(self, **kwargs):  # noqa: ANN003
        return kwargs

    @staticmethod
    def node_event(event_id: str) -> str:
        return f"event:{event_id}"

    @staticmethod
    def node_artifact(kind: str, index_name: str, doc_id: str) -> str:
        return f"{kind}:{index_name}:{doc_id}"


class _ElasticsearchRecomputeStub:
    def __init__(self) -> None:
        self._client = None
        self.created_indices: list[str] = []
        self.indexed_docs: list[tuple[str, str]] = []

    async def connect(self) -> None:
        self._client = object()

    async def index_exists(self, index_name: str) -> bool:
        return index_name in self.created_indices

    async def delete_index(self, index_name: str) -> None:
        self.created_indices = [idx for idx in self.created_indices if idx != index_name]

    async def create_index(self, index_name: str, mappings=None, settings=None) -> None:  # noqa: ANN001
        self.created_indices.append(index_name)

    async def index_document(
        self,
        index_name: str,
        document: dict,
        *,
        doc_id: str | None = None,
        refresh: bool = False,  # noqa: FBT001, FBT002
        version=None,  # noqa: ANN001
        version_type=None,  # noqa: ANN001
    ) -> None:
        _ = (document, refresh, version, version_type)
        self.indexed_docs.append((index_name, str(doc_id or "")))

    async def delete_document(self, index_name: str, doc_id: str, **kwargs) -> None:  # noqa: ANN003
        _ = (index_name, doc_id, kwargs)

    async def refresh_index(self, index_name: str) -> None:  # noqa: ARG002
        return None


@pytest.mark.asyncio
async def test_recompute_projection_task_updates_progress_for_long_running_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)
    envelopes = [
        SimpleNamespace(
            event_type="ONTOLOGY_CLASS_CREATED",
            event_id=f"evt-{idx}",
            sequence_number=idx + 1,
            occurred_at=now,
            metadata={"ontology": "main@abc123"},
            data={
                "db_name": "demo_db",
                "branch": "main",
                "class_id": f"Class{idx}",
                "label": f"Class {idx}",
                "description": "desc",
                "properties": [{"name": "id", "type": "xsd:string", "label": "id"}],
                "relationships": [],
            },
        )
        for idx in range(3)
    ]

    class _EventStoreStub:
        async def connect(self) -> None:
            return None

        async def replay_events(self, from_dt, to_dt, event_types):  # noqa: ANN001
            _ = (from_dt, to_dt, event_types)
            for envelope in envelopes:
                yield envelope

    monkeypatch.setattr(
        "bff.services.admin_recompute_projection_service.EventStore",
        _EventStoreStub,
    )

    request = RecomputeProjectionRequest(
        db_name="demo_db",
        projection="ontologies",
        branch="main",
        from_ts=now,
        to_ts=now,
        promote=False,
        max_events=2,
    )
    es = _ElasticsearchRecomputeStub()
    redis_stub = _RedisResultStub()
    progress_manager = _ProgressTaskManagerStub()

    await recompute_projection_task(
        task_id="task-progress",
        request=request,
        elasticsearch_service=es,  # type: ignore[arg-type]
        redis_service=redis_stub,  # type: ignore[arg-type]
        audit_store=_AuditStoreStub(),  # type: ignore[arg-type]
        lineage_store=_LineageStoreStub(),  # type: ignore[arg-type]
        task_manager=progress_manager,  # type: ignore[arg-type]
        requested_by="admin",
        request_ip="127.0.0.1",
    )

    assert len(progress_manager.updates) >= 2
    assert any(update["current"] == 1 for update in progress_manager.updates)
    assert any("completed" in str(update["message"]).lower() for update in progress_manager.updates)
    assert "recompute_result:task-progress" in redis_stub.saved
    assert redis_stub.saved["recompute_result:task-progress"]["status"] == "completed"
