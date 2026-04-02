from __future__ import annotations

import asyncio
import json
from contextlib import nullcontext
from datetime import datetime, timezone

import pytest

import connector_sync_worker.main as connector_sync_module
from connector_sync_worker.main import ConnectorSyncWorker
from shared.models.event_envelope import EventEnvelope
from shared.services.registries.connector_registry import ConnectorMapping, ConnectorSource, SyncState
from shared.services.registries.processed_event_registry import ClaimDecision, ClaimResult


class _FakeTracing:
    def span(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return nullcontext()


class _FakeAdapter:
    def __init__(self) -> None:
        self.last_import_config = None
        self.snapshot_calls = 0
        self.incremental_calls = 0
        self.cdc_calls = 0

    async def snapshot_extract(self, *, config, secrets, import_config):  # noqa: ANN001, ANN003
        self.snapshot_calls += 1
        self.last_import_config = import_config
        _ = config, secrets, import_config
        from data_connector.adapters.base import ConnectorExtractResult

        return ConnectorExtractResult(
            columns=["id", "name"],
            rows=[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            next_state={"watermark": 2},
        )

    async def incremental_extract(self, *, config, secrets, import_config, sync_state):  # noqa: ANN001, ANN003
        self.incremental_calls += 1
        return await self.snapshot_extract(config=config, secrets=secrets, import_config=import_config)

    async def cdc_extract(self, *, config, secrets, import_config, sync_state):  # noqa: ANN001, ANN003
        self.cdc_calls += 1
        return await self.snapshot_extract(config=config, secrets=secrets, import_config=import_config)


class _FakeAdapterFactory:
    def __init__(self) -> None:
        self.adapter = _FakeAdapter()

    def get_adapter(self, connector_kind: str):  # noqa: ANN001
        _ = connector_kind
        return self.adapter


class _FakeRegistry:
    def __init__(self) -> None:
        self.source: ConnectorSource | None = None
        self.connection_source: ConnectorSource | None = None
        self.mapping: ConnectorMapping | None = None
        self.sync_state: SyncState | None = None
        self.sync_outcomes: list[dict] = []
        self.sync_state_updates: list[dict] = []

    async def get_source(self, *, source_type: str, source_id: str):
        if self.source and self.source.source_type == source_type and self.source.source_id == source_id:
            return self.source
        if self.connection_source and self.connection_source.source_type == source_type and self.connection_source.source_id == source_id:
            return self.connection_source
        return None

    async def get_mapping(self, *, source_type: str, source_id: str):
        _ = source_type, source_id
        return self.mapping

    async def get_sync_state(self, *, source_type: str, source_id: str):
        _ = source_type, source_id
        return self.sync_state

    async def get_connection_secrets(self, *, source_type: str, source_id: str):
        _ = source_type, source_id
        return {"access_token": "token-1"}

    async def upsert_sync_state_json(self, *, source_type: str, source_id: str, sync_state_json: dict, merge: bool):  # noqa: ARG002
        self.sync_state_updates.append(
            {
                "source_type": source_type,
                "source_id": source_id,
                "sync_state_json": dict(sync_state_json),
            }
        )
        return sync_state_json

    async def record_sync_outcome(self, **kwargs):  # noqa: ANN003
        self.sync_outcomes.append(dict(kwargs))


class _FailingSyncStateRegistry(_FakeRegistry):
    async def upsert_sync_state_json(self, *, source_type: str, source_id: str, sync_state_json: dict, merge: bool):  # noqa: ARG002
        raise RuntimeError(f"cannot persist sync state for {source_type}:{source_id}")


class _FakeIngestService:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def ingest_rows(self, **kwargs):  # noqa: ANN003
        self.calls.append(dict(kwargs))
        return {
            "dataset": {
                "dataset_id": "dataset-1",
                "db_name": kwargs.get("db_name"),
                "name": "connector_dataset",
            },
            "version": {
                "version_id": "version-1",
                "lakefs_commit_id": "c1",
                "artifact_key": "s3://raw-datasets/c1/path.csv",
                "row_count": 2,
            },
            "objectify_job_id": None,
        }


class _FakeProcessed:
    def __init__(self) -> None:
        self.claim_calls: list[dict] = []
        self.done_calls: list[dict] = []
        self.failed_calls: list[dict] = []
        self.heartbeat_calls: list[dict] = []

    async def claim(self, **kwargs):  # noqa: ANN003
        self.claim_calls.append(dict(kwargs))
        return ClaimResult(decision=ClaimDecision.CLAIMED, attempt_count=1)

    async def mark_done(self, **kwargs):  # noqa: ANN003
        self.done_calls.append(dict(kwargs))

    async def mark_failed(self, **kwargs):  # noqa: ANN003
        self.failed_calls.append(dict(kwargs))

    async def heartbeat(self, **kwargs):  # noqa: ANN003
        self.heartbeat_calls.append(dict(kwargs))
        return False


class _FakeMessage:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def error(self):  # noqa: D401
        return None

    def value(self):
        return json.dumps(self._payload).encode("utf-8")

    def headers(self):
        return []

    def topic(self):
        return "connector-updates"

    def partition(self):
        return 0

    def offset(self):
        return 1


class _FakeConsumer:
    def __init__(self, worker: ConnectorSyncWorker) -> None:
        self.worker = worker
        self._messages: list[_FakeMessage] = []
        self.commit_calls: list[dict] = []

    def poll(self, timeout: float = 1.0):  # noqa: ARG002
        if self._messages:
            return self._messages.pop(0)
        return None

    def commit(self, msg, asynchronous: bool = False):  # noqa: ANN001, ARG002
        self.commit_calls.append({"msg": msg, "async": asynchronous})
        self.worker.running = False

    def commit_sync(self, msg):  # noqa: ANN001
        self.commit_calls.append({"msg": msg, "async": False})
        self.worker.running = False


@pytest.mark.asyncio
async def test_sync_worker_process_connector_update_snapshot() -> None:
    worker = ConnectorSyncWorker()
    registry = _FakeRegistry()
    ingest = _FakeIngestService()

    registry.source = ConnectorSource(
        source_type="google_sheets",
        source_id="sheet-1",
        enabled=True,
        config_json={
            "sheet_url": "https://docs.google.com/spreadsheets/d/sheet-1/edit",
            "import_mode": "SNAPSHOT",
            "table_import_config": {"type": "jdbcImportConfig", "query": "SELECT * FROM external_sheet"},
        },
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    registry.mapping = ConnectorMapping(
        mapping_id="map-1",
        source_type="google_sheets",
        source_id="sheet-1",
        status="active",
        enabled=True,
        target_db_name="demo",
        target_branch="main",
        target_class_label="User",
        field_mappings=[],
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    registry.sync_state = SyncState(
        source_type="google_sheets",
        source_id="sheet-1",
        last_seen_cursor="cursor-1",
        last_emitted_seq=1,
        last_polled_at=datetime.now(timezone.utc),
        last_success_at=None,
        last_failure_at=None,
        last_error=None,
        attempt_count=0,
        rate_limit_until=None,
        next_retry_at=None,
        last_command_id=None,
        sync_state_json={"watermark": 1},
        updated_at=datetime.now(timezone.utc),
    )

    worker.registry = registry
    worker.adapter_factory = _FakeAdapterFactory()
    worker.ingest_service = ingest
    worker.tracing = _FakeTracing()

    envelope = EventEnvelope.from_connector_update(
        source_type="google_sheets",
        source_id="sheet-1",
        cursor="cursor-2",
        data={"source_type": "google_sheets", "source_id": "sheet-1"},
    )

    version_id = await worker._process_connector_update(envelope)

    assert version_id == "version-1"
    assert ingest.calls
    call = ingest.calls[0]
    assert call["db_name"] == "demo"
    assert call["import_mode"] == "SNAPSHOT"
    assert call["source_type"] == "google_sheets"
    assert call["source_id"] == "sheet-1"
    assert registry.sync_state_updates
    assert registry.sync_state_updates[0]["sync_state_json"] == {"watermark": 2}


@pytest.mark.asyncio
async def test_sync_worker_process_connector_update_treats_post_ingest_sync_state_failure_as_non_retryable() -> None:
    worker = ConnectorSyncWorker()
    registry = _FailingSyncStateRegistry()
    ingest = _FakeIngestService()

    registry.source = ConnectorSource(
        source_type="google_sheets",
        source_id="sheet-1",
        enabled=True,
        config_json={
            "sheet_url": "https://docs.google.com/spreadsheets/d/sheet-1/edit",
            "import_mode": "SNAPSHOT",
            "table_import_config": {"type": "jdbcImportConfig", "query": "SELECT * FROM external_sheet"},
        },
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    registry.mapping = ConnectorMapping(
        mapping_id="map-1",
        source_type="google_sheets",
        source_id="sheet-1",
        status="active",
        enabled=True,
        target_db_name="demo",
        target_branch="main",
        target_class_label="User",
        field_mappings=[],
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )

    worker.registry = registry
    worker.adapter_factory = _FakeAdapterFactory()
    worker.ingest_service = ingest
    worker.tracing = _FakeTracing()

    envelope = EventEnvelope.from_connector_update(
        source_type="google_sheets",
        source_id="sheet-1",
        cursor="cursor-2",
        data={"source_type": "google_sheets", "source_id": "sheet-1"},
    )

    with pytest.raises(connector_sync_module._SyncStatePersistenceError):
        await worker._process_connector_update(envelope)

    assert ingest.calls
    assert worker._is_retryable_error(
        connector_sync_module._SyncStatePersistenceError("sync-state persistence failed"),
        payload=envelope,
    ) is False


@pytest.mark.asyncio
async def test_sync_worker_process_connector_update_file_import_uses_file_config_key() -> None:
    worker = ConnectorSyncWorker()
    registry = _FakeRegistry()
    ingest = _FakeIngestService()
    adapter_factory = _FakeAdapterFactory()

    registry.source = ConnectorSource(
        source_type="mysql_file_import",
        source_id="file-1",
        enabled=True,
        config_json={
            "connection_id": "conn-1",
            "import_mode": "SNAPSHOT",
            "file_import_config": {"type": "jdbcImportConfig", "query": "SELECT * FROM file_source"},
        },
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    registry.connection_source = ConnectorSource(
        source_type="mysql_connection",
        source_id="conn-1",
        enabled=True,
        config_json={"type": "MySqlConnectionConfig", "host": "localhost", "database": "demo"},
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    registry.mapping = ConnectorMapping(
        mapping_id="map-file-1",
        source_type="mysql_file_import",
        source_id="file-1",
        status="active",
        enabled=True,
        target_db_name="demo",
        target_branch="main",
        target_class_label="User",
        field_mappings=[],
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )

    worker.registry = registry
    worker.adapter_factory = adapter_factory
    worker.ingest_service = ingest
    worker.tracing = _FakeTracing()

    envelope = EventEnvelope.from_connector_update(
        source_type="mysql_file_import",
        source_id="file-1",
        cursor="cursor-2",
        data={"source_type": "mysql_file_import", "source_id": "file-1"},
    )

    version_id = await worker._process_connector_update(envelope)

    assert version_id == "version-1"
    assert adapter_factory.adapter.last_import_config == {"type": "jdbcImportConfig", "query": "SELECT * FROM file_source"}


@pytest.mark.asyncio
async def test_sync_worker_process_connector_update_streaming_uses_cdc_extract() -> None:
    worker = ConnectorSyncWorker()
    registry = _FakeRegistry()
    ingest = _FakeIngestService()
    adapter_factory = _FakeAdapterFactory()

    registry.source = ConnectorSource(
        source_type="postgresql_table_import",
        source_id="table-1",
        enabled=True,
        config_json={
            "connection_id": "conn-streaming-1",
            "import_mode": "STREAMING",
            "table_import_config": {
                "type": "postgreSqlImportConfig",
                "query": "SELECT * FROM orders",
                "watermarkColumn": "updated_at",
            },
        },
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    registry.connection_source = ConnectorSource(
        source_type="postgresql_connection",
        source_id="conn-streaming-1",
        enabled=True,
        config_json={"type": "PostgreSqlConnectionConfig", "host": "localhost", "database": "demo"},
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    registry.mapping = ConnectorMapping(
        mapping_id="map-streaming-1",
        source_type="postgresql_table_import",
        source_id="table-1",
        status="active",
        enabled=True,
        target_db_name="demo",
        target_branch="main",
        target_class_label="Order",
        field_mappings=[],
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )

    worker.registry = registry
    worker.adapter_factory = adapter_factory
    worker.ingest_service = ingest
    worker.tracing = _FakeTracing()

    envelope = EventEnvelope.from_connector_update(
        source_type="postgresql_table_import",
        source_id="table-1",
        cursor="cursor-2",
        data={"source_type": "postgresql_table_import", "source_id": "table-1"},
    )
    version_id = await worker._process_connector_update(envelope)

    assert version_id == "version-1"
    assert adapter_factory.adapter.cdc_calls == 1
    assert adapter_factory.adapter.incremental_calls == 0
    assert ingest.calls[0]["import_mode"] == "STREAMING"


@pytest.mark.asyncio
async def test_sync_worker_handle_envelope_rejects_unknown() -> None:
    worker = ConnectorSyncWorker()
    envelope = EventEnvelope(
        event_type="UNKNOWN",
        aggregate_type="connector",
        aggregate_id="x",
        data={"source_type": "unknown"},
        metadata={"kind": "other"},
    )
    with pytest.raises(ValueError):
        await worker._handle_envelope(envelope)


@pytest.mark.asyncio
async def test_sync_worker_handle_envelope_accepts_sqlserver_table_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = ConnectorSyncWorker()
    envelope = EventEnvelope.from_connector_update(
        source_type="sqlserver_table_import",
        source_id="import-1",
        cursor="cursor-1",
        data={"source_type": "sqlserver_table_import", "source_id": "import-1"},
    )

    async def _handle_stub(payload):  # noqa: ANN001
        _ = payload
        return "version-1"

    monkeypatch.setattr(worker, "_process_connector_update", _handle_stub)
    result = await worker._handle_envelope(envelope)
    assert result == "version-1"


@pytest.mark.asyncio
async def test_sync_worker_handle_envelope_accepts_mysql_file_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = ConnectorSyncWorker()
    envelope = EventEnvelope.from_connector_update(
        source_type="mysql_file_import",
        source_id="import-2",
        cursor="cursor-1",
        data={"source_type": "mysql_file_import", "source_id": "import-2"},
    )

    async def _handle_stub(payload):  # noqa: ANN001
        _ = payload
        return "version-2"

    monkeypatch.setattr(worker, "_process_connector_update", _handle_stub)
    result = await worker._handle_envelope(envelope)
    assert result == "version-2"


@pytest.mark.asyncio
async def test_sync_worker_run_processes_message(monkeypatch: pytest.MonkeyPatch) -> None:
    worker = ConnectorSyncWorker()
    worker.tracing = _FakeTracing()
    worker.registry = _FakeRegistry()
    worker.processed = _FakeProcessed()

    envelope = EventEnvelope.from_connector_update(
        source_type="google_sheets",
        source_id="sheet-1",
        cursor="cursor-1",
        data={"source_type": "google_sheets", "source_id": "sheet-1"},
    )
    worker.consumer = _FakeConsumer(worker)
    worker.consumer._messages = [_FakeMessage(envelope.model_dump(mode="json"))]

    async def _handle_stub(envelope):  # noqa: ANN001
        return "version-1"

    monkeypatch.setattr(worker, "_handle_envelope", _handle_stub)
    monkeypatch.setattr(worker, "_heartbeat_loop", lambda **kwargs: asyncio.sleep(0))  # noqa: ARG005

    await asyncio.wait_for(worker.run(), timeout=1)

    assert worker.processed.claim_calls
    assert worker.processed.done_calls
    assert worker.registry.sync_outcomes


@pytest.mark.asyncio
async def test_sync_worker_heartbeat_loop_stops(monkeypatch: pytest.MonkeyPatch) -> None:
    worker = ConnectorSyncWorker()
    worker.processed = _FakeProcessed()

    async def _noop_sleep(_):  # noqa: ANN001
        return None

    monkeypatch.setattr(asyncio, "sleep", _noop_sleep)

    await asyncio.wait_for(worker._heartbeat_loop(handler="h", event_id="e"), timeout=1)
    assert worker.processed.heartbeat_calls
