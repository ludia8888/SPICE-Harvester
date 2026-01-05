from __future__ import annotations

import asyncio
import json
from contextlib import nullcontext
from datetime import datetime, timezone

import pytest

from connector_sync_worker import main as sync_module
from connector_sync_worker.main import ConnectorSyncWorker
from shared.models.event_envelope import EventEnvelope
from shared.services.connector_registry import ConnectorMapping, ConnectorSource
from shared.services.processed_event_registry import ClaimDecision, ClaimResult


class _FakeTracing:
    def span(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return nullcontext()


class _FakeRegistry:
    def __init__(self) -> None:
        self.source: ConnectorSource | None = None
        self.mapping: ConnectorMapping | None = None
        self.upserts: list[tuple[str, str, dict]] = []
        self.sync_outcomes: list[dict] = []

    async def get_source(self, *, source_type: str, source_id: str):
        return self.source

    async def get_mapping(self, *, source_type: str, source_id: str):
        return self.mapping

    async def upsert_source(self, *, source_type: str, source_id: str, enabled: bool, config_json: dict) -> None:
        self.upserts.append((source_type, source_id, config_json))

    async def record_sync_outcome(self, **kwargs):  # noqa: ANN003
        self.sync_outcomes.append(dict(kwargs))


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


class _FakeSheets:
    def __init__(self) -> None:
        self.values = [["id", "name"], [1, "Alice"], [2, "Bob"]]

    async def fetch_sheet_values(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return "sheet-1", {"sheets": []}, "Sheet1", None, self.values


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


class _FakeHttp:
    def __init__(self) -> None:
        self.get_calls: list[dict] = []
        self.post_calls: list[dict] = []

    async def get(self, url: str, **kwargs):  # noqa: ANN003
        self.get_calls.append({"url": url, **kwargs})
        return _FakeResponse(
            {
                "properties": [
                    {"name": "id", "label": "ID", "type": "xsd:integer"},
                    {"name": "name", "type": "xsd:string"},
                ]
            }
        )

    async def post(self, url: str, **kwargs):  # noqa: ANN003
        self.post_calls.append({"url": url, **kwargs})
        return _FakeResponse({"command_id": "cmd-1"})


class _FakeLineage:
    def __init__(self) -> None:
        self.links: list[dict] = []

    def node_event(self, value: str) -> str:
        return f"event:{value}"

    async def record_link(self, **kwargs):  # noqa: ANN003
        self.links.append(dict(kwargs))


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


@pytest.mark.asyncio
async def test_sync_worker_bff_scope_headers() -> None:
    worker = ConnectorSyncWorker()
    assert worker._bff_scope_headers(db_name="") == {}
    assert worker._bff_scope_headers(db_name="demo") == {"X-DB-Name": "demo", "X-Project": "demo"}


@pytest.mark.asyncio
async def test_sync_worker_fetch_schema_and_target_types(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sync_module.ServiceConfig, "get_bff_url", lambda: "http://bff")

    worker = ConnectorSyncWorker()
    worker.http = _FakeHttp()

    schema = await worker._fetch_ontology_schema(db_name="demo", class_label="User", branch="main")
    assert schema["properties"][0]["name"] == "id"

    types = await worker._target_field_types(db_name="demo", class_label="User", branch="main")
    assert types["id"] == "xsd:integer"
    assert types["ID"] == "xsd:integer"
    assert types["name"] == "xsd:string"


@pytest.mark.asyncio
async def test_sync_worker_process_google_sheets_update(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sync_module.ServiceConfig, "get_bff_url", lambda: "http://bff")

    class _FakeOAuth:
        client_id = "client"
        client_secret = "secret"

        async def refresh_access_token(self, refresh_token: str):  # noqa: ARG002
            return {"access_token": "new-token", "refresh_token": "refresh-token", "expires_at": 123}

    monkeypatch.setattr("data_connector.google_sheets.auth.GoogleOAuth2Client", _FakeOAuth)

    worker = ConnectorSyncWorker()
    registry = _FakeRegistry()
    worker.registry = registry
    worker.sheets = _FakeSheets()
    worker.http = _FakeHttp()
    worker.lineage = _FakeLineage()

    registry.source = ConnectorSource(
        source_type="google_sheets",
        source_id="sheet-1",
        enabled=True,
        config_json={
            "sheet_url": "https://docs.google.com/spreadsheets/d/sheet-1/edit",
            "refresh_token": "refresh-token",
            "max_import_rows": 10,
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

    envelope = EventEnvelope.from_connector_update(
        source_type="google_sheets",
        source_id="sheet-1",
        cursor="cursor-1",
        data={"source_type": "google_sheets", "source_id": "sheet-1"},
    )

    command_id = await worker._process_google_sheets_update(envelope)

    assert command_id == "cmd-1"
    assert registry.upserts, "Expected token refresh update"
    assert worker.lineage.links, "Expected lineage link recorded"


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
        return "cmd-1"

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
