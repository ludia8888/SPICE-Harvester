from __future__ import annotations

import asyncio
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone

import pytest

from connector_trigger_service import main as trigger_module
from connector_trigger_service.main import ConnectorTriggerService
from shared.models.event_envelope import EventEnvelope
from shared.services.connector_registry import ConnectorSource, OutboxItem, SyncState


class _FakeTracing:
    def span(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return nullcontext()


class _FakeRegistry:
    def __init__(self) -> None:
        self.initialized = False
        self.closed = False
        self.sources: list[ConnectorSource] = []
        self.sync_state: SyncState | None = None
        self.poll_envelope: EventEnvelope | None = None
        self.outbox_batches: list[list[OutboxItem]] = []
        self.published: list[str] = []
        self.failed: list[tuple[str, str]] = []
        self.upserts: list[tuple[str, str, dict]] = []
        self.on_claim: callable | None = None

    async def initialize(self) -> None:
        self.initialized = True

    async def close(self) -> None:
        self.closed = True

    async def get_sync_state(self, *, source_type: str, source_id: str) -> SyncState | None:
        return self.sync_state

    async def list_sources(self, *, source_type: str, enabled: bool, limit: int):
        return self.sources

    async def record_poll_result(self, **kwargs):  # noqa: ANN003
        return self.poll_envelope

    async def claim_outbox_batch(self, *, limit: int):
        if self.on_claim:
            self.on_claim()
        if self.outbox_batches:
            return self.outbox_batches.pop(0)
        return []

    async def mark_outbox_published(self, *, outbox_id: str) -> None:
        self.published.append(outbox_id)

    async def mark_outbox_failed(self, *, outbox_id: str, error: str) -> None:
        self.failed.append((outbox_id, error))

    async def upsert_source(self, *, source_type: str, source_id: str, enabled: bool, config_json: dict) -> None:
        self.upserts.append((source_type, source_id, config_json))


class _FakeSheets:
    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key
        self.closed = False
        self.values: list[list[str]] = [["id", "name"], ["1", "Alice"]]

    async def close(self) -> None:
        self.closed = True

    async def fetch_sheet_values(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return "sheet-1", {"sheets": []}, "Sheet1", None, self.values


class _FakeProducer:
    def __init__(self, config: dict) -> None:
        self.config = config
        self.produced: list[dict] = []
        self.flush_calls: list[int | None] = []

    def produce(self, **kwargs):  # noqa: ANN003
        self.produced.append(kwargs)

    def flush(self, timeout: int | None = None) -> int:
        self.flush_calls.append(timeout)
        return 0


@pytest.mark.asyncio
async def test_trigger_service_initialize_and_close(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(trigger_module, "get_tracing_service", lambda *_: _FakeTracing())
    monkeypatch.setattr(trigger_module, "ConnectorRegistry", _FakeRegistry)
    monkeypatch.setattr(trigger_module, "GoogleSheetsService", _FakeSheets)
    monkeypatch.setattr(trigger_module, "Producer", _FakeProducer)
    monkeypatch.setattr(trigger_module.ServiceConfig, "get_kafka_bootstrap_servers", lambda: "localhost:1234")

    service = ConnectorTriggerService()
    await service.initialize()

    assert isinstance(service.registry, _FakeRegistry)
    assert isinstance(service.sheets, _FakeSheets)
    assert isinstance(service.producer, _FakeProducer)
    assert service.registry.initialized is True

    registry = service.registry
    sheets = service.sheets

    await service.close()

    assert registry.closed is True
    assert sheets.closed is True


@pytest.mark.asyncio
async def test_trigger_service_is_due(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(trigger_module, "get_tracing_service", lambda *_: _FakeTracing())
    service = ConnectorTriggerService()
    registry = _FakeRegistry()
    service.registry = registry

    source = ConnectorSource(
        source_type="google_sheets",
        source_id="sheet-1",
        enabled=True,
        config_json={"polling_interval": 10},
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )

    assert await service._is_due(source) is True

    registry.sync_state = SyncState(
        source_type="google_sheets",
        source_id="sheet-1",
        last_seen_cursor=None,
        last_emitted_seq=0,
        last_polled_at=datetime.now(timezone.utc) - timedelta(seconds=2),
        last_success_at=None,
        last_failure_at=None,
        last_error=None,
        attempt_count=0,
        rate_limit_until=None,
        next_retry_at=None,
        last_command_id=None,
        updated_at=datetime.now(timezone.utc),
    )

    assert await service._is_due(source) is False


@pytest.mark.asyncio
async def test_trigger_service_poll_google_sheets_refreshes_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(trigger_module, "get_tracing_service", lambda *_: _FakeTracing())

    service = ConnectorTriggerService()
    registry = _FakeRegistry()
    sheets = _FakeSheets()
    service.registry = registry
    service.sheets = sheets

    source = ConnectorSource(
        source_type="google_sheets",
        source_id="sheet-1",
        enabled=True,
        config_json={
            "sheet_url": "https://docs.google.com/spreadsheets/d/sheet-1/edit",
            "refresh_token": "refresh-token",
        },
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )

    registry.sources = [source]
    registry.poll_envelope = EventEnvelope.from_connector_update(
        source_type="google_sheets",
        source_id="sheet-1",
        cursor="cursor-1",
    )

    class _FakeOAuth:
        client_id = "client"
        client_secret = "secret"

        async def refresh_access_token(self, refresh_token: str):  # noqa: ARG002
            return {"access_token": "new-token", "refresh_token": "refresh-token", "expires_at": 123}

    monkeypatch.setattr("data_connector.google_sheets.auth.GoogleOAuth2Client", _FakeOAuth)

    await service._poll_google_sheets(source)

    assert registry.upserts, "Expected refresh token update"


@pytest.mark.asyncio
async def test_trigger_service_publish_outbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(trigger_module, "get_tracing_service", lambda *_: _FakeTracing())

    service = ConnectorTriggerService()
    registry = _FakeRegistry()
    producer = _FakeProducer({"bootstrap.servers": "localhost:1234"})

    now = datetime.now(timezone.utc)
    registry.outbox_batches = [
        [
            OutboxItem(
                outbox_id="outbox-1",
                event_id="event-1",
                source_type="google_sheets",
                source_id="sheet-1",
                sequence_number=1,
                payload={"metadata": {"traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"}},
                status="pending",
                publish_attempts=0,
                created_at=now,
                published_at=None,
                last_error=None,
            )
        ]
    ]

    service.registry = registry
    service.producer = producer
    service.tracing = _FakeTracing()
    service.running = True

    registry.on_claim = lambda: setattr(service, "running", False)

    await asyncio.wait_for(service._publish_outbox_loop(), timeout=1)

    assert producer.produced, "Expected producer.produce to be called"
    assert registry.published == ["outbox-1"]
