from __future__ import annotations

import pytest

from shared.services import pipeline_control_plane_events as cpe


@pytest.mark.asyncio
async def test_control_plane_events_always_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENABLE_PIPELINE_CONTROL_PLANE_EVENTS", "false")
    called = {"append": False}

    async def _connect() -> None:
        return None

    async def _append(event):  # noqa: ANN001
        called["append"] = True

    monkeypatch.setattr(cpe.event_store, "connect", _connect)
    monkeypatch.setattr(cpe.event_store, "append_event", _append)

    emitted = await cpe.emit_pipeline_control_plane_event(
        event_type="PIPELINE_BUILD_REQUESTED",
        pipeline_id="p1",
        event_id="job-1",
        data={"pipeline_id": "p1"},
    )

    assert emitted is True
    assert called["append"] is True


@pytest.mark.asyncio
async def test_control_plane_event_emits_with_topic(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENABLE_PIPELINE_CONTROL_PLANE_EVENTS", "true")
    captured = {}

    async def _connect() -> None:
        return None

    async def _append(event):  # noqa: ANN001
        captured["event"] = event

    monkeypatch.setattr(cpe.event_store, "connect", _connect)
    monkeypatch.setattr(cpe.event_store, "append_event", _append)

    emitted = await cpe.emit_pipeline_control_plane_event(
        event_type="PIPELINE_BUILD_REQUESTED",
        pipeline_id="p1",
        event_id="job/1",
        data={"pipeline_id": "p1"},
        actor="user1",
    )

    assert emitted is True
    event = captured.get("event")
    assert event is not None
    assert event.event_type == "PIPELINE_BUILD_REQUESTED"
    assert event.event_id == "job_1"
    assert event.metadata.get("kafka_topic")
