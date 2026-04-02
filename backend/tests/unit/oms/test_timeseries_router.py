"""Tests for OMS Foundry-style Time Series Property router."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from oms.routers import timeseries as timeseries_module
from oms.routers.timeseries import timeseries_router
from shared.dependencies.providers import get_elasticsearch_service, get_storage_service
from shared.security.database_access import DatabaseAccessRegistryUnavailableError

app = FastAPI()
app.include_router(timeseries_router)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_TS_POINTS = [
    {"time": "2024-01-01T00:00:00Z", "value": 10.0},
    {"time": "2024-01-02T00:00:00Z", "value": 20.5},
    {"time": "2024-01-03T00:00:00Z", "value": 30.1},
    {"time": "2024-01-04T00:00:00Z", "value": 25.7},
]

_ES_HIT_WITH_TS = {
    "instance_id": "sensor-001",
    "class_id": "Sensor",
    "properties": [
        {"name": "temperature", "value": "s3://timeseries-data/test_db/main/Sensor/sensor-001/temperature.json", "type": "timeseries"},
        {"name": "name", "value": "Main Sensor", "type": "string"},
    ],
}

_ES_HIT_WITH_TS_ID_ONLY = {
    "instance_id": "sensor-001",
    "class_id": "Sensor",
    "properties": [
        {"id": "temperature", "value": "s3://timeseries-data/test_db/main/Sensor/sensor-001/temperature.json", "type": "timeseries"},
    ],
}


@pytest.fixture
def mock_es():
    es = AsyncMock()
    es.search = AsyncMock(return_value={"total": 1, "hits": [_ES_HIT_WITH_TS]})
    return es


@pytest.fixture
def mock_storage():
    storage = AsyncMock()
    storage.load_json = AsyncMock(return_value={"points": _TS_POINTS})
    storage.save_bytes = AsyncMock(return_value="abc123")
    return storage


@pytest.fixture(autouse=True)
def override_deps(mock_es, mock_storage):
    async def fake_es():
        return mock_es

    async def fake_storage():
        return mock_storage

    app.dependency_overrides[get_elasticsearch_service] = fake_es
    app.dependency_overrides[get_storage_service] = fake_storage
    yield
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# firstPoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_first_point_returns_earliest_point(mock_es, mock_storage):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v2/ontologies/test_db/objects/Sensor/sensor-001/timeseries/temperature/firstPoint",
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["time"] == "2024-01-01T00:00:00Z"
    assert body["value"] == 10.0


@pytest.mark.asyncio
async def test_get_first_point_404_when_instance_not_found(mock_es):
    mock_es.search = AsyncMock(return_value={"total": 0, "hits": []})

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v2/ontologies/test_db/objects/Sensor/unknown/timeseries/temperature/firstPoint",
        )

    assert resp.status_code == 404
    assert resp.json()["errorCode"] == "ObjectNotFound"


@pytest.mark.asyncio
async def test_get_first_point_404_when_property_not_timeseries(mock_es):
    mock_es.search = AsyncMock(return_value={
        "total": 1,
        "hits": [{
            "instance_id": "sensor-001",
            "class_id": "Sensor",
            "properties": [{"name": "name", "value": "Main", "type": "string"}],
        }],
    })

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v2/ontologies/test_db/objects/Sensor/sensor-001/timeseries/temperature/firstPoint",
        )

    assert resp.status_code == 404
    assert resp.json()["errorCode"] == "PropertyNotFound"


# ---------------------------------------------------------------------------
# lastPoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_last_point_returns_latest_point(mock_es, mock_storage):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v2/ontologies/test_db/objects/Sensor/sensor-001/timeseries/temperature/lastPoint",
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["time"] == "2024-01-04T00:00:00Z"
    assert body["value"] == 25.7


@pytest.mark.asyncio
async def test_get_first_point_supports_id_only_property_entries(mock_es, mock_storage):
    mock_es.search = AsyncMock(return_value={"total": 1, "hits": [_ES_HIT_WITH_TS_ID_ONLY]})

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v2/ontologies/test_db/objects/Sensor/sensor-001/timeseries/temperature/firstPoint",
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["time"] == "2024-01-01T00:00:00Z"


# ---------------------------------------------------------------------------
# streamPoints
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_points_returns_all_points(mock_es, mock_storage):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/v2/ontologies/test_db/objects/Sensor/sensor-001/timeseries/temperature/streamPoints",
            json={},
        )

    assert resp.status_code == 200
    lines = [line for line in resp.text.strip().split("\n") if line.strip()]
    assert len(lines) == 4
    first = json.loads(lines[0])
    assert first["time"] == "2024-01-01T00:00:00Z"


@pytest.mark.asyncio
async def test_stream_points_with_absolute_range(mock_es, mock_storage):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/v2/ontologies/test_db/objects/Sensor/sensor-001/timeseries/temperature/streamPoints",
            json={
                "range": {
                    "type": "absolute",
                    "startTime": "2024-01-02T00:00:00Z",
                    "endTime": "2024-01-03T00:00:00Z",
                },
            },
        )

    assert resp.status_code == 200
    lines = [line for line in resp.text.strip().split("\n") if line.strip()]
    assert len(lines) == 2
    points = [json.loads(line) for line in lines]
    assert points[0]["time"] == "2024-01-02T00:00:00Z"
    assert points[1]["time"] == "2024-01-03T00:00:00Z"


@pytest.mark.asyncio
async def test_stream_points_404_when_no_ts_data(mock_es, mock_storage):
    mock_storage.load_json = AsyncMock(side_effect=FileNotFoundError("not found"))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/v2/ontologies/test_db/objects/Sensor/sensor-001/timeseries/temperature/streamPoints",
            json={},
        )

    # streamPoints returns empty stream when no data (not 404)
    assert resp.status_code == 200
    assert resp.text.strip() == ""


@pytest.mark.asyncio
async def test_timeseries_skips_role_enforcement_without_actor_headers(monkeypatch: pytest.MonkeyPatch):
    checker = AsyncMock()
    monkeypatch.setattr(timeseries_module, "enforce_database_role", checker)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v2/ontologies/test_db/objects/Sensor/sensor-001/timeseries/temperature/firstPoint",
        )

    assert resp.status_code == 200
    checker.assert_not_awaited()


@pytest.mark.asyncio
async def test_timeseries_returns_permission_denied_when_actor_header_present_and_role_check_fails(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _deny(**kwargs):  # noqa: ANN001, ANN003
        _ = kwargs
        raise ValueError("permission denied")

    monkeypatch.setattr(timeseries_module, "enforce_database_role", _deny)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v2/ontologies/test_db/objects/Sensor/sensor-001/timeseries/temperature/firstPoint",
            headers={"X-User-ID": "alice"},
        )

    assert resp.status_code == 403
    body = resp.json()
    assert body["errorCode"] == "PERMISSION_DENIED"
    assert body["errorName"] == "PermissionDenied"


@pytest.mark.asyncio
async def test_timeseries_allows_registry_degrade_when_actor_role_check_is_unverifiable(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _unavailable(**kwargs):  # noqa: ANN001, ANN003
        _ = kwargs
        raise DatabaseAccessRegistryUnavailableError("Database access registry unavailable")

    monkeypatch.setattr(timeseries_module, "enforce_database_role", _unavailable)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v2/ontologies/test_db/objects/Sensor/sensor-001/timeseries/temperature/firstPoint",
            headers={"X-User-ID": "alice"},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["time"] == "2024-01-01T00:00:00Z"
    assert body["value"] == 10.0
