"""Tests for OMS Foundry-style Attachment Property router."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from oms.routers import attachments as attachments_module
from oms.routers.attachments import attachments_upload_router, attachments_property_router
from shared.dependencies.providers import get_elasticsearch_service, get_storage_service
from shared.security.database_access import DatabaseAccessRegistryUnavailableError

app = FastAPI()
app.include_router(attachments_upload_router)
app.include_router(attachments_property_router)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_ATTACHMENT_META = {
    "rid": "ri.attachments.main.attachment.aaaa-bbbb-cccc",
    "filename": "report.pdf",
    "sizeBytes": 12345,
    "mediaType": "application/pdf",
}

_ES_HIT_SINGLE = {
    "instance_id": "emp-001",
    "class_id": "Employee",
    "properties": [
        {
            "name": "resume",
            "value": json.dumps(_ATTACHMENT_META),
            "type": "attachment",
        },
        {"name": "name", "value": "Alice", "type": "string"},
    ],
}

_ES_HIT_SINGLE_ID_ONLY = {
    "instance_id": "emp-001",
    "class_id": "Employee",
    "properties": [
        {
            "id": "resume",
            "value": json.dumps(_ATTACHMENT_META),
            "type": "attachment",
        },
    ],
}

_MULTI_ATTACHMENTS = [
    _ATTACHMENT_META,
    {
        "rid": "ri.attachments.main.attachment.dddd-eeee-ffff",
        "filename": "photo.jpg",
        "sizeBytes": 67890,
        "mediaType": "image/jpeg",
    },
]

_ES_HIT_MULTI = {
    "instance_id": "emp-002",
    "class_id": "Employee",
    "properties": [
        {
            "name": "documents",
            "value": json.dumps(_MULTI_ATTACHMENTS),
            "type": "attachment",
        },
    ],
}


@pytest.fixture
def mock_es():
    es = AsyncMock()
    es.search = AsyncMock(return_value={"total": 1, "hits": [_ES_HIT_SINGLE]})
    return es


@pytest.fixture
def mock_storage():
    storage = AsyncMock()
    storage.save_bytes = AsyncMock(return_value="sha256-checksum")
    storage.load_bytes = AsyncMock(return_value=b"%PDF-1.4 binary content")
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
# Upload
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upload_attachment_stores_to_s3_and_returns_metadata(mock_storage):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/v2/ontologies/attachments/upload?filename=report.pdf",
            content=b"fake-pdf-bytes",
            headers={"Content-Type": "application/octet-stream"},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["filename"] == "report.pdf"
    assert body["sizeBytes"] == len(b"fake-pdf-bytes")
    assert body["mediaType"] == "application/pdf"
    assert body["rid"].startswith("ri.attachments.main.attachment.")
    mock_storage.save_bytes.assert_called_once()


@pytest.mark.asyncio
async def test_upload_attachment_rejects_empty_body(mock_storage):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/v2/ontologies/attachments/upload?filename=empty.txt",
            content=b"",
            headers={"Content-Type": "application/octet-stream"},
        )

    assert resp.status_code == 400
    assert resp.json()["errorCode"] == "INVALID_ARGUMENT"


# ---------------------------------------------------------------------------
# List property attachments
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_property_attachments_single(mock_es):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v2/ontologies/test_db/objects/Employee/emp-001/attachments/resume",
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["type"] == "single"
    assert body["rid"] == "ri.attachments.main.attachment.aaaa-bbbb-cccc"
    assert body["filename"] == "report.pdf"


@pytest.mark.asyncio
async def test_list_property_attachments_supports_id_only_property_entries(mock_es):
    mock_es.search = AsyncMock(return_value={"total": 1, "hits": [_ES_HIT_SINGLE_ID_ONLY]})

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v2/ontologies/test_db/objects/Employee/emp-001/attachments/resume",
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["type"] == "single"
    assert body["rid"] == "ri.attachments.main.attachment.aaaa-bbbb-cccc"


@pytest.mark.asyncio
async def test_list_property_attachments_multiple(mock_es):
    mock_es.search = AsyncMock(return_value={"total": 1, "hits": [_ES_HIT_MULTI]})

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v2/ontologies/test_db/objects/Employee/emp-002/attachments/documents",
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["type"] == "multiple"
    assert len(body["data"]) == 2
    assert body["data"][0]["rid"] == "ri.attachments.main.attachment.aaaa-bbbb-cccc"
    assert body["data"][1]["rid"] == "ri.attachments.main.attachment.dddd-eeee-ffff"


@pytest.mark.asyncio
async def test_list_property_attachments_404_not_found(mock_es):
    mock_es.search = AsyncMock(return_value={"total": 0, "hits": []})

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v2/ontologies/test_db/objects/Employee/unknown/attachments/resume",
        )

    assert resp.status_code == 404
    assert resp.json()["errorCode"] == "ObjectNotFound"


# ---------------------------------------------------------------------------
# Get attachment by RID
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_attachment_by_rid_returns_metadata(mock_es):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v2/ontologies/test_db/objects/Employee/emp-001/attachments/resume/ri.attachments.main.attachment.aaaa-bbbb-cccc",
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["rid"] == "ri.attachments.main.attachment.aaaa-bbbb-cccc"
    assert body["filename"] == "report.pdf"


@pytest.mark.asyncio
async def test_get_attachment_by_rid_not_found(mock_es):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v2/ontologies/test_db/objects/Employee/emp-001/attachments/resume/ri.attachments.main.attachment.nonexistent",
        )

    assert resp.status_code == 404
    assert resp.json()["errorCode"] == "AttachmentNotFound"


# ---------------------------------------------------------------------------
# Get attachment content
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_attachment_content_returns_binary(mock_es, mock_storage):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v2/ontologies/test_db/objects/Employee/emp-001/attachments/resume/content",
        )

    assert resp.status_code == 200
    assert resp.content == b"%PDF-1.4 binary content"
    assert "application/pdf" in resp.headers.get("content-type", "")


@pytest.mark.asyncio
async def test_get_attachment_content_by_rid_returns_binary(mock_es, mock_storage):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v2/ontologies/test_db/objects/Employee/emp-001/attachments/resume/ri.attachments.main.attachment.aaaa-bbbb-cccc/content",
        )

    assert resp.status_code == 200
    assert resp.content == b"%PDF-1.4 binary content"


@pytest.mark.asyncio
async def test_get_attachment_content_404_when_s3_missing(mock_es, mock_storage):
    mock_storage.load_bytes = AsyncMock(side_effect=FileNotFoundError("not found"))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v2/ontologies/test_db/objects/Employee/emp-001/attachments/resume/content",
        )

    assert resp.status_code == 404
    assert resp.json()["errorCode"] == "AttachmentContentNotFound"


@pytest.mark.asyncio
async def test_attachments_skip_role_enforcement_without_actor_headers(monkeypatch: pytest.MonkeyPatch):
    checker = AsyncMock()
    monkeypatch.setattr(attachments_module, "enforce_database_role", checker)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v2/ontologies/test_db/objects/Employee/emp-001/attachments/resume",
        )

    assert resp.status_code == 200
    checker.assert_not_awaited()


@pytest.mark.asyncio
async def test_attachments_return_permission_denied_when_actor_header_present_and_role_check_fails(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _deny(**kwargs):  # noqa: ANN001, ANN003
        _ = kwargs
        raise ValueError("permission denied")

    monkeypatch.setattr(attachments_module, "enforce_database_role", _deny)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v2/ontologies/test_db/objects/Employee/emp-001/attachments/resume",
            headers={"X-User-ID": "alice"},
        )

    assert resp.status_code == 403
    body = resp.json()
    assert body["errorCode"] == "PERMISSION_DENIED"
    assert body["errorName"] == "PermissionDenied"


@pytest.mark.asyncio
async def test_attachments_allow_registry_degrade_when_actor_role_check_is_unverifiable(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _unavailable(**kwargs):  # noqa: ANN001, ANN003
        _ = kwargs
        raise DatabaseAccessRegistryUnavailableError("Database access registry unavailable")

    monkeypatch.setattr(attachments_module, "enforce_database_role", _unavailable)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/v2/ontologies/test_db/objects/Employee/emp-001/attachments/resume",
            headers={"X-User-ID": "alice"},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["type"] == "single"
    assert body["rid"] == "ri.attachments.main.attachment.aaaa-bbbb-cccc"
