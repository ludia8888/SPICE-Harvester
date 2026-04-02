from datetime import datetime, timezone

import pytest
from botocore.exceptions import ClientError

from shared.errors.infra_errors import StorageUnavailableError
from shared.services.storage.storage_service import StorageService


class _FakeS3Client:
    def __init__(self, pages):
        self._pages = pages

    def list_objects_v2(self, **kwargs):
        token = kwargs.get("ContinuationToken")
        return self._pages.get(token, {"IsTruncated": False, "Contents": []})


class _ReadableBody:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self.closed = False

    def read(self) -> bytes:
        return self._payload

    def close(self) -> None:
        self.closed = True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_list_command_files_paginates_filters_and_sorts():
    prefix = "demo/main/Ticket/ticket-1/"
    t1 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
    t3 = datetime(2026, 1, 1, 0, 0, 2, tzinfo=timezone.utc)

    pages = {
        None: {
            "IsTruncated": True,
            "NextContinuationToken": "p2",
            "Contents": [
                {"Key": f"{prefix}b.json", "LastModified": t2},
                {"Key": f"{prefix}a.json", "LastModified": t1},
                {"Key": f"{prefix}note.txt", "LastModified": t3},
            ],
        },
        "p2": {
            "IsTruncated": False,
            "Contents": [
                {"Key": f"{prefix}c.json", "LastModified": t3},
            ],
        },
    }

    service = StorageService.__new__(StorageService)
    service.client = _FakeS3Client(pages)

    keys = await StorageService.list_command_files(service, bucket="bucket", prefix=prefix.rstrip("/"))
    assert keys == [f"{prefix}a.json", f"{prefix}b.json", f"{prefix}c.json"]


def test_storage_service_does_not_disable_tls_verify_by_default(monkeypatch):
    import shared.services.storage.storage_service as storage_module

    captured = {}

    class FakeBoto3:
        def client(self, service_name, **kwargs):
            captured["service_name"] = service_name
            captured["kwargs"] = kwargs
            return object()

    monkeypatch.setattr(storage_module, "HAS_BOTO3", True)
    monkeypatch.setattr(storage_module, "boto3", FakeBoto3(), raising=False)

    storage_module.StorageService(
        endpoint_url="https://example.com",
        access_key="access",
        secret_key="secret",
        use_ssl=True,
    )

    assert captured["service_name"] == "s3"
    assert "verify" not in captured["kwargs"]


def test_storage_service_respects_explicit_tls_verify_flag(monkeypatch):
    import shared.services.storage.storage_service as storage_module

    captured = {}

    class FakeBoto3:
        def client(self, service_name, **kwargs):
            captured["service_name"] = service_name
            captured["kwargs"] = kwargs
            return object()

    monkeypatch.setattr(storage_module, "HAS_BOTO3", True)
    monkeypatch.setattr(storage_module, "boto3", FakeBoto3(), raising=False)

    storage_module.StorageService(
        endpoint_url="https://example.com",
        access_key="access",
        secret_key="secret",
        use_ssl=True,
        ssl_verify=False,
    )

    assert captured["service_name"] == "s3"
    assert captured["kwargs"]["verify"] is False


def test_storage_service_supports_tls_ca_bundle_path(monkeypatch):
    import shared.services.storage.storage_service as storage_module

    captured = {}

    class FakeBoto3:
        def client(self, service_name, **kwargs):
            captured["service_name"] = service_name
            captured["kwargs"] = kwargs
            return object()

    monkeypatch.setattr(storage_module, "HAS_BOTO3", True)
    monkeypatch.setattr(storage_module, "boto3", FakeBoto3(), raising=False)

    storage_module.StorageService(
        endpoint_url="https://example.com",
        access_key="access",
        secret_key="secret",
        use_ssl=True,
        ssl_verify="/tmp/ca-bundle.pem",
    )

    assert captured["service_name"] == "s3"
    assert captured["kwargs"]["verify"] == "/tmp/ca-bundle.pem"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_replay_instance_state_best_effort_skips_invalid_command_files():
    service = StorageService.__new__(StorageService)

    async def _load_json(bucket: str, key: str):
        _ = bucket
        if key.endswith("bad.json"):
            raise ValueError("bad command")
        return {
            "command_type": "CREATE_INSTANCE",
            "command_id": "cmd-1",
            "instance_id": "ticket-1",
            "class_id": "Ticket",
            "db_name": "demo",
            "created_at": "2026-01-01T00:00:00Z",
            "payload": {"status": "OPEN"},
        }

    service.load_json = _load_json

    state = await StorageService.replay_instance_state(
        service,
        bucket="bucket",
        command_files=["bad.json", "good.json"],
    )

    assert state is not None
    assert state["status"] == "OPEN"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_replay_instance_state_strict_raises_on_invalid_command_file():
    service = StorageService.__new__(StorageService)

    async def _load_json(bucket: str, key: str):
        _ = bucket, key
        raise ValueError("bad command")

    service.load_json = _load_json

    with pytest.raises(RuntimeError, match="Failed to process command file bad.json"):
        await StorageService.replay_instance_state(
            service,
            bucket="bucket",
            command_files=["bad.json"],
            strict=True,
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_load_bytes_closes_stream_body() -> None:
    body = _ReadableBody(b"payload")

    class _Client:
        def get_object(self, **kwargs):
            _ = kwargs
            return {"Body": body}

    service = StorageService.__new__(StorageService)
    service.client = _Client()

    data = await StorageService.load_bytes(service, "bucket", "demo.json")

    assert data == b"payload"
    assert body.closed is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_list_objects_paginated_raises_typed_error_on_transport_failure() -> None:
    service = StorageService.__new__(StorageService)

    class _Client:
        def list_objects_v2(self, **kwargs):
            _ = kwargs
            raise ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "denied"}},
                "ListObjectsV2",
            )

    service.client = _Client()

    with pytest.raises(StorageUnavailableError):
        await StorageService.list_objects_paginated(service, bucket="bucket", prefix="demo/")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_bucket_exists_raises_typed_error_on_access_denied() -> None:
    service = StorageService.__new__(StorageService)

    class _Client:
        def head_bucket(self, **kwargs):
            _ = kwargs
            raise ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "denied"}},
                "HeadBucket",
            )

    service.client = _Client()

    with pytest.raises(StorageUnavailableError):
        await StorageService.bucket_exists(service, "bucket")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_delete_prefix_raises_when_delete_objects_reports_partial_failure() -> None:
    service = StorageService.__new__(StorageService)

    class _Client:
        def list_objects_v2(self, **kwargs):
            _ = kwargs
            return {
                "IsTruncated": False,
                "Contents": [{"Key": "demo/a.json"}],
            }

        def delete_objects(self, **kwargs):
            _ = kwargs
            return {"Errors": [{"Key": "demo/a.json", "Code": "AccessDenied", "Message": "denied"}]}

    service.client = _Client()

    with pytest.raises(StorageUnavailableError):
        await StorageService.delete_prefix(service, bucket="bucket", prefix="demo")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_delete_object_raises_typed_error_on_access_denied() -> None:
    service = StorageService.__new__(StorageService)

    class _Client:
        def delete_object(self, **kwargs):
            _ = kwargs
            raise ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "denied"}},
                "DeleteObject",
            )

    service.client = _Client()

    with pytest.raises(StorageUnavailableError):
        await StorageService.delete_object(service, "bucket", "demo.json")
