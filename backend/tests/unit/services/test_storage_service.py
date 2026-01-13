from datetime import datetime, timezone

import pytest

from shared.services.storage_service import StorageService


class _FakeS3Client:
    def __init__(self, pages):
        self._pages = pages

    def list_objects_v2(self, **kwargs):
        token = kwargs.get("ContinuationToken")
        return self._pages.get(token, {"IsTruncated": False, "Contents": []})


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
    import shared.services.storage_service as storage_module

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
    import shared.services.storage_service as storage_module

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
    import shared.services.storage_service as storage_module

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
