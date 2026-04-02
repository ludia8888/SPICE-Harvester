from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from message_relay.main import EventPublisher


class _AsyncBody:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.closed = False

    async def read(self) -> bytes:
        return self.payload

    def close(self) -> None:
        self.closed = True


class _ClientContext:
    def __init__(self, client) -> None:  # noqa: ANN001
        self._client = client

    async def __aenter__(self):  # noqa: ANN201
        return self._client

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        _ = exc_type, exc, tb
        return False


class _Session:
    def __init__(self, client) -> None:  # noqa: ANN001
        self._client = client

    def client(self, *args, **kwargs):  # noqa: ANN002, ANN003, ANN201
        _ = args, kwargs
        return _ClientContext(self._client)


@pytest.mark.asyncio
async def test_load_checkpoint_returns_empty_for_missing_object() -> None:
    body = _AsyncBody(b"{}")

    class _Client:
        async def get_object(self, **kwargs):  # noqa: ANN003, ANN201
            _ = kwargs
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

    publisher = EventPublisher.__new__(EventPublisher)
    publisher.session = _Session(_Client())
    publisher.bucket_name = "bucket"
    publisher.checkpoint_key = "checkpoint.json"
    publisher._s3_client_kwargs = lambda: {}

    checkpoint = await EventPublisher._load_checkpoint(publisher)

    assert checkpoint == {}
    assert body.closed is False


@pytest.mark.asyncio
async def test_load_checkpoint_raises_for_non_missing_client_error() -> None:
    class _Client:
        async def get_object(self, **kwargs):  # noqa: ANN003, ANN201
            _ = kwargs
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")

    publisher = EventPublisher.__new__(EventPublisher)
    publisher.session = _Session(_Client())
    publisher.bucket_name = "bucket"
    publisher.checkpoint_key = "checkpoint.json"
    publisher._s3_client_kwargs = lambda: {}

    with pytest.raises(ClientError):
        await EventPublisher._load_checkpoint(publisher)


@pytest.mark.asyncio
async def test_process_events_propagates_checkpoint_load_failures() -> None:
    publisher = EventPublisher.__new__(EventPublisher)

    async def _raise_checkpoint():
        raise RuntimeError("checkpoint unavailable")

    publisher._load_checkpoint = _raise_checkpoint

    with pytest.raises(RuntimeError, match="checkpoint unavailable"):
        await EventPublisher.process_events(publisher)


def test_s3_client_kwargs_derive_tls_from_endpoint_scheme() -> None:
    publisher = EventPublisher.__new__(EventPublisher)
    publisher.endpoint_url = "https://minio.example.com"
    publisher.access_key = "access"
    publisher.secret_key = "secret"

    kwargs = EventPublisher._s3_client_kwargs(publisher)

    assert kwargs["use_ssl"] is True
