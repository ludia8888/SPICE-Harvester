from __future__ import annotations

import asyncio
import os
import time
import uuid
from urllib.parse import urlparse

import pytest

from shared.config.settings import get_settings
from shared.services.core.command_status_service import CommandStatusService
from shared.services.kafka.safe_consumer import SafeKafkaConsumer
from shared.services.storage.lakefs_client import LakeFSClient
from shared.services.storage.lakefs_storage_service import create_lakefs_storage_service
from shared.services.storage.redis_service import RedisService
from shared.services.storage.storage_service import create_storage_service

pytestmark = [pytest.mark.smoke, pytest.mark.requires_infra]


def _redis_params() -> tuple[str, int, str, int]:
    redis_url = (os.getenv("REDIS_URL") or "").strip()
    if redis_url:
        parsed = urlparse(redis_url)
        host = parsed.hostname or "localhost"
        port = parsed.port or 6379
        password = parsed.password or ""
        db = int((parsed.path or "/0").lstrip("/") or 0)
        return host, port, password, db
    return ("localhost", 6379, "", 0)


def _postgres_dsn() -> str:
    return (
        os.getenv("POSTGRES_URL")
        or os.getenv("POSTGRES_DSN")
        or "postgresql://spiceadmin:spicepass123@localhost:5433/spicedb"
    )


def _kafka_bootstrap() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS") or os.getenv("KAFKA_SERVERS") or "localhost:39092"


@pytest.mark.asyncio
@pytest.mark.redis_boundary
async def test_redis_boundary_command_status_roundtrip() -> None:
    host, port, password, db = _redis_params()
    redis_service = RedisService(host=host, port=port, password=password or None, db=db)
    await redis_service.connect()
    try:
        service = CommandStatusService(redis_service)
        command_id = f"smoke-{uuid.uuid4().hex}"
        await service.create_command_status(
            command_id=command_id,
            command_type="SMOKE_BOUNDARY",
            aggregate_id="agg",
            payload={"kind": "redis_boundary"},
            user_id=None,
        )
        raw_status = await redis_service.get_command_status(command_id)
        assert raw_status is not None
        assert raw_status.get("status") == "PENDING"
        assert ((raw_status.get("data") or {}).get("command_type")) == "SMOKE_BOUNDARY"

        status = await service.get_command_details(command_id)
        assert status is not None
        assert status.get("command_id") == command_id
        assert status.get("command_type") == "SMOKE_BOUNDARY"
    finally:
        await redis_service.disconnect()


@pytest.mark.asyncio
@pytest.mark.postgres_boundary
async def test_postgres_boundary_basic_query() -> None:
    try:
        import asyncpg
    except ImportError:
        pytest.fail("asyncpg not installed (required for Postgres boundary smoke)")

    conn = await asyncpg.connect(_postgres_dsn())
    try:
        assert await conn.fetchval("SELECT 1") == 1
    finally:
        await conn.close()


@pytest.mark.kafka_boundary
def test_kafka_boundary_produce_consume_roundtrip() -> None:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    bootstrap = _kafka_bootstrap()
    topic = f"spice-smoke-boundary-{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": bootstrap})
    try:
        futures = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)], request_timeout=10.0)
        for future in futures.values():
            try:
                future.result()
            except Exception as exc:
                if "already exists" not in str(exc).lower():
                    raise
    except Exception:
        pass

    producer = Producer({"bootstrap.servers": bootstrap, "client.id": "smoke-boundary-producer"})
    value = f"smoke-{uuid.uuid4().hex}".encode("utf-8")
    producer.produce(topic=topic, key=b"k", value=value)
    producer.flush(10.0)

    consumer = SafeKafkaConsumer(
        group_id=f"smoke-boundary-consumer-{uuid.uuid4().hex[:8]}",
        topics=[topic],
        service_name="smoke-boundary",
        extra_config={"bootstrap.servers": bootstrap, "auto.offset.reset": "earliest"},
    )
    try:
        deadline = time.monotonic() + 20.0
        seen = []
        while time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue
            if msg.value() is not None:
                seen.append(msg.value())
                consumer.commit_sync(msg)
                if value in seen:
                    break
        assert value in seen
    finally:
        consumer.close()
        try:
            futures = admin.delete_topics([topic], operation_timeout=10)
            for future in futures.values():
                future.result()
        except Exception:
            pass


@pytest.mark.asyncio
@pytest.mark.s3_boundary
async def test_s3_boundary_storage_roundtrip() -> None:
    storage = create_storage_service(get_settings())
    assert storage is not None, "StorageService unavailable (boto3 missing?)"

    bucket = "spice-smoke-boundary"
    key = f"s3-boundary/{uuid.uuid4().hex}.json"
    payload = {"kind": "s3_boundary", "value": uuid.uuid4().hex}

    await storage.create_bucket(bucket)
    await storage.save_json(bucket, key, payload)
    loaded = await storage.load_json(bucket, key)
    assert loaded["kind"] == payload["kind"]
    assert loaded["value"] == payload["value"]
    assert await storage.delete_object(bucket, key) is True


@pytest.mark.asyncio
@pytest.mark.lakefs_boundary
@pytest.mark.s3_boundary
async def test_lakefs_boundary_branch_commit_roundtrip() -> None:
    settings = get_settings()
    repo = (os.getenv("LAKEFS_RAW_REPOSITORY") or settings.storage.lakefs_raw_repository or "raw-datasets").strip()
    branch = f"smoke-boundary-{uuid.uuid4().hex[:8]}"
    relative_key = f"smoke/{uuid.uuid4().hex}.json"

    lakefs_client = LakeFSClient()
    lakefs_storage = create_lakefs_storage_service(settings)
    assert lakefs_storage is not None, "LakeFSStorageService unavailable (boto3 missing?)"

    try:
        await lakefs_client.create_branch(repository=repo, name=branch, source="main")
        await lakefs_storage.save_json(
            bucket=repo,
            key=f"{branch}/{relative_key}",
            data={"kind": "lakefs_boundary", "branch": branch},
        )
        commit_id = await lakefs_client.commit(
            repository=repo,
            branch=branch,
            message=f"smoke boundary {branch}",
            metadata={"kind": "lakefs_boundary"},
        )
        head_commit_id = await lakefs_client.get_branch_head_commit_id(repository=repo, branch=branch)
        assert head_commit_id == commit_id
        objects = await lakefs_client.list_objects(repository=repo, ref=commit_id, prefix="smoke/")
        assert objects, "lakeFS commit did not expose the smoke object prefix"
    finally:
        try:
            await lakefs_client.delete_branch(repository=repo, name=branch)
        except Exception:
            pass
