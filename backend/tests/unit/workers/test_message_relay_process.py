from __future__ import annotations

import os
import time
import uuid
from datetime import datetime, timezone

import boto3
import pytest
from botocore.client import Config

from message_relay.main import EventPublisher
from shared.config.app_config import AppConfig


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin123"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _cleanup_bucket(client, bucket: str) -> None:
    response = client.list_objects_v2(Bucket=bucket)
    for obj in response.get("Contents", []):
        client.delete_object(Bucket=bucket, Key=obj["Key"])
    try:
        client.delete_bucket(Bucket=bucket)
    except Exception:
        pass


@pytest.mark.asyncio
async def test_event_publisher_processes_index(monkeypatch: pytest.MonkeyPatch) -> None:
    bucket = f"event-store-test-{uuid.uuid4().hex[:8]}"
    monkeypatch.setenv("EVENT_STORE_BUCKET", bucket)
    monkeypatch.setenv("EVENT_PUBLISHER_BATCH_SIZE", "1")

    publisher = EventPublisher()
    await publisher.initialize()

    client = _s3_client()

    try:
        event_id = uuid.uuid4().hex
        ts_ms = int(time.time() * 1000)
        day = datetime.now(timezone.utc)
        index_prefix = f"indexes/by-date/{day.year:04d}/{day.month:02d}/{day.day:02d}/"
        index_key = f"{index_prefix}{ts_ms}_{event_id}.json"
        event_key = f"events/test/{event_id}.json"

        client.put_object(
            Bucket=bucket,
            Key=event_key,
            Body=b"{\"event_id\": \"" + event_id.encode("utf-8") + b"\"}",
        )
        client.put_object(
            Bucket=bucket,
            Key=index_key,
            Body=(
                "{"
                f"\"s3_key\":\"{event_key}\","
                f"\"event_id\":\"{event_id}\","
                f"\"aggregate_id\":\"agg-1\","
                f"\"kafka_topic\":\"{AppConfig.INSTANCE_EVENTS_TOPIC}\""
                "}"
            ).encode("utf-8"),
        )

        published = await publisher.process_events()
        assert published == 1
    finally:
        await publisher.shutdown()
        _cleanup_bucket(client, bucket)
