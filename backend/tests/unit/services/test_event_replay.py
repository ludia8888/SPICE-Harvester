from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone

import boto3
import pytest
from botocore.client import Config

from shared.services.event_replay import EventReplayService


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin123"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except Exception:
        client.create_bucket(Bucket=bucket)


def _put_event(client, bucket: str, key: str, payload: dict) -> None:
    client.put_object(Bucket=bucket, Key=key, Body=json.dumps(payload).encode("utf-8"))


def _cleanup_prefix(client, bucket: str, prefix: str) -> None:
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = response.get("Contents", [])
    if not contents:
        return
    for obj in contents:
        client.delete_object(Bucket=bucket, Key=obj["Key"])


@pytest.mark.asyncio
async def test_event_replay_aggregate_and_history() -> None:
    client = _s3_client()
    bucket = "instance-events"
    _ensure_bucket(client, bucket)

    db_name = f"db-{uuid.uuid4().hex[:6]}"
    class_id = "Customer"
    aggregate_id = "cust-1"
    prefix = f"{db_name}/{class_id}/{aggregate_id}/"

    service = EventReplayService(
        s3_endpoint=os.environ.get("MINIO_ENDPOINT_URL", "http://localhost:9000"),
        s3_access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        s3_secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin123"),
        bucket_name=bucket,
    )

    try:
        create_event = {
            "command_type": "CREATE_INSTANCE",
            "command_id": "cmd-1",
            "sequence_number": 1,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "payload": {"name": "Alice"},
        }
        update_event = {
            "command_type": "UPDATE_INSTANCE",
            "command_id": "cmd-2",
            "sequence_number": 2,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": {"email": "alice@example.com"},
        }

        _put_event(client, bucket, prefix + "1.json", create_event)
        _put_event(client, bucket, prefix + "2.json", update_event)

        replay = await service.replay_aggregate(db_name, class_id, aggregate_id)
        assert replay["status"] == "REPLAYED"
        assert replay["event_count"] == 2
        assert replay["final_state"]["data"]["email"] == "alice@example.com"

        history = await service.get_aggregate_history(db_name, class_id, aggregate_id)
        assert len(history) == 2
        assert history[0]["command_id"] == "cmd-1"
    finally:
        _cleanup_prefix(client, bucket, prefix)


@pytest.mark.asyncio
async def test_event_replay_all_and_determinism() -> None:
    client = _s3_client()
    bucket = "instance-events"
    _ensure_bucket(client, bucket)

    db_name = f"db-{uuid.uuid4().hex[:6]}"
    class_id = "Order"
    prefix = f"{db_name}/{class_id}/"

    service = EventReplayService(
        s3_endpoint=os.environ.get("MINIO_ENDPOINT_URL", "http://localhost:9000"),
        s3_access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        s3_secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin123"),
        bucket_name=bucket,
    )

    try:
        for agg_id in ("order-1", "order-2"):
            key_prefix = f"{prefix}{agg_id}/"
            _put_event(
                client,
                bucket,
                key_prefix + "1.json",
                {
                    "command_type": "CREATE_INSTANCE",
                    "command_id": f"cmd-{agg_id}",
                    "sequence_number": 1,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "payload": {"total": 10},
                },
            )

        replay_all = await service.replay_all_aggregates(db_name, class_id)
        assert replay_all["status"] == "REPLAYED"
        assert replay_all["aggregate_count"] == 2

        deterministic = await service.verify_replay_determinism(db_name, class_id, "order-1")
        assert deterministic["deterministic"] is True
    finally:
        _cleanup_prefix(client, bucket, prefix)
