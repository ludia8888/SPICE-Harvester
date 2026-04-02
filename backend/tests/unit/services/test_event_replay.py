from __future__ import annotations

import io
import json
import uuid
from datetime import datetime, timezone

import pytest

from shared.services.events.event_replay import EventReplayService


class _InMemoryS3Client:
    def __init__(self) -> None:
        self._buckets: dict[str, dict[str, dict]] = {}

    def head_bucket(self, *, Bucket: str) -> None:  # noqa: N803
        if Bucket not in self._buckets:
            raise RuntimeError("Bucket not found")

    def create_bucket(self, *, Bucket: str) -> None:  # noqa: N803
        self._buckets.setdefault(Bucket, {})

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None:  # noqa: N803
        self.create_bucket(Bucket=Bucket)
        self._buckets[Bucket][Key] = {
            "Body": bytes(Body),
            "LastModified": datetime.now(timezone.utc),
        }

    def get_object(self, *, Bucket: str, Key: str):  # noqa: N803, ANN001
        body = self._buckets[Bucket][Key]["Body"]
        return {"Body": io.BytesIO(body)}

    def list_objects_v2(
        self,
        *,
        Bucket: str,
        Prefix: str,
        MaxKeys: int = 1000,
        ContinuationToken: str | None = None,
    ):  # noqa: N803, ANN001
        objects = self._buckets.get(Bucket, {})
        keys = [k for k in objects.keys() if k.startswith(Prefix)]
        keys = sorted(keys)
        start = int(ContinuationToken or 0)
        page = keys[start : start + int(MaxKeys)]
        if not page:
            return {}
        next_index = start + len(page)
        return {
            "Contents": [
                {
                    "Key": key,
                    "LastModified": objects[key]["LastModified"],
                    "Size": len(objects[key]["Body"]),
                }
                for key in page
            ],
            "IsTruncated": next_index < len(keys),
            "NextContinuationToken": str(next_index) if next_index < len(keys) else None,
        }

    def delete_object(self, *, Bucket: str, Key: str) -> None:  # noqa: N803
        bucket = self._buckets.get(Bucket, {})
        bucket.pop(Key, None)


def _s3_client():
    return _InMemoryS3Client()


def _ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except Exception:
        client.create_bucket(Bucket=bucket)


def _put_event(client, bucket: str, key: str, payload: dict) -> None:
    client.put_object(Bucket=bucket, Key=key, Body=json.dumps(payload).encode("utf-8"))


def _put_current_layout_event(
    client,
    bucket: str,
    *,
    class_id: str,
    aggregate_id: str,
    event_id: str,
    sequence_number: int,
    payload: dict,
    command_type: str,
) -> None:
    event_key = f"events/2026/01/01/{class_id}/{aggregate_id}/{event_id}.json"
    _put_event(
        client,
        bucket,
        event_key,
        {
            "event_id": event_id,
            "event_type": f"{command_type}_REQUESTED",
            "aggregate_type": class_id,
            "aggregate_id": aggregate_id,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "actor": "tester",
            "data": {
                "db_name": payload["db_name"],
                "payload": payload["payload"],
            },
            "metadata": {
                "command_type": command_type,
                "command_id": payload["command_id"],
            },
            "schema_version": "1",
            "sequence_number": sequence_number,
        },
    )
    _put_event(
        client,
        bucket,
        f"indexes/by-aggregate/{class_id}/{aggregate_id}/{sequence_number:020d}_{event_id}.json",
        {
            "event_id": event_id,
            "event_type": f"{command_type}_REQUESTED",
            "aggregate_type": class_id,
            "aggregate_id": aggregate_id,
            "sequence_number": sequence_number,
            "s3_key": event_key,
        },
    )


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

    service = EventReplayService(bucket_name=bucket, s3_client=client)

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

    service = EventReplayService(bucket_name=bucket, s3_client=client)

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


@pytest.mark.asyncio
async def test_event_replay_paginates_and_not_found_is_deterministic() -> None:
    client = _s3_client()
    bucket = "instance-events"
    _ensure_bucket(client, bucket)

    db_name = f"db-{uuid.uuid4().hex[:6]}"
    class_id = "Account"
    aggregate_id = "acct-1"
    prefix = f"{db_name}/{class_id}/{aggregate_id}/"
    service = EventReplayService(bucket_name=bucket, s3_client=client)

    try:
        for idx in range(1005):
            _put_event(
                client,
                bucket,
                prefix + f"{idx:04d}.json",
                {
                    "command_type": "UPDATE_INSTANCE" if idx else "CREATE_INSTANCE",
                    "command_id": f"cmd-{idx}",
                    "sequence_number": idx + 1,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "payload": {"value": idx},
                },
            )

        replay = await service.replay_aggregate(db_name, class_id, aggregate_id)
        assert replay["event_count"] == 1005
        assert replay["total_events"] == 1005

        history = await service.get_aggregate_history(db_name, class_id, aggregate_id)
        assert len(history) == 1005

        missing = await service.verify_replay_determinism(db_name, class_id, "missing")
        assert missing["deterministic"] is True
        assert missing["hashes"] == ["status:NOT_FOUND", "status:NOT_FOUND", "status:NOT_FOUND"]
    finally:
        _cleanup_prefix(client, bucket, f"{db_name}/{class_id}/")


@pytest.mark.asyncio
async def test_event_replay_supports_current_event_store_layout() -> None:
    client = _s3_client()
    bucket = "instance-events"
    _ensure_bucket(client, bucket)

    db_name = "demo-db"
    class_id = "Invoice"
    aggregate_id = "inv-1"

    service = EventReplayService(bucket_name=bucket, s3_client=client)

    _put_current_layout_event(
        client,
        bucket,
        class_id=class_id,
        aggregate_id=aggregate_id,
        event_id="evt-1",
        sequence_number=1,
        command_type="CREATE_INSTANCE",
        payload={"db_name": db_name, "command_id": "cmd-1", "payload": {"amount": 10}},
    )
    _put_current_layout_event(
        client,
        bucket,
        class_id=class_id,
        aggregate_id=aggregate_id,
        event_id="evt-2",
        sequence_number=2,
        command_type="UPDATE_INSTANCE",
        payload={"db_name": db_name, "command_id": "cmd-2", "payload": {"amount": 25}},
    )

    replay = await service.replay_aggregate(db_name, class_id, aggregate_id)
    history = await service.get_aggregate_history(db_name, class_id, aggregate_id)
    replay_all = await service.replay_all_aggregates(db_name, class_id)

    assert replay["status"] == "REPLAYED"
    assert replay["event_count"] == 2
    assert replay["final_state"]["data"]["amount"] == 25
    assert [item["command_id"] for item in history] == ["cmd-1", "cmd-2"]
    assert replay_all["aggregates"][aggregate_id]["event_count"] == 2
