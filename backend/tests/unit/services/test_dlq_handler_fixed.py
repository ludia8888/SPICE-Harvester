from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone

import pytest
import redis.asyncio as aioredis
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from shared.config.service_config import ServiceConfig
from shared.services.dlq_handler_fixed import DLQHandlerFixed, FailedMessage, RetryPolicy


def _bootstrap_servers() -> str:
    return ServiceConfig.get_kafka_bootstrap_servers()


def _ensure_topic(topic: str) -> None:
    admin = AdminClient({"bootstrap.servers": _bootstrap_servers()})
    existing = admin.list_topics(timeout=10)
    if topic in existing.topics:
        return
    fs = admin.create_topics([NewTopic(topic=topic, num_partitions=1, replication_factor=1)])
    for _, f in fs.items():
        try:
            f.result(timeout=10)
        except Exception:
            pass


@pytest.mark.asyncio
async def test_dlq_handler_retry_and_poison_flow() -> None:
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    redis_client = aioredis.from_url(redis_url)

    dlq_topic = f"dlq-test-{uuid.uuid4().hex[:6]}"
    poison_topic = f"{dlq_topic}.poison"
    _ensure_topic(poison_topic)

    handler = DLQHandlerFixed(
        dlq_topic=dlq_topic,
        kafka_config={"bootstrap.servers": _bootstrap_servers()},
        redis_client=redis_client,
        retry_policy=RetryPolicy(max_retries=1),
        poison_topic=poison_topic,
    )
    handler.producer = Producer({"bootstrap.servers": _bootstrap_servers()})

    failed_msg = FailedMessage(
        message_id="msg-1",
        topic="topic-1",
        partition=0,
        offset=1,
        key=None,
        value=json.dumps({"payload": "x"}),
        error="schema validation failed",
        retry_count=0,
        first_failure_time=datetime.now(timezone.utc),
    )
    failed_msg.next_retry_time = failed_msg.calculate_next_retry_time(handler.retry_policy)

    try:
        await handler._add_to_retry_queue(failed_msg)
        stored = await redis_client.hgetall(f"dlq:retry:{failed_msg.message_id}")
        assert stored

        await handler._move_to_poison_queue(failed_msg)
        poison_list = await redis_client.lrange("dlq:poison:messages", 0, 0)
        assert poison_list
    finally:
        await redis_client.delete(f"dlq:retry:{failed_msg.message_id}")
        await redis_client.delete("dlq:poison:messages")
        await redis_client.aclose()


@pytest.mark.asyncio
async def test_dlq_handler_retry_success_records_recovery() -> None:
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    redis_client = aioredis.from_url(redis_url)

    dlq_topic = f"dlq-test-{uuid.uuid4().hex[:6]}"
    handler = DLQHandlerFixed(
        dlq_topic=dlq_topic,
        kafka_config={"bootstrap.servers": _bootstrap_servers()},
        redis_client=redis_client,
    )

    async def processor(payload):
        assert payload["ok"] is True
        return {"status": "ok"}

    handler.message_processors["topic-1"] = processor

    failed_msg = FailedMessage(
        message_id="msg-2",
        topic="topic-1",
        partition=0,
        offset=1,
        key=None,
        value=json.dumps({"ok": True}),
        error="boom",
        retry_count=0,
        first_failure_time=datetime.now(timezone.utc),
    )

    try:
        await handler._retry_message(failed_msg)
        recovered = await redis_client.lrange("dlq:recovered:messages", 0, 0)
        assert recovered
        assert handler.metrics["messages_recovered"] == 1
    finally:
        await redis_client.delete("dlq:recovered:messages")
        await redis_client.aclose()
