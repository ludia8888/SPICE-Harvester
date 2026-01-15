from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pytest

from shared.services.dlq_handler_fixed import DLQHandlerFixed, FailedMessage, RetryPolicy


def _bootstrap_servers() -> str:
    return "localhost:9092"


class _FakeProducer:
    def __init__(self) -> None:
        self.messages: list[Dict[str, Any]] = []

    def produce(self, *, topic: str, key: Optional[bytes] = None, value: str) -> None:
        self.messages.append({"topic": topic, "key": key, "value": value})

    def flush(self, *_args, **_kwargs) -> int:
        return 0


class _FakeRedis:
    def __init__(self) -> None:
        self._hashes: dict[str, dict[str, str]] = {}
        self._lists: dict[str, list[str]] = {}

    async def hset(self, key: str, *, mapping: dict[str, Any]) -> int:
        payload: dict[str, str] = {}
        for k, v in (mapping or {}).items():
            payload[str(k)] = "" if v is None else str(v)
        self._hashes[str(key)] = payload
        return len(payload)

    async def expire(self, _key: str, _ttl: int) -> bool:
        return True

    async def hgetall(self, key: str) -> dict[bytes, bytes]:
        data = self._hashes.get(str(key), {})
        return {k.encode(): v.encode() for k, v in data.items()}

    async def lpush(self, key: str, value: str) -> int:
        items = self._lists.setdefault(str(key), [])
        items.insert(0, str(value))
        return len(items)

    async def ltrim(self, key: str, start: int, end: int) -> bool:
        items = self._lists.get(str(key), [])
        if end < 0:
            trimmed = items[start:]
        else:
            trimmed = items[start : end + 1]
        self._lists[str(key)] = trimmed
        return True

    async def lrange(self, key: str, start: int, end: int) -> list[bytes]:
        items = self._lists.get(str(key), [])
        if end < 0:
            sliced = items[start:]
        else:
            sliced = items[start : end + 1]
        return [item.encode() for item in sliced]

    async def delete(self, *keys: str) -> int:
        removed = 0
        for key in keys:
            if str(key) in self._hashes:
                del self._hashes[str(key)]
                removed += 1
            if str(key) in self._lists:
                del self._lists[str(key)]
                removed += 1
        return removed

    async def aclose(self) -> None:
        return None


@pytest.mark.asyncio
async def test_dlq_handler_retry_and_poison_flow() -> None:
    redis_client = _FakeRedis()

    dlq_topic = f"dlq-test-{uuid.uuid4().hex[:6]}"
    poison_topic = f"{dlq_topic}.poison"

    handler = DLQHandlerFixed(
        dlq_topic=dlq_topic,
        kafka_config={"bootstrap.servers": _bootstrap_servers()},
        redis_client=redis_client,
        retry_policy=RetryPolicy(max_retries=1),
        poison_topic=poison_topic,
    )
    handler.producer = _FakeProducer()

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
        assert handler.producer.messages
        assert handler.producer.messages[-1]["topic"] == poison_topic
        poison_list = await redis_client.lrange("dlq:poison:messages", 0, 0)
        assert poison_list
    finally:
        await redis_client.delete(f"dlq:retry:{failed_msg.message_id}")
        await redis_client.delete("dlq:poison:messages")
        await redis_client.aclose()


@pytest.mark.asyncio
async def test_dlq_handler_retry_success_records_recovery() -> None:
    redis_client = _FakeRedis()

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
