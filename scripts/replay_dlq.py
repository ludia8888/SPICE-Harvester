#!/usr/bin/env python3
"""
Replay DLQ messages back to their original topics.

Supports common DLQ payload shapes:
- {"original_topic": "...", "original_value": "...", "original_key": "..."}
- {"job": {...}} (objectify DLQ -> publishes job to OBJECTIFY_JOBS_TOPIC)
- EventEnvelope JSON (re-publishes raw payload to metadata.kafka_topic or INSTANCE_EVENTS_TOPIC)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Optional, Tuple

from confluent_kafka import Consumer, Producer, KafkaError


def _default_bootstrap() -> str:
    if os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
        return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    host = (os.getenv("KAFKA_HOST") or "127.0.0.1").strip()
    port = (
        os.getenv("KAFKA_PORT_HOST")
        or os.getenv("KAFKA_PORT")
        or "9092"
    )
    return f"{host}:{port}"


def _encode_payload(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    return json.dumps(value, ensure_ascii=False, default=str).encode("utf-8")


def _resolve_target(
    payload: dict[str, Any],
    raw_value: bytes,
    explicit_target: Optional[str],
) -> Tuple[Optional[str], Optional[bytes], Optional[bytes], str]:
    if "original_topic" in payload and "original_value" in payload:
        target = explicit_target or str(payload.get("original_topic") or "").strip()
        key_raw = payload.get("original_key")
        return target or None, _encode_payload(payload.get("original_value")), _encode_payload(key_raw), "original_value"

    if isinstance(payload.get("job"), dict):
        target = explicit_target or os.getenv("OBJECTIFY_JOBS_TOPIC") or "objectify-jobs"
        job = payload.get("job") or {}
        key = str(job.get("job_id") or "objectify-job")
        return target, _encode_payload(job), key.encode("utf-8"), "objectify_job"

    if payload.get("event_type") and payload.get("aggregate_id"):
        metadata = payload.get("metadata") or {}
        target = explicit_target or metadata.get("kafka_topic") or os.getenv("INSTANCE_EVENTS_TOPIC")
        key = str(payload.get("aggregate_id") or payload.get("event_id") or "")
        return target, raw_value, key.encode("utf-8") if key else None, "event_envelope"

    return None, None, None, "unknown"


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay DLQ messages back to Kafka topics.")
    parser.add_argument("--dlq-topic", required=True, help="DLQ topic name")
    parser.add_argument("--target-topic", help="Override target topic")
    parser.add_argument("--bootstrap", default=_default_bootstrap(), help="Kafka bootstrap servers")
    parser.add_argument("--group-id", default="dlq-replay", help="Consumer group id")
    parser.add_argument("--max-messages", type=int, default=100, help="Max messages to replay")
    parser.add_argument("--dry-run", action="store_true", help="Do not publish, just print")
    parser.add_argument(
        "--skip-unknown",
        action="store_true",
        help="Skip/commit unknown payloads instead of stopping",
    )
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer({"bootstrap.servers": args.bootstrap})

    processed = 0
    replayed = 0
    skipped = 0
    try:
        consumer.subscribe([args.dlq_topic])
        while processed < args.max_messages:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Kafka error: {msg.error()}", file=sys.stderr)
                continue

            processed += 1
            raw_value = msg.value() or b""
            try:
                payload = json.loads(raw_value.decode("utf-8"))
            except Exception:
                payload = {}

            target, value, key, kind = _resolve_target(payload, raw_value, args.target_topic)
            if not target or value is None:
                skipped += 1
                print(f"Skipping message (kind={kind}) at {msg.topic()}:{msg.partition()}:{msg.offset()}")
                if args.skip_unknown:
                    consumer.commit(message=msg, asynchronous=False)
                    continue
                break

            if args.dry_run:
                print(f"DRY RUN -> {target} (kind={kind}) bytes={len(value)}")
                consumer.commit(message=msg, asynchronous=False)
                replayed += 1
                continue

            producer.produce(target, key=key, value=value)
            producer.flush(10)
            consumer.commit(message=msg, asynchronous=False)
            replayed += 1
            print(f"Replayed to {target} (kind={kind})")
    finally:
        try:
            consumer.close()
        except Exception:
            pass

    print(f"Processed={processed} Replayed={replayed} Skipped={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
