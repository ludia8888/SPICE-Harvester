"""
Event Publisher Service
S3/MinIO Event Store(SSoT)에 저장된 이벤트를 Kafka로 발행하는 서비스.

Legacy DB 기반 delivery-buffer를 제거하고, Event Store 자체를 "발행 원천"으로 사용합니다.

Flow:
1) Producers append EventEnvelope JSON to S3/MinIO (events/...)
2) EventPublisher tails indexes/by-date/... and publishes to Kafka
3) Checkpoint is stored back to S3 (checkpoints/...)
"""

import asyncio
import json
import logging
import os
import signal
import time
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone, timedelta
from collections import OrderedDict

import aioboto3
from botocore.exceptions import ClientError

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from shared.config.service_config import ServiceConfig
from shared.config.app_config import AppConfig
from shared.models.event_envelope import EventEnvelope

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class _RecentPublishedEventIds:
    """Best-effort in-memory dedup window (publisher is still at-least-once)."""

    def __init__(self, max_events: int):
        self._max_events = max(0, int(max_events))
        self._lru: "OrderedDict[str, None]" = OrderedDict()

    def mark_published(self, event_id: str) -> None:
        if self._max_events <= 0 or not event_id:
            return
        if event_id in self._lru:
            self._lru.move_to_end(event_id)
            return
        self._lru[event_id] = None
        if len(self._lru) > self._max_events:
            self._lru.popitem(last=False)

    def was_published(self, event_id: Optional[str]) -> bool:
        if self._max_events <= 0 or not event_id:
            return False
        if event_id in self._lru:
            self._lru.move_to_end(event_id)
            return True
        return False

    def load(self, event_ids: List[str]) -> None:
        if self._max_events <= 0:
            return
        if not isinstance(event_ids, list):
            return
        # Oldest -> newest expected; preserve relative ordering.
        for event_id in event_ids:
            if not isinstance(event_id, str):
                continue
            self.mark_published(event_id)

    def snapshot(self, max_events: int) -> List[str]:
        max_events = max(0, int(max_events))
        if max_events <= 0:
            return []
        ids = list(self._lru.keys())
        return ids[-max_events:]


class EventPublisher:
    """S3/MinIO Event Store -> Kafka publisher."""
    
    def __init__(self):
        self.running = False
        self.kafka_servers = ServiceConfig.get_kafka_bootstrap_servers()
        self.producer: Optional[Producer] = None
        self.bucket_name = os.getenv("EVENT_STORE_BUCKET", "spice-event-store")

        self.endpoint_url = ServiceConfig.get_minio_endpoint()
        self.access_key = ServiceConfig.get_minio_access_key()
        self.secret_key = ServiceConfig.get_minio_secret_key()
        self.session = aioboto3.Session()

        self.checkpoint_key = os.getenv(
            "EVENT_PUBLISHER_CHECKPOINT_KEY", "checkpoints/event_publisher.json"
        )
        self.poll_interval = int(
            os.getenv("EVENT_PUBLISHER_POLL_INTERVAL")
            or os.getenv("MESSAGE_RELAY_POLL_INTERVAL")
            or "3"
        )
        self.batch_size = int(
            os.getenv("EVENT_PUBLISHER_BATCH_SIZE")
            or os.getenv("MESSAGE_RELAY_BATCH_SIZE")
            or "200"
        )
        self.kafka_flush_batch_size = int(
            os.getenv("EVENT_PUBLISHER_KAFKA_FLUSH_BATCH_SIZE") or str(self.batch_size)
        )
        self.kafka_flush_timeout_s = float(os.getenv("EVENT_PUBLISHER_KAFKA_FLUSH_TIMEOUT_SECONDS", "10"))
        self.metrics_log_interval_s = int(os.getenv("EVENT_PUBLISHER_METRICS_LOG_INTERVAL_SECONDS", "30"))
        self.lookback_seconds = int(os.getenv("EVENT_PUBLISHER_LOOKBACK_SECONDS", "600"))
        self.lookback_max_keys = int(os.getenv("EVENT_PUBLISHER_LOOKBACK_MAX_KEYS", "50"))
        self.dedup_max_events = int(os.getenv("EVENT_PUBLISHER_DEDUP_MAX_EVENTS", "10000"))
        self.dedup_checkpoint_max_events = int(os.getenv("EVENT_PUBLISHER_DEDUP_CHECKPOINT_MAX_EVENTS", "2000"))
        self._recent_published_event_ids = _RecentPublishedEventIds(self.dedup_max_events)
        self._recent_loaded_from_checkpoint = False
        self.start_timestamp = os.getenv("EVENT_PUBLISHER_START_TIMESTAMP")  # ISO8601 optional

        self._metrics_total: Dict[str, int] = {
            "index_keys_considered": 0,
            "events_published": 0,
            "events_skipped_dedup": 0,
            "checkpoint_saves": 0,
            "kafka_flush_count": 0,
            "kafka_flush_ms_total": 0,
            "kafka_delivery_failures": 0,
            "kafka_produce_buffer_full": 0,
        }
        self._last_metrics_log_at = time.monotonic()
        
    async def initialize(self):
        """서비스 초기화"""
        # Kafka Producer 설정
        self.producer = Producer({
            'bootstrap.servers': self.kafka_servers,
            'client.id': 'event-publisher',
            # Producer-level idempotence (handles retry-induced duplicates within a session)
            'enable.idempotence': True,
            'acks': 'all',  # 모든 replica가 메시지를 받을 때까지 대기
            'retries': 1000000,  # allow long retry window; dedup is handled downstream too
            # Idempotent producer preserves ordering with max.in.flight <= 5.
            'max.in.flight.requests.per.connection': 5,
        })
        
        # Kafka 토픽 생성 확인
        await self.ensure_kafka_topics()

        # Ensure bucket exists
        async with self.session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            use_ssl=False,
        ) as s3:
            try:
                await s3.head_bucket(Bucket=self.bucket_name)
            except ClientError:
                await s3.create_bucket(Bucket=self.bucket_name)
                await s3.put_bucket_versioning(
                    Bucket=self.bucket_name,
                    VersioningConfiguration={"Status": "Enabled"},
                )

        logger.info(
            f"EventPublisher initialized (bucket={self.bucket_name}, endpoint={self.endpoint_url})"
        )
        
    async def ensure_kafka_topics(self):
        """필요한 Kafka 토픽이 존재하는지 확인하고 없으면 생성"""
        admin_client = AdminClient({'bootstrap.servers': self.kafka_servers})
        
        topics: List[NewTopic] = []
        for name in AppConfig.get_all_topics():
            topics.append(
                NewTopic(
                    topic=name,
                    num_partitions=3,
                    replication_factor=1,
                    config={"retention.ms": "604800000"},  # 7일 보관
                )
            )
        
        # 기존 토픽 확인
        existing_topics = admin_client.list_topics(timeout=10)
        existing_topic_names = set(existing_topics.topics.keys())
        
        # 새로운 토픽만 생성
        new_topics = [t for t in topics if t.topic not in existing_topic_names]
        
        if new_topics:
            fs = admin_client.create_topics(new_topics)
            for topic, f in fs.items():
                try:
                    f.result()  # 토픽 생성 완료 대기
                    logger.info(f"Created Kafka topic: {topic}")
                except Exception as e:
                    logger.error(f"Failed to create topic {topic}: {e}")
        else:
            logger.info("All required Kafka topics already exist")

    async def _load_checkpoint(self) -> Dict[str, Any]:
        async with self.session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            use_ssl=False,
        ) as s3:
            try:
                obj = await s3.get_object(Bucket=self.bucket_name, Key=self.checkpoint_key)
                raw = await obj["Body"].read()
                return json.loads(raw.decode("utf-8"))
            except ClientError:
                return {}

    async def _save_checkpoint(self, checkpoint: Dict[str, Any]) -> None:
        checkpoint["updated_at"] = datetime.now(timezone.utc).isoformat()
        checkpoint["recent_event_ids"] = self._recent_published_event_ids.snapshot(self.dedup_checkpoint_max_events)
        async with self.session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            use_ssl=False,
        ) as s3:
            await s3.put_object(
                Bucket=self.bucket_name,
                Key=self.checkpoint_key,
                Body=json.dumps(checkpoint).encode("utf-8"),
                ContentType="application/json",
            )

    def _log_metrics_if_due(self) -> None:
        interval = int(self.metrics_log_interval_s or 0)
        if interval <= 0:
            return
        now = time.monotonic()
        if now - float(self._last_metrics_log_at or 0.0) < interval:
            return
        self._last_metrics_log_at = now

        m = self._metrics_total
        flush_count = int(m.get("kafka_flush_count") or 0)
        flush_ms_total = int(m.get("kafka_flush_ms_total") or 0)
        avg_flush_ms = (flush_ms_total / flush_count) if flush_count else 0.0
        logger.info(
            "Publisher metrics: "
            f"published={m.get('events_published', 0)} "
            f"dedup_skipped={m.get('events_skipped_dedup', 0)} "
            f"checkpoint_saves={m.get('checkpoint_saves', 0)} "
            f"flush_count={flush_count} avg_flush_ms={avg_flush_ms:.1f} "
            f"delivery_failures={m.get('kafka_delivery_failures', 0)} "
            f"buffer_full={m.get('kafka_produce_buffer_full', 0)}"
        )

    def _flush_producer(self, *, timeout_s: float) -> int:
        if not self.producer:
            raise RuntimeError("Kafka producer not initialized")
        start = time.monotonic()
        remaining = self.producer.flush(timeout=timeout_s)
        duration_ms = int((time.monotonic() - start) * 1000)
        self._metrics_total["kafka_flush_count"] += 1
        self._metrics_total["kafka_flush_ms_total"] += duration_ms
        return int(remaining or 0)

    def _initial_checkpoint(self) -> Dict[str, Any]:
        if self.start_timestamp:
            try:
                dt = datetime.fromisoformat(self.start_timestamp.replace("Z", "+00:00"))
                dt = dt.astimezone(timezone.utc)
                return {"last_timestamp_ms": int(dt.timestamp() * 1000), "last_index_key": None}
            except Exception as e:
                logger.warning(f"Invalid EVENT_PUBLISHER_START_TIMESTAMP, ignoring: {e}")

        # Default: start "now - 5 minutes" to avoid flooding on first start.
        dt = datetime.now(timezone.utc) - timedelta(minutes=5)
        return {"last_timestamp_ms": int(dt.timestamp() * 1000), "last_index_key": None}

    @staticmethod
    def _advance_checkpoint(checkpoint: Dict[str, Any], *, ts_ms: Optional[int], idx_key: str) -> bool:
        """Advance the durable checkpoint monotonically (never move backwards)."""
        if not isinstance(checkpoint, dict):
            return False
        if not idx_key:
            return False
        if ts_ms is None:
            return False

        previous_ts = int(checkpoint.get("last_timestamp_ms") or 0)
        previous_key = checkpoint.get("last_index_key")

        if ts_ms > previous_ts:
            checkpoint["last_timestamp_ms"] = int(ts_ms)
            checkpoint["last_index_key"] = idx_key
            return True

        if ts_ms == previous_ts:
            if not previous_key or str(idx_key) > str(previous_key):
                checkpoint["last_index_key"] = idx_key
                return True

        return False

    async def _list_next_index_keys(self, checkpoint: Dict[str, Any]) -> List[str]:
        last_ts_ms = int(checkpoint.get("last_timestamp_ms") or 0)
        last_key = checkpoint.get("last_index_key")

        # Lookback scan: reduces risk of missing late-arriving index entries.
        # Publisher is still at-least-once; downstream idempotency is the contract.
        lookback_ms = max(0, int(self.lookback_seconds)) * 1000
        effective_start_ts_ms = max(0, last_ts_ms - lookback_ms) if last_ts_ms > 0 else 0
        start_dt = datetime.fromtimestamp(effective_start_ts_ms / 1000, tz=timezone.utc)
        today = datetime.now(timezone.utc)

        forward: List[str] = []
        lookback: List[str] = []
        lookback_limit = max(0, int(self.lookback_max_keys))
        # Reserve some budget for lookback keys so late-arrivals don't starve forever under high throughput.
        lookback_limit = min(lookback_limit, self.batch_size)

        async with self.session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            use_ssl=False,
        ) as s3:
            paginator = s3.get_paginator("list_objects_v2")

            day = datetime(start_dt.year, start_dt.month, start_dt.day, tzinfo=timezone.utc)
            last_day = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)

            while day <= last_day:
                prefix = f"indexes/by-date/{day.year:04d}/{day.month:02d}/{day.day:02d}/"
                day_keys: List[str] = []
                async for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        day_keys.append(obj["Key"])

                if day_keys:
                    day_keys.sort()
                    for k in day_keys:
                        filename = k.rsplit("/", 1)[-1]
                        ts_str = filename.split("_", 1)[0]
                        if not ts_str.isdigit():
                            continue
                        ts_ms = int(ts_str)
                        if ts_ms < effective_start_ts_ms:
                            continue
                        is_forward = False
                        if ts_ms > last_ts_ms:
                            is_forward = True
                        elif ts_ms == last_ts_ms:
                            if not last_key or k > last_key:
                                is_forward = True

                        if is_forward:
                            if len(forward) < self.batch_size:
                                forward.append(k)
                        else:
                            if lookback_limit > 0:
                                lookback.append(k)
                                # Keep only the most recent lookback candidates (keys are time-sortable).
                                if len(lookback) > lookback_limit * 5:
                                    lookback.sort()
                                    lookback = lookback[-(lookback_limit * 2) :]

                day += timedelta(days=1)

                # Best-effort early stop: if we already have enough forward keys and a healthy lookback sample.
                if len(forward) >= self.batch_size and (lookback_limit == 0 or len(lookback) >= lookback_limit):
                    break

        forward.sort()
        lookback.sort(reverse=True)

        selected_lookback = lookback[:lookback_limit]
        remaining = self.batch_size - len(selected_lookback)
        selected_forward = forward[: max(0, remaining)]
        return selected_forward + selected_lookback

    async def process_events(self) -> int:
        """Tail S3 by-date index and publish to Kafka."""
        checkpoint = await self._load_checkpoint()
        if not checkpoint:
            checkpoint = self._initial_checkpoint()
        if not checkpoint.get("last_timestamp_ms"):
            checkpoint.update(self._initial_checkpoint())

        if not self._recent_loaded_from_checkpoint:
            recent = checkpoint.get("recent_event_ids")
            if isinstance(recent, list):
                try:
                    self._recent_published_event_ids.load(recent)
                except Exception:
                    pass
            self._recent_loaded_from_checkpoint = True

        index_keys = await self._list_next_index_keys(checkpoint)
        if not index_keys:
            self._log_metrics_if_due()
            return 0

        self._metrics_total["index_keys_considered"] += len(index_keys)

        published = 0
        checkpoint_dirty = False
        flush_batch_size = max(1, int(self.kafka_flush_batch_size or self.batch_size or 1))
        flush_timeout_s = max(0.1, float(self.kafka_flush_timeout_s or 10.0))

        batch: List[Dict[str, Any]] = []
        delivery_results: Dict[str, Optional[str]] = {}

        def delivery_cb_factory(idx_key: str):
            def on_delivery(err, _msg):
                delivery_results[idx_key] = str(err) if err is not None else None

            return on_delivery

        async def flush_batch(*, reason: str) -> None:
            nonlocal published, checkpoint_dirty
            if not batch:
                return
            if not self.producer:
                raise RuntimeError("Kafka producer not initialized")

            # Service callbacks before/after flush.
            self.producer.poll(0)
            remaining = self._flush_producer(timeout_s=flush_timeout_s)
            self.producer.poll(0)

            prefix_len = 0
            first_failure: Optional[str] = None

            for item in batch:
                idx_key = item["idx_key"]
                status = delivery_results.get(idx_key, "__missing__")
                if status is None:
                    prefix_len += 1
                    continue
                if status == "__missing__":
                    first_failure = f"delivery_status_missing idx={idx_key}"
                else:
                    first_failure = f"delivery_error idx={idx_key} err={status}"
                break

            # Advance checkpoint for confirmed-delivered prefix only.
            for item in batch[:prefix_len]:
                self._recent_published_event_ids.mark_published(item.get("event_id") or "")
                if self._advance_checkpoint(checkpoint, ts_ms=item.get("ts_ms"), idx_key=item["idx_key"]):
                    checkpoint_dirty = True
                published += 1
                self._metrics_total["events_published"] += 1

            if checkpoint_dirty:
                await self._save_checkpoint(checkpoint)
                checkpoint_dirty = False
                self._metrics_total["checkpoint_saves"] += 1

            if prefix_len:
                logger.info(f"Published batch size={prefix_len} reason={reason}")

            # Clear batch state before raising (publisher is at-least-once; duplicates are safe).
            batch.clear()
            delivery_results.clear()

            if remaining or first_failure:
                self._metrics_total["kafka_delivery_failures"] += 1
                details = first_failure or f"kafka_flush_remaining={remaining}"
                raise RuntimeError(f"Kafka batch delivery failed ({reason}): {details}")

        async with self.session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            use_ssl=False,
        ) as s3:
            try:
                for idx_key in index_keys:
                    filename = idx_key.rsplit("/", 1)[-1]
                    ts_str = filename.split("_", 1)[0]
                    ts_ms = int(ts_str) if ts_str.isdigit() else None

                    idx_obj = await s3.get_object(Bucket=self.bucket_name, Key=idx_key)
                    idx_data = json.loads((await idx_obj["Body"].read()).decode("utf-8"))
                    s3_key = idx_data.get("s3_key")
                    if not s3_key:
                        raise RuntimeError("index entry missing s3_key")

                    event_id = idx_data.get("event_id")
                    if not event_id:
                        try:
                            event_id = s3_key.rsplit("/", 1)[-1].split(".", 1)[0]
                        except Exception:
                            event_id = None

                    if self._recent_published_event_ids.was_published(event_id):
                        self._metrics_total["events_skipped_dedup"] += 1
                        # Duplicate index entry (or replay within same process): skip publish,
                        # but keep advancing the durable checkpoint if this key is ahead.
                        if self._advance_checkpoint(checkpoint, ts_ms=ts_ms, idx_key=idx_key):
                            checkpoint_dirty = True
                        logger.debug(f"Skipped duplicate publish (event_id={event_id}, idx={idx_key})")
                        continue

                    ev_obj = await s3.get_object(Bucket=self.bucket_name, Key=s3_key)
                    ev_bytes = await ev_obj["Body"].read()

                    # Determine topic routing (preferred: index entry / metadata.kafka_topic)
                    topic = idx_data.get("kafka_topic")
                    env: Optional[EventEnvelope] = None
                    raw_payload: Optional[Dict[str, Any]] = None
                    if not topic:
                        try:
                            env = EventEnvelope.model_validate_json(ev_bytes)
                            topic = env.metadata.get("kafka_topic") if isinstance(env.metadata, dict) else None
                        except Exception:
                            env = None
                            topic = None
                    if not topic:
                        try:
                            raw_payload = json.loads(ev_bytes.decode("utf-8"))
                        except Exception:
                            raw_payload = None

                    aggregate_type = ""
                    if env:
                        aggregate_type = str(env.aggregate_type or "")
                    elif raw_payload:
                        data_payload = raw_payload.get("data") if isinstance(raw_payload.get("data"), dict) else None
                        aggregate_type = str(
                            raw_payload.get("aggregate_type")
                            or raw_payload.get("aggregateType")
                            or (data_payload.get("aggregate_type") if data_payload else "")
                            or (data_payload.get("aggregateType") if data_payload else "")
                        )
                        metadata_payload = raw_payload.get("metadata") if isinstance(raw_payload.get("metadata"), dict) else None
                        if data_payload and not metadata_payload:
                            metadata_payload = data_payload.get("metadata") if isinstance(data_payload.get("metadata"), dict) else None
                        if metadata_payload and not topic:
                            topic = metadata_payload.get("kafka_topic")

                    if not topic and aggregate_type:
                        normalized_type = aggregate_type.lower()
                        if normalized_type in {"dataset", "pipeline"}:
                            topic = AppConfig.PIPELINE_EVENTS_TOPIC
                        elif normalized_type == "instance":
                            topic = AppConfig.INSTANCE_EVENTS_TOPIC
                        elif normalized_type == "ontology":
                            topic = AppConfig.ONTOLOGY_EVENTS_TOPIC
                        elif normalized_type.startswith("connector"):
                            topic = AppConfig.CONNECTOR_UPDATES_TOPIC

                    if not topic:
                        logger.warning("Skipping event without kafka_topic: %s", s3_key)
                        if self._advance_checkpoint(checkpoint, ts_ms=ts_ms, idx_key=idx_key):
                            checkpoint_dirty = True
                        continue

                    key_bytes = (idx_data.get("aggregate_id") or idx_data.get("event_id") or "").encode("utf-8")

                    try:
                        self.producer.produce(
                            topic=topic,
                            value=ev_bytes,
                            key=key_bytes,
                            callback=delivery_cb_factory(idx_key),
                        )
                    except BufferError:
                        # Local buffer is full: flush pending messages then retry.
                        self._metrics_total["kafka_produce_buffer_full"] += 1
                        if batch:
                            await flush_batch(reason="buffer_full")
                        else:
                            remaining = self._flush_producer(timeout_s=flush_timeout_s)
                            if remaining:
                                raise RuntimeError(
                                    f"kafka delivery timed out while draining buffer (remaining={remaining})"
                                )
                        self.producer.produce(
                            topic=topic,
                            value=ev_bytes,
                            key=key_bytes,
                            callback=delivery_cb_factory(idx_key),
                        )

                    self.producer.poll(0)
                    batch.append(
                        {
                            "idx_key": idx_key,
                            "ts_ms": ts_ms,
                            "event_id": event_id or idx_data.get("event_id") or "",
                        }
                    )

                    if len(batch) >= flush_batch_size:
                        await flush_batch(reason="batch_full")

                # Flush remaining events at end.
                await flush_batch(reason="end")

            except Exception as e:
                # Best-effort: flush and checkpoint any pending batch prefix before bubbling up.
                try:
                    await flush_batch(reason="error_cleanup")
                except Exception as flush_err:
                    logger.error(f"Failed to flush pending batch after error: {flush_err}")
                logger.error(f"Failed to publish: {e}")
                raise

        # If we only advanced checkpoint via dedup-skips (no batch flush), persist once.
        if checkpoint_dirty:
            await self._save_checkpoint(checkpoint)
            self._metrics_total["checkpoint_saves"] += 1

        self._log_metrics_if_due()
        return published
            
    async def run(self):
        """메인 실행 루프"""
        self.running = True
        logger.info(f"EventPublisher started. Polling interval: {self.poll_interval}s")
        
        while self.running:
            try:
                # 이벤트 처리
                processed = await self.process_events()
                
                if processed > 0:
                    logger.info(f"Processed {processed} events")
                else:
                    # 처리할 이벤트가 없으면 대기
                    await asyncio.sleep(self.poll_interval)
                    
            except Exception as e:
                logger.error(f"Error in relay loop: {e}")
                await asyncio.sleep(self.poll_interval)
                
    async def shutdown(self):
        """서비스 종료"""
        logger.info("Shutting down EventPublisher...")
        self.running = False
        
        if self.producer:
            self.producer.flush()

        logger.info("EventPublisher shut down successfully")


async def main():
    """메인 진입점"""
    relay = EventPublisher()
    
    # 종료 시그널 핸들러
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}")
        asyncio.create_task(relay.shutdown())
        
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await relay.initialize()
        await relay.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        await relay.shutdown()
        raise


if __name__ == "__main__":
    asyncio.run(main())
