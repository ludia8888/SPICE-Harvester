"""
Message Relay Service
Outbox 테이블에서 이벤트를 읽어 Kafka로 전송하는 서비스

🔥 MIGRATION: Enhanced to include S3/MinIO Event Store references
- Kafka messages now include S3 event references
- Consumers can gradually migrate to reading from S3
- PostgreSQL payload still included for backward compatibility
"""

import asyncio
import json
import logging
import os
import signal
from typing import Optional, Dict, Any
from datetime import datetime, timezone

import asyncpg
from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

from shared.config.service_config import ServiceConfig
from shared.config.app_config import AppConfig

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class MessageRelay:
    """Outbox Pattern Message Relay Service
    
    🔥 MIGRATION: Enhanced with S3/MinIO Event Store integration
    """
    
    def __init__(self):
        self.running = False
        self.postgres_url = ServiceConfig.get_postgres_url()
        self.kafka_servers = ServiceConfig.get_kafka_bootstrap_servers()
        self.producer: Optional[Producer] = None
        self.conn: Optional[asyncpg.Connection] = None
        
        # 🔥 S3/MinIO Event Store configuration
        self.s3_enabled = os.getenv("ENABLE_S3_EVENT_STORE", "false").lower() == "true"
        self.s3_bucket = "spice-event-store"
        self.minio_endpoint = ServiceConfig.get_minio_endpoint() if self.s3_enabled else None
        
    async def initialize(self):
        """서비스 초기화"""
        # Kafka Producer 설정
        self.producer = Producer({
            'bootstrap.servers': self.kafka_servers,
            'client.id': 'message-relay',
            'acks': 'all',  # 모든 replica가 메시지를 받을 때까지 대기
            'retries': 3,
            'max.in.flight.requests.per.connection': 1,  # 순서 보장
            # REMOVED: 'compression.type': 'snappy',  # Python consumers can't read Snappy!
        })
        
        # Kafka 토픽 생성 확인
        await self.ensure_kafka_topics()
        
        # PostgreSQL 연결
        self.conn = await asyncpg.connect(self.postgres_url)
        
        # 🔥 Log S3/MinIO Event Store configuration
        if self.s3_enabled:
            logger.info(f"🔥 S3/MinIO Event Store ENABLED - endpoint: {self.minio_endpoint}, bucket: {self.s3_bucket}")
            logger.info("🔥 Messages will include S3 references for gradual migration")
        else:
            logger.info("⚠️ S3/MinIO Event Store DISABLED - using legacy mode")
            
        logger.info("Message Relay initialized successfully")
        
    async def ensure_kafka_topics(self):
        """필요한 Kafka 토픽이 존재하는지 확인하고 없으면 생성"""
        admin_client = AdminClient({'bootstrap.servers': self.kafka_servers})
        
        # 생성할 토픽 리스트
        topics = [
            NewTopic(
                topic=AppConfig.ONTOLOGY_EVENTS_TOPIC,
                num_partitions=3,
                replication_factor=1,
                config={
                    'retention.ms': '604800000',  # 7일 보관
                    # REMOVED: 'compression.type': 'snappy'  # Python consumers can't read Snappy!
                }
            )
        ]
        
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
            
    def delivery_report(self, err, msg):
        """Kafka 메시지 전송 결과 콜백"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
    def _build_s3_reference(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """🔥 Build S3/MinIO Event Store reference for a message
        
        Returns a reference to where the event is stored in S3/MinIO
        """
        try:
            # Parse the payload to extract event details
            payload = json.loads(message['payload']) if isinstance(message['payload'], str) else message['payload']
            
            # Extract event metadata
            event_id = message['id']
            aggregate_type = payload.get('aggregate_type', 'unknown')
            aggregate_id = payload.get('aggregate_id', 'unknown')
            
            # Get timestamp from payload or use current time
            timestamp_str = payload.get('timestamp') or payload.get('created_at')
            if timestamp_str:
                # Parse timestamp (handle various formats)
                if isinstance(timestamp_str, str):
                    # Simple parsing - you might need more robust parsing
                    dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                else:
                    dt = datetime.now(timezone.utc)
            else:
                dt = datetime.now(timezone.utc)
            
            # Build S3 key path (matching event_store.py structure)
            s3_key = (
                f"events/{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/"
                f"{aggregate_type}/{aggregate_id}/{event_id}.json"
            )
            
            return {
                "bucket": self.s3_bucket,
                "key": s3_key,
                "endpoint": self.minio_endpoint,
                "event_id": str(event_id)
            }
        except Exception as e:
            logger.warning(f"Failed to build S3 reference: {e}")
            return None
            
    async def process_events(self):
        """Outbox 테이블에서 메시지(Command/Event)를 읽어 Kafka로 전송"""
        batch_size = int(os.getenv("MESSAGE_RELAY_BATCH_SIZE", "100"))
        
        async with self.conn.transaction():
            # 처리되지 않은 메시지를 조회하고 잠금
            messages = await self.conn.fetch(
                """
                SELECT id, message_type, topic, payload, retry_count
                FROM spice_outbox.outbox
                WHERE processed_at IS NULL
                  AND (retry_count < 5 OR retry_count IS NULL)
                  AND (last_retry_at IS NULL OR last_retry_at < NOW() - INTERVAL '1 minute' * POWER(2, retry_count))
                ORDER BY created_at
                LIMIT $1
                FOR UPDATE SKIP LOCKED
                """,
                batch_size
            )
            
            if not messages:
                return 0
                
            processed_count = 0
            
            for message in messages:
                try:
                    # 🔥 MIGRATION: Build enriched message with S3 reference
                    kafka_message = {
                        "message_type": message['message_type'],
                        "payload": json.loads(message['payload']) if isinstance(message['payload'], str) else message['payload'],
                        "metadata": {
                            "relay_timestamp": datetime.now(timezone.utc).isoformat(),
                            "retry_count": message['retry_count'] or 0
                        }
                    }
                    
                    # Add S3 reference if enabled
                    if self.s3_enabled:
                        s3_ref = self._build_s3_reference(message)
                        if s3_ref:
                            kafka_message["s3_reference"] = s3_ref
                            kafka_message["metadata"]["storage_mode"] = "dual_write"
                            logger.debug(f"Added S3 reference: {s3_ref['key']}")
                        else:
                            kafka_message["metadata"]["storage_mode"] = "postgres_only"
                    else:
                        kafka_message["metadata"]["storage_mode"] = "legacy"
                    
                    # Serialize to JSON
                    kafka_value = json.dumps(kafka_message)
                    
                    # Kafka로 메시지 발행
                    self.producer.produce(
                        topic=message['topic'],
                        value=kafka_value,
                        key=str(message['id']).encode('utf-8'),
                        callback=self.delivery_report
                    )
                    
                    # 발행 성공 시 처리 완료 표시
                    await self.conn.execute(
                        """
                        UPDATE spice_outbox.outbox 
                        SET processed_at = NOW()
                        WHERE id = $1
                        """,
                        message['id']
                    )
                    
                    processed_count += 1
                    storage_mode = kafka_message["metadata"].get("storage_mode", "unknown")
                    logger.info(f"🔥 Relayed {message['message_type']} {message['id']} to topic {message['topic']} (mode: {storage_mode})")
                    
                except Exception as e:
                    # 에러 발생 시 재시도 카운트 증가
                    logger.error(f"Failed to relay message {message['id']}: {e}")
                    
                    await self.conn.execute(
                        """
                        UPDATE spice_outbox.outbox 
                        SET retry_count = COALESCE(retry_count, 0) + 1,
                            last_retry_at = NOW()
                        WHERE id = $1
                        """,
                        message['id']
                    )
                    
            # 남은 메시지 전송 완료 대기
            self.producer.flush(timeout=10)
            
            return processed_count
            
    async def run(self):
        """메인 실행 루프"""
        self.running = True
        poll_interval = int(os.getenv("MESSAGE_RELAY_POLL_INTERVAL", "5"))
        
        logger.info(f"Message Relay started. Polling interval: {poll_interval}s")
        
        while self.running:
            try:
                # 이벤트 처리
                processed = await self.process_events()
                
                if processed > 0:
                    logger.info(f"Processed {processed} events")
                else:
                    # 처리할 이벤트가 없으면 대기
                    await asyncio.sleep(poll_interval)
                    
            except Exception as e:
                logger.error(f"Error in relay loop: {e}")
                await asyncio.sleep(poll_interval)
                
    async def shutdown(self):
        """서비스 종료"""
        logger.info("Shutting down Message Relay...")
        self.running = False
        
        if self.producer:
            self.producer.flush()
            
        if self.conn:
            await self.conn.close()
            
        logger.info("Message Relay shut down successfully")


async def main():
    """메인 진입점"""
    relay = MessageRelay()
    
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