"""
Message Relay Service
Outbox 테이블에서 이벤트를 읽어 Kafka로 전송하는 서비스
"""

import asyncio
import json
import logging
import os
import signal
from typing import Optional

import asyncpg
from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

from shared.config.service_config import ServiceConfig

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class MessageRelay:
    """Outbox Pattern Message Relay Service"""
    
    def __init__(self):
        self.running = False
        self.postgres_url = ServiceConfig.get_postgres_url()
        self.kafka_servers = ServiceConfig.get_kafka_bootstrap_servers()
        self.producer: Optional[Producer] = None
        self.conn: Optional[asyncpg.Connection] = None
        
    async def initialize(self):
        """서비스 초기화"""
        # Kafka Producer 설정
        self.producer = Producer({
            'bootstrap.servers': self.kafka_servers,
            'client.id': 'message-relay',
            'acks': 'all',  # 모든 replica가 메시지를 받을 때까지 대기
            'retries': 3,
            'max.in.flight.requests.per.connection': 1,  # 순서 보장
            'compression.type': 'snappy',
        })
        
        # Kafka 토픽 생성 확인
        await self.ensure_kafka_topics()
        
        # PostgreSQL 연결
        self.conn = await asyncpg.connect(self.postgres_url)
        logger.info("Message Relay initialized successfully")
        
    async def ensure_kafka_topics(self):
        """필요한 Kafka 토픽이 존재하는지 확인하고 없으면 생성"""
        admin_client = AdminClient({'bootstrap.servers': self.kafka_servers})
        
        # 생성할 토픽 리스트
        topics = [
            NewTopic(
                topic="ontology_events",
                num_partitions=3,
                replication_factor=1,
                config={
                    'retention.ms': '604800000',  # 7일 보관
                    'compression.type': 'snappy'
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
            
    async def process_events(self):
        """Outbox 테이블에서 이벤트를 읽어 Kafka로 전송"""
        batch_size = int(os.getenv("MESSAGE_RELAY_BATCH_SIZE", "100"))
        
        async with self.conn.transaction():
            # 처리되지 않은 이벤트를 조회하고 잠금
            events = await self.conn.fetch(
                """
                SELECT id, topic, payload, retry_count
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
            
            if not events:
                return 0
                
            processed_count = 0
            
            for event in events:
                try:
                    # Kafka로 메시지 발행
                    self.producer.produce(
                        topic=event['topic'],
                        value=event['payload'],
                        key=str(event['id']).encode('utf-8'),
                        callback=self.delivery_report
                    )
                    
                    # 발행 성공 시 처리 완료 표시
                    await self.conn.execute(
                        """
                        UPDATE spice_outbox.outbox 
                        SET processed_at = NOW()
                        WHERE id = $1
                        """,
                        event['id']
                    )
                    
                    processed_count += 1
                    logger.info(f"Relayed event {event['id']} to topic {event['topic']}")
                    
                except Exception as e:
                    # 에러 발생 시 재시도 카운트 증가
                    logger.error(f"Failed to relay event {event['id']}: {e}")
                    
                    await self.conn.execute(
                        """
                        UPDATE spice_outbox.outbox 
                        SET retry_count = COALESCE(retry_count, 0) + 1,
                            last_retry_at = NOW()
                        WHERE id = $1
                        """,
                        event['id']
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