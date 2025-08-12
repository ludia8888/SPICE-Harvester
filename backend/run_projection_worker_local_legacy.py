#!/usr/bin/env python3
"""
Projection Worker를 로컬에서 실행
Kafka에서 Event를 받아 Elasticsearch에 Projection 생성
THINK ULTRA - 실제 작동하는 구현
"""

import asyncio
import os
import sys
import json
import signal
import logging
from typing import Optional
from datetime import datetime
import aiohttp

# 환경 변수 설정 (로컬 실행용)
os.environ["KAFKA_HOST"] = "127.0.0.1"
os.environ["KAFKA_PORT"] = "9092"
os.environ["ELASTICSEARCH_HOST"] = "localhost"
os.environ["ELASTICSEARCH_PORT"] = "9201"
os.environ["REDIS_HOST"] = "localhost"
os.environ["REDIS_PORT"] = "6379"
os.environ["LOG_LEVEL"] = "INFO"

# backend 경로 추가
sys.path.insert(0, "/Users/isihyeon/Desktop/SPICE HARVESTER/backend")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

from confluent_kafka import Consumer


class ProjectionWorkerRunner:
    """Projection Worker 실행 관리"""
    
    def __init__(self):
        self.consumer = None
        self.running = False
        self.es_url = "http://localhost:9201"
        
    async def initialize(self):
        """초기화"""
        logger.info("Initializing Projection Worker...")
        
        # Kafka Consumer 생성
        self.consumer = Consumer({
            'bootstrap.servers': '127.0.0.1:9092',
            'group.id': 'projection-worker',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 6000,
            'max.poll.interval.ms': 300000
        })
        
        # instance_events 토픽 구독
        self.consumer.subscribe(['instance_events'])
        logger.info("✅ Subscribed to instance_events topic")
        
    async def process_event(self, event_data: dict) -> bool:
        """Event를 처리하여 Elasticsearch에 저장"""
        try:
            event_type = event_data.get('event_type')
            db_name = event_data.get('db_name')
            class_id = event_data.get('class_id')
            instance_id = event_data.get('instance_id')
            data = event_data.get('data', {})
            
            # Index 이름 생성
            index_name = f"instances_{db_name.replace('-', '_')}"
            
            logger.info(f"Processing {event_type} for {instance_id}")
            
            if event_type == 'INSTANCE_CREATED':
                # Elasticsearch에 문서 생성
                doc = {
                    "@type": class_id,
                    "@id": instance_id,
                    "instance_id": instance_id,
                    "class_id": class_id,
                    **data.get('payload', {}),
                    "created_at": event_data.get('occurred_at'),
                    "s3_path": event_data.get('s3_path'),
                    "s3_checksum": event_data.get('s3_checksum')
                }
                
                # Elasticsearch에 저장
                async with aiohttp.ClientSession() as session:
                    # Index가 없으면 생성
                    async with session.head(f"{self.es_url}/{index_name}") as resp:
                        if resp.status == 404:
                            # Index 생성
                            mapping = {
                                "mappings": {
                                    "properties": {
                                        "@type": {"type": "keyword"},
                                        "@id": {"type": "keyword"},
                                        "instance_id": {"type": "keyword"},
                                        "class_id": {"type": "keyword"},
                                        "created_at": {"type": "date"},
                                        "s3_path": {"type": "keyword"},
                                        "s3_checksum": {"type": "keyword"}
                                    }
                                }
                            }
                            async with session.put(
                                f"{self.es_url}/{index_name}",
                                json=mapping
                            ) as create_resp:
                                if create_resp.status in [200, 201]:
                                    logger.info(f"✅ Created index: {index_name}")
                    
                    # 문서 저장
                    async with session.post(
                        f"{self.es_url}/{index_name}/_doc/{instance_id}",
                        json=doc
                    ) as resp:
                        if resp.status in [200, 201]:
                            result = await resp.json()
                            logger.info(f"✅ Saved to Elasticsearch: {instance_id}")
                            logger.info(f"   Result: {result.get('result')}")
                            return True
                        else:
                            error = await resp.text()
                            logger.error(f"❌ Failed to save: {error}")
                            return False
                            
            elif event_type == 'INSTANCE_UPDATED':
                # 업데이트 처리
                updates = data.get('updates', {})
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{self.es_url}/{index_name}/_update/{instance_id}",
                        json={"doc": updates}
                    ) as resp:
                        if resp.status in [200, 201]:
                            logger.info(f"✅ Updated in Elasticsearch: {instance_id}")
                            return True
                        else:
                            error = await resp.text()
                            logger.error(f"❌ Failed to update: {error}")
                            return False
                            
            elif event_type == 'INSTANCE_DELETED':
                # 삭제 처리 - 실제로는 삭제 플래그만 설정
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{self.es_url}/{index_name}/_update/{instance_id}",
                        json={"doc": {"deleted": True, "deleted_at": datetime.utcnow().isoformat()}}
                    ) as resp:
                        if resp.status in [200, 201]:
                            logger.info(f"✅ Marked as deleted: {instance_id}")
                            return True
                        else:
                            error = await resp.text()
                            logger.error(f"❌ Failed to mark deleted: {error}")
                            return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing event: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    async def run(self):
        """메인 실행 루프"""
        logger.info("=" * 60)
        logger.info("🚀 Projection Worker 시작")
        logger.info("=" * 60)
        
        await self.initialize()
        
        self.running = True
        processed_count = 0
        
        logger.info("Waiting for events from Kafka...")
        
        while self.running:
            try:
                # Kafka에서 메시지 폴링
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    logger.error(f"Kafka error: {msg.error()}")
                    continue
                
                # 메시지 처리
                try:
                    event_data = json.loads(msg.value().decode('utf-8'))
                    
                    event_id = event_data.get('event_id')
                    event_type = event_data.get('event_type')
                    instance_id = event_data.get('instance_id')
                    
                    logger.info(f"📥 Event received:")
                    logger.info(f"   ID: {event_id}")
                    logger.info(f"   Type: {event_type}")
                    logger.info(f"   Instance: {instance_id}")
                    
                    # Event 처리
                    success = await self.process_event(event_data)
                    
                    if success:
                        processed_count += 1
                        # 메시지 커밋
                        self.consumer.commit()
                        logger.info(f"✅ Event processed successfully")
                    else:
                        logger.error(f"Failed to process event")
                        # 실패해도 커밋 (재처리 로직은 별도로 구현 필요)
                        self.consumer.commit()
                        
                except json.JSONDecodeError as e:
                    logger.error(f"JSON parsing failed: {e}")
                    self.consumer.commit()
                except Exception as e:
                    logger.error(f"Event processing failed: {e}")
                    self.consumer.commit()
                    
            except KeyboardInterrupt:
                logger.info("User interrupt received")
                break
            except Exception as e:
                logger.error(f"Processing loop error: {e}")
                await asyncio.sleep(1)
        
        logger.info(f"📊 Total events processed: {processed_count}")
        
    async def shutdown(self):
        """종료 처리"""
        logger.info("Shutting down Projection Worker...")
        self.running = False
        
        if self.consumer:
            self.consumer.close()
        
        logger.info("✅ Projection Worker shutdown complete")


async def main():
    """메인 실행"""
    runner = ProjectionWorkerRunner()
    
    # 시그널 핸들러
    def signal_handler(sig, frame):
        logger.info(f"Signal received: {sig}")
        runner.running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await runner.run()
    except Exception as e:
        logger.error(f"Runtime error: {e}")
    finally:
        await runner.shutdown()


if __name__ == "__main__":
    logger.info("Starting Projection Worker...")
    logger.info(f"Kafka: 127.0.0.1:9092")
    logger.info(f"Elasticsearch: localhost:9201")
    
    asyncio.run(main())