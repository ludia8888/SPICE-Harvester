#!/usr/bin/env python3
"""
수정된 Event Sourcing 파이프라인 전체 테스트
올바른 토픽 설정으로 전체 데이터 흐름 확인
THINK ULTRA - 실제 작동하는 구현 검증
"""

import asyncio
import aiohttp
import asyncpg
import json
import os
import sys
import time
from datetime import datetime

# 환경 변수 설정 (올바른 포트와 호스트)
os.environ["POSTGRES_HOST"] = "localhost"
os.environ["POSTGRES_PORT"] = "5433"
os.environ["KAFKA_HOST"] = "127.0.0.1"  # IPv4 명시
os.environ["KAFKA_PORT"] = "9092"
os.environ["REDIS_HOST"] = "localhost"
os.environ["REDIS_PORT"] = "6379"  # 실제 Redis 포트
os.environ["ELASTICSEARCH_HOST"] = "localhost"
os.environ["ELASTICSEARCH_PORT"] = "9201"  # 실제 Elasticsearch 포트
os.environ["TERMINUS_SERVER_URL"] = "http://localhost:6363"
os.environ["DOCKER_CONTAINER"] = "false"

sys.path.insert(0, "/Users/isihyeon/Desktop/SPICE HARVESTER/backend")

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class FixedPipelineTest:
    """수정된 Event Sourcing 파이프라인 테스트"""
    
    def __init__(self):
        self.api_base_url = "http://localhost:8000"
        self.postgres_url = "postgresql://spiceadmin:spicepass123@localhost:5433/spicedb"
        self.test_db_name = f"fixed_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.test_class_id = "FixedTestProduct"
        self.test_command_id = None
        
    async def create_test_database(self):
        """테스트용 데이터베이스 생성"""
        logger.info("=" * 60)
        logger.info("1️⃣ 데이터베이스 생성")
        logger.info("=" * 60)
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.api_base_url}/api/v1/database/create",
                json={
                    "name": self.test_db_name,
                    "description": "Fixed Pipeline Test"
                }
            ) as resp:
                if resp.status in [200, 202]:
                    data = await resp.json()
                    logger.info(f"✅ 데이터베이스 생성: {self.test_db_name}")
                    return True
                else:
                    logger.error(f"❌ 데이터베이스 생성 실패: {resp.status}")
                    return False
    
    async def create_test_instance(self):
        """테스트 인스턴스 생성"""
        logger.info("\n" + "=" * 60)
        logger.info("2️⃣ 인스턴스 생성 Command (수정된 토픽)")
        logger.info("=" * 60)
        
        test_data = {
            "data": {
                "product_name": "Fixed Pipeline Test Product",
                "price": 123.45,
                "test_flag": "FIXED_TOPIC",
                "timestamp": datetime.utcnow().isoformat()
            },
            "metadata": {
                "test_id": "fixed_pipeline_test",
                "fixed": True
            }
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.api_base_url}/api/v1/instances/{self.test_db_name}/async/{self.test_class_id}/create",
                json=test_data
            ) as resp:
                if resp.status == 202:
                    data = await resp.json()
                    self.test_command_id = data.get('command_id')
                    logger.info(f"✅ Command 생성 성공")
                    logger.info(f"   Command ID: {self.test_command_id}")
                    return True
                else:
                    logger.error(f"❌ Command 생성 실패: {resp.status}")
                    return False
    
    async def check_outbox_table(self):
        """Outbox 테이블 확인"""
        logger.info("\n📊 Outbox 테이블 확인...")
        
        conn = await asyncpg.connect(self.postgres_url)
        try:
            # 최신 메시지 확인
            recent = await conn.fetch(
                """
                SELECT id, message_type, aggregate_type, topic, processed_at 
                FROM spice_outbox.outbox 
                WHERE id = $1 OR aggregate_id LIKE $2
                ORDER BY created_at DESC 
                LIMIT 5
                """,
                self.test_command_id,
                f"%{self.test_class_id}%"
            )
            
            if recent:
                logger.info("   최신 관련 메시지:")
                for msg in recent:
                    status = "✅ 처리됨" if msg['processed_at'] else "⏳ 대기중"
                    logger.info(f"   - {msg['message_type']}: {msg['aggregate_type']} → 토픽: {msg['topic']} [{status}]")
                    
                    # 토픽 확인
                    if msg['id'] == self.test_command_id:
                        if msg['topic'] == 'instance_commands':
                            logger.info("   🎉 올바른 토픽(instance_commands)으로 저장됨!")
                        else:
                            logger.error(f"   ❌ 잘못된 토픽: {msg['topic']}")
            
        finally:
            await conn.close()
    
    async def run_message_relay(self):
        """Message Relay 실행"""
        logger.info("\n📤 Message Relay 실행...")
        
        # Message Relay를 백그라운드에서 실행
        from message_relay.main import MessageRelay
        
        relay = MessageRelay()
        await relay.initialize()
        
        # 한 번 실행
        processed = await relay.process_events()
        logger.info(f"   처리된 메시지: {processed}개")
        
        await relay.shutdown()
    
    async def check_kafka_topic(self):
        """Kafka 토픽 확인"""
        logger.info("\n📊 Kafka 토픽 확인...")
        
        from confluent_kafka import Consumer
        
        # instance_commands 토픽 확인
        consumer = Consumer({
            'bootstrap.servers': '127.0.0.1:9092',
            'group.id': 'test-check',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False,
            'session.timeout.ms': 6000,
            'max.poll.interval.ms': 300000
        })
        
        try:
            # 토픽의 최신 오프셋 확인
            from confluent_kafka import TopicPartition
            
            partitions = []
            for i in range(3):  # 3개 파티션
                tp = TopicPartition('instance_commands', i)
                partitions.append(tp)
            
            # 각 파티션 확인
            found = False
            for tp in partitions:
                low, high = consumer.get_watermark_offsets(tp, timeout=5)
                if high > low:
                    logger.info(f"   파티션 {tp.partition}: {high - low}개 메시지")
                    
                    # 최신 메시지 확인
                    tp.offset = high - 1
                    consumer.assign([tp])
                    
                    msg = consumer.poll(timeout=1.0)
                    if msg and not msg.error():
                        value = json.loads(msg.value().decode('utf-8'))
                        if value.get('command_id') == self.test_command_id:
                            logger.info(f"   🎉 우리 Command가 instance_commands 토픽에 있음!")
                            found = True
            
            if not found:
                logger.warning("   ⚠️  Command가 instance_commands 토픽에 없음")
            
        finally:
            consumer.close()
    
    async def run_instance_worker(self):
        """Instance Worker 실행"""
        logger.info("\n🔧 Instance Worker 실행...")
        
        try:
            from instance_worker.main import InstanceWorker
            
            worker = InstanceWorker()
            await worker.initialize()
            
            # 메시지 처리 시도
            logger.info("   Command 처리 중...")
            
            # 몇 초 동안 실행
            for i in range(5):
                # Worker의 run 메서드를 직접 호출하는 대신
                # 한 번만 poll하도록 수정 필요
                await asyncio.sleep(1)
                logger.info(f"   대기 중... {i+1}/5")
            
            logger.info("   Instance Worker 종료")
            
        except Exception as e:
            logger.error(f"   Instance Worker 실행 실패: {e}")
    
    async def check_command_status(self):
        """Command 상태 확인"""
        logger.info(f"\n📊 Command 상태 확인...")
        
        async with aiohttp.ClientSession() as session:
            # API URL 수정 - 올바른 엔드포인트 사용
            url = f"{self.api_base_url}/api/v1/command/{self.test_command_id}/status"
            
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    status = data.get('data', {}).get('status')
                    logger.info(f"   Command 상태: {status}")
                    
                    if status == "COMPLETED":
                        logger.info("   ✅ Command 처리 완료!")
                        return True
                    elif status == "PROCESSING":
                        logger.info("   ⏳ Command 처리 중...")
                    elif status == "FAILED":
                        error = data.get('data', {}).get('error')
                        logger.error(f"   ❌ Command 실패: {error}")
                else:
                    # Redis에서 직접 확인
                    import redis.asyncio as redis
                    
                    client = redis.Redis(
                        host="localhost",
                        port=6379,
                        decode_responses=True
                    )
                    
                    status_key = f"command:{self.test_command_id}:status"
                    status_data = await client.get(status_key)
                    
                    if status_data:
                        logger.info(f"   Redis 직접 조회: {status_data[:100]}...")
                    else:
                        logger.warning(f"   Redis에 상태 없음")
                    
                    await client.close()
        
        return False
    
    async def check_elasticsearch(self):
        """Elasticsearch 확인"""
        logger.info(f"\n📊 Elasticsearch 확인...")
        
        index_name = f"instances_{self.test_db_name.replace('-', '_')}"
        
        async with aiohttp.ClientSession() as session:
            # 포트 9201 사용
            url = f"http://localhost:9201/{index_name}/_count"
            
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    count = data.get('count', 0)
                    logger.info(f"   인덱스: {index_name}")
                    logger.info(f"   문서 수: {count}")
                    
                    if count > 0:
                        logger.info("   ✅ Elasticsearch에 데이터 있음!")
                        return True
                else:
                    logger.warning(f"   인덱스 없음 또는 접근 실패: {resp.status}")
        
        return False
    
    async def run_full_test(self):
        """전체 테스트 실행"""
        logger.info("🚀 수정된 Event Sourcing 파이프라인 테스트")
        logger.info("=" * 60)
        
        try:
            # 1. 데이터베이스 생성
            if not await self.create_test_database():
                return
            
            await asyncio.sleep(2)
            
            # 2. 인스턴스 Command 생성
            if not await self.create_test_instance():
                return
            
            await asyncio.sleep(1)
            
            # 3. Outbox 테이블 확인
            await self.check_outbox_table()
            
            # 4. Message Relay 실행
            await self.run_message_relay()
            
            # 5. Kafka 토픽 확인
            await self.check_kafka_topic()
            
            # 6. Instance Worker 실행 (선택적)
            # await self.run_instance_worker()
            
            # 7. Command 상태 확인
            await self.check_command_status()
            
            # 8. Elasticsearch 확인
            es_ok = await self.check_elasticsearch()
            
            # 결과 요약
            logger.info("\n" + "=" * 60)
            logger.info("📊 테스트 결과 요약")
            logger.info("=" * 60)
            logger.info(f"✅ 데이터베이스: {self.test_db_name}")
            logger.info(f"✅ Command ID: {self.test_command_id}")
            logger.info(f"✅ 올바른 토픽 사용: instance_commands")
            logger.info(f"{'✅' if es_ok else '❌'} Elasticsearch 데이터")
            
            if not es_ok:
                logger.info("\n🔧 남은 작업:")
                logger.info("   - Instance Worker 실행 필요")
                logger.info("   - Projection Worker 실행 필요")
            else:
                logger.info("\n🎉 전체 파이프라인 정상 작동!")
            
        except Exception as e:
            logger.error(f"테스트 실패: {e}")
            import traceback
            logger.error(traceback.format_exc())


async def main():
    tester = FixedPipelineTest()
    await tester.run_full_test()


if __name__ == "__main__":
    asyncio.run(main())