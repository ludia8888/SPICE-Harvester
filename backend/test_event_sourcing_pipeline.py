#!/usr/bin/env python3
"""
Event Sourcing 파이프라인 전체 테스트
올바른 환경 변수 설정으로 전체 데이터 흐름 확인
"""

import asyncio
import aiohttp
import asyncpg
import json
import os
import sys
import time
from datetime import datetime

# 환경 변수 설정 (Docker 환경이 아님을 명시)
os.environ["POSTGRES_HOST"] = "localhost"
os.environ["POSTGRES_PORT"] = "5433"
os.environ["KAFKA_HOST"] = "localhost"
os.environ["REDIS_HOST"] = "localhost"
os.environ["ELASTICSEARCH_HOST"] = "localhost"
os.environ["TERMINUS_SERVER_URL"] = "http://localhost:6363"
os.environ["DOCKER_CONTAINER"] = "false"  # Docker 환경이 아님을 명시

# backend 디렉토리를 sys.path에 추가
sys.path.insert(0, "/Users/isihyeon/Desktop/SPICE HARVESTER/backend")

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class EventSourcingPipelineTest:
    """Event Sourcing 파이프라인 전체 테스트"""
    
    def __init__(self):
        self.api_base_url = "http://localhost:8000"
        self.postgres_url = "postgresql://spiceadmin:spicepass123@localhost:5433/spicedb"
        self.test_db_name = f"es_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.test_class_id = "TestProduct"
        
    async def test_database_creation(self):
        """데이터베이스 생성 테스트"""
        logger.info("=" * 60)
        logger.info("1️⃣ 데이터베이스 생성 테스트")
        logger.info("=" * 60)
        
        async with aiohttp.ClientSession() as session:
            # 데이터베이스 생성
            async with session.post(
                f"{self.api_base_url}/api/v1/database/create",
                json={
                    "name": self.test_db_name,
                    "description": "Event Sourcing Pipeline Test"
                }
            ) as resp:
                status = resp.status
                data = await resp.json()
                
                if status in [200, 202]:
                    logger.info(f"✅ 데이터베이스 생성 성공: {self.test_db_name}")
                    logger.info(f"   응답: {data}")
                    return True
                else:
                    logger.error(f"❌ 데이터베이스 생성 실패: {status}")
                    logger.error(f"   응답: {data}")
                    return False
    
    async def check_outbox_before(self):
        """Outbox 테이블 이전 상태 확인"""
        logger.info("\n📊 Outbox 테이블 이전 상태 확인...")
        
        conn = await asyncpg.connect(self.postgres_url)
        try:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM spice_outbox.outbox WHERE processed_at IS NULL"
            )
            logger.info(f"   미처리 메시지 수: {count}")
            
            # 최근 5개 메시지 확인
            recent = await conn.fetch(
                """
                SELECT id, message_type, aggregate_type, created_at 
                FROM spice_outbox.outbox 
                ORDER BY created_at DESC 
                LIMIT 5
                """
            )
            
            if recent:
                logger.info("   최근 메시지:")
                for msg in recent:
                    logger.info(f"   - {msg['message_type']}: {msg['aggregate_type']} ({msg['created_at']})")
            
            return count
            
        finally:
            await conn.close()
    
    async def test_instance_creation(self):
        """인스턴스 생성 Command 테스트"""
        logger.info("\n=" * 60)
        logger.info("2️⃣ 인스턴스 생성 Command 테스트")
        logger.info("=" * 60)
        
        async with aiohttp.ClientSession() as session:
            # 비동기 인스턴스 생성 API 호출
            test_data = {
                "data": {
                    "product_name": "ES Test Product",
                    "price": 99.99,
                    "test_timestamp": datetime.utcnow().isoformat()
                },
                "metadata": {
                    "test_id": "es_pipeline_test"
                }
            }
            
            async with session.post(
                f"{self.api_base_url}/api/v1/instances/{self.test_db_name}/async/{self.test_class_id}/create",
                json=test_data
            ) as resp:
                status = resp.status
                data = await resp.json()
                
                if status == 202:
                    logger.info(f"✅ Command 생성 성공 (202 Accepted)")
                    logger.info(f"   Command ID: {data.get('command_id')}")
                    logger.info(f"   상태: {data.get('status')}")
                    return data.get('command_id')
                else:
                    logger.error(f"❌ Command 생성 실패: {status}")
                    logger.error(f"   응답: {data}")
                    return None
    
    async def check_outbox_after(self, before_count: int):
        """Outbox 테이블 이후 상태 확인"""
        logger.info("\n📊 Outbox 테이블 이후 상태 확인...")
        
        conn = await asyncpg.connect(self.postgres_url)
        try:
            # 3초 대기 (Message Relay가 처리할 시간)
            await asyncio.sleep(3)
            
            after_count = await conn.fetchval(
                "SELECT COUNT(*) FROM spice_outbox.outbox WHERE processed_at IS NULL"
            )
            
            total_count = await conn.fetchval(
                "SELECT COUNT(*) FROM spice_outbox.outbox"
            )
            
            logger.info(f"   전체 메시지 수: {total_count}")
            logger.info(f"   미처리 메시지 수: {after_count}")
            logger.info(f"   변화: {before_count} → {after_count}")
            
            # 새로 추가된 메시지 확인
            if total_count > before_count:
                new_messages = await conn.fetch(
                    """
                    SELECT id, message_type, aggregate_type, topic, processed_at
                    FROM spice_outbox.outbox 
                    ORDER BY created_at DESC 
                    LIMIT 5
                    """
                )
                
                logger.info("   최신 메시지:")
                for msg in new_messages:
                    status = "✅ 처리됨" if msg['processed_at'] else "⏳ 대기중"
                    logger.info(f"   - {msg['message_type']}: {msg['aggregate_type']} → {msg['topic']} [{status}]")
            
            return after_count
            
        finally:
            await conn.close()
    
    async def check_elasticsearch(self):
        """Elasticsearch 데이터 확인"""
        logger.info("\n📊 Elasticsearch 데이터 확인...")
        
        async with aiohttp.ClientSession() as session:
            # 인덱스 확인
            index_name = f"instances_{self.test_db_name.replace('-', '_')}"
            
            async with session.get(
                f"http://localhost:9200/{index_name}/_count"
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    count = data.get('count', 0)
                    logger.info(f"✅ Elasticsearch 인덱스 존재: {index_name}")
                    logger.info(f"   문서 수: {count}")
                    
                    if count > 0:
                        # 문서 내용 확인
                        async with session.get(
                            f"http://localhost:9200/{index_name}/_search?size=1"
                        ) as search_resp:
                            search_data = await search_resp.json()
                            hits = search_data.get('hits', {}).get('hits', [])
                            if hits:
                                logger.info("   샘플 문서:")
                                logger.info(f"   {json.dumps(hits[0]['_source'], indent=2)[:200]}...")
                    
                    return count > 0
                else:
                    logger.warning(f"⚠️  Elasticsearch 인덱스 없음: {index_name}")
                    return False
    
    async def test_command_status(self, command_id: str):
        """Command 상태 확인"""
        logger.info(f"\n📊 Command 상태 확인: {command_id}")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.api_base_url}/api/v1/command/{command_id}/status"
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    status = data.get('data', {}).get('status')
                    logger.info(f"   Command 상태: {status}")
                    
                    if status == "COMPLETED":
                        logger.info("   ✅ Command 처리 완료!")
                    elif status == "FAILED":
                        logger.error(f"   ❌ Command 실패: {data.get('data', {}).get('error')}")
                    else:
                        logger.info(f"   ⏳ Command 처리중... ({status})")
                    
                    return status
                else:
                    logger.warning(f"⚠️  Command 상태 확인 실패: {resp.status}")
                    return None
    
    async def run_full_test(self):
        """전체 파이프라인 테스트 실행"""
        logger.info("🚀 Event Sourcing 파이프라인 전체 테스트 시작")
        logger.info("=" * 60)
        
        try:
            # 1. 데이터베이스 생성
            if not await self.test_database_creation():
                logger.error("데이터베이스 생성 실패 - 테스트 중단")
                return
            
            await asyncio.sleep(2)
            
            # 2. Outbox 이전 상태 확인
            before_count = await self.check_outbox_before()
            
            # 3. 인스턴스 생성 Command 발행
            command_id = await self.test_instance_creation()
            if not command_id:
                logger.error("Command 생성 실패 - 테스트 중단")
                return
            
            # 4. Outbox 이후 상태 확인
            await self.check_outbox_after(before_count)
            
            # 5. Command 처리 대기 (최대 10초)
            for i in range(10):
                await asyncio.sleep(1)
                status = await self.test_command_status(command_id)
                if status in ["COMPLETED", "FAILED"]:
                    break
            
            # 6. Elasticsearch 데이터 확인
            await asyncio.sleep(2)
            es_success = await self.check_elasticsearch()
            
            # 7. 결과 요약
            logger.info("\n" + "=" * 60)
            logger.info("📊 파이프라인 테스트 결과 요약")
            logger.info("=" * 60)
            logger.info(f"✅ 데이터베이스 생성: {self.test_db_name}")
            logger.info(f"✅ Command 생성: {command_id}")
            logger.info(f"{'✅' if es_success else '❌'} Elasticsearch 데이터: {'있음' if es_success else '없음'}")
            
            if es_success:
                logger.info("\n🎉 Event Sourcing 파이프라인이 정상 작동합니다!")
            else:
                logger.info("\n⚠️  파이프라인 일부 구성 요소가 작동하지 않습니다.")
                logger.info("   다음을 확인하세요:")
                logger.info("   - Message Relay 서비스 실행 여부")
                logger.info("   - Instance Worker 서비스 실행 여부")
                logger.info("   - Projection Worker 서비스 실행 여부")
                logger.info("   - Kafka 연결 상태")
            
        except Exception as e:
            logger.error(f"테스트 중 오류 발생: {e}")
            import traceback
            logger.error(traceback.format_exc())


async def main():
    tester = EventSourcingPipelineTest()
    await tester.run_full_test()


if __name__ == "__main__":
    asyncio.run(main())