#!/usr/bin/env python3
"""
Redis 연결 테스트 및 Command Status 저장 검증
THINK ULTRA - 실제 연결 문제 파악
"""

import asyncio
import redis.asyncio as redis
import json
import os
from datetime import datetime

# 환경 변수 설정
os.environ["REDIS_HOST"] = "localhost"
os.environ["REDIS_PORT"] = "6380"  # Docker Redis 실제 포트

import sys
sys.path.insert(0, "/Users/isihyeon/Desktop/SPICE HARVESTER/backend")

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_redis_connection():
    """Redis 연결 테스트"""
    logger.info("=" * 60)
    logger.info("🔍 Redis 연결 테스트")
    logger.info("=" * 60)
    
    # 다양한 포트로 시도
    ports_to_try = [6380, 6379]
    
    for port in ports_to_try:
        try:
            logger.info(f"\n포트 {port} 시도중...")
            
            # Redis 클라이언트 생성
            client = redis.Redis(
                host="localhost",
                port=port,
                decode_responses=True
            )
            
            # 연결 테스트
            pong = await client.ping()
            if pong:
                logger.info(f"✅ Redis 연결 성공! (포트: {port})")
                
                # 정보 가져오기
                info = await client.info()
                logger.info(f"   Redis 버전: {info.get('redis_version', 'Unknown')}")
                logger.info(f"   사용 메모리: {info.get('used_memory_human', 'Unknown')}")
                
                # 테스트 데이터 저장
                test_key = f"test:connection:{datetime.now().timestamp()}"
                await client.set(test_key, "OK", ex=60)
                value = await client.get(test_key)
                logger.info(f"   테스트 쓰기/읽기: {value}")
                
                # Command Status 테스트
                command_id = "ed4645c1-7dc4-45e8-b358-19a3fededafd"
                status_key = f"command:{command_id}:status"
                
                # 저장된 상태 확인
                existing = await client.get(status_key)
                if existing:
                    logger.info(f"   ✅ Command 상태 발견: {existing}")
                else:
                    logger.info(f"   ❌ Command 상태 없음: {status_key}")
                    
                    # 수동으로 상태 저장
                    status_data = {
                        "status": "PENDING",
                        "command_id": command_id,
                        "created_at": datetime.utcnow().isoformat()
                    }
                    await client.set(status_key, json.dumps(status_data), ex=3600)
                    logger.info(f"   📝 Command 상태 수동 저장 완료")
                
                # 모든 command 키 검색
                pattern = "command:*"
                keys = []
                async for key in client.scan_iter(match=pattern):
                    keys.append(key)
                
                logger.info(f"\n   📋 전체 Command 키 ({len(keys)}개):")
                for key in keys[:10]:  # 처음 10개만
                    value = await client.get(key)
                    logger.info(f"      - {key}: {value[:100] if value else 'None'}")
                
                await client.close()
                return True
                
        except Exception as e:
            logger.error(f"❌ 포트 {port} 연결 실패: {e}")
    
    return False


async def test_redis_service():
    """RedisService 클래스 테스트"""
    logger.info("\n" + "=" * 60)
    logger.info("🔍 RedisService 클래스 테스트")
    logger.info("=" * 60)
    
    try:
        from shared.services.redis_service import RedisService, create_redis_service
        from shared.config.settings import ApplicationSettings
        
        # 환경 변수 오버라이드
        os.environ["REDIS_PORT"] = "6380"
        
        settings = ApplicationSettings()
        logger.info(f"   Redis Host: {settings.database.redis_host}")
        logger.info(f"   Redis Port: {settings.database.redis_port}")
        logger.info(f"   Redis URL: {settings.database.redis_url}")
        
        # RedisService 생성
        redis_service = create_redis_service(settings)
        await redis_service.connect()
        
        logger.info("   ✅ RedisService 연결 성공")
        
        # CommandStatusService 테스트
        from shared.services.command_status_service import CommandStatusService
        
        command_status_service = CommandStatusService(redis_service)
        
        # Command 상태 설정
        test_command_id = "test-command-" + str(datetime.now().timestamp())
        await command_status_service.set_command_status(
            command_id=test_command_id,
            status="PENDING",
            metadata={"test": "data"}
        )
        
        # Command 상태 조회
        status = await command_status_service.get_command_status(test_command_id)
        logger.info(f"   Command 상태 저장/조회 테스트: {status}")
        
        # 실제 Command ID로 조회
        real_command_id = "ed4645c1-7dc4-45e8-b358-19a3fededafd"
        real_status = await command_status_service.get_command_status(real_command_id)
        
        if real_status:
            logger.info(f"   ✅ 실제 Command 상태 발견: {real_status}")
        else:
            logger.info(f"   ❌ 실제 Command 상태 없음")
            
            # 수동 저장
            await command_status_service.set_command_status(
                command_id=real_command_id,
                status="PENDING",
                metadata={
                    "command_type": "CREATE_INSTANCE",
                    "db_name": "es_test_20250811_143713",
                    "class_id": "TestProduct",
                    "created_at": datetime.utcnow().isoformat()
                }
            )
            logger.info(f"   📝 실제 Command 상태 수동 저장 완료")
        
        await redis_service.disconnect()
        
    except Exception as e:
        logger.error(f"❌ RedisService 테스트 실패: {e}")
        import traceback
        logger.error(traceback.format_exc())


async def main():
    # Redis 직접 연결 테스트
    redis_ok = await test_redis_connection()
    
    if redis_ok:
        # RedisService 테스트
        await test_redis_service()
    else:
        logger.error("Redis 연결 실패 - 서비스 테스트 건너뜀")
    
    logger.info("\n" + "=" * 60)
    logger.info("📊 결과 요약")
    logger.info("=" * 60)
    logger.info(f"Redis 연결: {'✅ 성공' if redis_ok else '❌ 실패'}")
    logger.info(f"포트: 6380 (Docker Redis)")
    
    if not redis_ok:
        logger.info("\n🔧 해결 방법:")
        logger.info("1. Docker Redis 확인: docker-compose ps redis")
        logger.info("2. Redis 재시작: docker-compose restart redis")
        logger.info("3. 포트 확인: lsof -i :6380")


if __name__ == "__main__":
    asyncio.run(main())