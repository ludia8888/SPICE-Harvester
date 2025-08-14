#!/usr/bin/env python3
"""
Message Relay를 로컬에서 직접 실행
Docker 서비스 없이 Outbox → Kafka 메시지 전달 처리
"""

import asyncio
import os
import sys

# 환경 변수 설정 (로컬 실행용)
os.environ["POSTGRES_HOST"] = "localhost"
os.environ["POSTGRES_PORT"] = "5432"
os.environ["POSTGRES_USER"] = "spiceadmin"
os.environ["POSTGRES_PASSWORD"] = "spicepass123"
os.environ["POSTGRES_DB"] = "spicedb"
os.environ["KAFKA_HOST"] = "localhost"
os.environ["KAFKA_PORT"] = "9092"
os.environ["MESSAGE_RELAY_BATCH_SIZE"] = "100"
os.environ["MESSAGE_RELAY_POLL_INTERVAL"] = "2"
os.environ["LOG_LEVEL"] = "INFO"

# backend 경로 추가
sys.path.insert(0, "/Users/isihyeon/Desktop/SPICE HARVESTER/backend")

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def main():
    """Message Relay 로컬 실행"""
    logger.info("=" * 60)
    logger.info("🚀 Message Relay 로컬 실행 시작")
    logger.info("=" * 60)
    
    try:
        # Message Relay import
        from message_relay.main import MessageRelay
        
        # Message Relay 인스턴스 생성
        relay = MessageRelay()
        
        logger.info("Message Relay 초기화 중...")
        await relay.initialize()
        
        logger.info("Message Relay 실행 중...")
        logger.info("Ctrl+C로 중지 가능")
        
        # 메인 루프 실행
        await relay.run()
        
    except KeyboardInterrupt:
        logger.info("\n사용자 중단 요청")
        if 'relay' in locals():
            await relay.shutdown()
    except Exception as e:
        logger.error(f"Message Relay 실행 실패: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        if 'relay' in locals():
            await relay.shutdown()


if __name__ == "__main__":
    logger.info("Message Relay 로컬 실행 준비...")
    logger.info(f"PostgreSQL: localhost:5432")
    logger.info(f"Kafka: localhost:9092")
    
    asyncio.run(main())