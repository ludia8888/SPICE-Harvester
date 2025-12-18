#!/usr/bin/env python3
"""
EventPublisher를 로컬에서 직접 실행
Docker 서비스 없이 S3/MinIO tail → Kafka 발행 처리
"""

import asyncio
import os

# 환경 변수 설정 (로컬 실행용)
os.environ["KAFKA_HOST"] = "localhost"
os.environ["KAFKA_PORT"] = "9092"
os.environ["EVENT_PUBLISHER_BATCH_SIZE"] = "200"
os.environ["EVENT_PUBLISHER_POLL_INTERVAL"] = "2"
os.environ["LOG_LEVEL"] = "INFO"

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def main():
    """EventPublisher 로컬 실행"""
    logger.info("=" * 60)
    logger.info("🚀 EventPublisher 로컬 실행 시작")
    logger.info("=" * 60)
    
    try:
        from message_relay.main import EventPublisher
        
        relay = EventPublisher()
        
        logger.info("EventPublisher 초기화 중...")
        await relay.initialize()
        
        logger.info("EventPublisher 실행 중...")
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
    logger.info("EventPublisher 로컬 실행 준비...")
    logger.info(f"Kafka: localhost:9092")
    
    asyncio.run(main())
