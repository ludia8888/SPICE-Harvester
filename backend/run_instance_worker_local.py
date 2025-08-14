#!/usr/bin/env python3
"""
Instance Worker를 로컬에서 실제로 실행
Kafka에서 Command를 받아 처리하고 Event 발행
THINK ULTRA - 실제 작동하는 구현
"""

import asyncio
import os
import sys
import signal
import logging
from typing import Optional

# 환경 변수 설정 (로컬 실행용)
os.environ["POSTGRES_HOST"] = "localhost"
os.environ["POSTGRES_PORT"] = "5432"
os.environ["POSTGRES_USER"] = "spiceadmin"
os.environ["POSTGRES_PASSWORD"] = "spicepass123"
os.environ["POSTGRES_DB"] = "spicedb"
os.environ["KAFKA_HOST"] = "127.0.0.1"
os.environ["KAFKA_PORT"] = "9092"
os.environ["REDIS_HOST"] = "localhost"
os.environ["REDIS_PORT"] = "6379"
os.environ["TERMINUS_SERVER_URL"] = "http://localhost:6363"
os.environ["TERMINUS_USER"] = "admin"
os.environ["TERMINUS_ACCOUNT"] = "admin"
os.environ["TERMINUS_KEY"] = "admin"
os.environ["MINIO_ENDPOINT_URL"] = "http://localhost:9000"
os.environ["MINIO_ACCESS_KEY"] = "minioadmin"
os.environ["MINIO_SECRET_KEY"] = "minioadmin123"
os.environ["INSTANCE_BUCKET"] = "instance-events"
os.environ["LOG_LEVEL"] = "INFO"

# backend 경로 추가
sys.path.insert(0, "/Users/isihyeon/Desktop/SPICE HARVESTER/backend")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class InstanceWorkerRunner:
    """Instance Worker 실행 관리"""
    
    def __init__(self):
        self.worker = None
        self.running = False
        
    async def run(self):
        """Instance Worker 실행"""
        logger.info("=" * 60)
        logger.info("🚀 Instance Worker 로컬 실행 시작")
        logger.info("=" * 60)
        
        try:
            from instance_worker.main import InstanceWorker
            
            # Worker 인스턴스 생성
            self.worker = InstanceWorker()
            
            logger.info("Instance Worker 초기화 중...")
            await self.worker.initialize()
            logger.info("✅ Instance Worker 초기화 완료")
            
            logger.info("Kafka에서 Command 대기 중...")
            logger.info("토픽: instance_commands")
            
            self.running = True
            
            # 메인 처리 루프
            while self.running:
                try:
                    # Kafka에서 메시지 폴링
                    msg = self.worker.consumer.poll(timeout=1.0)
                    
                    if msg is None:
                        continue
                        
                    if msg.error():
                        logger.error(f"Kafka 에러: {msg.error()}")
                        continue
                    
                    # 메시지 처리
                    try:
                        import json
                        command_data = json.loads(msg.value().decode('utf-8'))
                        
                        command_id = command_data.get('command_id')
                        command_type = command_data.get('command_type')
                        aggregate_id = command_data.get('aggregate_id')
                        
                        logger.info(f"📥 Command 수신:")
                        logger.info(f"   ID: {command_id}")
                        logger.info(f"   Type: {command_type}")
                        logger.info(f"   Aggregate: {aggregate_id}")
                        
                        # Command 처리
                        await self.worker.process_command(command_data)
                        
                        # 메시지 커밋
                        self.worker.consumer.commit()
                        
                        logger.info(f"✅ Command 처리 완료: {command_id}")
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON 파싱 실패: {e}")
                    except Exception as e:
                        logger.error(f"Command 처리 실패: {e}")
                        import traceback
                        logger.error(traceback.format_exc())
                        
                except KeyboardInterrupt:
                    logger.info("사용자 중단 요청")
                    break
                except Exception as e:
                    logger.error(f"처리 루프 에러: {e}")
                    await asyncio.sleep(1)
            
        except Exception as e:
            logger.error(f"Instance Worker 실행 실패: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Worker 종료"""
        logger.info("Instance Worker 종료 중...")
        self.running = False
        
        if self.worker:
            if hasattr(self.worker, 'consumer') and self.worker.consumer:
                self.worker.consumer.close()
            if hasattr(self.worker, 'producer') and self.worker.producer:
                self.worker.producer.flush()
            if hasattr(self.worker, 'redis_service') and self.worker.redis_service:
                await self.worker.redis_service.disconnect()
            if hasattr(self.worker, 'terminus_service') and self.worker.terminus_service:
                await self.worker.terminus_service.disconnect()
        
        logger.info("✅ Instance Worker 종료 완료")


async def main():
    """메인 실행"""
    runner = InstanceWorkerRunner()
    
    # 시그널 핸들러
    def signal_handler(sig, frame):
        logger.info(f"시그널 수신: {sig}")
        runner.running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await runner.run()
    except Exception as e:
        logger.error(f"실행 실패: {e}")


if __name__ == "__main__":
    logger.info("Instance Worker 로컬 실행 준비...")
    logger.info(f"Kafka: 127.0.0.1:9092")
    logger.info(f"Redis: localhost:6379")
    logger.info(f"PostgreSQL: localhost:5432")
    logger.info(f"TerminusDB: localhost:6363")
    logger.info(f"MinIO: localhost:9000")
    
    asyncio.run(main())