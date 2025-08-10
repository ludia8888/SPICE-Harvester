"""
Ontology Worker Service
Command를 처리하여 실제 TerminusDB 작업을 수행하는 워커 서비스
"""

import asyncio
import json
import logging
import os
import signal
from typing import Optional, Dict, Any

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
import asyncpg

from shared.config.service_config import ServiceConfig
from shared.config.app_config import AppConfig
from shared.config.settings import ApplicationSettings
from shared.models.commands import (
    BaseCommand, CommandType, CommandStatus, 
    OntologyCommand, DatabaseCommand, BranchCommand
)
from shared.models.events import (
    BaseEvent, EventType,
    OntologyEvent, DatabaseEvent, BranchEvent,
    CommandFailedEvent
)
from shared.models.config import ConnectionConfig
from oms.services.async_terminus import AsyncTerminusService
from shared.services.redis_service import RedisService, create_redis_service
from shared.services.command_status_service import CommandStatusService

# Observability imports
from shared.observability.tracing import get_tracing_service, trace_endpoint
from shared.observability.metrics import get_metrics_collector
from shared.observability.context_propagation import ContextPropagator

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class OntologyWorker:
    """온톨로지 Command를 처리하는 워커"""
    
    def __init__(self):
        self.running = False
        self.kafka_servers = ServiceConfig.get_kafka_bootstrap_servers()
        self.consumer: Optional[Consumer] = None
        self.producer: Optional[Producer] = None
        self.terminus_service: Optional[AsyncTerminusService] = None
        self.redis_service: Optional[RedisService] = None
        self.command_status_service: Optional[CommandStatusService] = None
        self.tracing_service = None
        self.metrics_collector = None
        self.context_propagator = ContextPropagator()
        
    async def initialize(self):
        """워커 초기화"""
        # Kafka Consumer 설정
        self.consumer = Consumer({
            'bootstrap.servers': self.kafka_servers,
            'group.id': 'ontology-worker-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,  # 5분
            'session.timeout.ms': 45000,  # 45초
        })
        
        # Kafka Producer 설정 (Event 발행용)
        self.producer = Producer({
            'bootstrap.servers': self.kafka_servers,
            'client.id': 'ontology-worker',
            'acks': 'all',
            'retries': 3,
            'compression.type': 'snappy',
        })
        
        # TerminusDB 연결 설정
        connection_info = ConnectionConfig(
            server_url=ServiceConfig.get_terminus_url(),
            user=os.getenv("TERMINUS_USER", "admin"),
            account=os.getenv("TERMINUS_ACCOUNT", "admin"),
            key=os.getenv("TERMINUS_KEY", "admin123"),
        )
        self.terminus_service = AsyncTerminusService(connection_info)
        await self.terminus_service.connect()
        
        # Redis 연결 설정
        settings = ApplicationSettings()
        self.redis_service = create_redis_service(settings)
        await self.redis_service.connect()
        self.command_status_service = CommandStatusService(self.redis_service)
        logger.info("Redis connection established")
        
        # Command 토픽 구독
        self.consumer.subscribe([AppConfig.ONTOLOGY_COMMANDS_TOPIC])
        
        # Initialize OpenTelemetry
        self.tracing_service = get_tracing_service("ontology-worker")
        self.metrics_collector = get_metrics_collector("ontology-worker")
        
        logger.info("Ontology Worker initialized successfully")
        
    async def process_command(self, command_data: Dict[str, Any]) -> None:
        """Command 처리"""
        command_id = command_data.get('command_id')
        
        try:
            # Redis에 처리 시작 상태 업데이트
            if command_id and self.command_status_service:
                await self.command_status_service.start_processing(
                    command_id=command_id,
                    worker_id=f"worker-{os.getpid()}"
                )
            
            command_type = command_data.get('command_type')
            
            import sys
            sys.stdout.flush()
            sys.stdout.flush()
            sys.stdout.flush()
            
            # Command 유형별 처리
            if command_type == CommandType.CREATE_ONTOLOGY_CLASS:
                sys.stdout.flush()
                await self.handle_create_ontology(command_data)
                sys.stdout.flush()
            elif command_type == CommandType.UPDATE_ONTOLOGY_CLASS:
                await self.handle_update_ontology(command_data)
            elif command_type == CommandType.DELETE_ONTOLOGY_CLASS:
                await self.handle_delete_ontology(command_data)
            elif command_type == CommandType.CREATE_DATABASE:
                await self.handle_create_database(command_data)
            elif command_type == CommandType.DELETE_DATABASE:
                await self.handle_delete_database(command_data)
            else:
                logger.warning(f"Unknown command type: {command_type}")
                
        except Exception as e:
            logger.error(f"Error processing command: {e}")
            
            # Redis에 실패 상태 업데이트
            if command_id and self.command_status_service:
                await self.command_status_service.fail_command(
                    command_id=command_id,
                    error=str(e)
                )
            
            # 실패 이벤트 발행
            await self.publish_failure_event(command_data, str(e))
            raise
            
    async def handle_create_ontology(self, command_data: Dict[str, Any]) -> None:
        """온톨로지 생성 처리"""
        import sys
        sys.stdout.flush()
        sys.stdout.flush()
        
        payload = command_data.get('payload', {})
        db_name = payload.get('db_name')  # Fixed: get db_name from payload
        command_id = command_data.get('command_id')
        
        sys.stdout.flush()
        sys.stdout.flush()
        sys.stdout.flush()
        
        
        try:
            # TerminusDB에 온톨로지 생성
            from shared.models.ontology import OntologyBase
            ontology_obj = OntologyBase(
                id=payload.get('class_id'),
                label=payload.get('label'),
                description=payload.get('description'),
                properties=payload.get('properties', []),
                relationships=payload.get('relationships', []),
                parent_class=payload.get('parent_class'),
                abstract=payload.get('abstract', False)
            )
            
            result = await self.terminus_service.create_ontology(db_name, ontology_obj)  # Fixed: call create_ontology with OntologyBase
            
            
            # 성공 이벤트 생성
            event = OntologyEvent(
                event_type=EventType.ONTOLOGY_CLASS_CREATED,
                db_name=db_name,
                class_id=payload.get('class_id'),
                command_id=command_id,
                data={
                    "class_id": payload.get('class_id'),
                    "label": payload.get('label'),
                    "description": payload.get('description'),
                    "properties": payload.get('properties', []),
                    "relationships": payload.get('relationships', []),
                    "parent_class": payload.get('parent_class'),
                    "abstract": payload.get('abstract', False),
                },
                occurred_by=command_data.get('created_by', 'system')
            )
            
            # 이벤트 발행
            await self.publish_event(event)
            
            # Redis에 완료 상태 업데이트
            if command_id and self.command_status_service:
                await self.command_status_service.complete_command(
                    command_id=command_id,
                    result={
                        "class_id": payload.get('class_id'),
                        "message": f"Successfully created ontology class: {payload.get('class_id')}"
                        }
                )
            
            logger.info(f"Successfully created ontology class: {payload.get('class_id')}")
            
        except Exception as e:
            logger.error(f"Failed to create ontology class: {e}")
            raise
            
    async def handle_update_ontology(self, command_data: Dict[str, Any]) -> None:
        """온톨로지 업데이트 처리"""
        db_name = command_data.get('db_name')
        payload = command_data.get('payload', {})
        class_id = payload.get('class_id')
        updates = payload.get('updates', {})
        command_id = command_data.get('command_id')
        
        logger.info(f"Updating ontology class: {class_id} in database: {db_name}")
        
        try:
            # 기존 데이터 조회
            existing = await self.terminus_service.get_ontology(db_name, class_id)
            if not existing:
                raise Exception(f"Ontology class '{class_id}' not found")
            
            # 업데이트 데이터 병합
            merged_data = {**existing, **updates}
            merged_data["id"] = class_id  # ID는 변경 불가
            
            # TerminusDB 업데이트
            result = await self.terminus_service.update_ontology(db_name, class_id, merged_data)
            
            # 성공 이벤트 생성
            event = OntologyEvent(
                event_type=EventType.ONTOLOGY_CLASS_UPDATED,
                db_name=db_name,
                class_id=class_id,
                command_id=command_id,
                data={
                    "class_id": class_id,
                    "updates": updates,
                    "merged_data": merged_data,
                },
                occurred_by=command_data.get('created_by', 'system')
            )
            
            # 이벤트 발행
            await self.publish_event(event)
            
            # Redis에 완료 상태 업데이트
            if command_id and self.command_status_service:
                await self.command_status_service.complete_command(
                    command_id=command_id,
                    result={
                        "class_id": class_id,
                        "message": f"Successfully updated ontology class: {class_id}",
                        "updates": updates,
                        }
                )
            
            logger.info(f"Successfully updated ontology class: {class_id}")
            
        except Exception as e:
            logger.error(f"Failed to update ontology class: {e}")
            raise
            
    async def handle_delete_ontology(self, command_data: Dict[str, Any]) -> None:
        """온톨로지 삭제 처리"""
        db_name = command_data.get('db_name')
        payload = command_data.get('payload', {})
        class_id = payload.get('class_id')
        command_id = command_data.get('command_id')
        
        logger.info(f"Deleting ontology class: {class_id} from database: {db_name}")
        
        try:
            # TerminusDB에서 삭제
            success = await self.terminus_service.delete_ontology(db_name, class_id)
            
            if not success:
                raise Exception(f"Ontology class '{class_id}' not found")
            
            # 성공 이벤트 생성
            event = OntologyEvent(
                event_type=EventType.ONTOLOGY_CLASS_DELETED,
                db_name=db_name,
                class_id=class_id,
                command_id=command_id,
                data={
                    "class_id": class_id
                },
                occurred_by=command_data.get('created_by', 'system')
            )
            
            # 이벤트 발행
            await self.publish_event(event)
            
            # Redis에 완료 상태 업데이트
            if command_id and self.command_status_service:
                await self.command_status_service.complete_command(
                    command_id=command_id,
                    result={
                        "class_id": class_id,
                        "message": f"Successfully deleted ontology class: {class_id}"
                    }
                )
            
            logger.info(f"Successfully deleted ontology class: {class_id}")
            
        except Exception as e:
            logger.error(f"Failed to delete ontology class: {e}")
            raise
            
    async def handle_create_database(self, command_data: Dict[str, Any]) -> None:
        """데이터베이스 생성 처리"""
        payload = command_data.get('payload', {})
        db_name = payload.get('database_name')  # Fixed: was 'name', should be 'database_name'
        command_id = command_data.get('command_id')
        
        logger.info(f"Creating database: {db_name}")
        
        try:
            # TerminusDB에 데이터베이스 생성
            result = await self.terminus_service.create_database(
                db_name,
                payload.get('description', '')  # Fixed: only pass 2 args, no label
            )
            
            # 성공 이벤트 생성
            event = DatabaseEvent(
                event_type=EventType.DATABASE_CREATED,
                db_name=db_name,
                command_id=command_id,
                data={
                    "db_name": db_name,
                    "label": payload.get('label'),
                    "description": payload.get('description'),
                },
                occurred_by=command_data.get('created_by', 'system')
            )
            
            # 이벤트 발행
            await self.publish_event(event)
            logger.info(f"Successfully created database: {db_name}")
            
        except Exception as e:
            logger.error(f"Failed to create database: {e}")
            raise
            
    async def handle_delete_database(self, command_data: Dict[str, Any]) -> None:
        """데이터베이스 삭제 처리"""
        payload = command_data.get('payload', {})
        db_name = payload.get('database_name')  # Fixed: was 'name', should be 'database_name'
        command_id = command_data.get('command_id')
        
        logger.info(f"Deleting database: {db_name}")
        
        try:
            # TerminusDB에서 데이터베이스 삭제
            success = await self.terminus_service.delete_database(db_name)
            
            if not success:
                raise Exception(f"Database '{db_name}' not found")
            
            # 성공 이벤트 생성
            event = DatabaseEvent(
                event_type=EventType.DATABASE_DELETED,
                db_name=db_name,
                command_id=command_id,
                data={
                    "db_name": db_name
                },
                occurred_by=command_data.get('created_by', 'system')
            )
            
            # 이벤트 발행
            await self.publish_event(event)
            logger.info(f"Successfully deleted database: {db_name}")
            
        except Exception as e:
            logger.error(f"Failed to delete database: {e}")
            raise
            
    async def publish_event(self, event: BaseEvent) -> None:
        """이벤트 발행"""
        self.producer.produce(
            topic=AppConfig.ONTOLOGY_EVENTS_TOPIC,
            value=event.model_dump_json(),
            key=str(event.event_id).encode('utf-8')
        )
        self.producer.flush()
        
    async def publish_failure_event(self, command_data: Dict[str, Any], error: str) -> None:
        """실패 이벤트 발행"""
        event = CommandFailedEvent(
            aggregate_type=command_data.get('aggregate_type', 'Unknown'),
            aggregate_id=command_data.get('aggregate_id', 'Unknown'),
            command_id=command_data.get('command_id'),
            command_type=command_data.get('command_type', 'Unknown'),
            error_message=error,
            error_details={
                "command_data": command_data
            },
            retry_count=command_data.get('retry_count', 0)
        )
        
        await self.publish_event(event)
        
    async def run(self):
        """메인 실행 루프"""
        self.running = True
        
        logger.info("Ontology Worker started")
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue
                    
                try:
                    # 메시지 파싱
                    value = msg.value().decode('utf-8')
                    command_data = json.loads(value)
                    
                    logger.info(f"Processing command: {command_data.get('command_type')} "
                               f"for {command_data.get('aggregate_id')}")
                    
                    # Command 처리
                    await self.process_command(command_data)
                    
                    # 처리 성공 시 오프셋 커밋
                    self.consumer.commit(asynchronous=False)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse message: {e}")
                    # 파싱 실패해도 오프셋은 커밋 (무한 루프 방지)
                    self.consumer.commit(asynchronous=False)
                except Exception as e:
                    logger.error(f"Error processing command: {e}")
                    # 처리 실패 시 재시도를 위해 커밋하지 않음
                    # (Kafka consumer group이 재시작되면 다시 처리됨)
                    
        except KeyboardInterrupt:
            logger.info("Worker interrupted by user")
        finally:
            await self.shutdown()
            
    async def shutdown(self):
        """워커 종료"""
        logger.info("Shutting down Ontology Worker...")
        self.running = False
        
        if self.consumer:
            self.consumer.close()
            
        if self.terminus_service:
            await self.terminus_service.disconnect()
            
        if self.redis_service:
            await self.redis_service.disconnect()
            
        logger.info("Ontology Worker shut down successfully")


async def main():
    """메인 진입점"""
    worker = OntologyWorker()
    
    # 종료 시그널 핸들러
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}")
        worker.running = False
        
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await worker.initialize()
        await worker.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())