"""
Instance Worker Service
Instance Command를 처리하여 S3 저장 및 TerminusDB 작업을 수행하는 워커 서비스
"""

import asyncio
import hashlib
import json
import logging
import os
import signal
from datetime import datetime
from typing import Optional, Dict, Any
from uuid import uuid4

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
import asyncpg

from shared.config.service_config import ServiceConfig
from shared.config.app_config import AppConfig
from shared.models.commands import (
    BaseCommand, CommandType, CommandStatus, 
    InstanceCommand
)
from shared.models.events import (
    BaseEvent, EventType,
    InstanceEvent,
    CommandFailedEvent
)
from shared.models.config import ConnectionConfig
from oms.services.async_terminus import AsyncTerminusService
from shared.services.redis_service import RedisService, create_redis_service
from shared.services.command_status_service import CommandStatusService
from shared.services.storage_service import StorageService, create_storage_service
from shared.utils.id_generator import generate_instance_id

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class InstanceWorker:
    """인스턴스 Command를 처리하는 워커"""
    
    def __init__(self):
        self.running = False
        self.kafka_servers = ServiceConfig.get_kafka_bootstrap_servers()
        self.consumer: Optional[Consumer] = None
        self.producer: Optional[Producer] = None
        self.terminus_service: Optional[AsyncTerminusService] = None
        self.redis_service: Optional[RedisService] = None
        self.command_status_service: Optional[CommandStatusService] = None
        self.storage_service: Optional[StorageService] = None
        self.instance_bucket = AppConfig.INSTANCE_BUCKET
        
    async def initialize(self):
        """워커 초기화"""
        # Kafka Consumer 설정
        self.consumer = Consumer({
            'bootstrap.servers': self.kafka_servers,
            'group.id': 'instance-worker-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,  # 5분
            'session.timeout.ms': 45000,  # 45초
        })
        
        # Kafka Producer 설정 (Event 발행용)
        self.producer = Producer({
            'bootstrap.servers': self.kafka_servers,
            'client.id': 'instance-worker',
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
        self.redis_service = create_redis_service()
        await self.redis_service.connect()
        self.command_status_service = CommandStatusService(self.redis_service)
        logger.info("Redis connection established")
        
        # S3/MinIO 연결 설정
        self.storage_service = create_storage_service()
        # 버킷 생성 (없으면)
        await self.storage_service.create_bucket(self.instance_bucket)
        logger.info(f"Storage service initialized with bucket: {self.instance_bucket}")
        
        # Command 토픽 구독
        self.consumer.subscribe([AppConfig.INSTANCE_COMMANDS_TOPIC])
        logger.info("Instance Worker initialized successfully")
        
    async def process_command(self, command_data: Dict[str, Any]) -> None:
        """Command 처리"""
        command_id = command_data.get('command_id')
        
        try:
            # Redis에 처리 시작 상태 업데이트
            if command_id and self.command_status_service:
                await self.command_status_service.start_processing(
                    command_id=command_id,
                    worker_id=f"instance-worker-{os.getpid()}"
                )
            
            command_type = command_data.get('command_type')
            
            # Command 유형별 처리
            if command_type == CommandType.CREATE_INSTANCE:
                await self.handle_create_instance(command_data)
            elif command_type == CommandType.UPDATE_INSTANCE:
                await self.handle_update_instance(command_data)
            elif command_type == CommandType.DELETE_INSTANCE:
                await self.handle_delete_instance(command_data)
            elif command_type == CommandType.BULK_CREATE_INSTANCES:
                await self.handle_bulk_create_instances(command_data)
            elif command_type == CommandType.BULK_UPDATE_INSTANCES:
                await self.handle_bulk_update_instances(command_data)
            elif command_type == CommandType.BULK_DELETE_INSTANCES:
                await self.handle_bulk_delete_instances(command_data)
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
            
    async def handle_create_instance(self, command_data: Dict[str, Any]) -> None:
        """인스턴스 생성 처리"""
        db_name = command_data.get('db_name')
        class_id = command_data.get('class_id')
        payload = command_data.get('payload', {})
        command_id = command_data.get('command_id')
        metadata = command_data.get('metadata', {})
        
        # 인스턴스 ID 생성 (제공되지 않은 경우)
        instance_id = generate_instance_id(class_id)
        
        logger.info(f"Creating instance: {instance_id} of class: {class_id} in database: {db_name}")
        
        try:
            # 1. S3에 Command 전체 내용 저장
            s3_path = self.storage_service.generate_instance_path(
                db_name, class_id, instance_id, command_id
            )
            
            command_to_save = {
                **command_data,
                "instance_id": instance_id,
                "saved_at": datetime.utcnow().isoformat()
            }
            
            s3_checksum = await self.storage_service.save_json(
                bucket=self.instance_bucket,
                key=s3_path,
                data=command_to_save,
                metadata={
                    "command_type": CommandType.CREATE_INSTANCE,
                    "db_name": db_name,
                    "class_id": class_id,
                    "instance_id": instance_id
                }
            )
            
            logger.info(f"Saved command to S3: {s3_path} (checksum: {s3_checksum})")
            
            # 2. TerminusDB에 인스턴스 문서 생성
            instance_data = {
                "@id": instance_id,
                "@type": class_id,
                **payload,
                "_metadata": {
                    "created_at": datetime.utcnow().isoformat(),
                    "created_by": command_data.get('created_by', 'system'),
                    "command_id": command_id,
                    "s3_checksum": s3_checksum,
                    "version": 1
                }
            }
            
            # TerminusDB에 문서 생성
            result = await self.terminus_service.create_document(
                db_name=db_name,
                document_data=instance_data
            )
            
            # 3. 성공 이벤트 생성 (latest.json 제거 - 순수 append-only 유지)
            event = InstanceEvent(
                event_type=EventType.INSTANCE_CREATED,
                db_name=db_name,
                class_id=class_id,
                instance_id=instance_id,
                command_id=command_id,
                s3_path=s3_path,
                s3_checksum=s3_checksum,
                data={
                    "instance_id": instance_id,
                    "class_id": class_id,
                    "payload": payload,
                    "terminus_result": result
                },
                occurred_by=command_data.get('created_by', 'system')
            )
            
            # 5. 이벤트 발행
            await self.publish_event(event)
            
            # 6. Redis에 완료 상태 업데이트
            if command_id and self.command_status_service:
                await self.command_status_service.complete_command(
                    command_id=command_id,
                    result={
                        "instance_id": instance_id,
                        "class_id": class_id,
                        "message": f"Successfully created instance: {instance_id}",
                        "s3_path": s3_path,
                        "s3_checksum": s3_checksum,
                        "terminus_result": result
                    }
                )
            
            logger.info(f"Successfully created instance: {instance_id}")
            
        except Exception as e:
            logger.error(f"Failed to create instance: {e}")
            raise
            
    async def handle_update_instance(self, command_data: Dict[str, Any]) -> None:
        """인스턴스 수정 처리"""
        db_name = command_data.get('db_name')
        class_id = command_data.get('class_id')
        instance_id = command_data.get('instance_id')
        payload = command_data.get('payload', {})
        command_id = command_data.get('command_id')
        
        logger.info(f"Updating instance: {instance_id} of class: {class_id} in database: {db_name}")
        
        try:
            # 1. S3에 Command 저장
            s3_path = self.storage_service.generate_instance_path(
                db_name, class_id, instance_id, command_id
            )
            
            command_to_save = {
                **command_data,
                "saved_at": datetime.utcnow().isoformat()
            }
            
            s3_checksum = await self.storage_service.save_json(
                bucket=self.instance_bucket,
                key=s3_path,
                data=command_to_save,
                metadata={
                    "command_type": CommandType.UPDATE_INSTANCE,
                    "db_name": db_name,
                    "class_id": class_id,
                    "instance_id": instance_id
                }
            )
            
            # 2. TerminusDB에서 기존 문서 조회
            # TerminusDB document API를 직접 사용
            try:
                endpoint = f"/api/document/{self.terminus_service.connection_info.account}/{db_name}/{class_id}/{instance_id}"
                existing_doc = await self.terminus_service._make_request("GET", endpoint)
            except Exception as e:
                raise Exception(f"Instance '{instance_id}' not found: {e}")
            
            if not existing_doc:
                raise Exception(f"Instance '{instance_id}' not found")
            
            # 3. 문서 업데이트
            current_version = existing_doc.get("_metadata", {}).get("version", 1)
            updated_data = {
                **existing_doc,
                **payload,
                "@id": instance_id,  # ID는 변경 불가
                "@type": class_id,   # Type도 변경 불가
                "_metadata": {
                    **existing_doc.get("_metadata", {}),
                    "updated_at": datetime.utcnow().isoformat(),
                    "updated_by": command_data.get('created_by', 'system'),
                    "command_id": command_id,
                    "s3_checksum": s3_checksum,
                    "version": current_version + 1
                }
            }
            
            # TerminusDB 업데이트 - PUT 요청 사용
            update_endpoint = f"/api/document/{self.terminus_service.connection_info.account}/{db_name}"
            result = await self.terminus_service._make_request(
                "PUT",
                update_endpoint,
                data=updated_data
            )
            
            # 4. 성공 이벤트 생성 (latest.json 제거 - 순수 append-only 유지)
            event = InstanceEvent(
                event_type=EventType.INSTANCE_UPDATED,
                db_name=db_name,
                class_id=class_id,
                instance_id=instance_id,
                command_id=command_id,
                s3_path=s3_path,
                s3_checksum=s3_checksum,
                data={
                    "instance_id": instance_id,
                    "updates": payload,
                    "version": current_version + 1,
                    "terminus_result": result
                },
                occurred_by=command_data.get('created_by', 'system')
            )
            
            # 6. 이벤트 발행
            await self.publish_event(event)
            
            # 7. Redis에 완료 상태 업데이트
            if command_id and self.command_status_service:
                await self.command_status_service.complete_command(
                    command_id=command_id,
                    result={
                        "instance_id": instance_id,
                        "message": f"Successfully updated instance: {instance_id}",
                        "s3_path": s3_path,
                        "s3_checksum": s3_checksum,
                        "version": current_version + 1,
                        "terminus_result": result
                    }
                )
            
            logger.info(f"Successfully updated instance: {instance_id}")
            
        except Exception as e:
            logger.error(f"Failed to update instance: {e}")
            raise
            
    async def handle_delete_instance(self, command_data: Dict[str, Any]) -> None:
        """인스턴스 삭제 처리"""
        db_name = command_data.get('db_name')
        class_id = command_data.get('class_id')
        instance_id = command_data.get('instance_id')
        command_id = command_data.get('command_id')
        
        logger.info(f"Deleting instance: {instance_id} from database: {db_name}")
        
        try:
            # 1. S3에 삭제 Command 저장
            s3_path = self.storage_service.generate_instance_path(
                db_name, class_id, instance_id, command_id
            )
            
            command_to_save = {
                **command_data,
                "deleted_at": datetime.utcnow().isoformat()
            }
            
            s3_checksum = await self.storage_service.save_json(
                bucket=self.instance_bucket,
                key=s3_path,
                data=command_to_save,
                metadata={
                    "command_type": CommandType.DELETE_INSTANCE,
                    "db_name": db_name,
                    "class_id": class_id,
                    "instance_id": instance_id
                }
            )
            
            # 2. TerminusDB에서 문서 삭제
            # TerminusDB document API를 직접 사용
            try:
                delete_endpoint = f"/api/document/{self.terminus_service.connection_info.account}/{db_name}"
                delete_params = {
                    "graph_type": "instance",
                    "id": instance_id,
                    "author": self.terminus_service.connection_info.user,
                    "message": f"Delete instance {instance_id}"
                }
                result = await self.terminus_service._make_request(
                    "DELETE",
                    delete_endpoint,
                    params=delete_params
                )
                success = True
            except Exception as e:
                if "not found" in str(e).lower():
                    raise Exception(f"Instance '{instance_id}' not found")
                raise
            
            # 3. 삭제 Command도 append-only로 저장됨 (위의 S3 저장으로 충분)
            # 별도의 deletion marker는 불필요 - 모든 상태 변경은 Command로 추적
            
            # 4. 성공 이벤트 생성
            event = InstanceEvent(
                event_type=EventType.INSTANCE_DELETED,
                db_name=db_name,
                class_id=class_id,
                instance_id=instance_id,
                command_id=command_id,
                s3_path=s3_path,
                s3_checksum=s3_checksum,
                data={
                    "instance_id": instance_id,
                    "class_id": class_id
                },
                occurred_by=command_data.get('created_by', 'system')
            )
            
            # 5. 이벤트 발행
            await self.publish_event(event)
            
            # 6. Redis에 완료 상태 업데이트
            if command_id and self.command_status_service:
                await self.command_status_service.complete_command(
                    command_id=command_id,
                    result={
                        "instance_id": instance_id,
                        "message": f"Successfully deleted instance: {instance_id}",
                        "s3_path": s3_path,
                        "s3_checksum": s3_checksum
                    }
                )
            
            logger.info(f"Successfully deleted instance: {instance_id}")
            
        except Exception as e:
            logger.error(f"Failed to delete instance: {e}")
            raise
            
    async def handle_bulk_create_instances(self, command_data: Dict[str, Any]) -> None:
        """대량 인스턴스 생성 처리"""
        db_name = command_data.get('db_name')
        class_id = command_data.get('class_id')
        payload = command_data.get('payload', {})
        instances = payload.get('instances', [])
        command_id = command_data.get('command_id')
        
        logger.info(f"Creating {len(instances)} instances of class: {class_id} in database: {db_name}")
        
        created_instances = []
        failed_instances = []
        
        try:
            # S3에 Bulk Command 저장
            bulk_s3_path = f"{db_name}/{class_id}/bulk/{command_id}.json"
            bulk_s3_checksum = await self.storage_service.save_json(
                bucket=self.instance_bucket,
                key=bulk_s3_path,
                data=command_data
            )
            
            # 각 인스턴스 처리
            for idx, instance_data in enumerate(instances):
                instance_id = generate_instance_id(f"{class_id}_{idx}")
                
                try:
                    # 개별 인스턴스 생성 로직
                    instance_doc = {
                        "@id": instance_id,
                        "@type": class_id,
                        **instance_data,
                        "_metadata": {
                            "created_at": datetime.utcnow().isoformat(),
                            "created_by": command_data.get('created_by', 'system'),
                            "command_id": command_id,
                            "bulk_index": idx,
                            "version": 1
                        }
                    }
                    
                    # TerminusDB에 생성
                    result = await self.terminus_service.create_document(
                        db_name=db_name,
                        document=instance_doc
                    )
                    
                    created_instances.append({
                        "instance_id": instance_id,
                        "index": idx,
                        "result": result
                    })
                    
                except Exception as e:
                    logger.error(f"Failed to create instance at index {idx}: {e}")
                    failed_instances.append({
                        "index": idx,
                        "error": str(e)
                    })
            
            # 성공 이벤트 생성
            event = InstanceEvent(
                event_type=EventType.INSTANCES_BULK_CREATED,
                db_name=db_name,
                class_id=class_id,
                instance_id=f"bulk_{command_id}",  # Bulk operation ID
                command_id=command_id,
                s3_path=bulk_s3_path,
                s3_checksum=bulk_s3_checksum,
                data={
                    "total_count": len(instances),
                    "created_count": len(created_instances),
                    "failed_count": len(failed_instances),
                    "created_instances": created_instances,
                    "failed_instances": failed_instances
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
                        "message": f"Bulk created {len(created_instances)} instances",
                        "total_count": len(instances),
                        "created_count": len(created_instances),
                        "failed_count": len(failed_instances),
                        "s3_path": bulk_s3_path
                    }
                )
            
            logger.info(f"Bulk creation completed: {len(created_instances)} succeeded, {len(failed_instances)} failed")
            
        except Exception as e:
            logger.error(f"Failed to process bulk create: {e}")
            raise
    
    async def handle_bulk_update_instances(self, command_data: Dict[str, Any]) -> None:
        """대량 인스턴스 수정 처리"""
        # TODO: 구현 필요
        logger.warning("Bulk update instances not implemented yet")
        
    async def handle_bulk_delete_instances(self, command_data: Dict[str, Any]) -> None:
        """대량 인스턴스 삭제 처리"""
        # TODO: 구현 필요
        logger.warning("Bulk delete instances not implemented yet")
        
    async def publish_event(self, event: BaseEvent) -> None:
        """이벤트 발행"""
        self.producer.produce(
            topic=AppConfig.INSTANCE_EVENTS_TOPIC,
            value=event.json(),
            key=str(event.event_id).encode('utf-8')
        )
        self.producer.flush()
        
    async def publish_failure_event(self, command_data: Dict[str, Any], error: str) -> None:
        """실패 이벤트 발행"""
        event = CommandFailedEvent(
            aggregate_type=command_data.get('aggregate_type', 'Instance'),
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
        
        logger.info("Instance Worker started")
        
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
                    
        except KeyboardInterrupt:
            logger.info("Worker interrupted by user")
        finally:
            await self.shutdown()
            
    async def shutdown(self):
        """워커 종료"""
        logger.info("Shutting down Instance Worker...")
        self.running = False
        
        if self.consumer:
            self.consumer.close()
            
        if self.terminus_service:
            await self.terminus_service.disconnect()
            
        if self.redis_service:
            await self.redis_service.disconnect()
            
        logger.info("Instance Worker shut down successfully")


async def main():
    """메인 진입점"""
    worker = InstanceWorker()
    
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