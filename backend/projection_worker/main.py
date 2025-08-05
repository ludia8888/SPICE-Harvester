"""
Projection Worker Service
Instance와 Ontology 이벤트를 Elasticsearch에 프로젝션하는 워커 서비스
"""

import asyncio
import json
import logging
import os
import signal
from datetime import datetime
from typing import Optional, Dict, Any, List
from uuid import uuid4

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

from shared.config.service_config import ServiceConfig
from shared.models.events import (
    BaseEvent, EventType,
    InstanceEvent,
    OntologyEvent
)
from shared.services import (
    RedisService, create_redis_service,
    ElasticsearchService, create_elasticsearch_service
)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ProjectionWorker:
    """Instance와 Ontology 이벤트를 Elasticsearch에 프로젝션하는 워커"""
    
    def __init__(self):
        self.running = False
        self.kafka_servers = ServiceConfig.get_kafka_bootstrap_servers()
        self.consumer: Optional[Consumer] = None
        self.producer: Optional[Producer] = None
        self.redis_service: Optional[RedisService] = None
        self.elasticsearch_service: Optional[ElasticsearchService] = None
        
        # 인덱스 이름
        self.instances_index = "instances"
        self.ontologies_index = "ontologies"
        
        # DLQ 토픽
        self.dlq_topic = "projection_failures_dlq"
        
        # 재시도 설정
        self.max_retries = 5
        self.retry_count = {}
        
    async def initialize(self):
        """워커 초기화"""
        # Kafka Consumer 설정 (멀티 토픽 구독)
        self.consumer = Consumer({
            'bootstrap.servers': self.kafka_servers,
            'group.id': 'projection-worker-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,  # 5분
            'session.timeout.ms': 45000,  # 45초
        })
        
        # Kafka Producer 설정 (실패 이벤트 발행용)
        self.producer = Producer({
            'bootstrap.servers': self.kafka_servers,
            'client.id': 'projection-worker',
            'acks': 'all',
            'retries': 3,
            'compression.type': 'snappy',
        })
        
        # Redis 연결 설정 (온톨로지 캐싱용)
        self.redis_service = create_redis_service()
        await self.redis_service.connect()
        logger.info("Redis connection established")
        
        # Elasticsearch 연결 설정
        self.elasticsearch_service = create_elasticsearch_service()
        await self.elasticsearch_service.connect()
        logger.info("Elasticsearch connection established")
        
        # 인덱스 생성 및 매핑 설정
        await self._setup_indices()
        
        # 토픽 구독
        topics = ['instance_events', 'ontology_events']
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {topics}")
        
    async def _setup_indices(self):
        """Elasticsearch 인덱스 생성 및 매핑 설정"""
        try:
            # instances 인덱스 설정
            instances_mapping = await self._load_mapping('instances_mapping.json')
            if not await self.elasticsearch_service.index_exists(self.instances_index):
                await self.elasticsearch_service.create_index(
                    self.instances_index,
                    mappings=instances_mapping['mappings'],
                    settings=instances_mapping['settings']
                )
                logger.info(f"Created index: {self.instances_index}")
            
            # ontologies 인덱스 설정
            ontologies_mapping = await self._load_mapping('ontologies_mapping.json')
            if not await self.elasticsearch_service.index_exists(self.ontologies_index):
                await self.elasticsearch_service.create_index(
                    self.ontologies_index,
                    mappings=ontologies_mapping['mappings'],
                    settings=ontologies_mapping['settings']
                )
                logger.info(f"Created index: {self.ontologies_index}")
                
        except Exception as e:
            logger.error(f"Failed to setup indices: {e}")
            raise
            
    async def _load_mapping(self, filename: str) -> Dict[str, Any]:
        """매핑 파일 로드"""
        mapping_path = os.path.join(
            os.path.dirname(__file__), 
            "mappings", 
            filename
        )
        try:
            with open(mapping_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load mapping {filename}: {e}")
            raise
            
    async def run(self):
        """메인 실행 루프"""
        self.running = True
        logger.info("Projection Worker started")
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        continue
                        
                try:
                    # 이벤트 처리
                    await self._process_event(msg)
                    # 성공 시 오프셋 커밋
                    self.consumer.commit(msg)
                    
                except Exception as e:
                    logger.error(f"Failed to process event: {e}")
                    # 재시도 로직
                    await self._handle_retry(msg, e)
                    
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
        finally:
            await self._shutdown()
            
    async def _process_event(self, msg):
        """이벤트 처리"""
        try:
            event_data = json.loads(msg.value().decode('utf-8'))
            event_type = event_data.get('event_type')
            topic = msg.topic()
            
            logger.info(f"Processing event: {event_type} from topic: {topic}")
            
            if topic == 'instance_events':
                await self._handle_instance_event(event_data)
            elif topic == 'ontology_events':
                await self._handle_ontology_event(event_data)
            else:
                logger.warning(f"Unknown topic: {topic}")
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse event JSON: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing event: {e}")
            raise
            
    async def _handle_instance_event(self, event_data: Dict[str, Any]):
        """인스턴스 이벤트 처리"""
        try:
            event_type = event_data.get('event_type')
            instance_data = event_data.get('data', {})
            event_id = event_data.get('event_id')
            
            if event_type == EventType.INSTANCE_CREATED.value:
                await self._handle_instance_created(instance_data, event_id, event_data)
            elif event_type == EventType.INSTANCE_UPDATED.value:
                await self._handle_instance_updated(instance_data, event_id, event_data)
            elif event_type == EventType.INSTANCE_DELETED.value:
                await self._handle_instance_deleted(instance_data, event_id, event_data)
            else:
                logger.warning(f"Unknown instance event type: {event_type}")
                
        except Exception as e:
            logger.error(f"Error handling instance event: {e}")
            raise
            
    async def _handle_ontology_event(self, event_data: Dict[str, Any]):
        """온톨로지 이벤트 처리"""
        try:
            event_type = event_data.get('event_type')
            ontology_data = event_data.get('data', {})
            event_id = event_data.get('event_id')
            
            if event_type == EventType.ONTOLOGY_CLASS_CREATED.value:
                await self._handle_ontology_class_created(ontology_data, event_id, event_data)
            elif event_type == EventType.ONTOLOGY_CLASS_UPDATED.value:
                await self._handle_ontology_class_updated(ontology_data, event_id, event_data)
            elif event_type == EventType.ONTOLOGY_CLASS_DELETED.value:
                await self._handle_ontology_class_deleted(ontology_data, event_id, event_data)
            else:
                logger.warning(f"Unknown ontology event type: {event_type}")
                
        except Exception as e:
            logger.error(f"Error handling ontology event: {e}")
            raise
            
    async def _handle_instance_created(self, instance_data: Dict[str, Any], event_id: str, event_data: Dict[str, Any]):
        """인스턴스 생성 이벤트 처리"""
        try:
            # 클래스 라벨 조회 (Redis 캐시 활용)
            class_label = await self._get_class_label(instance_data.get('class_id'))
            
            # Elasticsearch 문서 구성
            doc = {
                'instance_id': instance_data.get('instance_id'),
                'class_id': instance_data.get('class_id'),
                'class_label': class_label,
                'properties': self._normalize_properties(instance_data.get('properties', [])),
                'data': instance_data,  # 원본 데이터 (enabled: false)
                'event_id': event_id,
                'event_timestamp': event_data.get('timestamp'),
                'version': 1,
                'db_name': instance_data.get('db_name'),
                'branch': instance_data.get('branch'),
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }
            
            # 멱등성을 위해 event_id를 문서 ID로 사용
            await self.elasticsearch_service.index_document(
                self.instances_index,
                doc,
                doc_id=event_id,
                refresh=True
            )
            
            logger.info(f"Instance created in Elasticsearch: {instance_data.get('instance_id')}")
            
        except Exception as e:
            logger.error(f"Failed to handle instance created: {e}")
            raise
            
    async def _handle_instance_updated(self, instance_data: Dict[str, Any], event_id: str, event_data: Dict[str, Any]):
        """인스턴스 업데이트 이벤트 처리"""
        try:
            # 클래스 라벨 조회
            class_label = await self._get_class_label(instance_data.get('class_id'))
            
            # 기존 문서 조회
            existing_doc = await self.elasticsearch_service.get_document(
                self.instances_index,
                instance_data.get('instance_id')
            )
            
            version = 1
            if existing_doc:
                version = existing_doc.get('version', 0) + 1
            
            # 업데이트 문서 구성
            doc = {
                'instance_id': instance_data.get('instance_id'),
                'class_id': instance_data.get('class_id'),
                'class_label': class_label,
                'properties': self._normalize_properties(instance_data.get('properties', [])),
                'data': instance_data,
                'event_id': event_id,
                'event_timestamp': event_data.get('timestamp'),
                'version': version,
                'db_name': instance_data.get('db_name'),
                'branch': instance_data.get('branch'),
                'updated_at': datetime.utcnow().isoformat()
            }
            
            # 문서 업데이트
            await self.elasticsearch_service.update_document(
                self.instances_index,
                instance_data.get('instance_id'),
                doc=doc,
                upsert=doc,
                refresh=True
            )
            
            logger.info(f"Instance updated in Elasticsearch: {instance_data.get('instance_id')}")
            
        except Exception as e:
            logger.error(f"Failed to handle instance updated: {e}")
            raise
            
    async def _handle_instance_deleted(self, instance_data: Dict[str, Any], event_id: str, event_data: Dict[str, Any]):
        """인스턴스 삭제 이벤트 처리"""
        try:
            instance_id = instance_data.get('instance_id')
            
            # 문서 삭제
            success = await self.elasticsearch_service.delete_document(
                self.instances_index,
                instance_id,
                refresh=True
            )
            
            if success:
                logger.info(f"Instance deleted from Elasticsearch: {instance_id}")
            else:
                logger.warning(f"Instance not found for deletion: {instance_id}")
                
        except Exception as e:
            logger.error(f"Failed to handle instance deleted: {e}")
            raise
            
    async def _handle_ontology_class_created(self, ontology_data: Dict[str, Any], event_id: str, event_data: Dict[str, Any]):
        """온톨로지 클래스 생성 이벤트 처리"""
        try:
            # Elasticsearch 문서 구성
            doc = {
                'class_id': ontology_data.get('id'),
                'label': ontology_data.get('label'),
                'description': ontology_data.get('description'),
                'properties': ontology_data.get('properties', []),
                'relationships': ontology_data.get('relationships', []),
                'parent_classes': ontology_data.get('parent_classes', []),
                'child_classes': ontology_data.get('child_classes', []),
                'db_name': ontology_data.get('db_name'),
                'branch': ontology_data.get('branch'),
                'version': 1,
                'event_id': event_id,
                'event_timestamp': event_data.get('timestamp'),
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }
            
            # 인덱싱
            await self.elasticsearch_service.index_document(
                self.ontologies_index,
                doc,
                doc_id=ontology_data.get('id'),
                refresh=True
            )
            
            # Redis에 클래스 라벨 캐싱
            await self._cache_class_label(
                ontology_data.get('id'),
                ontology_data.get('label')
            )
            
            logger.info(f"Ontology class created in Elasticsearch: {ontology_data.get('id')}")
            
        except Exception as e:
            logger.error(f"Failed to handle ontology class created: {e}")
            raise
            
    async def _handle_ontology_class_updated(self, ontology_data: Dict[str, Any], event_id: str, event_data: Dict[str, Any]):
        """온톨로지 클래스 업데이트 이벤트 처리"""
        try:
            # 기존 문서 조회
            existing_doc = await self.elasticsearch_service.get_document(
                self.ontologies_index,
                ontology_data.get('id')
            )
            
            version = 1
            if existing_doc:
                version = existing_doc.get('version', 0) + 1
            
            # 업데이트 문서 구성
            doc = {
                'class_id': ontology_data.get('id'),
                'label': ontology_data.get('label'),
                'description': ontology_data.get('description'),
                'properties': ontology_data.get('properties', []),
                'relationships': ontology_data.get('relationships', []),
                'parent_classes': ontology_data.get('parent_classes', []),
                'child_classes': ontology_data.get('child_classes', []),
                'db_name': ontology_data.get('db_name'),
                'branch': ontology_data.get('branch'),
                'version': version,
                'event_id': event_id,
                'event_timestamp': event_data.get('timestamp'),
                'updated_at': datetime.utcnow().isoformat()
            }
            
            # 문서 업데이트
            await self.elasticsearch_service.update_document(
                self.ontologies_index,
                ontology_data.get('id'),
                doc=doc,
                upsert=doc,
                refresh=True
            )
            
            # Redis 캐시 업데이트
            await self._cache_class_label(
                ontology_data.get('id'),
                ontology_data.get('label')
            )
            
            logger.info(f"Ontology class updated in Elasticsearch: {ontology_data.get('id')}")
            
        except Exception as e:
            logger.error(f"Failed to handle ontology class updated: {e}")
            raise
            
    async def _handle_ontology_class_deleted(self, ontology_data: Dict[str, Any], event_id: str, event_data: Dict[str, Any]):
        """온톨로지 클래스 삭제 이벤트 처리"""
        try:
            class_id = ontology_data.get('id')
            
            # 문서 삭제
            success = await self.elasticsearch_service.delete_document(
                self.ontologies_index,
                class_id,
                refresh=True
            )
            
            # Redis 캐시 삭제
            await self.redis_service.delete(f"class_label:{class_id}")
            
            if success:
                logger.info(f"Ontology class deleted from Elasticsearch: {class_id}")
            else:
                logger.warning(f"Ontology class not found for deletion: {class_id}")
                
        except Exception as e:
            logger.error(f"Failed to handle ontology class deleted: {e}")
            raise
            
    async def _get_class_label(self, class_id: str) -> Optional[str]:
        """Redis에서 클래스 라벨 조회"""
        try:
            if not class_id:
                return None
                
            cached_label = await self.redis_service.client.get(f"class_label:{class_id}")
            if cached_label:
                return cached_label
                
            # 캐시에 없으면 Elasticsearch에서 조회
            doc = await self.elasticsearch_service.get_document(
                self.ontologies_index,
                class_id
            )
            
            if doc:
                label = doc.get('label')
                if label:
                    # 캐시에 저장 (1시간 TTL)
                    await self.redis_service.client.setex(
                        f"class_label:{class_id}",
                        3600,
                        label
                    )
                    return label
                    
            return None
            
        except Exception as e:
            logger.error(f"Failed to get class label for {class_id}: {e}")
            return None
            
    async def _cache_class_label(self, class_id: str, label: str):
        """클래스 라벨을 Redis에 캐싱"""
        try:
            await self.redis_service.client.setex(
                f"class_label:{class_id}",
                3600,  # 1시간 TTL
                label
            )
        except Exception as e:
            logger.error(f"Failed to cache class label: {e}")
            
    def _normalize_properties(self, properties: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """속성을 검색 최적화된 형태로 정규화"""
        normalized = []
        for prop in properties:
            normalized.append({
                'name': prop.get('name'),
                'value': str(prop.get('value', '')),
                'type': prop.get('type')
            })
        return normalized
        
    async def _handle_retry(self, msg, error):
        """재시도 처리"""
        try:
            key = f"{msg.topic()}:{msg.partition()}:{msg.offset()}"
            retry_count = self.retry_count.get(key, 0) + 1
            
            if retry_count <= self.max_retries:
                self.retry_count[key] = retry_count
                logger.warning(f"Retrying message (attempt {retry_count}/{self.max_retries}): {key}")
                await asyncio.sleep(retry_count * 2)  # 지수 백오프
                return
                
            # 최대 재시도 횟수 초과 시 DLQ로 전송
            logger.error(f"Max retries exceeded for message: {key}, sending to DLQ")
            await self._send_to_dlq(msg, error)
            
            # 재시도 카운트 제거
            if key in self.retry_count:
                del self.retry_count[key]
                
            # 오프셋 커밋 (DLQ 전송 후)
            self.consumer.commit(msg)
            
        except Exception as e:
            logger.error(f"Error in retry handling: {e}")
            
    async def _send_to_dlq(self, msg, error):
        """실패한 메시지를 DLQ로 전송"""
        try:
            dlq_message = {
                'original_topic': msg.topic(),
                'original_partition': msg.partition(),
                'original_offset': msg.offset(),
                'original_value': msg.value().decode('utf-8'),
                'error': str(error),
                'timestamp': datetime.utcnow().isoformat(),
                'worker': 'projection-worker'
            }
            
            self.producer.produce(
                self.dlq_topic,
                key=f"{msg.topic()}:{msg.partition()}:{msg.offset()}",
                value=json.dumps(dlq_message)
            )
            self.producer.flush()
            
            logger.info(f"Message sent to DLQ: {dlq_message}")
            
        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {e}")
            
    async def _shutdown(self):
        """워커 종료"""
        logger.info("Shutting down Projection Worker...")
        
        self.running = False
        
        if self.consumer:
            self.consumer.close()
            
        if self.producer:
            self.producer.flush()
            
        if self.elasticsearch_service:
            await self.elasticsearch_service.disconnect()
            
        if self.redis_service:
            await self.redis_service.disconnect()
            
        logger.info("Projection Worker stopped")


async def main():
    """메인 함수"""
    worker = ProjectionWorker()
    
    # 시그널 핸들러 설정
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}")
        worker.running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await worker.initialize()
        await worker.run()
    except Exception as e:
        logger.error(f"Worker failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())