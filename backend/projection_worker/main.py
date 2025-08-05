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
from shared.config.search_config import (
    get_instances_index_name,
    get_ontologies_index_name,
    DEFAULT_INDEX_SETTINGS
)
from shared.config.app_config import AppConfig
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
        
        # 생성된 인덱스 캐시 (중복 생성 방지)
        self.created_indices = set()
        
        # DLQ 토픽
        self.dlq_topic = AppConfig.PROJECTION_DLQ_TOPIC
        
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
        topics = [AppConfig.INSTANCE_EVENTS_TOPIC, AppConfig.ONTOLOGY_EVENTS_TOPIC]
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {topics}")
        
    async def _setup_indices(self):
        """매핑 파일 로드 (인덱스는 DB별로 동적 생성)"""
        try:
            # 매핑 파일만 미리 로드
            self.instances_mapping = await self._load_mapping('instances_mapping.json')
            self.ontologies_mapping = await self._load_mapping('ontologies_mapping.json')
            logger.info("Loaded index mappings successfully")
                
        except Exception as e:
            logger.error(f"Failed to load mappings: {e}")
            raise
            
    async def _ensure_index_exists(self, db_name: str, index_type: str = "instances"):
        """특정 데이터베이스의 인덱스가 존재하는지 확인하고 없으면 생성"""
        if index_type == "instances":
            index_name = get_instances_index_name(db_name)
            mapping = self.instances_mapping
        else:
            index_name = get_ontologies_index_name(db_name)
            mapping = self.ontologies_mapping
            
        # 이미 생성된 인덱스는 스킵
        if index_name in self.created_indices:
            return index_name
            
        try:
            if not await self.elasticsearch_service.index_exists(index_name):
                # 설정 병합 (매핑 파일 설정 + 기본 설정)
                settings = mapping.get('settings', {}).copy()
                settings.update(DEFAULT_INDEX_SETTINGS)
                
                await self.elasticsearch_service.create_index(
                    index_name,
                    mappings=mapping['mappings'],
                    settings=settings
                )
                logger.info(f"Created index: {index_name} for database: {db_name}")
                
            self.created_indices.add(index_name)
            return index_name
            
        except Exception as e:
            logger.error(f"Failed to ensure index exists for {db_name}: {e}")
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
            
            if topic == AppConfig.INSTANCE_EVENTS_TOPIC:
                await self._handle_instance_event(event_data)
            elif topic == AppConfig.ONTOLOGY_EVENTS_TOPIC:
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
            # 데이터베이스 이름 추출
            db_name = event_data.get('db_name') or instance_data.get('db_name')
            if not db_name:
                raise ValueError("db_name is required for instance creation")
                
            # 인덱스 확인 및 생성
            index_name = await self._ensure_index_exists(db_name, "instances")
            
            # 클래스 라벨 조회 (Redis 캐시 활용)
            class_label = await self._get_class_label(instance_data.get('class_id'), db_name)
            
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
                'db_name': db_name,
                'branch': instance_data.get('branch'),
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }
            
            # 멱등성을 위해 event_id를 문서 ID로 사용
            await self.elasticsearch_service.index_document(
                index_name,
                doc,
                doc_id=event_id,
                refresh=True
            )
            
            logger.info(f"Instance created in Elasticsearch: {instance_data.get('instance_id')} in index: {index_name}")
            
        except Exception as e:
            logger.error(f"Failed to handle instance created: {e}")
            raise
            
    async def _handle_instance_updated(self, instance_data: Dict[str, Any], event_id: str, event_data: Dict[str, Any]):
        """인스턴스 업데이트 이벤트 처리"""
        try:
            # 데이터베이스 이름 추출
            db_name = event_data.get('db_name') or instance_data.get('db_name')
            if not db_name:
                raise ValueError("db_name is required for instance update")
                
            # 인덱스 확인 및 생성
            index_name = await self._ensure_index_exists(db_name, "instances")
            
            # 클래스 라벨 조회
            class_label = await self._get_class_label(instance_data.get('class_id'), db_name)
            
            # 기존 문서 조회
            existing_doc = await self.elasticsearch_service.get_document(
                index_name,
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
                'db_name': db_name,
                'branch': instance_data.get('branch'),
                'updated_at': datetime.utcnow().isoformat()
            }
            
            # 문서 업데이트
            await self.elasticsearch_service.update_document(
                index_name,
                instance_data.get('instance_id'),
                doc=doc,
                upsert=doc,
                refresh=True
            )
            
            logger.info(f"Instance updated in Elasticsearch: {instance_data.get('instance_id')} in index: {index_name}")
            
        except Exception as e:
            logger.error(f"Failed to handle instance updated: {e}")
            raise
            
    async def _handle_instance_deleted(self, instance_data: Dict[str, Any], event_id: str, event_data: Dict[str, Any]):
        """인스턴스 삭제 이벤트 처리"""
        try:
            # 데이터베이스 이름 추출
            db_name = event_data.get('db_name') or instance_data.get('db_name')
            if not db_name:
                raise ValueError("db_name is required for instance deletion")
                
            # 인덱스 이름 결정
            index_name = get_instances_index_name(db_name)
            instance_id = instance_data.get('instance_id')
            
            # 문서 삭제
            success = await self.elasticsearch_service.delete_document(
                index_name,
                instance_id,
                refresh=True
            )
            
            if success:
                logger.info(f"Instance deleted from Elasticsearch: {instance_id} from index: {index_name}")
            else:
                logger.warning(f"Instance not found for deletion: {instance_id} in index: {index_name}")
                
        except Exception as e:
            logger.error(f"Failed to handle instance deleted: {e}")
            raise
            
    async def _handle_ontology_class_created(self, ontology_data: Dict[str, Any], event_id: str, event_data: Dict[str, Any]):
        """온톨로지 클래스 생성 이벤트 처리"""
        try:
            # 데이터베이스 이름 추출
            db_name = event_data.get('db_name') or ontology_data.get('db_name')
            if not db_name:
                raise ValueError("db_name is required for ontology class creation")
                
            # 인덱스 확인 및 생성
            index_name = await self._ensure_index_exists(db_name, "ontologies")
            
            # Elasticsearch 문서 구성
            doc = {
                'class_id': ontology_data.get('id'),
                'label': ontology_data.get('label'),
                'description': ontology_data.get('description'),
                'properties': ontology_data.get('properties', []),
                'relationships': ontology_data.get('relationships', []),
                'parent_classes': ontology_data.get('parent_classes', []),
                'child_classes': ontology_data.get('child_classes', []),
                'db_name': db_name,
                'branch': ontology_data.get('branch'),
                'version': 1,
                'event_id': event_id,
                'event_timestamp': event_data.get('timestamp'),
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }
            
            # 인덱싱
            await self.elasticsearch_service.index_document(
                index_name,
                doc,
                doc_id=ontology_data.get('id'),
                refresh=True
            )
            
            # Redis에 클래스 라벨 캐싱 (DB별로 키 구분)
            await self._cache_class_label(
                ontology_data.get('id'),
                ontology_data.get('label'),
                db_name
            )
            
            logger.info(f"Ontology class created in Elasticsearch: {ontology_data.get('id')} in index: {index_name}")
            
        except Exception as e:
            logger.error(f"Failed to handle ontology class created: {e}")
            raise
            
    async def _handle_ontology_class_updated(self, ontology_data: Dict[str, Any], event_id: str, event_data: Dict[str, Any]):
        """온톨로지 클래스 업데이트 이벤트 처리"""
        try:
            # 데이터베이스 이름 추출
            db_name = event_data.get('db_name') or ontology_data.get('db_name')
            if not db_name:
                raise ValueError("db_name is required for ontology class update")
                
            # 인덱스 확인 및 생성
            index_name = await self._ensure_index_exists(db_name, "ontologies")
            
            # 기존 문서 조회
            existing_doc = await self.elasticsearch_service.get_document(
                index_name,
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
                'db_name': db_name,
                'branch': ontology_data.get('branch'),
                'version': version,
                'event_id': event_id,
                'event_timestamp': event_data.get('timestamp'),
                'updated_at': datetime.utcnow().isoformat()
            }
            
            # 문서 업데이트
            await self.elasticsearch_service.update_document(
                index_name,
                ontology_data.get('id'),
                doc=doc,
                upsert=doc,
                refresh=True
            )
            
            # Redis 캐시 업데이트 (DB별로 키 구분)
            await self._cache_class_label(
                ontology_data.get('id'),
                ontology_data.get('label'),
                db_name
            )
            
            logger.info(f"Ontology class updated in Elasticsearch: {ontology_data.get('id')} in index: {index_name}")
            
        except Exception as e:
            logger.error(f"Failed to handle ontology class updated: {e}")
            raise
            
    async def _handle_ontology_class_deleted(self, ontology_data: Dict[str, Any], event_id: str, event_data: Dict[str, Any]):
        """온톨로지 클래스 삭제 이벤트 처리"""
        try:
            # 데이터베이스 이름 추출
            db_name = event_data.get('db_name') or ontology_data.get('db_name')
            if not db_name:
                raise ValueError("db_name is required for ontology class deletion")
                
            # 인덱스 이름 결정
            index_name = get_ontologies_index_name(db_name)
            class_id = ontology_data.get('id')
            
            # 문서 삭제
            success = await self.elasticsearch_service.delete_document(
                index_name,
                class_id,
                refresh=True
            )
            
            # Redis 캐시 삭제 (DB별로 키 구분)
            await self.redis_service.delete(AppConfig.get_class_label_key(db_name, class_id))
            
            if success:
                logger.info(f"Ontology class deleted from Elasticsearch: {class_id} from index: {index_name}")
            else:
                logger.warning(f"Ontology class not found for deletion: {class_id} in index: {index_name}")
                
        except Exception as e:
            logger.error(f"Failed to handle ontology class deleted: {e}")
            raise
            
    async def _get_class_label(self, class_id: str, db_name: str) -> Optional[str]:
        """Redis에서 클래스 라벨 조회"""
        try:
            if not class_id or not db_name:
                return None
                
            cache_key = AppConfig.get_class_label_key(db_name, class_id)
            cached_label = await self.redis_service.client.get(cache_key)
            if cached_label:
                return cached_label
                
            # 캐시에 없으면 Elasticsearch에서 조회
            index_name = get_ontologies_index_name(db_name)
            doc = await self.elasticsearch_service.get_document(
                index_name,
                class_id
            )
            
            if doc:
                label = doc.get('label')
                if label:
                    # 캐시에 저장 (1시간 TTL)
                    await self.redis_service.client.setex(
                        cache_key,
                        3600,
                        label
                    )
                    return label
                    
            return None
            
        except Exception as e:
            logger.error(f"Failed to get class label for {class_id} in {db_name}: {e}")
            return None
            
    async def _cache_class_label(self, class_id: str, label: str, db_name: str):
        """클래스 라벨을 Redis에 캐싱"""
        try:
            if not class_id or not label or not db_name:
                return
                
            cache_key = AppConfig.get_class_label_key(db_name, class_id)
            await self.redis_service.client.setex(
                cache_key,
                3600,  # 1시간 TTL
                label
            )
        except Exception as e:
            logger.error(f"Failed to cache class label for {class_id} in {db_name}: {e}")
            
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