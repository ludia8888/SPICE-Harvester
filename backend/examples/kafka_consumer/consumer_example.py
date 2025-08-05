"""
Kafka Consumer Example
온톨로지 이벤트를 구독하여 처리하는 예제
"""

import asyncio
import json
import logging
import signal
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient

from shared.config.service_config import ServiceConfig

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class OntologyEventConsumer:
    """온톨로지 이벤트 컨슈머 예제"""
    
    def __init__(self, consumer_group: str = "ontology-consumer-group"):
        self.consumer_group = consumer_group
        self.kafka_servers = ServiceConfig.get_kafka_bootstrap_servers()
        self.consumer: Optional[Consumer] = None
        self.running = False
        
    def initialize(self):
        """컨슈머 초기화"""
        self.consumer = Consumer({
            'bootstrap.servers': self.kafka_servers,
            'group.id': self.consumer_group,
            'auto.offset.reset': 'earliest',  # 처음부터 읽기
            'enable.auto.commit': False,  # 수동 커밋
            'max.poll.interval.ms': 300000,  # 5분
            'session.timeout.ms': 45000,  # 45초
        })
        
        # 토픽 구독
        topics = ['ontology_events']
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {topics}")
        
    def process_event(self, event: dict):
        """이벤트 처리 로직"""
        event_type = event.get('event_type')
        aggregate_id = event.get('aggregate_id')
        data = event.get('data', {})
        
        logger.info(f"Processing event: {event_type} for {aggregate_id}")
        
        # 이벤트 타입별 처리
        if event_type == 'ONTOLOGY_CLASS_CREATED':
            self.handle_class_created(aggregate_id, data)
        elif event_type == 'ONTOLOGY_CLASS_UPDATED':
            self.handle_class_updated(aggregate_id, data)
        elif event_type == 'ONTOLOGY_CLASS_DELETED':
            self.handle_class_deleted(aggregate_id, data)
        else:
            logger.warning(f"Unknown event type: {event_type}")
            
    def handle_class_created(self, class_id: str, data: dict):
        """온톨로지 클래스 생성 이벤트 처리"""
        logger.info(f"Ontology class created: {class_id}")
        logger.info(f"  Label: {data.get('label')}")
        logger.info(f"  Description: {data.get('description')}")
        logger.info(f"  Properties: {len(data.get('properties', []))}")
        
        # 여기에 실제 비즈니스 로직 구현
        # 예: ElasticSearch 인덱싱, 캐시 갱신, 다른 서비스 호출 등
        
    def handle_class_updated(self, class_id: str, data: dict):
        """온톨로지 클래스 업데이트 이벤트 처리"""
        logger.info(f"Ontology class updated: {class_id}")
        logger.info(f"  Updates: {data.get('updates')}")
        
        # 여기에 실제 비즈니스 로직 구현
        
    def handle_class_deleted(self, class_id: str, data: dict):
        """온톨로지 클래스 삭제 이벤트 처리"""
        logger.info(f"Ontology class deleted: {class_id}")
        
        # 여기에 실제 비즈니스 로직 구현
        
    def run(self):
        """메인 실행 루프"""
        self.running = True
        
        logger.info(f"Consumer started. Group: {self.consumer_group}")
        
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
                    event = json.loads(value)
                    
                    logger.debug(f"Received message from partition {msg.partition()}, offset {msg.offset()}")
                    
                    # 이벤트 처리
                    self.process_event(event)
                    
                    # 처리 성공 시 오프셋 커밋
                    self.consumer.commit(asynchronous=False)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse message: {e}")
                    # 파싱 실패해도 오프셋은 커밋 (무한 루프 방지)
                    self.consumer.commit(asynchronous=False)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    # 처리 실패 시 재시도 로직 구현 가능
                    
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        finally:
            self.shutdown()
            
    def shutdown(self):
        """컨슈머 종료"""
        logger.info("Shutting down consumer...")
        self.running = False
        
        if self.consumer:
            self.consumer.close()
            
        logger.info("Consumer shut down successfully")


def main():
    """메인 진입점"""
    consumer = OntologyEventConsumer()
    
    # 종료 시그널 핸들러
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}")
        consumer.running = False
        
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        consumer.initialize()
        consumer.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()