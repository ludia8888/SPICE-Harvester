#!/usr/bin/env python3
"""
Kafka 토픽의 메시지 확인
THINK ULTRA - 실제 메시지가 Kafka에 도달했는지 검증
"""

import os
import sys
import json
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient

# 환경 변수
os.environ["KAFKA_HOST"] = "127.0.0.1"  # IPv4 명시
os.environ["KAFKA_PORT"] = "9092"

sys.path.insert(0, "/Users/isihyeon/Desktop/SPICE HARVESTER/backend")

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_kafka_topics():
    """Kafka 토픽 및 메시지 확인"""
    logger.info("=" * 60)
    logger.info("🔍 Kafka 토픽 확인")
    logger.info("=" * 60)
    
    # AdminClient로 토픽 목록 확인
    admin_client = AdminClient({
        'bootstrap.servers': '127.0.0.1:9092'  # IPv4 명시
    })
    
    try:
        # 토픽 목록 가져오기
        metadata = admin_client.list_topics(timeout=5)
        
        logger.info(f"\n📋 Kafka 토픽 목록:")
        topics_to_check = []
        for topic in metadata.topics:
            if not topic.startswith('_'):  # 내부 토픽 제외
                partitions = metadata.topics[topic].partitions
                logger.info(f"   - {topic} (파티션: {len(partitions)}개)")
                topics_to_check.append(topic)
        
        # 각 토픽의 메시지 확인
        for topic_name in topics_to_check:
            if 'command' in topic_name.lower() or 'event' in topic_name.lower():
                logger.info(f"\n📌 토픽 '{topic_name}' 메시지 확인:")
                check_topic_messages(topic_name)
                
    except Exception as e:
        logger.error(f"❌ Kafka 토픽 확인 실패: {e}")


def check_topic_messages(topic_name):
    """특정 토픽의 메시지 확인"""
    consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': f'check-messages-{topic_name}',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'session.timeout.ms': 6000,
        'max.poll.interval.ms': 300000
    })
    
    try:
        # 토픽 구독
        consumer.subscribe([topic_name])
        
        # 파티션 정보 가져오기
        partitions = consumer.list_topics(topic=topic_name).topics[topic_name].partitions
        
        # 각 파티션의 오프셋 정보 확인
        topic_partitions = []
        for partition_id in partitions:
            tp = TopicPartition(topic_name, partition_id)
            topic_partitions.append(tp)
        
        # 시작과 끝 오프셋 가져오기
        for tp in topic_partitions:
            low, high = consumer.get_watermark_offsets(tp, timeout=5)
            logger.info(f"   파티션 {tp.partition}: 오프셋 범위 [{low}, {high})")
            
            if high > low:
                logger.info(f"      → {high - low}개 메시지 존재")
                
                # 최근 메시지 몇 개 읽기
                tp.offset = max(low, high - 5)  # 최근 5개만
                consumer.assign([tp])
                
                msg_count = 0
                while msg_count < 5:
                    msg = consumer.poll(timeout=1.0)
                    if msg is None:
                        break
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            break
                        else:
                            logger.error(f"      메시지 에러: {msg.error()}")
                            break
                    
                    try:
                        value = msg.value().decode('utf-8')
                        data = json.loads(value)
                        
                        # 메시지 요약 출력
                        msg_type = data.get('command_type') or data.get('event_type') or 'Unknown'
                        msg_id = data.get('command_id') or data.get('event_id') or 'Unknown'
                        timestamp = data.get('created_at') or data.get('occurred_at') or 'Unknown'
                        
                        logger.info(f"      메시지 {msg_count + 1}:")
                        logger.info(f"         ID: {msg_id}")
                        logger.info(f"         Type: {msg_type}")
                        logger.info(f"         Time: {timestamp}")
                        
                        if 'aggregate_id' in data:
                            logger.info(f"         Aggregate: {data['aggregate_id']}")
                        
                        msg_count += 1
                        
                    except json.JSONDecodeError:
                        logger.error(f"      JSON 파싱 실패: {value[:100]}...")
                    except Exception as e:
                        logger.error(f"      메시지 처리 오류: {e}")
            else:
                logger.info(f"      → 메시지 없음")
        
    except Exception as e:
        logger.error(f"   토픽 메시지 확인 실패: {e}")
    finally:
        consumer.close()


def main():
    # Kafka 연결 테스트
    logger.info("Kafka 연결 테스트...")
    
    try:
        # Producer로 연결 테스트
        from confluent_kafka import Producer
        
        producer = Producer({
            'bootstrap.servers': '127.0.0.1:9092',
            'client.id': 'connection-test'
        })
        
        # 메타데이터 가져오기 (연결 확인)
        metadata = producer.list_topics(timeout=5)
        logger.info(f"✅ Kafka 연결 성공! 브로커 수: {len(metadata.brokers)}")
        
        # 토픽 확인
        check_kafka_topics()
        
    except Exception as e:
        logger.error(f"❌ Kafka 연결 실패: {e}")
        logger.info("\n해결 방법:")
        logger.info("1. Kafka 실행 확인: docker-compose ps kafka")
        logger.info("2. 포트 확인: lsof -i :9092")
        logger.info("3. Kafka 재시작: docker-compose restart kafka")


if __name__ == "__main__":
    main()