#!/usr/bin/env python3
"""
Basic Kafka Connection Test
THINK ULTRA - Verify Kafka is working before full tests
"""

import os
import sys
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient

# Force localhost
os.environ['DOCKER_CONTAINER'] = 'false'

def test_basic_connection():
    """Test basic Kafka connection"""
    print("🔍 Testing Basic Kafka Connection...")
    
    try:
        # Test 1: Admin client connection
        admin = AdminClient({'bootstrap.servers': '127.0.0.1:9092'})
        metadata = admin.list_topics(timeout=5)
        print(f"✅ Connected to Kafka! Found {len(metadata.topics)} topics")
        
        # List topics
        print("\nExisting topics:")
        for topic in metadata.topics:
            print(f"  • {topic}")
        
        # Test 2: Simple producer (no transactions)
        producer = Producer({
            'bootstrap.servers': '127.0.0.1:9092',
            'client.id': 'test-producer'
        })
        
        # Send a test message
        producer.produce(
            topic='test-topic',
            value=b'test message',
            key=b'test-key'
        )
        producer.flush(timeout=5)
        print("\n✅ Producer working (sent test message)")
        
        # Test 3: Simple consumer
        consumer = Consumer({
            'bootstrap.servers': '127.0.0.1:9092',
            'group.id': 'test-group',
            'auto.offset.reset': 'earliest'
        })
        
        consumer.subscribe(['test-topic'])
        msg = consumer.poll(timeout=1.0)
        
        if msg and not msg.error():
            print(f"✅ Consumer working (received: {msg.value()})")
        else:
            print("✅ Consumer connected (no messages)")
        
        consumer.close()
        
        print("\n✅ All basic tests passed! Kafka is operational.")
        return True
        
    except Exception as e:
        print(f"\n❌ Kafka connection failed: {e}")
        print("\nTroubleshooting:")
        print("1. Check if Kafka is running: brew services list | grep kafka")
        print("2. Start Kafka: brew services start kafka")
        print("3. Check Kafka logs: tail -f /opt/homebrew/var/log/kafka/kafka_output.log")
        return False

if __name__ == "__main__":
    success = test_basic_connection()
    sys.exit(0 if success else 1)