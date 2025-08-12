#!/usr/bin/env python3
"""
Fixed DLQ Handler Test
THINK ULTRA - Proper test with correct async handling
"""

import asyncio
import json
import time
from datetime import datetime, timezone
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
import redis.asyncio as aioredis
from shared.services.dlq_handler import DLQHandler, RetryPolicy, RetryStrategy
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_dlq_handler_properly():
    """Test DLQ handler with proper async handling"""
    
    print("🔍 TESTING DLQ HANDLER - FIXED VERSION")
    print("=" * 60)
    
    kafka_config = {'bootstrap.servers': '127.0.0.1:9092'}
    dlq_topic = 'test_dlq_fixed'
    
    # 1. Setup
    print("\n1. Setting up...")
    admin = AdminClient(kafka_config)
    
    # Clean up first
    try:
        admin.delete_topics([dlq_topic, f"{dlq_topic}.poison"])
        await asyncio.sleep(2)
    except:
        pass
    
    # Create topic
    new_topic = NewTopic(dlq_topic, num_partitions=1, replication_factor=1)
    admin.create_topics([new_topic])
    await asyncio.sleep(2)
    print("   ✅ Topic created")
    
    # 2. Setup Redis
    redis_client = aioredis.from_url('redis://localhost:6379')
    await redis_client.ping()
    print("   ✅ Redis connected")
    
    # 3. Create DLQ handler with fast retry for testing
    retry_policy = RetryPolicy(
        max_retries=2,  # Will retry twice
        initial_delay_ms=500,  # 0.5 second
        max_delay_ms=2000,  # 2 seconds max
        backoff_multiplier=2.0,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF
    )
    
    dlq_handler = DLQHandler(
        dlq_topic=dlq_topic,
        kafka_config=kafka_config,
        redis_client=redis_client,
        retry_policy=retry_policy
    )
    print("   ✅ DLQ handler created")
    
    # 4. Register processor that fails first time, succeeds second
    process_counts = {}
    successful_ids = []
    
    async def test_processor(message):
        msg_id = message.get('id', 'unknown')
        process_counts[msg_id] = process_counts.get(msg_id, 0) + 1
        count = process_counts[msg_id]
        
        print(f"   🔸 Processing message {msg_id}, attempt #{count}")
        
        if count == 1:
            # First attempt fails
            raise Exception(f"Simulated failure for {msg_id}")
        else:
            # Second attempt succeeds
            successful_ids.append(msg_id)
            return f"Success for {msg_id} after {count} attempts"
    
    dlq_handler.register_processor('original_topic', test_processor)
    print("   ✅ Processor registered")
    
    # 5. Send messages to DLQ BEFORE starting handler
    print("\n2. Sending messages to DLQ...")
    producer = Producer(kafka_config)
    
    num_messages = 3  # Reduced for faster testing
    for i in range(num_messages):
        dlq_msg = {
            'original_topic': 'original_topic',
            'original_value': json.dumps({'id': i, 'data': f'test-{i}'}),
            'error': 'Initial processing failed',
            'retry_count': 0,
            'first_failure_time': datetime.now(timezone.utc).isoformat()
        }
        producer.produce(dlq_topic, value=json.dumps(dlq_msg))
    
    producer.flush()
    print(f"   ✅ Sent {num_messages} messages to DLQ")
    
    # 6. Verify messages are in topic
    print("\n3. Verifying messages in topic...")
    consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': 'verify-group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([dlq_topic])
    
    count = 0
    for _ in range(10):
        msg = consumer.poll(timeout=0.5)
        if msg and not msg.error():
            count += 1
    consumer.close()
    print(f"   ✅ Verified {count} messages in topic")
    
    # 7. Start DLQ processing
    print("\n4. Starting DLQ handler...")
    await dlq_handler.start_processing()
    print("   ✅ DLQ handler started (background tasks running)")
    
    # 8. Monitor processing
    print("\n5. Monitoring processing...")
    print("   Expected timeline:")
    print("   • T+0s: Initial processing of 3 messages")
    print("   • T+0.5s: First retry (500ms delay)")
    print("   • T+1.5s: All messages should be recovered")
    
    start_time = time.time()
    max_wait = 5  # Maximum 5 seconds
    
    while time.time() - start_time < max_wait:
        await asyncio.sleep(0.5)
        
        metrics = await dlq_handler.get_metrics()
        elapsed = time.time() - start_time
        
        print(f"   T+{elapsed:.1f}s: processed={metrics['messages_processed']}, "
              f"retried={metrics['messages_retried']}, "
              f"recovered={metrics['messages_recovered']}")
        
        # Check if all messages are recovered
        if metrics['messages_recovered'] >= num_messages:
            print(f"\n   ✅ All {num_messages} messages recovered!")
            break
    
    # 9. Final metrics
    print("\n6. Final results:")
    final_metrics = await dlq_handler.get_metrics()
    print(f"   • Messages processed: {final_metrics['messages_processed']}")
    print(f"   • Messages retried: {final_metrics['messages_retried']}")
    print(f"   • Messages recovered: {final_metrics['messages_recovered']}")
    print(f"   • Messages poisoned: {final_metrics['messages_poisoned']}")
    print(f"   • Successful IDs: {successful_ids}")
    
    # 10. Stop handler
    print("\n7. Stopping DLQ handler...")
    await dlq_handler.stop_processing()
    print("   ✅ DLQ handler stopped")
    
    # 11. Cleanup
    admin.delete_topics([dlq_topic, f"{dlq_topic}.poison"])
    await redis_client.aclose()  # Use aclose() instead of close()
    
    # 12. Verify results
    success = (
        final_metrics['messages_processed'] == num_messages and
        final_metrics['messages_recovered'] == num_messages and
        len(successful_ids) == num_messages
    )
    
    if success:
        print("\n✅ TEST PASSED! DLQ handler works correctly")
    else:
        print("\n❌ TEST FAILED! Some messages not recovered")
    
    return success

if __name__ == "__main__":
    result = asyncio.run(test_dlq_handler_properly())
    exit(0 if result else 1)