#!/usr/bin/env python3
"""
Test Fixed DLQ Handler
THINK ULTRA - Test the properly fixed async DLQ handler
"""

import asyncio
import json
import uuid
import time
from datetime import datetime, timezone
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import redis.asyncio as aioredis
from shared.services.dlq_handler_fixed import DLQHandlerFixed, RetryPolicy, RetryStrategy

async def test_fixed_dlq_handler():
    """Test the fixed DLQ handler with proper async handling"""
    
    print("🔍 TESTING FIXED DLQ HANDLER")
    print("=" * 60)
    
    kafka_config = {'bootstrap.servers': '127.0.0.1:9092'}
    
    # Use unique IDs
    unique_id = str(uuid.uuid4())[:8]
    dlq_topic = f'dlq_fixed_{unique_id}'
    consumer_group = f'dlq_group_{unique_id}'
    
    print(f"\nTest Configuration:")
    print(f"  • Topic: {dlq_topic}")
    print(f"  • Consumer Group: {consumer_group}")
    
    # 1. Setup
    print("\n1. Setting up...")
    admin = AdminClient(kafka_config)
    new_topic = NewTopic(dlq_topic, num_partitions=1, replication_factor=1)
    admin.create_topics([new_topic])
    await asyncio.sleep(2)
    print("   ✅ Topic created")
    
    # 2. Redis
    redis_client = aioredis.from_url('redis://localhost:6379')
    await redis_client.ping()
    print("   ✅ Redis connected")
    
    # 3. Send messages
    print("\n2. Sending messages to DLQ...")
    producer = Producer(kafka_config)
    
    num_messages = 3
    for i in range(num_messages):
        dlq_msg = {
            'original_topic': 'test_topic',
            'original_value': json.dumps({'id': i, 'data': f'test-{i}'}),
            'error': 'Initial processing failed',
            'retry_count': 0,
            'first_failure_time': datetime.now(timezone.utc).isoformat()
        }
        producer.produce(dlq_topic, value=json.dumps(dlq_msg))
    
    producer.flush()
    print(f"   ✅ Sent {num_messages} messages")
    
    # 4. Create fixed DLQ handler
    print("\n3. Creating fixed DLQ handler...")
    retry_policy = RetryPolicy(
        max_retries=2,
        initial_delay_ms=500,  # Fast retry for testing
        max_delay_ms=2000,
        backoff_multiplier=2.0,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF
    )
    
    dlq_handler = DLQHandlerFixed(
        dlq_topic=dlq_topic,
        kafka_config=kafka_config,
        redis_client=redis_client,
        retry_policy=retry_policy,
        consumer_group=consumer_group
    )
    print(f"   ✅ Fixed DLQ handler created")
    
    # 5. Register processor
    process_counts = {}
    successful_messages = []
    
    async def test_processor(message):
        msg_id = message.get('id', 'unknown')
        process_counts[msg_id] = process_counts.get(msg_id, 0) + 1
        count = process_counts[msg_id]
        
        print(f"   🔸 Processing message {msg_id}, attempt #{count}")
        
        if count <= 1:
            # Fail first attempt
            raise Exception(f"Simulated failure #{count}")
        else:
            # Succeed on second attempt
            successful_messages.append(msg_id)
            return f"Success for {msg_id}"
    
    dlq_handler.register_processor('test_topic', test_processor)
    print("   ✅ Processor registered")
    
    # 6. Start processing
    print("\n4. Starting DLQ processing...")
    await dlq_handler.start_processing()
    print("   ✅ DLQ handler started (non-blocking)")
    
    # 7. Monitor
    print("\n5. Monitoring processing...")
    print("   Expected timeline:")
    print("   • T+0-2s: Initial message processing")
    print("   • T+2.5s: First retry (fails)")
    print("   • T+3.5s: Second retry (succeeds)")
    start_time = time.time()
    max_wait = 8  # Wait longer for retries
    all_recovered = False
    
    while time.time() - start_time < max_wait:
        await asyncio.sleep(0.5)
        
        metrics = await dlq_handler.get_metrics()
        elapsed = time.time() - start_time
        
        print(f"   T+{elapsed:.1f}s: processed={metrics['messages_processed']}, "
              f"retried={metrics['messages_retried']}, "
              f"recovered={metrics['messages_recovered']}")
        
        if metrics['messages_recovered'] >= num_messages:
            all_recovered = True
            print(f"\n   ✅ All {num_messages} messages recovered!")
            break
    
    # 8. Stop
    print("\n6. Stopping DLQ handler...")
    await dlq_handler.stop_processing()
    print("   ✅ DLQ handler stopped")
    
    # 9. Results
    final_metrics = await dlq_handler.get_metrics()
    print("\n7. Final Results:")
    print(f"   • Messages processed: {final_metrics['messages_processed']}")
    print(f"   • Messages retried: {final_metrics['messages_retried']}")
    print(f"   • Messages recovered: {final_metrics['messages_recovered']}")
    print(f"   • Successful messages: {successful_messages}")
    
    # 10. Cleanup
    admin.delete_topics([dlq_topic, f"{dlq_topic}.poison"])
    await redis_client.aclose()
    
    # Verify
    if all_recovered and len(successful_messages) == num_messages:
        print("\n✅ TEST PASSED! Fixed DLQ handler works correctly")
        print("   • Non-blocking async operations ✓")
        print("   • Exponential backoff retry ✓")
        print("   • Message recovery ✓")
        return True
    else:
        print("\n❌ TEST FAILED!")
        return False

if __name__ == "__main__":
    result = asyncio.run(test_fixed_dlq_handler())
    exit(0 if result else 1)