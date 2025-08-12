#!/usr/bin/env python3
"""
Debug DLQ Background Tasks
THINK ULTRA - Find exactly where it hangs
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
import redis.asyncio as aioredis
import logging

# Enable debug logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

async def debug_background_tasks():
    """Debug the background tasks directly"""
    
    print("🔍 DEBUGGING DLQ BACKGROUND TASKS")
    print("=" * 60)
    
    kafka_config = {'bootstrap.servers': '127.0.0.1:9092'}
    unique_id = str(uuid.uuid4())[:8]
    dlq_topic = f'dlq_debug_{unique_id}'
    
    # Create topic
    admin = AdminClient(kafka_config)
    new_topic = NewTopic(dlq_topic, num_partitions=1, replication_factor=1)
    admin.create_topics([new_topic])
    await asyncio.sleep(2)
    print(f"✅ Created topic: {dlq_topic}")
    
    # Send message
    producer = Producer(kafka_config)
    test_msg = {
        'original_topic': 'test',
        'original_value': json.dumps({'id': 1}),
        'error': 'test',
        'retry_count': 0,
        'first_failure_time': datetime.now(timezone.utc).isoformat()
    }
    producer.produce(dlq_topic, value=json.dumps(test_msg))
    producer.flush()
    print("✅ Sent message to DLQ")
    
    # Create simple consumer to test
    print("\nTesting consumer directly...")
    consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': f'test_{unique_id}',
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([dlq_topic])
    
    # Simulate what _process_loop does
    print("Simulating _process_loop...")
    processing = True
    messages_found = 0
    
    async def process_loop_simulation():
        nonlocal messages_found
        poll_count = 0
        
        while processing and poll_count < 5:
            poll_count += 1
            print(f"  Poll #{poll_count}...")
            
            # This is what DLQ handler does
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                print("    No message")
                continue
                
            if msg.error():
                print(f"    Error: {msg.error()}")
                continue
            
            print(f"    ✅ Got message: {msg.value()[:50]}...")
            messages_found += 1
            consumer.commit(asynchronous=False)
            
            # Process the message (simplified)
            dlq_data = json.loads(msg.value().decode('utf-8'))
            print(f"    Parsed: topic={dlq_data.get('original_topic')}")
    
    # Run with timeout
    try:
        await asyncio.wait_for(process_loop_simulation(), timeout=10)
    except asyncio.TimeoutError:
        print("  ❌ Process loop timed out!")
    
    processing = False
    consumer.close()
    
    print(f"\nResult: Found {messages_found} messages")
    
    # Now test the actual DLQ handler with minimal setup
    print("\n" + "="*60)
    print("Testing actual DLQ handler...")
    
    redis_client = aioredis.from_url('redis://localhost:6379')
    
    # Import and create handler
    from shared.services.dlq_handler import DLQHandler, RetryPolicy, RetryStrategy
    
    # Create another message since we consumed the first one
    producer.produce(dlq_topic, value=json.dumps(test_msg))
    producer.flush()
    print("✅ Sent another message for DLQ handler")
    
    retry_policy = RetryPolicy(
        max_retries=0,
        initial_delay_ms=0,
        strategy=RetryStrategy.IMMEDIATE
    )
    
    dlq_handler = DLQHandler(
        dlq_topic=dlq_topic,
        kafka_config=kafka_config,
        redis_client=redis_client,
        retry_policy=retry_policy,
        consumer_group=f'dlq_{unique_id}'
    )
    
    # Register processor
    processor_called = [False]
    async def test_processor(message):
        processor_called[0] = True
        print(f"  🎯 Processor called!")
        return "Success"
    
    dlq_handler.register_processor('test', test_processor)
    
    print("\nStarting DLQ handler (will run for 3 seconds)...")
    
    # Start handler
    await dlq_handler.start_processing()
    
    # Wait and check
    for i in range(3):
        await asyncio.sleep(1)
        metrics = await dlq_handler.get_metrics()
        print(f"  T+{i+1}s: processed={metrics['messages_processed']}, processor_called={processor_called[0]}")
    
    # Stop handler
    print("\nStopping handler...")
    await dlq_handler.stop_processing()
    
    # Cleanup
    admin.delete_topics([dlq_topic])
    await redis_client.aclose()
    
    print("\n" + "="*60)
    if processor_called[0]:
        print("✅ SUCCESS: DLQ handler processed message")
    else:
        print("❌ FAILURE: DLQ handler didn't process message")
        print("\nPossible issues:")
        print("1. Consumer group offset already past the message")
        print("2. Background task not polling correctly")
        print("3. Message processing logic issue")

if __name__ == "__main__":
    asyncio.run(debug_background_tasks())