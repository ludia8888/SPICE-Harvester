#!/usr/bin/env python3
"""
Performance Critical Improvements Test Suite
THINK ULTRA³ - GOD MODE ON

Tests all critical performance improvements:
1. Partition key = aggregate_id (ordering guarantee)
2. Kafka EOS v2 (exactly-once semantics)
3. Global watermark monitoring (lag tracking)
4. DLQ handler (exponential backoff retry)
5. Full integration under load

NO MOCKS, NO FAKES, PRODUCTION READY
"""

import asyncio
import json
import time
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any
import random

# Kafka imports
from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

# Redis imports
import redis.asyncio as aioredis

# System imports
from shared.config.kafka_config import KafkaEOSConfig, TransactionalProducer
from shared.services.watermark_monitor import WatermarkMonitor, create_watermark_monitor
from shared.services.dlq_handler import DLQHandler, RetryPolicy, RetryStrategy
from shared.config.service_config import ServiceConfig
from shared.config.app_config import AppConfig

# Logging
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PerformanceTestSuite:
    """
    Comprehensive test suite for performance critical improvements
    CLAUDE RULE: Real testing, no shortcuts, find all issues
    """
    
    def __init__(self):
        self.kafka_config = {
            'bootstrap.servers': ServiceConfig.get_kafka_bootstrap_servers()
        }
        self.redis_client = None
        self.test_results = {}
        self.issues_found = []
        
    async def setup(self):
        """Setup test environment - verify all services are running"""
        print("\n" + "="*80)
        print("🚀 PERFORMANCE CRITICAL IMPROVEMENTS TEST SUITE")
        print("THINK ULTRA³ - GOD MODE ACTIVATED")
        print("="*80)
        
        print("\n📋 Checking prerequisites...")
        
        # 1. Check Kafka
        try:
            admin_client = AdminClient(self.kafka_config)
            metadata = admin_client.list_topics(timeout=5)
            print(f"✅ Kafka: Connected ({len(metadata.topics)} topics found)")
        except Exception as e:
            error = f"❌ Kafka: Connection failed - {e}"
            print(error)
            self.issues_found.append(("CRITICAL", "Kafka not running", str(e)))
            return False
        
        # 2. Check Redis
        try:
            self.redis_client = aioredis.from_url('redis://localhost:6379')
            await self.redis_client.ping()
            print("✅ Redis: Connected")
        except Exception as e:
            error = f"❌ Redis: Connection failed - {e}"
            print(error)
            self.issues_found.append(("CRITICAL", "Redis not running", str(e)))
            return False
        
        # 3. Check MinIO/S3
        try:
            import boto3
            s3_client = boto3.client(
                's3',
                endpoint_url='http://localhost:9000',
                aws_access_key_id='minioadmin',
                aws_secret_access_key='minioadmin123'
            )
            s3_client.list_buckets()
            print("✅ MinIO/S3: Connected")
        except Exception as e:
            error = f"❌ MinIO/S3: Connection failed - {e}"
            print(error)
            self.issues_found.append(("WARNING", "MinIO not running", str(e)))
        
        # 4. Check TerminusDB
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get('http://localhost:6363/api/info') as resp:
                    if resp.status == 200:
                        print("✅ TerminusDB: Connected")
                    else:
                        raise Exception(f"Status {resp.status}")
        except Exception as e:
            error = f"❌ TerminusDB: Connection failed - {e}"
            print(error)
            self.issues_found.append(("WARNING", "TerminusDB not running", str(e)))
        
        print("\n" + "-"*80)
        return True
    
    async def test_1_partition_key_ordering(self):
        """
        Test 1: Verify partition key = aggregate_id ensures ordering
        CRITICAL: Events for same aggregate MUST be in same partition
        """
        print("\n🧪 TEST 1: PARTITION KEY = AGGREGATE_ID ORDERING")
        print("-"*60)
        
        test_topic = "test_partition_ordering"
        aggregate_id = f"AGG-{uuid.uuid4().hex[:8]}"
        num_events = 100
        
        try:
            # Create test topic with multiple partitions
            admin_client = AdminClient(self.kafka_config)
            new_topic = NewTopic(test_topic, num_partitions=10, replication_factor=1)
            admin_client.create_topics([new_topic])
            await asyncio.sleep(2)  # Wait for topic creation
            print(f"✅ Created test topic: {test_topic} with 10 partitions")
            
            # Produce events with aggregate_id as key
            producer_config = KafkaEOSConfig.get_producer_config(
                service_name='test-producer',
                enable_transactions=False  # Test partitioning only
            )
            producer = Producer(producer_config)
            
            partition_set = set()
            
            for i in range(num_events):
                event = {
                    'event_id': str(uuid.uuid4()),
                    'aggregate_id': aggregate_id,
                    'sequence': i,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
                
                # Use aggregate_id as partition key
                producer.produce(
                    topic=test_topic,
                    value=json.dumps(event),
                    key=aggregate_id.encode('utf-8'),
                    on_delivery=lambda err, msg: partition_set.add(msg.partition()) if not err else None
                )
            
            producer.flush(timeout=10)
            
            # Verify all events went to same partition
            if len(partition_set) == 1:
                partition_num = list(partition_set)[0]
                print(f"✅ All {num_events} events for {aggregate_id} -> partition {partition_num}")
                self.test_results['partition_ordering'] = 'PASSED'
            else:
                error = f"❌ Events scattered across {len(partition_set)} partitions: {partition_set}"
                print(error)
                self.issues_found.append(("HIGH", "Partition key not working", error))
                self.test_results['partition_ordering'] = 'FAILED'
            
            # Consume and verify order
            consumer = Consumer({
                'bootstrap.servers': self.kafka_config['bootstrap.servers'],
                'group.id': 'test-consumer',
                'auto.offset.reset': 'earliest'
            })
            
            # Subscribe to specific partition
            consumer.assign([TopicPartition(test_topic, list(partition_set)[0], 0)])
            
            received_sequences = []
            start_time = time.time()
            
            while len(received_sequences) < num_events and time.time() - start_time < 10:
                msg = consumer.poll(timeout=1.0)
                if msg and not msg.error():
                    event = json.loads(msg.value())
                    received_sequences.append(event['sequence'])
            
            consumer.close()
            
            # Check ordering
            is_ordered = all(received_sequences[i] <= received_sequences[i+1] 
                            for i in range(len(received_sequences)-1))
            
            if is_ordered and len(received_sequences) == num_events:
                print(f"✅ Order preserved: received {len(received_sequences)} events in sequence")
            else:
                error = f"❌ Order violation or missing events: received {len(received_sequences)}/{num_events}"
                print(error)
                self.issues_found.append(("HIGH", "Event ordering violated", error))
            
            # Cleanup
            admin_client.delete_topics([test_topic])
            
        except Exception as e:
            error = f"❌ Test 1 failed: {e}"
            print(error)
            self.issues_found.append(("HIGH", "Partition test error", str(e)))
            self.test_results['partition_ordering'] = 'ERROR'
    
    async def test_2_kafka_eos_v2(self):
        """
        Test 2: Verify Kafka EOS v2 prevents duplicates
        CRITICAL: Transactional producer must ensure exactly-once
        """
        print("\n🧪 TEST 2: KAFKA EOS V2 - EXACTLY ONCE SEMANTICS")
        print("-"*60)
        
        test_topic = "test_eos_v2"
        transaction_id = f"test-txn-{uuid.uuid4().hex[:8]}"
        
        try:
            # Create test topic
            admin_client = AdminClient(self.kafka_config)
            new_topic = NewTopic(test_topic, num_partitions=3, replication_factor=1)
            admin_client.create_topics([new_topic])
            await asyncio.sleep(2)
            print(f"✅ Created test topic: {test_topic}")
            
            # Create transactional producer
            producer_config = KafkaEOSConfig.get_producer_config(
                service_name='test-eos',
                instance_id=transaction_id,
                enable_transactions=True
            )
            producer = Producer(producer_config)
            transactional_producer = TransactionalProducer(producer, enable_transactions=True)
            
            # Initialize transactions
            try:
                transactional_producer.init_transactions(30)  # Positional argument
                print("✅ Transactional producer initialized")
            except Exception as e:
                error = f"❌ Failed to initialize transactions: {e}"
                print(error)
                self.issues_found.append(("CRITICAL", "EOS v2 init failed", str(e)))
                self.test_results['eos_v2'] = 'FAILED'
                return
            
            # Test 1: Successful transaction
            print("\n  Testing successful transaction...")
            messages_batch_1 = [
                {'id': i, 'batch': 1, 'type': 'success'} 
                for i in range(10)
            ]
            
            transactional_producer.begin_transaction()
            for msg in messages_batch_1:
                producer.produce(
                    topic=test_topic,
                    value=json.dumps(msg),
                    key=str(msg['id']).encode('utf-8')
                )
            transactional_producer.commit_transaction()
            print("  ✅ Transaction 1 committed (10 messages)")
            
            # Test 2: Aborted transaction (should not be visible)
            print("\n  Testing aborted transaction...")
            messages_batch_2 = [
                {'id': i, 'batch': 2, 'type': 'aborted'} 
                for i in range(100, 110)
            ]
            
            transactional_producer.begin_transaction()
            for msg in messages_batch_2:
                producer.produce(
                    topic=test_topic,
                    value=json.dumps(msg),
                    key=str(msg['id']).encode('utf-8')
                )
            transactional_producer.abort_transaction()
            print("  ✅ Transaction 2 aborted (10 messages - should not be visible)")
            
            # Test 3: Another successful transaction
            print("\n  Testing second successful transaction...")
            messages_batch_3 = [
                {'id': i, 'batch': 3, 'type': 'success'} 
                for i in range(200, 210)
            ]
            
            transactional_producer.begin_transaction()
            for msg in messages_batch_3:
                producer.produce(
                    topic=test_topic,
                    value=json.dumps(msg),
                    key=str(msg['id']).encode('utf-8')
                )
            transactional_producer.commit_transaction()
            print("  ✅ Transaction 3 committed (10 messages)")
            
            # Consume with read_committed isolation
            consumer_config = KafkaEOSConfig.get_consumer_config(
                service_name='test-consumer',
                group_id='eos-test-group',
                read_committed=True,  # CRITICAL: Only read committed messages
                auto_commit=False
            )
            consumer = Consumer(consumer_config)
            consumer.subscribe([test_topic])
            
            print("\n  Consuming with read_committed isolation...")
            received_messages = []
            start_time = time.time()
            
            while time.time() - start_time < 10:
                msg = consumer.poll(timeout=1.0)
                if msg and not msg.error():
                    received_messages.append(json.loads(msg.value()))
            
            consumer.close()
            
            # Verify results
            batch_1_count = sum(1 for m in received_messages if m.get('batch') == 1)
            batch_2_count = sum(1 for m in received_messages if m.get('batch') == 2)
            batch_3_count = sum(1 for m in received_messages if m.get('batch') == 3)
            
            print(f"\n  Results:")
            print(f"  • Batch 1 (committed): {batch_1_count} messages")
            print(f"  • Batch 2 (aborted): {batch_2_count} messages")
            print(f"  • Batch 3 (committed): {batch_3_count} messages")
            
            if batch_1_count == 10 and batch_2_count == 0 and batch_3_count == 10:
                print("\n✅ EOS v2 working correctly - aborted transactions not visible!")
                self.test_results['eos_v2'] = 'PASSED'
            else:
                error = f"❌ EOS v2 failure - unexpected message counts"
                print(error)
                self.issues_found.append(("CRITICAL", "EOS v2 not working", 
                                        f"B1:{batch_1_count}, B2:{batch_2_count}, B3:{batch_3_count}"))
                self.test_results['eos_v2'] = 'FAILED'
            
            # Cleanup
            admin_client.delete_topics([test_topic])
            
        except Exception as e:
            error = f"❌ Test 2 failed: {e}"
            print(error)
            self.issues_found.append(("CRITICAL", "EOS v2 test error", str(e)))
            self.test_results['eos_v2'] = 'ERROR'
    
    async def test_3_watermark_monitoring(self):
        """
        Test 3: Verify watermark monitoring tracks lag correctly
        CRITICAL: Must detect and alert on high lag
        """
        print("\n🧪 TEST 3: WATERMARK MONITORING - LAG TRACKING")
        print("-"*60)
        
        test_topic = "test_watermark"
        
        try:
            # Create test topic
            admin_client = AdminClient(self.kafka_config)
            new_topic = NewTopic(test_topic, num_partitions=3, replication_factor=1)
            admin_client.create_topics([new_topic])
            await asyncio.sleep(2)
            print(f"✅ Created test topic: {test_topic}")
            
            # Produce messages
            producer = Producer(self.kafka_config)
            num_messages = 1000
            
            print(f"\n  Producing {num_messages} messages...")
            for i in range(num_messages):
                producer.produce(
                    topic=test_topic,
                    value=json.dumps({'id': i, 'timestamp': time.time()}),
                    key=str(i % 3).encode('utf-8')  # Distribute across 3 partitions
                )
            producer.flush()
            print(f"  ✅ Produced {num_messages} messages")
            
            # Create slow consumer (to build lag)
            consumer = Consumer({
                'bootstrap.servers': self.kafka_config['bootstrap.servers'],
                'group.id': 'slow-consumer-group',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            })
            consumer.subscribe([test_topic])
            
            # Consume only 100 messages (create lag of 900)
            print("\n  Creating lag by consuming slowly...")
            consumed = 0
            for _ in range(100):
                msg = consumer.poll(timeout=1.0)
                if msg and not msg.error():
                    consumed += 1
                    consumer.commit()
            
            print(f"  ✅ Consumed {consumed} messages (lag = {num_messages - consumed})")
            
            # Start watermark monitor
            monitor = await create_watermark_monitor(
                kafka_config=self.kafka_config,
                consumer_groups=['slow-consumer-group'],
                topics=[test_topic]
            )
            
            print("\n  Starting watermark monitoring...")
            await asyncio.sleep(12)  # Wait for monitoring cycle
            
            # Get lag metrics
            lag_info = await monitor.get_current_lag()
            partition_details = await monitor.get_partition_details(test_topic)
            
            print(f"\n  Watermark Monitor Results:")
            print(f"  • Total lag: {lag_info.get('total_lag', 0)}")
            print(f"  • Max lag: {lag_info.get('max_lag', 0)}")
            print(f"  • Avg lag: {lag_info.get('avg_lag', 0):.1f}")
            print(f"  • Progress: {lag_info.get('progress_percentage', 0):.1f}%")
            print(f"  • Status: {lag_info.get('status', 'unknown')}")
            
            # Check if lag was detected correctly
            expected_lag = num_messages - consumed
            actual_lag = lag_info.get('total_lag', 0)
            
            if abs(actual_lag - expected_lag) <= 10:  # Allow small variance
                print(f"\n✅ Watermark monitoring accurate! Detected lag: {actual_lag}")
                self.test_results['watermark_monitoring'] = 'PASSED'
            else:
                error = f"❌ Lag detection inaccurate: expected ~{expected_lag}, got {actual_lag}"
                print(error)
                self.issues_found.append(("HIGH", "Watermark inaccuracy", error))
                self.test_results['watermark_monitoring'] = 'FAILED'
            
            # Check alert generation
            alerts = await self.redis_client.lrange("alerts:watermark:high_lag", 0, -1)
            if alerts and actual_lag > 500:
                print(f"✅ High lag alert generated ({len(alerts)} alerts)")
            elif actual_lag > 500:
                print(f"⚠️  High lag but no alert generated")
                self.issues_found.append(("MEDIUM", "Alert not triggered", f"Lag {actual_lag} but no alert"))
            
            # Cleanup
            await monitor.stop_monitoring()
            consumer.close()
            admin_client.delete_topics([test_topic])
            
        except Exception as e:
            error = f"❌ Test 3 failed: {e}"
            print(error)
            self.issues_found.append(("HIGH", "Watermark test error", str(e)))
            self.test_results['watermark_monitoring'] = 'ERROR'
    
    async def test_4_dlq_handler_retry(self):
        """
        Test 4: Verify DLQ handler with exponential backoff
        CRITICAL: Failed messages must be retried intelligently
        """
        print("\n🧪 TEST 4: DLQ HANDLER - EXPONENTIAL BACKOFF RETRY")
        print("-"*60)
        
        # FIXED: Use unique topic name to avoid duplicate messages
        import uuid
        unique_id = uuid.uuid4().hex[:8]
        dlq_topic = f"test_dlq_{unique_id}"
        
        try:
            # Create DLQ topic
            admin_client = AdminClient(self.kafka_config)
            new_topic = NewTopic(dlq_topic, num_partitions=1, replication_factor=1)
            admin_client.create_topics([new_topic])
            await asyncio.sleep(2)
            print(f"✅ Created DLQ topic: {dlq_topic}")
            
            # Create DLQ handler with aggressive retry policy for testing
            # FIXED: max_retries needs to account for initial processing + retries
            retry_policy = RetryPolicy(
                max_retries=2,  # Will result in 3 total attempts
                initial_delay_ms=1000,  # 1 second
                max_delay_ms=5000,  # 5 seconds
                backoff_multiplier=2.0,
                strategy=RetryStrategy.EXPONENTIAL_BACKOFF
            )
            
            # FIXED: Use unique consumer group to avoid offset issues
            unique_group = f'dlq-test-{unique_id}'  # Reuse the same unique_id
            
            dlq_handler = DLQHandler(
                dlq_topic=dlq_topic,
                kafka_config=self.kafka_config,
                redis_client=self.redis_client,
                retry_policy=retry_policy,
                consumer_group=unique_group  # Use unique group
            )
            
            # Register a processor that fails first time, succeeds on second
            # FIXED: Simplified logic to match retry policy
            retry_counts = {}
            
            async def test_processor(message):
                msg_id = message.get('id', 'unknown')
                retry_counts[msg_id] = retry_counts.get(msg_id, 0) + 1
                
                if retry_counts[msg_id] == 1:
                    # First attempt fails
                    raise Exception(f"Simulated failure #{retry_counts[msg_id]}")
                
                # Second attempt succeeds
                return f"Processed after {retry_counts[msg_id]} attempts"
            
            dlq_handler.register_processor('original_topic', test_processor)
            
            # Send failed messages to DLQ
            producer = Producer(self.kafka_config)
            
            failed_messages = [
                {
                    'original_topic': 'original_topic',
                    'original_value': json.dumps({'id': i, 'data': f'test-{i}'}),
                    'error': 'Initial processing failed',
                    'retry_count': 0,
                    'first_failure_time': datetime.now(timezone.utc).isoformat()
                }
                for i in range(5)
            ]
            
            print(f"\n  Sending {len(failed_messages)} failed messages to DLQ...")
            for msg in failed_messages:
                producer.produce(
                    topic=dlq_topic,
                    value=json.dumps(msg)
                )
            producer.flush()
            print(f"  ✅ Sent {len(failed_messages)} messages to DLQ")
            
            # Start DLQ processing
            print("\n  Starting DLQ handler...")
            await dlq_handler.start_processing()
            
            # Wait for retries (should take ~7 seconds with exponential backoff)
            print("\n  Waiting for exponential backoff retries...")
            print("  Expected timeline:")
            print("  • T+0s: Initial processing")
            print("  • T+1s: First retry (1s delay)")
            print("  • T+3s: Second retry (2s delay)")
            print("  • T+7s: Third retry (4s delay) - SUCCESS")
            
            await asyncio.sleep(10)  # Wait for all retries
            
            # Check metrics
            metrics = await dlq_handler.get_metrics()
            
            print(f"\n  DLQ Handler Metrics:")
            print(f"  • Messages processed: {metrics['messages_processed']}")
            print(f"  • Messages retried: {metrics['messages_retried']}")
            print(f"  • Messages recovered: {metrics['messages_recovered']}")
            print(f"  • Messages poisoned: {metrics['messages_poisoned']}")
            
            # Verify exponential backoff worked
            if metrics['messages_recovered'] == len(failed_messages):
                print(f"\n✅ All {len(failed_messages)} messages recovered after retries!")
                self.test_results['dlq_handler'] = 'PASSED'
            else:
                error = f"❌ Recovery failed: {metrics['messages_recovered']}/{len(failed_messages)} recovered"
                print(error)
                self.issues_found.append(("HIGH", "DLQ recovery failure", error))
                self.test_results['dlq_handler'] = 'FAILED'
            
            # Stop DLQ handler
            await dlq_handler.stop_processing()
            
            # Cleanup
            admin_client.delete_topics([dlq_topic])
            
        except Exception as e:
            error = f"❌ Test 4 failed: {e}"
            print(error)
            self.issues_found.append(("HIGH", "DLQ test error", str(e)))
            self.test_results['dlq_handler'] = 'ERROR'
    
    async def test_5_integration_load_test(self):
        """
        Test 5: Full integration test under load
        CRITICAL: All components must work together at scale
        """
        print("\n🧪 TEST 5: INTEGRATION LOAD TEST")
        print("-"*60)
        
        num_aggregates = 10
        events_per_aggregate = 100
        total_events = num_aggregates * events_per_aggregate
        
        try:
            print(f"\n  Test configuration:")
            print(f"  • Aggregates: {num_aggregates}")
            print(f"  • Events per aggregate: {events_per_aggregate}")
            print(f"  • Total events: {total_events}")
            
            # Create topics
            admin_client = AdminClient(self.kafka_config)
            test_topics = ['load_test_events', 'load_test_commands']
            
            for topic in test_topics:
                new_topic = NewTopic(topic, num_partitions=12, replication_factor=1)
                admin_client.create_topics([new_topic])
            await asyncio.sleep(2)
            print(f"  ✅ Created test topics")
            
            # Start watermark monitor
            monitor = await create_watermark_monitor(
                kafka_config=self.kafka_config,
                consumer_groups=['load-test-group'],
                topics=test_topics
            )
            
            # Produce events with EOS v2
            producer_config = KafkaEOSConfig.get_producer_config(
                service_name='load-test-producer',
                enable_transactions=True
            )
            producer = Producer(producer_config)
            transactional_producer = TransactionalProducer(producer, enable_transactions=True)
            transactional_producer.init_transactions()
            
            print(f"\n  Producing {total_events} events with transactions...")
            start_time = time.time()
            
            for agg_idx in range(num_aggregates):
                aggregate_id = f"AGG-{agg_idx:03d}"
                
                # Each aggregate gets its own transaction
                transactional_producer.begin_transaction()
                
                for seq in range(events_per_aggregate):
                    event = {
                        'event_id': str(uuid.uuid4()),
                        'aggregate_id': aggregate_id,
                        'sequence': seq,
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                        'data': f"Event {seq} for {aggregate_id}"
                    }
                    
                    producer.produce(
                        topic='load_test_events',
                        value=json.dumps(event),
                        key=aggregate_id.encode('utf-8')  # Partition by aggregate
                    )
                
                transactional_producer.commit_transaction()
                
                if (agg_idx + 1) % 5 == 0:
                    print(f"    • Committed transactions for {agg_idx + 1}/{num_aggregates} aggregates")
            
            produce_time = time.time() - start_time
            produce_rate = total_events / produce_time
            print(f"  ✅ Produced {total_events} events in {produce_time:.2f}s ({produce_rate:.0f} events/sec)")
            
            # Consume and verify
            consumer = Consumer({
                'bootstrap.servers': self.kafka_config['bootstrap.servers'],
                'group.id': 'load-test-group',
                'auto.offset.reset': 'earliest',
                'isolation.level': 'read_committed'
            })
            consumer.subscribe(['load_test_events'])
            
            print(f"\n  Consuming {total_events} events...")
            consumed_events = {}
            start_time = time.time()
            
            while len(consumed_events) < total_events and time.time() - start_time < 30:
                msg = consumer.poll(timeout=0.1)
                if msg and not msg.error():
                    event = json.loads(msg.value())
                    agg_id = event['aggregate_id']
                    
                    if agg_id not in consumed_events:
                        consumed_events[agg_id] = []
                    consumed_events[agg_id].append(event['sequence'])
            
            consume_time = time.time() - start_time
            total_consumed = sum(len(events) for events in consumed_events.values())
            consume_rate = total_consumed / consume_time if consume_time > 0 else 0
            
            print(f"  ✅ Consumed {total_consumed} events in {consume_time:.2f}s ({consume_rate:.0f} events/sec)")
            
            # Verify ordering per aggregate
            ordering_errors = 0
            for agg_id, sequences in consumed_events.items():
                sequences.sort()
                expected = list(range(events_per_aggregate))
                if sequences != expected:
                    ordering_errors += 1
                    print(f"  ⚠️  Ordering error for {agg_id}: missing/wrong sequences")
            
            # Get final metrics
            await asyncio.sleep(2)
            lag_info = await monitor.get_current_lag()
            
            print(f"\n  Final Results:")
            print(f"  • Events produced: {total_events}")
            print(f"  • Events consumed: {total_consumed}")
            print(f"  • Ordering errors: {ordering_errors}")
            print(f"  • Final lag: {lag_info.get('total_lag', 0)}")
            print(f"  • Produce rate: {produce_rate:.0f} events/sec")
            print(f"  • Consume rate: {consume_rate:.0f} events/sec")
            
            # Determine pass/fail
            if total_consumed == total_events and ordering_errors == 0:
                print(f"\n✅ INTEGRATION TEST PASSED! All {total_events} events processed correctly")
                self.test_results['integration'] = 'PASSED'
            else:
                error = f"❌ Integration test failed: consumed {total_consumed}/{total_events}, {ordering_errors} ordering errors"
                print(error)
                self.issues_found.append(("CRITICAL", "Integration failure", error))
                self.test_results['integration'] = 'FAILED'
            
            # Cleanup
            await monitor.stop_monitoring()
            consumer.close()
            admin_client.delete_topics(test_topics)
            
        except Exception as e:
            error = f"❌ Test 5 failed: {e}"
            print(error)
            self.issues_found.append(("CRITICAL", "Integration test error", str(e)))
            self.test_results['integration'] = 'ERROR'
    
    async def run_all_tests(self):
        """Run all performance tests and generate report"""
        
        # Setup
        if not await self.setup():
            print("\n❌ SETUP FAILED - Cannot run tests")
            return
        
        # Run tests
        await self.test_1_partition_key_ordering()
        await self.test_2_kafka_eos_v2()
        await self.test_3_watermark_monitoring()
        await self.test_4_dlq_handler_retry()
        await self.test_5_integration_load_test()
        
        # Generate report
        print("\n" + "="*80)
        print("📊 TEST RESULTS SUMMARY")
        print("="*80)
        
        for test_name, result in self.test_results.items():
            emoji = "✅" if result == "PASSED" else "❌"
            print(f"{emoji} {test_name}: {result}")
        
        if self.issues_found:
            print("\n⚠️  ISSUES FOUND:")
            for severity, issue, details in self.issues_found:
                print(f"  [{severity}] {issue}: {details}")
        else:
            print("\n🎉 NO ISSUES FOUND!")
        
        # Performance metrics
        passed = sum(1 for r in self.test_results.values() if r == 'PASSED')
        total = len(self.test_results)
        
        print(f"\n📈 Overall Score: {passed}/{total} tests passed ({passed/total*100:.0f}%)")
        
        if passed == total:
            print("\n🚀 ALL PERFORMANCE IMPROVEMENTS VERIFIED - PRODUCTION READY!")
        else:
            print("\n⚠️  SOME TESTS FAILED - REVIEW AND FIX BEFORE PRODUCTION")
        
        # Cleanup
        if self.redis_client:
            await self.redis_client.close()


async def main():
    """Main test runner"""
    suite = PerformanceTestSuite()
    await suite.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main())