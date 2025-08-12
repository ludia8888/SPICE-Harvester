#!/usr/bin/env python3
"""
Test Transactional Producer Issue
THINK ULTRA - Find root cause of EOS v2 initialization failure
"""

import os
import uuid
from confluent_kafka import Producer, Consumer

# Force localhost
os.environ['DOCKER_CONTAINER'] = 'false'

def test_transactional_producer():
    """Test transactional producer initialization"""
    
    print("🔍 Testing Transactional Producer (EOS v2)...")
    print("-" * 60)
    
    # Test 1: Idempotent producer (no transactions)
    print("\n1. Testing idempotent producer (without transactions)...")
    try:
        config = {
            'bootstrap.servers': '127.0.0.1:9092',
            'client.id': 'idempotent-test',
            'enable.idempotence': True,
            'acks': 'all',
            'retries': 10,
            'max.in.flight.requests.per.connection': 5
        }
        
        producer = Producer(config)
        
        # Send test message
        producer.produce(
            topic='test-idempotent',
            value=b'idempotent message',
            key=b'test-key'
        )
        producer.flush(timeout=5)
        
        print("   ✅ Idempotent producer works!")
        
    except Exception as e:
        print(f"   ❌ Idempotent producer failed: {e}")
        return False
    
    # Test 2: Transactional producer
    print("\n2. Testing transactional producer (full EOS v2)...")
    try:
        transaction_id = f"test-txn-{uuid.uuid4().hex[:8]}"
        
        config = {
            'bootstrap.servers': '127.0.0.1:9092',
            'client.id': 'transactional-test',
            'transactional.id': transaction_id,
            'enable.idempotence': True,
            'acks': 'all',
            'retries': 10,
            'max.in.flight.requests.per.connection': 5
        }
        
        print(f"   Transaction ID: {transaction_id}")
        
        producer = Producer(config)
        
        # Initialize transactions
        print("   Initializing transactions...")
        producer.init_transactions(30)  # Timeout as positional argument
        print("   ✅ Transactions initialized!")
        
        # Begin transaction
        print("   Beginning transaction...")
        producer.begin_transaction()
        
        # Send messages
        for i in range(5):
            producer.produce(
                topic='test-transactional',
                value=f'message-{i}'.encode(),
                key=f'key-{i}'.encode()
            )
        
        # Commit transaction
        print("   Committing transaction...")
        producer.commit_transaction(10)  # Timeout as positional argument
        
        print("   ✅ Transactional producer works!")
        return True
        
    except Exception as e:
        print(f"   ❌ Transactional producer failed: {e}")
        print("\n   Likely causes:")
        print("   1. Broker not configured for transactions")
        print("   2. Missing transaction state log topic")
        print("   3. Insufficient replicas for transaction log")
        
        # Try to get more details
        print("\n   Checking broker configuration...")
        try:
            # Test if __transaction_state topic exists
            consumer = Consumer({
                'bootstrap.servers': '127.0.0.1:9092',
                'group.id': 'test-check'
            })
            
            metadata = consumer.list_topics(timeout=5)
            
            if '__transaction_state' in metadata.topics:
                print("   ✅ __transaction_state topic exists")
            else:
                print("   ❌ __transaction_state topic missing")
                print("   Fix: Broker needs transaction.state.log.replication.factor=1")
                print("        and transaction.state.log.min.isr=1 for single node")
            
            consumer.close()
            
        except Exception as check_error:
            print(f"   ❌ Could not check topics: {check_error}")
        
        return False
    
    finally:
        print("\n" + "-" * 60)

def test_simplified_transactions():
    """Test with simplified transaction config for single-node Kafka"""
    
    print("\n3. Testing simplified transaction config (single-node)...")
    
    # For single-node Kafka, we need minimal replication
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
        
        # Create transaction state topic with replication factor 1
        admin = AdminClient({'bootstrap.servers': '127.0.0.1:9092'})
        
        # Try to create __transaction_state topic
        transaction_topic = NewTopic(
            '__transaction_state',
            num_partitions=50,
            replication_factor=1,
            config={
                'min.insync.replicas': '1',
                'compression.type': 'uncompressed',
                'cleanup.policy': 'delete',
                'segment.ms': '604800000'
            }
        )
        
        print("   Creating __transaction_state topic...")
        result = admin.create_topics([transaction_topic])
        
        for topic, future in result.items():
            try:
                future.result()
                print(f"   ✅ Created {topic}")
            except Exception as e:
                if 'already exists' in str(e).lower():
                    print(f"   ℹ️  {topic} already exists")
                else:
                    print(f"   ❌ Failed to create {topic}: {e}")
        
        # Now try transactional producer again
        transaction_id = f"simple-txn-{uuid.uuid4().hex[:8]}"
        
        config = {
            'bootstrap.servers': '127.0.0.1:9092',
            'transactional.id': transaction_id,
            'enable.idempotence': True
        }
        
        producer = Producer(config)
        
        print("   Initializing transactions (simplified)...")
        producer.init_transactions(30)  # Timeout as positional argument
        print("   ✅ Simplified transactions work!")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Simplified transaction failed: {e}")
        return False

if __name__ == "__main__":
    # Test regular transactional producer
    success = test_transactional_producer()
    
    if not success:
        print("\n" + "=" * 60)
        print("Attempting workaround for single-node Kafka...")
        print("=" * 60)
        success = test_simplified_transactions()
    
    if success:
        print("\n✅ TRANSACTIONAL PRODUCER READY!")
    else:
        print("\n❌ TRANSACTIONAL PRODUCER NOT AVAILABLE")
        print("\nTo fix:")
        print("1. Edit /opt/homebrew/etc/kafka/server.properties")
        print("2. Add these lines:")
        print("   transaction.state.log.replication.factor=1")
        print("   transaction.state.log.min.isr=1")
        print("3. Restart Kafka: brew services restart kafka")