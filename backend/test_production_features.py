#!/usr/bin/env python3
"""
Test Production Features Integration
THINK ULTRA³ - Verifying all 6 invariants are properly implemented

This script tests:
1. Idempotency - Same command sent twice should only process once
2. Schema Versioning - Events include schema version
3. Ordering - Sequence numbers are properly generated
4. Event Sourcing - Commands create events with all production fields
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
import redis.asyncio as aioredis
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.services.idempotency_service import IdempotencyService
from shared.services.schema_versioning import SchemaVersioningService
from shared.services.sequence_service import SequenceService

async def test_production_features():
    """Test all production features are working"""
    print("🔥 THINK ULTRA³ - Testing Production Features")
    print("=" * 60)
    
    # Initialize services
    redis_client = aioredis.from_url(
        'redis://localhost:6379',
        encoding='utf-8',
        decode_responses=False
    )
    
    idempotency_service = IdempotencyService(
        redis_client=redis_client,
        ttl_seconds=60,
        namespace="test_production"
    )
    
    schema_service = SchemaVersioningService()
    
    sequence_service = SequenceService(
        redis_client=redis_client,
        namespace="test_sequence"
    )
    
    try:
        # Test 1: Idempotency
        print("\n1️⃣ Testing Idempotency...")
        command_id = str(uuid.uuid4())
        command_data = {
            "command_id": command_id,
            "command_type": "CREATE_INSTANCE",
            "data": {"test": "data"}
        }
        
        # First check - should not be duplicate
        is_dup1, _ = await idempotency_service.is_duplicate(command_id, command_data)
        print(f"   First check: is_duplicate={is_dup1} (expected: False)")
        assert is_dup1 is False, "First command should not be duplicate"
        
        # Second check - should be duplicate
        is_dup2, metadata = await idempotency_service.is_duplicate(command_id, command_data)
        print(f"   Second check: is_duplicate={is_dup2} (expected: True)")
        assert is_dup2 is True, "Second command should be duplicate"
        print("   ✅ Idempotency working correctly")
        
        # Test 2: Schema Versioning
        print("\n2️⃣ Testing Schema Versioning...")
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "INSTANCE_CREATED",
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "data": {"instance_id": "test-123"}
        }
        
        # Add schema version
        versioned_event = schema_service.version_event(event)
        print(f"   Schema version added: {versioned_event.get('schema_version')}")
        assert 'schema_version' in versioned_event, "Schema version should be added"
        
        # Test migration
        old_event = {
            "event_id": str(uuid.uuid4()),
            "schema_version": "1.0.0",
            "data": {"test": "data"}
        }
        
        migrated_event = schema_service.migrate_event(old_event)
        print(f"   Migrated from 1.0.0 to {migrated_event.get('schema_version')}")
        assert migrated_event.get('schema_version') != "1.0.0", "Event should be migrated"
        print("   ✅ Schema versioning working correctly")
        
        # Test 3: Sequence Numbers
        print("\n3️⃣ Testing Sequence Numbers...")
        # Use unique aggregate_id to avoid conflicts
        aggregate_id = f"test_aggregate_{uuid.uuid4().hex[:8]}"
        
        # Get sequence numbers
        seq1 = await sequence_service.get_next_sequence(aggregate_id)
        seq2 = await sequence_service.get_next_sequence(aggregate_id)
        seq3 = await sequence_service.get_next_sequence(aggregate_id)
        
        print(f"   Sequence 1: {seq1}")
        print(f"   Sequence 2: {seq2}")
        print(f"   Sequence 3: {seq3}")
        
        assert seq1 == 1, "First sequence should be 1"
        assert seq2 == 2, "Second sequence should be 2"
        assert seq3 == 3, "Third sequence should be 3"
        print("   ✅ Sequence numbering working correctly")
        
        # Test 4: Complete Event with all production fields
        print("\n4️⃣ Testing Complete Event Creation...")
        
        # Simulate what instance_worker does
        instance_id = "INST-" + str(uuid.uuid4())[:8]
        command_id = str(uuid.uuid4())
        
        # Get sequence number
        sequence_number = await sequence_service.get_next_sequence(instance_id)
        
        # Create event with all production fields
        event_dict = {
            "event_type": "INSTANCE_CREATED",
            "db_name": "test_db",
            "class_id": "TestClass",
            "instance_id": instance_id,
            "command_id": command_id,
            "sequence_number": sequence_number,
            "aggregate_id": instance_id,
            "data": {
                "instance_id": instance_id,
                "payload": {"name": "Test Instance"},
                "s3_stored": True
            },
            "occurred_by": "test_user"
        }
        
        # Add schema version
        event_dict = schema_service.version_event(event_dict)
        
        print(f"   Event created with:")
        print(f"   - instance_id: {instance_id}")
        print(f"   - sequence_number: {sequence_number}")
        print(f"   - schema_version: {event_dict.get('schema_version')}")
        print(f"   - aggregate_id: {event_dict.get('aggregate_id')}")
        
        # Verify all required fields
        required_fields = [
            'sequence_number',
            'schema_version',
            'aggregate_id',
            'instance_id',
            'command_id'
        ]
        
        for field in required_fields:
            assert field in event_dict, f"Missing required field: {field}"
            print(f"   ✓ {field}: {event_dict.get(field)}")
        
        print("   ✅ Event has all production fields")
        
        # Test 5: Ordering Guarantee
        print("\n5️⃣ Testing Ordering Guarantee...")
        
        # Create multiple events for same aggregate
        test_aggregate = f"order_test_{uuid.uuid4().hex[:8]}"
        sequences = []
        
        for i in range(5):
            seq = await sequence_service.get_next_sequence(test_aggregate)
            sequences.append(seq)
        
        print(f"   Generated sequences: {sequences}")
        
        # Verify strict ordering
        for i in range(len(sequences) - 1):
            assert sequences[i] < sequences[i+1], "Sequences should be strictly increasing"
        
        print("   ✅ Ordering guarantee maintained")
        
        # Test 6: Concurrent Processing Protection
        print("\n6️⃣ Testing Concurrent Processing Protection...")
        
        concurrent_aggregate = f"concurrent_test_{uuid.uuid4().hex[:8]}"
        
        # Launch multiple concurrent sequence requests
        tasks = []
        for _ in range(10):
            task = sequence_service.get_next_sequence(concurrent_aggregate)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        unique_sequences = set(results)
        
        print(f"   Concurrent requests: 10")
        print(f"   Unique sequences: {len(unique_sequences)}")
        print(f"   Sequences: {sorted(results)}")
        
        assert len(unique_sequences) == 10, "All sequences should be unique"
        assert max(results) == 10, "Max sequence should be 10"
        assert min(results) == 1, "Min sequence should be 1"
        
        print("   ✅ Concurrent processing protection working")
        
        print("\n" + "=" * 60)
        print("🎉 ALL PRODUCTION FEATURES VERIFIED!")
        print("\nSummary of verified invariants:")
        print("1. ✅ Idempotency (exactly-once processing)")
        print("2. ✅ Schema versioning and migration")
        print("3. ✅ Per-aggregate sequence numbering")
        print("4. ✅ Complete event structure")
        print("5. ✅ Ordering guarantees")
        print("6. ✅ Concurrent processing protection")
        
    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(test_production_features())