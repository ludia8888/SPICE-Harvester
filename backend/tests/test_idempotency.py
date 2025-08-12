#!/usr/bin/env python3
"""
Test Idempotency Service
THINK ULTRA³ - Verifying exactly-once processing semantics

Tests idempotency guarantees for event processing,
ensuring duplicate events are properly handled.
"""

import asyncio
import json
import uuid
from datetime import datetime
import redis.asyncio as aioredis
import pytest

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.services.idempotency_service import IdempotencyService, IdempotentEventProcessor


class TestIdempotency:
    """Test suite for idempotency service"""
    
    @pytest.fixture
    async def redis_client(self):
        """Create Redis client for testing"""
        client = aioredis.from_url(
            'redis://localhost:6379',
            encoding='utf-8',
            decode_responses=True
        )
        yield client
        await client.close()
    
    @pytest.fixture
    async def idempotency_service(self, redis_client):
        """Create idempotency service instance"""
        service = IdempotencyService(
            redis_client=redis_client,
            ttl_seconds=60,  # Short TTL for testing
            namespace="test_idempotency"
        )
        # Clean up test keys before and after
        await redis_client.flushdb()
        yield service
        await redis_client.flushdb()
    
    @pytest.mark.asyncio
    async def test_duplicate_detection(self, idempotency_service):
        """Test that duplicate events are properly detected"""
        event_id = str(uuid.uuid4())
        event_data = {
            "type": "test_event",
            "data": "test_data"
        }
        
        # First check - should not be duplicate
        is_dup1, metadata1 = await idempotency_service.is_duplicate(event_id, event_data)
        assert is_dup1 is False
        assert metadata1 is None
        
        # Second check - should be duplicate
        is_dup2, metadata2 = await idempotency_service.is_duplicate(event_id, event_data)
        assert is_dup2 is True
        assert metadata2 is not None
        assert metadata2['event_id'] == event_id
    
    @pytest.mark.asyncio
    async def test_event_processing_idempotency(self, idempotency_service):
        """Test idempotent event processing"""
        processor = IdempotentEventProcessor(idempotency_service)
        
        event_id = str(uuid.uuid4())
        event_data = {
            "event_id": event_id,
            "type": "test_event",
            "value": 42
        }
        
        # Track processing count
        process_count = 0
        
        async def process_func(data):
            nonlocal process_count
            process_count += 1
            return {"processed": True, "value": data["value"] * 2}
        
        # First processing - should execute
        processed1, result1 = await processor.process_event(
            event_id=event_id,
            event_data=event_data,
            processor_func=process_func
        )
        
        assert processed1 is True
        assert result1["value"] == 84
        assert process_count == 1
        
        # Second processing - should skip
        processed2, result2 = await processor.process_event(
            event_id=event_id,
            event_data=event_data,
            processor_func=process_func
        )
        
        assert processed2 is False
        assert process_count == 1  # Should not increment
    
    @pytest.mark.asyncio
    async def test_aggregate_level_idempotency(self, idempotency_service):
        """Test idempotency at aggregate level"""
        aggregate_id = "aggregate_123"
        
        # Different events for same aggregate
        event1_id = str(uuid.uuid4())
        event2_id = str(uuid.uuid4())
        
        # Both should process initially
        is_dup1, _ = await idempotency_service.is_duplicate(
            event1_id, aggregate_id=aggregate_id
        )
        is_dup2, _ = await idempotency_service.is_duplicate(
            event2_id, aggregate_id=aggregate_id
        )
        
        assert is_dup1 is False
        assert is_dup2 is False
        
        # Duplicate of event1 should be detected
        is_dup3, _ = await idempotency_service.is_duplicate(
            event1_id, aggregate_id=aggregate_id
        )
        assert is_dup3 is True
    
    @pytest.mark.asyncio
    async def test_failure_handling(self, idempotency_service):
        """Test handling of failed event processing"""
        processor = IdempotentEventProcessor(idempotency_service)
        
        event_id = str(uuid.uuid4())
        event_data = {"event_id": event_id, "fail": True}
        
        async def failing_processor(data):
            raise ValueError("Processing failed")
        
        # Processing should fail and mark as failed
        with pytest.raises(ValueError):
            await processor.process_event(
                event_id=event_id,
                event_data=event_data,
                processor_func=failing_processor
            )
        
        # Check that failure was recorded
        status = await idempotency_service.get_processing_status(event_id)
        assert status is not None
        assert status['status'] == 'failed'
        assert 'error' in status
    
    @pytest.mark.asyncio
    async def test_concurrent_processing(self, idempotency_service):
        """Test concurrent processing of same event"""
        processor = IdempotentEventProcessor(idempotency_service)
        
        event_id = str(uuid.uuid4())
        event_data = {"event_id": event_id, "concurrent": True}
        
        process_count = 0
        
        async def slow_processor(data):
            nonlocal process_count
            await asyncio.sleep(0.1)  # Simulate slow processing
            process_count += 1
            return {"processed": True}
        
        # Launch multiple concurrent processors
        tasks = []
        for _ in range(5):
            task = processor.process_event(
                event_id=event_id,
                event_data=event_data,
                processor_func=slow_processor
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        
        # Only one should have actually processed
        assert process_count == 1
        
        # Count successful vs duplicate results
        processed_count = sum(1 for processed, _ in results if processed)
        assert processed_count == 1


async def run_tests():
    """Run all idempotency tests"""
    print("🧪 Testing Idempotency Service")
    print("=" * 60)
    
    # Initialize Redis
    redis_client = aioredis.from_url(
        'redis://localhost:6379',
        encoding='utf-8',
        decode_responses=True
    )
    
    # Create service
    service = IdempotencyService(
        redis_client=redis_client,
        ttl_seconds=60,
        namespace="test"
    )
    
    # Clean test namespace
    await redis_client.flushdb()
    
    try:
        # Test 1: Basic duplicate detection
        print("\n1️⃣ Testing duplicate detection...")
        event_id = str(uuid.uuid4())
        
        is_dup1, _ = await service.is_duplicate(event_id)
        print(f"   First check: duplicate={is_dup1} (expected: False)")
        assert is_dup1 is False
        
        is_dup2, metadata = await service.is_duplicate(event_id)
        print(f"   Second check: duplicate={is_dup2} (expected: True)")
        assert is_dup2 is True
        print("   ✅ Duplicate detection working")
        
        # Test 2: Idempotent processing
        print("\n2️⃣ Testing idempotent processing...")
        processor = IdempotentEventProcessor(service)
        
        event_id2 = str(uuid.uuid4())
        event_data = {"id": event_id2, "value": 10}
        
        process_count = 0
        
        async def test_processor(data):
            nonlocal process_count
            process_count += 1
            return {"result": data["value"] * 2}
        
        # First processing
        processed1, result1 = await processor.process_event(
            event_id=event_id2,
            event_data=event_data,
            processor_func=test_processor
        )
        print(f"   First processing: processed={processed1}, count={process_count}")
        
        # Duplicate processing
        processed2, result2 = await processor.process_event(
            event_id=event_id2,
            event_data=event_data,
            processor_func=test_processor
        )
        print(f"   Duplicate processing: processed={processed2}, count={process_count}")
        
        assert process_count == 1  # Should only process once
        print("   ✅ Idempotent processing working")
        
        # Test 3: Concurrent processing
        print("\n3️⃣ Testing concurrent processing...")
        event_id3 = str(uuid.uuid4())
        concurrent_count = 0
        
        async def concurrent_processor(data):
            nonlocal concurrent_count
            await asyncio.sleep(0.05)
            concurrent_count += 1
            return {"processed": True}
        
        # Launch 10 concurrent processors
        tasks = []
        for _ in range(10):
            task = processor.process_event(
                event_id=event_id3,
                event_data={"id": event_id3},
                processor_func=concurrent_processor
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        
        print(f"   Concurrent attempts: 10")
        print(f"   Actual processing count: {concurrent_count}")
        assert concurrent_count == 1
        print("   ✅ Concurrent processing protection working")
        
        # Test 4: Failure handling
        print("\n4️⃣ Testing failure handling...")
        event_id4 = str(uuid.uuid4())
        
        async def failing_processor(data):
            raise ValueError("Intentional failure")
        
        try:
            await processor.process_event(
                event_id=event_id4,
                event_data={"id": event_id4},
                processor_func=failing_processor
            )
        except ValueError:
            pass
        
        status = await service.get_processing_status(event_id4)
        print(f"   Failed event status: {status['status']}")
        assert status['status'] == 'failed'
        print("   ✅ Failure handling working")
        
        print("\n" + "=" * 60)
        print("✅ All idempotency tests passed!")
        
    finally:
        await redis_client.close()


if __name__ == "__main__":
    asyncio.run(run_tests())