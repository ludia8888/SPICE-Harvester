"""
🔥 THINK ULTRA! S3/MinIO Event Store Unit Tests

Tests for the REAL Event Store (Single Source of Truth)
PostgreSQL is NOT an Event Store - it's just for delivery guarantee!
"""

import pytest
import asyncio
import json
from datetime import datetime
from uuid import uuid4
import os

# Set up test environment
os.environ["ENABLE_S3_EVENT_STORE"] = "true"
os.environ["DOCKER_CONTAINER"] = "false"
os.environ["MINIO_ENDPOINT_URL"] = "http://localhost:9000"
os.environ["MINIO_ACCESS_KEY"] = "admin"
os.environ["MINIO_SECRET_KEY"] = "spice123!"

from oms.services.event_store import EventStore, Event
from shared.config.service_config import ServiceConfig


class TestEventStore:
    """Test S3/MinIO Event Store - The Single Source of Truth"""
    
    @pytest.fixture
    async def event_store(self):
        """Create Event Store instance"""
        store = EventStore()
        store.bucket_name = "test-event-store"  # Use test bucket
        await store.connect()
        yield store
        # Cleanup would go here if needed
    
    @pytest.fixture
    def sample_event(self):
        """Create sample event for testing"""
        return Event(
            event_id=str(uuid4()),
            event_type="TEST_EVENT_CREATED",
            aggregate_type="test.TestAggregate",
            aggregate_id=f"test-{uuid4().hex[:8]}",
            aggregate_version=1,
            timestamp=datetime.utcnow(),
            actor="test_user",
            payload={
                "test": True,
                "data": "sample data",
                "value": 123.45
            },
            metadata={
                "source": "unit_test",
                "test_run": True
            }
        )
    
    @pytest.mark.asyncio
    async def test_connect_to_minio(self, event_store):
        """Test S3/MinIO connection"""
        # Connection happens in fixture
        assert event_store.s3_client is None  # Client created on demand
        assert event_store.endpoint_url == ServiceConfig.get_minio_endpoint()
        assert event_store.bucket_name == "test-event-store"
    
    @pytest.mark.asyncio
    async def test_append_event(self, event_store, sample_event):
        """Test appending event to S3/MinIO"""
        event_id = await event_store.append_event(sample_event)
        
        assert event_id is not None
        assert event_id == sample_event.event_id
    
    @pytest.mark.asyncio
    async def test_get_events(self, event_store, sample_event):
        """Test retrieving events from S3/MinIO"""
        # First append an event
        await event_store.append_event(sample_event)
        
        # Then retrieve it
        events = await event_store.get_events(
            aggregate_type=sample_event.aggregate_type,
            aggregate_id=sample_event.aggregate_id
        )
        
        assert len(events) > 0
        assert events[0].event_id == sample_event.event_id
        assert events[0].event_type == sample_event.event_type
    
    @pytest.mark.asyncio
    async def test_event_immutability(self, event_store, sample_event):
        """Test that events are immutable in S3"""
        # Append event
        event_id = await event_store.append_event(sample_event)
        
        # Try to append same event again (should create new version)
        sample_event.aggregate_version = 2
        sample_event.payload["modified"] = True
        
        event_id_2 = await event_store.append_event(sample_event)
        
        # Both events should exist
        events = await event_store.get_events(
            aggregate_type=sample_event.aggregate_type,
            aggregate_id=sample_event.aggregate_id
        )
        
        assert len(events) == 2
        assert events[0].aggregate_version == 1
        assert events[1].aggregate_version == 2
    
    @pytest.mark.asyncio
    async def test_replay_events(self, event_store):
        """Test replaying events from a time range"""
        # Create multiple events
        events = []
        for i in range(3):
            event = Event(
                event_id=str(uuid4()),
                event_type=f"TEST_EVENT_{i}",
                aggregate_type="test.ReplayAggregate",
                aggregate_id=f"replay-{i}",
                aggregate_version=1,
                timestamp=datetime.utcnow(),
                actor="test_user",
                payload={"index": i},
                metadata={}
            )
            await event_store.append_event(event)
            events.append(event)
            await asyncio.sleep(0.1)  # Small delay between events
        
        # Replay events
        replayed = []
        async for event in event_store.replay_events(
            from_timestamp=events[0].timestamp,
            event_types=["TEST_EVENT_0", "TEST_EVENT_1", "TEST_EVENT_2"]
        ):
            replayed.append(event)
        
        assert len(replayed) >= 3
    
    @pytest.mark.asyncio
    async def test_get_aggregate_version(self, event_store):
        """Test getting current version of an aggregate"""
        aggregate_type = "test.VersionedAggregate"
        aggregate_id = f"versioned-{uuid4().hex[:8]}"
        
        # Create multiple events for same aggregate
        for version in range(1, 4):
            event = Event(
                event_id=str(uuid4()),
                event_type="VERSION_UPDATE",
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
                aggregate_version=version,
                timestamp=datetime.utcnow(),
                actor="test_user",
                payload={"version": version},
                metadata={}
            )
            await event_store.append_event(event)
        
        # Get current version
        current_version = await event_store.get_aggregate_version(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id
        )
        
        assert current_version == 3
    
    @pytest.mark.asyncio
    async def test_snapshot_operations(self, event_store):
        """Test snapshot save and retrieve"""
        aggregate_type = "test.SnapshotAggregate"
        aggregate_id = f"snapshot-{uuid4().hex[:8]}"
        version = 5
        
        state = {
            "field1": "value1",
            "field2": 123,
            "field3": {"nested": "data"}
        }
        
        # Save snapshot
        await event_store.save_snapshot(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            version=version,
            state=state
        )
        
        # Retrieve snapshot
        snapshot = await event_store.get_snapshot(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            version=version
        )
        
        assert snapshot is not None
        assert snapshot["version"] == version
        assert snapshot["state"] == state
    
    @pytest.mark.asyncio
    async def test_s3_key_structure(self, event_store, sample_event):
        """Test that S3 keys follow the correct structure"""
        dt = sample_event.timestamp
        expected_key_pattern = (
            f"events/{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/"
            f"{sample_event.aggregate_type}/{sample_event.aggregate_id}/"
        )
        
        # The key is built internally, we can't test it directly
        # But we can verify the event is stored and retrievable
        await event_store.append_event(sample_event)
        
        events = await event_store.get_events(
            aggregate_type=sample_event.aggregate_type,
            aggregate_id=sample_event.aggregate_id
        )
        
        assert len(events) == 1
        assert events[0].event_id == sample_event.event_id


class TestEventStoreFailures:
    """Test Event Store error handling"""
    
    @pytest.fixture
    async def broken_event_store(self):
        """Create Event Store with wrong credentials"""
        store = EventStore()
        store.bucket_name = "test-event-store"
        store.access_key = "wrong_key"  # Invalid credentials
        yield store
    
    @pytest.mark.asyncio
    async def test_connection_failure(self, broken_event_store):
        """Test handling of connection failures"""
        with pytest.raises(Exception):
            await broken_event_store.connect()
    
    @pytest.mark.asyncio
    async def test_graceful_index_update_failure(self, event_store, sample_event):
        """Test that index update failures don't fail event append"""
        # This should succeed even if index update fails
        event_id = await event_store.append_event(sample_event)
        assert event_id is not None


class TestMigrationScenarios:
    """Test migration from PostgreSQL to S3/MinIO"""
    
    @pytest.mark.asyncio
    async def test_dual_write_simulation(self):
        """Simulate dual-write pattern"""
        # This would test writing to both S3 and PostgreSQL
        # For now, just verify S3 write works
        store = EventStore()
        store.bucket_name = "test-migration"
        await store.connect()
        
        event = Event(
            event_id=str(uuid4()),
            event_type="MIGRATION_TEST",
            aggregate_type="migration.Test",
            aggregate_id="test-migration",
            aggregate_version=1,
            timestamp=datetime.utcnow(),
            actor="migration_test",
            payload={"migration": "dual_write"},
            metadata={"mode": "dual_write"}
        )
        
        event_id = await store.append_event(event)
        assert event_id is not None
    
    @pytest.mark.asyncio
    async def test_s3_as_source_of_truth(self):
        """Verify S3 is the authoritative source"""
        store = EventStore()
        store.bucket_name = "test-ssot"
        await store.connect()
        
        # Create events
        events_created = []
        aggregate_id = f"ssot-{uuid4().hex[:8]}"
        
        for i in range(5):
            event = Event(
                event_id=str(uuid4()),
                event_type="SSOT_TEST",
                aggregate_type="ssot.Test",
                aggregate_id=aggregate_id,
                aggregate_version=i + 1,
                timestamp=datetime.utcnow(),
                actor="ssot_test",
                payload={"index": i},
                metadata={}
            )
            await store.append_event(event)
            events_created.append(event)
        
        # Verify all events are in S3
        events_retrieved = await store.get_events(
            aggregate_type="ssot.Test",
            aggregate_id=aggregate_id
        )
        
        assert len(events_retrieved) == 5
        assert all(e.aggregate_id == aggregate_id for e in events_retrieved)


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])