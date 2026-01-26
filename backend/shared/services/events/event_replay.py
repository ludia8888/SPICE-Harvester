#!/usr/bin/env python3
"""
Event Replay Service
THINK ULTRA³ - S3 based deterministic replay

Replays events from S3 to reconstruct state at any point in time
NO MOCKS, REAL EVENT SOURCING
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import boto3
from botocore.client import Config
import hashlib
from collections import defaultdict

class EventReplayService:
    """
    Deterministic event replay from S3 storage
    Reconstructs complete state history from immutable event log
    """
    
    def __init__(
        self,
        s3_endpoint: str = "http://localhost:9000",
        s3_access_key: str = "minioadmin",
        s3_secret_key: str = "minioadmin123",
        bucket_name: str = "instance-events"
    ):
        self.s3_client = boto3.client(
            's3',
            endpoint_url=s3_endpoint,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        self.bucket_name = bucket_name
        
    async def replay_aggregate(
        self, 
        db_name: str, 
        class_id: str, 
        aggregate_id: str,
        up_to_sequence: Optional[int] = None,
        up_to_timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Replay all events for a specific aggregate
        
        Args:
            db_name: Database name
            class_id: Class/Type ID
            aggregate_id: Aggregate instance ID
            up_to_sequence: Replay up to this sequence number (inclusive)
            up_to_timestamp: Replay up to this timestamp
            
        Returns:
            Final state after replay
        """
        print(f"🔄 Replaying aggregate: {aggregate_id}")
        
        # List all events for this aggregate
        prefix = f"{db_name}/{class_id}/{aggregate_id}/"
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=prefix,
            MaxKeys=1000
        )
        
        if 'Contents' not in response:
            return {
                "aggregate_id": aggregate_id,
                "status": "NOT_FOUND",
                "event_count": 0,
                "final_state": None
            }
        
        # Load all events
        events = []
        for obj in response['Contents']:
            obj_data = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=obj['Key']
            )
            content = json.loads(obj_data['Body'].read())
            events.append({
                "key": obj['Key'],
                "timestamp": obj['LastModified'],
                "content": content
            })
        
        # Sort by sequence number if available, else by timestamp
        events.sort(key=lambda x: (
            x['content'].get('sequence_number', 0),
            x['timestamp']
        ))
        
        # Apply filters
        filtered_events = []
        for event in events:
            content = event['content']
            
            # Check sequence filter
            if up_to_sequence is not None:
                seq = content.get('sequence_number', 0)
                if seq > up_to_sequence:
                    continue
            
            # Check timestamp filter
            if up_to_timestamp is not None:
                if event['timestamp'].replace(tzinfo=timezone.utc) > up_to_timestamp:
                    continue
            
            filtered_events.append(event)
        
        # Replay events to build final state
        state = {
            "aggregate_id": aggregate_id,
            "class_id": class_id,
            "version": 0,
            "created_at": None,
            "updated_at": None,
            "deleted": False,
            "data": {}
        }
        
        for event in filtered_events:
            state = self._apply_event(state, event['content'])
        
        # Calculate state hash for verification
        state_hash = hashlib.sha256(
            json.dumps(state, sort_keys=True, default=str).encode()
        ).hexdigest()
        
        return {
            "aggregate_id": aggregate_id,
            "status": "REPLAYED",
            "event_count": len(filtered_events),
            "total_events": len(events),
            "final_state": state,
            "state_hash": state_hash,
            "replay_timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    def _apply_event(self, state: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply a single event to the current state
        
        This is the core of Event Sourcing - each event transforms the state
        """
        command_type = event.get('command_type')
        
        if command_type == 'CREATE_INSTANCE':
            # Create event - initialize state
            state['version'] = 1
            state['created_at'] = event.get('created_at', event.get('timestamp'))
            state['updated_at'] = state['created_at']
            state['data'] = event.get('payload', {})
            state['deleted'] = False
            
        elif command_type == 'UPDATE_INSTANCE':
            # Update event - merge changes
            state['version'] += 1
            state['updated_at'] = event.get('timestamp')
            payload = event.get('payload', {})
            state['data'].update(payload)
            
        elif command_type == 'DELETE_INSTANCE':
            # Delete event - mark as deleted but preserve data
            state['version'] += 1
            state['deleted'] = True
            state['deleted_at'] = event.get('timestamp')
            state['deleted_by'] = event.get('created_by', 'unknown')
            
        # Add event metadata
        state['last_event_id'] = event.get('command_id')
        state['last_sequence'] = event.get('sequence_number', state.get('last_sequence', 0))
        
        return state
    
    async def replay_all_aggregates(
        self,
        db_name: str,
        class_id: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Replay all aggregates of a specific class
        
        Returns:
            Dictionary of all aggregate states
        """
        print(f"🔄 Replaying all {class_id} aggregates in {db_name}")
        
        # List all objects for this class
        prefix = f"{db_name}/{class_id}/"
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=prefix,
            MaxKeys=1000
        )
        
        if 'Contents' not in response:
            return {
                "status": "NO_AGGREGATES",
                "aggregate_count": 0,
                "aggregates": {}
            }
        
        # Group by aggregate ID
        aggregates = defaultdict(list)
        for obj in response['Contents']:
            parts = obj['Key'].split('/')
            if len(parts) >= 3:
                aggregate_id = parts[2]
                aggregates[aggregate_id].append(obj)
        
        # Replay each aggregate
        results = {}
        for aggregate_id in list(aggregates.keys())[:limit]:
            result = await self.replay_aggregate(db_name, class_id, aggregate_id)
            results[aggregate_id] = result
        
        return {
            "status": "REPLAYED",
            "aggregate_count": len(results),
            "total_aggregates": len(aggregates),
            "aggregates": results,
            "replay_timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    async def point_in_time_replay(
        self,
        db_name: str,
        class_id: str,
        aggregate_id: str,
        target_time: datetime
    ) -> Dict[str, Any]:
        """
        Replay aggregate state at a specific point in time
        
        This enables time-travel queries - see the state as it was at any past moment
        """
        print(f"⏰ Point-in-time replay for {aggregate_id} at {target_time}")
        
        return await self.replay_aggregate(
            db_name=db_name,
            class_id=class_id,
            aggregate_id=aggregate_id,
            up_to_timestamp=target_time
        )
    
    async def verify_replay_determinism(
        self,
        db_name: str,
        class_id: str,
        aggregate_id: str
    ) -> Dict[str, Any]:
        """
        Verify that replaying produces deterministic results
        
        Replays multiple times and ensures identical results
        """
        print(f"🔍 Verifying replay determinism for {aggregate_id}")
        
        # Replay 3 times
        replays = []
        for i in range(3):
            result = await self.replay_aggregate(db_name, class_id, aggregate_id)
            replays.append(result['state_hash'])
        
        # Check all hashes are identical
        all_identical = len(set(replays)) == 1
        
        return {
            "aggregate_id": aggregate_id,
            "deterministic": all_identical,
            "hashes": replays,
            "verification": "PASSED" if all_identical else "FAILED"
        }
    
    async def get_aggregate_history(
        self,
        db_name: str,
        class_id: str,
        aggregate_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get complete event history for an aggregate
        
        Returns all events in chronological order
        """
        print(f"📜 Getting history for {aggregate_id}")
        
        prefix = f"{db_name}/{class_id}/{aggregate_id}/"
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=prefix,
            MaxKeys=1000
        )
        
        if 'Contents' not in response:
            return []
        
        # Load all events with metadata
        events = []
        for obj in response['Contents']:
            obj_data = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=obj['Key']
            )
            content = json.loads(obj_data['Body'].read())
            
            events.append({
                "timestamp": obj['LastModified'].isoformat(),
                "key": obj['Key'],
                "size": obj['Size'],
                "command_type": content.get('command_type'),
                "command_id": content.get('command_id'),
                "sequence_number": content.get('sequence_number', 0),
                "created_by": content.get('created_by', 'unknown')
            })
        
        # Sort chronologically
        events.sort(key=lambda x: (x['sequence_number'], x['timestamp']))
        
        return events


async def demo_replay():
    """
    Demo the replay functionality
    """
    replay_service = EventReplayService()
    
    # Test replay for a sample aggregate
    result = await replay_service.replay_aggregate(
        db_name="integration_test_db",
        class_id="IntegrationProduct",
        aggregate_id="IntegrationProduct_inst_08121140"
    )
    
    print("\n📊 REPLAY RESULT:")
    print(f"   Status: {result['status']}")
    print(f"   Events: {result['event_count']}")
    if result['final_state']:
        print(f"   Final Version: {result['final_state']['version']}")
        print(f"   State Hash: {result['state_hash'][:16]}...")
    
    # Verify determinism
    determinism = await replay_service.verify_replay_determinism(
        db_name="integration_test_db",
        class_id="IntegrationProduct",
        aggregate_id="IntegrationProduct_inst_08121140"
    )
    
    print("\n🔍 DETERMINISM CHECK:")
    print(f"   Result: {determinism['verification']}")
    print(f"   Hashes: {determinism['hashes'][0][:16]}...")
    
    return result


if __name__ == "__main__":
    result = asyncio.run(demo_replay())
    print("\n✅ Event Replay Service ready!")