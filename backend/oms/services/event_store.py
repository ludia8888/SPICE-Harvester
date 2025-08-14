"""
🔥 THINK ULTRA! The REAL Event Store using S3/MinIO

This is the Single Source of Truth (SSoT) for all events.
PostgreSQL Outbox is just a delivery guarantee mechanism, NOT an event store.

Aligned with Palantir Foundry architecture:
- S3/MinIO = Immutable event log (SSoT)
- TerminusDB = Graph relationships  
- Elasticsearch = Search indexes
- PostgreSQL = Delivery guarantee only
"""

import json
import uuid
from datetime import datetime
from typing import List, AsyncIterator, Optional, Dict, Any
from pathlib import Path
import asyncio

import aioboto3
from botocore.exceptions import ClientError
from pydantic import BaseModel

from shared.config.service_config import ServiceConfig
import logging

logger = logging.getLogger(__name__)


class Event(BaseModel):
    """Immutable event stored in S3/MinIO"""
    event_id: str
    event_type: str
    aggregate_type: str
    aggregate_id: str
    aggregate_version: int
    timestamp: datetime
    actor: Optional[str] = None
    payload: Dict[str, Any]
    metadata: Dict[str, Any] = {}
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EventStore:
    """
    The REAL Event Store using S3/MinIO as Single Source of Truth.
    
    This is the authoritative, immutable event log that serves as the
    foundation for Event Sourcing. PostgreSQL Outbox is NOT an event store,
    it's just a delivery guarantee mechanism.
    
    Event Storage Structure:
    /events/{year}/{month}/{day}/{aggregate_type}/{aggregate_id}/{event_id}.json
    
    Index Structure:
    /indexes/by-aggregate/{aggregate_type}/{aggregate_id}/manifest.json
    /indexes/by-date/{year}/{month}/{day}/events.json
    /indexes/by-type/{event_type}/{year}/{month}/events.json
    """
    
    def __init__(self):
        self.endpoint_url = ServiceConfig.get_minio_endpoint()
        self.access_key = ServiceConfig.get_minio_access_key()
        self.secret_key = ServiceConfig.get_minio_secret_key()
        self.bucket_name = "spice-event-store"  # The SSoT bucket
        self.session = None
        self.s3_client = None
        
    async def connect(self):
        """Initialize S3/MinIO connection"""
        try:
            self.session = aioboto3.Session()
            
            async with self.session.client(
                's3',
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                use_ssl=False
            ) as s3:
                # Ensure bucket exists
                try:
                    await s3.head_bucket(Bucket=self.bucket_name)
                    logger.info(f"✅ Event Store bucket '{self.bucket_name}' exists")
                except ClientError:
                    await s3.create_bucket(Bucket=self.bucket_name)
                    logger.info(f"✅ Created Event Store bucket '{self.bucket_name}'")
                    
                    # Set bucket versioning for immutability
                    await s3.put_bucket_versioning(
                        Bucket=self.bucket_name,
                        VersioningConfiguration={'Status': 'Enabled'}
                    )
                    logger.info("✅ Enabled versioning for immutability")
                    
        except Exception as e:
            logger.error(f"Failed to connect to S3/MinIO Event Store: {e}")
            raise
    
    async def append_event(self, event: Event) -> str:
        """
        Append an immutable event to S3/MinIO.
        This is the ONLY place where events are authoritatively stored.
        
        Returns:
            event_id: The unique identifier of the stored event
        """
        if not event.event_id:
            event.event_id = str(uuid.uuid4())
            
        # Build S3 key path
        dt = event.timestamp
        key = (
            f"events/{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/"
            f"{event.aggregate_type}/{event.aggregate_id}/{event.event_id}.json"
        )
        
        try:
            async with self.session.client(
                's3',
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                use_ssl=False
            ) as s3:
                # Store the immutable event
                await s3.put_object(
                    Bucket=self.bucket_name,
                    Key=key,
                    Body=event.json().encode('utf-8'),
                    ContentType='application/json',
                    Metadata={
                        'event-type': event.event_type,
                        'aggregate-type': event.aggregate_type,
                        'aggregate-id': event.aggregate_id,
                        'aggregate-version': str(event.aggregate_version),
                        'timestamp': event.timestamp.isoformat()
                    }
                )
                
                logger.info(
                    f"✅ Event stored in S3/MinIO (SSoT): {event.event_id} "
                    f"[{event.event_type}] for {event.aggregate_type}/{event.aggregate_id}"
                )
                
                # Update indexes asynchronously
                asyncio.create_task(self._update_indexes(event, key))
                
                return event.event_id
                
        except Exception as e:
            logger.error(f"Failed to store event in S3/MinIO: {e}")
            raise
    
    async def get_events(
        self, 
        aggregate_type: str, 
        aggregate_id: str,
        from_version: Optional[int] = None,
        to_version: Optional[int] = None
    ) -> List[Event]:
        """
        Retrieve all events for an aggregate from S3/MinIO.
        This reads from the Single Source of Truth.
        """
        events = []
        
        try:
            async with self.session.client(
                's3',
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                use_ssl=False
            ) as s3:
                # List all events for this aggregate
                prefix = f"events/"
                paginator = s3.get_paginator('list_objects_v2')
                
                async for page in paginator.paginate(
                    Bucket=self.bucket_name,
                    Prefix=prefix
                ):
                    if 'Contents' not in page:
                        continue
                        
                    for obj in page['Contents']:
                        key = obj['Key']
                        
                        # Check if this event belongs to our aggregate
                        if f"/{aggregate_type}/{aggregate_id}/" not in key:
                            continue
                            
                        # Get the event
                        response = await s3.get_object(
                            Bucket=self.bucket_name,
                            Key=key
                        )
                        
                        event_data = await response['Body'].read()
                        event = Event.parse_raw(event_data)
                        
                        # Filter by version if specified
                        if from_version and event.aggregate_version < from_version:
                            continue
                        if to_version and event.aggregate_version > to_version:
                            continue
                            
                        events.append(event)
                
                # Sort by version
                events.sort(key=lambda e: e.aggregate_version)
                
                logger.info(
                    f"Retrieved {len(events)} events for "
                    f"{aggregate_type}/{aggregate_id} from S3/MinIO"
                )
                
                return events
                
        except Exception as e:
            logger.error(f"Failed to retrieve events from S3/MinIO: {e}")
            raise
    
    async def replay_events(
        self,
        from_timestamp: datetime,
        to_timestamp: Optional[datetime] = None,
        event_types: Optional[List[str]] = None
    ) -> AsyncIterator[Event]:
        """
        Replay events from S3/MinIO for a time range.
        This is used for system recovery, debugging, or building new projections.
        """
        try:
            async with self.session.client(
                's3',
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                use_ssl=False
            ) as s3:
                # Build prefix from date range
                start_year = from_timestamp.year
                start_month = from_timestamp.month
                start_day = from_timestamp.day
                
                prefix = f"events/{start_year:04d}/"
                
                paginator = s3.get_paginator('list_objects_v2')
                
                async for page in paginator.paginate(
                    Bucket=self.bucket_name,
                    Prefix=prefix
                ):
                    if 'Contents' not in page:
                        continue
                        
                    for obj in page['Contents']:
                        key = obj['Key']
                        
                        # Get the event
                        response = await s3.get_object(
                            Bucket=self.bucket_name,
                            Key=key
                        )
                        
                        event_data = await response['Body'].read()
                        event = Event.parse_raw(event_data)
                        
                        # Filter by timestamp
                        if event.timestamp < from_timestamp:
                            continue
                        if to_timestamp and event.timestamp > to_timestamp:
                            continue
                            
                        # Filter by event type
                        if event_types and event.event_type not in event_types:
                            continue
                            
                        yield event
                        
        except Exception as e:
            logger.error(f"Failed to replay events from S3/MinIO: {e}")
            raise
    
    async def get_aggregate_version(
        self,
        aggregate_type: str,
        aggregate_id: str
    ) -> int:
        """
        Get the current version of an aggregate by counting events in S3.
        """
        events = await self.get_events(aggregate_type, aggregate_id)
        return len(events)
    
    async def _update_indexes(self, event: Event, key: str):
        """
        Update various indexes for efficient querying.
        Indexes are derived data, not the source of truth.
        """
        try:
            async with self.session.client(
                's3',
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                use_ssl=False
            ) as s3:
                # Update aggregate index
                aggregate_index_key = (
                    f"indexes/by-aggregate/{event.aggregate_type}/"
                    f"{event.aggregate_id}/events.json"
                )
                
                # Get existing index or create new
                try:
                    response = await s3.get_object(
                        Bucket=self.bucket_name,
                        Key=aggregate_index_key
                    )
                    index_data = await response['Body'].read()
                    index = json.loads(index_data)
                except ClientError:
                    index = {"events": [], "last_version": 0}
                
                # Add event reference to index
                index["events"].append({
                    "event_id": event.event_id,
                    "version": event.aggregate_version,
                    "timestamp": event.timestamp.isoformat(),
                    "s3_key": key
                })
                index["last_version"] = event.aggregate_version
                
                # Save updated index
                await s3.put_object(
                    Bucket=self.bucket_name,
                    Key=aggregate_index_key,
                    Body=json.dumps(index).encode('utf-8'),
                    ContentType='application/json'
                )
                
                logger.debug(f"Updated aggregate index for {event.aggregate_type}/{event.aggregate_id}")
                
        except Exception as e:
            logger.warning(f"Failed to update indexes: {e}")
            # Index update failures don't fail the event append
    
    async def get_snapshot(
        self,
        aggregate_type: str,
        aggregate_id: str,
        version: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get a snapshot of an aggregate at a specific version.
        Snapshots are optional optimizations, not the source of truth.
        """
        snapshot_key = (
            f"snapshots/{aggregate_type}/{aggregate_id}/"
            f"v{version or 'latest'}.json"
        )
        
        try:
            async with self.session.client(
                's3',
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                use_ssl=False
            ) as s3:
                response = await s3.get_object(
                    Bucket=self.bucket_name,
                    Key=snapshot_key
                )
                
                snapshot_data = await response['Body'].read()
                return json.loads(snapshot_data)
                
        except ClientError:
            return None
    
    async def save_snapshot(
        self,
        aggregate_type: str,
        aggregate_id: str,
        version: int,
        state: Dict[str, Any]
    ):
        """
        Save a snapshot for performance optimization.
        Remember: Events are the truth, snapshots are just caches.
        """
        snapshot_key = f"snapshots/{aggregate_type}/{aggregate_id}/v{version}.json"
        latest_key = f"snapshots/{aggregate_type}/{aggregate_id}/vlatest.json"
        
        snapshot = {
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "version": version,
            "timestamp": datetime.utcnow().isoformat(),
            "state": state
        }
        
        try:
            async with self.session.client(
                's3',
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                use_ssl=False
            ) as s3:
                # Save versioned snapshot
                await s3.put_object(
                    Bucket=self.bucket_name,
                    Key=snapshot_key,
                    Body=json.dumps(snapshot).encode('utf-8'),
                    ContentType='application/json'
                )
                
                # Update latest snapshot
                await s3.put_object(
                    Bucket=self.bucket_name,
                    Key=latest_key,
                    Body=json.dumps(snapshot).encode('utf-8'),
                    ContentType='application/json'
                )
                
                logger.info(
                    f"Saved snapshot for {aggregate_type}/{aggregate_id} "
                    f"at version {version}"
                )
                
        except Exception as e:
            logger.warning(f"Failed to save snapshot: {e}")
            # Snapshot failures don't affect the system


# Global instance
event_store = EventStore()


async def get_event_store() -> EventStore:
    """Dependency to get the Event Store instance"""
    return event_store