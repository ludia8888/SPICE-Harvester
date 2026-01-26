"""
Idempotency Service for Event Processing
THINK ULTRA³ - Ensuring exactly-once processing semantics

This service provides idempotency guarantees for event processing,
preventing duplicate event processing in distributed systems.
"""

import asyncio
import json
import hashlib
from typing import Optional, Dict, Any, Tuple
from datetime import datetime, timezone, timedelta
import redis.asyncio as aioredis
import logging

logger = logging.getLogger(__name__)


class IdempotencyService:
    """
    Provides idempotency guarantees for event processing.
    
    Uses Redis SET NX (set if not exists) with TTL for distributed lock
    and deduplication. Ensures exactly-once processing semantics.
    """
    
    def __init__(
        self,
        redis_client: aioredis.Redis,
        ttl_seconds: int = 86400,  # 24 hours default
        namespace: str = "idempotency"
    ):
        """
        Initialize idempotency service.
        
        Args:
            redis_client: Async Redis client
            ttl_seconds: TTL for idempotency keys (default 24 hours)
            namespace: Redis key namespace
        """
        self.redis = redis_client
        self.ttl_seconds = ttl_seconds
        self.namespace = namespace
        
    def _generate_key(self, event_id: str, aggregate_id: Optional[str] = None) -> str:
        """
        Generate Redis key for idempotency check.
        
        Args:
            event_id: Unique event identifier
            aggregate_id: Optional aggregate identifier for partitioning
            
        Returns:
            Redis key string
        """
        if aggregate_id:
            return f"{self.namespace}:{aggregate_id}:{event_id}"
        return f"{self.namespace}:{event_id}"
    
    def _generate_event_hash(self, event_data: Dict[str, Any]) -> str:
        """
        Generate deterministic hash of event data.
        
        Args:
            event_data: Event payload
            
        Returns:
            SHA256 hash of canonical JSON
        """
        # Sort keys for deterministic JSON
        canonical_json = json.dumps(event_data, sort_keys=True)
        return hashlib.sha256(canonical_json.encode()).hexdigest()
    
    async def is_duplicate(
        self,
        event_id: str,
        event_data: Optional[Dict[str, Any]] = None,
        aggregate_id: Optional[str] = None
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Check if event is duplicate and acquire processing lock.
        
        Args:
            event_id: Unique event identifier
            event_data: Optional event payload for hash comparison
            aggregate_id: Optional aggregate identifier
            
        Returns:
            Tuple of (is_duplicate, stored_metadata)
        """
        key = self._generate_key(event_id, aggregate_id)
        
        # Prepare metadata to store
        metadata = {
            "event_id": event_id,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "ttl_seconds": self.ttl_seconds
        }
        
        if event_data:
            metadata["event_hash"] = self._generate_event_hash(event_data)
            
        if aggregate_id:
            metadata["aggregate_id"] = aggregate_id
        
        # Try to set with NX (only if not exists)
        success = await self.redis.set(
            key,
            json.dumps(metadata),
            nx=True,  # Only set if not exists
            ex=self.ttl_seconds  # Expire after TTL
        )
        
        if success:
            # First time processing this event
            logger.debug(f"Event {event_id} registered for processing")
            return False, None
        else:
            # Event already processed or being processed
            stored_data = await self.redis.get(key)
            if stored_data:
                stored_metadata = json.loads(stored_data)
                logger.warning(
                    f"Duplicate event detected: {event_id}, "
                    f"originally processed at {stored_metadata.get('processed_at')}"
                )
                return True, stored_metadata
            else:
                # Key expired between check and get (rare edge case)
                logger.warning(f"Event {event_id} key expired during check")
                return False, None
    
    async def mark_processed(
        self,
        event_id: str,
        result: Optional[Dict[str, Any]] = None,
        aggregate_id: Optional[str] = None
    ) -> bool:
        """
        Mark event as successfully processed with optional result.
        
        Args:
            event_id: Unique event identifier
            result: Optional processing result to store
            aggregate_id: Optional aggregate identifier
            
        Returns:
            Success status
        """
        key = self._generate_key(event_id, aggregate_id)
        
        # Update metadata with result
        metadata = {
            "event_id": event_id,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "status": "completed"
        }
        
        if result:
            metadata["result"] = result
            
        if aggregate_id:
            metadata["aggregate_id"] = aggregate_id
        
        # Update with extended TTL
        await self.redis.setex(
            key,
            self.ttl_seconds,
            json.dumps(metadata)
        )
        
        logger.debug(f"Event {event_id} marked as processed")
        return True
    
    async def mark_failed(
        self,
        event_id: str,
        error: str,
        aggregate_id: Optional[str] = None,
        retry_after: Optional[int] = None
    ) -> bool:
        """
        Mark event as failed with error details.
        
        Args:
            event_id: Unique event identifier
            error: Error message
            aggregate_id: Optional aggregate identifier
            retry_after: Optional seconds before retry allowed
            
        Returns:
            Success status
        """
        key = self._generate_key(event_id, aggregate_id)
        
        # Store failure metadata
        metadata = {
            "event_id": event_id,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "status": "failed",
            "error": error
        }
        
        if aggregate_id:
            metadata["aggregate_id"] = aggregate_id
            
        if retry_after:
            metadata["retry_after"] = (
                datetime.now(timezone.utc) + timedelta(seconds=retry_after)
            ).isoformat()
        
        # Use shorter TTL for failed events to allow retry
        ttl = retry_after if retry_after else 300  # 5 minutes default
        
        await self.redis.setex(
            key,
            ttl,
            json.dumps(metadata)
        )
        
        logger.error(f"Event {event_id} marked as failed: {error}")
        return True
    
    async def get_processing_status(
        self,
        event_id: str,
        aggregate_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get current processing status of an event.
        
        Args:
            event_id: Unique event identifier
            aggregate_id: Optional aggregate identifier
            
        Returns:
            Processing metadata if exists
        """
        key = self._generate_key(event_id, aggregate_id)
        
        stored_data = await self.redis.get(key)
        if stored_data:
            return json.loads(stored_data)
        return None
    
    async def cleanup_expired(self, pattern: Optional[str] = None) -> int:
        """
        Clean up expired idempotency keys (Redis handles this automatically).
        
        Args:
            pattern: Optional pattern to match keys
            
        Returns:
            Number of keys checked
        """
        # Redis automatically expires keys, this is for manual cleanup if needed
        search_pattern = f"{self.namespace}:{pattern}*" if pattern else f"{self.namespace}:*"
        
        count = 0
        async for key in self.redis.scan_iter(match=search_pattern):
            # Check if key has TTL
            ttl = await self.redis.ttl(key)
            if ttl == -1:  # No expiration set
                # Set expiration to default TTL
                await self.redis.expire(key, self.ttl_seconds)
                count += 1
                
        logger.info(f"Cleaned up {count} keys without TTL")
        return count


class IdempotentEventProcessor:
    """
    Wrapper for idempotent event processing.
    
    Ensures events are processed exactly once even in case of
    retries, failures, or duplicate deliveries.
    """
    
    def __init__(self, idempotency_service: IdempotencyService):
        """
        Initialize idempotent processor.
        
        Args:
            idempotency_service: Idempotency service instance
        """
        self.idempotency = idempotency_service
        
    async def process_event(
        self,
        event_id: str,
        event_data: Dict[str, Any],
        processor_func: callable,
        aggregate_id: Optional[str] = None
    ) -> Tuple[bool, Any]:
        """
        Process event with idempotency guarantee.
        
        Args:
            event_id: Unique event identifier
            event_data: Event payload
            processor_func: Async function to process event
            aggregate_id: Optional aggregate identifier
            
        Returns:
            Tuple of (processed, result)
        """
        # Check for duplicate
        is_duplicate, metadata = await self.idempotency.is_duplicate(
            event_id,
            event_data,
            aggregate_id
        )
        
        if is_duplicate:
            logger.info(f"Skipping duplicate event: {event_id}")
            return False, metadata
        
        try:
            # Process event
            result = await processor_func(event_data)
            
            # Mark as successfully processed
            await self.idempotency.mark_processed(
                event_id,
                result,
                aggregate_id
            )
            
            return True, result
            
        except Exception as e:
            # Mark as failed
            await self.idempotency.mark_failed(
                event_id,
                str(e),
                aggregate_id,
                retry_after=60  # Allow retry after 1 minute
            )
            raise