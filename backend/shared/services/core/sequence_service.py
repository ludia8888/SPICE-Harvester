"""
Sequence Service for Per-Aggregate Ordering
THINK ULTRA³ - Ensuring event ordering within aggregates

This service provides sequence number generation for events,
ensuring proper ordering within each aggregate.
"""

import asyncio
from typing import Dict, Optional
import redis.asyncio as aioredis
import logging

logger = logging.getLogger(__name__)


class SequenceService:
    """
    Provides sequence number generation for aggregates.
    
    Uses Redis atomic increment to ensure unique, ordered sequence numbers
    per aggregate across distributed workers.
    """
    
    def __init__(
        self,
        redis_client: aioredis.Redis,
        namespace: str = "sequence"
    ):
        """
        Initialize sequence service.
        
        Args:
            redis_client: Async Redis client
            namespace: Redis key namespace
        """
        self.redis = redis_client
        self.namespace = namespace
        self._local_cache: Dict[str, int] = {}
        
    def _get_key(self, aggregate_id: str) -> str:
        """
        Get Redis key for aggregate sequence.
        
        Args:
            aggregate_id: Aggregate identifier
            
        Returns:
            Redis key
        """
        return f"{self.namespace}:{aggregate_id}"
    
    async def get_next_sequence(self, aggregate_id: str) -> int:
        """
        Get next sequence number for aggregate.
        
        Args:
            aggregate_id: Aggregate identifier
            
        Returns:
            Next sequence number (1-based)
        """
        key = self._get_key(aggregate_id)
        
        # Atomic increment
        sequence = await self.redis.incr(key)
        
        # Update local cache
        self._local_cache[aggregate_id] = sequence
        
        logger.debug(f"Generated sequence {sequence} for aggregate {aggregate_id}")
        return sequence
    
    async def get_current_sequence(self, aggregate_id: str) -> int:
        """
        Get current sequence number without incrementing.
        
        Args:
            aggregate_id: Aggregate identifier
            
        Returns:
            Current sequence number (0 if none)
        """
        # Check local cache first
        if aggregate_id in self._local_cache:
            return self._local_cache[aggregate_id]
        
        key = self._get_key(aggregate_id)
        
        # Get from Redis
        value = await self.redis.get(key)
        if value:
            sequence = int(value)
            self._local_cache[aggregate_id] = sequence
            return sequence
        
        return 0
    
    async def set_sequence(self, aggregate_id: str, sequence: int) -> bool:
        """
        Set sequence number for aggregate (used for recovery/replay).
        
        Args:
            aggregate_id: Aggregate identifier
            sequence: Sequence number to set
            
        Returns:
            Success status
        """
        key = self._get_key(aggregate_id)
        
        # Only set if greater than current
        lua_script = """
            local current = redis.call('GET', KEYS[1])
            if not current or tonumber(ARGV[1]) > tonumber(current) then
                redis.call('SET', KEYS[1], ARGV[1])
                return 1
            else
                return 0
            end
        """
        
        result = await self.redis.eval(lua_script, 1, key, sequence)
        
        if result:
            self._local_cache[aggregate_id] = sequence
            logger.info(f"Set sequence {sequence} for aggregate {aggregate_id}")
            return True
        else:
            logger.warning(f"Sequence {sequence} not set for {aggregate_id} (current is higher)")
            return False
    
    async def reset_aggregate(self, aggregate_id: str) -> bool:
        """
        Reset sequence for an aggregate (dangerous - use carefully).
        
        Args:
            aggregate_id: Aggregate identifier
            
        Returns:
            Success status
        """
        key = self._get_key(aggregate_id)
        
        result = await self.redis.delete(key)
        
        if aggregate_id in self._local_cache:
            del self._local_cache[aggregate_id]
        
        logger.warning(f"Reset sequence for aggregate {aggregate_id}")
        return bool(result)
    
    async def get_batch_sequences(
        self,
        aggregate_id: str,
        count: int
    ) -> tuple[int, int]:
        """
        Reserve a batch of sequence numbers for bulk operations.
        
        Args:
            aggregate_id: Aggregate identifier
            count: Number of sequences to reserve
            
        Returns:
            Tuple of (start_sequence, end_sequence)
        """
        key = self._get_key(aggregate_id)
        
        # Atomic increment by count
        end_sequence = await self.redis.incrby(key, count)
        start_sequence = end_sequence - count + 1
        
        # Update local cache
        self._local_cache[aggregate_id] = end_sequence
        
        logger.debug(
            f"Reserved sequences {start_sequence}-{end_sequence} "
            f"for aggregate {aggregate_id}"
        )
        
        return start_sequence, end_sequence
    
    async def get_all_sequences(self, pattern: Optional[str] = None) -> Dict[str, int]:
        """
        Get all current sequences (for monitoring/debugging).
        
        Args:
            pattern: Optional pattern to match aggregate IDs
            
        Returns:
            Dictionary of aggregate_id -> sequence
        """
        search_pattern = f"{self.namespace}:{pattern}*" if pattern else f"{self.namespace}:*"
        
        sequences = {}
        async for key in self.redis.scan_iter(match=search_pattern):
            # Extract aggregate_id from key
            aggregate_id = key.decode().replace(f"{self.namespace}:", "")
            value = await self.redis.get(key)
            if value:
                sequences[aggregate_id] = int(value)
        
        return sequences
    
    async def cleanup_old_sequences(self, ttl_seconds: int = 86400 * 30) -> int:
        """
        Set TTL on sequence keys for inactive aggregates.
        
        Args:
            ttl_seconds: TTL in seconds (default 30 days)
            
        Returns:
            Number of keys with TTL set
        """
        pattern = f"{self.namespace}:*"
        count = 0
        
        async for key in self.redis.scan_iter(match=pattern):
            # Check if key has TTL
            ttl = await self.redis.ttl(key)
            if ttl == -1:  # No TTL set
                # Set TTL
                await self.redis.expire(key, ttl_seconds)
                count += 1
        
        logger.info(f"Set TTL on {count} sequence keys")
        return count


class SequenceValidator:
    """
    Validates event sequences for consistency.
    """
    
    def __init__(self, sequence_service: SequenceService):
        """
        Initialize sequence validator.
        
        Args:
            sequence_service: Sequence service instance
        """
        self.sequence_service = sequence_service
        self._expected_sequences: Dict[str, int] = {}
    
    async def validate_sequence(
        self,
        aggregate_id: str,
        sequence: int,
        allow_gaps: bool = False
    ) -> tuple[bool, Optional[str]]:
        """
        Validate sequence number for aggregate.
        
        Args:
            aggregate_id: Aggregate identifier
            sequence: Sequence number to validate
            allow_gaps: Whether to allow gaps in sequence
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Get expected sequence
        if aggregate_id in self._expected_sequences:
            expected = self._expected_sequences[aggregate_id] + 1
        else:
            # Get from service
            current = await self.sequence_service.get_current_sequence(aggregate_id)
            expected = current + 1
        
        if sequence < expected:
            return False, f"Duplicate or old sequence: {sequence} < {expected}"
        
        if sequence > expected and not allow_gaps:
            return False, f"Sequence gap detected: {sequence} > {expected}"
        
        # Update expected
        self._expected_sequences[aggregate_id] = sequence
        
        return True, None
    
    def reset_expectations(self):
        """Reset local sequence expectations"""
        self._expected_sequences.clear()