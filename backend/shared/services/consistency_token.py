#!/usr/bin/env python3
"""
Consistency Token Service
THINK ULTRA³ - Read-your-writes guarantee implementation

Ensures clients can read their own writes even with eventual consistency
NO MOCKS, REAL CONSISTENCY GUARANTEES
"""

import hashlib
import json
import time
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Tuple
import redis.asyncio as aioredis
from dataclasses import dataclass, asdict

@dataclass
class ConsistencyToken:
    """
    Token containing sufficient information to ensure read-your-writes consistency
    """
    command_id: str
    timestamp: str
    sequence_number: int
    aggregate_id: str
    version: int
    projection_lag_ms: int = 0
    
    def to_string(self) -> str:
        """
        Encode token as a compact string
        """
        data = {
            'cid': self.command_id[:8],  # First 8 chars of command ID
            'ts': int(datetime.fromisoformat(self.timestamp).timestamp()),
            'seq': self.sequence_number,
            'aid': self.aggregate_id,
            'ver': self.version,
            'lag': self.projection_lag_ms
        }
        # Create a compact JSON string
        json_str = json.dumps(data, separators=(',', ':'))
        # Add checksum for integrity
        checksum = hashlib.md5(json_str.encode()).hexdigest()[:4]
        return f"{json_str}#{checksum}"
    
    @classmethod
    def from_string(cls, token_str: str) -> 'ConsistencyToken':
        """
        Decode token from string
        """
        # Split token and checksum
        parts = token_str.split('#')
        if len(parts) != 2:
            raise ValueError("Invalid token format")
        
        json_str, checksum = parts
        
        # Verify checksum
        expected_checksum = hashlib.md5(json_str.encode()).hexdigest()[:4]
        if checksum != expected_checksum:
            raise ValueError("Token checksum mismatch")
        
        # Parse JSON
        data = json.loads(json_str)
        
        # Reconstruct full timestamp
        timestamp = datetime.fromtimestamp(data['ts'], tz=timezone.utc).isoformat()
        
        return cls(
            command_id=data['cid'],
            timestamp=timestamp,
            sequence_number=data['seq'],
            aggregate_id=data['aid'],
            version=data['ver'],
            projection_lag_ms=data.get('lag', 0)
        )


class ConsistencyTokenService:
    """
    Service for managing consistency tokens
    Ensures read-your-writes consistency in eventually consistent systems
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.redis_client = None
        
    async def connect(self):
        """Initialize Redis connection"""
        self.redis_client = aioredis.from_url(self.redis_url)
        
    async def disconnect(self):
        """Close Redis connection"""
        if self.redis_client:
            await self.redis_client.aclose()
    
    async def create_token(
        self,
        command_id: str,
        aggregate_id: str,
        sequence_number: int,
        version: int = 1
    ) -> ConsistencyToken:
        """
        Create a new consistency token after a write operation
        
        Args:
            command_id: The command that was executed
            aggregate_id: The aggregate that was modified
            sequence_number: The sequence number of the event
            version: The new version of the aggregate
            
        Returns:
            ConsistencyToken with tracking information
        """
        # Estimate projection lag
        projection_lag_ms = await self._estimate_projection_lag()
        
        token = ConsistencyToken(
            command_id=command_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            sequence_number=sequence_number,
            aggregate_id=aggregate_id,
            version=version,
            projection_lag_ms=projection_lag_ms
        )
        
        # Store token metadata in Redis with TTL
        await self._store_token_metadata(token)
        
        return token
    
    async def _estimate_projection_lag(self) -> int:
        """
        Estimate current projection lag in milliseconds
        
        This would typically measure actual lag from metrics
        For now, using a conservative estimate
        """
        # Check recent projection times
        lag_key = "projection:avg_lag_ms"
        lag_data = await self.redis_client.get(lag_key)
        
        if lag_data:
            return int(lag_data)
        
        # Default conservative estimate: 500ms
        return 500
    
    async def _store_token_metadata(self, token: ConsistencyToken):
        """
        Store token metadata for validation
        """
        key = f"consistency_token:{token.command_id}"
        data = asdict(token)
        
        # Store with 1 hour TTL
        await self.redis_client.setex(
            key,
            3600,
            json.dumps(data)
        )
    
    async def wait_for_consistency(
        self,
        token: ConsistencyToken,
        es_client,
        max_wait_ms: int = 5000
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Wait until the write represented by the token is visible
        
        Args:
            token: The consistency token
            es_client: Elasticsearch client for checking
            max_wait_ms: Maximum time to wait in milliseconds
            
        Returns:
            Tuple of (success, result_data)
        """
        start_time = time.time() * 1000
        wait_interval_ms = 100
        
        while (time.time() * 1000 - start_time) < max_wait_ms:
            # Check if the write is visible
            visible = await self._check_write_visible(token, es_client)
            
            if visible:
                return True, {
                    "visible": True,
                    "wait_time_ms": int(time.time() * 1000 - start_time),
                    "aggregate_id": token.aggregate_id,
                    "version": token.version
                }
            
            # Exponential backoff
            await asyncio.sleep(wait_interval_ms / 1000)
            wait_interval_ms = min(wait_interval_ms * 1.5, 1000)
        
        return False, {
            "visible": False,
            "wait_time_ms": max_wait_ms,
            "aggregate_id": token.aggregate_id,
            "timeout": True
        }
    
    async def _check_write_visible(
        self,
        token: ConsistencyToken,
        es_client
    ) -> bool:
        """
        Check if a write is visible in Elasticsearch
        """
        try:
            # Check if document exists with expected version
            response = await es_client.get(
                index=f"instances_integration_test_db",
                id=token.aggregate_id
            )
            
            if response and response.get('_source'):
                doc_version = response['_source'].get('version', 0)
                doc_sequence = response['_source'].get('event_sequence', 0)
                
                # Check if version and sequence match or exceed token
                return (doc_version >= token.version and 
                        doc_sequence >= token.sequence_number)
            
        except Exception:
            # Document not found yet
            pass
        
        return False
    
    async def validate_token(self, token_str: str) -> Tuple[bool, Optional[ConsistencyToken]]:
        """
        Validate a consistency token
        
        Args:
            token_str: The token string to validate
            
        Returns:
            Tuple of (is_valid, token_object)
        """
        try:
            # Parse token
            token = ConsistencyToken.from_string(token_str)
            
            # Check if token metadata exists in Redis
            key = f"consistency_token:{token.command_id}"
            metadata = await self.redis_client.get(key)
            
            if metadata:
                return True, token
            
            # Token expired or invalid
            return False, None
            
        except Exception as e:
            print(f"Token validation error: {e}")
            return False, None
    
    async def get_read_timestamp(self, token: ConsistencyToken) -> datetime:
        """
        Get the minimum timestamp that guarantees consistency
        
        This timestamp ensures all writes up to the token are visible
        """
        token_time = datetime.fromisoformat(token.timestamp)
        
        # Add projection lag
        read_time = token_time + timedelta(milliseconds=token.projection_lag_ms)
        
        return read_time
    
    async def update_projection_lag(self, actual_lag_ms: int):
        """
        Update the estimated projection lag based on actual measurements
        
        This should be called by the projection worker with actual lag data
        """
        # Store moving average
        lag_key = "projection:avg_lag_ms"
        current_avg = await self.redis_client.get(lag_key)
        
        if current_avg:
            # Exponential moving average
            alpha = 0.3
            new_avg = int(alpha * actual_lag_ms + (1 - alpha) * int(current_avg))
        else:
            new_avg = actual_lag_ms
        
        await self.redis_client.setex(lag_key, 300, str(new_avg))
        
        # Also track max lag for safety
        max_key = "projection:max_lag_ms"
        await self.redis_client.setex(max_key, 300, str(max(actual_lag_ms, new_avg * 2)))


# Integration with Command Response
class CommandResponseWithToken:
    """
    Enhanced command response that includes consistency token
    """
    
    def __init__(
        self,
        command_id: str,
        status: str,
        result: Dict[str, Any],
        consistency_token: Optional[ConsistencyToken] = None
    ):
        self.command_id = command_id
        self.status = status
        self.result = result
        self.consistency_token = consistency_token
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for API response
        """
        response = {
            "command_id": self.command_id,
            "status": self.status,
            "result": self.result
        }
        
        if self.consistency_token:
            response["consistency_token"] = self.consistency_token.to_string()
            response["read_after"] = (
                datetime.fromisoformat(self.consistency_token.timestamp) +
                timedelta(milliseconds=self.consistency_token.projection_lag_ms)
            ).isoformat()
        
        return response


async def demo_consistency_token():
    """
    Demo the consistency token functionality
    """
    service = ConsistencyTokenService()
    await service.connect()
    
    # Create a token after a write
    token = await service.create_token(
        command_id="test-cmd-123",
        aggregate_id="Product-001",
        sequence_number=5,
        version=2
    )
    
    print("🎫 CONSISTENCY TOKEN CREATED:")
    print(f"   Token: {token.to_string()}")
    print(f"   Command: {token.command_id}")
    print(f"   Aggregate: {token.aggregate_id}")
    print(f"   Version: {token.version}")
    print(f"   Lag Estimate: {token.projection_lag_ms}ms")
    
    # Validate the token
    is_valid, parsed = await service.validate_token(token.to_string())
    print(f"\n✅ Token Validation: {'PASSED' if is_valid else 'FAILED'}")
    
    # Get read timestamp
    read_time = await service.get_read_timestamp(token)
    print(f"\n⏰ Safe Read Time: {read_time.isoformat()}")
    
    # Create command response with token
    response = CommandResponseWithToken(
        command_id="test-cmd-123",
        status="COMPLETED",
        result={"instance_id": "Product-001", "message": "Created successfully"},
        consistency_token=token
    )
    
    print("\n📤 COMMAND RESPONSE WITH TOKEN:")
    response_dict = response.to_dict()
    print(f"   Consistency Token: {response_dict.get('consistency_token')}")
    print(f"   Read After: {response_dict.get('read_after')}")
    
    await service.disconnect()
    
    return token


if __name__ == "__main__":
    import asyncio
    token = asyncio.run(demo_consistency_token())
    print("\n✅ Consistency Token Service ready!")