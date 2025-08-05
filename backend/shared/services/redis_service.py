"""
Redis Client Service

Provides a centralized Redis client with connection pooling, 
error handling, and common operations for command status tracking.
"""

import asyncio
import json
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import redis.asyncio as redis
from redis.asyncio.connection import ConnectionPool
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)


class RedisService:
    """
    Async Redis client service with connection pooling and error handling.
    
    Features:
    - Connection pooling for performance
    - Automatic reconnection
    - JSON serialization support
    - TTL management
    - Pub/Sub support for real-time updates
    """
    
    def __init__(
        self,
        host: str = "redis",
        port: int = 6379,
        password: Optional[str] = None,
        db: int = 0,
        decode_responses: bool = True,
        max_connections: int = 50,
        socket_timeout: int = 5,
        connection_timeout: int = 5,
        retry_on_timeout: bool = True
    ):
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        
        # Create connection pool
        self.pool = ConnectionPool(
            host=host,
            port=port,
            password=password,
            db=db,
            decode_responses=decode_responses,
            max_connections=max_connections,
            socket_timeout=socket_timeout,
            socket_connect_timeout=connection_timeout,
            retry_on_timeout=retry_on_timeout,
            health_check_interval=30
        )
        
        self._client: Optional[redis.Redis] = None
        self._pubsub: Optional[redis.client.PubSub] = None
        
    async def connect(self) -> None:
        """Initialize Redis connection."""
        try:
            self._client = redis.Redis(connection_pool=self.pool)
            await self._client.ping()
            logger.info(f"Connected to Redis at {self.host}:{self.port}")
        except RedisError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
            
    async def disconnect(self) -> None:
        """Close Redis connection and pool."""
        try:
            if self._pubsub:
                await self._pubsub.close()
            if self._client:
                await self._client.close()
            await self.pool.disconnect()
            logger.info("Disconnected from Redis")
        except Exception as e:
            logger.error(f"Error disconnecting from Redis: {e}")
            
    @property
    def client(self) -> redis.Redis:
        """Get Redis client instance."""
        if not self._client:
            raise RuntimeError("Redis client not connected. Call connect() first.")
        return self._client
        
    # Command Status Operations
    
    async def set_command_status(
        self,
        command_id: str,
        status: str,
        data: Optional[Dict[str, Any]] = None,
        ttl: int = 86400  # 24 hours default
    ) -> None:
        """
        Set command status with optional data.
        
        Args:
            command_id: Unique command identifier
            status: Command status (PENDING, PROCESSING, COMPLETED, FAILED)
            data: Optional additional data
            ttl: Time to live in seconds
        """
        key = f"command:{command_id}:status"
        value = {
            "status": status,
            "updated_at": datetime.utcnow().isoformat(),
            "data": data or {}
        }
        
        await self.client.setex(
            key,
            ttl,
            json.dumps(value)
        )
        
        # Publish status update for real-time notifications
        await self.publish_command_update(command_id, value)
        
    async def get_command_status(self, command_id: str) -> Optional[Dict[str, Any]]:
        """
        Get command status and data.
        
        Args:
            command_id: Unique command identifier
            
        Returns:
            Command status data or None if not found
        """
        key = f"command:{command_id}:status"
        data = await self.client.get(key)
        
        if data:
            return json.loads(data)
        return None
        
    async def update_command_progress(
        self,
        command_id: str,
        progress: int,
        message: Optional[str] = None
    ) -> None:
        """
        Update command execution progress.
        
        Args:
            command_id: Unique command identifier
            progress: Progress percentage (0-100)
            message: Optional progress message
        """
        status_data = await self.get_command_status(command_id)
        if status_data:
            status_data["data"]["progress"] = progress
            if message:
                status_data["data"]["progress_message"] = message
                
            await self.set_command_status(
                command_id,
                status_data["status"],
                status_data["data"]
            )
            
    async def set_command_result(
        self,
        command_id: str,
        result: Dict[str, Any],
        ttl: int = 86400
    ) -> None:
        """
        Store command execution result.
        
        Args:
            command_id: Unique command identifier
            result: Command execution result
            ttl: Time to live in seconds
        """
        key = f"command:{command_id}:result"
        await self.client.setex(
            key,
            ttl,
            json.dumps(result)
        )
        
    async def get_command_result(self, command_id: str) -> Optional[Dict[str, Any]]:
        """
        Get command execution result.
        
        Args:
            command_id: Unique command identifier
            
        Returns:
            Command result or None if not found
        """
        key = f"command:{command_id}:result"
        data = await self.client.get(key)
        
        if data:
            return json.loads(data)
        return None
        
    # Pub/Sub Operations for Real-time Updates
    
    async def publish_command_update(
        self,
        command_id: str,
        data: Dict[str, Any]
    ) -> None:
        """
        Publish command status update to subscribers.
        
        Args:
            command_id: Unique command identifier
            data: Update data
        """
        channel = f"command_updates:{command_id}"
        await self.client.publish(channel, json.dumps(data))
        
    async def subscribe_command_updates(
        self,
        command_id: str,
        callback: callable
    ) -> redis.client.PubSub:
        """
        Subscribe to command status updates.
        
        Args:
            command_id: Unique command identifier
            callback: Async callback function to handle updates
            
        Returns:
            PubSub instance
        """
        if not self._pubsub:
            self._pubsub = self.client.pubsub()
            
        channel = f"command_updates:{command_id}"
        await self._pubsub.subscribe(channel)
        
        # Start listening in background
        asyncio.create_task(self._listen_for_updates(self._pubsub, callback))
        
        return self._pubsub
        
    async def _listen_for_updates(
        self,
        pubsub: redis.client.PubSub,
        callback: callable
    ) -> None:
        """Listen for pub/sub updates and call callback."""
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    data = json.loads(message["data"])
                    await callback(data)
        except Exception as e:
            logger.error(f"Error in pub/sub listener: {e}")
            
    # General Operations
    
    async def set_json(
        self,
        key: str,
        value: Dict[str, Any],
        ttl: Optional[int] = None
    ) -> None:
        """Set JSON value with optional TTL."""
        if ttl:
            await self.client.setex(key, ttl, json.dumps(value))
        else:
            await self.client.set(key, json.dumps(value))
            
    async def get_json(self, key: str) -> Optional[Dict[str, Any]]:
        """Get JSON value."""
        data = await self.client.get(key)
        if data:
            return json.loads(data)
        return None
        
    async def delete(self, key: str) -> bool:
        """Delete key."""
        return await self.client.delete(key) > 0
        
    async def exists(self, key: str) -> bool:
        """Check if key exists."""
        return await self.client.exists(key) > 0
        
    async def expire(self, key: str, seconds: int) -> bool:
        """Set expiration on key."""
        return await self.client.expire(key, seconds)
        
    async def keys(self, pattern: str) -> List[str]:
        """Get keys matching pattern."""
        return await self.client.keys(pattern)
        
    async def ping(self) -> bool:
        """Check Redis connection."""
        try:
            return await self.client.ping()
        except RedisError:
            return False


# Factory function for creating Redis service instances
def create_redis_service(
    host: Optional[str] = None,
    port: Optional[int] = None,
    password: Optional[str] = None
) -> RedisService:
    """
    Create Redis service instance with environment-based configuration.
    
    Args:
        host: Redis host (defaults to env var or 'redis')
        port: Redis port (defaults to env var or 6379)
        password: Redis password (defaults to env var)
        
    Returns:
        RedisService instance
    """
    import os
    
    return RedisService(
        host=host or os.getenv("REDIS_HOST", "redis"),
        port=port or int(os.getenv("REDIS_PORT", "6379")),
        password=password or os.getenv("REDIS_PASSWORD", "spicepass123")
    )