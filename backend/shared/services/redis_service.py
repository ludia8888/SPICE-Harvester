"""
Redis Client Service

Provides a centralized Redis client with connection pooling, 
error handling, and common operations for command status tracking.
"""

import asyncio
import json
import logging
import os
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta, timezone
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
            
    async def initialize(self) -> None:
        """ServiceContainer-compatible initialization method."""
        await self.connect()
            
    async def disconnect(self) -> None:
        """Close Redis connection and pool."""
        try:
            # Clean up listeners first
            await self.cleanup_listeners()
            
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
        from shared.config.app_config import AppConfig
        key = AppConfig.get_command_status_key(command_id)
        value = {
            "status": status,
            "updated_at": datetime.now(timezone.utc).isoformat(),
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
        from shared.config.app_config import AppConfig
        key = AppConfig.get_command_status_key(command_id)
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
        from shared.config.app_config import AppConfig
        key = AppConfig.get_command_result_key(command_id)
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
        from shared.config.app_config import AppConfig
        key = AppConfig.get_command_result_key(command_id)
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
        callback: callable,
        task_manager: Optional['BackgroundTaskManager'] = None
    ) -> redis.client.PubSub:
        """
        Subscribe to command status updates with proper task tracking.
        
        Args:
            command_id: Unique command identifier
            callback: Async callback function to handle updates
            task_manager: Optional BackgroundTaskManager for tracking
            
        Returns:
            PubSub instance
        """
        if not self._pubsub:
            self._pubsub = self.client.pubsub()
            
        channel = f"command_updates:{command_id}"
        await self._pubsub.subscribe(channel)
        
        # Start listening with proper tracking
        if task_manager:
            # Use BackgroundTaskManager for proper tracking
            await task_manager.create_task(
                self._listen_for_updates,
                self._pubsub,
                callback,
                command_id,
                task_name=f"Redis PubSub listener for {command_id}",
                task_type="pubsub_listener",
                metadata={"command_id": command_id, "channel": channel}
            )
        else:
            # Fallback with improved error handling
            task = asyncio.create_task(
                self._listen_for_updates(self._pubsub, callback, command_id)
            )
            # Add done callback for error logging
            task.add_done_callback(self._handle_listener_done)
            # Store task reference for cleanup
            if not hasattr(self, '_listener_tasks'):
                self._listener_tasks = {}
            self._listener_tasks[command_id] = task
        
        return self._pubsub
        
    async def _listen_for_updates(
        self,
        pubsub: redis.client.PubSub,
        callback: callable,
        command_id: str
    ) -> None:
        """
        Listen for pub/sub updates with improved error handling.
        
        Args:
            pubsub: Redis pubsub instance
            callback: Callback function for messages
            command_id: Command ID for tracking
        """
        logger.info(f"Starting pub/sub listener for command {command_id}")
        
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        await callback(data)
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON in pub/sub message for {command_id}: {e}")
                    except Exception as e:
                        logger.error(f"Error in callback for {command_id}: {e}")
                        # Continue listening despite callback errors
                        
        except asyncio.CancelledError:
            logger.info(f"Pub/sub listener for {command_id} was cancelled")
            raise
        except Exception as e:
            logger.error(f"Fatal error in pub/sub listener for {command_id}: {e}")
            raise
        finally:
            logger.info(f"Pub/sub listener for {command_id} stopped")
            
    def _handle_listener_done(self, task: asyncio.Task) -> None:
        """Handle completion of a listener task."""
        try:
            task.result()
        except asyncio.CancelledError:
            logger.debug("Listener task was cancelled")
        except Exception as e:
            logger.error(f"Listener task failed: {e}")
            
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
        
    async def set(self, key: str, value: str, ttl: Optional[int] = None) -> bool:
        """Set key-value pair with optional TTL."""
        if ttl:
            return await self.client.setex(key, ttl, value)
        return await self.client.set(key, value)
    
    async def get(self, key: str) -> Optional[str]:
        """Get value for key."""
        return await self.client.get(key)
    
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
    
    async def cleanup_listeners(self) -> None:
        """Clean up all active pub/sub listeners."""
        if hasattr(self, '_listener_tasks'):
            for command_id, task in list(self._listener_tasks.items()):
                if not task.done():
                    logger.info(f"Cancelling listener task for {command_id}")
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            self._listener_tasks.clear()
            
        if self._pubsub:
            await self._pubsub.unsubscribe()
            await self._pubsub.close()
            self._pubsub = None
            
    async def scan_keys(self, pattern: str, count: int = 100) -> List[str]:
        """
        Scan keys matching pattern without blocking.
        
        Args:
            pattern: Key pattern to match
            count: Approximate number of keys per iteration
            
        Returns:
            List of matching keys
        """
        keys = []
        cursor = 0
        
        while True:
            cursor, batch = await self.client.scan(cursor, match=pattern, count=count)
            keys.extend(batch)
            if cursor == 0:
                break
                
        return keys


# Factory function for creating Redis service instances
def create_redis_service(settings: 'ApplicationSettings') -> RedisService:
    """
    Redis 서비스 팩토리 함수 (Anti-pattern 13 해결)
    
    Args:
        settings: 중앙화된 애플리케이션 설정 객체
        
    Returns:
        RedisService 인스턴스
        
    Note:
        이 함수는 더 이상 내부에서 환경변수를 로드하지 않습니다.
        모든 설정은 ApplicationSettings를 통해 중앙화되어 관리됩니다.
    """
    return RedisService(
        host=settings.database.redis_host,
        port=settings.database.redis_port,
        password=settings.database.redis_password
    )


def create_redis_service_legacy(
    host: Optional[str] = None,
    port: Optional[int] = None,
    password: Optional[str] = None
) -> RedisService:
    """
    레거시 Redis 서비스 팩토리 함수 (하위 호환성)
    
    이 함수는 기존 코드와의 호환성을 위해 유지되며,
    마이그레이션 완료 후 제거될 예정입니다.
    
    Args:
        host: Redis host (defaults to env var or 'redis')
        port: Redis port (defaults to env var or 6379)
        password: Redis password (defaults to env var)
        
    Returns:
        RedisService instance
    """
    from shared.config.service_config import ServiceConfig
    
    return RedisService(
        host=host or ServiceConfig.get_redis_host(),
        port=port or ServiceConfig.get_redis_port(),
        password=password or os.getenv("REDIS_PASSWORD", "spicepass123")
    )