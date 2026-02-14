"""
WebSocket Service for Real-time Command Status Updates

Redis Pub/Sub와 WebSocket을 연결하여 실시간 Command 상태 업데이트를 제공합니다.
"""

import asyncio
import json
import logging
from typing import TYPE_CHECKING, Dict, Set, Optional, Any, List
from datetime import datetime, timezone
from fastapi import WebSocket
from dataclasses import dataclass, field

from shared.services.storage.redis_service import RedisService

if TYPE_CHECKING:
    from shared.services.core.background_task_manager import BackgroundTaskManager

logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class WebSocketConnection:
    """WebSocket 연결 정보"""
    websocket: WebSocket
    client_id: str
    user_id: Optional[str] = None
    subscribed_commands: Set[str] = field(default_factory=set)
    subscribed_schema_subjects: Set[str] = field(default_factory=set)  # "db_name:subject_type:subject_id"
    connected_at: datetime = field(default_factory=utc_now)
    last_ping: datetime = field(default_factory=utc_now)


class WebSocketConnectionManager:
    """
    WebSocket 연결 관리자
    
    - 클라이언트 연결/해제 관리
    - Command별 구독 관리
    - 메시지 브로드캐스팅
    """
    
    def __init__(self):
        # client_id -> WebSocketConnection
        self.active_connections: Dict[str, WebSocketConnection] = {}
        # command_id -> set of client_ids
        self.command_subscribers: Dict[str, Set[str]] = {}
        # user_id -> set of client_ids
        self.user_connections: Dict[str, Set[str]] = {}
        # schema subject key -> set of client_ids (for schema drift notifications)
        # key format: "db_name" or "db_name:subject_type:subject_id"
        self.schema_subscribers: Dict[str, Set[str]] = {}
        
    async def connect(
        self, 
        websocket: WebSocket, 
        client_id: str, 
        user_id: Optional[str] = None
    ) -> None:
        """새로운 WebSocket 연결 수락"""
        await websocket.accept()
        
        connection = WebSocketConnection(
            websocket=websocket,
            client_id=client_id,
            user_id=user_id
        )
        
        self.active_connections[client_id] = connection
        
        # 사용자별 연결 추적
        if user_id:
            if user_id not in self.user_connections:
                self.user_connections[user_id] = set()
            self.user_connections[user_id].add(client_id)
            
        logger.info(f"WebSocket client {client_id} connected (user: {user_id})")
        
    async def disconnect(self, client_id: str) -> None:
        """WebSocket 연결 해제"""
        if client_id not in self.active_connections:
            return

        connection = self.active_connections[client_id]

        # Command 구독 해제
        for command_id in list(connection.subscribed_commands):
            await self.unsubscribe_command(client_id, command_id)

        # Schema 구독 해제
        for subject_key in list(connection.subscribed_schema_subjects):
            await self.unsubscribe_schema_changes(client_id, subject_key)

        # 사용자별 연결에서 제거
        if connection.user_id and connection.user_id in self.user_connections:
            self.user_connections[connection.user_id].discard(client_id)
            if not self.user_connections[connection.user_id]:
                del self.user_connections[connection.user_id]

        # 연결 제거
        del self.active_connections[client_id]

        logger.info(f"WebSocket client {client_id} disconnected")
        
    async def subscribe_command(self, client_id: str, command_id: str) -> bool:
        """특정 Command에 대한 업데이트 구독"""
        if client_id not in self.active_connections:
            return False
            
        connection = self.active_connections[client_id]
        connection.subscribed_commands.add(command_id)
        
        if command_id not in self.command_subscribers:
            self.command_subscribers[command_id] = set()
        self.command_subscribers[command_id].add(client_id)
        
        logger.debug(f"Client {client_id} subscribed to command {command_id}")
        return True
        
    async def unsubscribe_command(self, client_id: str, command_id: str) -> bool:
        """Command 구독 해제"""
        if client_id not in self.active_connections:
            return False
            
        connection = self.active_connections[client_id]
        connection.subscribed_commands.discard(command_id)
        
        if command_id in self.command_subscribers:
            self.command_subscribers[command_id].discard(client_id)
            if not self.command_subscribers[command_id]:
                del self.command_subscribers[command_id]
                
        logger.debug(f"Client {client_id} unsubscribed from command {command_id}")
        return True
        
    async def send_to_client(
        self, 
        client_id: str, 
        message: Dict[str, Any]
    ) -> bool:
        """특정 클라이언트에게 메시지 전송"""
        if client_id not in self.active_connections:
            return False
            
        try:
            connection = self.active_connections[client_id]
            await connection.websocket.send_text(json.dumps(message))
            return True
        except Exception as e:
            logger.error(f"Failed to send message to client {client_id}: {e}")
            await self.disconnect(client_id)
            return False
            
    async def broadcast_command_update(
        self, 
        command_id: str, 
        update_data: Dict[str, Any]
    ) -> int:
        """Command 업데이트를 구독 중인 클라이언트들에게 브로드캐스트"""
        if command_id not in self.command_subscribers:
            return 0
            
        message = {
            "type": "command_update",
            "command_id": command_id,
            "timestamp": utc_now().isoformat(),
            "data": update_data
        }
        
        sent_count = 0
        failed_clients = []
        
        for client_id in self.command_subscribers[command_id].copy():
            success = await self.send_to_client(client_id, message)
            if success:
                sent_count += 1
            else:
                failed_clients.append(client_id)
                
        # 실패한 클라이언트들 정리
        for client_id in failed_clients:
            await self.disconnect(client_id)
            
        logger.debug(f"Broadcasted command {command_id} update to {sent_count} clients")
        return sent_count
        
    async def send_to_user(
        self, 
        user_id: str, 
        message: Dict[str, Any]
    ) -> int:
        """특정 사용자의 모든 연결에 메시지 전송"""
        if user_id not in self.user_connections:
            return 0
            
        sent_count = 0
        for client_id in self.user_connections[user_id].copy():
            success = await self.send_to_client(client_id, message)
            if success:
                sent_count += 1
                
        return sent_count

    async def broadcast_to_all(self, message: Dict[str, Any]) -> int:
        """Broadcast a message to all connected clients."""
        if not self.active_connections:
            return 0

        sent_count = 0
        failed_clients: List[str] = []

        for client_id in list(self.active_connections.keys()):
            success = await self.send_to_client(client_id, message)
            if success:
                sent_count += 1
            else:
                failed_clients.append(client_id)

        for client_id in failed_clients:
            await self.disconnect(client_id)

        return sent_count
        
    async def ping_all_clients(self) -> None:
        """모든 클라이언트에 ping 전송 (연결 상태 확인)"""
        current_time = utc_now()
        
        ping_message = {
            "type": "ping",
            "timestamp": current_time.isoformat()
        }
        
        failed_clients = []
        
        for client_id, connection in self.active_connections.items():
            try:
                await connection.websocket.send_text(json.dumps(ping_message))
                connection.last_ping = current_time
            except Exception as e:
                logger.warning(f"Ping failed for client {client_id}: {e}")
                failed_clients.append(client_id)
                
        # 실패한 클라이언트들 정리
        for client_id in failed_clients:
            await self.disconnect(client_id)
            
    def get_connection_stats(self) -> Dict[str, Any]:
        """연결 통계 반환"""
        return {
            "total_connections": len(self.active_connections),
            "total_users": len(self.user_connections),
            "command_subscriptions": len(self.command_subscribers),
            "schema_subscriptions": len(self.schema_subscribers),
            "connections_per_user": {
                user_id: len(client_ids)
                for user_id, client_ids in self.user_connections.items()
            }
        }

    # ============================================================
    # Schema Drift Subscription Methods
    # ============================================================

    def _build_schema_subject_key(
        self,
        db_name: str,
        subject_type: Optional[str] = None,
        subject_id: Optional[str] = None,
    ) -> str:
        """Build schema subscription key."""
        if subject_type and subject_id:
            return f"{db_name}:{subject_type}:{subject_id}"
        return db_name

    async def subscribe_schema_changes(
        self,
        client_id: str,
        db_name: str,
        subject_type: Optional[str] = None,
        subject_id: Optional[str] = None,
        severity_filter: Optional[List[str]] = None,
    ) -> bool:
        """
        Subscribe to schema change notifications.

        Args:
            client_id: WebSocket client ID
            db_name: Database name to subscribe to
            subject_type: Optional specific subject type (e.g., "dataset", "mapping_spec")
            subject_id: Optional specific subject ID
            severity_filter: Optional list of severities to receive (e.g., ["warning", "breaking"])

        Returns:
            True if subscription was successful
        """
        if client_id not in self.active_connections:
            return False

        subject_key = self._build_schema_subject_key(db_name, subject_type, subject_id)

        connection = self.active_connections[client_id]
        connection.subscribed_schema_subjects.add(subject_key)

        if subject_key not in self.schema_subscribers:
            self.schema_subscribers[subject_key] = set()
        self.schema_subscribers[subject_key].add(client_id)

        logger.debug(f"Client {client_id} subscribed to schema changes: {subject_key}")
        return True

    async def unsubscribe_schema_changes(
        self,
        client_id: str,
        subject_key: str,
    ) -> bool:
        """Unsubscribe from schema change notifications."""
        if client_id not in self.active_connections:
            return False

        connection = self.active_connections[client_id]
        connection.subscribed_schema_subjects.discard(subject_key)

        if subject_key in self.schema_subscribers:
            self.schema_subscribers[subject_key].discard(client_id)
            if not self.schema_subscribers[subject_key]:
                del self.schema_subscribers[subject_key]

        logger.debug(f"Client {client_id} unsubscribed from schema changes: {subject_key}")
        return True

    async def broadcast_schema_drift(
        self,
        db_name: str,
        drift_payload: Dict[str, Any],
    ) -> int:
        """
        Broadcast schema drift notification to subscribed clients.

        Args:
            db_name: Database name where drift occurred
            drift_payload: Schema drift notification payload containing:
                - event_type: "schema_drift_detected"
                - subject_type: "dataset" or "mapping_spec"
                - subject_id: ID of the affected subject
                - severity: "info", "warning", or "breaking"
                - changes: List of schema changes
                - detected_at: ISO timestamp

        Returns:
            Number of clients notified
        """
        subject_type = drift_payload.get("subject_type", "")
        subject_id = drift_payload.get("subject_id", "")

        message = {
            "type": "schema_drift",
            "db_name": db_name,
            "timestamp": utc_now().isoformat(),
            "data": drift_payload,
        }

        # Find all matching subscribers
        # Match: exact subject, db-level, or db+type level subscriptions
        matching_keys = set()

        # Exact db_name match (subscribes to all changes in db)
        if db_name in self.schema_subscribers:
            matching_keys.add(db_name)

        # db + subject_type match
        type_key = f"{db_name}:{subject_type}"
        if type_key in self.schema_subscribers:
            matching_keys.add(type_key)

        # Exact subject match
        subject_key = f"{db_name}:{subject_type}:{subject_id}"
        if subject_key in self.schema_subscribers:
            matching_keys.add(subject_key)

        # Collect all client IDs
        client_ids: Set[str] = set()
        for key in matching_keys:
            client_ids.update(self.schema_subscribers.get(key, set()))

        # Send to all matched clients
        sent_count = 0
        failed_clients: List[str] = []

        for client_id in client_ids:
            success = await self.send_to_client(client_id, message)
            if success:
                sent_count += 1
            else:
                failed_clients.append(client_id)

        # Clean up failed clients
        for client_id in failed_clients:
            await self.disconnect(client_id)

        logger.info(
            f"Broadcasted schema drift for {db_name}/{subject_type}/{subject_id} "
            f"to {sent_count} clients"
        )
        return sent_count

    async def get_schema_subscription_info(self, client_id: str) -> Dict[str, Any]:
        """Get schema subscription info for a client."""
        if client_id not in self.active_connections:
            return {"subscribed": False, "subjects": []}

        connection = self.active_connections[client_id]
        return {
            "subscribed": len(connection.subscribed_schema_subjects) > 0,
            "subjects": list(connection.subscribed_schema_subjects),
        }


class WebSocketNotificationService:
    """
    WebSocket 알림 서비스 with proper task tracking
    
    Redis Pub/Sub 이벤트를 받아서 WebSocket 클라이언트들에게 전달
    Addresses Anti-pattern 14 by using BackgroundTaskManager for all async tasks
    """
    
    def __init__(
        self, 
        redis_service: RedisService,
        connection_manager: WebSocketConnectionManager,
        task_manager: Optional['BackgroundTaskManager'] = None
    ):
        self.redis = redis_service
        self.connection_manager = connection_manager
        self.task_manager = task_manager
        self.running = False
        self._pubsub_task: Optional[asyncio.Task] = None
        self._pubsub_task_id: Optional[str] = None
        
    async def start(self) -> None:
        """알림 서비스 시작 with proper task tracking"""
        if self.running:
            return
            
        self.running = True
        
        if self.task_manager:
            # Use BackgroundTaskManager for proper tracking
            self._pubsub_task_id = await self.task_manager.create_task(
                self._listen_redis_updates,
                task_name="WebSocket Redis PubSub listener",
                task_type="websocket_pubsub",
                metadata={"service": "websocket_notification"}
            )
            logger.info(f"WebSocket notification service started with task ID: {self._pubsub_task_id}")
        else:
            # Fallback with improved error handling
            self._pubsub_task = asyncio.create_task(self._listen_redis_updates())
            self._pubsub_task.add_done_callback(self._handle_pubsub_task_done)
            logger.info("WebSocket notification service started (without task manager)")
        
    async def stop(self) -> None:
        """알림 서비스 중지 with proper cleanup"""
        self.running = False
        
        if self.task_manager and self._pubsub_task_id:
            # Cancel through task manager
            await self.task_manager.cancel_task(self._pubsub_task_id)
            self._pubsub_task_id = None
        elif self._pubsub_task:
            # Fallback cancellation
            self._pubsub_task.cancel()
            try:
                await self._pubsub_task
            except asyncio.CancelledError:
                pass
            self._pubsub_task = None
                
        logger.info("WebSocket notification service stopped")
        
    def _handle_pubsub_task_done(self, task: asyncio.Task) -> None:
        """Handle completion of pubsub task."""
        try:
            task.result()
        except asyncio.CancelledError:
            logger.info("WebSocket pubsub task was cancelled")
        except Exception as e:
            logger.error(f"WebSocket pubsub task failed: {e}")
            # Attempt to restart if still running
            if self.running:
                logger.info("Attempting to restart WebSocket pubsub listener...")
                asyncio.create_task(self._restart_pubsub_listener())
        
    async def _restart_pubsub_listener(self) -> None:
        """Restart the pubsub listener after a failure."""
        await asyncio.sleep(5)  # Wait before restarting
        if self.running:
            logger.info("Restarting WebSocket pubsub listener...")
            await self.start()
        
    async def _listen_redis_updates(self) -> None:
        """Redis Pub/Sub 채널을 수신하여 WebSocket으로 전달 with improved error handling"""
        pubsub = None
        try:
            # command_updates 및 schema_drift 채널 구독
            pubsub = self.redis.client.pubsub()
            await pubsub.psubscribe("command_updates:*", "schema_drift:*")

            logger.info("Subscribed to Redis command_updates and schema_drift channels")

            async for message in pubsub.listen():
                if not self.running:
                    break

                if message["type"] == "pmessage":
                    try:
                        # 채널 정보 추출
                        raw_channel = message.get("channel")
                        if isinstance(raw_channel, bytes):
                            channel = raw_channel.decode()
                        else:
                            channel = str(raw_channel or "")

                        # 메시지 데이터 파싱
                        raw_data = message.get("data")
                        if isinstance(raw_data, bytes):
                            payload = raw_data.decode()
                        else:
                            payload = raw_data
                        update_data = json.loads(payload) if payload else {}

                        # 채널 타입에 따라 처리
                        if channel.startswith("command_updates:"):
                            command_id = channel.split(":")[-1]
                            await self.connection_manager.broadcast_command_update(
                                command_id, update_data
                            )
                        elif channel.startswith("schema_drift:"):
                            # schema_drift:db_name 형식
                            db_name = channel.split(":", 1)[-1]
                            await self.connection_manager.broadcast_schema_drift(
                                db_name, update_data
                            )

                    except Exception as e:
                        logger.error(f"Error processing Redis message: {e}")

        except asyncio.CancelledError:
            logger.info("Redis pub/sub listener was cancelled")
            raise
        except Exception as e:
            logger.error(f"Redis pub/sub listener error: {e}")
            raise  # Let the task manager handle retry logic
        finally:
            if pubsub:
                try:
                    await pubsub.punsubscribe("command_updates:*", "schema_drift:*")
                    await pubsub.close()
                except Exception as e:
                    logger.error(f"Error closing pubsub: {e}")
                    
    async def notify_task_update(self, update_data: Dict[str, Any]) -> None:
        """
        Send task update notification to all connected clients.

        This is used by BackgroundTaskManager to send real-time updates.
        """
        await self.connection_manager.broadcast_to_all({
            "type": "task_update",
            "data": update_data,
            "timestamp": utc_now().isoformat()
        })

    async def publish_schema_drift(
        self,
        db_name: str,
        drift_payload: Dict[str, Any],
    ) -> None:
        """
        Publish schema drift notification via Redis Pub/Sub.

        This publishes to the schema_drift:{db_name} channel, which is then
        picked up by the pub/sub listener and broadcast to WebSocket clients.

        Args:
            db_name: Database name where drift occurred
            drift_payload: Schema drift notification payload
        """
        channel = f"schema_drift:{db_name}"
        try:
            await self.redis.client.publish(channel, json.dumps(drift_payload))
            logger.info(f"Published schema drift to {channel}")
        except Exception as e:
            logger.error(f"Failed to publish schema drift to {channel}: {e}")

    async def notify_schema_drift_direct(
        self,
        db_name: str,
        drift_payload: Dict[str, Any],
    ) -> int:
        """
        Send schema drift notification directly to WebSocket clients.

        Use this for immediate notification without going through Redis Pub/Sub.

        Args:
            db_name: Database name where drift occurred
            drift_payload: Schema drift notification payload

        Returns:
            Number of clients notified
        """
        return await self.connection_manager.broadcast_schema_drift(db_name, drift_payload)


# 싱글톤 인스턴스들
_connection_manager: Optional[WebSocketConnectionManager] = None
_notification_service: Optional[WebSocketNotificationService] = None


def get_connection_manager() -> WebSocketConnectionManager:
    """WebSocket 연결 관리자 싱글톤 인스턴스 반환"""
    global _connection_manager
    if _connection_manager is None:
        _connection_manager = WebSocketConnectionManager()
    return _connection_manager


def get_notification_service(redis_service: RedisService) -> WebSocketNotificationService:
    """WebSocket 알림 서비스 싱글톤 인스턴스 반환"""
    global _notification_service
    if _notification_service is None:
        _notification_service = WebSocketNotificationService(
            redis_service, get_connection_manager()
        )
    return _notification_service
