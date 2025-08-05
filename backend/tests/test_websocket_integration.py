"""
WebSocket Integration Tests

Test the complete WebSocket functionality including:
- Connection management
- Command status updates
- Redis Pub/Sub integration
- Client message handling
"""

import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient
from fastapi.websockets import WebSocket

from shared.services.websocket_service import (
    WebSocketConnectionManager,
    WebSocketNotificationService,
)
from shared.services.redis_service import RedisService


class TestWebSocketConnectionManager:
    """Test WebSocket connection management"""

    @pytest.fixture
    def manager(self):
        return WebSocketConnectionManager()

    @pytest.fixture
    def mock_websocket(self):
        mock_ws = AsyncMock(spec=WebSocket)
        mock_ws.accept = AsyncMock()
        mock_ws.send_text = AsyncMock()
        return mock_ws

    @pytest.mark.asyncio
    async def test_connect_and_disconnect(self, manager, mock_websocket):
        """Test basic connection and disconnection"""
        client_id = "test_client_123"
        user_id = "test_user"
        
        # Test connection
        await manager.connect(mock_websocket, client_id, user_id)
        
        assert client_id in manager.active_connections
        assert user_id in manager.user_connections
        assert client_id in manager.user_connections[user_id]
        mock_websocket.accept.assert_called_once()
        
        # Test disconnection
        await manager.disconnect(client_id)
        
        assert client_id not in manager.active_connections
        assert user_id not in manager.user_connections

    @pytest.mark.asyncio
    async def test_command_subscription(self, manager, mock_websocket):
        """Test command subscription and unsubscription"""
        client_id = "test_client_123"
        command_id = "test_command_456"
        
        # Connect first
        await manager.connect(mock_websocket, client_id)
        
        # Test subscription
        success = await manager.subscribe_command(client_id, command_id)
        assert success is True
        assert command_id in manager.command_subscribers
        assert client_id in manager.command_subscribers[command_id]
        
        # Test unsubscription
        success = await manager.unsubscribe_command(client_id, command_id)
        assert success is True
        assert command_id not in manager.command_subscribers

    @pytest.mark.asyncio
    async def test_broadcast_command_update(self, manager, mock_websocket):
        """Test broadcasting command updates to subscribed clients"""
        client_id = "test_client_123"
        command_id = "test_command_456"
        update_data = {"status": "PROCESSING", "progress": 50}
        
        # Connect and subscribe
        await manager.connect(mock_websocket, client_id)
        await manager.subscribe_command(client_id, command_id)
        
        # Broadcast update
        sent_count = await manager.broadcast_command_update(command_id, update_data)
        
        assert sent_count == 1
        mock_websocket.send_text.assert_called_once()
        
        # Verify message content
        call_args = mock_websocket.send_text.call_args[0][0]
        message = json.loads(call_args)
        assert message["type"] == "command_update"
        assert message["command_id"] == command_id
        assert message["data"] == update_data

    @pytest.mark.asyncio
    async def test_failed_client_cleanup(self, manager, mock_websocket):
        """Test cleanup of failed clients during broadcast"""
        client_id = "test_client_123"
        command_id = "test_command_456"
        
        # Setup connection and subscription
        await manager.connect(mock_websocket, client_id)
        await manager.subscribe_command(client_id, command_id)
        
        # Mock websocket to fail during send
        mock_websocket.send_text.side_effect = Exception("Connection lost")
        
        # Broadcast should handle the failure and cleanup
        sent_count = await manager.broadcast_command_update(command_id, {})
        
        assert sent_count == 0
        assert client_id not in manager.active_connections

    def test_connection_stats(self, manager):
        """Test connection statistics"""
        stats = manager.get_connection_stats()
        
        expected_keys = {
            "total_connections",
            "total_users", 
            "command_subscriptions",
            "connections_per_user"
        }
        assert set(stats.keys()) == expected_keys
        assert all(isinstance(v, (int, dict)) for v in stats.values())


class TestWebSocketNotificationService:
    """Test WebSocket notification service with Redis integration"""

    @pytest.fixture
    def mock_redis_service(self):
        mock_redis = AsyncMock(spec=RedisService)
        mock_redis.client = AsyncMock()
        return mock_redis

    @pytest.fixture
    def mock_connection_manager(self):
        return AsyncMock(spec=WebSocketConnectionManager)

    @pytest.fixture
    def notification_service(self, mock_redis_service, mock_connection_manager):
        return WebSocketNotificationService(mock_redis_service, mock_connection_manager)

    @pytest.mark.asyncio
    async def test_service_start_stop(self, notification_service):
        """Test starting and stopping the notification service"""
        # Test start
        await notification_service.start()
        assert notification_service.running is True
        assert notification_service._pubsub_task is not None
        
        # Test stop
        await notification_service.stop()
        assert notification_service.running is False

    @pytest.mark.asyncio
    async def test_redis_message_processing(self, notification_service, mock_connection_manager):
        """Test processing Redis Pub/Sub messages"""
        # Mock Redis pubsub
        mock_pubsub = AsyncMock()
        mock_message = {
            "type": "pmessage",
            "channel": b"command_updates:test_command_123",
            "data": b'{"status": "COMPLETED", "result": "success"}'
        }
        
        # Mock pubsub.listen() to return our test message
        async def mock_listen():
            yield mock_message
            
        mock_pubsub.listen.return_value = mock_listen()
        mock_pubsub.psubscribe = AsyncMock()
        mock_pubsub.close = AsyncMock()
        
        notification_service.redis.client.pubsub.return_value = mock_pubsub
        
        # Start service and let it process one message
        await notification_service.start()
        
        # Give it a moment to process
        await asyncio.sleep(0.1)
        
        # Stop service
        await notification_service.stop()
        
        # Verify Redis subscription was set up
        mock_pubsub.psubscribe.assert_called_with("command_updates:*")
        
        # Verify message was broadcast to WebSocket manager
        mock_connection_manager.broadcast_command_update.assert_called_with(
            "test_command_123",
            {"status": "COMPLETED", "result": "success"}
        )


class TestWebSocketEndpoints:
    """Integration tests for WebSocket endpoints"""

    @pytest.fixture
    def app(self):
        """Create test FastAPI app with WebSocket router"""
        from fastapi import FastAPI
        from bff.routers import websocket
        
        app = FastAPI()
        app.include_router(websocket.router, prefix="/ws")
        return app

    @pytest.fixture
    def client(self, app):
        return TestClient(app)

    def test_websocket_test_page(self, client):
        """Test the WebSocket test page endpoint"""
        response = client.get("/ws/test")
        
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]
        assert "SPICE HARVESTER WebSocket Test" in response.text

    def test_websocket_stats_endpoint(self, client):
        """Test the WebSocket stats endpoint"""
        with patch('bff.routers.websocket.get_ws_manager') as mock_get_manager:
            mock_manager = MagicMock()
            mock_manager.get_connection_stats.return_value = {
                "total_connections": 0,
                "total_users": 0,
                "command_subscriptions": 0,
                "connections_per_user": {}
            }
            mock_get_manager.return_value = mock_manager
            
            response = client.get("/ws/stats")
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "success"
            assert "data" in data

    def test_trigger_test_update_endpoint(self, client):
        """Test the manual update trigger endpoint"""
        with patch('bff.routers.websocket.get_ws_manager') as mock_get_manager:
            mock_manager = AsyncMock()
            mock_manager.broadcast_command_update.return_value = 2
            mock_get_manager.return_value = mock_manager
            
            response = client.post("/ws/test/trigger-update/test_command_123")
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "success"
            assert data["command_id"] == "test_command_123"
            assert "sent to 2 clients" in data["message"]


class TestWebSocketFlow:
    """End-to-end WebSocket flow tests"""

    @pytest.mark.asyncio
    async def test_complete_command_status_flow(self):
        """Test complete flow from command creation to WebSocket update"""
        from shared.services.redis_service import RedisService
        from shared.services.command_status_service import CommandStatusService, CommandStatus
        
        # Mock Redis service
        mock_redis = AsyncMock(spec=RedisService)
        mock_redis.set_command_status = AsyncMock()
        mock_redis.publish_command_update = AsyncMock()
        
        # Create command status service
        status_service = CommandStatusService(mock_redis)
        
        # Simulate command lifecycle
        command_id = "test_command_123"
        
        # 1. Create command status
        await status_service.create_command_status(
            command_id=command_id,
            command_type="CREATE_ONTOLOGY",
            aggregate_id="TestClass",
            payload={"label": "Test Class"}
        )
        
        # 2. Update to processing
        await status_service.update_status(
            command_id=command_id,
            status=CommandStatus.PROCESSING,
            progress=50
        )
        
        # 3. Complete command
        await status_service.complete_command(
            command_id=command_id,
            result={"created_class_id": "TestClass"}
        )
        
        # Verify Redis operations were called
        assert mock_redis.set_command_status.call_count == 3
        assert mock_redis.publish_command_update.call_count == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])