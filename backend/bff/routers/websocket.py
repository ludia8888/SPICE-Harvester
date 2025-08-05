"""
BFF WebSocket Router for Real-time Command Status Updates

클라이언트와 WebSocket 연결을 통해 실시간 Command 상태 업데이트를 제공합니다.
"""

import asyncio
import json
import logging
import uuid
from typing import Optional, Dict, Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, Query, HTTPException
from fastapi.responses import HTMLResponse

from shared.services.websocket_service import (
    get_connection_manager, 
    get_notification_service,
    WebSocketConnectionManager
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ws", tags=["WebSocket"])


# WebSocket 연결 관리자 의존성
def get_ws_manager() -> WebSocketConnectionManager:
    """WebSocket 연결 관리자 의존성"""
    return get_connection_manager()


@router.websocket("/commands/{command_id}")
async def websocket_command_updates(
    websocket: WebSocket,
    command_id: str,
    client_id: Optional[str] = Query(default=None),
    user_id: Optional[str] = Query(default=None),
    manager: WebSocketConnectionManager = Depends(get_ws_manager)
):
    """
    특정 Command의 실시간 상태 업데이트 구독
    
    Args:
        command_id: 구독할 Command ID
        client_id: 클라이언트 고유 ID (선택사항, 자동 생성)
        user_id: 사용자 ID (선택사항, 인증 시 사용)
    """
    if not client_id:
        client_id = f"client_{uuid.uuid4().hex[:8]}"
        
    try:
        # WebSocket 연결 수락
        await manager.connect(websocket, client_id, user_id)
        
        # 특정 Command 구독
        await manager.subscribe_command(client_id, command_id)
        
        # 연결 성공 메시지 전송
        await websocket.send_text(json.dumps({
            "type": "connection_established",
            "client_id": client_id,
            "command_id": command_id,
            "message": f"Subscribed to command {command_id} updates"
        }))
        
        logger.info(f"WebSocket client {client_id} subscribed to command {command_id}")
        
        # 연결 유지 및 메시지 처리
        while True:
            try:
                # 클라이언트로부터 메시지 대기 (ping/pong, 구독 변경 등)
                data = await websocket.receive_text()
                message = json.loads(data)
                
                await handle_client_message(websocket, client_id, message, manager)
                
            except WebSocketDisconnect:
                logger.info(f"WebSocket client {client_id} disconnected")
                break
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "message": "Invalid JSON format"
                }))
            except Exception as e:
                logger.error(f"Error in WebSocket connection {client_id}: {e}")
                await websocket.send_text(json.dumps({
                    "type": "error", 
                    "message": "Internal server error"
                }))
                
    except Exception as e:
        logger.error(f"WebSocket connection error for client {client_id}: {e}")
    finally:
        await manager.disconnect(client_id)


@router.websocket("/commands")
async def websocket_user_commands(
    websocket: WebSocket,
    user_id: str = Query(..., description="사용자 ID (필수)"),
    client_id: Optional[str] = Query(default=None),
    manager: WebSocketConnectionManager = Depends(get_ws_manager)
):
    """
    사용자의 모든 Command 실시간 업데이트 구독
    
    Args:
        user_id: 사용자 ID (필수)
        client_id: 클라이언트 고유 ID (선택사항, 자동 생성)
    """
    if not client_id:
        client_id = f"user_{user_id}_{uuid.uuid4().hex[:8]}"
        
    try:
        # WebSocket 연결 수락
        await manager.connect(websocket, client_id, user_id)
        
        # 연결 성공 메시지 전송
        await websocket.send_text(json.dumps({
            "type": "connection_established",
            "client_id": client_id,
            "user_id": user_id,
            "message": f"Connected to user {user_id} command updates"
        }))
        
        logger.info(f"WebSocket client {client_id} connected for user {user_id}")
        
        # 연결 유지 및 메시지 처리
        while True:
            try:
                data = await websocket.receive_text()
                message = json.loads(data)
                
                await handle_client_message(websocket, client_id, message, manager)
                
            except WebSocketDisconnect:
                logger.info(f"WebSocket client {client_id} disconnected")
                break
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "message": "Invalid JSON format"
                }))
            except Exception as e:
                logger.error(f"Error in WebSocket connection {client_id}: {e}")
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "message": "Internal server error"
                }))
                
    except Exception as e:
        logger.error(f"WebSocket connection error for client {client_id}: {e}")
    finally:
        await manager.disconnect(client_id)


async def handle_client_message(
    websocket: WebSocket,
    client_id: str,
    message: Dict[str, Any],
    manager: WebSocketConnectionManager
) -> None:
    """클라이언트로부터 받은 메시지 처리"""
    
    message_type = message.get("type")
    
    if message_type == "ping":
        # Ping에 대한 Pong 응답
        await websocket.send_text(json.dumps({
            "type": "pong",
            "timestamp": message.get("timestamp")
        }))
        
    elif message_type == "subscribe":
        # 새로운 Command 구독
        command_id = message.get("command_id")
        if command_id:
            success = await manager.subscribe_command(client_id, command_id)
            await websocket.send_text(json.dumps({
                "type": "subscription_result",
                "action": "subscribe",
                "command_id": command_id,
                "success": success
            }))
        else:
            await websocket.send_text(json.dumps({
                "type": "error",
                "message": "command_id is required for subscription"
            }))
            
    elif message_type == "unsubscribe":
        # Command 구독 해제
        command_id = message.get("command_id")
        if command_id:
            success = await manager.unsubscribe_command(client_id, command_id)
            await websocket.send_text(json.dumps({
                "type": "subscription_result",
                "action": "unsubscribe", 
                "command_id": command_id,
                "success": success
            }))
        else:
            await websocket.send_text(json.dumps({
                "type": "error",
                "message": "command_id is required for unsubscription"
            }))
            
    elif message_type == "get_subscriptions":
        # 현재 구독 목록 반환
        connection = manager.active_connections.get(client_id)
        if connection:
            await websocket.send_text(json.dumps({
                "type": "subscriptions",
                "subscribed_commands": list(connection.subscribed_commands)
            }))
        else:
            await websocket.send_text(json.dumps({
                "type": "error",
                "message": "Connection not found"
            }))
            
    else:
        await websocket.send_text(json.dumps({
            "type": "error",
            "message": f"Unknown message type: {message_type}"
        }))


@router.get("/stats")
async def websocket_stats(
    manager: WebSocketConnectionManager = Depends(get_ws_manager)
):
    """WebSocket 연결 통계 조회"""
    return {
        "status": "success",
        "data": manager.get_connection_stats()
    }


@router.get("/test")
async def websocket_test_page():
    """WebSocket 테스트용 HTML 페이지"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>SPICE HARVESTER WebSocket Test</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .container { max-width: 800px; margin: 0 auto; }
            .section { margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }
            input, button { margin: 5px; padding: 8px; }
            #messages { height: 300px; overflow-y: scroll; border: 1px solid #ccc; padding: 10px; }
            .message { margin: 5px 0; padding: 5px; background: #f5f5f5; border-radius: 3px; }
            .error { background: #ffebee; color: #c62828; }
            .success { background: #e8f5e8; color: #2e7d32; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🔌 SPICE HARVESTER WebSocket Test</h1>
            
            <div class="section">
                <h3>Connection Settings</h3>
                <input type="text" id="commandId" placeholder="Command ID (optional)" />
                <input type="text" id="userId" placeholder="User ID (optional)" />
                <input type="text" id="clientId" placeholder="Client ID (optional)" />
                <br>
                <button onclick="connectToCommand()">Connect to Command</button>
                <button onclick="connectToUser()">Connect to User Commands</button>
                <button onclick="disconnect()">Disconnect</button>
                <span id="status">Disconnected</span>
            </div>
            
            <div class="section">
                <h3>Actions</h3>
                <input type="text" id="subscribeCommandId" placeholder="Command ID to subscribe" />
                <button onclick="subscribe()">Subscribe</button>
                <button onclick="unsubscribe()">Unsubscribe</button>
                <button onclick="getSubscriptions()">Get Subscriptions</button>
                <button onclick="ping()">Ping</button>
            </div>
            
            <div class="section">
                <h3>Messages</h3>
                <button onclick="clearMessages()">Clear</button>
                <div id="messages"></div>
            </div>
        </div>

        <script>
            let socket = null;
            
            function addMessage(message, type = 'info') {
                const div = document.createElement('div');
                div.className = `message ${type}`;
                div.innerHTML = `<strong>${new Date().toLocaleTimeString()}</strong>: ${JSON.stringify(message, null, 2)}`;
                document.getElementById('messages').appendChild(div);
                document.getElementById('messages').scrollTop = document.getElementById('messages').scrollHeight;
            }
            
            function updateStatus(status) {
                document.getElementById('status').textContent = status;
            }
            
            function connectToCommand() {
                const commandId = document.getElementById('commandId').value || 'test-command-123';
                const userId = document.getElementById('userId').value;
                const clientId = document.getElementById('clientId').value;
                
                let url = `/ws/commands/${commandId}`;
                const params = new URLSearchParams();
                if (userId) params.append('user_id', userId);
                if (clientId) params.append('client_id', clientId);
                if (params.toString()) url += '?' + params.toString();
                
                connect(url);
            }
            
            function connectToUser() {
                const userId = document.getElementById('userId').value || 'test-user';
                const clientId = document.getElementById('clientId').value;
                
                let url = `/ws/commands?user_id=${userId}`;
                if (clientId) url += `&client_id=${clientId}`;
                
                connect(url);
            }
            
            function connect(url) {
                if (socket) {
                    socket.close();
                }
                
                const wsUrl = `ws://${window.location.host}${url}`;
                socket = new WebSocket(wsUrl);
                
                socket.onopen = function() {
                    updateStatus('Connected');
                    addMessage('Connected to ' + wsUrl, 'success');
                };
                
                socket.onclose = function() {
                    updateStatus('Disconnected');
                    addMessage('Connection closed', 'error');
                };
                
                socket.onerror = function(error) {
                    updateStatus('Error');
                    addMessage('WebSocket error: ' + error, 'error');
                };
                
                socket.onmessage = function(event) {
                    const data = JSON.parse(event.data);
                    addMessage(data, data.type === 'error' ? 'error' : 'success');
                };
            }
            
            function disconnect() {
                if (socket) {
                    socket.close();
                    socket = null;
                }
            }
            
            function subscribe() {
                const commandId = document.getElementById('subscribeCommandId').value;
                if (!commandId) {
                    alert('Please enter a command ID to subscribe');
                    return;
                }
                sendMessage({type: 'subscribe', command_id: commandId});
            }
            
            function unsubscribe() {
                const commandId = document.getElementById('subscribeCommandId').value;
                if (!commandId) {
                    alert('Please enter a command ID to unsubscribe');
                    return;
                }
                sendMessage({type: 'unsubscribe', command_id: commandId});
            }
            
            function getSubscriptions() {
                sendMessage({type: 'get_subscriptions'});
            }
            
            function ping() {
                sendMessage({type: 'ping', timestamp: new Date().toISOString()});
            }
            
            function sendMessage(message) {
                if (socket && socket.readyState === WebSocket.OPEN) {
                    socket.send(JSON.stringify(message));
                    addMessage('Sent: ' + JSON.stringify(message));
                } else {
                    alert('Not connected to WebSocket');
                }
            }
            
            function clearMessages() {
                document.getElementById('messages').innerHTML = '';
            }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


# Utility functions for manual testing
@router.post("/test/trigger-update/{command_id}")
async def trigger_test_update(
    command_id: str,
    status: str = "PROCESSING",
    progress: int = 50,
    manager: WebSocketConnectionManager = Depends(get_ws_manager)
):
    """테스트용: 수동으로 Command 업데이트 트리거"""
    update_data = {
        "status": status,
        "progress": progress,
        "updated_at": "2025-08-05T12:00:00Z",
        "message": f"Test update: {status}"
    }
    
    sent_count = await manager.broadcast_command_update(command_id, update_data)
    
    return {
        "status": "success",
        "message": f"Test update sent to {sent_count} clients",
        "command_id": command_id,
        "update_data": update_data
    }