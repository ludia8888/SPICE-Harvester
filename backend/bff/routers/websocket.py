"""
BFF WebSocket Router for Real-time Command Status Updates

클라이언트와 WebSocket 연결을 통해 실시간 Command 상태 업데이트를 제공합니다.
"""

import asyncio
import json
import logging
import re
import uuid
from typing import Optional, Dict, Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, Query, HTTPException
from fastapi.responses import HTMLResponse

from shared.services.websocket_service import (
    get_connection_manager, 
    get_notification_service,
    WebSocketConnectionManager
)
from bff.middleware.auth import enforce_bff_websocket_auth

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
    token: Optional[str] = Query(default=None, description="Auth token (X-Admin-Token/Bearer)"),
    manager: WebSocketConnectionManager = Depends(get_ws_manager)
):
    """
    특정 Command의 실시간 상태 업데이트 구독
    
    Args:
        command_id: 구독할 Command ID
        client_id: 클라이언트 고유 ID (선택사항, 자동 생성)
        user_id: 사용자 ID (선택사항, 인증 시 사용)
    """
    if not await enforce_bff_websocket_auth(websocket, token):
        return

    # 입력 검증 (Security Enhancement)
    if client_id:
        if len(client_id) > 50 or not re.match(r"^[a-zA-Z0-9_-]+$", client_id):
            await websocket.close(code=4000, reason="Invalid client_id format")
            return
    else:
        client_id = f"client_{uuid.uuid4().hex[:8]}"
    
    if user_id:
        if len(user_id) > 50 or not re.match(r"^[a-zA-Z0-9_-]+$", user_id):
            await websocket.close(code=4000, reason="Invalid user_id format")
            return
        
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
    token: Optional[str] = Query(default=None, description="Auth token (X-Admin-Token/Bearer)"),
    manager: WebSocketConnectionManager = Depends(get_ws_manager)
):
    """
    사용자의 모든 Command 실시간 업데이트 구독
    
    Args:
        user_id: 사용자 ID (필수)
        client_id: 클라이언트 고유 ID (선택사항, 자동 생성)
    """
    if not await enforce_bff_websocket_auth(websocket, token):
        return

    # 입력 검증 (Security Enhancement)
    if len(user_id) > 50 or not re.match(r"^[a-zA-Z0-9_-]+$", user_id):
        await websocket.close(code=4000, reason="Invalid user_id format")
        return
    
    if client_id:
        if len(client_id) > 50 or not re.match(r"^[a-zA-Z0-9_-]+$", client_id):
            await websocket.close(code=4000, reason="Invalid client_id format")
            return
    else:
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
