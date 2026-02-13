"""
BFF WebSocket Router for Real-time Command Status Updates

클라이언트와 WebSocket 연결을 통해 실시간 Command 상태 업데이트를 제공합니다.
"""


from typing import Optional

from fastapi import APIRouter, WebSocket, Depends, Query

from shared.services.core.websocket_service import get_connection_manager, WebSocketConnectionManager
from bff.services import websocket_service

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
    await websocket_service.run_command_updates(
        websocket=websocket,
        command_id=command_id,
        client_id=client_id,
        user_id=user_id,
        token=token,
        manager=manager,
    )


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
    await websocket_service.run_user_updates(
        websocket=websocket,
        user_id=user_id,
        client_id=client_id,
        token=token,
        manager=manager,
    )
