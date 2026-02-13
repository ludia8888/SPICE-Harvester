"""
WebSocket session orchestration (BFF).

Extracted from `bff.routers.websocket` to keep routers thin and to centralize
connection/session control flow behind a small facade.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from typing import Any, Dict, Optional

from fastapi import WebSocket, WebSocketDisconnect

from bff.middleware.auth import enforce_bff_websocket_auth
from shared.services.core.websocket_service import WebSocketConnectionManager
from shared.observability.tracing import trace_external_call

logger = logging.getLogger(__name__)

_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")
_MAX_ID_LENGTH = 50


def _is_valid_identifier(value: str) -> bool:
    if not value:
        return False
    if len(value) > _MAX_ID_LENGTH:
        return False
    return bool(_ID_PATTERN.match(value))


async def _send_json(websocket: WebSocket, payload: Dict[str, Any]) -> None:
    await websocket.send_text(json.dumps(payload))


async def _send_error(websocket: WebSocket, message: str) -> None:
    await _send_json(websocket, {"type": "error", "message": message})


async def _send_connection_established(
    websocket: WebSocket,
    *,
    client_id: str,
    command_id: Optional[str] = None,
    user_id: Optional[str] = None,
) -> None:
    payload: Dict[str, Any] = {
        "type": "connection_established",
        "client_id": client_id,
    }
    if command_id:
        payload.update(
            {
                "command_id": command_id,
                "message": f"Subscribed to command {command_id} updates",
            }
        )
    if user_id and not command_id:
        payload.update(
            {
                "user_id": user_id,
                "message": f"Connected to user {user_id} command updates",
            }
        )
    await _send_json(websocket, payload)


@trace_external_call("bff.websocket.handle_client_message")
async def handle_client_message(
    *,
    websocket: WebSocket,
    client_id: str,
    message: Dict[str, Any],
    manager: WebSocketConnectionManager,
) -> None:
    """Handle a message received from the client."""
    message_type = message.get("type")

    if message_type == "ping":
        await _send_json(
            websocket,
            {
                "type": "pong",
                "timestamp": message.get("timestamp"),
            },
        )
        return

    if message_type == "subscribe":
        command_id = message.get("command_id")
        if command_id:
            success = await manager.subscribe_command(client_id, command_id)
            await _send_json(
                websocket,
                {
                    "type": "subscription_result",
                    "action": "subscribe",
                    "command_id": command_id,
                    "success": success,
                },
            )
        else:
            await _send_error(websocket, "command_id is required for subscription")
        return

    if message_type == "unsubscribe":
        command_id = message.get("command_id")
        if command_id:
            success = await manager.unsubscribe_command(client_id, command_id)
            await _send_json(
                websocket,
                {
                    "type": "subscription_result",
                    "action": "unsubscribe",
                    "command_id": command_id,
                    "success": success,
                },
            )
        else:
            await _send_error(websocket, "command_id is required for unsubscription")
        return

    if message_type == "get_subscriptions":
        connection = manager.active_connections.get(client_id)
        if connection:
            await _send_json(
                websocket,
                {
                    "type": "subscriptions",
                    "subscribed_commands": list(connection.subscribed_commands),
                },
            )
        else:
            await _send_error(websocket, "Connection not found")
        return

    await _send_error(websocket, f"Unknown message type: {message_type}")


async def _run_session(
    *,
    websocket: WebSocket,
    client_id: str,
    user_id: Optional[str],
    token: Optional[str],
    manager: WebSocketConnectionManager,
    command_id: Optional[str] = None,
) -> None:
    if not await enforce_bff_websocket_auth(websocket, token):
        return

    try:
        await manager.connect(websocket, client_id, user_id)

        if command_id:
            await manager.subscribe_command(client_id, command_id)

        await _send_connection_established(
            websocket,
            client_id=client_id,
            command_id=command_id,
            user_id=user_id,
        )

        if command_id:
            logger.info("WebSocket client %s subscribed to command %s", client_id, command_id)
        elif user_id:
            logger.info("WebSocket client %s connected for user %s", client_id, user_id)
        else:
            logger.info("WebSocket client %s connected", client_id)

        while True:
            try:
                data = await websocket.receive_text()
                message = json.loads(data)
                await handle_client_message(
                    websocket=websocket,
                    client_id=client_id,
                    message=message,
                    manager=manager,
                )
            except WebSocketDisconnect:
                logger.info("WebSocket client %s disconnected", client_id)
                break
            except json.JSONDecodeError:
                await _send_error(websocket, "Invalid JSON format")
            except Exception as exc:
                logger.error("Error in WebSocket connection %s: %s", client_id, exc)
                await _send_error(websocket, "Internal server error")
    except Exception as exc:
        logger.error("WebSocket connection error for client %s: %s", client_id, exc)
    finally:
        await manager.disconnect(client_id)


@trace_external_call("bff.websocket.run_command_updates")
async def run_command_updates(
    *,
    websocket: WebSocket,
    command_id: str,
    client_id: Optional[str],
    user_id: Optional[str],
    token: Optional[str],
    manager: WebSocketConnectionManager,
) -> None:
    if client_id:
        if not _is_valid_identifier(client_id):
            await websocket.close(code=4000, reason="Invalid client_id format")
            return
    else:
        client_id = f"client_{uuid.uuid4().hex[:8]}"

    if user_id:
        if not _is_valid_identifier(user_id):
            await websocket.close(code=4000, reason="Invalid user_id format")
            return

    await _run_session(
        websocket=websocket,
        client_id=client_id,
        user_id=user_id,
        token=token,
        manager=manager,
        command_id=command_id,
    )


@trace_external_call("bff.websocket.run_user_updates")
async def run_user_updates(
    *,
    websocket: WebSocket,
    user_id: str,
    client_id: Optional[str],
    token: Optional[str],
    manager: WebSocketConnectionManager,
) -> None:
    if not _is_valid_identifier(user_id):
        await websocket.close(code=4000, reason="Invalid user_id format")
        return

    if client_id:
        if not _is_valid_identifier(client_id):
            await websocket.close(code=4000, reason="Invalid client_id format")
            return
    else:
        client_id = f"user_{user_id}_{uuid.uuid4().hex[:8]}"

    await _run_session(
        websocket=websocket,
        client_id=client_id,
        user_id=user_id,
        token=token,
        manager=manager,
    )
