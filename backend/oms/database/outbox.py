"""
Outbox Pattern implementation for reliable message publishing
메시지(Command/Event)의 원자적 발행을 위한 Outbox 패턴 구현
"""

import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from enum import Enum

import asyncpg
from pydantic import BaseModel, Field

from oms.database.postgres import PostgresDatabase
from shared.models.commands import BaseCommand, CommandType
from shared.models.events import BaseEvent, EventType


class MessageType(str, Enum):
    """메시지 유형"""
    COMMAND = "COMMAND"
    EVENT = "EVENT"


class OutboxMessage(BaseModel):
    """Outbox 메시지 모델 (Command 또는 Event)"""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    message_type: MessageType
    aggregate_type: str
    aggregate_id: str
    topic: str
    payload: Dict[str, Any]
    created_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None
    retry_count: int = 0
    last_retry_at: Optional[datetime] = None


class OutboxService:
    """Outbox 패턴을 사용한 메시지 발행 서비스"""
    
    def __init__(self, db: PostgresDatabase):
        self.db = db
        
    async def publish_command(
        self,
        connection: asyncpg.Connection,
        command: BaseCommand,
        topic: str = "ontology_commands"
    ) -> str:
        """
        트랜잭션 내에서 Command를 outbox 테이블에 발행
        
        Args:
            connection: 활성 트랜잭션 연결
            command: 발행할 Command 객체
            topic: Kafka 토픽 이름
            
        Returns:
            생성된 메시지 ID
        """
        message_id = str(command.command_id)
        
        # Outbox 테이블에 Command 삽입
        await connection.execute(
            """
            INSERT INTO spice_outbox.outbox 
            (id, message_type, aggregate_type, aggregate_id, topic, payload)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            message_id,
            MessageType.COMMAND,
            command.aggregate_type,
            command.aggregate_id,
            topic,
            command.json()
        )
        
        return message_id
        
    async def publish_event(
        self,
        connection: asyncpg.Connection,
        event: BaseEvent,
        topic: str = "ontology_events"
    ) -> str:
        """
        트랜잭션 내에서 Event를 outbox 테이블에 발행
        
        Args:
            connection: 활성 트랜잭션 연결
            event: 발행할 Event 객체
            topic: Kafka 토픽 이름
            
        Returns:
            생성된 메시지 ID
        """
        message_id = str(event.event_id)
        
        # Outbox 테이블에 Event 삽입
        await connection.execute(
            """
            INSERT INTO spice_outbox.outbox 
            (id, message_type, aggregate_type, aggregate_id, topic, payload)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            message_id,
            MessageType.EVENT,
            event.aggregate_type,
            event.aggregate_id,
            topic,
            event.json()
        )
        
        return message_id
        
    async def get_unprocessed_messages(
        self, 
        message_type: Optional[MessageType] = None,
        limit: int = 100
    ) -> List[OutboxMessage]:
        """
        처리되지 않은 메시지 조회
        
        Args:
            message_type: 조회할 메시지 유형 (None이면 모든 유형)
            limit: 조회할 최대 메시지 수
            
        Returns:
            OutboxMessage 리스트
        """
        if message_type:
            rows = await self.db.fetch(
                """
                SELECT id, message_type, aggregate_type, aggregate_id, topic, 
                       payload, created_at, processed_at, retry_count, last_retry_at
                FROM spice_outbox.outbox
                WHERE processed_at IS NULL AND message_type = $1
                ORDER BY created_at
                LIMIT $2
                """,
                message_type,
                limit
            )
        else:
            rows = await self.db.fetch(
                """
                SELECT id, message_type, aggregate_type, aggregate_id, topic, 
                       payload, created_at, processed_at, retry_count, last_retry_at
                FROM spice_outbox.outbox
                WHERE processed_at IS NULL
                ORDER BY created_at
                LIMIT $1
                """,
                limit
            )
        
        messages = []
        for row in rows:
            messages.append(OutboxMessage(
                id=str(row['id']),
                message_type=row['message_type'],
                aggregate_type=row['aggregate_type'],
                aggregate_id=row['aggregate_id'],
                topic=row['topic'],
                payload=json.loads(row['payload']) if isinstance(row['payload'], str) else row['payload'],
                created_at=row['created_at'],
                processed_at=row['processed_at'],
                retry_count=row['retry_count'],
                last_retry_at=row['last_retry_at']
            ))
            
        return messages
        
    async def mark_as_processed(self, event_id: str) -> None:
        """
        이벤트를 처리됨으로 표시
        
        Args:
            event_id: 이벤트 ID
        """
        await self.db.execute(
            """
            UPDATE spice_outbox.outbox 
            SET processed_at = NOW()
            WHERE id = $1
            """,
            event_id
        )
        
    async def increment_retry_count(self, event_id: str) -> None:
        """
        이벤트 재시도 횟수 증가
        
        Args:
            event_id: 이벤트 ID
        """
        await self.db.execute(
            """
            UPDATE spice_outbox.outbox 
            SET retry_count = retry_count + 1,
                last_retry_at = NOW()
            WHERE id = $1
            """,
            event_id
        )