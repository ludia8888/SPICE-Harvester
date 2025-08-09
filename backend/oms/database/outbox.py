"""
Outbox Pattern implementation for reliable message publishing
메시지(Command/Event)의 원자적 발행을 위한 Outbox 패턴 구현
Enhanced with optimistic locking support for MVCC
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
from shared.models.base import OptimisticLockError, ConcurrencyControl
from shared.config.app_config import AppConfig


class MessageType(str, Enum):
    """메시지 유형"""
    COMMAND = "COMMAND"
    EVENT = "EVENT"


class OutboxMessage(BaseModel):
    """
    Outbox 메시지 모델 (Command 또는 Event)
    Enhanced with version field for optimistic locking support
    """
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
    # Version field for optimistic locking
    entity_version: Optional[int] = Field(
        None, 
        description="Version of the entity for optimistic locking"
    )


class OutboxService:
    """
    Outbox 패턴을 사용한 메시지 발행 서비스
    
    Enhanced with optimistic locking support through version checking.
    Follows SRP - only responsible for outbox message management.
    """
    
    def __init__(self, db: PostgresDatabase):
        self.db = db
        self.concurrency_control = ConcurrencyControl()
        
    async def publish_command(
        self,
        connection: asyncpg.Connection,
        command: BaseCommand,
        topic: str = AppConfig.ONTOLOGY_COMMANDS_TOPIC
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
            command.model_dump_json()
        )
        
        return message_id
        
    async def publish_event(
        self,
        connection: asyncpg.Connection,
        event: BaseEvent,
        topic: str = AppConfig.ONTOLOGY_EVENTS_TOPIC
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
            event.model_dump_json()
        )
        
        return message_id
    
    async def publish_command_with_version(
        self,
        connection: asyncpg.Connection,
        command: BaseCommand,
        expected_version: Optional[int] = None,
        current_version: Optional[int] = None,
        topic: str = AppConfig.ONTOLOGY_COMMANDS_TOPIC
    ) -> str:
        """
        트랜잭션 내에서 Command를 outbox 테이블에 발행 (optimistic locking 지원)
        
        Args:
            connection: 활성 트랜잭션 연결
            command: 발행할 Command 객체
            expected_version: 클라이언트가 기대하는 엔티티 버전
            current_version: 현재 데이터베이스의 엔티티 버전
            topic: Kafka 토픽 이름
            
        Returns:
            생성된 메시지 ID
            
        Raises:
            OptimisticLockError: 버전 충돌 시
        """
        # Version check if provided
        if expected_version is not None and current_version is not None:
            self.concurrency_control.validate_version_for_update(
                current_version=current_version,
                provided_version=expected_version
            )
        
        message_id = str(command.command_id)
        
        # Add version info to payload if available
        payload = command.model_dump_json()
        if current_version is not None:
            payload_dict = json.loads(payload)
            payload_dict['entity_version'] = current_version + 1
            payload = json.dumps(payload_dict)
        
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
            payload
        )
        
        return message_id
    
    async def publish_event_with_version(
        self,
        connection: asyncpg.Connection,
        event: BaseEvent,
        entity_version: Optional[int] = None,
        topic: str = AppConfig.ONTOLOGY_EVENTS_TOPIC
    ) -> str:
        """
        트랜잭션 내에서 Event를 outbox 테이블에 발행 (version 정보 포함)
        
        Args:
            connection: 활성 트랜잭션 연결
            event: 발행할 Event 객체
            entity_version: 엔티티의 현재 버전
            topic: Kafka 토픽 이름
            
        Returns:
            생성된 메시지 ID
        """
        message_id = str(event.event_id)
        
        # Add version info to payload if available
        payload = event.model_dump_json()
        if entity_version is not None:
            payload_dict = json.loads(payload)
            payload_dict['entity_version'] = entity_version
            payload = json.dumps(payload_dict)
        
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
            payload
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
            # Extract version from payload if present
            payload = json.loads(row['payload']) if isinstance(row['payload'], str) else row['payload']
            entity_version = payload.get('entity_version') if isinstance(payload, dict) else None
            
            messages.append(OutboxMessage(
                id=str(row['id']),
                message_type=row['message_type'],
                aggregate_type=row['aggregate_type'],
                aggregate_id=row['aggregate_id'],
                topic=row['topic'],
                payload=payload,
                created_at=row['created_at'],
                processed_at=row['processed_at'],
                retry_count=row['retry_count'],
                last_retry_at=row['last_retry_at'],
                entity_version=entity_version
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