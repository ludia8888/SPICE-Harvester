"""
Outbox Pattern implementation for reliable event publishing
이벤트의 원자적 발행을 위한 Outbox 패턴 구현
"""

import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from enum import Enum

import asyncpg
from pydantic import BaseModel, Field

from oms.database.postgres import PostgresDatabase


class EventType(str, Enum):
    """이벤트 유형 정의"""
    ONTOLOGY_CLASS_CREATED = "ONTOLOGY_CLASS_CREATED"
    ONTOLOGY_CLASS_UPDATED = "ONTOLOGY_CLASS_UPDATED"
    ONTOLOGY_CLASS_DELETED = "ONTOLOGY_CLASS_DELETED"
    PROPERTY_CREATED = "PROPERTY_CREATED"
    PROPERTY_UPDATED = "PROPERTY_UPDATED"
    PROPERTY_DELETED = "PROPERTY_DELETED"
    RELATIONSHIP_CREATED = "RELATIONSHIP_CREATED"
    RELATIONSHIP_UPDATED = "RELATIONSHIP_UPDATED"
    RELATIONSHIP_DELETED = "RELATIONSHIP_DELETED"


class OutboxEvent(BaseModel):
    """Outbox 이벤트 모델"""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    aggregate_type: str
    aggregate_id: str
    topic: str
    payload: Dict[str, Any]
    created_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None
    retry_count: int = 0
    last_retry_at: Optional[datetime] = None


class OutboxService:
    """Outbox 패턴을 사용한 이벤트 발행 서비스"""
    
    def __init__(self, db: PostgresDatabase):
        self.db = db
        
    async def publish_event(
        self,
        connection: asyncpg.Connection,
        event_type: EventType,
        aggregate_type: str,
        aggregate_id: str,
        data: Dict[str, Any],
        topic: str = "ontology_events",
        additional_context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        트랜잭션 내에서 이벤트를 outbox 테이블에 발행
        
        Args:
            connection: 활성 트랜잭션 연결
            event_type: 이벤트 유형
            aggregate_type: 엔티티 유형 (OntologyClass, Property 등)
            aggregate_id: 엔티티 ID
            data: 이벤트 데이터
            topic: Kafka 토픽 이름
            additional_context: 추가 컨텍스트 정보
            
        Returns:
            생성된 이벤트 ID
        """
        event_id = str(uuid.uuid4())
        
        # 이벤트 페이로드 구성
        payload = {
            "event_id": event_id,
            "event_type": event_type,
            "timestamp": datetime.utcnow().isoformat(),
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "data": data,
        }
        
        # 추가 컨텍스트가 있으면 포함
        if additional_context:
            payload.update(additional_context)
        
        # Outbox 테이블에 이벤트 삽입
        await connection.execute(
            """
            INSERT INTO spice_outbox.outbox (id, aggregate_type, aggregate_id, topic, payload)
            VALUES ($1, $2, $3, $4, $5)
            """,
            event_id,
            aggregate_type,
            aggregate_id,
            topic,
            json.dumps(payload)
        )
        
        return event_id
        
    async def get_unprocessed_events(self, limit: int = 100) -> List[OutboxEvent]:
        """
        처리되지 않은 이벤트 조회
        
        Args:
            limit: 조회할 최대 이벤트 수
            
        Returns:
            OutboxEvent 리스트
        """
        rows = await self.db.fetch(
            """
            SELECT id, aggregate_type, aggregate_id, topic, payload, 
                   created_at, processed_at, retry_count, last_retry_at
            FROM spice_outbox.outbox
            WHERE processed_at IS NULL
            ORDER BY created_at
            LIMIT $1
            """,
            limit
        )
        
        events = []
        for row in rows:
            events.append(OutboxEvent(
                id=str(row['id']),
                aggregate_type=row['aggregate_type'],
                aggregate_id=row['aggregate_id'],
                topic=row['topic'],
                payload=json.loads(row['payload']),
                created_at=row['created_at'],
                processed_at=row['processed_at'],
                retry_count=row['retry_count'],
                last_retry_at=row['last_retry_at']
            ))
            
        return events
        
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