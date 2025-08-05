"""
Event Models for Command/Event Sourcing Pattern
실행 결과(사실)를 나타내는 모델들
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class EventType(str, Enum):
    """이벤트 유형"""
    # Ontology Events (성공)
    ONTOLOGY_CLASS_CREATED = "ONTOLOGY_CLASS_CREATED"
    ONTOLOGY_CLASS_UPDATED = "ONTOLOGY_CLASS_UPDATED"
    ONTOLOGY_CLASS_DELETED = "ONTOLOGY_CLASS_DELETED"
    
    # Property Events (성공)
    PROPERTY_CREATED = "PROPERTY_CREATED"
    PROPERTY_UPDATED = "PROPERTY_UPDATED"
    PROPERTY_DELETED = "PROPERTY_DELETED"
    
    # Relationship Events (성공)
    RELATIONSHIP_CREATED = "RELATIONSHIP_CREATED"
    RELATIONSHIP_UPDATED = "RELATIONSHIP_UPDATED"
    RELATIONSHIP_DELETED = "RELATIONSHIP_DELETED"
    
    # Database Events (성공)
    DATABASE_CREATED = "DATABASE_CREATED"
    DATABASE_DELETED = "DATABASE_DELETED"
    
    # Branch Events (성공)
    BRANCH_CREATED = "BRANCH_CREATED"
    BRANCH_DELETED = "BRANCH_DELETED"
    BRANCH_MERGED = "BRANCH_MERGED"
    BRANCH_REBASED = "BRANCH_REBASED"
    
    # Instance Events (성공)
    INSTANCE_CREATED = "INSTANCE_CREATED"
    INSTANCE_UPDATED = "INSTANCE_UPDATED"
    INSTANCE_DELETED = "INSTANCE_DELETED"
    INSTANCES_BULK_CREATED = "INSTANCES_BULK_CREATED"
    INSTANCES_BULK_UPDATED = "INSTANCES_BULK_UPDATED"
    INSTANCES_BULK_DELETED = "INSTANCES_BULK_DELETED"
    
    # Failure Events (실패)
    COMMAND_FAILED = "COMMAND_FAILED"
    COMMAND_RETRY_EXHAUSTED = "COMMAND_RETRY_EXHAUSTED"


class BaseEvent(BaseModel):
    """기본 이벤트 모델"""
    event_id: UUID = Field(default_factory=uuid4, description="이벤트 고유 ID")
    event_type: EventType = Field(..., description="이벤트 유형")
    aggregate_type: str = Field(..., description="대상 엔티티 유형")
    aggregate_id: str = Field(..., description="대상 엔티티 ID")
    command_id: Optional[UUID] = Field(None, description="원인이 된 명령 ID")
    data: Dict[str, Any] = Field(..., description="이벤트 데이터")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="메타데이터")
    occurred_at: datetime = Field(default_factory=datetime.utcnow, description="발생 시각")
    occurred_by: Optional[str] = Field(None, description="발생자")
    version: int = Field(1, description="이벤트 버전")
    
    class Config:
        json_encoders = {
            UUID: str,
            datetime: lambda v: v.isoformat()
        }


class OntologyEvent(BaseEvent):
    """온톨로지 관련 이벤트"""
    db_name: str = Field(..., description="데이터베이스 이름")
    class_id: str = Field(..., description="클래스 ID")
    
    def __init__(self, **data):
        if "aggregate_type" not in data:
            data["aggregate_type"] = "OntologyClass"
        if "aggregate_id" not in data:
            data["aggregate_id"] = data.get("class_id", "")
        super().__init__(**data)


class PropertyEvent(BaseEvent):
    """속성 관련 이벤트"""
    db_name: str = Field(..., description="데이터베이스 이름")
    class_id: str = Field(..., description="클래스 ID")
    property_name: str = Field(..., description="속성 이름")
    
    def __init__(self, **data):
        if "aggregate_type" not in data:
            data["aggregate_type"] = "Property"
        if "aggregate_id" not in data:
            data["aggregate_id"] = f"{data.get('class_id', '')}.{data.get('property_name', '')}"
        super().__init__(**data)


class RelationshipEvent(BaseEvent):
    """관계 관련 이벤트"""
    db_name: str = Field(..., description="데이터베이스 이름")
    relationship_id: str = Field(..., description="관계 ID")
    source_class: str = Field(..., description="출발 클래스")
    target_class: str = Field(..., description="도착 클래스")
    
    def __init__(self, **data):
        if "aggregate_type" not in data:
            data["aggregate_type"] = "Relationship"
        if "aggregate_id" not in data:
            data["aggregate_id"] = data.get("relationship_id", "")
        super().__init__(**data)


class DatabaseEvent(BaseEvent):
    """데이터베이스 관련 이벤트"""
    db_name: str = Field(..., description="데이터베이스 이름")
    
    def __init__(self, **data):
        if "aggregate_type" not in data:
            data["aggregate_type"] = "Database"
        if "aggregate_id" not in data:
            data["aggregate_id"] = data.get("db_name", "")
        super().__init__(**data)


class BranchEvent(BaseEvent):
    """브랜치 관련 이벤트"""
    db_name: str = Field(..., description="데이터베이스 이름")
    branch_name: str = Field(..., description="브랜치 이름")
    
    def __init__(self, **data):
        if "aggregate_type" not in data:
            data["aggregate_type"] = "Branch"
        if "aggregate_id" not in data:
            data["aggregate_id"] = f"{data.get('db_name', '')}/{data.get('branch_name', '')}"
        super().__init__(**data)


class InstanceEvent(BaseEvent):
    """인스턴스 관련 이벤트"""
    db_name: str = Field(..., description="데이터베이스 이름")
    class_id: str = Field(..., description="클래스 ID")
    instance_id: str = Field(..., description="인스턴스 ID")
    s3_path: Optional[str] = Field(None, description="S3 저장 경로")
    s3_checksum: Optional[str] = Field(None, description="S3 저장 파일의 SHA256 체크섬")
    
    def __init__(self, **data):
        if "aggregate_type" not in data:
            data["aggregate_type"] = "Instance"
        if "aggregate_id" not in data:
            # aggregate_id 형식: {db_name}:{class_id}:{instance_id}
            data["aggregate_id"] = f"{data.get('db_name', '')}:{data.get('class_id', '')}:{data.get('instance_id', '')}"
        super().__init__(**data)


class CommandFailedEvent(BaseEvent):
    """명령 실패 이벤트"""
    command_type: str = Field(..., description="실패한 명령 유형")
    error_message: str = Field(..., description="에러 메시지")
    error_details: Optional[Dict[str, Any]] = Field(None, description="에러 상세 정보")
    retry_count: int = Field(0, description="재시도 횟수")
    
    def __init__(self, **data):
        if "event_type" not in data:
            data["event_type"] = EventType.COMMAND_FAILED
        super().__init__(**data)