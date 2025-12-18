"""
Command Models for Command/Event Sourcing Pattern
명령(의도)을 나타내는 모델들
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class CommandType(str, Enum):
    """명령 유형"""
    # Ontology Commands
    CREATE_ONTOLOGY_CLASS = "CREATE_ONTOLOGY_CLASS"
    UPDATE_ONTOLOGY_CLASS = "UPDATE_ONTOLOGY_CLASS"
    DELETE_ONTOLOGY_CLASS = "DELETE_ONTOLOGY_CLASS"
    
    # Property Commands
    CREATE_PROPERTY = "CREATE_PROPERTY"
    UPDATE_PROPERTY = "UPDATE_PROPERTY"
    DELETE_PROPERTY = "DELETE_PROPERTY"
    
    # Relationship Commands
    CREATE_RELATIONSHIP = "CREATE_RELATIONSHIP"
    UPDATE_RELATIONSHIP = "UPDATE_RELATIONSHIP"
    DELETE_RELATIONSHIP = "DELETE_RELATIONSHIP"
    
    # Database Commands
    CREATE_DATABASE = "CREATE_DATABASE"
    DELETE_DATABASE = "DELETE_DATABASE"
    
    # Branch Commands
    CREATE_BRANCH = "CREATE_BRANCH"
    DELETE_BRANCH = "DELETE_BRANCH"
    MERGE_BRANCH = "MERGE_BRANCH"
    REBASE_BRANCH = "REBASE_BRANCH"
    
    # Instance Commands
    CREATE_INSTANCE = "CREATE_INSTANCE"
    UPDATE_INSTANCE = "UPDATE_INSTANCE"
    DELETE_INSTANCE = "DELETE_INSTANCE"
    BULK_CREATE_INSTANCES = "BULK_CREATE_INSTANCES"
    BULK_UPDATE_INSTANCES = "BULK_UPDATE_INSTANCES"
    BULK_DELETE_INSTANCES = "BULK_DELETE_INSTANCES"


class CommandStatus(str, Enum):
    """명령 상태"""
    PENDING = "PENDING"  # 대기 중
    PROCESSING = "PROCESSING"  # 처리 중
    COMPLETED = "COMPLETED"  # 완료
    FAILED = "FAILED"  # 실패
    CANCELLED = "CANCELLED"  # 취소됨
    RETRYING = "RETRYING"  # 재시도 중


class BaseCommand(BaseModel):
    """기본 명령 모델"""
    command_id: UUID = Field(default_factory=uuid4, description="명령 고유 ID")
    command_type: CommandType = Field(..., description="명령 유형")
    aggregate_type: str = Field(..., description="대상 엔티티 유형")
    aggregate_id: str = Field(..., description="대상 엔티티 ID")
    payload: Dict[str, Any] = Field(..., description="명령 데이터")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="메타데이터")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="생성 시각")
    created_by: Optional[str] = Field(None, description="생성자")
    version: int = Field(1, description="명령 버전")
    checksum: Optional[str] = Field(None, description="페이로드 체크섬 (SHA256)")
    expected_seq: Optional[int] = Field(
        None,
        description="Optimistic concurrency guard: expected current aggregate sequence",
        ge=0,
    )


class OntologyCommand(BaseCommand):
    """온톨로지 관련 명령"""
    db_name: str = Field(..., description="데이터베이스 이름")
    branch: Optional[str] = Field("main", description="브랜치 이름")
    
    def __init__(self, **data):
        if "aggregate_type" not in data:
            data["aggregate_type"] = "OntologyClass"
        super().__init__(**data)


class PropertyCommand(BaseCommand):
    """속성 관련 명령"""
    db_name: str = Field(..., description="데이터베이스 이름")
    class_id: str = Field(..., description="클래스 ID")
    
    def __init__(self, **data):
        if "aggregate_type" not in data:
            data["aggregate_type"] = "Property"
        super().__init__(**data)


class RelationshipCommand(BaseCommand):
    """관계 관련 명령"""
    db_name: str = Field(..., description="데이터베이스 이름")
    source_class: str = Field(..., description="출발 클래스")
    target_class: str = Field(..., description="도착 클래스")
    
    def __init__(self, **data):
        if "aggregate_type" not in data:
            data["aggregate_type"] = "Relationship"
        super().__init__(**data)


class DatabaseCommand(BaseCommand):
    """데이터베이스 관련 명령"""
    def __init__(self, **data):
        if "aggregate_type" not in data:
            data["aggregate_type"] = "Database"
        super().__init__(**data)


class BranchCommand(BaseCommand):
    """브랜치 관련 명령"""
    db_name: str = Field(..., description="데이터베이스 이름")
    
    def __init__(self, **data):
        if "aggregate_type" not in data:
            data["aggregate_type"] = "Branch"
        super().__init__(**data)


class InstanceCommand(BaseCommand):
    """인스턴스 관련 명령"""
    db_name: str = Field(..., description="데이터베이스 이름")
    class_id: str = Field(..., description="클래스 ID")
    instance_id: Optional[str] = Field(None, description="인스턴스 ID (UPDATE/DELETE 시 필수)")
    branch: Optional[str] = Field("main", description="브랜치 이름")
    
    def __init__(self, **data):
        if "aggregate_type" not in data:
            data["aggregate_type"] = "Instance"
        # aggregate_id 형식: {db_name}:{class_id}:{instance_id}
        if "aggregate_id" not in data:
            parts = [data.get("db_name", ""), data.get("class_id", "")]
            if data.get("instance_id"):
                parts.append(data["instance_id"])
            data["aggregate_id"] = ":".join(parts)
        super().__init__(**data)


class CommandResult(BaseModel):
    """명령 실행 결과"""
    command_id: UUID = Field(..., description="명령 ID")
    status: CommandStatus = Field(..., description="실행 상태")
    result: Optional[Dict[str, Any]] = Field(None, description="실행 결과")
    error: Optional[str] = Field(None, description="에러 메시지")
    completed_at: Optional[datetime] = Field(None, description="완료 시각")
    retry_count: int = Field(0, description="재시도 횟수")
