"""
Synchronous API Wrapper Models

동기 API 래퍼를 위한 요청/응답 모델 정의
"""

from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


class SyncOptions(BaseModel):
    """동기 API 실행 옵션"""
    
    timeout: float = Field(
        default=30.0,
        ge=1.0,
        le=300.0,
        description="최대 대기 시간 (초). 1-300초 사이"
    )
    
    poll_interval: float = Field(
        default=0.5,
        ge=0.1,
        le=5.0,
        description="상태 확인 간격 (초). 0.1-5초 사이"
    )
    
    include_progress: bool = Field(
        default=True,
        description="응답에 진행률 정보 포함 여부"
    )
    
    retry_on_failure: bool = Field(
        default=False,
        description="실패 시 재시도 여부"
    )
    
    max_retries: int = Field(
        default=3,
        ge=1,
        le=10,
        description="최대 재시도 횟수 (retry_on_failure가 True일 때만 적용)"
    )


class SyncResult(BaseModel):
    """동기 API 실행 결과"""
    
    success: bool = Field(
        description="작업 성공 여부"
    )
    
    command_id: str = Field(
        description="실행된 Command ID"
    )
    
    data: Optional[Dict[str, Any]] = Field(
        default=None,
        description="작업 결과 데이터"
    )
    
    error: Optional[str] = Field(
        default=None,
        description="에러 메시지 (실패 시)"
    )
    
    execution_time: float = Field(
        description="실행 시간 (초)"
    )
    
    final_status: str = Field(
        description="최종 상태 (COMPLETED, FAILED, TIMEOUT)"
    )
    
    progress_history: Optional[list] = Field(
        default=None,
        description="진행률 이력 (include_progress가 True일 때)"
    )
    
    retry_count: int = Field(
        default=0,
        description="재시도 횟수"
    )


class TimeoutError(Exception):
    """Command 실행 타임아웃 에러"""
    
    def __init__(self, command_id: str, timeout: float, last_status: str):
        self.command_id = command_id
        self.timeout = timeout
        self.last_status = last_status
        super().__init__(
            f"Command {command_id} timed out after {timeout} seconds. "
            f"Last status: {last_status}"
        )