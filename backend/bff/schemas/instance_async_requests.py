"""Async instance request schemas (BFF).

These schemas are shared across async instance endpoints that accept label-keyed
payloads and forward commands to OMS.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class InstanceCreateRequest(BaseModel):
    """인스턴스 생성 요청 (Label 기반)"""

    data: Dict[str, Any] = Field(..., description="인스턴스 데이터 (Label 키 사용)")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="메타데이터")


class InstanceUpdateRequest(BaseModel):
    """인스턴스 수정 요청 (Label 기반)"""

    data: Dict[str, Any] = Field(..., description="수정할 데이터 (Label 키 사용)")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="메타데이터")


class BulkInstanceCreateRequest(BaseModel):
    """대량 인스턴스 생성 요청 (Label 기반)"""

    instances: List[Dict[str, Any]] = Field(..., description="인스턴스 데이터 목록 (Label 키 사용)")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="메타데이터")

