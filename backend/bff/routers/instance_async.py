"""
BFF 비동기 인스턴스 라우터 - Command Pattern 기반
사용자 친화적인 Label 기반 인터페이스 제공
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from bff.dependencies import (
    get_oms_client,
    get_label_mapper,
)
from bff.services.oms_client import OMSClient
from shared.utils.label_mapper import LabelMapper
from shared.models.commands import CommandResult, CommandStatus
from shared.models.common import BaseResponse
from shared.security.input_sanitizer import (
    SecurityViolationError,
    sanitize_input,
    validate_db_name,
    validate_class_id,
    validate_instance_id,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/database/{db_name}/instances", tags=["Async Instance Management"])


# Request Models (Label 기반)
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


async def convert_labels_to_ids(
    data: Dict[str, Any],
    db_name: str,
    class_id: str,
    label_mapper: LabelMapper
) -> Dict[str, Any]:
    """
    Label 기반 데이터를 ID 기반으로 변환
    
    Args:
        data: Label 기반 데이터
        db_name: 데이터베이스 이름
        class_id: 클래스 ID
        label_mapper: Label 매퍼
        
    Returns:
        ID 기반 데이터
    """
    # 클래스의 속성 매핑 정보 가져오기
    mappings = await label_mapper.get_class_mappings(db_name, class_id)
    
    converted_data = {}
    for label, value in data.items():
        # Label을 ID로 변환
        property_id = None
        for prop_id, prop_info in mappings.get("properties", {}).items():
            if prop_info.get("label") == label:
                property_id = prop_id
                break
        
        if property_id:
            converted_data[property_id] = value
        else:
            # 매핑되지 않은 Label은 그대로 사용 (새로운 속성일 수 있음)
            logger.warning(f"No mapping found for label '{label}' in class '{class_id}'")
            converted_data[label] = value
    
    return converted_data


@router.post("/{class_label}/create", response_model=CommandResult)
async def create_instance_async(
    db_name: str,
    class_label: str,
    request: InstanceCreateRequest,
    oms_client: OMSClient = Depends(get_oms_client),
    label_mapper: LabelMapper = Depends(get_label_mapper),
    user_id: Optional[str] = None,
):
    """
    인스턴스 생성 명령을 비동기로 처리 (Label 기반)
    
    사용자는 Label을 사용하여 데이터를 제공하며,
    BFF가 이를 ID로 변환하여 OMS에 전달합니다.
    """
    try:
        # 입력 검증
        db_name = validate_db_name(db_name)
        sanitized_data = sanitize_input(request.data)
        
        # Label을 ID로 변환
        class_id = await label_mapper.get_class_id_by_label(db_name, class_label)
        if not class_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"클래스 '{class_label}'을 찾을 수 없습니다"
            )
        
        # 데이터의 Label을 ID로 변환
        converted_data = await convert_labels_to_ids(
            sanitized_data,
            db_name,
            class_id,
            label_mapper
        )
        
        # OMS 비동기 API 호출
        oms_request = {
            "data": converted_data,
            "metadata": {
                **request.metadata,
                "original_class_label": class_label,
                "user_id": user_id
            }
        }
        
        response = await oms_client.post(
            f"/instances/{db_name}/async/{class_id}/create",
            json=oms_request
        )
        
        # OMS 응답을 그대로 반환
        return CommandResult(**response)
        
    except HTTPException:
        raise
    except SecurityViolationError as e:
        logger.warning(f"Security violation in instance creation: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error creating instance command: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"인스턴스 생성 명령 실패: {str(e)}"
        )


@router.put("/{class_label}/{instance_id}/update", response_model=CommandResult)
async def update_instance_async(
    db_name: str,
    class_label: str,
    instance_id: str,
    request: InstanceUpdateRequest,
    oms_client: OMSClient = Depends(get_oms_client),
    label_mapper: LabelMapper = Depends(get_label_mapper),
    user_id: Optional[str] = None,
):
    """
    인스턴스 수정 명령을 비동기로 처리 (Label 기반)
    """
    try:
        # 입력 검증
        db_name = validate_db_name(db_name)
        validate_instance_id(instance_id)
        sanitized_data = sanitize_input(request.data)
        
        # Label을 ID로 변환
        class_id = await label_mapper.get_class_id_by_label(db_name, class_label)
        if not class_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"클래스 '{class_label}'을 찾을 수 없습니다"
            )
        
        # 데이터의 Label을 ID로 변환
        converted_data = await convert_labels_to_ids(
            sanitized_data,
            db_name,
            class_id,
            label_mapper
        )
        
        # OMS 비동기 API 호출
        oms_request = {
            "data": converted_data,
            "metadata": {
                **request.metadata,
                "original_class_label": class_label,
                "user_id": user_id
            }
        }
        
        response = await oms_client.put(
            f"/instances/{db_name}/async/{class_id}/{instance_id}/update",
            json=oms_request
        )
        
        return CommandResult(**response)
        
    except HTTPException:
        raise
    except SecurityViolationError as e:
        logger.warning(f"Security violation in instance update: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error updating instance command: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"인스턴스 수정 명령 실패: {str(e)}"
        )


@router.delete("/{class_label}/{instance_id}/delete", response_model=CommandResult)
async def delete_instance_async(
    db_name: str,
    class_label: str,
    instance_id: str,
    oms_client: OMSClient = Depends(get_oms_client),
    label_mapper: LabelMapper = Depends(get_label_mapper),
    user_id: Optional[str] = None,
):
    """
    인스턴스 삭제 명령을 비동기로 처리 (Label 기반)
    """
    try:
        # 입력 검증
        db_name = validate_db_name(db_name)
        validate_instance_id(instance_id)
        
        # Label을 ID로 변환
        class_id = await label_mapper.get_class_id_by_label(db_name, class_label)
        if not class_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"클래스 '{class_label}'을 찾을 수 없습니다"
            )
        
        # OMS 비동기 API 호출
        response = await oms_client.delete(
            f"/instances/{db_name}/async/{class_id}/{instance_id}/delete"
        )
        
        return CommandResult(**response)
        
    except HTTPException:
        raise
    except SecurityViolationError as e:
        logger.warning(f"Security violation in instance deletion: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error deleting instance command: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"인스턴스 삭제 명령 실패: {str(e)}"
        )


@router.post("/{class_label}/bulk-create", response_model=CommandResult)
async def bulk_create_instances_async(
    db_name: str,
    class_label: str,
    request: BulkInstanceCreateRequest,
    oms_client: OMSClient = Depends(get_oms_client),
    label_mapper: LabelMapper = Depends(get_label_mapper),
    user_id: Optional[str] = None,
):
    """
    대량 인스턴스 생성 명령을 비동기로 처리 (Label 기반)
    """
    try:
        # 입력 검증
        db_name = validate_db_name(db_name)
        sanitized_instances = [sanitize_input(instance) for instance in request.instances]
        
        # Label을 ID로 변환
        class_id = await label_mapper.get_class_id_by_label(db_name, class_label)
        if not class_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"클래스 '{class_label}'을 찾을 수 없습니다"
            )
        
        # 각 인스턴스 데이터의 Label을 ID로 변환
        converted_instances = []
        for instance_data in sanitized_instances:
            converted_data = await convert_labels_to_ids(
                instance_data,
                db_name,
                class_id,
                label_mapper
            )
            converted_instances.append(converted_data)
        
        # OMS 비동기 API 호출
        oms_request = {
            "instances": converted_instances,
            "metadata": {
                **request.metadata,
                "original_class_label": class_label,
                "user_id": user_id
            }
        }
        
        response = await oms_client.post(
            f"/instances/{db_name}/async/{class_id}/bulk-create",
            json=oms_request
        )
        
        return CommandResult(**response)
        
    except HTTPException:
        raise
    except SecurityViolationError as e:
        logger.warning(f"Security violation in bulk instance creation: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error creating bulk instance command: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"대량 인스턴스 생성 명령 실패: {str(e)}"
        )


@router.get("/command/{command_id}/status", response_model=CommandResult)
async def get_instance_command_status(
    db_name: str,
    command_id: str,
    oms_client: OMSClient = Depends(get_oms_client),
):
    """
    인스턴스 명령의 상태 조회
    """
    try:
        # 입력 검증
        db_name = validate_db_name(db_name)
        
        # OMS API 호출
        response = await oms_client.get(
            f"/instances/{db_name}/async/command/{command_id}/status"
        )
        
        return CommandResult(**response)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting command status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"명령 상태 조회 실패: {str(e)}"
        )