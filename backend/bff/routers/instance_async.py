"""
BFF 비동기 인스턴스 라우터 - Command Pattern 기반
사용자 친화적인 Label 기반 인터페이스 제공
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, status, Query
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
    sanitize_label_input,
    input_sanitizer,
    validate_branch_name,
    validate_db_name,
    validate_class_id,
    validate_instance_id,
)
from shared.utils.language import get_accept_language

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/databases/{db_name}/instances", tags=["Async Instance Management"])

def _raise_httpx_as_http_exception(exc: httpx.HTTPStatusError) -> None:
    resp = getattr(exc, "response", None)
    if resp is None:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="OMS 요청 실패",
        ) from exc

    detail: Any
    try:
        payload = resp.json()
        if isinstance(payload, dict) and "detail" in payload:
            detail = payload["detail"]
        else:
            detail = payload
    except Exception:
        detail = (resp.text or "").strip() or f"OMS returned HTTP {resp.status_code}"

    raise HTTPException(status_code=int(resp.status_code), detail=detail) from exc


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
    label_mapper: LabelMapper,
    *,
    lang: str = "ko",
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
    def _fallback_langs(primary: str) -> List[str]:
        langs = [primary, "ko", "en"]
        out: List[str] = []
        for item in langs:
            if item and item not in out:
                out.append(item)
        return out

    converted: Dict[str, Any] = {}
    unknown_labels: List[str] = []

    for label, value in data.items():
        property_id: Optional[str] = None
        for candidate_lang in _fallback_langs(lang):
            property_id = await label_mapper.get_property_id(db_name, class_id, label, candidate_lang)
            if property_id:
                break

        if property_id:
            converted[property_id] = value
            continue

        # Allow advanced clients to pass internal property_id directly (must be a safe field name).
        try:
            input_sanitizer.sanitize_field_name(label)
            converted[label] = value
        except Exception:
            unknown_labels.append(label)

    if unknown_labels:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "unknown_label_keys",
                "class_id": class_id,
                "labels": unknown_labels,
                "hint": "Use existing property labels (as defined in ontology) or pass internal property_id keys.",
            },
        )

    return converted


@router.post(
    "/{class_label}/create",
    response_model=CommandResult,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_instance_async(
    db_name: str,
    class_label: str,
    request: InstanceCreateRequest,
    http_request: Request,
    branch: str = Query("main", description="Target branch (default: main)"),
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
        branch = validate_branch_name(branch)
        sanitized_data = sanitize_label_input(request.data)
        
        lang = get_accept_language(http_request)

        # Resolve class_label → class_id (fallback: treat class_label as class_id)
        class_id = await label_mapper.get_class_id(db_name, class_label, lang)
        if not class_id:
            class_id = await label_mapper.get_class_id(db_name, class_label, "ko")
        if not class_id:
            class_id = await label_mapper.get_class_id(db_name, class_label, "en")
        if not class_id:
            # Last resort: accept internal class_id directly if valid.
            class_id = validate_class_id(class_label)
        
        # 데이터의 Label을 ID로 변환
        converted_data = await convert_labels_to_ids(
            sanitized_data,
            db_name,
            class_id,
            label_mapper,
            lang=lang,
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
            f"/api/v1/instances/{db_name}/async/{class_id}/create",
            params={"branch": branch},
            json=oms_request
        )
        
        # OMS 응답을 그대로 반환
        return CommandResult(**response)
        
    except HTTPException:
        raise
    except httpx.HTTPStatusError as e:
        _raise_httpx_as_http_exception(e)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OMS 요청 실패") from e
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


@router.put(
    "/{class_label}/{instance_id}/update",
    response_model=CommandResult,
    status_code=status.HTTP_202_ACCEPTED,
)
async def update_instance_async(
    db_name: str,
    class_label: str,
    instance_id: str,
    request: InstanceUpdateRequest,
    http_request: Request,
    expected_seq: int = Query(..., ge=0, description="Expected current aggregate sequence (OCC)"),
    branch: str = Query("main", description="Target branch (default: main)"),
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
        branch = validate_branch_name(branch)
        sanitized_data = sanitize_label_input(request.data)
        
        lang = get_accept_language(http_request)

        class_id = await label_mapper.get_class_id(db_name, class_label, lang)
        if not class_id:
            class_id = await label_mapper.get_class_id(db_name, class_label, "ko")
        if not class_id:
            class_id = await label_mapper.get_class_id(db_name, class_label, "en")
        if not class_id:
            class_id = validate_class_id(class_label)
        
        # 데이터의 Label을 ID로 변환
        converted_data = await convert_labels_to_ids(
            sanitized_data,
            db_name,
            class_id,
            label_mapper,
            lang=lang,
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
            f"/api/v1/instances/{db_name}/async/{class_id}/{instance_id}/update",
            params={"expected_seq": expected_seq, "branch": branch},
            json=oms_request
        )
        
        return CommandResult(**response)
        
    except HTTPException:
        raise
    except httpx.HTTPStatusError as e:
        _raise_httpx_as_http_exception(e)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OMS 요청 실패") from e
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


@router.delete(
    "/{class_label}/{instance_id}/delete",
    response_model=CommandResult,
    status_code=status.HTTP_202_ACCEPTED,
)
async def delete_instance_async(
    db_name: str,
    class_label: str,
    instance_id: str,
    http_request: Request,
    branch: str = Query("main", description="Target branch (default: main)"),
    expected_seq: int = Query(..., ge=0, description="Expected current aggregate sequence (OCC)"),
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
        branch = validate_branch_name(branch)
        
        lang = get_accept_language(http_request)

        class_id = await label_mapper.get_class_id(db_name, class_label, lang)
        if not class_id:
            class_id = await label_mapper.get_class_id(db_name, class_label, "ko")
        if not class_id:
            class_id = await label_mapper.get_class_id(db_name, class_label, "en")
        if not class_id:
            class_id = validate_class_id(class_label)
        
        # OMS 비동기 API 호출
        response = await oms_client.delete(
            f"/api/v1/instances/{db_name}/async/{class_id}/{instance_id}/delete",
            params={"expected_seq": expected_seq, "branch": branch},
        )
        
        return CommandResult(**response)
        
    except HTTPException:
        raise
    except httpx.HTTPStatusError as e:
        _raise_httpx_as_http_exception(e)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OMS 요청 실패") from e
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


@router.post(
    "/{class_label}/bulk-create",
    response_model=CommandResult,
    status_code=status.HTTP_202_ACCEPTED,
)
async def bulk_create_instances_async(
    db_name: str,
    class_label: str,
    request: BulkInstanceCreateRequest,
    http_request: Request,
    branch: str = Query("main", description="Target branch (default: main)"),
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
        branch = validate_branch_name(branch)
        sanitized_instances = [sanitize_label_input(instance) for instance in request.instances]
        
        lang = get_accept_language(http_request)

        class_id = await label_mapper.get_class_id(db_name, class_label, lang)
        if not class_id:
            class_id = await label_mapper.get_class_id(db_name, class_label, "ko")
        if not class_id:
            class_id = await label_mapper.get_class_id(db_name, class_label, "en")
        if not class_id:
            class_id = validate_class_id(class_label)
        
        # 각 인스턴스 데이터의 Label을 ID로 변환
        converted_instances = []
        for instance_data in sanitized_instances:
            converted_data = await convert_labels_to_ids(
                instance_data,
                db_name,
                class_id,
                label_mapper,
                lang=lang,
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
            f"/api/v1/instances/{db_name}/async/{class_id}/bulk-create",
            params={"branch": branch},
            json=oms_request
        )
        
        return CommandResult(**response)
        
    except HTTPException:
        raise
    except httpx.HTTPStatusError as e:
        _raise_httpx_as_http_exception(e)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OMS 요청 실패") from e
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


#
# NOTE: Command status is global and should be queried via:
#   GET /api/v1/commands/{command_id}/status
# This avoids misleading db-scoped status endpoints that cannot reliably prove
# the command belongs to the given db_name.
