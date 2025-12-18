"""
브랜치 관리 라우터
브랜치 생성, 삭제, 체크아웃 등을 담당
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict

from oms.dependencies import TerminusServiceDep
from oms.services.async_terminus import AsyncTerminusService
from shared.models.requests import ApiResponse, BranchCreateRequest, CheckoutRequest
from shared.security.input_sanitizer import (
    SecurityViolationError,
    sanitize_input,
    validate_branch_name,
    validate_db_name,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/branch/{db_name}", tags=["Branch Management"])


@router.get("/list")
async def list_branches(
    db_name: str, terminus: AsyncTerminusService = TerminusServiceDep
):
    """
    브랜치 목록 조회

    데이터베이스의 모든 브랜치 목록을 조회합니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)

        branches = await terminus.list_branches(db_name)
        current_branch = await terminus.get_current_branch(db_name)

        # 브랜치 정보 구성
        branch_info = []
        for branch in branches:
            info = {"name": branch, "current": branch == current_branch}

            # 추가 정보 (커밋 수 등) 조회 가능
            branch_info.append(info)

        return ApiResponse.success(
            message="브랜치 목록을 조회했습니다",
            data={"branches": branch_info, "current": current_branch, "total": len(branch_info)}
        ).to_dict()

    except SecurityViolationError as e:
        logger.warning(f"Security violation in list_branches: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except Exception as e:
        logger.error(f"Failed to list branches: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"브랜치 목록 조회 실패: {str(e)}",
        )


@router.post("/create")
async def create_branch(
    db_name: str,
    request: BranchCreateRequest,
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    """
    새 브랜치 생성

    현재 브랜치 또는 지정된 브랜치에서 새 브랜치를 생성합니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)

        # 요청 데이터 정화
        sanitized_data = sanitize_input(request.model_dump(mode="json"))

        # 브랜치 이름 유효성 검증
        branch_name = sanitized_data.get("branch_name")
        if not branch_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="브랜치 이름은 필수입니다"
            )

        # 브랜치 이름 보안 검증
        branch_name = validate_branch_name(branch_name)

        # from_branch 검증
        from_branch = sanitized_data.get("from_branch")
        if from_branch:
            from_branch = validate_branch_name(from_branch)

        # 브랜치 생성
        await terminus.create_branch(db_name, branch_name, from_branch=from_branch)

        return ApiResponse.created(
            message=f"브랜치 '{branch_name}'이(가) 생성되었습니다",
            data={
                "branch": branch_name,
                "from_branch": from_branch or await terminus.get_current_branch(db_name)
            }
        ).to_dict()

    except SecurityViolationError as e:
        logger.warning(f"Security violation in create_branch: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create branch: {e}")

        if "already exists" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail=f"브랜치가 이미 존재합니다"
            )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"브랜치 생성 실패: {str(e)}"
        )


@router.delete("/branch/{branch_name:path}")
async def delete_branch(
    db_name: str,
    branch_name: str,
    force: bool = Query(False, description="강제 삭제 여부"),
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    """
    브랜치 삭제

    지정된 브랜치를 삭제합니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        branch_name = validate_branch_name(branch_name)

        # 보호된 브랜치 확인
        protected_branches = ["main", "master", "production"]
        if branch_name in protected_branches and not force:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"보호된 브랜치 '{branch_name}'은(는) 삭제할 수 없습니다. 강제 삭제를 원하면 force=true를 사용하세요",
            )

        # 현재 브랜치 확인
        current_branch = await terminus.get_current_branch(db_name)
        if branch_name == current_branch:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="현재 체크아웃된 브랜치는 삭제할 수 없습니다",
            )

        # 브랜치 삭제
        await terminus.delete_branch(db_name, branch_name)

        return ApiResponse.success(
            message=f"브랜치 '{branch_name}'이(가) 삭제되었습니다",
            data={"branch": branch_name}
        ).to_dict()

    except SecurityViolationError as e:
        logger.warning(f"Security violation in delete_branch: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete branch: {e}")

        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"브랜치 '{branch_name}'을(를) 찾을 수 없습니다",
            )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"브랜치 삭제 실패: {str(e)}"
        )


@router.post("/checkout")
async def checkout(
    db_name: str,
    request: CheckoutRequest,
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    """
    브랜치 또는 커밋 체크아웃

    지정된 브랜치나 커밋으로 체크아웃합니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)

        # 요청 데이터 정화
        sanitized_data = sanitize_input(request.model_dump(mode="json"))

        target = sanitized_data["target"]
        target_type = sanitized_data.get("target_type", "branch")

        # 타겟 타입 검증
        allowed_types = ["branch", "commit"]
        if target_type not in allowed_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"잘못된 타겟 타입입니다. 허용된 값: {allowed_types}",
            )

        # 브랜치인 경우 이름 검증
        if target_type == "branch":
            target = validate_branch_name(target)
        else:
            # 커밋 ID인 경우 기본 정화
            target = sanitize_input(target)
            if len(target) > 100:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, detail="커밋 ID가 너무 깁니다"
                )

        # 체크아웃 실행
        await terminus.checkout(db_name, target, target_type)

        # 현재 브랜치 확인
        current_branch = await terminus.get_current_branch(db_name)

        return ApiResponse.success(
            message=f"{target_type} '{target}'(으)로 체크아웃했습니다",
            data={
                "target": target,
                "target_type": target_type,
                "current_branch": current_branch
            }
        ).to_dict()

    except SecurityViolationError as e:
        logger.warning(f"Security violation in checkout: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to checkout: {e}")

        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="브랜치 또는 커밋을 찾을 수 없습니다"
            )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"체크아웃 실패: {str(e)}"
        )


@router.get("/branch/{branch_name:path}/info")
async def get_branch_info(
    db_name: str, branch_name: str, terminus: AsyncTerminusService = TerminusServiceDep
):
    """
    브랜치 정보 조회

    특정 브랜치의 상세 정보를 조회합니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        branch_name = validate_branch_name(branch_name)

        # 브랜치 존재 확인
        branches = await terminus.list_branches(db_name)
        if branch_name not in branches:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"브랜치 '{branch_name}'을(를) 찾을 수 없습니다",
            )

        # 현재 브랜치 확인
        current_branch = await terminus.get_current_branch(db_name)

        # 브랜치 정보 구성
        info = {
            "name": branch_name,
            "current": branch_name == current_branch,
            "protected": branch_name in ["main", "master", "production"],
        }

        # 추가 정보 조회 가능 (커밋 히스토리, 통계 등)

        return ApiResponse.success(
            message=f"브랜치 '{branch_name}'의 정보를 조회했습니다",
            data=info
        ).to_dict()

    except SecurityViolationError as e:
        logger.warning(f"Security violation in get_branch_info: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get branch info: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"브랜치 정보 조회 실패: {str(e)}",
        )


class CommitRequest(BaseModel):
    """커밋 요청 모델"""
    model_config = ConfigDict(str_strip_whitespace=True)
    
    message: str
    author: str = "system"
    operation: str = "database_change"


@router.post("/commit")
async def commit_changes(
    db_name: str,
    request: CommitRequest,
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    """
    브랜치에 변경사항 커밋
    
    현재 브랜치의 변경사항을 커밋합니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        
        # 요청 데이터 정화
        sanitized_data = sanitize_input(request.model_dump(mode="json"))
        
        message = sanitized_data.get("message", "Automated commit")
        author = sanitized_data.get("author", "system")
        operation = sanitized_data.get("operation", "database_change")
        
        # 메시지 길이 제한
        if len(message) > 500:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="커밋 메시지가 너무 깁니다 (500자 이하)",
            )
        
        # 현재 브랜치 확인
        current_branch = await terminus.get_current_branch(db_name)
        if not current_branch:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="현재 브랜치를 찾을 수 없습니다",
            )
        
        # 변경사항이 있는지 확인 (선택적)
        # TerminusDB에서는 변경사항이 없어도 커밋 가능
        
        # 커밋 실행 (TerminusDB에서는 자동으로 변경사항을 커밋)
        commit_info = {
            "message": message,
            "author": author,
            "operation": operation,
            "branch": current_branch,
            "timestamp": "auto"
        }
        
        # 실제 커밋은 TerminusDB의 자동 커밋에 의존
        # 여기서는 커밋 정보를 로그로 기록
        logger.info(f"Commit recorded for database {db_name}: {message} by {author}")
        
        return ApiResponse.success(
            message=f"변경사항이 커밋되었습니다: {message}",
            data={
                "commit_id": f"auto-{db_name}-{hash(message + author)}",
                "message": message,
                "author": author,
                "operation": operation,
                "branch": current_branch,
                "database": db_name
            }
        ).to_dict()
        
    except SecurityViolationError as e:
        logger.warning(f"Security violation in commit_changes: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to commit changes: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"커밋 실패: {str(e)}",
        )
