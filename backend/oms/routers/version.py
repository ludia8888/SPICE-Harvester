"""
버전 관리 라우터
커밋, 히스토리, 머지, 롤백 등을 담당
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict, Field

from oms.dependencies import TerminusServiceDep
from oms.services.async_terminus import AsyncTerminusService
from shared.dependencies.providers import AuditLogStoreDep
from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import (
    SecurityViolationError,
    sanitize_input,
    validate_branch_name,
    validate_db_name,
)
from shared.utils.commit_utils import coerce_commit_id
from shared.utils.diff_utils import normalize_diff_response
from shared.utils.branch_utils import protected_branch_write_message
from shared.config.settings import get_settings
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.observability.tracing import trace_endpoint

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/version/{db_name}", tags=["Version Control"])


class CommitRequest(BaseModel):
    """커밋 요청"""

    message: str
    author: Optional[str] = "admin"

    model_config = ConfigDict(
        json_schema_extra={"example": {"message": "Add new product ontology", "author": "admin"}}
    )


class MergeRequest(BaseModel):
    """머지 요청"""

    source_branch: str
    target_branch: Optional[str] = None  # None이면 현재 브랜치
    strategy: str = "auto"  # "auto", "ours", "theirs"

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "source_branch": "feature/new-ontology",
                "target_branch": "main",
                "strategy": "auto",
            }
        }
    )


class RollbackRequest(BaseModel):
    """롤백 요청"""

    # Accept both `target_commit` (new) and legacy `target` payloads.
    target_commit: str = Field(..., alias="target")  # 커밋 ID 또는 상대 참조 (예: HEAD~1)
    reason: Optional[str] = None

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={"example": {"target_commit": "HEAD~1", "reason": "Rollback to stable commit"}},
    )


_PROTECTED_BRANCHES = {"main", "master", "production", "prod"}


def _rollback_enabled() -> bool:
    """
    Rollback is effectively a "force-push/reset" of the ontology graph.

    운영 원칙(권장):
    - 온톨로지 스키마는 앞으로만 진화 (forward-only)
    - 과거 판단은 과거 의미 체계로 재현 (Versioning + Recompute)

    따라서 롤백은 기본적으로 비활성화하고(운영 차단),
    필요 시 명시적으로 ENABLE_OMS_ROLLBACK=true로 켤 수 있게만 둡니다.
    """
    return bool(get_settings().features.enable_oms_rollback)


@router.get("/head")
@trace_endpoint("oms.version.get_head")
async def get_branch_head_commit(
    db_name: str,
    branch: str = Query("main", description="브랜치 이름 (default: main)"),
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    """
    브랜치 HEAD 커밋 ID 조회

    운영 관점에서 "데이터/산출물 버전"과 "온톨로지 버전"의 정확 일치를
    배포 게이트로 사용하기 위한 최소 API.
    """
    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)

        raw_branches = await terminus.version_control_service.list_branches(db_name)
        if not isinstance(raw_branches, list):
            raw_branches = []
        for item in raw_branches:
            if not isinstance(item, dict):
                continue
            if str(item.get("name") or "").strip() != branch:
                continue
            head_commit_id = coerce_commit_id(item.get("head"))
            return ApiResponse.success(
                message="브랜치 head 커밋을 조회했습니다",
                data={"branch": branch, "head_commit_id": head_commit_id},
            ).to_dict()

        raise classified_http_exception(
            status.HTTP_404_NOT_FOUND,
            f"브랜치 '{branch}'을(를) 찾을 수 없습니다",
            code=ErrorCode.RESOURCE_NOT_FOUND,
        )
    except SecurityViolationError as e:
        logger.warning("Security violation in get_branch_head_commit: %s", e)
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get branch head commit: %s", e)
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"브랜치 head 커밋 조회 실패: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )


@router.post("/commit")
@trace_endpoint("oms.version.create_commit")
async def create_commit(
    db_name: str,
    request: CommitRequest,
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    """
    변경사항 커밋

    현재 브랜치의 변경사항을 커밋합니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)

        # 요청 데이터 정화
        sanitized_data = sanitize_input(request.model_dump(mode="json"))

        # 커밋 메시지 검증
        if not sanitized_data.get("message"):
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST, "커밋 메시지는 필수입니다",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

        # 커밋 생성
        commit_id = await terminus.commit(
            db_name, message=sanitized_data["message"], author=sanitized_data.get("author", "admin")
        )

        return ApiResponse.created(
            message="커밋이 생성되었습니다",
            data={
                "commit_id": commit_id,
                "author": request.author,
                "commit_message": request.message
            }
        ).to_dict()

    except SecurityViolationError as e:
        logger.warning(f"Security violation in create_commit: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except Exception as e:
        logger.error(f"Failed to create commit: {e}")

        if "no changes" in str(e).lower():
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST, "커밋할 변경사항이 없습니다",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR, f"커밋 생성 실패: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )


@router.get("/history")
@trace_endpoint("oms.version.get_history")
async def get_commit_history(
    db_name: str,
    branch: Optional[str] = Query(None, description="브랜치 이름"),
    limit: int = Query(10, description="조회할 커밋 수"),
    offset: int = Query(0, description="오프셋"),
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    """
    커밋 히스토리 조회

    브랜치의 커밋 히스토리를 조회합니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)

        # 브랜치 이름 검증 (지정된 경우)
        if branch:
            branch = validate_branch_name(branch)

        # 페이징 파라미터 검증
        if limit < 1 or limit > 1000:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST, "limit은 1-1000 범위여야 합니다",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )
        if offset < 0:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST, "offset은 0 이상이어야 합니다",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

        # 브랜치가 지정되지 않으면 현재 브랜치 사용
        if not branch:
            branch = await terminus.get_current_branch(db_name)

        # 히스토리 조회
        history = await terminus.get_commit_history(
            db_name, branch=branch, limit=limit, offset=offset
        )

        return ApiResponse.success(
            message="커밋 히스토리를 조회했습니다",
            data={
                "branch": branch,
                "commits": history,
                "total": len(history),
                "limit": limit,
                "offset": offset
            }
        ).to_dict()

    except SecurityViolationError as e:
        logger.warning(f"Security violation in get_commit_history: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get commit history: {e}")
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"커밋 히스토리 조회 실패: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )


@router.get("/diff")
@trace_endpoint("oms.version.get_diff")
async def get_diff(
    db_name: str,
    from_ref: str = Query(..., description="시작 참조 (브랜치 또는 커밋)"),
    to_ref: str = Query("HEAD", description="끝 참조 (브랜치 또는 커밋)"),
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    """
    차이점 조회

    두 참조 간의 차이점을 조회합니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        from_ref = sanitize_input(from_ref)
        to_ref = sanitize_input(to_ref)

        # 참조 길이 검증
        if len(from_ref) > 100 or len(to_ref) > 100:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST, "참조 이름이 너무 깁니다",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

        # 차이점 조회
        diff = await terminus.diff(db_name, from_ref, to_ref)

        normalized = normalize_diff_response(from_ref, to_ref, diff)

        return ApiResponse.success(
            message="차이점을 조회했습니다",
            data=normalized,
        ).to_dict()

    except SecurityViolationError as e:
        logger.warning(f"Security violation in get_diff: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get diff: {e}")

        if "not found" in str(e).lower():
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND, "참조를 찾을 수 없습니다",
                code=ErrorCode.RESOURCE_NOT_FOUND,
            )

        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR, f"차이점 조회 실패: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )


@router.post("/merge")
@trace_endpoint("oms.version.merge")
async def merge_branches(
    db_name: str,
    request: MergeRequest,
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    """
    브랜치 머지

    소스 브랜치를 대상 브랜치로 머지합니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)

        # 요청 데이터 정화
        sanitized_data = sanitize_input(request.model_dump(mode="json"))

        # 브랜치 이름 검증
        source_branch = validate_branch_name(sanitized_data["source_branch"])
        target_branch = sanitized_data.get("target_branch")
        if target_branch:
            target_branch = validate_branch_name(target_branch)

        # 대상 브랜치가 지정되지 않으면 현재 브랜치 사용
        if not target_branch:
            target_branch = await terminus.get_current_branch(db_name)

        # 같은 브랜치 머지 방지
        if source_branch == target_branch:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST, "소스와 대상 브랜치가 동일합니다",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

        # 머지 전략 검증
        allowed_strategies = ["auto", "ours", "theirs"]
        strategy = sanitized_data.get("strategy", "auto")
        if strategy not in allowed_strategies:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                f"잘못된 머지 전략입니다. 허용된 값: {allowed_strategies}",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

        # 머지 실행
        result = await terminus.merge(
            db_name, source_branch=source_branch, target_branch=target_branch, strategy=strategy
        )

        return ApiResponse.success(
            message=f"브랜치 '{source_branch}'을(를) '{target_branch}'(으)로 머지했습니다",
            data={
                "source_branch": source_branch,
                "target_branch": target_branch,
                "strategy": strategy,
                "conflicts": result.get("conflicts", []),
                "merged": result.get("merged", True)
            }
        ).to_dict()

    except SecurityViolationError as e:
        logger.warning(f"Security violation in merge_branches: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to merge branches: {e}")

        if "conflict" in str(e).lower():
            raise classified_http_exception(
                status.HTTP_409_CONFLICT,
                "머지 충돌이 발생했습니다. 수동으로 해결해야 합니다",
                code=ErrorCode.CONFLICT,
            )

        if "not found" in str(e).lower():
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND, "브랜치를 찾을 수 없습니다",
                code=ErrorCode.RESOURCE_NOT_FOUND,
            )

        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR, f"브랜치 머지 실패: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )


@router.post("/rollback")
@trace_endpoint("oms.version.rollback")
async def rollback(
    db_name: str,
    request: RollbackRequest,
    audit_store: AuditLogStoreDep,
    branch: Optional[str] = Query(
        None,
        description="Target branch to reset (default: current branch). Protected branches are blocked.",
    ),
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    """
    변경사항 롤백

    지정된 커밋으로 롤백합니다.
    """
    try:
        if not _rollback_enabled():
            raise classified_http_exception(
                status.HTTP_403_FORBIDDEN,
                "Rollback endpoint is disabled by default (set ENABLE_OMS_ROLLBACK=true to enable in non-prod)",
                code=ErrorCode.PERMISSION_DENIED,
            )

        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)

        # 요청 데이터 정화
        sanitized_data = sanitize_input(request.model_dump(mode="json"))
        target = sanitized_data["target_commit"]

        target_branch = branch
        if target_branch:
            target_branch = validate_branch_name(target_branch)
        else:
            target_branch = await terminus.get_current_branch(db_name)
            target_branch = validate_branch_name(target_branch)

        # Protected branch safety: never allow reset/force-push in production-like branches.
        if target_branch in _PROTECTED_BRANCHES:
            try:
                await audit_store.log(
                    partition_key=f"db:{db_name}",
                    actor="oms",
                    action="VERSION_ROLLBACK_BLOCKED",
                    status="failure",
                    resource_type="terminus_branch",
                    resource_id=f"terminus:{db_name}:{target_branch}",
                    metadata={
                        "db_name": db_name,
                        "branch": target_branch,
                        "target": target,
                        "target_commit": target,
                        "protected": True,
                        "reason": "protected_branch",
                    },
                )
            except Exception:
                # Blocking a dangerous operation should still work even if audit is degraded.
                logging.getLogger(__name__).warning("Broad exception fallback at oms/routers/version.py:492", exc_info=True)
                pass

            raise classified_http_exception(
                status.HTTP_403_FORBIDDEN,
                protected_branch_write_message(),
                code=ErrorCode.PERMISSION_DENIED,
            )

        # 타겟 검증
        if len(target) > 100:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST, "롤백 타겟이 너무 깁니다",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

        # Audit is mandatory for rollback attempts (fail-closed when enabled).
        try:
            await audit_store.log(
                partition_key=f"db:{db_name}",
                actor="oms",
                action="VERSION_ROLLBACK_REQUESTED",
                status="success",
                resource_type="terminus_branch",
                resource_id=f"terminus:{db_name}:{target_branch}",
                metadata={
                    "db_name": db_name,
                    "branch": target_branch,
                    "target": target,
                    "target_commit": target,
                    "reason": getattr(request, "reason", None),
                },
            )
        except Exception as e:
            raise classified_http_exception(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                f"Rollback requires audit logging, but audit store is unavailable: {e}",
                code=ErrorCode.UPSTREAM_UNAVAILABLE,
            ) from e

        # 롤백 실행 (branch reset)
        await terminus.version_control_service.reset_branch(
            db_name, branch_name=target_branch, commit_id=target
        )

        try:
            await audit_store.log(
                partition_key=f"db:{db_name}",
                actor="oms",
                action="VERSION_ROLLBACK_APPLIED",
                status="success",
                resource_type="terminus_branch",
                resource_id=f"terminus:{db_name}:{target_branch}",
                metadata={
                    "db_name": db_name,
                    "branch": target_branch,
                    "target": target,
                    "target_commit": target,
                },
            )
        except Exception:
            # Best-effort; request log is already written.
            logging.getLogger(__name__).warning("Broad exception fallback at oms/routers/version.py:553", exc_info=True)
            pass

        return ApiResponse.success(
            message=f"'{target}'(으)로 롤백했습니다",
            data={
                "target": target,
                "target_commit": target,
                "branch": target_branch,
                "current_branch": await terminus.get_current_branch(db_name),
            }
        ).to_dict()

    except SecurityViolationError as e:
        logger.warning(f"Security violation in rollback: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to rollback: {e}")

        try:
            await audit_store.log(
                partition_key=f"db:{db_name}",
                actor="oms",
                action="VERSION_ROLLBACK_FAILED",
                status="failure",
                resource_type="terminus_branch",
                resource_id=f"terminus:{db_name}:{branch or 'current'}",
                metadata={
                    "db_name": db_name,
                    "branch": branch,
                    "target": getattr(request, "target_commit", None),
                    "target_commit": getattr(request, "target_commit", None),
                    "reason": getattr(request, "reason", None),
                },
                error=str(e),
            )
        except Exception as exc:
            logger.warning(
                "Audit log failed: VERSION_ROLLBACK_FAILED (db=%s branch=%s): %s",
                db_name,
                branch,
                exc,
                exc_info=True,
            )

        if "not found" in str(e).lower():
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND, f"대상을 찾을 수 없습니다",
                code=ErrorCode.RESOURCE_NOT_FOUND,
            )

        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR, f"롤백 실패: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )


@router.post("/rebase")
@trace_endpoint("oms.version.rebase")
async def rebase_branch(
    db_name: str,
    onto: str = Query(..., description="리베이스 대상 브랜치"),
    branch: Optional[str] = Query(None, description="리베이스할 브랜치 (기본: 현재 브랜치)"),
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    """
    브랜치 리베이스

    브랜치를 다른 브랜치 위로 리베이스합니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        onto = validate_branch_name(onto)

        if branch:
            branch = validate_branch_name(branch)

        # 브랜치가 지정되지 않으면 현재 브랜치 사용
        if not branch:
            branch = await terminus.get_current_branch(db_name)
            branch = validate_branch_name(branch)

        # 같은 브랜치 리베이스 방지
        if branch == onto:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "리베이스 대상과 브랜치가 동일합니다",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

        # 리베이스 실행
        await terminus.rebase(db_name, onto=onto, branch=branch)

        return ApiResponse.success(
            message=f"브랜치 '{branch}'을(를) '{onto}' 위로 리베이스했습니다",
            data={
                "branch": branch,
                "onto": onto,
                "success": True
            }
        ).to_dict()

    except SecurityViolationError as e:
        logger.warning(f"Security violation in rebase_branch: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to rebase: {e}")

        if "conflict" in str(e).lower():
            raise classified_http_exception(
                status.HTTP_409_CONFLICT, "리베이스 충돌이 발생했습니다",
                code=ErrorCode.CONFLICT,
            )

        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR, f"리베이스 실패: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )


@router.get("/common-ancestor")
@trace_endpoint("oms.version.get_common_ancestor")
async def get_common_ancestor(
    db_name: str,
    branch1: str = Query(..., description="첫 번째 브랜치"),
    branch2: str = Query(..., description="두 번째 브랜치"),
    terminus: AsyncTerminusService = TerminusServiceDep,
):
    """
    두 브랜치의 공통 조상 찾기
    
    Three-way merge를 위한 공통 조상 커밋을 찾습니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        branch1 = validate_branch_name(branch1)
        branch2 = validate_branch_name(branch2)
        
        # 공통 조상 찾기
        common_ancestor = await terminus.find_common_ancestor(db_name, branch1, branch2)
        
        if not common_ancestor:
            return ApiResponse.success(
                message=f"브랜치 '{branch1}'과 '{branch2}'의 공통 조상을 찾을 수 없습니다",
                data={
                    "common_ancestor": None
                }
            ).to_dict()
        
        # 공통 조상 커밋 정보 가져오기
        history = await terminus.get_commit_history(db_name, limit=100)
        ancestor_info = None
        for commit in history:
            if commit.get("identifier") == common_ancestor:
                ancestor_info = commit
                break
        
        return ApiResponse.success(
            message=f"공통 조상 커밋을 찾았습니다: {common_ancestor[:8]}",
            data={
                "common_ancestor": common_ancestor,
                "ancestor_info": ancestor_info,
                "branch1": branch1,
                "branch2": branch2
            }
        ).to_dict()
        
    except SecurityViolationError as e:
        logger.warning(f"Security violation in get_common_ancestor: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to find common ancestor: {e}")
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"공통 조상 찾기 실패: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )
