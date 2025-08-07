"""
BFF Merge Conflict Router
Foundry-style 병합 충돌 해결 API
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status

# BFF dependencies import
from bff.dependencies import OMSClient, get_oms_client
from shared.models.common import BaseResponse

# shared 모델 import
from shared.models.requests import ApiResponse, MergeRequest

# 보안 모듈 import - ENABLED FOR SECURITY
from shared.security.input_sanitizer import (
    SecurityViolationError,
    validate_branch_name,
    validate_db_name,
)

# 충돌 변환기 import
from bff.utils.conflict_converter import ConflictConverter


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/database/{db_name}/merge", tags=["Merge Conflict Resolution"])


@router.post("/simulate", response_model=ApiResponse)
async def simulate_merge(
    db_name: str, request: MergeRequest, oms_client: OMSClient = Depends(get_oms_client)
) -> ApiResponse:
    """
    병합 시뮬레이션 - 실제 병합 없이 충돌 감지

    TerminusDB의 diff API를 사용하여 충돌 가능성을 미리 확인합니다.
    Foundry-style 충돌 감지 및 미리보기 기능을 제공합니다.

    Args:
        db_name: 데이터베이스 이름
        request: 병합 요청 정보

    Returns:
        충돌 정보와 병합 미리보기가 포함된 응답
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        source_branch = validate_branch_name(request.source_branch)
        target_branch = validate_branch_name(request.target_branch)

        logger.info(f"Starting merge simulation: {source_branch} -> {target_branch} in {db_name}")

        # 1. 브랜치 존재 여부 확인
        try:
            source_info = await oms_client.client.get(
                f"/api/v1/branch/{db_name}/branch/{source_branch}/info"
            )
            source_info.raise_for_status()

            target_info = await oms_client.client.get(
                f"/api/v1/branch/{db_name}/branch/{target_branch}/info"
            )
            target_info.raise_for_status()

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"브랜치를 찾을 수 없습니다: {e}"
            )

        # 2. TerminusDB diff API를 사용하여 변경사항 분석
        try:
            diff_response = await oms_client.client.get(
                f"/api/v1/version/{db_name}/diff",
                params={"from_ref": target_branch, "to_ref": source_branch},
            )
            diff_response.raise_for_status()
            diff_data = diff_response.json()  # httpx response.json() is sync, not async!

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"차이 분석 실패: {e}"
            )

        # 3. 양방향 diff로 충돌 가능성 감지
        try:
            reverse_diff_response = await oms_client.client.get(
                f"/api/v1/version/{db_name}/diff",
                params={"from_ref": source_branch, "to_ref": target_branch},
            )
            reverse_diff_response.raise_for_status()
            reverse_diff_data = reverse_diff_response.json()  # httpx response.json() is sync!

        except Exception as e:
            logger.warning(f"역방향 diff 분석 실패: {e}")
            reverse_diff_data = {"data": {"changes": []}}

        # 4. 공통 조상 찾기 (Three-way merge를 위한)
        common_ancestor = None
        try:
            # OMS 서비스로 직접 API 호출하여 공통 조상 찾기
            ancestor_response = await oms_client.client.get(
                f"/api/v1/version/{db_name}/common-ancestor",
                params={"branch1": source_branch, "branch2": target_branch},
            )
            if ancestor_response.status_code == 200:
                ancestor_data = ancestor_response.json()  # httpx response.json() is sync!
                common_ancestor = ancestor_data.get("data", {}).get("common_ancestor")
                logger.info(f"Found common ancestor: {common_ancestor}")
        except Exception as e:
            logger.warning(f"공통 조상 찾기 실패: {e}")

        # 5. 충돌 감지 엔진 실행 (공통 조상 정보 포함)
        conflicts = await _detect_merge_conflicts(
            diff_data.get("data", {}).get("changes", []),
            reverse_diff_data.get("data", {}).get("changes", []),
            common_ancestor=common_ancestor,
            db_name=db_name,
            oms_client=oms_client,
        )

        # 6. Foundry 스타일 충돌 형식으로 변환
        converter = ConflictConverter()
        foundry_conflicts = await converter.convert_conflicts_to_foundry_format(
            conflicts, db_name, source_branch, target_branch
        )

        # 7. 병합 통계 계산
        source_changes = diff_data.get("data", {}).get("changes", [])
        merge_stats = {
            "changes_to_apply": len(source_changes),
            "conflicts_detected": len(foundry_conflicts),
            "mergeable": len(foundry_conflicts) == 0,
            "requires_manual_resolution": len(foundry_conflicts) > 0,
        }

        logger.info(f"Merge simulation completed: {merge_stats}")

        return ApiResponse(
            status="success",
            message=f"병합 시뮬레이션 완료: {merge_stats['conflicts_detected']}개 충돌 감지",
            data={
                "merge_preview": {
                    "source_branch": source_branch,
                    "target_branch": target_branch,
                    "strategy": (
                        request.strategy.value
                        if hasattr(request.strategy, "value")
                        else str(request.strategy)
                    ),
                    "conflicts": foundry_conflicts,
                    "changes": source_changes,
                    "statistics": merge_stats,
                }
            },
        )

    except SecurityViolationError as e:
        logger.warning(f"Security violation in simulate_merge: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Merge simulation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"병합 시뮬레이션 실패: {e}"
        )


@router.post("/resolve", response_model=ApiResponse)
async def resolve_merge_conflicts(
    db_name: str, request: Dict[str, Any], oms_client: OMSClient = Depends(get_oms_client)
) -> ApiResponse:
    """
    수동 병합 충돌 해결

    사용자가 제공한 해결책으로 충돌을 해결하고 실제 병합을 수행합니다.

    Args:
        db_name: 데이터베이스 이름
        request: 충돌 해결 정보

    Returns:
        병합 결과
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)

        source_branch = validate_branch_name(request.get("source_branch"))
        target_branch = validate_branch_name(request.get("target_branch"))
        resolutions = request.get("resolutions", [])

        logger.info(f"Starting manual merge resolution: {source_branch} -> {target_branch}")

        # 1. 해결책 검증
        if not resolutions:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="충돌 해결책이 제공되지 않았습니다"
            )

        # 2. 각 해결책을 TerminusDB 형식으로 변환
        terminus_resolutions = []
        for resolution in resolutions:
            terminus_resolution = await _convert_resolution_to_terminus_format(resolution)
            terminus_resolutions.append(terminus_resolution)

        # 3. TerminusDB merge API 호출 (해결책 포함)
        merge_data = {
            "source": source_branch,
            "target": target_branch,
            "strategy": request.get("strategy", "merge"),
            "message": request.get(
                "message", f"Resolve conflicts: {source_branch} -> {target_branch}"
            ),
            "author": request.get("author"),
            "conflict_resolutions": terminus_resolutions,
        }

        try:
            merge_response = await oms_client.client.post(
                f"/api/v1/version/{db_name}/merge", json=merge_data
            )
            merge_response.raise_for_status()
            merge_result = merge_response.json()  # httpx response.json() is sync!

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"병합 실행 실패: {e}"
            )

        logger.info(f"Manual merge resolution completed successfully")

        return ApiResponse(
            status="success",
            message="충돌이 해결되고 병합이 완료되었습니다",
            data={
                "merge_result": merge_result.get("data", {}),
                "resolved_conflicts": len(resolutions),
            },
        )

    except SecurityViolationError as e:
        logger.warning(f"Security violation in resolve_merge_conflicts: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Manual merge resolution failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"충돌 해결 실패: {e}"
        )


async def _detect_merge_conflicts(
    source_changes: List[Dict[str, Any]], 
    target_changes: List[Dict[str, Any]],
    common_ancestor: Optional[str] = None,
    db_name: Optional[str] = None,
    oms_client: Optional[OMSClient] = None,
) -> List[Dict[str, Any]]:
    """
    3-way 병합 충돌 감지 엔진 (공통 조상 기반)

    Args:
        source_changes: 소스 브랜치의 변경사항
        target_changes: 대상 브랜치의 변경사항
        common_ancestor: 공통 조상 커밋 ID
        db_name: 데이터베이스 이름
        oms_client: OMS 클라이언트

    Returns:
        감지된 충돌 목록
    """
    conflicts = []

    # 경로별로 변경사항 그룹화
    source_paths = {
        change.get("path", change.get("id", "unknown")): change for change in source_changes
    }
    target_paths = {
        change.get("path", change.get("id", "unknown")): change for change in target_changes
    }

    # 공통 조상에서의 값을 가져오는 함수
    async def get_ancestor_value(path: str) -> Optional[Any]:
        if not common_ancestor or not db_name or not oms_client:
            return None
        
        try:
            # 공통 조상 시점의 데이터 조회
            response = await oms_client.client.get(
                f"/api/v1/version/{db_name}/checkout",
                params={"commit": common_ancestor},
            )
            if response.status_code == 200:
                # 해당 경로의 값 추출 (간단한 구현)
                # 실제로는 더 복잡한 경로 탐색이 필요할 수 있음
                return {"commit": common_ancestor, "value": None}
        except Exception as e:
            logger.warning(f"Failed to get ancestor value for {path}: {e}")
        return None

    # 동일한 경로에서 서로 다른 변경이 있는 경우 충돌로 감지
    for path in source_paths:
        if path in target_paths:
            source_change = source_paths[path]
            target_change = target_paths[path]

            # 동일한 값으로의 변경은 충돌이 아님
            if source_change.get("new_value") != target_change.get("new_value"):
                # 공통 조상에서의 값 가져오기
                ancestor_info = await get_ancestor_value(path) if common_ancestor else None
                
                # Three-way merge 충돌 분석
                conflict_type = "content_conflict"
                auto_resolvable = False
                suggested_resolution = None
                
                if ancestor_info and common_ancestor:
                    # 3-way merge 로직
                    ancestor_value = ancestor_info.get("value")
                    source_value = source_change.get("new_value")
                    target_value = target_change.get("new_value")
                    
                    # 자동 해결 가능한 경우 판단
                    if ancestor_value == source_value:
                        # Source는 변경이 없고 Target만 변경됨 - Target 선택
                        auto_resolvable = True
                        suggested_resolution = "target"
                        conflict_type = "modify_no_conflict"
                    elif ancestor_value == target_value:
                        # Target은 변경이 없고 Source만 변경됨 - Source 선택
                        auto_resolvable = True
                        suggested_resolution = "source"
                        conflict_type = "modify_no_conflict"
                    else:
                        # 양쪽 모두 다르게 변경됨 - 진짜 충돌
                        conflict_type = "modify_modify_conflict"
                
                conflict = {
                    "path": path,
                    "type": conflict_type,
                    "source_change": source_change,
                    "target_change": target_change,
                    "common_ancestor": common_ancestor,
                    "ancestor_value": ancestor_info,
                    "is_three_way": bool(common_ancestor),
                    "auto_resolvable": auto_resolvable,
                    "suggested_resolution": suggested_resolution,
                }
                conflicts.append(conflict)

    return conflicts


async def _convert_resolution_to_terminus_format(resolution: Dict[str, Any]) -> Dict[str, Any]:
    """
    Foundry 해결책을 TerminusDB 형식으로 변환

    Args:
        resolution: Foundry 스타일 해결책

    Returns:
        TerminusDB 형식 해결책
    """
    return {
        "path": resolution.get("path"),
        "resolution_type": resolution.get("resolution_type", "use_value"),
        "value": resolution.get("resolved_value"),
        "metadata": resolution.get("metadata", {}),
    }
