"""Merge conflict service (BFF).

Extracted from `bff.routers.merge_conflict` to keep routers thin and to
centralize merge simulation + conflict resolution flows (Facade pattern).
"""

from __future__ import annotations

import inspect
import logging
from typing import Any, Dict, List, Optional

import httpx
from fastapi import HTTPException, status

from bff.dependencies import OMSClient
from bff.utils.conflict_converter import ConflictConverter
from shared.models.requests import ApiResponse, MergeRequest
from shared.security.input_sanitizer import SecurityViolationError, validate_branch_name, validate_db_name
from shared.utils.diff_utils import normalize_diff_changes

logger = logging.getLogger(__name__)

_SECURITY_VIOLATION_DETAIL = "입력 데이터에 보안 위반이 감지되었습니다"


async def _await_if_needed(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _raise_for_status(response: Any) -> None:
    await _await_if_needed(response.raise_for_status())


async def _response_json(response: Any) -> Any:
    return await _await_if_needed(response.json())


def _validate_inputs(*, db_name: str, source_branch: str, target_branch: str) -> tuple[str, str, str]:
    db_name = validate_db_name(db_name)
    source_branch = validate_branch_name(source_branch)
    target_branch = validate_branch_name(target_branch)
    return db_name, source_branch, target_branch


async def simulate_merge(*, db_name: str, request: MergeRequest, oms_client: OMSClient) -> ApiResponse:
    """
    Simulate a merge (no write) and return UI-friendly conflict format.
    """
    try:
        db_name, source_branch, target_branch = _validate_inputs(
            db_name=db_name,
            source_branch=request.source_branch,
            target_branch=request.target_branch,
        )
        logger.info("Starting merge simulation: %s -> %s in %s", source_branch, target_branch, db_name)

        try:
            source_info = await oms_client.client.get(f"/api/v1/branch/{db_name}/branch/{source_branch}/info")
            await _raise_for_status(source_info)

            target_info = await oms_client.client.get(f"/api/v1/branch/{db_name}/branch/{target_branch}/info")
            await _raise_for_status(target_info)
        except httpx.HTTPStatusError as exc:
            if getattr(exc, "response", None) is not None and exc.response.status_code == 404:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="브랜치를 찾을 수 없습니다",
                ) from exc
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="브랜치 조회 실패") from exc
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="브랜치 조회 실패") from exc
        except Exception as exc:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="브랜치 조회 실패") from exc

        try:
            diff_response = await oms_client.client.get(
                f"/api/v1/version/{db_name}/diff",
                params={"from_ref": target_branch, "to_ref": source_branch},
            )
            await _raise_for_status(diff_response)
            diff_data = await _response_json(diff_response)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"차이 분석 실패: {exc}",
            ) from exc

        try:
            reverse_diff_response = await oms_client.client.get(
                f"/api/v1/version/{db_name}/diff",
                params={"from_ref": source_branch, "to_ref": target_branch},
            )
            await _raise_for_status(reverse_diff_response)
            reverse_diff_data = await _response_json(reverse_diff_response)
        except Exception as exc:
            logger.warning("역방향 diff 분석 실패: %s", exc)
            reverse_diff_data = {"data": {"changes": []}}

        source_changes_raw = (diff_data.get("data", {}) or {}).get("changes", [])
        source_changes = normalize_diff_changes(source_changes_raw)
        target_changes_raw = (reverse_diff_data.get("data", {}) or {}).get("changes", [])
        target_changes = normalize_diff_changes(target_changes_raw)

        common_ancestor = None
        try:
            ancestor_response = await oms_client.client.get(
                f"/api/v1/version/{db_name}/common-ancestor",
                params={"branch1": source_branch, "branch2": target_branch},
            )
            if ancestor_response.status_code == 200:
                ancestor_data = await _response_json(ancestor_response)
                common_ancestor = ancestor_data.get("data", {}).get("common_ancestor")
                logger.info("Found common ancestor: %s", common_ancestor)
        except Exception as exc:
            logger.warning("공통 조상 찾기 실패: %s", exc)

        conflicts = await _detect_merge_conflicts(
            source_changes,
            target_changes,
            common_ancestor=common_ancestor,
            db_name=db_name,
            oms_client=oms_client,
        )

        converter = ConflictConverter()
        ui_conflicts = await converter.convert_conflicts_to_ui_format(conflicts, db_name, source_branch, target_branch)

        merge_stats = {
            "changes_to_apply": len(source_changes),
            "conflicts_detected": len(ui_conflicts),
            "mergeable": len(ui_conflicts) == 0,
            "requires_manual_resolution": len(ui_conflicts) > 0,
        }
        logger.info("Merge simulation completed: %s", merge_stats)

        return ApiResponse(
            status="success",
            message=f"병합 시뮬레이션 완료: {merge_stats['conflicts_detected']}개 충돌 감지",
            data={
                "merge_preview": {
                    "source_branch": source_branch,
                    "target_branch": target_branch,
                    "strategy": request.strategy.value if hasattr(request.strategy, "value") else str(request.strategy),
                    "conflicts": ui_conflicts,
                    "changes": source_changes,
                    "statistics": merge_stats,
                }
            },
        )

    except SecurityViolationError as exc:
        logger.warning("Security violation in simulate_merge: %s", exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=_SECURITY_VIOLATION_DETAIL) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Merge simulation failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"병합 시뮬레이션 실패: {exc}",
        ) from exc


async def resolve_merge_conflicts(*, db_name: str, request: Dict[str, Any], oms_client: OMSClient) -> ApiResponse:
    """
    Resolve merge conflicts using user-provided resolutions, then perform merge.
    """
    try:
        db_name = validate_db_name(db_name)
        source_branch = validate_branch_name(request.get("source_branch"))
        target_branch = validate_branch_name(request.get("target_branch"))
        resolutions = request.get("resolutions", [])

        logger.info("Starting manual merge resolution: %s -> %s", source_branch, target_branch)

        if not resolutions:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="충돌 해결책이 제공되지 않았습니다",
            )

        terminus_resolutions = []
        for resolution in resolutions:
            terminus_resolutions.append(await _convert_resolution_to_terminus_format(resolution))

        merge_data = {
            "source": source_branch,
            "target": target_branch,
            "strategy": request.get("strategy", "merge"),
            "message": request.get("message", f"Resolve conflicts: {source_branch} -> {target_branch}"),
            "author": request.get("author"),
            "conflict_resolutions": terminus_resolutions,
        }

        try:
            merge_response = await oms_client.client.post(f"/api/v1/version/{db_name}/merge", json=merge_data)
            await _raise_for_status(merge_response)
            merge_result = await _response_json(merge_response)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"병합 실행 실패: {exc}",
            ) from exc

        logger.info("Manual merge resolution completed successfully")
        return ApiResponse(
            status="success",
            message="충돌이 해결되고 병합이 완료되었습니다",
            data={
                "merge_result": merge_result.get("data", {}),
                "resolved_conflicts": len(resolutions),
            },
        )

    except SecurityViolationError as exc:
        logger.warning("Security violation in resolve_merge_conflicts: %s", exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=_SECURITY_VIOLATION_DETAIL) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Manual merge resolution failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"충돌 해결 실패: {exc}",
        ) from exc


async def _detect_merge_conflicts(
    source_changes: List[Dict[str, Any]],
    target_changes: List[Dict[str, Any]],
    common_ancestor: Optional[str] = None,
    db_name: Optional[str] = None,
    oms_client: Optional[OMSClient] = None,
) -> List[Dict[str, Any]]:
    conflicts = []

    source_paths = {change.get("path", change.get("id", "unknown")): change for change in source_changes}
    target_paths = {change.get("path", change.get("id", "unknown")): change for change in target_changes}

    async def get_ancestor_value(path: str) -> Optional[Any]:
        if not common_ancestor or not db_name or not oms_client:
            return None

        try:
            response = await oms_client.client.get(
                f"/api/v1/version/{db_name}/checkout",
                params={"commit": common_ancestor},
            )
            if response.status_code == 200:
                return {"commit": common_ancestor, "value": None}
        except Exception as exc:
            logger.warning("Failed to get ancestor value for %s: %s", path, exc)
        return None

    for path in source_paths:
        if path not in target_paths:
            continue
        source_change = source_paths[path]
        target_change = target_paths[path]

        if source_change.get("new_value") == target_change.get("new_value"):
            continue

        ancestor_info = await get_ancestor_value(path) if common_ancestor else None
        conflict_type = "content_conflict"
        auto_resolvable = False
        suggested_resolution = None

        if ancestor_info and common_ancestor:
            ancestor_value = ancestor_info.get("value")
            source_value = source_change.get("new_value")
            target_value = target_change.get("new_value")

            if ancestor_value == source_value:
                auto_resolvable = True
                suggested_resolution = "target"
                conflict_type = "modify_no_conflict"
            elif ancestor_value == target_value:
                auto_resolvable = True
                suggested_resolution = "source"
                conflict_type = "modify_no_conflict"
            else:
                conflict_type = "modify_modify_conflict"

        conflicts.append(
            {
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
        )

    return conflicts


async def _convert_resolution_to_terminus_format(resolution: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "path": resolution.get("path"),
        "resolution_type": resolution.get("resolution_type", "use_value"),
        "value": resolution.get("resolved_value"),
        "metadata": resolution.get("metadata", {}),
    }

