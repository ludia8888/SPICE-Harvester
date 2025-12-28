"""
Version Control Service for TerminusDB
브랜치, 커밋, 머지 등 버전 관리 기능
"""

import asyncio
import logging
import os
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from shared.utils.terminus_branch import decode_branch_name, encode_branch_name

from .base import BaseTerminusService
from .database import DatabaseService
from oms.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class VersionControlService(BaseTerminusService):
    """
    TerminusDB 버전 관리 서비스
    
    브랜치 생성/삭제, 커밋, 머지, 이력 조회 등의 기능을 제공합니다.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # DatabaseService 인스턴스 (데이터베이스 존재 확인용)
        self.db_service = DatabaseService(*args, **kwargs)

    async def disconnect(self) -> None:
        """
        Close nested services before closing this client's HTTP resources.

        Why:
        - VersionControlService internally instantiates a DatabaseService to run `ensure_db_exists`.
        - If callers only disconnect the outer service, the inner DatabaseService's httpx client
          remains open and triggers `ResourceWarning: unclosed transport` in tests and short-lived
          processes.
        """
        try:
            await self.db_service.disconnect()
        finally:
            await super().disconnect()

    _encode_branch_name = staticmethod(encode_branch_name)
    _decode_branch_name = staticmethod(decode_branch_name)

    @staticmethod
    def _is_origin_branch_missing_error(exc: Exception) -> bool:
        """
        TerminusDB can transiently report `OriginBranchDoesNotExist` right after DB creation.
        The branch (usually `main`) becomes available shortly after the initial commit is created.
        """
        message = str(exc)
        return "OriginBranchDoesNotExist" in message or "api:OriginBranchDoesNotExist" in message

    @staticmethod
    def _is_branch_already_exists_error(exc: Exception) -> bool:
        message = str(exc)
        return (
            "BranchAlreadyExists" in message
            or "api:BranchAlreadyExists" in message
            or "already exists" in message.lower()
        )

    @staticmethod
    def _is_transient_request_handler_error(exc: Exception) -> bool:
        """
        TerminusDB occasionally returns a 500 "Unexpected failure in request handler" right after DB creation.

        In practice this is a race in Terminus' internal initialization and usually resolves quickly.
        We treat it as transient to avoid leaking 5xx responses to callers on fresh DBs.
        """
        message = str(exc)
        return (
            "Unexpected failure in request handler" in message
            or "TerminusDB API error: 500" in message
            or "TerminusDB API error: 503" in message
        )
    
    async def list_branches(self, db_name: str) -> List[Dict[str, Any]]:
        """
        데이터베이스의 모든 브랜치 목록 조회
        
        TerminusDB v12 no longer supports `GET /api/branch/...` (405).
        Instead, branch refs are available via the commits stream endpoint:
        `GET /api/document/{account}/{db}/local/_commits?type=Branch`
        
        Args:
            db_name: 데이터베이스 이름
            
        Returns:
            브랜치 목록
        """
        try:
            await self.db_service.ensure_db_exists(db_name)
            
            endpoint = f"/api/document/{self.connection_info.account}/{db_name}/local/_commits"
            result = await self._make_request("GET", endpoint, params={"type": "Branch"})

            raw_items: List[Dict[str, Any]] = []
            if isinstance(result, dict):
                raw_items = [result] if result else []
            elif isinstance(result, list):
                raw_items = [i for i in result if isinstance(i, dict)]

            branches: List[Dict[str, Any]] = []
            for item in raw_items:
                name = item.get("name")
                if not name and isinstance(item.get("@id"), str):
                    # Example: "@id": "Branch/main"
                    name = item["@id"].split("/", 1)[-1]
                if not name:
                    continue
                name = self._decode_branch_name(str(name))
                branches.append(
                    {
                        "name": name,
                        "head": item.get("head"),
                        "created": item.get("created") or item.get("timestamp"),
                    }
                )
            return branches
            
        except Exception as e:
            logger.error(f"Failed to list branches: {e}")
            raise DatabaseError(f"브랜치 목록 조회 실패: {e}")
    
    async def create_branch(
        self,
        db_name: str,
        branch_name: str,
        source_branch: str = "main",
        empty: bool = False
    ) -> Dict[str, Any]:
        """
        새 브랜치 생성
        
        Args:
            db_name: 데이터베이스 이름
            branch_name: 새 브랜치 이름
            source_branch: 소스 브랜치 (기본값: main)
            empty: 빈 브랜치 생성 여부
            
        Returns:
            생성된 브랜치 정보
        """
        try:
            await self.db_service.ensure_db_exists(db_name)
            
            # 브랜치 존재 확인
            branches = await self.list_branches(db_name)
            if any(b["name"] == branch_name for b in branches):
                raise DatabaseError(f"브랜치 '{branch_name}'이(가) 이미 존재합니다")

            target_branch = self._encode_branch_name(branch_name)
            source_branch_encoded = self._encode_branch_name(source_branch)

            endpoint = f"/api/branch/{self.connection_info.account}/{db_name}/local/branch/{target_branch}"
            
            data = {
                "origin": f"{self.connection_info.account}/{db_name}/local/branch/{source_branch_encoded}",
            }
            
            if empty:
                data["empty"] = True

            # TerminusDB can briefly return OriginBranchDoesNotExist (HTTP 400) right after DB creation.
            # Retry with a small backoff so callers (e.g. OMS smoke tests, UI flows) don't see flaky 500s.
            max_attempts = 8
            base_delay_seconds = 0.15
            for attempt in range(max_attempts):
                try:
                    await self._make_request("POST", endpoint, data)
                    break
                except Exception as e:
                    if self._is_branch_already_exists_error(e):
                        break
                    if not self._is_origin_branch_missing_error(e) or attempt == max_attempts - 1:
                        raise
                    delay = base_delay_seconds * (attempt + 1)
                    logger.warning(
                        "Origin branch '%s' not ready for db '%s' (attempt %s/%s); retrying in %.2fs",
                        source_branch,
                        db_name,
                        attempt + 1,
                        max_attempts,
                        delay,
                    )
                    await asyncio.sleep(delay)
            
            logger.info(f"Branch '{branch_name}' created in database '{db_name}'")
            
            return {
                "name": branch_name,
                "source": source_branch,
                "created_at": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to create branch: {e}")
            raise DatabaseError(f"브랜치 생성 실패: {e}")
    
    async def delete_branch(
        self,
        db_name: str,
        branch_name: str
    ) -> bool:
        """
        브랜치 삭제
        
        Args:
            db_name: 데이터베이스 이름
            branch_name: 삭제할 브랜치 이름
            
        Returns:
            삭제 성공 여부
        """
        try:
            # main 브랜치는 삭제 불가
            if branch_name == "main":
                raise DatabaseError("main 브랜치는 삭제할 수 없습니다")
            
            # 브랜치 존재 확인
            branches = await self.list_branches(db_name)
            if not any(b["name"] == branch_name for b in branches):
                raise DatabaseError(f"브랜치 '{branch_name}'을(를) 찾을 수 없습니다")

            encoded_branch = self._encode_branch_name(branch_name)
            endpoint = f"/api/branch/{self.connection_info.account}/{db_name}/local/branch/{encoded_branch}"
            await self._make_request("DELETE", endpoint)
            
            logger.info(f"Branch '{branch_name}' deleted from database '{db_name}'")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete branch: {e}")
            raise DatabaseError(f"브랜치 삭제 실패: {e}")

    async def checkout_branch(self, db_name: str, branch_name: str) -> bool:
        """
        TerminusDB는 stateless HTTP API이므로 'checkout'은 서버 상태를 바꾸지 않습니다.
        다만, 브랜치 존재 여부를 검증하는 용도로 제공합니다.
        """
        branches = await self.list_branches(db_name)
        if not any(b.get("name") == branch_name for b in branches if isinstance(b, dict)):
            raise DatabaseError(f"브랜치 '{branch_name}'을(를) 찾을 수 없습니다")
        return True
    
    async def reset_branch(
        self,
        db_name: str,
        branch_name: str,
        commit_id: str
    ) -> Dict[str, Any]:
        """
        브랜치를 특정 커밋으로 리셋
        
        Args:
            db_name: 데이터베이스 이름
            branch_name: 브랜치 이름
            commit_id: 대상 커밋 ID
            
        Returns:
            리셋 결과
        """
        try:
            encoded_branch = self._encode_branch_name(branch_name)
            endpoint = f"/api/reset/{self.connection_info.account}/{db_name}/branch/{encoded_branch}"
            
            data = {
                "commit": commit_id,
                "author": self.connection_info.user,
                "message": f"Reset branch {branch_name} to commit {commit_id}"
            }
            
            await self._make_request("POST", endpoint, data)
            
            logger.info(f"Branch '{branch_name}' reset to commit '{commit_id}'")
            
            return {
                "branch": branch_name,
                "reset_to": commit_id,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to reset branch: {e}")
            raise DatabaseError(f"브랜치 리셋 실패: {e}")
    
    async def get_commits(
        self,
        db_name: str,
        branch_name: str = "main",
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        브랜치의 커밋 이력 조회
        
        Args:
            db_name: 데이터베이스 이름
            branch_name: 브랜치 이름
            limit: 최대 결과 수
            offset: 시작 위치
            
        Returns:
            커밋 목록
        """
        await self.db_service.ensure_db_exists(db_name)

        encoded_branch = self._encode_branch_name(branch_name)
        endpoint = f"/api/log/{self.connection_info.account}/{db_name}/local/branch/{encoded_branch}"
        params = {
            "limit": limit,
            "offset": offset
        }

        max_attempts = int(os.getenv("TERMINUS_COMMITS_RETRY_ATTEMPTS", "5") or "5")
        base_delay = float(os.getenv("TERMINUS_COMMITS_RETRY_DELAY_SECONDS", "0.4") or "0.4")

        last_error: Optional[Exception] = None
        for attempt in range(1, max_attempts + 1):
            try:
                result = await self._make_request("GET", endpoint, params=params)

                commits: list[dict[str, Any]] = []

                # 응답 파싱
                if isinstance(result, list):
                    for commit_info in result:
                        commits.append(self._parse_commit_info(commit_info))
                elif isinstance(result, dict):
                    if "@graph" in result:
                        for commit_info in result["@graph"]:
                            commits.append(self._parse_commit_info(commit_info))
                    elif "commits" in result:
                        for commit_info in result["commits"]:
                            commits.append(self._parse_commit_info(commit_info))

                return commits
            except Exception as e:
                last_error = e
                # Empty history is acceptable if the branch/ref is not ready yet on a freshly created DB.
                if self._is_origin_branch_missing_error(e):
                    await asyncio.sleep(base_delay * attempt)
                    continue
                if self._is_transient_request_handler_error(e):
                    await asyncio.sleep(base_delay * attempt)
                    continue
                if "404" in str(e) or "not found" in str(e).lower():
                    return []
                break

        logger.error(f"Failed to get commits after retries: {last_error}")
        # Fail-open for history endpoint callers; avoid returning 5xx on brand-new DBs.
        if last_error and self._is_transient_request_handler_error(last_error):
            return []
        raise DatabaseError(f"커밋 이력 조회 실패: {last_error}")
    
    async def create_commit(
        self,
        db_name: str,
        branch_name: str,
        message: str,
        author: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        새 커밋 생성
        
        Args:
            db_name: 데이터베이스 이름
            branch_name: 브랜치 이름
            message: 커밋 메시지
            author: 작성자 (기본값: 연결 사용자)
            
        Returns:
            생성된 커밋 정보
        """
        try:
            encoded_branch = self._encode_branch_name(branch_name)
            endpoint = f"/api/woql/{self.connection_info.account}/{db_name}/local/branch/{encoded_branch}"
            
            # 빈 WOQL 쿼리로 커밋 생성
            data = {
                "query": {"@type": "True"},  # 빈 쿼리
                "author": author or self.connection_info.user,
                "message": message
            }
            
            result = await self._make_request("POST", endpoint, data)

            # Try to extract commit id from various TerminusDB versions.
            commit_id: Optional[str] = None
            if isinstance(result, str):
                commit_id = result
            elif isinstance(result, dict):
                commit_id = (
                    result.get("commit")
                    or result.get("commit_id")
                    or result.get("identifier")
                    or result.get("@id")
                )
                if not commit_id and isinstance(result.get("@graph"), list) and result["@graph"]:
                    first = result["@graph"][0]
                    if isinstance(first, dict):
                        commit_id = first.get("identifier") or first.get("@id")
            
            logger.info(f"Commit created in branch '{branch_name}' of database '{db_name}'")
            
            return {
                "commit_id": commit_id,
                "branch": branch_name,
                "message": message,
                "author": author or self.connection_info.user,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to create commit: {e}")
            raise DatabaseError(f"커밋 생성 실패: {e}")
    
    async def rebase_branch(
        self,
        db_name: str,
        branch_name: str,
        target_branch: str = "main",
        message: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        브랜치 리베이스
        
        Args:
            db_name: 데이터베이스 이름
            branch_name: 리베이스할 브랜치
            target_branch: 대상 브랜치
            message: 리베이스 메시지
            
        Returns:
            리베이스 결과
        """
        try:
            endpoint = f"/api/rebase/{self.connection_info.account}/{db_name}"

            encoded_branch = self._encode_branch_name(branch_name)
            encoded_target = self._encode_branch_name(target_branch)

            data = {
                "rebase_from": f"{self.connection_info.account}/{db_name}/branch/{encoded_branch}",
                "rebase_to": f"{self.connection_info.account}/{db_name}/branch/{encoded_target}",
                "author": self.connection_info.user,
                "message": message or f"Rebase {branch_name} onto {target_branch}"
            }
            
            result = await self._make_request("POST", endpoint, data)
            
            logger.info(f"Branch '{branch_name}' rebased onto '{target_branch}'")
            
            return {
                "branch": branch_name,
                "rebased_onto": target_branch,
                "result": result,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to rebase branch: {e}")
            raise DatabaseError(f"브랜치 리베이스 실패: {e}")

    async def rebase(
        self,
        db_name: str,
        *,
        branch: str,
        onto: str,
        message: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Router 호환 rebase API (branch -> onto)."""
        return await self.rebase_branch(db_name, branch_name=branch, target_branch=onto, message=message)
    
    async def squash_commits(
        self,
        db_name: str,
        branch_name: str,
        commit_id: str,
        message: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        커밋 스쿼시 (여러 커밋을 하나로 합치기)
        
        Args:
            db_name: 데이터베이스 이름
            branch_name: 브랜치 이름
            commit_id: 스쿼시 대상 커밋 ID
            message: 새 커밋 메시지
            
        Returns:
            스쿼시 결과
        """
        try:
            encoded_branch = self._encode_branch_name(branch_name)
            endpoint = f"/api/squash/{self.connection_info.account}/{db_name}/branch/{encoded_branch}"
            
            data = {
                "commit": commit_id,
                "author": self.connection_info.user,
                "message": message or f"Squashed commits up to {commit_id}"
            }
            
            result = await self._make_request("POST", endpoint, data)
            
            logger.info(f"Commits squashed in branch '{branch_name}'")
            
            return {
                "branch": branch_name,
                "squashed_to": commit_id,
                "result": result,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to squash commits: {e}")
            raise DatabaseError(f"커밋 스쿼시 실패: {e}")
    
    async def get_diff(
        self,
        db_name: str,
        from_ref: str,
        to_ref: str
    ) -> Dict[str, Any]:
        """
        두 참조(브랜치/커밋) 간의 차이점 조회
        
        Args:
            db_name: 데이터베이스 이름
            from_ref: 시작 참조 (브랜치명 또는 커밋 ID)
            to_ref: 대상 참조 (브랜치명 또는 커밋 ID)
            
        Returns:
            차이점 정보
        """
        try:
            # TerminusDB diff API varies by version; try a few known shapes.
            endpoint = "/api/diff"

            account_db = f"{self.connection_info.account}/{db_name}"
            full_from = f"{account_db}/local/branch/{from_ref}"
            full_to = f"{account_db}/local/branch/{to_ref}"

            candidates = [
                {"database": account_db, "from": from_ref, "to": to_ref},
                {"database": account_db, "before": from_ref, "after": to_ref},
                {"from": full_from, "to": full_to},
                {"before": full_from, "after": full_to},
                {"from": from_ref, "to": to_ref},
                {"before": from_ref, "after": to_ref},
            ]

            last_error: Optional[Exception] = None
            for payload in candidates:
                try:
                    result = await self._make_request("POST", endpoint, payload)
                    return {"from": from_ref, "to": to_ref, "changes": result}
                except Exception as e:
                    last_error = e
                    continue

            raise DatabaseError(f"차이점 조회 실패: {last_error}")
            
        except Exception as e:
            logger.error(f"Failed to get diff: {e}")
            raise DatabaseError(f"차이점 조회 실패: {e}")

    async def diff(self, db_name: str, from_ref: str, to_ref: str) -> Any:
        """Router 호환 diff API: changes(리스트/딕트)만 반환."""
        diff_result = await self.get_diff(db_name, from_ref, to_ref)
        if isinstance(diff_result, dict) and "changes" in diff_result:
            return diff_result["changes"]
        return diff_result

    async def commit(
        self,
        db_name: str,
        message: str,
        author: str = "admin",
        branch: Optional[str] = None,
    ) -> str:
        """Router 호환 commit API: commit_id 문자열 반환."""
        info = await self.create_commit(db_name, branch_name=branch or "main", message=message, author=author)
        commit_id = info.get("commit_id")
        return commit_id or ""

    async def get_commit_history(
        self,
        db_name: str,
        branch: str = "main",
        limit: int = 10,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Router 호환 commit history API."""
        return await self.get_commits(db_name, branch_name=branch, limit=limit, offset=offset)

    async def merge(
        self,
        db_name: str,
        *,
        source_branch: str,
        target_branch: str,
        strategy: str = "auto",
        author: Optional[str] = None,
        message: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Router 호환 merge API.

        TerminusDB merge API도 버전별로 파라미터가 달라 여러 형태를 시도합니다.
        """
        endpoint = "/api/merge"
        account_db = f"{self.connection_info.account}/{db_name}"

        base = {}
        if author:
            base["author"] = author
        if message:
            base["message"] = message
        if strategy:
            base["strategy"] = strategy

        candidates = [
            {**base, "database": account_db, "from": source_branch, "into": target_branch},
            {**base, "database": account_db, "source": source_branch, "target": target_branch},
            {**base, "database": account_db, "from_branch": source_branch, "to_branch": target_branch},
            {**base, "from": source_branch, "into": target_branch},
            {**base, "source": source_branch, "target": target_branch},
            {**base, "from_branch": source_branch, "to_branch": target_branch},
        ]

        last_error: Optional[Exception] = None
        for payload in candidates:
            try:
                result = await self._make_request("POST", endpoint, payload)
                # Normalize result shape a bit for routers.
                if isinstance(result, dict):
                    return {
                        "merged": result.get("merged", True),
                        "conflicts": result.get("conflicts", []),
                        "raw": result,
                    }
                return {"merged": True, "conflicts": [], "raw": result}
            except Exception as e:
                last_error = e
                continue

        raise DatabaseError(f"브랜치 머지 실패: {last_error}")
    
    def _parse_commit_info(self, commit_data: Any) -> Dict[str, Any]:
        """커밋 정보 파싱"""
        if isinstance(commit_data, str):
            return {
                "id": commit_data,
                "identifier": commit_data,
                "message": "",
                "author": "",
                "timestamp": None
            }
        elif isinstance(commit_data, dict):
            identifier = commit_data.get("identifier") or commit_data.get("id") or commit_data.get("@id", "")
            return {
                "id": identifier,
                "identifier": identifier,
                "message": commit_data.get("message", commit_data.get("comment", "")),
                "author": commit_data.get("author", ""),
                "timestamp": commit_data.get("timestamp", commit_data.get("created")),
                "parent": commit_data.get("parent"),
                "branch": commit_data.get("branch")
            }
        else:
            return {
                "id": str(commit_data),
                "identifier": str(commit_data),
                "message": "",
                "author": "",
                "timestamp": None
            }
